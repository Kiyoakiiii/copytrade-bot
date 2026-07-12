from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Any

from app.services.calculator import SIZING_MODE_ACCOUNT_RATIO, calculate_target_notional_by_account_ratio
from app.services.execution_router import ExecutionVenue
from app.services.hedge_orders import HedgeAction, HedgeModeOrder, build_hedge_mode_order
from app.services.leader_config import decimal_to_string
from app.services.target_position import PositionSide


class AllocationStatus(str, Enum):
    OPEN = "OPEN"
    REDUCING = "REDUCING"
    CLOSED = "CLOSED"
    BLOCKED = "BLOCKED"
    NEEDS_MANUAL_REVIEW = "NEEDS_MANUAL_REVIEW"


class AllocationTransitionAction(str, Enum):
    OPEN = "OPEN"
    INCREASE = "INCREASE"
    REDUCE = "REDUCE"
    CLOSE = "CLOSE"
    FLIP_CLOSE_FIRST = "FLIP_CLOSE_FIRST"
    FLIP_OPEN_SECOND = "FLIP_OPEN_SECOND"
    NOOP = "NOOP"
    BLOCK = "BLOCK"


class AllocationScopeError(ValueError):
    pass


ALLOCATION_TRANSITION_TOLERANCE = Decimal("0.00000001")
ALLOCATION_DUST_LIFECYCLE_NOTIONAL = Decimal("10")
LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK = "LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK"
MAX_POSITION_NOTIONAL_CAP_APPLIED = "MAX_POSITION_NOTIONAL_CAP_APPLIED"
DUST_LIFECYCLE_ACCOUNT_RATIO_RESET = "DUST_LIFECYCLE_ACCOUNT_RATIO_RESET"


@dataclass(frozen=True)
class AllocationTransitionPlan:
    action: AllocationTransitionAction
    old_side: PositionSide | None
    new_side: PositionSide
    target_notional: Decimal
    current_allocation_notional: Decimal
    delta_notional: Decimal
    close_qty_limit: Decimal
    open_qty: Decimal
    reduce_only: bool
    reason: str
    sizing_mode: str = SIZING_MODE_ACCOUNT_RATIO
    formula_inputs: dict[str, Any] | None = None


@dataclass(frozen=True)
class LeaderTargetAllocation:
    leader_id: int
    leader_address: str
    symbol: str
    coin: str
    position_side: PositionSide
    target_notional_abs: Decimal
    target_notional_signed_from_leader_view: Decimal
    target_qty: Decimal
    copy_multiplier: Decimal
    dex: str = ""
    canonical_coin: str | None = None


@dataclass(frozen=True)
class LeaderPositionAllocation:
    leader_id: int
    leader_address: str
    hyperliquid_coin: str
    binance_symbol: str | None
    position_side: PositionSide
    target_notional: Decimal
    allocated_notional: Decimal
    allocated_qty: Decimal
    avg_entry_price: Decimal
    last_leader_account_value: Decimal
    last_leader_position_notional: Decimal
    copy_multiplier: Decimal
    last_leader_position_size: Decimal | None = None
    dex: str = ""
    canonical_coin: str | None = None
    status: AllocationStatus = AllocationStatus.OPEN
    last_source_fill_id: str | None = None
    execution_venue: ExecutionVenue = ExecutionVenue.BINANCE
    venue_account: str | None = None
    venue_symbol: str | None = None


@dataclass(frozen=True)
class AggregatePosition:
    symbol: str
    long_qty: Decimal
    short_qty: Decimal


@dataclass(frozen=True)
class AllocationValidationResult:
    ok: bool
    symbol: str
    long_allocated_qty: Decimal
    short_allocated_qty: Decimal
    binance_long_qty: Decimal
    binance_short_qty: Decimal
    tolerance: Decimal
    source: str = "AUTO"
    event_type: str = "OK"
    reason: str | None = None


def _q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


def plan_leader_allocation_transition(
    *,
    leader_id: int,
    execution_venue: str | ExecutionVenue,
    dex: str,
    canonical_coin: str,
    leader_side: str | PositionSide,
    leader_position_notional: Decimal | None,
    leader_position_size: Decimal | None = None,
    leader_account_value_used: Decimal | None,
    follower_account_value_used: Decimal | None,
    copy_multiplier: Decimal,
    current_allocation: Any,
    max_position_notional: Decimal | None = None,
    leader_fill_notional: Decimal | None = None,
    leader_previous_position_size: Decimal | None = None,
    leader_fill_is_reduce_or_close: bool = False,
) -> AllocationTransitionPlan:
    """Plan one leader/venue/dex/coin allocation transition using ACCOUNT_RATIO only."""

    venue = _venue_value(execution_venue)
    dex_key = str(dex or "").lower()
    canonical = str(canonical_coin or "").upper()
    old_side = _allocation_side(current_allocation)
    new_side = _side_or_flat(leader_side, leader_position_notional)
    current_notional = _allocation_notional(current_allocation)
    current_qty = _allocation_qty(current_allocation)
    active_current = _active_allocation(current_allocation)
    formula_inputs = {
        "leader_id": int(leader_id),
        "execution_venue": venue,
        "dex": dex_key,
        "canonical_coin": canonical,
        "leader_side": new_side.value,
        "leader_position_notional": str(leader_position_notional) if leader_position_notional is not None else None,
        "leader_position_size": str(leader_position_size) if leader_position_size is not None else None,
        "previous_leader_position_size": str(getattr(current_allocation, "last_leader_position_size", "")) if current_allocation is not None else None,
        "leader_fill_previous_position_size": str(leader_previous_position_size) if leader_previous_position_size is not None else None,
        "leader_fill_is_reduce_or_close": bool(leader_fill_is_reduce_or_close),
        "previous_leader_position_notional": str(getattr(current_allocation, "last_leader_position_notional", "")) if current_allocation is not None else None,
        "leader_fill_notional": str(leader_fill_notional) if leader_fill_notional is not None else None,
        "leader_account_value_used": str(leader_account_value_used) if leader_account_value_used is not None else None,
        "follower_account_value_used": str(follower_account_value_used) if follower_account_value_used is not None else None,
        "copy_multiplier": decimal_to_string(copy_multiplier),
        "max_position_notional_cap": None,
        "target_notional_before_cap": None,
        "warnings": [],
        "formula": (
            "target_notional = follower_account_value_used * "
            "abs(leader_position_notional / leader_account_value_used) * copy_multiplier"
        ),
    }

    if copy_multiplier <= 0:
        return _transition(
            AllocationTransitionAction.BLOCK,
            old_side,
            new_side,
            Decimal("0"),
            current_notional,
            Decimal("0"),
            current_qty,
            Decimal("0"),
            False,
            "copy_multiplier must be positive",
            formula_inputs,
        )

    if new_side == PositionSide.FLAT or leader_position_notional is None or leader_position_notional == 0:
        if active_current and current_notional > ALLOCATION_TRANSITION_TOLERANCE:
            return _transition(
                AllocationTransitionAction.CLOSE,
                old_side,
                PositionSide.FLAT,
                Decimal("0.00000000"),
                current_notional,
                -current_notional,
                current_qty,
                Decimal("0"),
                True,
                "leader flat; close only this leader allocation",
                formula_inputs,
            )
        return _transition(
            AllocationTransitionAction.NOOP,
            old_side,
            PositionSide.FLAT,
            Decimal("0.00000000"),
            current_notional,
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            False,
            "leader flat and allocation already flat",
            formula_inputs,
        )

    account_value_block_reason = _account_value_block_reason(
        leader_account_value_used=leader_account_value_used,
        follower_account_value_used=follower_account_value_used,
    )
    target: Decimal | None = None
    if account_value_block_reason is None:
        target = calculate_target_notional_by_account_ratio(
            leader_account_value=leader_account_value_used,
            leader_position_notional=leader_position_notional,
            follower_account_value=follower_account_value_used,
            copy_multiplier=copy_multiplier,
        )
        target_before_cap = target
        target, cap, cap_applied = _cap_target_notional(target, max_position_notional)
        if cap is not None:
            formula_inputs["max_position_notional_cap"] = str(cap)
            formula_inputs["target_notional_before_cap"] = str(target_before_cap)
        if cap_applied:
            formula_inputs["warnings"].append(MAX_POSITION_NOTIONAL_CAP_APPLIED)
            formula_inputs["risk_cap_formula"] = (
                "target_notional = min(account_ratio_target_notional, max_position_notional_cap)"
            )
        formula_inputs["target_notional"] = str(target)

    if old_side and old_side != new_side:
        if active_current and current_notional > ALLOCATION_TRANSITION_TOLERANCE:
            return _transition(
                AllocationTransitionAction.FLIP_CLOSE_FIRST,
                old_side,
                new_side,
                target if target is not None else Decimal("0"),
                current_notional,
                -current_notional,
                current_qty,
                Decimal("0"),
                True,
                "opposite side; close this leader old allocation first",
                formula_inputs,
            )
        if target is None:
            return _account_value_block_transition(
                old_side=old_side,
                new_side=new_side,
                current_notional=current_notional,
                reason=account_value_block_reason,
                formula_inputs=formula_inputs,
            )
        return _transition(
            AllocationTransitionAction.FLIP_OPEN_SECOND,
            old_side,
            new_side,
            target,
            Decimal("0"),
            target,
            Decimal("0"),
            Decimal("0"),
            False,
            "old allocation flat; open new side after close confirmation",
            formula_inputs,
        )

    dust_lifecycle_reset = _same_side_dust_lifecycle_should_use_account_ratio(
        current_allocation=current_allocation,
        old_side=old_side,
        new_side=new_side,
        current_notional=current_notional,
        leader_position_size=leader_position_size,
        leader_position_notional=leader_position_notional,
        leader_previous_position_size=leader_previous_position_size,
    )
    if dust_lifecycle_reset:
        formula_inputs["warnings"].append(DUST_LIFECYCLE_ACCOUNT_RATIO_RESET)
        formula_inputs["dust_lifecycle_reset_formula"] = (
            "same-side allocation and previous leader position were below the actionable minimum; "
            "treat the current real leader position as a fresh account-ratio lifecycle instead of "
            "multiplying a dust residual"
        )
        if target is None:
            return _account_value_block_transition(
                old_side=old_side,
                new_side=new_side,
                current_notional=current_notional,
                reason=account_value_block_reason,
                formula_inputs=formula_inputs,
            )
        delta = target - current_notional
        if delta > ALLOCATION_TRANSITION_TOLERANCE:
            return _transition(
                AllocationTransitionAction.INCREASE if active_current else AllocationTransitionAction.OPEN,
                old_side,
                new_side,
                target,
                current_notional,
                delta,
                Decimal("0"),
                Decimal("0"),
                False,
                "dust residual lifecycle reset; use fresh account-ratio sizing",
                formula_inputs,
            )

    incremental_increase = _proportional_increase_delta(
        current_allocation=current_allocation,
        current_notional=current_notional,
        current_qty=current_qty,
        leader_position_size=leader_position_size,
        leader_position_notional=leader_position_notional,
        leader_previous_position_size=leader_previous_position_size,
    )
    if current_allocation is not None and old_side == new_side and incremental_increase is not None:
        incremental_delta, increment_source, increase_ratio, increase_qty = incremental_increase
        incremental_delta_before_pending_offset = incremental_delta
        incremental_qty_before_pending_offset = increase_qty
        pending_reduce_notional = min(
            _allocation_pending_reduce_notional(current_allocation, current_notional, current_qty),
            current_notional,
        )
        pending_reduce_qty = min(_allocation_pending_reduce_qty(current_allocation), current_qty)
        pending_reduce_offset_qty = min(increase_qty, pending_reduce_qty)
        pending_reduce_offset = (
            min(incremental_delta, pending_reduce_notional)
            if pending_reduce_qty <= ALLOCATION_TRANSITION_TOLERANCE
            else min(incremental_delta, pending_reduce_notional * (pending_reduce_offset_qty / pending_reduce_qty))
        )
        if pending_reduce_offset_qty > ALLOCATION_TRANSITION_TOLERANCE or pending_reduce_offset > ALLOCATION_TRANSITION_TOLERANCE:
            pending_remaining_notional = _q(max(Decimal("0"), pending_reduce_notional - pending_reduce_offset))
            pending_remaining_qty = _q(max(Decimal("0"), pending_reduce_qty - pending_reduce_offset_qty))
            formula_inputs["pending_reduce_offset_notional"] = str(_q(pending_reduce_offset))
            formula_inputs["pending_reduce_remaining_notional"] = str(pending_remaining_notional)
            formula_inputs["pending_reduce_remaining_qty"] = str(pending_remaining_qty)
            formula_inputs["pending_reduce_before_increase_notional"] = str(pending_reduce_notional)
            formula_inputs["pending_reduce_before_increase_qty"] = str(pending_reduce_qty)
            incremental_delta = max(Decimal("0"), incremental_delta - pending_reduce_offset)
            increase_qty = max(Decimal("0"), increase_qty - pending_reduce_offset_qty)
        target_before_increment_cap = current_notional + incremental_delta
        target, cap, cap_applied = _cap_target_notional(target_before_increment_cap, max_position_notional)
        delta = target - current_notional
        if cap_applied and target_before_increment_cap > current_notional + ALLOCATION_TRANSITION_TOLERANCE:
            increase_qty = _q(increase_qty * max(Decimal("0"), delta / (target_before_increment_cap - current_notional)))
        if cap is not None:
            formula_inputs["max_position_notional_cap"] = str(cap)
            formula_inputs["target_notional_before_cap"] = str(target_before_increment_cap)
        if cap_applied and MAX_POSITION_NOTIONAL_CAP_APPLIED not in formula_inputs["warnings"]:
            formula_inputs["warnings"].append(MAX_POSITION_NOTIONAL_CAP_APPLIED)
            formula_inputs["risk_cap_formula"] = (
                "target_notional = min(current_allocation_notional + proportional_increase_target_notional, "
                "max_position_notional_cap)"
            )
        formula_inputs["target_notional"] = str(target)
        formula_inputs["increase_formula"] = (
            "same-side increase uses leader position-size increase ratio inside the current allocation lifecycle; "
            "account balance changes do not resize this lifecycle and skipped below-min add notional is not accumulated"
        )
        formula_inputs["increase_delta_source"] = increment_source
        formula_inputs["fill_delta_target_notional"] = str(incremental_delta)
        formula_inputs["increase_delta_before_pending_offset_notional"] = str(_q(incremental_delta_before_pending_offset))
        formula_inputs["increase_qty_before_pending_offset"] = str(_q(incremental_qty_before_pending_offset))
        formula_inputs["increase_qty"] = str(_q(increase_qty))
        formula_inputs["leader_position_increase_ratio"] = str(increase_ratio)
        formula_inputs["follower_increase_ratio"] = str(increase_ratio)
        if increment_source == "leader_position_notional":
            formula_inputs["warnings"].append(LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK)
        if delta > ALLOCATION_TRANSITION_TOLERANCE:
            return _transition(
                AllocationTransitionAction.INCREASE if active_current else AllocationTransitionAction.OPEN,
                old_side,
                new_side,
                target,
                current_notional,
                delta,
                Decimal("0"),
                increase_qty,
                False,
                "leader increased same-side position; copy only this leader add",
                formula_inputs,
            )
        if pending_reduce_offset > ALLOCATION_TRANSITION_TOLERANCE:
            return _transition(
                AllocationTransitionAction.NOOP,
                old_side,
                new_side,
                current_notional,
                current_notional,
                Decimal("0"),
                Decimal("0"),
                Decimal("0"),
                False,
                "leader add offsets pending reduce; no follower increase yet",
                formula_inputs,
            )
        return _transition(
            AllocationTransitionAction.NOOP,
            old_side,
            new_side,
            target,
            current_notional,
            delta,
            Decimal("0"),
            Decimal("0"),
            False,
            "same-side leader add is at or above cap / inside tolerance",
            formula_inputs,
        )

    if (not active_current or current_notional <= ALLOCATION_TRANSITION_TOLERANCE) and leader_fill_is_reduce_or_close:
        return _transition(
            AllocationTransitionAction.NOOP,
            old_side,
            new_side,
            current_notional,
            current_notional,
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            False,
            "leader reduce/close fill cannot open or increase follower allocation",
            formula_inputs,
        )

    if not active_current or current_notional <= ALLOCATION_TRANSITION_TOLERANCE:
        if target is None:
            return _account_value_block_transition(
                old_side=old_side,
                new_side=new_side,
                current_notional=current_notional,
                reason=account_value_block_reason,
                formula_inputs=formula_inputs,
            )
        return _transition(
            AllocationTransitionAction.OPEN,
            old_side,
            new_side,
            target,
            Decimal("0"),
            target,
            Decimal("0"),
            Decimal("0"),
            False,
            "new leader allocation",
            formula_inputs,
        )

    proportional_reduce = _proportional_reduce_plan(
        current_allocation=current_allocation,
        current_notional=current_notional,
        current_qty=current_qty,
        leader_position_size=leader_position_size,
        leader_position_notional=leader_position_notional,
        leader_previous_position_size=leader_previous_position_size,
    )
    if proportional_reduce is not None:
        target, close_qty_limit, remaining_ratio, ratio_source = proportional_reduce
        target, cap, cap_applied = _cap_target_notional(target, max_position_notional)
        delta = target - current_notional
        if cap is not None:
            formula_inputs["max_position_notional_cap"] = str(cap)
        if cap_applied:
            close_qty_limit = _close_qty_limit_for_delta(abs(delta), current_allocation)
            if MAX_POSITION_NOTIONAL_CAP_APPLIED not in formula_inputs["warnings"]:
                formula_inputs["warnings"].append(MAX_POSITION_NOTIONAL_CAP_APPLIED)
            formula_inputs["risk_cap_formula"] = (
                "target_notional = min(proportional_reduce_target_notional, max_position_notional_cap)"
            )
        formula_inputs["target_notional"] = str(target)
        formula_inputs["reduce_formula"] = (
            "same-side reduce uses leader remaining position ratio; "
            "target_notional = (current_allocation_notional - pending_reduce_notional) * remaining_ratio"
        )
        formula_inputs["remaining_ratio"] = str(remaining_ratio)
        formula_inputs["follower_reduce_ratio"] = str(Decimal("1") - remaining_ratio)
        formula_inputs["reduce_ratio_source"] = ratio_source
        formula_inputs["pending_reduce_qty_before_plan"] = str(_allocation_pending_reduce_qty(current_allocation))
        formula_inputs["pending_reduce_notional_before_plan"] = str(
            _allocation_pending_reduce_notional(current_allocation, current_notional, current_qty)
        )
        if ratio_source == "leader_position_notional":
            formula_inputs["warnings"].append(LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK)
        if delta < -ALLOCATION_TRANSITION_TOLERANCE:
            return _transition(
                AllocationTransitionAction.REDUCE,
                old_side,
                new_side,
                target,
                current_notional,
                delta,
                close_qty_limit,
                Decimal("0"),
                True,
                "leader reduced same-side position; reduce follower by same percentage",
                formula_inputs,
            )

    if leader_fill_is_reduce_or_close:
        return _transition(
            AllocationTransitionAction.NOOP,
            old_side,
            new_side,
            current_notional,
            current_notional,
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            False,
            "leader reduce/close fill cannot create account-ratio catch-up increase",
            formula_inputs,
        )

    cap = _decimal_or_none(max_position_notional)
    if old_side == new_side:
        if cap is not None and cap > 0 and current_notional > _q(cap) + ALLOCATION_TRANSITION_TOLERANCE:
            target = _q(cap)
            formula_inputs["max_position_notional_cap"] = str(target)
            formula_inputs["target_notional"] = str(target)
            if MAX_POSITION_NOTIONAL_CAP_APPLIED not in formula_inputs["warnings"]:
                formula_inputs["warnings"].append(MAX_POSITION_NOTIONAL_CAP_APPLIED)
            close_qty_limit = _close_qty_limit_for_delta(abs(target - current_notional), current_allocation)
            return _transition(
                AllocationTransitionAction.REDUCE,
                old_side,
                new_side,
                target,
                current_notional,
                target - current_notional,
                close_qty_limit,
                Decimal("0"),
                True,
                "max position cap below current allocation",
                formula_inputs,
            )
        return _transition(
            AllocationTransitionAction.NOOP,
            old_side,
            new_side,
            current_notional,
            current_notional,
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            False,
            "same-side leader size unchanged; keep lifecycle allocation",
            formula_inputs,
        )

    if target is None:
        return _account_value_block_transition(
            old_side=old_side,
            new_side=new_side,
            current_notional=current_notional,
            reason=account_value_block_reason,
            formula_inputs=formula_inputs,
        )

    delta = target - current_notional
    if delta > ALLOCATION_TRANSITION_TOLERANCE:
        return _transition(
            AllocationTransitionAction.INCREASE,
            old_side,
            new_side,
            target,
            current_notional,
            delta,
            Decimal("0"),
            Decimal("0"),
            False,
            "target above current allocation",
            formula_inputs,
        )
    if delta < -ALLOCATION_TRANSITION_TOLERANCE:
        close_qty_limit = _close_qty_limit_for_delta(abs(delta), current_allocation)
        return _transition(
            AllocationTransitionAction.REDUCE,
            old_side,
            new_side,
            target,
            current_notional,
            delta,
            close_qty_limit,
            Decimal("0"),
            True,
            "target below current allocation",
            formula_inputs,
        )
    return _transition(
        AllocationTransitionAction.NOOP,
        old_side,
        new_side,
        target,
        current_notional,
        delta,
        Decimal("0"),
        Decimal("0"),
        False,
        "inside allocation transition tolerance",
        formula_inputs,
    )


def assert_allocation_scope(
    order_plan: Any,
    allocation: Any | None = None,
    *,
    aggregate_follower_qty: Decimal | None = None,
    allocation_sum_qty: Decimal | None = None,
) -> None:
    fields = _payload(order_plan)
    action = str(fields.get("action") or fields.get("order_action") or "").upper()
    if action not in {"REDUCE", "CLOSE", "FLIP_CLOSE_FIRST", "CLOSE_OR_REDUCE"}:
        return
    allocation = allocation or fields.get("allocation") or fields.get("current_allocation")
    if allocation is None:
        raise AllocationScopeError("close/reduce requires a leader allocation")

    checks = {
        "leader_id": (_int_or_none(fields.get("leader_id")), _int_or_none(getattr(allocation, "leader_id", None))),
        "execution_venue": (_upper(fields.get("execution_venue")), _upper(getattr(allocation, "execution_venue", None))),
        "dex": (_lower(fields.get("dex")), _lower(getattr(allocation, "dex", None))),
        "canonical_coin": (_upper(fields.get("canonical_coin")), _upper(getattr(allocation, "canonical_coin", None))),
        "side": (_upper(fields.get("old_side") or fields.get("position_side") or fields.get("side")), _upper(getattr(allocation, "position_side", None))),
    }
    for name, (actual, expected) in checks.items():
        if expected is not None and actual != expected:
            raise AllocationScopeError(f"allocation scope mismatch on {name}: {actual} != {expected}")

    qty = _decimal_or_none(fields.get("quantity") or fields.get("close_qty") or fields.get("close_qty_limit"))
    allocation_qty = _allocation_qty(allocation)
    if qty is not None and qty > allocation_qty + ALLOCATION_TRANSITION_TOLERANCE:
        raise AllocationScopeError("close/reduce quantity exceeds this leader allocation qty")
    if qty is not None and aggregate_follower_qty is not None and qty > aggregate_follower_qty + ALLOCATION_TRANSITION_TOLERANCE:
        raise AllocationScopeError("close/reduce quantity exceeds aggregate follower position qty")
    if allocation_sum_qty is not None and aggregate_follower_qty is not None:
        if aggregate_follower_qty + ALLOCATION_TRANSITION_TOLERANCE < allocation_sum_qty and qty is None:
            raise AllocationScopeError("ALLOCATION_MISMATCH: aggregate follower qty below allocation sum")


def _transition(
    action: AllocationTransitionAction,
    old_side: PositionSide | None,
    new_side: PositionSide,
    target_notional: Decimal,
    current_allocation_notional: Decimal,
    delta_notional: Decimal,
    close_qty_limit: Decimal,
    open_qty: Decimal,
    reduce_only: bool,
    reason: str,
    formula_inputs: dict[str, Any],
) -> AllocationTransitionPlan:
    return AllocationTransitionPlan(
        action=action,
        old_side=old_side,
        new_side=new_side,
        target_notional=_q(target_notional),
        current_allocation_notional=_q(current_allocation_notional),
        delta_notional=_q(delta_notional),
        close_qty_limit=_q(close_qty_limit),
        open_qty=_q(open_qty),
        reduce_only=reduce_only,
        reason=reason,
        formula_inputs=formula_inputs,
    )


def _cap_target_notional(
    target: Decimal,
    max_position_notional: Decimal | None,
) -> tuple[Decimal, Decimal | None, bool]:
    cap = _decimal_or_none(max_position_notional)
    if cap is None or cap <= 0:
        return target, None, False
    cap = _q(cap)
    if target > cap:
        return cap, cap, True
    return target, cap, False


def _account_value_block_reason(
    *,
    leader_account_value_used: Decimal | None,
    follower_account_value_used: Decimal | None,
) -> str | None:
    if leader_account_value_used is None or leader_account_value_used <= 0:
        return "leader account value unavailable"
    if follower_account_value_used is None or follower_account_value_used <= 0:
        return "follower account value unavailable"
    return None


def _account_value_block_transition(
    *,
    old_side: PositionSide | None,
    new_side: PositionSide,
    current_notional: Decimal,
    reason: str | None,
    formula_inputs: dict[str, Any],
) -> AllocationTransitionPlan:
    return _transition(
        AllocationTransitionAction.BLOCK,
        old_side,
        new_side,
        Decimal("0"),
        current_notional,
        Decimal("0"),
        Decimal("0"),
        Decimal("0"),
        False,
        reason or "account value unavailable",
        formula_inputs,
    )


def _side_or_flat(value: str | PositionSide, notional: Decimal | None) -> PositionSide:
    if notional is not None:
        if notional > 0:
            return PositionSide.LONG
        if notional < 0:
            return PositionSide.SHORT
        return PositionSide.FLAT
    try:
        return PositionSide(str(value).upper())
    except ValueError:
        return PositionSide.FLAT


def _allocation_side(allocation: Any) -> PositionSide | None:
    if allocation is None:
        return None
    try:
        return PositionSide(str(getattr(allocation, "position_side", "")).upper())
    except ValueError:
        return None


def _active_allocation(allocation: Any) -> bool:
    if allocation is None:
        return False
    if str(getattr(allocation, "status", "")).upper() == AllocationStatus.CLOSED.value:
        return False
    return _allocation_notional(allocation) > ALLOCATION_TRANSITION_TOLERANCE or _allocation_qty(allocation) > ALLOCATION_TRANSITION_TOLERANCE


def _allocation_notional(allocation: Any) -> Decimal:
    if allocation is None:
        return Decimal("0")
    return abs(_decimal_or_none(getattr(allocation, "allocated_notional", None)) or Decimal("0"))


def _allocation_qty(allocation: Any) -> Decimal:
    if allocation is None:
        return Decimal("0")
    return abs(_decimal_or_none(getattr(allocation, "allocated_qty", None)) or Decimal("0"))


def _close_qty_limit_for_delta(delta_abs: Decimal, allocation: Any) -> Decimal:
    allocation_qty = _allocation_qty(allocation)
    avg_entry = _decimal_or_none(getattr(allocation, "avg_entry_price", None))
    if allocation_qty <= 0:
        return Decimal("0")
    if avg_entry is None or avg_entry <= 0:
        return allocation_qty
    return min(allocation_qty, _q(delta_abs / avg_entry))


def _proportional_reduce_plan(
    *,
    current_allocation: Any,
    current_notional: Decimal,
    current_qty: Decimal,
    leader_position_size: Decimal | None,
    leader_position_notional: Decimal | None,
    leader_previous_position_size: Decimal | None = None,
) -> tuple[Decimal, Decimal, Decimal, str] | None:
    explicit_previous_size = _decimal_or_none(leader_previous_position_size)
    remaining_ratio = _remaining_leader_ratio(
        current_value=leader_position_size,
        previous_value=explicit_previous_size
        if explicit_previous_size is not None
        else _decimal_or_none(getattr(current_allocation, "last_leader_position_size", None)),
    )
    ratio_source = "leader_fill_start_position_size" if explicit_previous_size is not None else "leader_position_size"
    if remaining_ratio is None:
        remaining_ratio = _remaining_leader_ratio(
            current_value=leader_position_notional,
            previous_value=_decimal_or_none(getattr(current_allocation, "last_leader_position_notional", None)),
        )
        ratio_source = "leader_position_notional"
    if remaining_ratio is None or remaining_ratio >= Decimal("1"):
        return None
    pending_qty = min(_allocation_pending_reduce_qty(current_allocation), current_qty)
    pending_notional = min(
        _allocation_pending_reduce_notional(current_allocation, current_notional, current_qty),
        current_notional,
    )
    effective_qty = max(Decimal("0"), current_qty - pending_qty)
    effective_notional = max(Decimal("0"), current_notional - pending_notional)
    incremental_close_qty = effective_qty * (Decimal("1") - remaining_ratio)
    target = _q(effective_notional * remaining_ratio)
    close_qty_limit = _q(min(current_qty, pending_qty + incremental_close_qty))
    return target, close_qty_limit, remaining_ratio, ratio_source


def _proportional_increase_delta(
    *,
    current_allocation: Any,
    current_notional: Decimal,
    current_qty: Decimal,
    leader_position_size: Decimal | None,
    leader_position_notional: Decimal | None,
    leader_previous_position_size: Decimal | None,
) -> tuple[Decimal, str, Decimal, Decimal] | None:
    if current_allocation is None:
        return None
    if current_notional <= ALLOCATION_TRANSITION_TOLERANCE:
        return None
    explicit_previous_size = _decimal_or_none(leader_previous_position_size)
    previous_size = (
        explicit_previous_size
        if explicit_previous_size is not None
        else _decimal_or_none(getattr(current_allocation, "last_leader_position_size", None))
    )
    current_size = _decimal_or_none(leader_position_size)
    if previous_size is not None and current_size is not None:
        previous_abs = abs(previous_size)
        current_abs = abs(current_size)
        if previous_abs <= ALLOCATION_TRANSITION_TOLERANCE:
            return None
        if current_abs <= previous_abs + ALLOCATION_TRANSITION_TOLERANCE:
            return None
        increase_ratio = (current_abs - previous_abs) / previous_abs
        source = "leader_fill_start_position_size" if explicit_previous_size is not None else "leader_position_size"
        return _q(current_notional * increase_ratio), source, increase_ratio, _q(current_qty * increase_ratio)

    previous_notional = _decimal_or_none(getattr(current_allocation, "last_leader_position_notional", None))
    current_leader_notional = _decimal_or_none(leader_position_notional)
    if previous_notional is None or current_leader_notional is None:
        return None
    previous_abs = abs(previous_notional)
    current_abs = abs(current_leader_notional)
    if previous_abs <= ALLOCATION_TRANSITION_TOLERANCE:
        return None
    if current_abs <= previous_abs + ALLOCATION_TRANSITION_TOLERANCE:
        return None
    increase_ratio = (current_abs - previous_abs) / previous_abs
    return (
        _q(_allocation_notional(current_allocation) * increase_ratio),
        "leader_position_notional",
        increase_ratio,
        _q(_allocation_qty(current_allocation) * increase_ratio),
    )


def _same_side_dust_lifecycle_should_use_account_ratio(
    *,
    current_allocation: Any,
    old_side: PositionSide | None,
    new_side: PositionSide,
    current_notional: Decimal,
    leader_position_size: Decimal | None,
    leader_position_notional: Decimal | None,
    leader_previous_position_size: Decimal | None,
) -> bool:
    if current_allocation is None or old_side != new_side or new_side == PositionSide.FLAT:
        return False
    if current_notional >= ALLOCATION_DUST_LIFECYCLE_NOTIONAL:
        return False

    current_leader_notional = _decimal_or_none(leader_position_notional)
    if current_leader_notional is None:
        return False
    current_leader_notional_abs = abs(current_leader_notional)
    if current_leader_notional_abs < ALLOCATION_DUST_LIFECYCLE_NOTIONAL:
        return False

    previous_leader_notional = _decimal_or_none(getattr(current_allocation, "last_leader_position_notional", None))
    previous_leader_notional_abs = abs(previous_leader_notional) if previous_leader_notional is not None else None
    if previous_leader_notional_abs is None:
        explicit_previous_size = _decimal_or_none(leader_previous_position_size)
        previous_size = (
            explicit_previous_size
            if explicit_previous_size is not None
            else _decimal_or_none(getattr(current_allocation, "last_leader_position_size", None))
        )
        current_size = _decimal_or_none(leader_position_size)
        if previous_size is None or current_size is None:
            return False
        previous_size_abs = abs(previous_size)
        current_size_abs = abs(current_size)
        if current_size_abs <= ALLOCATION_TRANSITION_TOLERANCE:
            return False
        previous_leader_notional_abs = _q(current_leader_notional_abs * previous_size_abs / current_size_abs)

    return previous_leader_notional_abs < ALLOCATION_DUST_LIFECYCLE_NOTIONAL


def _allocation_pending_reduce_qty(allocation: Any) -> Decimal:
    value = _decimal_or_none(getattr(allocation, "pending_reduce_qty", None))
    if value is None or value <= 0:
        return Decimal("0")
    return value


def _allocation_pending_reduce_notional(allocation: Any, current_notional: Decimal, current_qty: Decimal) -> Decimal:
    value = _decimal_or_none(getattr(allocation, "pending_reduce_notional", None))
    if value is not None and value > 0:
        return value
    pending_qty = _allocation_pending_reduce_qty(allocation)
    if current_qty <= 0 or pending_qty <= 0:
        return Decimal("0")
    return _q(current_notional * min(Decimal("1"), pending_qty / current_qty))


def _remaining_leader_ratio(*, current_value: Decimal | None, previous_value: Decimal | None) -> Decimal | None:
    if current_value is None or previous_value is None:
        return None
    current_abs = abs(Decimal(str(current_value)))
    previous_abs = abs(Decimal(str(previous_value)))
    if previous_abs <= ALLOCATION_TRANSITION_TOLERANCE:
        return None
    if current_abs >= previous_abs:
        return None
    return max(Decimal("0"), min(Decimal("1"), current_abs / previous_abs))


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {
        "action": getattr(value, "action", None),
        "leader_id": getattr(value, "leader_id", None),
        "execution_venue": getattr(value, "execution_venue", None),
        "dex": getattr(value, "dex", None),
        "canonical_coin": getattr(value, "canonical_coin", None),
        "old_side": getattr(value, "old_side", None),
        "position_side": getattr(value, "position_side", None),
        "side": getattr(value, "side", None),
        "quantity": getattr(value, "quantity", None),
        "close_qty_limit": getattr(value, "close_qty_limit", None),
        "allocation": getattr(value, "allocation", None),
        "current_allocation": getattr(value, "current_allocation", None),
    }


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or str(value) == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None or str(value) == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _upper(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Enum):
        value = value.value
    return str(value or "").upper()


def _lower(value: Any) -> str:
    if isinstance(value, Enum):
        value = value.value
    return str(value or "").lower()


def _venue_value(value: str | ExecutionVenue) -> str:
    return value.value if isinstance(value, ExecutionVenue) else str(value).upper()


def calculate_leader_target_allocation(
    *,
    leader_id: int,
    leader_address: str,
    symbol: str,
    coin: str,
    dex: str = "",
    canonical_coin: str | None = None,
    leader_account_value: Decimal,
    leader_position_notional: Decimal,
    follower_equity: Decimal,
    copy_multiplier: Decimal,
    mark_price: Decimal,
) -> LeaderTargetAllocation:
    target_abs = calculate_target_notional_by_account_ratio(
        leader_account_value=leader_account_value,
        leader_position_notional=leader_position_notional,
        follower_account_value=follower_equity,
        copy_multiplier=copy_multiplier,
    )
    if leader_position_notional > 0:
        side = PositionSide.LONG
        signed = target_abs
    elif leader_position_notional < 0:
        side = PositionSide.SHORT
        signed = -target_abs
    else:
        side = PositionSide.FLAT
        signed = Decimal("0.00000000")
    target_qty = _q(target_abs / mark_price) if mark_price > 0 else Decimal("0.00000000")
    return LeaderTargetAllocation(
        leader_id=leader_id,
        leader_address=leader_address,
        symbol=symbol,
        coin=coin,
        dex=dex,
        canonical_coin=canonical_coin,
        position_side=side,
        target_notional_abs=target_abs,
        target_notional_signed_from_leader_view=signed,
        target_qty=target_qty,
        copy_multiplier=copy_multiplier,
    )


def apply_open_to_allocation(
    current: LeaderPositionAllocation,
    *,
    target_notional_abs: Decimal,
    side: PositionSide,
    mark_price: Decimal,
) -> tuple[HedgeModeOrder, LeaderPositionAllocation]:
    target_qty = _q(target_notional_abs / mark_price)
    delta_qty = target_qty - current.allocated_qty if current.position_side == side else target_qty
    if delta_qty <= 0:
        raise ValueError("open target does not increase allocation")
    order = build_hedge_mode_order(
        symbol=current.binance_symbol or current.venue_symbol or current.hyperliquid_coin,
        allocation_side=side,
        action=HedgeAction.OPEN_OR_INCREASE,
        quantity=delta_qty,
        is_close_intent=False,
    )
    updated = replace(
        current,
        position_side=side,
        target_notional=target_notional_abs,
        allocated_notional=target_notional_abs,
        allocated_qty=target_qty,
        avg_entry_price=mark_price,
        status=AllocationStatus.OPEN,
    )
    return order, updated


def apply_close_to_allocation(
    current: LeaderPositionAllocation,
    *,
    close_qty: Decimal,
    binance_position_qty: Decimal,
) -> tuple[HedgeModeOrder, LeaderPositionAllocation]:
    order = build_hedge_mode_order(
        symbol=current.binance_symbol or current.venue_symbol or current.hyperliquid_coin,
        allocation_side=current.position_side,
        action=HedgeAction.CLOSE_OR_REDUCE,
        quantity=close_qty,
        is_close_intent=True,
        leader_allocation_qty=current.allocated_qty,
        binance_position_qty=binance_position_qty,
    )
    remaining_qty = current.allocated_qty - close_qty
    if remaining_qty <= 0:
        updated = replace(
            current,
            target_notional=Decimal("0"),
            allocated_notional=Decimal("0"),
            allocated_qty=Decimal("0"),
            status=AllocationStatus.CLOSED,
        )
    else:
        remaining_notional = _q(remaining_qty * current.avg_entry_price)
        updated = replace(
            current,
            target_notional=remaining_notional,
            allocated_notional=remaining_notional,
            allocated_qty=remaining_qty,
            status=AllocationStatus.OPEN,
        )
    return order, updated


def apply_execution_fill_to_allocation(
    current: LeaderPositionAllocation,
    *,
    action: str,
    executed_qty: Decimal,
    avg_fill_price: Decimal,
) -> LeaderPositionAllocation:
    if executed_qty <= 0:
        raise ValueError("executed_qty must be positive")
    if avg_fill_price <= 0:
        raise ValueError("avg_fill_price must be positive")

    if action == "CLOSE_OR_REDUCE":
        remaining_qty = max(Decimal("0"), current.allocated_qty - executed_qty)
        if remaining_qty == 0:
            return replace(
                current,
                allocated_qty=Decimal("0"),
                allocated_notional=Decimal("0"),
                target_notional=Decimal("0"),
                status=AllocationStatus.CLOSED,
            )
        remaining_notional = _q(remaining_qty * current.avg_entry_price)
        return replace(
            current,
            allocated_qty=remaining_qty,
            allocated_notional=remaining_notional,
            target_notional=remaining_notional,
            status=AllocationStatus.OPEN,
        )

    if action != "OPEN_OR_INCREASE":
        raise ValueError("action must be OPEN_OR_INCREASE or CLOSE_OR_REDUCE")

    fill_notional = _q(executed_qty * avg_fill_price)
    new_qty = current.allocated_qty + executed_qty
    new_notional = _q(current.allocated_notional + fill_notional)
    avg_entry = _q(new_notional / new_qty)
    return replace(
        current,
        allocated_qty=new_qty,
        allocated_notional=new_notional,
        target_notional=new_notional,
        avg_entry_price=avg_entry,
        status=AllocationStatus.OPEN,
    )


def reconcile_leader_allocation_plan(
    *,
    current: LeaderPositionAllocation,
    target_notional_abs: Decimal,
    target_side: PositionSide,
    mark_price: Decimal,
    binance_position_qty: Decimal,
) -> list[tuple[HedgeModeOrder, LeaderPositionAllocation]]:
    if target_side == PositionSide.FLAT or target_notional_abs == 0:
        if current.allocated_qty <= 0:
            return []
        return [
            apply_close_to_allocation(
                current,
                close_qty=current.allocated_qty,
                binance_position_qty=binance_position_qty,
            )
        ]

    target_qty = _q(target_notional_abs / mark_price)
    if current.status == AllocationStatus.CLOSED or current.allocated_qty == 0:
        return [
            apply_open_to_allocation(
                current,
                target_notional_abs=target_notional_abs,
                side=target_side,
                mark_price=mark_price,
            )
        ]

    if current.position_side != target_side:
        close_step = apply_close_to_allocation(
            current,
            close_qty=current.allocated_qty,
            binance_position_qty=binance_position_qty,
        )
        closed = close_step[1]
        open_base = replace(closed, position_side=target_side)
        open_step = apply_open_to_allocation(
            open_base,
            target_notional_abs=target_notional_abs,
            side=target_side,
            mark_price=mark_price,
        )
        return [close_step, open_step]

    if target_qty > current.allocated_qty:
        return [
            apply_open_to_allocation(
                current,
                target_notional_abs=target_notional_abs,
                side=target_side,
                mark_price=mark_price,
            )
        ]
    if target_qty < current.allocated_qty:
        return [
            apply_close_to_allocation(
                current,
                close_qty=current.allocated_qty - target_qty,
                binance_position_qty=binance_position_qty,
            )
        ]
    return []


def validate_aggregate_allocations_vs_binance(
    allocations: list[LeaderPositionAllocation],
    binance_position: AggregatePosition,
    *,
    tolerance: Decimal,
    source: str = "AUTO",
) -> AllocationValidationResult:
    active = [
        a
        for a in allocations
        if a.status != AllocationStatus.CLOSED
        and a.execution_venue == ExecutionVenue.BINANCE
        and (a.binance_symbol or "").upper() == binance_position.symbol.upper()
    ]
    long_qty = sum(
        (a.allocated_qty for a in active if a.position_side == PositionSide.LONG),
        Decimal("0"),
    )
    short_qty = sum(
        (a.allocated_qty for a in active if a.position_side == PositionSide.SHORT),
        Decimal("0"),
    )
    long_diff = abs(long_qty - binance_position.long_qty)
    short_diff = abs(short_qty - binance_position.short_qty)
    ok = long_diff <= tolerance and short_diff <= tolerance
    return AllocationValidationResult(
        ok=ok,
        symbol=binance_position.symbol,
        long_allocated_qty=long_qty,
        short_allocated_qty=short_qty,
        binance_long_qty=binance_position.long_qty,
        binance_short_qty=binance_position.short_qty,
        tolerance=tolerance,
        source=source,
        event_type="OK" if ok else "ALLOCATION_MISMATCH",
        reason=None
        if ok
        else f"allocation mismatch long_diff={long_diff} short_diff={short_diff}",
    )


def can_open_new_allocation(result: AllocationValidationResult) -> bool:
    return result.ok


def validate_aggregate_allocations_vs_venue(
    allocations: list[LeaderPositionAllocation],
    *,
    venue: ExecutionVenue,
    venue_symbol: str,
    venue_account: str | None,
    long_qty: Decimal,
    short_qty: Decimal,
    tolerance: Decimal,
    source: str = "AUTO",
) -> AllocationValidationResult:
    active = [
        a
        for a in allocations
        if a.status != AllocationStatus.CLOSED
        and a.execution_venue == venue
        and (a.venue_symbol or a.binance_symbol or a.hyperliquid_coin).upper() == venue_symbol.upper()
        and (venue_account is None or a.venue_account == venue_account)
    ]
    allocated_long = sum(
        (a.allocated_qty for a in active if a.position_side == PositionSide.LONG),
        Decimal("0"),
    )
    allocated_short = sum(
        (a.allocated_qty for a in active if a.position_side == PositionSide.SHORT),
        Decimal("0"),
    )
    long_diff = abs(allocated_long - long_qty)
    short_diff = abs(allocated_short - short_qty)
    ok = long_diff <= tolerance and short_diff <= tolerance
    return AllocationValidationResult(
        ok=ok,
        symbol=venue_symbol,
        long_allocated_qty=allocated_long,
        short_allocated_qty=allocated_short,
        binance_long_qty=long_qty,
        binance_short_qty=short_qty,
        tolerance=tolerance,
        source=source,
        event_type="OK" if ok else "ALLOCATION_MISMATCH",
        reason=None
        if ok
        else f"allocation mismatch long_diff={long_diff} short_diff={short_diff}",
    )
