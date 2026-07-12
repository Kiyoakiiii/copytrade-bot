from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.services.calculator import SIZING_MODE_ACCOUNT_RATIO, calculate_target_notional_by_account_ratio


class SizingGuardError(ValueError):
    pass


def assert_sizing_mode_account_ratio(order_plan: Any) -> None:
    fields = _payload(order_plan)
    required = [
        "sizing_mode",
        "target_notional",
        "delta_notional",
    ]
    missing = [key for key in required if fields.get(key) is None or str(fields.get(key)) == ""]
    if missing:
        raise SizingGuardError(f"ACCOUNT_RATIO sizing metadata missing: {', '.join(missing)}")
    if fields["sizing_mode"] != SIZING_MODE_ACCOUNT_RATIO:
        raise SizingGuardError("sizing_mode must be ACCOUNT_RATIO")
    if fields.get("increase_delta_source"):
        _assert_incremental_increase_account_ratio(fields)
        return
    account_ratio_required = [
        "leader_account_value",
        "leader_account_value_source",
        "leader_position_notional",
        "follower_account_value",
        "follower_account_value_source",
        "leader_position_ratio",
        "copy_multiplier",
    ]
    missing = [key for key in account_ratio_required if fields.get(key) is None or str(fields.get(key)) == ""]
    if missing:
        raise SizingGuardError(f"ACCOUNT_RATIO sizing metadata missing: {', '.join(missing)}")
    expected_account_ratio = calculate_target_notional_by_account_ratio(
        leader_account_value=_decimal(fields["leader_account_value"], "leader_account_value"),
        leader_position_notional=_decimal(fields["leader_position_notional"], "leader_position_notional"),
        follower_account_value=_decimal(fields["follower_account_value"], "follower_account_value"),
        copy_multiplier=_decimal(fields["copy_multiplier"], "copy_multiplier"),
    )
    cap = _optional_decimal(fields.get("max_position_notional_cap"), "max_position_notional_cap")
    before_cap = _optional_decimal(fields.get("target_notional_before_cap"), "target_notional_before_cap")
    if before_cap is not None and abs(before_cap - expected_account_ratio) > Decimal("0.00000002"):
        raise SizingGuardError(
            f"ACCOUNT_RATIO pre-cap target mismatch: expected {expected_account_ratio}, got {before_cap}"
        )
    expected = min(expected_account_ratio, cap) if cap is not None and cap > 0 else expected_account_ratio
    actual = _decimal(fields["target_notional"], "target_notional")
    if abs(expected - actual) > Decimal("0.00000002"):
        raise SizingGuardError(f"ACCOUNT_RATIO target mismatch: expected {expected}, got {actual}")


def _assert_incremental_increase_account_ratio(fields: dict[str, Any]) -> None:
    fill_delta = _optional_decimal(fields.get("fill_delta_target_notional"), "fill_delta_target_notional")
    if fill_delta is None:
        raise SizingGuardError("ACCOUNT_RATIO incremental increase metadata missing: fill_delta_target_notional")
    source = str(fields.get("increase_delta_source") or "")
    if source == "leader_fill_notional":
        leader_fill_notional = _optional_decimal(fields.get("leader_fill_notional"), "leader_fill_notional")
        if leader_fill_notional is None:
            raise SizingGuardError("ACCOUNT_RATIO incremental increase metadata missing: leader_fill_notional")
        expected_delta_before_offset = calculate_target_notional_by_account_ratio(
            leader_account_value=_decimal(fields["leader_account_value"], "leader_account_value"),
            leader_position_notional=leader_fill_notional,
            follower_account_value=_decimal(fields["follower_account_value"], "follower_account_value"),
            copy_multiplier=_decimal(fields["copy_multiplier"], "copy_multiplier"),
        )
        pending_reduce_offset = _optional_decimal(
            fields.get("pending_reduce_offset_notional"),
            "pending_reduce_offset_notional",
        )
        if pending_reduce_offset is not None and pending_reduce_offset < 0:
            raise SizingGuardError("pending_reduce_offset_notional must be non-negative")
        expected_delta = (
            max(Decimal("0"), expected_delta_before_offset - pending_reduce_offset)
            if pending_reduce_offset is not None
            else expected_delta_before_offset
        )
        if abs(expected_delta - fill_delta) > Decimal("0.00000002"):
            raise SizingGuardError(
                f"ACCOUNT_RATIO fill-delta target mismatch: expected {expected_delta}, got {fill_delta}"
            )
    elif source in {"leader_position_size", "leader_fill_start_position_size", "leader_position_notional"}:
        current = _optional_decimal(fields.get("current_allocation_notional"), "current_allocation_notional")
        if current is None:
            raise SizingGuardError("ACCOUNT_RATIO incremental increase metadata missing: current_allocation_notional")
        increase_ratio = _position_increase_ratio(fields, source)
        expected_delta_before_offset = current * increase_ratio
        pending_reduce_offset = _optional_decimal(
            fields.get("pending_reduce_offset_notional"),
            "pending_reduce_offset_notional",
        )
        if pending_reduce_offset is not None and pending_reduce_offset < 0:
            raise SizingGuardError("pending_reduce_offset_notional must be non-negative")
        expected_delta = (
            max(Decimal("0"), expected_delta_before_offset - pending_reduce_offset)
            if pending_reduce_offset is not None
            else expected_delta_before_offset
        )
        if abs(expected_delta - fill_delta) > Decimal("0.00000002"):
            raise SizingGuardError(
                f"ACCOUNT_RATIO position-ratio increase target mismatch: expected {expected_delta}, got {fill_delta}"
            )
    else:
        raise SizingGuardError(f"unsupported incremental increase source: {source}")

    actual = _decimal(fields["target_notional"], "target_notional")
    delta = _decimal(fields["delta_notional"], "delta_notional")
    current = _optional_decimal(fields.get("current_allocation_notional"), "current_allocation_notional")
    if current is None:
        current = actual - delta
    expected_before_cap = current + fill_delta
    before_cap = _optional_decimal(fields.get("target_notional_before_cap"), "target_notional_before_cap")
    if before_cap is not None and abs(before_cap - expected_before_cap) > Decimal("0.00000002"):
        raise SizingGuardError(
            f"ACCOUNT_RATIO incremental pre-cap target mismatch: expected {expected_before_cap}, got {before_cap}"
        )
    cap = _optional_decimal(fields.get("max_position_notional_cap"), "max_position_notional_cap")
    expected = min(expected_before_cap, cap) if cap is not None and cap > 0 else expected_before_cap
    if abs(expected - actual) > Decimal("0.00000002"):
        raise SizingGuardError(f"ACCOUNT_RATIO incremental target mismatch: expected {expected}, got {actual}")


def prohibit_leader_notional_multiplier(*_: Any, **__: Any) -> Decimal:
    raise SizingGuardError("leader_notional * multiplier sizing is forbidden. Use ACCOUNT_RATIO.")


def prohibit_fill_size_multiplier(*_: Any, **__: Any) -> Decimal:
    raise SizingGuardError("fill_size * multiplier sizing is forbidden. Use ACCOUNT_RATIO.")


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    formula_inputs = _latency_trace_formula_inputs(getattr(value, "latency_trace", None))
    return {
        "sizing_mode": getattr(value, "sizing_mode", None),
        "leader_account_value": getattr(value, "leader_account_value", None),
        "leader_account_value_source": getattr(value, "leader_account_value_source", None),
        "leader_position_notional": getattr(value, "leader_position_notional", None),
        "follower_account_value": getattr(value, "follower_account_value", None),
        "follower_account_value_source": getattr(value, "follower_account_value_source", None),
        "leader_position_ratio": getattr(value, "leader_position_ratio", None),
        "copy_multiplier": getattr(value, "copy_multiplier", None),
        "target_notional": getattr(value, "target_notional", None),
        "delta_notional": getattr(value, "delta_notional", None),
        "current_allocation_notional": getattr(value, "current_allocation_notional", None)
        or formula_inputs.get("current_allocation_notional"),
        "leader_fill_notional": getattr(value, "leader_fill_notional", None)
        or formula_inputs.get("leader_fill_notional"),
        "leader_position_size": getattr(value, "leader_position_size", None)
        or formula_inputs.get("leader_position_size"),
        "previous_leader_position_size": getattr(value, "previous_leader_position_size", None)
        or formula_inputs.get("previous_leader_position_size"),
        "leader_fill_previous_position_size": getattr(value, "leader_fill_previous_position_size", None)
        or formula_inputs.get("leader_fill_previous_position_size"),
        "previous_leader_position_notional": getattr(value, "previous_leader_position_notional", None)
        or formula_inputs.get("previous_leader_position_notional"),
        "leader_position_increase_ratio": getattr(value, "leader_position_increase_ratio", None)
        or formula_inputs.get("leader_position_increase_ratio"),
        "increase_delta_source": getattr(value, "increase_delta_source", None)
        or formula_inputs.get("increase_delta_source"),
        "fill_delta_target_notional": getattr(value, "fill_delta_target_notional", None)
        or formula_inputs.get("fill_delta_target_notional"),
        "pending_reduce_offset_notional": getattr(value, "pending_reduce_offset_notional", None)
        or formula_inputs.get("pending_reduce_offset_notional"),
        "max_position_notional_cap": getattr(value, "max_position_notional_cap", None)
        or formula_inputs.get("max_position_notional_cap"),
        "target_notional_before_cap": getattr(value, "target_notional_before_cap", None)
        or formula_inputs.get("target_notional_before_cap"),
    }


def _decimal(value: Any, name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise SizingGuardError(f"{name} must be Decimal-compatible") from exc


def _optional_decimal(value: Any, name: str) -> Decimal | None:
    if value is None or str(value) == "":
        return None
    return _decimal(value, name)


def _position_increase_ratio(fields: dict[str, Any], source: str) -> Decimal:
    provided = _optional_decimal(fields.get("leader_position_increase_ratio"), "leader_position_increase_ratio")
    if provided is not None:
        if provided < 0:
            raise SizingGuardError("leader_position_increase_ratio must be non-negative")
        return provided
    if source == "leader_position_notional":
        previous = _optional_decimal(fields.get("previous_leader_position_notional"), "previous_leader_position_notional")
        current = _optional_decimal(fields.get("leader_position_notional"), "leader_position_notional")
    else:
        previous_key = (
            "leader_fill_previous_position_size"
            if source == "leader_fill_start_position_size"
            else "previous_leader_position_size"
        )
        previous = _optional_decimal(fields.get(previous_key), previous_key)
        current = _optional_decimal(fields.get("leader_position_size"), "leader_position_size")
    if previous is None or current is None:
        raise SizingGuardError("ACCOUNT_RATIO incremental increase metadata missing: leader position ratio inputs")
    previous_abs = abs(previous)
    current_abs = abs(current)
    if previous_abs <= Decimal("0.00000001"):
        raise SizingGuardError("previous leader position size/notional must be positive for increase ratio")
    if current_abs <= previous_abs:
        raise SizingGuardError("current leader position size/notional must exceed previous for increase ratio")
    return (current_abs - previous_abs) / previous_abs


def _latency_trace_formula_inputs(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    details = value.get("details")
    if not isinstance(details, dict):
        return {}
    formula_inputs = details.get("formula_inputs")
    return formula_inputs if isinstance(formula_inputs, dict) else {}
