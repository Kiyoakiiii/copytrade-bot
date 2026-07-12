from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from app.services.calculator import OrderSide, calculate_target_notional_by_account_ratio


class PositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


class TargetAction(str, Enum):
    OPEN = "OPEN"
    ADD = "ADD"
    REDUCE = "REDUCE"
    CLOSE = "CLOSE"
    FLIP_CLOSE = "FLIP_CLOSE"
    FLIP_OPEN = "FLIP_OPEN"


@dataclass(frozen=True)
class TargetPositionInput:
    leader_address: str
    coin: str
    binance_symbol: str
    leader_account_value: Decimal
    leader_position_notional: Decimal
    follower_equity: Decimal
    follower_current_notional: Decimal
    copy_multiplier: Decimal
    tolerance_bps: int = 50
    min_reconcile_notional: Decimal = Decimal("10")


@dataclass(frozen=True)
class TargetPositionInstruction:
    leader_address: str
    coin: str
    binance_symbol: str
    action: TargetAction
    side: OrderSide
    target_notional: Decimal
    delta_notional: Decimal
    order_notional: Decimal
    reduce_only: bool
    requires_position_flat: bool = False
    reason: str = "outside tolerance"


def signed_side(value: Decimal) -> PositionSide:
    if value > 0:
        return PositionSide.LONG
    if value < 0:
        return PositionSide.SHORT
    return PositionSide.FLAT


def side_for_delta(delta_notional: Decimal) -> OrderSide:
    return OrderSide.BUY if delta_notional > 0 else OrderSide.SELL


def close_side(current_notional: Decimal) -> OrderSide:
    return OrderSide.SELL if current_notional > 0 else OrderSide.BUY


def signed_target_notional(item: TargetPositionInput) -> Decimal:
    target_abs = calculate_target_notional_by_account_ratio(
        leader_account_value=item.leader_account_value,
        leader_position_notional=item.leader_position_notional,
        follower_account_value=item.follower_equity,
        copy_multiplier=item.copy_multiplier,
    )
    if item.leader_position_notional < 0:
        return -target_abs
    return target_abs


def delta_is_actionable(
    *,
    delta_abs: Decimal,
    target_abs: Decimal,
    min_reconcile_notional: Decimal,
    tolerance_bps: int,
) -> bool:
    if delta_abs < min_reconcile_notional:
        return False
    tolerance = max(target_abs, Decimal("1")) * Decimal(tolerance_bps) / Decimal("10000")
    return delta_abs > tolerance


def build_target_position_plan(
    item: TargetPositionInput,
) -> list[TargetPositionInstruction]:
    target = signed_target_notional(item)
    current = item.follower_current_notional
    leader_side = signed_side(item.leader_position_notional)
    follower_side = signed_side(current)

    if leader_side == PositionSide.FLAT:
        if follower_side == PositionSide.FLAT:
            return []
        return [
            TargetPositionInstruction(
                leader_address=item.leader_address,
                coin=item.coin,
                binance_symbol=item.binance_symbol,
                action=TargetAction.CLOSE,
                side=close_side(current),
                target_notional=Decimal("0.00000000"),
                delta_notional=-current,
                order_notional=abs(current),
                reduce_only=True,
                reason="leader flat",
            )
        ]

    delta = target - current
    if follower_side == PositionSide.FLAT:
        if not delta_is_actionable(
            delta_abs=abs(target),
            target_abs=abs(target),
            min_reconcile_notional=item.min_reconcile_notional,
            tolerance_bps=item.tolerance_bps,
        ):
            return []
        return [
            TargetPositionInstruction(
                leader_address=item.leader_address,
                coin=item.coin,
                binance_symbol=item.binance_symbol,
                action=TargetAction.OPEN,
                side=side_for_delta(target),
                target_notional=target,
                delta_notional=target,
                order_notional=abs(target),
                reduce_only=False,
            )
        ]

    if follower_side != leader_side:
        if not delta_is_actionable(
            delta_abs=abs(delta),
            target_abs=abs(target),
            min_reconcile_notional=item.min_reconcile_notional,
            tolerance_bps=item.tolerance_bps,
        ):
            return []
        return [
            TargetPositionInstruction(
                leader_address=item.leader_address,
                coin=item.coin,
                binance_symbol=item.binance_symbol,
                action=TargetAction.FLIP_CLOSE,
                side=close_side(current),
                target_notional=target,
                delta_notional=-current,
                order_notional=abs(current),
                reduce_only=True,
                reason="opposite direction close first",
            ),
            TargetPositionInstruction(
                leader_address=item.leader_address,
                coin=item.coin,
                binance_symbol=item.binance_symbol,
                action=TargetAction.FLIP_OPEN,
                side=side_for_delta(target),
                target_notional=target,
                delta_notional=target,
                order_notional=abs(target),
                reduce_only=False,
                requires_position_flat=True,
                reason="open after flat confirmation",
            ),
        ]

    if not delta_is_actionable(
        delta_abs=abs(delta),
        target_abs=abs(target),
        min_reconcile_notional=item.min_reconcile_notional,
        tolerance_bps=item.tolerance_bps,
    ):
        return []

    is_increase = abs(target) > abs(current)
    return [
        TargetPositionInstruction(
            leader_address=item.leader_address,
            coin=item.coin,
            binance_symbol=item.binance_symbol,
            action=TargetAction.ADD if is_increase else TargetAction.REDUCE,
            side=side_for_delta(delta),
            target_notional=target,
            delta_notional=delta,
            order_notional=abs(delta),
            reduce_only=not is_increase,
        )
    ]
