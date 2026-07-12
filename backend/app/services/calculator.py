from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Iterable

SIZING_MODE_ACCOUNT_RATIO = "ACCOUNT_RATIO"


class CopyAction(str, Enum):
    OPEN = "open"
    ADD = "add"
    REDUCE = "reduce"
    CLOSE = "close"
    FLIP = "flip"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class PositionOrder:
    action: CopyAction
    side: OrderSide
    notional: Decimal
    reduce_only: bool


class UnsupportedSizingMode(ValueError):
    pass


def to_decimal(value: Decimal | int | float | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def calculate_leader_position_ratio(
    *,
    leader_account_value: Decimal,
    leader_position_notional: Decimal,
) -> Decimal:
    if not isinstance(leader_account_value, Decimal) or not isinstance(leader_position_notional, Decimal):
        raise TypeError("calculate_leader_position_ratio requires Decimal inputs")
    if leader_account_value <= 0:
        raise ValueError("leader_account_value must be positive")
    return (abs(leader_position_notional) / leader_account_value).quantize(
        Decimal("0.00000008"), rounding=ROUND_HALF_UP
    )


def calculate_target_notional_by_account_ratio(
    *,
    leader_account_value: Decimal,
    leader_position_notional: Decimal,
    follower_account_value: Decimal,
    copy_multiplier: Decimal,
) -> Decimal:
    """Map the leader account-risk ratio onto the follower account.

    Formula:
        follower_account_value * abs(leader_position_notional / leader_account_value) * copy_multiplier
    """

    if not all(
        isinstance(value, Decimal)
        for value in (
            leader_account_value,
            leader_position_notional,
            follower_account_value,
            copy_multiplier,
        )
    ):
        raise TypeError("calculate_target_notional_by_account_ratio requires Decimal inputs")
    if leader_account_value <= 0:
        raise ValueError("leader_account_value must be positive")
    if follower_account_value <= 0:
        raise ValueError("follower_account_value must be positive")
    if copy_multiplier <= 0:
        raise ValueError("copy_multiplier must be positive")
    if leader_position_notional == 0:
        return Decimal("0.00000000")

    leader_position_ratio = abs(leader_position_notional) / leader_account_value
    return (follower_account_value * leader_position_ratio * copy_multiplier).quantize(
        Decimal("0.00000001"), rounding=ROUND_HALF_UP
    )


def calculate_copy_notional(
    leader_notional_delta: Decimal | int | float | str,
    leader_account_value: Decimal | int | float | str,
    follower_equity: Decimal | int | float | str,
    copy_multiplier: Decimal | int | float | str,
) -> Decimal:
    raise UnsupportedSizingMode("leader_notional_delta sizing is forbidden. Use ACCOUNT_RATIO target sizing.")


def calculate_target_position_notional(
    leader_account_value: Decimal,
    leader_position_notional: Decimal,
    follower_equity: Decimal,
    copy_multiplier: Decimal,
) -> Decimal:
    """Backward-compatible name for ACCOUNT_RATIO target sizing."""

    return calculate_target_notional_by_account_ratio(
        leader_account_value=leader_account_value,
        leader_position_notional=leader_position_notional,
        follower_account_value=follower_equity,
        copy_multiplier=copy_multiplier,
    )


def notional_from_fill(
    price: Decimal | int | float | str, size: Decimal | int | float | str
) -> Decimal:
    return (abs(to_decimal(price)) * abs(to_decimal(size))).quantize(
        Decimal("0.00000001"), rounding=ROUND_HALF_UP
    )


def _same_direction(old_qty: Decimal, new_qty: Decimal) -> bool:
    return old_qty == 0 or new_qty == 0 or (old_qty > 0 and new_qty > 0) or (
        old_qty < 0 and new_qty < 0
    )


def _increase_side(qty: Decimal) -> OrderSide:
    return OrderSide.BUY if qty > 0 else OrderSide.SELL


def _decrease_side(qty: Decimal) -> OrderSide:
    return OrderSide.SELL if qty > 0 else OrderSide.BUY


def position_delta_orders(
    old_signed_qty: Decimal | int | float | str,
    new_signed_qty: Decimal | int | float | str,
    mark_price: Decimal | int | float | str,
) -> list[PositionOrder]:
    """Convert a signed position change into copyable order intents.

    Positive qty means long, negative qty means short. Returned notionals are
    unscaled leader-side notionals; callers should pass each notional through
    calculate_copy_notional.
    """

    old_qty = to_decimal(old_signed_qty)
    new_qty = to_decimal(new_signed_qty)
    price = abs(to_decimal(mark_price))
    if price <= 0 or old_qty == new_qty:
        return []

    old_abs = abs(old_qty)
    new_abs = abs(new_qty)

    if old_qty == 0:
        return [
            PositionOrder(
                action=CopyAction.OPEN,
                side=_increase_side(new_qty),
                notional=(new_abs * price),
                reduce_only=False,
            )
        ]

    if new_qty == 0:
        return [
            PositionOrder(
                action=CopyAction.CLOSE,
                side=_decrease_side(old_qty),
                notional=(old_abs * price),
                reduce_only=True,
            )
        ]

    if _same_direction(old_qty, new_qty):
        if new_abs > old_abs:
            return [
                PositionOrder(
                    action=CopyAction.ADD,
                    side=_increase_side(new_qty),
                    notional=((new_abs - old_abs) * price),
                    reduce_only=False,
                )
            ]
        return [
            PositionOrder(
                action=CopyAction.REDUCE,
                side=_decrease_side(old_qty),
                notional=((old_abs - new_abs) * price),
                reduce_only=True,
            )
        ]

    return [
        PositionOrder(
            action=CopyAction.FLIP,
            side=_decrease_side(old_qty),
            notional=(old_abs * price),
            reduce_only=True,
        ),
        PositionOrder(
            action=CopyAction.FLIP,
            side=_increase_side(new_qty),
            notional=(new_abs * price),
            reduce_only=False,
        ),
    ]


def scale_position_orders(
    orders: Iterable[PositionOrder],
    leader_account_value: Decimal | int | float | str,
    follower_equity: Decimal | int | float | str,
    copy_multiplier: Decimal | int | float | str,
) -> list[PositionOrder]:
    raise UnsupportedSizingMode("Unsupported sizing mode. Use ACCOUNT_RATIO.")
