from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

from app.services.target_position import PositionSide


class HedgeAction(str, Enum):
    OPEN_OR_INCREASE = "OPEN_OR_INCREASE"
    CLOSE_OR_REDUCE = "CLOSE_OR_REDUCE"


@dataclass(frozen=True)
class HedgeModeOrder:
    symbol: str
    side: str
    position_side: str
    quantity: Decimal
    is_close_intent: bool

    @property
    def params(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "positionSide": self.position_side,
            "quantity": self.quantity,
        }


def build_hedge_mode_order(
    *,
    symbol: str,
    allocation_side: PositionSide,
    action: HedgeAction,
    quantity: Decimal,
    is_close_intent: bool,
    leader_allocation_qty: Decimal | None = None,
    binance_position_qty: Decimal | None = None,
) -> HedgeModeOrder:
    if allocation_side not in {PositionSide.LONG, PositionSide.SHORT}:
        raise ValueError("Hedge Mode orders require positionSide LONG or SHORT")
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if action == HedgeAction.CLOSE_OR_REDUCE and not is_close_intent:
        raise ValueError("close/reduce orders must be marked close intent")
    if action == HedgeAction.OPEN_OR_INCREASE and is_close_intent:
        raise ValueError("open/increase orders cannot be close intent")

    if action == HedgeAction.CLOSE_OR_REDUCE:
        if leader_allocation_qty is None or binance_position_qty is None:
            raise ValueError("close intent requires allocation and Binance side quantities")
        if quantity > leader_allocation_qty:
            raise ValueError("close quantity exceeds leader allocation")
        if quantity > binance_position_qty:
            raise ValueError("close quantity exceeds Binance positionSide quantity")

    if allocation_side == PositionSide.LONG:
        side = "BUY" if action == HedgeAction.OPEN_OR_INCREASE else "SELL"
    else:
        side = "SELL" if action == HedgeAction.OPEN_OR_INCREASE else "BUY"

    return HedgeModeOrder(
        symbol=symbol,
        side=side,
        position_side=allocation_side.value,
        quantity=quantity,
        is_close_intent=is_close_intent,
    )

