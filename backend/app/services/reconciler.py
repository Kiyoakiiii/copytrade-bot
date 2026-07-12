from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from app.services.calculator import OrderSide
from app.services.target_position import TargetPositionInput, build_target_position_plan


@dataclass(frozen=True)
class ReconcileInput:
    leader_address: str
    coin: str
    binance_symbol: str
    leader_position_notional: Decimal
    leader_account_value: Decimal
    follower_equity: Decimal
    follower_current_notional: Decimal
    copy_multiplier: Decimal
    tolerance_bps: int = 50
    min_reconcile_notional: Decimal = Decimal("10")
    last_execution_at: datetime | None = None
    cooldown_seconds: int = 0


@dataclass(frozen=True)
class ReconcileInstruction:
    leader_address: str
    coin: str
    binance_symbol: str
    target_notional: Decimal
    delta_notional: Decimal
    side: OrderSide
    reduce_only: bool
    reason: str


def compute_reconcile_instruction(
    item: ReconcileInput, *, now: datetime | None = None
) -> ReconcileInstruction | None:
    now = now or datetime.now(timezone.utc)
    if item.last_execution_at and item.cooldown_seconds > 0:
        if (now - item.last_execution_at).total_seconds() < item.cooldown_seconds:
            return None

    plan = build_target_position_plan(
        TargetPositionInput(
            leader_address=item.leader_address,
            coin=item.coin,
            binance_symbol=item.binance_symbol,
            leader_position_notional=item.leader_position_notional,
            leader_account_value=item.leader_account_value,
            follower_equity=item.follower_equity,
            follower_current_notional=item.follower_current_notional,
            copy_multiplier=item.copy_multiplier,
            tolerance_bps=item.tolerance_bps,
            min_reconcile_notional=item.min_reconcile_notional,
        )
    )
    if not plan:
        return None
    first = plan[0]
    return ReconcileInstruction(
        leader_address=item.leader_address,
        coin=item.coin,
        binance_symbol=item.binance_symbol,
        target_notional=first.target_notional,
        delta_notional=first.delta_notional,
        side=first.side,
        reduce_only=first.reduce_only,
        reason=first.reason,
    )
