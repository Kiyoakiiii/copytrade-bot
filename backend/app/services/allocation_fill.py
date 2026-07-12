from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from app.models import ExecutionOrder, LeaderPositionAllocationRecord


ALLOCATION_REDUCE_ORDER_ACTIONS = {"CLOSE_OR_REDUCE", "REDUCE", "CLOSE", "FLIP_CLOSE_FIRST"}
ALLOCATION_INCREASE_ORDER_ACTIONS = {"OPEN_OR_INCREASE", "OPEN", "INCREASE", "FLIP_OPEN_SECOND"}


@dataclass(frozen=True)
class AllocationFillApplyResult:
    before_notional: Decimal
    after_notional: Decimal
    before_qty: Decimal
    after_qty: Decimal
    applied_at: datetime


def apply_filled_order_to_allocation_state(
    allocation: LeaderPositionAllocationRecord,
    order: ExecutionOrder,
    *,
    fill_qty: Decimal,
    update_target_notional: bool,
    clear_deferred_reduce: bool,
) -> AllocationFillApplyResult | None:
    fill_qty = Decimal(fill_qty or 0)
    if fill_qty <= 0:
        return None
    before_notional = Decimal(allocation.allocated_notional or 0)
    before_qty = Decimal(allocation.allocated_qty or 0)
    avg_price = Decimal(order.avg_fill_price or allocation.avg_entry_price or order.estimated_price or 0)
    if avg_price <= 0:
        allocation.status = "BLOCKED"
        return None
    order_action = str(order.order_action or "").upper()
    if order_action in ALLOCATION_REDUCE_ORDER_ACTIONS:
        remaining_qty = max(Decimal("0"), Decimal(allocation.allocated_qty or 0) - fill_qty)
        allocation.allocated_qty = remaining_qty
        allocation.allocated_notional = _q(remaining_qty * Decimal(allocation.avg_entry_price or avg_price))
        if update_target_notional:
            allocation.target_notional = allocation.allocated_notional
        allocation.status = "CLOSED" if remaining_qty <= Decimal("0.00000001") else "OPEN"
    elif order_action in ALLOCATION_INCREASE_ORDER_ACTIONS:
        fill_notional = _q(fill_qty * avg_price)
        current_qty = Decimal(allocation.allocated_qty or 0)
        current_notional = Decimal(allocation.allocated_notional or 0)
        new_qty = current_qty + fill_qty
        new_notional = current_notional + fill_notional
        allocation.allocated_qty = new_qty
        allocation.allocated_notional = new_notional
        if update_target_notional:
            allocation.target_notional = new_notional
        allocation.avg_entry_price = _q(new_notional / new_qty) if new_qty > 0 else avg_price
        allocation.status = "OPEN"
    else:
        allocation.status = "BLOCKED"
        return None
    if clear_deferred_reduce:
        clear_deferred_reduce_state(allocation)
    applied_at = datetime.now(timezone.utc)
    allocation.last_reconcile_at = applied_at
    return AllocationFillApplyResult(
        before_notional=before_notional,
        after_notional=Decimal(allocation.allocated_notional or 0),
        before_qty=before_qty,
        after_qty=Decimal(allocation.allocated_qty or 0),
        applied_at=applied_at,
    )


def clear_deferred_reduce_state(allocation: LeaderPositionAllocationRecord) -> None:
    allocation.pending_reduce_qty = None
    allocation.pending_reduce_notional = None
    allocation.pending_reduce_reason = None
    allocation.pending_reduce_since = None
    allocation.pending_reduce_source_fill_id = None


def _q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
