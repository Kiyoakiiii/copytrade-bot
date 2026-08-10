from __future__ import annotations

from typing import Any

from sqlalchemy import func, select, tuple_

from app.models import ExecutionOrder, LeaderConfig
from app.services.execution_router import ExecutionVenue
from app.services.leader_config import normalize_leader_address


CopyOrderKey = tuple[str, str, str]


async def latest_copy_orders_by_market(
    db: Any,
    leaders: list[LeaderConfig],
    *,
    limit: int = 500,
) -> dict[CopyOrderKey, ExecutionOrder]:
    leader_addresses = [normalize_leader_address(leader.leader_address) for leader in leaders]
    if not leader_addresses:
        return {}
    rows = (
        await db.execute(
            select(ExecutionOrder)
            .where(ExecutionOrder.source_type == "AUTO_COPY")
            .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(ExecutionOrder.leader_address.in_(leader_addresses))
            .order_by(ExecutionOrder.created_at.desc(), ExecutionOrder.id.desc())
            .limit(limit)
        )
    ).scalars().all()
    latest: dict[CopyOrderKey, ExecutionOrder] = {}
    for row in rows:
        key = copy_order_key(
            leader_address=row.leader_address,
            dex=row.dex,
            canonical_coin=row.canonical_coin or row.venue_symbol or row.source_coin,
        )
        if key not in latest:
            latest[key] = row
    return latest


async def latest_copy_orders_for_markets(
    db: Any,
    market_keys: set[CopyOrderKey],
) -> dict[CopyOrderKey, ExecutionOrder]:
    """Load exactly one latest order for each visible leader/market scope.

    The overview only renders currently open leader positions. Loading the
    latest 500 complete order rows (including trace JSON) on every refresh was
    both unnecessary and expensive. The window query keeps the same newest
    ``created_at, id`` semantics while returning only rows the page can show.
    """

    normalized_keys = {
        copy_order_key(
            leader_address=leader_address,
            dex=dex,
            canonical_coin=canonical_coin,
        )
        for leader_address, dex, canonical_coin in market_keys
        if leader_address and canonical_coin
    }
    if not normalized_keys:
        return {}

    leader_expr = func.lower(ExecutionOrder.leader_address)
    dex_expr = func.lower(func.coalesce(ExecutionOrder.dex, ""))
    coin_expr = func.upper(
        func.coalesce(
            ExecutionOrder.canonical_coin,
            ExecutionOrder.venue_symbol,
            ExecutionOrder.source_coin,
            "",
        )
    )
    ranked = (
        select(
            ExecutionOrder.id.label("order_id"),
            func.row_number()
            .over(
                partition_by=(leader_expr, dex_expr, coin_expr),
                order_by=(
                    ExecutionOrder.created_at.desc(),
                    ExecutionOrder.id.desc(),
                ),
            )
            .label("scope_rank"),
        )
        .where(ExecutionOrder.source_type == "AUTO_COPY")
        .where(
            ExecutionOrder.execution_venue
            == ExecutionVenue.HYPERLIQUID.value
        )
        .where(
            tuple_(leader_expr, dex_expr, coin_expr).in_(
                sorted(normalized_keys)
            )
        )
        .subquery()
    )
    rows = (
        await db.execute(
            select(ExecutionOrder)
            .join(ranked, ranked.c.order_id == ExecutionOrder.id)
            .where(ranked.c.scope_rank == 1)
        )
    ).scalars().all()
    return {
        copy_order_key(
            leader_address=row.leader_address,
            dex=row.dex,
            canonical_coin=(
                row.canonical_coin or row.venue_symbol or row.source_coin
            ),
        ): row
        for row in rows
    }


def copy_order_key(*, leader_address: str | None, dex: str | None, canonical_coin: str | None) -> CopyOrderKey:
    return (
        normalize_leader_address(leader_address),
        str(dex or "").lower(),
        str(canonical_coin or "").upper(),
    )


def copy_order_status_payload(order: ExecutionOrder | None) -> dict[str, Any]:
    if order is None:
        return {}
    checklist = order.pre_trade_checklist or {}
    validator = checklist.get("order_validator") or {}
    error_code = checklist.get("error_code") or _validator_error_code(validator)
    display_status = _display_status(order, error_code=error_code, validator=validator)
    return {
        "last_copy_order_id": order.id,
        "last_copy_order_status": order.status,
        "last_copy_order_display_status": display_status,
        "last_copy_order_action": order.order_action,
        "last_copy_order_error_code": error_code,
        "last_copy_order_reason": order.error_message,
        "last_copy_order_created_at": order.created_at.isoformat() if order.created_at else None,
        "last_copy_order_target_notional": str(order.target_notional) if order.target_notional is not None else None,
        "last_copy_order_delta_notional": str(order.delta_notional) if order.delta_notional is not None else None,
        "last_copy_order_estimated_notional": str(validator.get("estimated_notional") or ""),
        "last_copy_order_min_order_value": str(validator.get("min_order_value") or ""),
        "last_copy_order_not_submitted_to_exchange": _not_submitted_to_exchange(order),
    }


def _display_status(order: ExecutionOrder, *, error_code: Any, validator: dict[str, Any]) -> str | None:
    status = str(order.status or "").upper()
    block_reason = str(validator.get("block_reason") or "").upper()
    if status == "FILLED":
        return "LAST_ORDER_FILLED"
    if error_code == "BELOW_MIN_ORDER_VALUE" or block_reason == "BLOCKED_TOO_SMALL":
        return "BLOCKED_BELOW_MIN_ORDER"
    if status == "BLOCKED":
        return "LAST_ORDER_BLOCKED"
    if status == "REJECTED":
        return "LAST_ORDER_REJECTED"
    if status == "FAILED":
        return "LAST_ORDER_FAILED"
    if status == "UNKNOWN":
        return "LAST_ORDER_UNKNOWN"
    return None


def _validator_error_code(validator: dict[str, Any]) -> str | None:
    errors = validator.get("errors") if isinstance(validator, dict) else None
    if isinstance(errors, list):
        if "BELOW_MIN_ORDER_VALUE" in errors:
            return "BELOW_MIN_ORDER_VALUE"
        if errors:
            return str(errors[0])
    return None


def _not_submitted_to_exchange(order: ExecutionOrder) -> bool:
    return bool(
        str(order.status or "").upper() == "BLOCKED"
        and not (
            order.request_payload_masked
            or order.raw_response
            or order.response_payload_masked
            or order.order_ack_at
            or order.order_id
            or order.venue_order_id
        )
    )
