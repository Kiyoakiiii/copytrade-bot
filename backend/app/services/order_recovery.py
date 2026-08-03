from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
from typing import Any

import structlog
from sqlalchemy import select, update

from app.core.config import Settings
from app.core.logging import redact_text
from app.db.session import SessionLocal
from app.models import (
    AllocationEvent,
    ExecutionOrder,
    LeaderPositionAllocationRecord,
    RiskEvent,
    SourceFillOutcome,
)
from app.services.auto_copy import RECOVERY_ORDER_STATUSES, extract_market_fill
from app.services.binance_client import BinanceFuturesClient
from app.services.execution_router import ExecutionVenue
from app.services.hyperliquid_execution import HyperliquidExecutionClient, recover_hyperliquid_unknown_order
from app.services.allocation_fill import apply_filled_order_to_allocation_state
from app.services.low_latency_watcher import (
    _close_zero_allocation_lifecycle,
    _hyperliquid_error,
    _hyperliquid_fill_qty_price,
    _hyperliquid_oid,
    _hyperliquid_status,
    _open_like_action,
    _record_allocation_event,
    _set_latency_fields,
    _stale_zero_allocation_reason,
)

log = structlog.get_logger(__name__)


async def recover_unresolved_orders(
    settings: Settings,
    *,
    min_pending_submit_age_seconds: float | None = None,
    unknown_oid_resubmit_age_seconds: float | None = None,
) -> int:
    recovered = 0
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                select(ExecutionOrder).where(
                    ExecutionOrder.source_type == "AUTO_COPY",
                    ExecutionOrder.status.in_(RECOVERY_ORDER_STATUSES),
                )
            )
        ).scalars().all()
        if min_pending_submit_age_seconds is not None:
            now = datetime.now(timezone.utc)
            rows = [
                order
                for order in rows
                if _recovery_order_due(
                    order,
                    now=now,
                    min_pending_submit_age_seconds=min_pending_submit_age_seconds,
                )
            ]
        binance_rows = [
            order
            for order in rows
            if order.execution_venue == ExecutionVenue.BINANCE.value and order.client_order_id
        ]
        hyperliquid_rows = [
            order
            for order in rows
            if order.execution_venue == ExecutionVenue.HYPERLIQUID.value and order.cloid
            and not _is_unstarted_hyperliquid_outbox_order(order)
        ]

        if settings.binance_api_key and settings.binance_api_secret and binance_rows:
            client = BinanceFuturesClient(settings)
            try:
                for order in binance_rows:
                    try:
                        response = await client.get_order(
                            symbol=order.binance_symbol or order.venue_symbol or "",
                            orig_client_order_id=order.client_order_id or "",
                        )
                    except Exception as exc:
                        db.add(
                            RiskEvent(
                                severity="warning",
                                event_type="ORDER_RECOVERY_FAILED",
                                symbol=order.binance_symbol or order.venue_symbol,
                                leader_address=order.leader_address,
                                message=f"could not recover Binance order status: {redact_text(exc)}",
                                metadata_json={"client_order_id": order.client_order_id},
                            )
                        )
                        continue

                    previous_executed_qty = order.executed_qty or Decimal("0")
                    _apply_order_response(order, response)
                    await _apply_allocation_delta(db, order, response, previous_executed_qty)
                    recovered += 1
            finally:
                await client.close()

        if settings.enable_hyperliquid_execution and hyperliquid_rows:
            clients: dict[str, HyperliquidExecutionClient] = {}
            try:
                for order in hyperliquid_rows:
                    execution_scope = str(order.venue_account or "").lower()
                    client = clients.get(execution_scope)
                    if client is None:
                        client = HyperliquidExecutionClient(
                            info_url=f"{settings.hyperliquid_execution_base_url()}/info",
                            private_key=settings.hyperliquid_private_key_value(),
                            account_address=(
                                settings.hyperliquid_account_address
                                or settings.hyperliquid_signer_address()
                            ),
                            vault_address=(
                                execution_scope
                                if execution_scope
                                else settings.hyperliquid_execution_vault_address()
                            ),
                            network=settings.hyperliquid_execution_network,
                        )
                        clients[execution_scope] = client
                    try:
                        response = await recover_hyperliquid_unknown_order(
                            client,
                            coin=order.hyperliquid_coin or order.venue_symbol or order.source_coin,
                            cloid=order.cloid or "",
                        )
                        if _is_hyperliquid_unknown_oid(response):
                            if order.signed_action_envelope:
                                response = await _replay_persisted_hyperliquid_action(
                                    client,
                                    order,
                                )
                                if _hyperliquid_error(response):
                                    response = await recover_hyperliquid_unknown_order(
                                        client,
                                        coin=order.hyperliquid_coin or order.venue_symbol or order.source_coin,
                                        cloid=order.cloid or "",
                                    )
                                if _is_hyperliquid_unknown_oid(response):
                                    order.status = "UNKNOWN"
                                    order.error_message = (
                                        "persisted signed action replayed but cloid remains unknown; "
                                        "kept for recovery without creating a new order"
                                    )
                                    await _update_source_fill_outcome(db, order, "SUBMISSION_UNKNOWN")
                                    continue
                            else:
                                now = datetime.now(timezone.utc)
                                if _should_defer_unknown_oid_recovery(
                                    order,
                                    now=now,
                                    unknown_oid_resubmit_age_seconds=unknown_oid_resubmit_age_seconds,
                                ):
                                    order.error_message = "Hyperliquid cloid not found yet; recovery deferred"
                                    await _update_source_fill_outcome(db, order, "SUBMISSION_UNKNOWN")
                                    continue
                    except Exception as exc:
                        safe_error = redact_text(exc)
                        db.add(
                            RiskEvent(
                                severity="warning",
                                event_type="ORDER_RECOVERY_FAILED",
                                symbol=order.hyperliquid_coin or order.venue_symbol,
                                leader_address=order.leader_address,
                                message=f"could not recover Hyperliquid order status: {safe_error}",
                                metadata_json={"cloid": order.cloid},
                            )
                        )
                        continue
                    previous_executed_qty = order.executed_qty or Decimal("0")
                    _apply_hyperliquid_order_response(order, response)
                    if _is_hyperliquid_unknown_oid(response):
                        await _close_zero_allocation_after_unsubmitted_hyperliquid_open(
                            db,
                            order,
                            reason=order.error_message,
                        )
                    await _apply_hyperliquid_allocation_delta(
                        db,
                        order,
                        previous_executed_qty,
                    )
                    await _update_source_fill_outcome_from_order(db, order)
                    recovered += 1
            finally:
                for client in clients.values():
                    await client.close()

        await db.commit()
    if recovered:
        log.info("recovered_unresolved_orders", count=recovered)
    return recovered


async def _replay_persisted_hyperliquid_action(
    client: HyperliquidExecutionClient,
    order: ExecutionOrder,
) -> dict[str, Any]:
    envelope = order.signed_action_envelope
    if not isinstance(envelope, dict):
        raise RuntimeError("persisted signed action envelope is invalid")
    if order.signed_action_hash and _signed_action_hash(envelope) != order.signed_action_hash:
        raise RuntimeError("persisted signed action envelope integrity check failed")
    envelope_nonce = ((envelope.get("payload") or {}).get("nonce"))
    if order.submit_nonce is None or int(envelope_nonce) != int(order.submit_nonce):
        raise RuntimeError("persisted signed action nonce does not match its outbox row")
    if order.submit_signer_scope and order.submit_signer_scope != client.signer_scope:
        raise RuntimeError("persisted signed action belongs to a different signer scope")
    return await client.submit_market_order_envelope(envelope)


def _signed_action_hash(envelope: dict[str, Any]) -> str:
    canonical = json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


async def _update_source_fill_outcome_from_order(db: Any, order: ExecutionOrder) -> None:
    status = str(order.status or "").upper()
    executed_qty = Decimal(order.executed_qty or 0)
    if executed_qty > 0:
        disposition = "EXECUTED"
    elif status in {"UNKNOWN", "PENDING_SUBMIT", "SUBMITTING", "SUBMITTED", "OPEN", "RESTING"}:
        disposition = "SUBMISSION_UNKNOWN"
    elif status in {"REJECTED", "FAILED", "BLOCKED"}:
        disposition = "MANUAL_REVIEW"
    else:
        disposition = "NO_ACTION_REQUIRED"
    await _update_source_fill_outcome(db, order, disposition)


async def _update_source_fill_outcome(
    db: Any,
    order: ExecutionOrder,
    disposition: str,
) -> None:
    await db.execute(
        update(SourceFillOutcome)
        .where(SourceFillOutcome.execution_order_id == order.id)
        .values(
            disposition=disposition,
            reason=redact_text(order.error_message)[:2000] if order.error_message else None,
            updated_at=datetime.now(timezone.utc),
        )
    )


def _recovery_order_due(
    order: ExecutionOrder,
    *,
    now: datetime,
    min_pending_submit_age_seconds: float,
) -> bool:
    status = str(order.status or "").upper()
    if status not in {"PENDING_SUBMIT", "SUBMITTING"}:
        return True
    reference = order.order_submit_started_at or order.created_at or order.updated_at
    if reference is None:
        return True
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return now - reference >= timedelta(seconds=max(0.0, float(min_pending_submit_age_seconds)))


def _is_unstarted_hyperliquid_outbox_order(order: ExecutionOrder) -> bool:
    return (
        str(order.status or "").upper() in {"PENDING_SUBMIT", "SUBMITTING"}
        and order.order_submit_started_at is None
        and order.order_submit_done_at is None
        and order.order_ack_at is None
        and not order.order_id
        and not order.venue_order_id
        and not order.raw_response
    )


def _apply_order_response(order: ExecutionOrder, response: dict[str, Any]) -> None:
    order.raw_response = response
    order.status = str(response.get("status", order.status))
    if response.get("orderId"):
        order.order_id = str(response["orderId"])
    market_fill = extract_market_fill(
        response,
        estimated_price=order.estimated_price or order.price or Decimal("0"),
    )
    order.executed_qty = market_fill.executed_qty
    order.avg_fill_price = market_fill.avg_fill_price
    order.cum_quote = market_fill.cum_quote
    order.slippage_bps = market_fill.slippage_bps
    if order.status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
        order.order_finalized_at = datetime.now(timezone.utc)


def _apply_hyperliquid_order_response(order: ExecutionOrder, response: dict[str, Any]) -> None:
    order.raw_response = response
    order.response_payload_masked = response
    if response.get("response") is not None:
        order.status = _hyperliquid_status(response)
        hyperliquid_error = _hyperliquid_error(response)
        if hyperliquid_error:
            order.error_message = f"Hyperliquid order rejected: {hyperliquid_error}"
        fill_qty, avg_px = _hyperliquid_fill_qty_price(response)
        if fill_qty is not None:
            order.executed_qty = fill_qty
        if avg_px is not None:
            order.avg_fill_price = avg_px
        if fill_qty is not None and avg_px is not None:
            order.cum_quote = _q(fill_qty * avg_px)
        oid = _hyperliquid_oid(response)
        if oid:
            order.order_id = oid
            order.venue_order_id = oid
        if order.status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
            order.order_finalized_at = datetime.now(timezone.utc)
        _set_latency_fields(order)
        return
    nested_order = response.get("order") if isinstance(response.get("order"), dict) else {}
    status = str(nested_order.get("status") or response.get("status") or order.status).upper()
    if status == "UNKNOWNOID":
        order.status = "FAILED"
        order.order_finalized_at = datetime.now(timezone.utc)
        order.error_message = "Hyperliquid order not found by cloid; auto resubmit disabled to prevent duplicate order"
        return
    if status == "ORDER":
        status = "SUBMITTED"
    order.status = status
    oid = nested_order.get("oid") or response.get("oid")
    order_payload = nested_order.get("order") if isinstance(nested_order.get("order"), dict) else {}
    if oid is None:
        oid = order_payload.get("oid")
    if oid is not None:
        order.venue_order_id = str(oid)
        order.order_id = str(oid)
    fill_qty, avg_px = _hyperliquid_order_status_fill_qty_price(order, response)
    if fill_qty is not None:
        order.executed_qty = fill_qty
    if avg_px is not None:
        order.avg_fill_price = avg_px
    if fill_qty is not None and avg_px is not None:
        order.cum_quote = _q(fill_qty * avg_px)
    if order.status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
        order.order_finalized_at = datetime.now(timezone.utc)


def _hyperliquid_order_status_fill_qty_price(
    order: ExecutionOrder,
    response: dict[str, Any],
) -> tuple[Decimal | None, Decimal | None]:
    nested_order = response.get("order") if isinstance(response.get("order"), dict) else {}
    status = str(nested_order.get("status") or response.get("status") or "").upper()
    if status != "FILLED":
        return None, None
    order_payload = nested_order.get("order") if isinstance(nested_order.get("order"), dict) else nested_order
    qty = _decimal_from_recovery_value(
        order_payload.get("origSz")
        or order_payload.get("totalSz")
        or order_payload.get("filledSz")
        or order_payload.get("sz")
    )
    if qty is None or qty <= 0:
        qty = Decimal(order.quantity or 0)
    avg_px = _decimal_from_recovery_value(
        order_payload.get("avgPx")
        or order_payload.get("avgPrice")
        or order.avg_fill_price
        or order.estimated_price
        or order.price
    )
    return qty if qty > 0 else None, avg_px if avg_px is not None and avg_px > 0 else None


def _decimal_from_recovery_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _is_hyperliquid_unknown_oid(response: dict[str, Any]) -> bool:
    if not isinstance(response, dict):
        return False
    nested_order = response.get("order") if isinstance(response.get("order"), dict) else {}
    status = str(nested_order.get("status") or response.get("status") or "").upper()
    return status == "UNKNOWNOID"


def _should_resubmit_unstarted_hyperliquid_order(order: ExecutionOrder) -> bool:
    return False


def _should_defer_unknown_oid_recovery(
    order: ExecutionOrder,
    *,
    now: datetime,
    unknown_oid_resubmit_age_seconds: float | None,
) -> bool:
    if not _can_defer_hyperliquid_unknown_oid(order):
        return False
    if _unknown_oid_resubmit_age(order, now=now) >= _unknown_oid_resubmit_threshold(
        unknown_oid_resubmit_age_seconds
    ):
        return False
    return str(order.status or "").upper() in RECOVERY_ORDER_STATUSES


def _should_resubmit_stale_unknown_oid_order(
    order: ExecutionOrder,
    *,
    now: datetime,
    unknown_oid_resubmit_age_seconds: float | None,
) -> bool:
    return False


def _can_defer_hyperliquid_unknown_oid(order: ExecutionOrder) -> bool:
    return (
        order.execution_venue == ExecutionVenue.HYPERLIQUID.value
        and bool(order.cloid)
    )


def _unknown_oid_resubmit_age(order: ExecutionOrder, *, now: datetime) -> timedelta:
    reference = order.order_submit_started_at or order.created_at or order.updated_at
    if reference is None:
        return timedelta.max
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    return now - reference


def _unknown_oid_resubmit_threshold(value: float | None) -> timedelta:
    if value is None:
        value = 30.0
    return timedelta(seconds=max(0.0, float(value)))


async def _resubmit_unstarted_hyperliquid_order(
    client: HyperliquidExecutionClient,
    order: ExecutionOrder,
) -> dict[str, Any]:
    raise RuntimeError("Hyperliquid auto-copy recovery resubmit is disabled to prevent duplicate orders")


async def _close_zero_allocation_after_unsubmitted_hyperliquid_open(
    db: Any,
    order: ExecutionOrder,
    *,
    reason: str | None,
) -> None:
    if not _open_like_action(order.order_action) or bool(order.reduce_only) or order.allocation_id is None:
        return
    allocation = await _load_allocation_for_recovery_update(db, order.allocation_id)
    stale_reason = _stale_zero_allocation_reason(allocation)
    if not stale_reason:
        return
    now = datetime.now(timezone.utc)
    close_reason = f"{stale_reason}: {reason or 'Hyperliquid order was not submitted'}"
    before_notional = Decimal(allocation.allocated_notional or 0)
    before_qty = Decimal(allocation.allocated_qty or 0)
    _close_zero_allocation_lifecycle(allocation, reason=close_reason, now=now)
    db.add(
        AllocationEvent(
            allocation_id=allocation.id,
            execution_order_id=order.id,
            leader_id=allocation.leader_id,
            leader_address=allocation.leader_address,
            source_fill_id=order.source_fill_id,
            execution_venue=allocation.execution_venue,
            dex=allocation.dex,
            canonical_coin=allocation.canonical_coin,
            position_side=allocation.position_side,
            action="RECOVERY_ZERO_OPEN_ABORTED",
            before_notional=before_notional,
            after_notional=Decimal("0"),
            before_qty=before_qty,
            after_qty=Decimal("0"),
            metadata_json={
                "order_id": order.id,
                "order_status": order.status,
                "order_action": order.order_action,
                "reason": close_reason,
            },
        )
    )
    db.add(
        RiskEvent(
            severity="warning",
            event_type="RECOVERY_ZERO_OPEN_ABORTED",
            symbol=order.canonical_coin or order.hyperliquid_coin or order.venue_symbol,
            leader_address=order.leader_address,
            message=close_reason,
            metadata_json={
                "allocation_id": allocation.id,
                "order_id": order.id,
                "source_fill_id": order.source_fill_id,
                "cloid": order.cloid,
            },
        )
    )


async def _apply_allocation_delta(
    db: Any,
    order: ExecutionOrder,
    response: dict[str, Any],
    previous_qty: Decimal,
) -> None:
    if order.allocation_id is None or order.order_action not in {
        "OPEN_OR_INCREASE",
        "OPEN",
        "INCREASE",
        "FLIP_OPEN_SECOND",
        "CLOSE_OR_REDUCE",
        "REDUCE",
        "CLOSE",
        "FLIP_CLOSE_FIRST",
    }:
        return
    allocation = await _load_allocation_for_recovery_update(db, order.allocation_id)
    if allocation is None:
        return

    executed_qty = Decimal(str(response.get("executedQty") or response.get("cumQty") or "0"))
    delta_qty = executed_qty - previous_qty
    if delta_qty <= 0:
        return
    avg_price = Decimal(str(response.get("avgPrice") or allocation.avg_entry_price or "0"))
    if avg_price <= 0:
        allocation.status = "BLOCKED"
        return

    if order.order_action in {"CLOSE_OR_REDUCE", "REDUCE", "CLOSE", "FLIP_CLOSE_FIRST"}:
        remaining_qty = max(Decimal("0"), allocation.allocated_qty - delta_qty)
        allocation.allocated_qty = remaining_qty
        allocation.allocated_notional = _q(remaining_qty * (allocation.avg_entry_price or avg_price))
        allocation.target_notional = allocation.allocated_notional
        allocation.status = "CLOSED" if remaining_qty == 0 else "OPEN"
        return

    fill_notional = _q(delta_qty * avg_price)
    new_qty = allocation.allocated_qty + delta_qty
    new_notional = _q(allocation.allocated_notional + fill_notional)
    allocation.allocated_qty = new_qty
    allocation.allocated_notional = new_notional
    allocation.target_notional = new_notional
    allocation.avg_entry_price = _q(new_notional / new_qty)
    allocation.status = "OPEN"


async def _apply_hyperliquid_allocation_delta(
    db: Any,
    order: ExecutionOrder,
    previous_qty: Decimal,
) -> None:
    if order.allocation_id is None or order.order_action not in {
        "OPEN_OR_INCREASE",
        "OPEN",
        "INCREASE",
        "FLIP_OPEN_SECOND",
        "CLOSE_OR_REDUCE",
        "REDUCE",
        "CLOSE",
        "FLIP_CLOSE_FIRST",
    }:
        return
    if str(order.status or "").upper() != "FILLED":
        return
    if await _allocation_fill_event_already_applied(db, order):
        return
    allocation = await _load_allocation_for_recovery_update(db, order.allocation_id)
    if allocation is None:
        return
    if await _allocation_fill_event_already_applied(db, order):
        return
    delta_qty = Decimal(order.executed_qty or 0) - Decimal(previous_qty or 0)
    result = apply_filled_order_to_allocation_state(
        allocation,
        order,
        fill_qty=delta_qty,
        update_target_notional=True,
        clear_deferred_reduce=False,
    )
    if result is None:
        return
    _record_recovery_fill_applied_event(
        db,
        allocation=allocation,
        order=order,
        before_notional=result.before_notional,
        after_notional=result.after_notional,
        before_qty=result.before_qty,
        after_qty=result.after_qty,
    )


async def _load_allocation_for_recovery_update(
    db: Any,
    allocation_id: int,
) -> LeaderPositionAllocationRecord | None:
    flush = getattr(db, "flush", None)
    if flush is not None:
        maybe = flush()
        if hasattr(maybe, "__await__"):
            await maybe

    scalar = getattr(db, "scalar", None)
    if scalar is not None:
        allocation = await scalar(
            select(LeaderPositionAllocationRecord)
            .where(LeaderPositionAllocationRecord.id == allocation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
            .limit(1)
        )
        if allocation is not None:
            return allocation

    get = getattr(db, "get", None)
    if get is None:
        return None
    return await get(LeaderPositionAllocationRecord, allocation_id)


def _record_recovery_fill_applied_event(
    db: Any,
    *,
    allocation: LeaderPositionAllocationRecord,
    order: ExecutionOrder,
    before_notional: Decimal | None,
    after_notional: Decimal | None,
    before_qty: Decimal | None,
    after_qty: Decimal | None,
) -> None:
    if not hasattr(db, "add"):
        return
    _record_allocation_event(
        db,
        allocation=allocation,
        order=order,
        action="FILL_APPLIED",
        before_notional=before_notional,
        after_notional=after_notional,
        before_qty=before_qty,
        after_qty=after_qty,
        metadata={"order_action": order.order_action, "source": "ORDER_RECOVERY"},
    )


async def _allocation_fill_event_already_applied(db: Any, order: ExecutionOrder) -> bool:
    if order.id is None:
        return False
    existing = await db.scalar(
        select(AllocationEvent.id)
        .where(AllocationEvent.execution_order_id == order.id)
        .where(AllocationEvent.action == "FILL_APPLIED")
        .limit(1)
    )
    return existing is not None

def _q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
