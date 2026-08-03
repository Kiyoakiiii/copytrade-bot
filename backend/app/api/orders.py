from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.encoders import jsonable_encoder
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy import func, select

from app.api.deps import CurrentUser, DbSession
from app.models import ExecutionOrder

router = APIRouter(tags=["orders"])


@router.get("/orders")
async def list_orders(
    _: CurrentUser,
    db: DbSession,
    leader: str | None = None,
    symbol: str | None = None,
    status: str | None = None,
    limit: int = Query(100, ge=1, le=500),
):
    query = select(ExecutionOrder).order_by(ExecutionOrder.created_at.desc()).limit(limit)
    if leader:
        query = query.where(ExecutionOrder.leader_address == leader)
    if symbol:
        symbol_u = symbol.upper()
        query = query.where(
            (func.upper(ExecutionOrder.binance_symbol) == symbol_u)
            | (func.upper(ExecutionOrder.venue_symbol) == symbol_u)
            | (func.upper(ExecutionOrder.hyperliquid_coin) == symbol_u)
            | (func.upper(ExecutionOrder.source_coin) == symbol_u)
            | (func.upper(ExecutionOrder.canonical_coin) == symbol_u)
        )
    if status:
        query = query.where(ExecutionOrder.status == status)
    result = await db.execute(query)
    rows = result.scalars().all()
    updated_at = max(
        [row.updated_at or row.created_at for row in rows if row.updated_at or row.created_at],
        default=None,
    )
    data_age_ms = _age_ms(updated_at)
    return {
        "data": [_order_payload(row) for row in rows],
        "data_source": "db_latest_state",
        "dataSource": "db_latest_state",
        "updated_at": updated_at.isoformat() if updated_at else None,
        "updatedAt": updated_at.isoformat() if updated_at else None,
        "data_age_ms": data_age_ms,
        "dataAgeMs": data_age_ms,
        "stale": False,
        "refresh_in_progress": False,
        "refreshInProgress": False,
        "last_error": None,
        "lastError": None,
    }


def _order_payload(row: ExecutionOrder) -> dict:
    payload = jsonable_encoder(row)
    # A persisted signed action is an internal recovery artifact. Never expose
    # its signature/envelope or signer fingerprint through the operator API.
    for internal_field in (
        "signed_action_envelope",
        "signed_action_hash",
        "submit_signer_scope",
        "submit_nonce",
    ):
        payload.pop(internal_field, None)
    checklist = row.pre_trade_checklist or {}
    validator = checklist.get("order_validator") or {}
    payload["order_validator"] = validator
    payload["validator_status"] = validator.get("validator_status")
    payload["error_code"] = checklist.get("error_code")
    payload["exchange_submit_attempted"] = bool(
        row.request_payload_masked
        or row.raw_response
        or row.response_payload_masked
        or row.order_ack_at
        or row.order_id
        or row.venue_order_id
    )
    payload["required_multiplier_to_pass_min_order"] = _required_multiplier(row, validator)
    payload["not_submitted_to_exchange"] = row.status == "BLOCKED" and not payload["exchange_submit_attempted"]
    return payload


def _required_multiplier(row: ExecutionOrder, validator: dict) -> str | None:
    try:
        min_value = Decimal(str(validator.get("min_order_value") or "0"))
        estimated = Decimal(str(validator.get("estimated_notional") or row.delta_notional or "0"))
        multiplier = Decimal(str(row.copy_multiplier or "0"))
    except Exception:
        return None
    if min_value <= 0 or estimated <= 0 or multiplier <= 0 or estimated >= min_value:
        return None
    required = (multiplier * min_value / estimated).quantize(Decimal("0.00000001"))
    return str(required)


def _age_ms(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return max(0, int((datetime.now(timezone.utc) - value).total_seconds() * 1000))
