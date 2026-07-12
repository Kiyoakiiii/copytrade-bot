from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select

from app.api.deps import CurrentUser, DbSession
from app.models import LatestAccountPosition, LatestLeaderState, LeaderPositionAllocationRecord
from app.services.account_state import position_payload
from app.services.leader_config import decimal_to_string

router = APIRouter(tags=["positions"])


@router.get("/positions/binance")
async def binance_positions(_: CurrentUser):
    return {"equity": None, "positions": []}


@router.get("/positions/leaders")
async def leader_positions(_: CurrentUser, db: DbSession):
    rows = (await db.execute(select(LatestLeaderState).order_by(LatestLeaderState.leader_address))).scalars().all()
    return [
        {
            "leader_address": row.leader_address,
            "accountValue": str(row.account_value),
            "withdrawable": str(row.withdrawable) if row.withdrawable is not None else None,
            "totalNtlPos": str(row.total_ntl_pos) if row.total_ntl_pos is not None else None,
            "totalMarginUsed": str(row.total_margin_used)
            if row.total_margin_used is not None
            else None,
            "positions": row.positions,
            "websocket_status": row.websocket_status,
            "updated_at": row.last_update_at.isoformat(),
        }
        for row in rows
    ]


@router.get("/positions/allocations")
async def leader_allocations(_: CurrentUser, db: DbSession):
    rows = (
        await db.execute(
            select(LeaderPositionAllocationRecord).order_by(
                LeaderPositionAllocationRecord.execution_venue,
                LeaderPositionAllocationRecord.venue_symbol,
                LeaderPositionAllocationRecord.leader_address,
                LeaderPositionAllocationRecord.position_side,
            )
        )
    ).scalars().all()
    return [
        {
            "leader_address": row.leader_address,
            "coin": row.hyperliquid_coin,
            "symbol": row.binance_symbol or row.venue_symbol,
            "binance_symbol": row.binance_symbol,
            "execution_venue": row.execution_venue,
            "venue_symbol": row.venue_symbol,
            "venue_account": row.venue_account,
            "position_side": row.position_side,
            "target_notional": str(row.target_notional),
            "allocated_notional": str(row.allocated_notional),
            "allocated_qty": str(row.allocated_qty),
            "copy_multiplier": decimal_to_string(row.copy_multiplier),
            "pending_reduce_qty": str(row.pending_reduce_qty)
            if row.pending_reduce_qty is not None
            else None,
            "pending_reduce_notional": str(row.pending_reduce_notional)
            if row.pending_reduce_notional is not None
            else None,
            "pending_reduce_reason": row.pending_reduce_reason,
            "pending_reduce_since": row.pending_reduce_since.isoformat()
            if row.pending_reduce_since
            else None,
            "status": row.status,
            "last_reconcile_at": row.last_reconcile_at.isoformat()
            if row.last_reconcile_at
            else None,
        }
        for row in rows
    ]


@router.get("/positions/history")
async def position_history(
    _: CurrentUser,
    db: DbSession,
    role: str | None = None,
    address: str | None = None,
    dex: str | None = None,
    include_open: bool = Query(True),
    include_closed: bool = Query(True),
    limit: int = Query(200, ge=1, le=1000),
):
    if not include_open and not include_closed:
        return []
    query = select(LatestAccountPosition)
    if role:
        query = query.where(LatestAccountPosition.role == role.upper())
    if address:
        query = query.where(LatestAccountPosition.address == address.lower())
    if dex is not None:
        query = query.where(LatestAccountPosition.dex == str(dex or "").strip().lower())
    if include_open and not include_closed:
        query = query.where(LatestAccountPosition.active.is_(True))
    elif include_closed and not include_open:
        query = query.where(LatestAccountPosition.active.is_(False))
    rows = (
        await db.execute(
            query.order_by(
                LatestAccountPosition.last_update_at.desc(),
                LatestAccountPosition.id.desc(),
            ).limit(limit)
        )
    ).scalars().all()
    return [position_payload(row) for row in rows]


@router.get("/leaders/stream")
async def stream_leaders(_: CurrentUser, db: DbSession):
    async def events():
        while True:
            rows = (await db.execute(select(LatestLeaderState).order_by(LatestLeaderState.leader_address))).scalars().all()
            payload = [
                {
                    "leader_address": row.leader_address,
                    "accountValue": str(row.account_value),
                    "positions": row.positions,
                    "websocket_status": row.websocket_status,
                    "updated_at": row.last_update_at.isoformat(),
                }
                for row in rows
            ]
            yield f"data: {json.dumps(payload)}\n\n"
            await asyncio.sleep(5)

    return StreamingResponse(events(), media_type="text/event-stream")
