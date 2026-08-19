from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.api.deps import CurrentUser, DbSession
from app.models import AppSetting, AuditLog


router = APIRouter(prefix="/research", tags=["research"])

JOB_PREFIX = "leader_research_job:"
ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
ACTIVE_STATUSES = {"QUEUED", "RUNNING"}
TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED"}
RESULT_CACHE_HOURS = 6


class ResearchJobCreate(BaseModel):
    tool: Literal["suitability", "balance"]
    address: str
    friction_bps: Decimal = Field(default=Decimal("5"), ge=0, le=100)
    target_tail_pct: Decimal = Field(default=Decimal("7.5"), gt=0, le=100)
    round_to: Decimal = Field(default=Decimal("10000"), gt=0)
    follower_balance: Decimal = Field(default=Decimal("20000"), gt=0)

    @field_validator("address")
    @classmethod
    def validate_address(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not ADDRESS_RE.fullmatch(normalized):
            raise ValueError("address must be a 0x-prefixed EVM address")
        return normalized


def _request_payload(payload: ResearchJobCreate) -> dict[str, str]:
    return {
        "tool": payload.tool,
        "address": payload.address,
        "friction_bps": str(payload.friction_bps),
        "target_tail_pct": str(payload.target_tail_pct),
        "round_to": str(payload.round_to),
        "follower_balance": str(payload.follower_balance),
    }


def _request_hash(payload: dict[str, str]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _public_job(row: AppSetting) -> dict[str, Any]:
    value = dict(row.value or {})
    request = dict(value.get("request") or {})
    return {
        "id": str(value.get("id") or row.key.removeprefix(JOB_PREFIX)),
        "status": str(value.get("status") or "UNKNOWN"),
        "tool": request.get("tool"),
        "address": request.get("address"),
        "parameters": {
            key: request.get(key)
            for key in ("friction_bps", "target_tail_pct", "round_to", "follower_balance")
        },
        "created_at": value.get("created_at") or row.created_at.isoformat(),
        "started_at": value.get("started_at"),
        "completed_at": value.get("completed_at"),
        "progress": value.get("progress"),
        "result": value.get("result"),
        "error": value.get("error"),
        "cached": bool(value.get("cached")),
    }


async def _recent_job_rows(db: DbSession, *, limit: int = 50) -> list[AppSetting]:
    return list(
        (
            await db.execute(
                select(AppSetting)
                .where(AppSetting.key.like(f"{JOB_PREFIX}%"))
                .order_by(AppSetting.updated_at.desc())
                .limit(limit)
            )
        ).scalars()
    )


@router.get("/jobs")
async def list_research_jobs(
    _: CurrentUser,
    db: DbSession,
    limit: int = Query(default=10, ge=1, le=50),
):
    rows = await _recent_job_rows(db, limit=limit)
    return [_public_job(row) for row in rows]


@router.get("/jobs/{job_id}")
async def get_research_job(job_id: str, _: CurrentUser, db: DbSession):
    try:
        normalized_id = str(uuid.UUID(job_id))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="research job not found") from exc
    row = await db.get(AppSetting, f"{JOB_PREFIX}{normalized_id}")
    if row is None:
        raise HTTPException(status_code=404, detail="research job not found")
    return _public_job(row)


@router.post("/jobs")
async def create_research_job(
    user: CurrentUser,
    payload: ResearchJobCreate,
    db: DbSession,
):
    request = _request_payload(payload)
    fingerprint = _request_hash(request)
    rows = await _recent_job_rows(db)

    for row in rows:
        value = dict(row.value or {})
        if value.get("request_hash") == fingerprint and value.get("status") in ACTIVE_STATUSES:
            return _public_job(row)

    cache_cutoff = datetime.now(timezone.utc) - timedelta(hours=RESULT_CACHE_HOURS)
    for row in rows:
        value = dict(row.value or {})
        if (
            value.get("request_hash") == fingerprint
            and value.get("status") == "COMPLETED"
            and row.updated_at >= cache_cutoff
        ):
            cached = _public_job(row)
            cached["cached"] = True
            return cached

    active = next(
        (
            row
            for row in rows
            if str((row.value or {}).get("status") or "") in ACTIVE_STATUSES
        ),
        None,
    )
    if active is not None:
        raise HTTPException(
            status_code=409,
            detail="another leader research job is already running; wait for it to finish",
        )

    now = datetime.now(timezone.utc)
    job_id = str(uuid.uuid4())
    value = {
        "id": job_id,
        "status": "QUEUED",
        "request": request,
        "request_hash": fingerprint,
        "created_at": now.isoformat(),
        "progress": "Waiting for the isolated, API-budgeted research worker",
        "result": None,
        "error": None,
    }
    await db.execute(insert(AppSetting).values(key=f"{JOB_PREFIX}{job_id}", value=value))
    db.add(
        AuditLog(
            user_id=user.id,
            action="leader_research.enqueue",
            metadata_json={"job_id": job_id, "tool": payload.tool},
        )
    )
    await db.commit()
    row = await db.get(AppSetting, f"{JOB_PREFIX}{job_id}")
    return _public_job(row)
