from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select

from app.api.dashboard import build_dashboard_realtime_payload
from app.api.deps import AppSettings, current_user
from app.db.session import SessionLocal
from app.models import (
    AppSetting,
    ExecutionOrder,
    LatestAccountPosition,
    LatestAccountState,
    LeaderConfig,
    LeaderPositionAllocationRecord,
    LeaderPositionBaseline,
)
from app.tasks.leader_state_poller import account_state_cache_status

router = APIRouter(tags=["stream"])

HEARTBEAT_SECONDS = 5.0
VERSION_POLL_SECONDS = 1.0


@router.get("/stream/dashboard")
async def stream_dashboard(request: Request, settings: AppSettings):
    async with SessionLocal() as db:
        await current_user(request, db, settings)
    return StreamingResponse(
        _dashboard_event_generator(request, settings),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def _dashboard_event_generator(request: Request, settings: AppSettings):
    last_seen = request.headers.get("last-event-id") or ""
    last_version = ""
    last_components: dict[str, str | None] = {}
    last_heartbeat = 0.0
    while not await request.is_disconnected():
        components = await dashboard_component_versions()
        version = dashboard_data_version(components)
        now_monotonic = asyncio.get_running_loop().time()
        if version != last_version or (last_seen and last_seen != version):
            async with SessionLocal() as db:
                snapshot = await build_dashboard_realtime_payload(
                    db=db,
                    settings=settings,
                    state_refresh=await account_state_cache_status(settings),
                )
            changed = _changed_components(last_components, components)
            for event_type, payload in _events_for_change(changed, snapshot):
                yield sse_event(event_type=event_type, payload=payload, data_version=version)
            last_components = components
            last_version = version
            last_seen = ""
            last_heartbeat = now_monotonic
        elif now_monotonic - last_heartbeat >= HEARTBEAT_SECONDS:
            yield sse_event(
                event_type="heartbeat",
                payload={"ok": True},
                data_version=version,
                stale=False,
                data_age_ms=None,
            )
            last_heartbeat = now_monotonic
        await asyncio.sleep(VERSION_POLL_SECONDS)


async def dashboard_component_versions() -> dict[str, str | None]:
    async with SessionLocal() as db:
        values = {
            "accounts": await _max_updated_at(db, LatestAccountState.updated_at, LatestAccountPosition.updated_at),
            "orders": await _max_updated_at(db, ExecutionOrder.updated_at, ExecutionOrder.created_at),
            "leaders": await _max_updated_at(db, LeaderConfig.updated_at, LeaderConfig.created_at),
            "allocations": await _max_updated_at(
                db,
                LeaderPositionAllocationRecord.updated_at,
                LeaderPositionAllocationRecord.created_at,
            ),
            "baseline": await _max_updated_at(
                db,
                LeaderPositionBaseline.updated_at,
                LeaderPositionBaseline.created_at,
            ),
            "app_settings": await _max_updated_at(db, AppSetting.updated_at),
        }
    return {key: _iso_or_none(value) for key, value in values.items()}


async def _max_updated_at(db: Any, *columns: Any) -> datetime | None:
    values: list[datetime] = []
    for column in columns:
        value = await db.scalar(select(func.max(column)))
        if value is not None:
            values.append(value)
    if not values:
        return None
    return max(value if value.tzinfo else value.replace(tzinfo=timezone.utc) for value in values)


def dashboard_data_version(components: dict[str, str | None]) -> str:
    raw = json.dumps(components, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def sse_event(
    *,
    event_type: str,
    payload: Any,
    data_version: str,
    stale: bool | None = None,
    data_age_ms: int | None = None,
) -> str:
    server_time = datetime.now(timezone.utc).isoformat()
    safe_payload = _redact_secrets(payload)
    body = {
        "event_id": f"{data_version}:{event_type}",
        "event_type": event_type,
        "server_time": server_time,
        "data_version": data_version,
        "payload": safe_payload,
        "stale": bool(safe_payload.get("stale")) if stale is None and isinstance(safe_payload, dict) else bool(stale),
        "stale_flags": _stale_flags(safe_payload),
        "data_age_ms": safe_payload.get("data_age_ms") if data_age_ms is None and isinstance(safe_payload, dict) else data_age_ms,
    }
    encoded = json.dumps(body, default=str, separators=(",", ":"))
    return f"id: {data_version}\nevent: {event_type}\ndata: {encoded}\n\n"


def _redact_secrets(value: Any) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            key_l = str(key).lower()
            if any(token in key_l for token in ("private", "secret", "signature", "api_key", "api-key")):
                result[str(key)] = "***"
            else:
                result[str(key)] = _redact_secrets(item)
        return result
    if isinstance(value, list):
        return [_redact_secrets(item) for item in value]
    return value


def _changed_components(previous: dict[str, str | None], current: dict[str, str | None]) -> set[str]:
    if not previous:
        return set(current)
    return {key for key, value in current.items() if previous.get(key) != value}


def _events_for_change(changed: set[str], snapshot: dict[str, Any]) -> list[tuple[str, Any]]:
    events: list[tuple[str, Any]] = [("dashboard_snapshot", snapshot)]
    if changed.intersection({"accounts", "leaders"}):
        events.append(("follower_state_update", snapshot.get("follower")))
        events.append(("leader_state_update", snapshot.get("leaders")))
        events.append(
            (
                "positions_update",
                {
                    "follower": (snapshot.get("follower") or {}).get("positions") or [],
                    "leaders": [
                        {
                            "leader": (leader.get("leader") or {}),
                            "positions": leader.get("positions") or [],
                        }
                        for leader in snapshot.get("leaders") or []
                    ],
                },
            )
        )
    if "orders" in changed:
        events.append(("orders_update", snapshot.get("recent_orders") or []))
        events.append(("latency_update", snapshot.get("latency") or {}))
    if "app_settings" in changed:
        events.append(("watcher_status_update", (snapshot.get("runtime") or {}) | {"state_refresh": snapshot.get("state_refresh")}))
        events.append(("task_health_update", snapshot.get("state_refresh") or {}))
        events.append(("preflight_update", snapshot.get("small_live_start_checklist") or {}))
    if "baseline" in changed:
        events.append(("baseline_status_update", snapshot.get("baseline") or {}))
    if "allocations" in changed:
        events.append(("allocation_status_update", snapshot.get("active_allocations") or []))
    return events


def _stale_flags(payload: Any) -> dict[str, bool]:
    if not isinstance(payload, dict):
        return {"payload": False}
    result = {"payload": bool(payload.get("stale"))}
    follower = payload.get("follower")
    if isinstance(follower, dict):
        result["follower"] = bool(follower.get("stale"))
    leaders = payload.get("leaders")
    if isinstance(leaders, list):
        result["leaders"] = any(isinstance(item, dict) and bool(item.get("stale")) for item in leaders)
    return result


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()
