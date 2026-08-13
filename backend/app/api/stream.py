from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text

from app.api.dashboard import build_dashboard_realtime_payload
from app.api.deps import AppSettings, current_user
from app.db.session import SessionLocal
from app.tasks.leader_state_poller import (
    account_state_cache_status,
    monitoring_account_state_stale_seconds,
)

router = APIRouter(tags=["stream"])

HEARTBEAT_SECONDS = 5.0
VERSION_POLL_SECONDS = 2.0
SNAPSHOT_CACHE_SECONDS = 5.0

# Dashboard change detection used to calculate MAX(updated_at) on several
# heavily-updated tables once per second, per browser.  PostgreSQL had to scan
# roughly a gigabyte of heap pages for every pass.  The cumulative per-table
# mutation counters are constant-size, require no application schema change,
# and are sufficient for invalidating a monitoring snapshot.
_DASHBOARD_COMPONENT_TABLES: dict[str, tuple[str, ...]] = {
    "accounts": ("latest_account_states", "latest_account_positions"),
    "orders": ("execution_orders",),
    "leaders": ("leader_configs",),
    "allocations": ("leader_position_allocations",),
    "baseline": ("leader_position_baselines",),
    "app_settings": ("app_settings",),
}
_snapshot_lock = asyncio.Lock()
_snapshot_cache_version = ""
_snapshot_cache_payload: dict[str, Any] | None = None
_snapshot_cache_at = 0.0


@router.get("/stream/dashboard")
async def stream_dashboard(request: Request, settings: AppSettings):
    async with SessionLocal() as db:
        # ``current_user`` is normally called by FastAPI's dependency injector,
        # which resolves its ``Cookie(...)`` default to a string.  This endpoint
        # calls it directly so that the authentication session can be closed
        # before the long-lived SSE response starts.  Pass the concrete cookie
        # value explicitly; otherwise the unresolved FastAPI Cookie descriptor
        # reaches the session-token verifier and raises TypeError.
        await current_user(
            request,
            db,
            settings,
            request.cookies.get(settings.session_cookie_name),
        )
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
            initial = not last_components
            snapshot = await _dashboard_snapshot(settings, version)
            changed = _changed_components(last_components, components)
            for event_type, payload in _events_for_change(changed, snapshot, initial=initial):
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
    relation_names = {
        relation
        for relations in _DASHBOARD_COMPONENT_TABLES.values()
        for relation in relations
    }
    async with SessionLocal() as db:
        rows = (
            await db.execute(
                text(
                    """
                    SELECT relname, n_tup_ins, n_tup_upd, n_tup_del, n_live_tup
                    FROM pg_stat_user_tables
                    WHERE schemaname = current_schema()
                      AND relname = ANY(:relation_names)
                    """
                ),
                {"relation_names": sorted(relation_names)},
            )
        ).mappings().all()
    stats = {
        str(row["relname"]): ":".join(
            str(int(row[name] or 0))
            for name in ("n_tup_ins", "n_tup_upd", "n_tup_del", "n_live_tup")
        )
        for row in rows
    }
    return {
        component: "|".join(f"{relation}={stats.get(relation, 'missing')}" for relation in relations)
        for component, relations in _DASHBOARD_COMPONENT_TABLES.items()
    }


async def _dashboard_snapshot(settings: AppSettings, version: str) -> dict[str, Any]:
    """Build one snapshot per observed version and share it across SSE clients."""

    global _snapshot_cache_at, _snapshot_cache_payload, _snapshot_cache_version
    now = asyncio.get_running_loop().time()
    if (
        _snapshot_cache_payload is not None
        and _snapshot_cache_version == version
        and now - _snapshot_cache_at <= SNAPSHOT_CACHE_SECONDS
    ):
        return _snapshot_cache_payload
    async with _snapshot_lock:
        now = asyncio.get_running_loop().time()
        if (
            _snapshot_cache_payload is not None
            and _snapshot_cache_version == version
            and now - _snapshot_cache_at <= SNAPSHOT_CACHE_SECONDS
        ):
            return _snapshot_cache_payload
        async with SessionLocal() as db:
            payload = await build_dashboard_realtime_payload(
                db=db,
                settings=settings,
                state_refresh=await account_state_cache_status(
                    settings,
                    max_age_seconds=monitoring_account_state_stale_seconds(settings),
                ),
            )
        _snapshot_cache_payload = payload
        _snapshot_cache_version = version
        _snapshot_cache_at = asyncio.get_running_loop().time()
        return payload


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


def _events_for_change(
    changed: set[str],
    snapshot: dict[str, Any],
    *,
    initial: bool = False,
) -> list[tuple[str, Any]]:
    if initial:
        return [("dashboard_snapshot", snapshot)]
    events: list[tuple[str, Any]] = []
    if changed.intersection({"accounts", "leaders"}):
        events.append(("follower_state_update", snapshot.get("follower")))
        events.append(("leader_state_update", snapshot.get("leaders")))
        events.append(
            (
                "positions_update",
                {"changed": True},
            )
        )
    if "orders" in changed:
        events.append(("orders_update", snapshot.get("recent_orders") or []))
        events.append(("latency_update", snapshot.get("latency") or {}))
    if "app_settings" in changed:
        events.append(("watcher_status_update", (snapshot.get("runtime") or {}) | {"state_refresh": snapshot.get("state_refresh")}))
        events.append(("task_health_update", snapshot.get("state_refresh") or {}))
        events.append(
            (
                "preflight_update",
                {
                    "small_live_start_checklist": snapshot.get("small_live_start_checklist") or {},
                    "preflight_blockers": snapshot.get("preflight_blockers") or [],
                },
            )
        )
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
