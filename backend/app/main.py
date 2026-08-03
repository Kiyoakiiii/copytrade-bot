from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import structlog
from sqlalchemy.dialects.postgresql import insert

from app.api import account_states, auth, dashboard, leaders, manual_orders, orders, positions, preflight, risk, stream, symbol_mappings, venues
from app.core.config import get_settings
from app.core.logging import configure_logging, redact_text
from app.db.session import SessionLocal
from app.models import AppSetting
from app.services.follower_migration import prepare_follower_runtime_identity
from app.services.leader_performance import run_leader_performance_refresher
from app.services.order_recovery import recover_unresolved_orders
from app.services.startup_config_validator import (
    bootstrap_leaders_from_settings,
    ensure_startup_defaults,
    store_startup_validation,
    validate_startup_config,
)
from app.services.task_status import store_task_status
from app.services.telegram_control_bot import (
    TELEGRAM_CONTROL_TASK_NAME,
    TELEGRAM_EXECUTION_ALERT_TASK_NAME,
    run_telegram_control_bot,
    run_telegram_execution_alerts,
    telegram_control_config_error,
)
from app.tasks.leader_state_poller import run_leader_state_poller
from app.tasks.low_latency_watcher import run_low_latency_watcher

settings = get_settings()
configure_logging(settings.log_level)
log = structlog.get_logger(__name__)

app = FastAPI(title=settings.app_name)


async def _supervised_background_task(
    name: str,
    task_factory: Callable[[], Awaitable[None]],
    *,
    restart_delay_seconds: float = 5.0,
) -> None:
    while True:
        try:
            await task_factory()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            safe_error = redact_text(exc)
            log.exception("background_task_failed", task_name=name, error=safe_error)
            async with SessionLocal() as db:
                await store_task_status(db, task_name=name, status="failed_restarting", last_error=safe_error[:500])
                await db.commit()
            await asyncio.sleep(restart_delay_seconds)


async def _order_recovery_loop() -> None:
    while True:
        try:
            recovered_orders = await recover_unresolved_orders(
                settings,
                min_pending_submit_age_seconds=settings.order_recovery_stale_pending_submit_seconds,
                unknown_oid_resubmit_age_seconds=settings.order_recovery_unknown_oid_resubmit_seconds,
            )
            async with SessionLocal() as db:
                await store_task_status(
                    db,
                    task_name="order_recovery",
                    status="running",
                    metadata={
                        "recovered_orders": recovered_orders,
                        "interval_seconds": settings.order_recovery_interval_seconds,
                        "stale_pending_submit_seconds": settings.order_recovery_stale_pending_submit_seconds,
                        "unknown_oid_resubmit_seconds": settings.order_recovery_unknown_oid_resubmit_seconds,
                    },
                )
                await db.commit()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            safe_error = redact_text(exc)
            log.exception("order_recovery_loop_failed", error=safe_error)
            async with SessionLocal() as db:
                await store_task_status(
                    db,
                    task_name="order_recovery",
                    status="failed_restarting",
                    last_error=safe_error[:500],
                )
                await db.commit()
        await asyncio.sleep(max(0.5, float(settings.order_recovery_interval_seconds)))


@app.middleware("http")
async def ip_allowlist_middleware(request: Request, call_next):
    allowed = settings.allowed_ips()
    if allowed and request.client and request.client.host not in allowed:
        return JSONResponse({"detail": "forbidden"}, status_code=403)
    return await call_next(request)


@app.middleware("http")
async def csrf_middleware(request: Request, call_next):
    if request.method in {"POST", "PATCH", "PUT", "DELETE"} and request.url.path != "/auth/login":
        session_cookie = request.cookies.get(settings.session_cookie_name)
        if session_cookie:
            cookie_token = request.cookies.get(settings.csrf_cookie_name)
            header_token = request.headers.get("x-csrf-token")
            if not cookie_token or not header_token or cookie_token != header_token:
                return JSONResponse({"detail": "csrf token missing"}, status_code=403)
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


@app.on_event("startup")
async def startup() -> None:
    async with SessionLocal() as db:
        await auth.bootstrap_admin(db, settings)
        await ensure_startup_defaults(db)
        await bootstrap_leaders_from_settings(db, settings)
        follower_migration = await prepare_follower_runtime_identity(db, settings=settings)
        startup_validation = await validate_startup_config(settings=settings, db=db)
        await store_startup_validation(db, startup_validation)
        await _prepare_startup_hyperliquid_risk_settings(db)
    telegram_config_error = telegram_control_config_error(settings)
    if settings.telegram_control_enabled and telegram_config_error is None:
        app.state.telegram_control_task = asyncio.create_task(
            _supervised_background_task(
                TELEGRAM_CONTROL_TASK_NAME,
                lambda: run_telegram_control_bot(settings),
            )
        )
        app.state.telegram_execution_alert_task = asyncio.create_task(
            _supervised_background_task(
                TELEGRAM_EXECUTION_ALERT_TASK_NAME,
                lambda: run_telegram_execution_alerts(settings),
            )
        )
    else:
        async with SessionLocal() as db:
            for task_name in (
                TELEGRAM_CONTROL_TASK_NAME,
                TELEGRAM_EXECUTION_ALERT_TASK_NAME,
            ):
                await store_task_status(
                    db,
                    task_name=task_name,
                    status="blocked_config" if settings.telegram_control_enabled else "disabled",
                    last_error=telegram_config_error,
                    metadata={"enabled": bool(settings.telegram_control_enabled)},
                )
            await db.commit()
        if telegram_config_error:
            log.error("telegram_control_config_blocked", error=telegram_config_error)
    if not follower_migration.ready:
        async with SessionLocal() as db:
            for task_name in ("order_recovery", "leader_state_poller", "low_latency_watcher"):
                await store_task_status(
                    db,
                    task_name=task_name,
                    status="blocked_follower_migration",
                    last_error="; ".join(follower_migration.blockers)[:500],
                    metadata={"follower_migration": follower_migration.payload},
                )
            await db.commit()
        log.error(
            "follower_migration_blocked",
            blockers=follower_migration.blockers,
        )
        return
    recovered_orders = await recover_unresolved_orders(
        settings,
        unknown_oid_resubmit_age_seconds=settings.order_recovery_unknown_oid_resubmit_seconds,
    )
    async with SessionLocal() as db:
        await store_task_status(
            db,
            task_name="order_recovery",
            status="completed",
            metadata={
                "recovered_orders": recovered_orders,
                "unknown_oid_resubmit_seconds": settings.order_recovery_unknown_oid_resubmit_seconds,
            },
        )
        await db.commit()
    app.state.leader_state_task = asyncio.create_task(
        _supervised_background_task("leader_state_poller", lambda: run_leader_state_poller(settings))
    )
    if settings.embedded_low_latency_watcher_enabled:
        app.state.low_latency_watcher_task = asyncio.create_task(
            _supervised_background_task("low_latency_watcher", lambda: run_low_latency_watcher(settings))
        )
    else:
        log.info("embedded_low_latency_watcher_disabled", execution_mode="dedicated_process")
    app.state.order_recovery_task = asyncio.create_task(_order_recovery_loop())
    if settings.leader_performance_refresh_enabled:
        app.state.leader_performance_task = asyncio.create_task(
            _supervised_background_task(
                "leader_performance",
                lambda: run_leader_performance_refresher(settings),
            )
        )
    else:
        log.info(
            "leader_performance_refresher_disabled",
            reason="historical analytics are isolated from the live trading host",
        )


@app.on_event("shutdown")
async def shutdown() -> None:
    tasks = []
    task = getattr(app.state, "leader_state_task", None)
    if task:
        task.cancel()
        tasks.append(task)
    low_latency_task = getattr(app.state, "low_latency_watcher_task", None)
    if low_latency_task:
        low_latency_task.cancel()
        tasks.append(low_latency_task)
    order_recovery_task = getattr(app.state, "order_recovery_task", None)
    if order_recovery_task:
        order_recovery_task.cancel()
        tasks.append(order_recovery_task)
    telegram_control_task = getattr(app.state, "telegram_control_task", None)
    if telegram_control_task:
        telegram_control_task.cancel()
        tasks.append(telegram_control_task)
    telegram_execution_alert_task = getattr(app.state, "telegram_execution_alert_task", None)
    if telegram_execution_alert_task:
        telegram_execution_alert_task.cancel()
        tasks.append(telegram_execution_alert_task)
    leader_performance_task = getattr(app.state, "leader_performance_task", None)
    if leader_performance_task:
        leader_performance_task.cancel()
        tasks.append(leader_performance_task)
    if tasks:
        with suppress(Exception):
            await asyncio.gather(*tasks, return_exceptions=True)


app.include_router(auth.router)
app.include_router(account_states.router)
app.include_router(dashboard.router)
app.include_router(leaders.router)
app.include_router(symbol_mappings.router)
app.include_router(venues.router)
app.include_router(positions.router)
app.include_router(preflight.router)
app.include_router(orders.router)
app.include_router(manual_orders.router)
app.include_router(risk.router)
app.include_router(stream.router)


async def _prepare_startup_hyperliquid_risk_settings(db) -> None:
    payload = {
        "ready": True,
        "ran": False,
        "reason": "Skipped blocking exchange risk refresh during API startup; low-latency watcher warms confirmed cache and hot path confirms missing markets before submit.",
        "coverage": None,
    }
    await _store_risk_settings_startup_status(db, payload)


async def _store_risk_settings_startup_status(db, payload: dict) -> None:
    stmt = (
        insert(AppSetting)
        .values(key="risk_settings_startup_status", value=payload)
        .on_conflict_do_update(index_elements=[AppSetting.key], set_={"value": payload})
    )
    await db.execute(stmt)
    await db.commit()
