from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.core.config import Settings
from app.db.session import SessionLocal
from app.models import AppSetting, LatestAccountState, LatestLeaderState
from app.services.account_abstraction import (
    AccountAbstractionService,
    resolve_account_value_for_sizing,
    save_account_abstraction_state,
)
from app.services.account_state import (
    FOLLOWER,
    INFO_ENDPOINT,
    LEADER,
    AccountStateService,
    error_account_state,
    parse_account_state,
    save_account_state,
)
from app.services.baseline import (
    baseline_capture_setting_key,
    capture_leader_position_baselines,
    sync_waiting_baselines_from_state,
)
from app.services.hyperliquid import HyperliquidInfoClient
from app.services.hyperliquid_dex import HyperliquidDexRegistry
from app.services.leader_config import active_leaders_statement, normalize_leader_address
from app.services.leader_state import leader_state_to_json, parse_leader_state
from app.services.task_status import store_task_status

log = structlog.get_logger(__name__)
_refresh_lock = asyncio.Lock()
_refresh_task: asyncio.Task | None = None
_refresh_started_at: datetime | None = None
_last_refresh_error: str | None = None
MONITORING_STATE_STALE_FLOOR_SECONDS = 30


def _background_info_client(settings: Settings, url: str) -> HyperliquidInfoClient:
    return HyperliquidInfoClient(
        url,
        min_request_interval_seconds=max(
            0.0,
            float(
                getattr(
                    settings,
                    "hyperliquid_background_info_min_interval_seconds",
                    0.05,
                )
                or 0.0
            ),
        ),
    )


def monitoring_account_state_stale_seconds(settings: Settings) -> int:
    """Freshness window for cached UI snapshots, not live order validation.

    A complete account-state pass makes multiple sequential exchange requests
    across every enabled DEX and leader.  The live-trading freshness threshold
    is deliberately much tighter, but applying it to a monitoring snapshot
    falsely marks rows near the beginning of a healthy poll as stale and makes
    read-only pages queue redundant refresh work.
    """

    hot_path_seconds = max(1, int(settings.account_state_stale_seconds or 1))
    poll_seconds = max(1, int(settings.account_state_poll_seconds or 1))
    return max(
        MONITORING_STATE_STALE_FLOOR_SECONDS,
        hot_path_seconds,
        poll_seconds * 2,
    )


async def run_leader_state_poller(settings: Settings, *, interval_seconds: int | None = None) -> None:
    interval_seconds = interval_seconds or settings.account_state_poll_seconds
    leader_client = _background_info_client(settings, settings.hyperliquid_info_url)
    follower_client = _background_info_client(
        settings,
        f"{settings.hyperliquid_execution_base_url()}/info",
    )
    try:
        while True:
            tick_started_at = asyncio.get_running_loop().time()
            try:
                async with _refresh_lock:
                    await poll_once(leader_client, follower_client=follower_client, settings=settings)
            except Exception as exc:
                log.exception("leader_state_poller_tick_failed", error=str(exc))
            elapsed_seconds = asyncio.get_running_loop().time() - tick_started_at
            await asyncio.sleep(max(0, interval_seconds - elapsed_seconds))
    except asyncio.CancelledError:
        raise
    finally:
        await leader_client.close()
        await follower_client.close()


async def ensure_recent_account_states(
    settings: Settings,
    *,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    max_age_seconds = max(1, int(max_age_seconds or settings.account_state_stale_seconds))
    freshness = await _account_state_freshness(settings, max_age_seconds=max_age_seconds)
    if not freshness["stale"]:
        return {"refreshed": False, **freshness}

    async with _refresh_lock:
        freshness = await _account_state_freshness(settings, max_age_seconds=max_age_seconds)
        if not freshness["stale"]:
            return {"refreshed": False, **freshness}

        started_at = datetime.now(timezone.utc)
        leader_client = _background_info_client(settings, settings.hyperliquid_info_url)
        follower_client = _background_info_client(
            settings,
            f"{settings.hyperliquid_execution_base_url()}/info",
        )
        try:
            await poll_once(leader_client, follower_client=follower_client, settings=settings)
        except Exception as exc:
            log.exception("on_demand_account_state_refresh_failed", error=str(exc))
            return {
                "refreshed": False,
                "refresh_failed": True,
                "error": str(exc)[:500],
                **freshness,
            }
        finally:
            await leader_client.close()
            await follower_client.close()

        refreshed = await _account_state_freshness(settings, max_age_seconds=max_age_seconds)
        done_at = datetime.now(timezone.utc)
        return {
            "refreshed": True,
            "refresh_failed": False,
            "refresh_started_at": started_at.isoformat(),
            "refresh_done_at": done_at.isoformat(),
            **refreshed,
        }


async def schedule_account_state_refresh_if_stale(
    settings: Settings,
    *,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    global _refresh_task, _refresh_started_at
    max_age_seconds = max(1, int(max_age_seconds or settings.account_state_stale_seconds))
    freshness = await _account_state_freshness(settings, max_age_seconds=max_age_seconds)
    refresh_in_progress = bool(_refresh_task and not _refresh_task.done())
    if freshness["stale"] and not refresh_in_progress:
        _refresh_started_at = datetime.now(timezone.utc)
        _refresh_task = asyncio.create_task(_run_scheduled_refresh(settings, max_age_seconds=max_age_seconds))
        refresh_in_progress = True
    return {
        "refreshed": False,
        "refresh_scheduled": freshness["stale"],
        "refresh_in_progress": refresh_in_progress,
        "refresh_started_at": _refresh_started_at.isoformat() if _refresh_started_at else None,
        "last_error": _last_refresh_error,
        **freshness,
    }


async def account_state_cache_status(
    settings: Settings,
    *,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    max_age_seconds = max(1, int(max_age_seconds or settings.account_state_stale_seconds))
    refresh_in_progress = bool(_refresh_task and not _refresh_task.done())
    return {
        "refresh_in_progress": refresh_in_progress,
        "refresh_started_at": _refresh_started_at.isoformat() if _refresh_started_at else None,
        "last_error": _last_refresh_error,
        **(await _account_state_freshness(settings, max_age_seconds=max_age_seconds)),
    }


async def _run_scheduled_refresh(settings: Settings, *, max_age_seconds: int) -> None:
    global _last_refresh_error
    async with _refresh_lock:
        freshness = await _account_state_freshness(settings, max_age_seconds=max_age_seconds)
        if not freshness["stale"]:
            return
        leader_client = _background_info_client(settings, settings.hyperliquid_info_url)
        follower_client = _background_info_client(
            settings,
            f"{settings.hyperliquid_execution_base_url()}/info",
        )
        try:
            await poll_once(leader_client, follower_client=follower_client, settings=settings)
            _last_refresh_error = None
        except Exception as exc:
            _last_refresh_error = str(exc)[:500]
            log.exception("scheduled_account_state_refresh_failed", error=str(exc))
        finally:
            await leader_client.close()
            await follower_client.close()


async def _account_state_freshness(settings: Settings, *, max_age_seconds: int) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    async with SessionLocal() as db:
        leaders = (await db.execute(active_leaders_statement())).scalars().all()
        enabled_dexes = HyperliquidDexRegistry(settings).enabled_dexes()
        required_scopes: list[tuple[str, str, str]] = []
        follower_address = settings.hyperliquid_follower_account_address()
        if follower_address:
            required_scopes.extend(
                (FOLLOWER, follower_address.lower(), dex.dex_name)
                for dex in enabled_dexes
            )
        for leader in leaders:
            leader_address = normalize_leader_address(leader.leader_address)
            required_scopes.extend(
                (LEADER, leader_address, dex.dex_name)
                for dex in enabled_dexes
            )
        if not required_scopes:
            return {
                "stale": False,
                "required_scopes": 0,
                "stale_scopes_count": 0,
                "oldest_age_seconds": None,
                "oldest_update_at": None,
                "max_age_seconds": max_age_seconds,
            }

        rows = (await db.execute(select(LatestAccountState))).scalars().all()

    rows_by_scope = {
        (row.role, str(row.address).lower(), str(row.dex or "").lower()): row
        for row in rows
    }
    stale_scopes: list[dict[str, Any]] = []
    oldest_age_seconds: int | None = None
    oldest_update_at: datetime | None = None
    for role, address, dex in required_scopes:
        row = rows_by_scope.get((role, address, str(dex or "").lower()))
        if row is None or row.last_update_at is None:
            stale_scopes.append({"role": role, "address": address, "dex": dex, "reason": "missing"})
            continue
        updated_at = row.last_update_at if row.last_update_at.tzinfo else row.last_update_at.replace(tzinfo=timezone.utc)
        age_seconds = max(0, int((now - updated_at).total_seconds()))
        if oldest_age_seconds is None or age_seconds > oldest_age_seconds:
            oldest_age_seconds = age_seconds
            oldest_update_at = updated_at
        if row.error_message or age_seconds > max_age_seconds:
            stale_scopes.append(
                {
                    "role": role,
                    "address": address,
                    "dex": dex,
                    "age_seconds": age_seconds,
                    "reason": "error" if row.error_message else "stale",
                }
            )
    return {
        "stale": bool(stale_scopes),
        "required_scopes": len(required_scopes),
        "stale_scopes_count": len(stale_scopes),
        "stale_scopes": stale_scopes[:8],
        "oldest_age_seconds": oldest_age_seconds,
        "oldest_update_at": oldest_update_at.isoformat() if oldest_update_at else None,
        "max_age_seconds": max_age_seconds,
    }


async def poll_once(
    client: HyperliquidInfoClient,
    *,
    follower_client: HyperliquidInfoClient | None = None,
    settings: Settings | None = None,
) -> None:
    async with SessionLocal() as db:
        leaders = (await db.execute(active_leaders_statement())).scalars().all()
        enabled_dexes = HyperliquidDexRegistry(settings).enabled_dexes() if settings else []
        mids_by_dex: dict[str, dict] = {}
        for dex in enabled_dexes:
            try:
                mids_by_dex[dex.dex_name] = await client.all_mids(dex.dex_name)
            except Exception as exc:
                mids_by_dex[dex.dex_name] = {}
                log.warning("price_mids_poll_failed", dex=dex.dex_name, error=str(exc))
        watcher_now = datetime.now(timezone.utc)
        watcher_payload = {
            "mode": "db_polling",
            "low_latency_primary": False,
            "low_latency_required_for_live": bool(settings and settings.low_latency_required_for_live),
            "websocket_connected": False,
            "low_latency_ready": False,
            "poll_fallback_count": 1,
            "source": "leader_state_poller",
            "active_leaders": [normalize_leader_address(leader.leader_address) for leader in leaders],
            "enabled_dexes": [dex.dex_name for dex in enabled_dexes],
            "subscribed_leaders_by_dex": {dex.dex_name: 0 for dex in enabled_dexes},
            "last_event_time_by_dex": {dex.dex_name: None for dex in enabled_dexes},
            "updated_at": watcher_now.isoformat(),
        }
        watcher_stmt = (
            insert(AppSetting)
            .values(key="state_poller_status", value=watcher_payload, updated_at=watcher_now)
            .on_conflict_do_update(
                index_elements=[AppSetting.key],
                set_={"value": watcher_payload, "updated_at": watcher_now},
            )
        )
        await db.execute(watcher_stmt)
        await store_task_status(
            db,
            task_name="leader_state_poller",
            metadata={
                "active_leaders": watcher_payload["active_leaders"],
                "enabled_dexes": watcher_payload["enabled_dexes"],
            },
        )
        await store_task_status(
            db,
            task_name="account_state_poller",
            metadata={
                "active_leaders": watcher_payload["active_leaders"],
                "enabled_dexes": watcher_payload["enabled_dexes"],
            },
        )
        if settings is not None:
            for leader in leaders:
                capture_status = await db.get(AppSetting, baseline_capture_setting_key(leader.id))
                if capture_status is not None and bool((capture_status.value or {}).get("ready")):
                    continue
                await capture_leader_position_baselines(
                    db,
                    leader=leader,
                    settings=settings,
                    info_client=client,
                    reason="poller missing baseline capture",
                    force_reset=True,
                )
        if settings is not None:
            follower_address = settings.hyperliquid_follower_account_address()
            low_latency_primary = bool(settings.low_latency_required_for_live)
            if follower_address and not low_latency_primary:
                follower_service = AccountStateService(follower_client or client)
                for dex in enabled_dexes:
                    try:
                        follower_state = await follower_service.fetch_state(
                            role=FOLLOWER,
                            address=follower_address,
                            dex=dex.dex_name,
                            account_label=f"My Hyperliquid Follower Account / {dex.display_name}",
                            source=INFO_ENDPOINT,
                            price_mids=mids_by_dex.get(dex.dex_name),
                        )
                        await save_account_state(db, follower_state)
                    except Exception as exc:
                        await save_account_state(
                            db,
                            error_account_state(
                                role=FOLLOWER,
                                address=follower_address,
                                dex=dex.dex_name,
                                account_label=f"My Hyperliquid Follower Account / {dex.display_name}",
                                error_message=str(exc),
                            ),
                        )
                        log.warning("follower_state_poll_failed", dex=dex.dex_name, error=str(exc))
                try:
                    spot_state = await (follower_client or client).spot_clearinghouse_state(follower_address)
                    spot_payload = {
                        "address": follower_address,
                        "usdc_total": _spot_balance(spot_state, "USDC"),
                        "balances_count": len(spot_state.get("balances") or []),
                        "source": "spotClearinghouseState",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                    spot_stmt = (
                        insert(AppSetting)
                        .values(key="follower_spot_state", value=spot_payload, updated_at=datetime.now(timezone.utc))
                        .on_conflict_do_update(
                            index_elements=[AppSetting.key],
                            set_={"value": spot_payload, "updated_at": datetime.now(timezone.utc)},
                        )
                    )
                    await db.execute(spot_stmt)
                except Exception as exc:
                    log.warning("follower_spot_state_poll_failed", error=str(exc))
                try:
                    abstraction = await AccountAbstractionService(
                        follower_client or client,
                        settings,
                    ).fetch_snapshot(
                        role=FOLLOWER,
                        address=follower_address,
                        dexes=[dex.dex_name for dex in enabled_dexes],
                    )
                    await save_account_abstraction_state(
                        db,
                        snapshot=abstraction,
                        resolved_by_dex={
                            dex.dex_name: resolve_account_value_for_sizing(
                                abstraction,
                                dex.dex_name,
                                settings,
                            )
                            for dex in enabled_dexes
                        },
                    )
                except Exception as exc:
                    log.warning("follower_account_abstraction_poll_failed", error=str(exc))
        for leader in leaders:
            for dex in enabled_dexes:
                try:
                    raw = await client.clearinghouse_state(leader.leader_address, dex=dex.dex_name)
                    now = datetime.now(timezone.utc)
                    if dex.dex_name == "":
                        state = parse_leader_state(
                            leader.leader_address,
                            raw,
                            websocket_status="connected",
                            updated_at=now,
                        )
                        payload = leader_state_to_json(state)
                        stmt = (
                            insert(LatestLeaderState)
                            .values(
                                leader_address=state.leader_address,
                                account_value=state.account_value,
                                withdrawable=state.withdrawable,
                                total_ntl_pos=state.total_ntl_pos,
                                total_margin_used=state.total_margin_used,
                                positions=payload["positions"],
                                websocket_status=state.websocket_status,
                                last_update_at=state.updated_at,
                            )
                            .on_conflict_do_update(
                                index_elements=[LatestLeaderState.leader_address],
                                set_={
                                    "account_value": state.account_value,
                                    "withdrawable": state.withdrawable,
                                    "total_ntl_pos": state.total_ntl_pos,
                                    "total_margin_used": state.total_margin_used,
                                    "positions": payload["positions"],
                                    "websocket_status": state.websocket_status,
                                    "last_update_at": state.updated_at,
                                    "updated_at": now,
                                },
                            )
                        )
                        await db.execute(stmt)
                    account_state = parse_account_state(
                        role=LEADER,
                        address=leader.leader_address,
                        dex=dex.dex_name,
                        clearinghouse_state=raw,
                        account_label=f"Leader {leader.id} / {dex.display_name}",
                        source=INFO_ENDPOINT,
                        updated_at=now,
                        price_mids=mids_by_dex.get(dex.dex_name),
                    )
                    await save_account_state(db, account_state)
                    await sync_waiting_baselines_from_state(
                        db,
                        leader=leader,
                        state=account_state,
                        now=now,
                    )
                except Exception as exc:
                    await save_account_state(
                        db,
                        error_account_state(
                            role=LEADER,
                            address=leader.leader_address,
                            dex=dex.dex_name,
                            account_label=f"Leader {leader.id} / {dex.display_name}",
                            error_message=str(exc),
                        ),
                    )
                    log.warning(
                        "leader_state_poll_failed",
                        leader_address=leader.leader_address,
                        dex=dex.dex_name,
                        error=str(exc),
                    )
            if _leader_requires_dynamic_account_abstraction(leader):
                try:
                    abstraction = await AccountAbstractionService(client, settings).fetch_snapshot(
                        role=LEADER,
                        address=leader.leader_address,
                        dexes=[dex.dex_name for dex in enabled_dexes],
                    )
                    await save_account_abstraction_state(
                        db,
                        snapshot=abstraction,
                        resolved_by_dex={
                            dex.dex_name: resolve_account_value_for_sizing(
                                abstraction,
                                dex.dex_name,
                                settings,
                            )
                            for dex in enabled_dexes
                        },
                    )
                except Exception as exc:
                    log.warning(
                        "leader_account_abstraction_poll_failed",
                        leader_address=leader.leader_address,
                        error=str(exc),
                    )
        await db.commit()


def _leader_requires_dynamic_account_abstraction(leader: Any) -> bool:
    """Live sizing uses the configured leader balance when it is valid."""
    try:
        return Decimal(leader.fixed_account_value or 0) <= 0
    except Exception:
        return True


def _spot_balance(spot_state: dict, coin: str) -> str | None:
    for row in spot_state.get("balances") or []:
        if str(row.get("coin", "")).upper() == coin.upper():
            try:
                return str(Decimal(str(row.get("total") or "0")))
            except Exception:
                return str(row.get("total"))
    return None
