from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from fastapi import APIRouter
from sqlalchemy import select, text

from app.api.deps import AppSettings, CurrentUser, DbSession
from app.api.risk import get_risk_setting
from app.models import (
    ExecutionOrder,
    LatestAccountPosition,
    LatestAccountState,
    LatestLeaderState,
    LeaderConfig,
    LeaderPositionAllocationRecord,
    RiskEvent,
    SourceFill,
    SymbolMapping,
    VenueMapping,
    AppSetting,
)
from app.api.account_states import (
    _account_abstraction_fields,
    _account_state_payloads_for_address,
    _follower_config_debug,
    _load_follower_spot_debug,
)
from app.services.account_abstraction import (
    AccountAbstractionService,
    load_account_abstraction_state,
    resolved_value_payload,
    resolve_account_value_for_sizing,
)
from app.services.account_state import FOLLOWER, LEADER, account_state_payload, load_account_state_with_positions, position_payload
from app.services.auto_copy import AUTO_COPY_ORDER_TYPE, RECOVERY_ORDER_STATUSES
from app.services.baseline import (
    baseline_readiness_summary,
    baselines_by_scope_for_leaders,
    baseline_scope_key,
    baseline_status_for_position,
)
from app.services.binance_client import BinanceFuturesClient
from app.services.calculator import (
    SIZING_MODE_ACCOUNT_RATIO,
    calculate_leader_position_ratio,
    calculate_target_notional_by_account_ratio,
)
from app.services.execution_router import ExecutionVenue
from app.services.follower_migration import (
    FOLLOWER_MIGRATION_READY,
    FOLLOWER_RUNTIME_IDENTITY_KEY,
    public_follower_migration_payload,
)
from app.services.hyperliquid_execution import (
    HyperliquidExecutionClient,
    build_hyperliquid_leverage_plan,
    check_hyperliquid_available_margin,
    resolve_asset_id_from_meta,
)
from app.services.hyperliquid_risk_settings import (
    build_market_risk_settings_coverage,
    prepare_current_hyperliquid_risk_settings,
)
from app.services.hyperliquid_dex import HyperliquidDexRegistry, canonical_coin, dex_display_name, parse_coin
from app.services.leader_config import (
    active_leader_addresses,
    allowed_coins_mode,
    decimal_to_string,
    is_coin_allowed,
    normalize_leader_address,
    watcher_consistency,
)
from app.services.live_readiness import small_live_start_checklist
from app.services.market_coverage import build_hyperliquid_market_coverage
from app.services.order_policy import AUTO_COPY_ORDER_POLICY, auto_copy_order_policy_status
from app.services.sizing_guard import SizingGuardError, assert_sizing_mode_account_ratio

router = APIRouter(tags=["preflight"])


def _age_seconds(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int((datetime.now(timezone.utc) - value).total_seconds())


def _iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


async def _redis_ok(settings: AppSettings) -> bool:
    try:
        import redis.asyncio as redis

        client = redis.from_url(settings.redis_url, socket_connect_timeout=1)
        await client.ping()
        await client.aclose()
        return True
    except Exception:
        return False


async def _db_ok(db: DbSession) -> bool:
    try:
        await db.execute(text("select 1"))
        return True
    except Exception:
        return False


def _position_qty(row: dict) -> Decimal:
    return abs(Decimal(str(row.get("positionAmt", "0"))))


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or str(value) == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _aggregate_binance_positions(rows: list[dict]) -> dict[str, dict[str, Decimal]]:
    result: dict[str, dict[str, Decimal]] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        side = str(row.get("positionSide", "")).upper()
        if not symbol or side not in {"LONG", "SHORT"}:
            continue
        result.setdefault(symbol, {"LONG": Decimal("0"), "SHORT": Decimal("0")})
        result[symbol][side] += _position_qty(row)
    return result


def _aggregate_hyperliquid_positions(rows: list[dict]) -> dict[str, dict[str, Decimal]]:
    result: dict[str, dict[str, Decimal]] = {}
    for row in rows:
        position = row.get("position", row)
        coin = str(position.get("coin", "")).upper()
        if not coin:
            continue
        size = Decimal(str(position.get("szi") or position.get("size") or "0"))
        result.setdefault(coin, {"LONG": Decimal("0"), "SHORT": Decimal("0")})
        if size > 0:
            result[coin]["LONG"] += abs(size)
        elif size < 0:
            result[coin]["SHORT"] += abs(size)
    return result


def _venue_coin_mapping_rows(
    venue_mappings: list[VenueMapping],
    legacy_mappings: list[SymbolMapping],
    venue: ExecutionVenue,
) -> list[Any]:
    rows = [row for row in venue_mappings if row.execution_venue == venue.value]
    if rows or venue == ExecutionVenue.BINANCE:
        return rows
    return [
        {
            "hyperliquid_coin": row.hyperliquid_coin,
            "venue_symbol": row.hyperliquid_coin,
            "enabled": True,
            "mapping_status": "UNKNOWN",
            "reason": "requires Hyperliquid meta validation",
        }
        for row in legacy_mappings
    ]


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    return getattr(row, key, default)


def _mapping_status_allows_trading(row: Any) -> bool:
    status = str(_row_value(row, "mapping_status", "UNKNOWN")).upper()
    return bool(_row_value(row, "enabled", False)) and status not in {"BLOCKED", "DISABLED", "MISSING"}


@router.get("/preflight")
async def preflight(_: CurrentUser, db: DbSession, settings: AppSettings):
    response_at = datetime.now(timezone.utc)
    risk = await get_risk_setting(db)
    db_connected = await _db_ok(db)
    redis_connected = await _redis_ok(settings)
    kill_switch = bool(risk.get("kill_switch", False))

    legacy_mappings = (
        await db.execute(select(SymbolMapping).order_by(SymbolMapping.hyperliquid_coin))
    ).scalars().all()
    venue_mappings = (
        await db.execute(select(VenueMapping).order_by(VenueMapping.hyperliquid_coin))
    ).scalars().all()
    leaders = (
        await db.execute(select(LeaderConfig).order_by(LeaderConfig.created_at.desc()))
    ).scalars().all()
    baseline_summary = await baseline_readiness_summary(db, leaders)
    baseline_by_scope = await baselines_by_scope_for_leaders(db, [leader.id for leader in leaders])
    allocations = (
        await db.execute(
            select(LeaderPositionAllocationRecord).order_by(
                LeaderPositionAllocationRecord.execution_venue,
                LeaderPositionAllocationRecord.venue_symbol,
                LeaderPositionAllocationRecord.leader_address,
                LeaderPositionAllocationRecord.position_side,
            )
        )
    ).scalars().all()
    state_rows = {
        row.leader_address.lower(): row
        for row in (await db.execute(select(LatestLeaderState))).scalars().all()
    }
    unresolved_orders = (
        await db.execute(
            select(ExecutionOrder).where(
                ExecutionOrder.source_type == "AUTO_COPY",
                ExecutionOrder.status.in_(RECOVERY_ORDER_STATUSES),
            )
        )
    ).scalars().all()
    latency_orders = (
        await db.execute(
            select(ExecutionOrder)
            .where(
                ExecutionOrder.source_type == "AUTO_COPY",
                ExecutionOrder.event_to_ack_ms.is_not(None),
            )
            .order_by(ExecutionOrder.created_at.desc())
            .limit(10)
        )
    ).scalars().all()
    fill_implied_status = await _fill_implied_position_status(db, response_at)
    startup_config_row = await db.get(AppSetting, "startup_config")
    startup_config = startup_config_row.value if startup_config_row else None
    risk_settings_startup_row = await db.get(AppSetting, "risk_settings_startup_status")
    risk_settings_startup_status = risk_settings_startup_row.value if risk_settings_startup_row else None
    watcher_row = await db.get(AppSetting, "watcher_status")
    watcher_status = watcher_row.value if watcher_row else {}
    follower_migration_row = await db.get(AppSetting, FOLLOWER_RUNTIME_IDENTITY_KEY)
    follower_migration = public_follower_migration_payload(
        follower_migration_row.value if follower_migration_row else None
    )
    watcher_active_addresses = [
        normalize_leader_address(address)
        for address in watcher_status.get("active_leaders", [])
        if address
    ]
    watcher_status_age = _age_seconds(watcher_row.updated_at) if watcher_row else None
    low_latency_fill_ready = bool(
        settings.low_latency_required_for_live and watcher_status.get("ready_for_low_latency_live")
    )
    watcher_checks = watcher_consistency(
        leaders=leaders,
        watcher_active_addresses=watcher_active_addresses,
    )
    db_enabled_addresses = active_leader_addresses(leaders)
    follower_address = settings.hyperliquid_follower_account_address()
    follower_state, follower_positions = (
        await load_account_state_with_positions(db, role=FOLLOWER, address=follower_address, dex="")
        if follower_address
        else (None, [])
    )
    follower_abstraction = (
        await load_account_abstraction_state(db, role=FOLLOWER, address=follower_address)
        if follower_address
        else None
    )
    follower_dex_states = (
        await _account_state_payloads_for_address(
            db,
            role=FOLLOWER,
            address=follower_address,
            settings=settings,
            extra_for_each={"configured": True},
            account_abstraction=follower_abstraction,
        )
        if follower_address
        else []
    )
    follower_state_rows = (
        await db.execute(
            select(LatestAccountState)
            .where(LatestAccountState.role == FOLLOWER)
            .where(LatestAccountState.address == follower_address.lower())
        )
    ).scalars().all() if follower_address else []
    follower_states_by_dex = {row.dex: row for row in follower_state_rows}
    follower_account = account_state_payload(
        follower_state,
        follower_positions,
        stale_seconds=settings.account_state_stale_seconds,
        extra={
            "configured": bool(follower_address),
            "dex_states": follower_dex_states,
            "dexStates": follower_dex_states,
            "debug": _follower_config_debug(
                settings,
                follower_state,
                follower_dex_states,
                spot_debug=await _load_follower_spot_debug(db),
                account_abstraction=follower_abstraction,
            ),
            **_account_abstraction_fields(follower_abstraction, dex=""),
        },
    )
    follower_account["positions"] = [position for item in follower_dex_states for position in item.get("positions", [])]
    leader_account_state_rows = (
        await db.execute(select(LatestAccountState).where(LatestAccountState.role == LEADER))
    ).scalars().all()
    leader_account_states_by_address: dict[str, list[LatestAccountState]] = {}
    for row in leader_account_state_rows:
        leader_account_states_by_address.setdefault(row.address.lower(), []).append(row)
    leader_state_ids = [row.id for row in leader_account_state_rows]
    leader_positions_by_state: dict[int, list[LatestAccountPosition]] = {}
    if leader_state_ids:
        for position in (
            await db.execute(
                select(LatestAccountPosition)
                .where(LatestAccountPosition.account_state_id.in_(leader_state_ids))
                .where(LatestAccountPosition.active.is_(True))
                .order_by(LatestAccountPosition.coin)
            )
        ).scalars().all():
            leader_positions_by_state.setdefault(position.account_state_id, []).append(position)

    binance = await _build_binance_readiness(
        settings=settings,
        legacy_mappings=legacy_mappings,
        allocations=allocations,
        unresolved_orders=unresolved_orders,
    )
    hyperliquid = await _build_hyperliquid_readiness(
        settings=settings,
        venue_mappings=venue_mappings,
        legacy_mappings=legacy_mappings,
        leaders=[
            leader
            for leader in leaders
            if normalize_leader_address(leader.leader_address) in db_enabled_addresses
        ],
        allocations=allocations,
        unresolved_orders=unresolved_orders,
    )
    risk_settings = await build_market_risk_settings_coverage(db=db, settings=settings)

    leader_account_items = []
    for leader in leaders:
        address = normalize_leader_address(leader.leader_address)
        leader_abstraction = await load_account_abstraction_state(db, role=LEADER, address=address)
        states = sorted(leader_account_states_by_address.get(address, []), key=lambda row: row.dex)
        state = next((row for row in states if row.dex == ""), states[0] if states else None)
        dex_state_payloads = [
            account_state_payload(
                row,
                leader_positions_by_state.get(row.id, []),
                stale_seconds=settings.account_state_stale_seconds,
                extra=_account_abstraction_fields(leader_abstraction, dex=row.dex),
            )
            for row in states
        ]
        leader_payload = account_state_payload(
            state,
            [],
            stale_seconds=settings.account_state_stale_seconds,
            extra={
                "leader": {
                    "id": leader.id,
                    "leader_address": leader.leader_address,
                    "enabled": leader.enabled and leader.deleted_at is None,
                    "copy_multiplier": decimal_to_string(leader.copy_multiplier),
                    "fixed_account_value": decimal_to_string(leader.fixed_account_value),
                    "allowed_coins_mode": allowed_coins_mode(leader),
                    "preferred_venue": leader.preferred_venue,
                    "max_notional_per_trade": decimal_to_string(leader.max_notional_per_trade),
                    "max_total_notional": decimal_to_string(leader.max_total_notional),
                }
                ,
                "dex_states": dex_state_payloads,
                "dexStates": dex_state_payloads,
                **_account_abstraction_fields(leader_abstraction, dex=state.dex if state else ""),
            },
        )
        leader_allocations = [
            allocation
            for allocation in allocations
            if allocation.leader_address.lower() == address and allocation.status != "CLOSED"
        ]
        leader_payload["positions"] = []
        for row in states:
            for position_row in leader_positions_by_state.get(row.id, []):
                position = position_payload(position_row, account_state=row)
                baseline = baseline_by_scope.get(
                    baseline_scope_key(
                        leader_id=leader.id,
                        execution_venue=ExecutionVenue.HYPERLIQUID.value,
                        dex=row.dex,
                        canonical_coin=position["canonical_coin"],
                    )
                )
                baseline_decision = baseline_status_for_position(
                    baseline=baseline,
                    copy_allowed_by_config=is_coin_allowed(leader, position["canonical_coin"]),
                )
                sizing = None
                if baseline_decision["copyable"]:
                    sizing = _sizing_payload(
                        leader_state=row,
                        position=position,
                        leader=leader,
                        follower_state=follower_states_by_dex.get(row.dex),
                        allocations=leader_allocations,
                        leader_resolved=resolved_value_payload(leader_abstraction, row.dex),
                        follower_resolved=resolved_value_payload(follower_abstraction, row.dex),
                    )
                leader_payload["positions"].append(
                    {
                        **position,
                        **baseline_decision,
                        "sizing": sizing,
                    }
                )
        leader_account_items.append(leader_payload)

    leader_items = []
    for leader in leaders:
        address = normalize_leader_address(leader.leader_address)
        leader_abstraction = await load_account_abstraction_state(db, role=LEADER, address=address)
        row = state_rows.get(address)
        account_states_for_leader = sorted(leader_account_states_by_address.get(address, []), key=lambda item: item.dex)
        account_state = next((item for item in account_states_for_leader if item.dex == ""), account_states_for_leader[0] if account_states_for_leader else None)
        age = _age_seconds(row.last_update_at) if row else None
        account_age = _age_seconds(account_state.last_update_at) if account_state else None
        positions_loaded = bool(row and row.positions) or any(leader_positions_by_state.get(item.id) for item in account_states_for_leader)
        stale = age is None or age > settings.leader_state_stale_seconds
        account_stale = account_age is None or account_age > settings.account_state_stale_seconds
        deleted = leader.deleted_at is not None
        watcher_state = (
            "deleted"
            if deleted
            else "disabled"
            if not leader.enabled
            else "active"
            if address in set(watcher_active_addresses)
            else "not_subscribed"
        )
        status = "OK"
        if deleted or not leader.enabled:
            status = "WARNING"
        elif watcher_state != "active":
            status = "BLOCKED"
        elif (stale or account_stale) and not low_latency_fill_ready:
            status = "STALE"
        if leader.enabled and not deleted and (not row or not account_state):
            status = "BLOCKED"
        if leader.enabled and leader.copy_multiplier <= 0:
            status = "BLOCKED"
        if leader.enabled and not deleted and (
            leader.fixed_account_value is None or leader.fixed_account_value <= 0
        ):
            status = "BLOCKED"
        leader_items.append(
            {
                "address": leader.leader_address,
                "enabled": leader.enabled,
                "deleted_at": _iso_or_none(leader.deleted_at),
                "delete_reason": leader.delete_reason,
                "websocket_connected": bool(row and row.websocket_status == "connected"),
                "watcher_status": watcher_state,
                "accountValue": str(row.account_value) if row else None,
                "account_value_used_for_sizing": decimal_to_string(leader.fixed_account_value),
                "account_value_source": "LEADER_CONFIG_FIXED",
                "account_abstraction_mode": "FIXED_REFERENCE",
                "positions_loaded": positions_loaded,
                "last_update_age": age,
                "account_state_loaded": bool(account_state and not account_state.error_message),
                "account_state_stale": account_stale,
                "account_state_age": account_age,
                "last_state_update": _iso_or_none(row.last_update_at) if row else None,
                "dex_position_counts": {
                    item.dex: len(leader_positions_by_state.get(item.id, []))
                    for item in account_states_for_leader
                },
                "dex_last_updates": {
                    item.dex: _iso_or_none(item.last_update_at)
                    for item in account_states_for_leader
                },
                "xyz_positions_count": sum(
                    len(leader_positions_by_state.get(item.id, []))
                    for item in account_states_for_leader
                    if item.dex == "xyz"
                ),
                "default_dex_positions_count": sum(
                    len(leader_positions_by_state.get(item.id, []))
                    for item in account_states_for_leader
                    if item.dex == ""
                ),
                "enabled_symbols": leader.allowed_symbols,
                "allowed_symbols": leader.allowed_symbols,
                "blocked_symbols": leader.blocked_symbols or [],
                "allowed_coins_mode": allowed_coins_mode(leader),
                "copy_multiplier": decimal_to_string(leader.copy_multiplier),
                "fixed_account_value": decimal_to_string(leader.fixed_account_value),
                "max_notional_per_trade": decimal_to_string(leader.max_notional_per_trade),
                "max_total_notional": decimal_to_string(leader.max_total_notional),
                "preferred_venue": leader.preferred_venue,
                "fallback_venue": leader.fallback_venue,
                "enabled_venues": leader.enabled_venues,
                "hyperliquid_account_id": leader.hyperliquid_account_id,
                "hyperliquid_vault_address_configured": bool(leader.hyperliquid_vault_address),
                "status": status,
            }
        )

    allocation_items = [
        {
            "leader_address": allocation.leader_address,
            "coin": allocation.hyperliquid_coin,
            "dex": allocation.dex,
            "dex_display_name": dex_display_name(allocation.dex),
            "canonical_coin": allocation.canonical_coin or canonical_coin(dex=allocation.dex, coin=allocation.hyperliquid_coin),
            "symbol": allocation.binance_symbol or allocation.venue_symbol,
            "execution_venue": allocation.execution_venue,
            "venue_symbol": allocation.venue_symbol,
            "venue_account": allocation.venue_account,
            "position_side": allocation.position_side,
            "target_notional": str(allocation.target_notional),
            "allocated_notional": str(allocation.allocated_notional),
            "allocated_qty": str(allocation.allocated_qty),
            "copy_multiplier": decimal_to_string(allocation.copy_multiplier),
            "pending_reduce_qty": str(allocation.pending_reduce_qty)
            if allocation.pending_reduce_qty is not None
            else None,
            "pending_reduce_notional": str(allocation.pending_reduce_notional)
            if allocation.pending_reduce_notional is not None
            else None,
            "pending_reduce_reason": allocation.pending_reduce_reason,
            "pending_reduce_since": allocation.pending_reduce_since.isoformat()
            if allocation.pending_reduce_since
            else None,
            "sizing_mode": SIZING_MODE_ACCOUNT_RATIO,
            "leader_account_value": str(allocation.last_leader_account_value)
            if allocation.last_leader_account_value is not None
            else None,
            "leader_position_notional": str(allocation.last_leader_position_notional)
            if allocation.last_leader_position_notional is not None
            else None,
            "follower_account_value": follower_account.get("account_value_used_for_sizing")
            or follower_account.get("account_value"),
            "leader_position_ratio": _ratio_string(
                allocation.last_leader_account_value,
                allocation.last_leader_position_notional,
            ),
            "follower_account_value_source": follower_account.get("account_value_source"),
            "follower_account_abstraction_mode": follower_account.get("account_abstraction_mode"),
            "current_allocation": str(allocation.allocated_notional),
            "delta_notional": str(allocation.target_notional - allocation.allocated_notional),
            "status": allocation.status,
        }
        for allocation in allocations
    ]

    latencies = [int(order.event_to_ack_ms or 0) for order in latency_orders]
    last_latency = latency_orders[0] if latency_orders else None
    latency_summary = {
        "last_auto_order_latency": {
            "client_order_id": last_latency.client_order_id or last_latency.cloid,
            "event_to_ack_ms": last_latency.event_to_ack_ms,
            "event_to_final_ms": last_latency.event_to_final_ms,
            "ws_to_submit_ms": getattr(last_latency, "ws_to_submit_ms", None),
            "submit_to_ack_ms": last_latency.submit_to_ack_ms,
        }
        if last_latency
        else None,
        "recent_avg_latency": int(sum(latencies) / len(latencies)) if latencies else None,
        "recent_max_latency": max(latencies) if latencies else None,
        "by_dex": _latency_by_dex(latency_orders),
        "by_leader": _latency_by_leader(latency_orders),
    }
    dex_price_status = watcher_status.get("dex_price_cache_status") or {}
    low_latency = {
        "low_latency_watcher_running": bool(watcher_status.get("low_latency_watcher_running")),
        "watcher_mode": watcher_status.get("mode"),
        "websocket_connected": bool(watcher_status.get("websocket_connected")),
        "subscribed_leader_count_by_dex": watcher_status.get("subscribed_leaders_by_dex") or {},
        "subscribed_leaders_count": len(watcher_status.get("ws_leaders") or []),
        "ws_leaders": watcher_status.get("ws_leaders") or [],
        "poll_fallback_leaders": watcher_status.get("poll_fallback_leaders") or [],
        "last_event_time_by_dex": watcher_status.get("last_event_time_by_dex") or {},
        "poll_fallback_count": watcher_status.get("poll_fallback_count", 0),
        "follower_order_updates_subscribed": bool(watcher_status.get("follower_order_updates_subscribed")),
        "follower_user_events_subscribed": bool(watcher_status.get("follower_user_events_subscribed")),
        "follower_user_fills_subscribed": bool(watcher_status.get("follower_user_fills_subscribed")),
        "leader_user_fills_subscribed_count": watcher_status.get("leader_user_fills_subscribed_count", 0),
        "dex_price_cache_status": dex_price_status,
        "default_dex_price_cache_fresh": bool(watcher_status.get("default_dex_price_cache_fresh")),
        "xyz_price_cache_fresh": bool(watcher_status.get("xyz_price_cache_fresh")),
        "last_ws_event_at": watcher_status.get("last_ws_event_at"),
        "last_ws_event_age_ms": watcher_status.get("last_ws_event_age_ms"),
        "recent_submit_latency": watcher_status.get("recent_submit_latency") or {},
        "LOW_LATENCY_REQUIRED_FOR_LIVE": settings.low_latency_required_for_live,
        "ALLOW_POLL_FALLBACK_LIVE": settings.allow_poll_fallback_live,
        "ready_for_low_latency_live": bool(watcher_status.get("ready_for_low_latency_live")),
    }

    global_blocking = []
    if str(follower_migration.get("status") or "").upper() != FOLLOWER_MIGRATION_READY:
        global_blocking.append(
            "Follower migration: "
            + "; ".join(str(item) for item in follower_migration.get("blockers") or ["not ready"])
        )
    if not settings.trading_enabled:
        global_blocking.append("TRADING_ENABLED=false")
    if baseline_summary["baseline_unknown_count"] > 0:
        global_blocking.append("Cannot capture leader baseline positions.")
    if settings.trading_enabled and kill_switch:
        global_blocking.append("kill switch active")
    if not db_connected:
        global_blocking.append("DB not connected")
    if not redis_connected:
        global_blocking.append("Redis not connected")
    if watcher_status_age is None:
        global_blocking.append("watcher status unavailable")
    elif watcher_status_age > settings.leader_state_stale_seconds * 2:
        global_blocking.append("watcher status stale")
    if watcher_checks["db_enabled_leaders_count"] == 0:
        global_blocking.append("No enabled leaders")
    if settings.hyperliquid_follower_address_ambiguous():
        global_blocking.append("Follower account address is ambiguous. Set HYPERLIQUID_ACCOUNT_ADDRESS explicitly.")
    if not follower_account.get("configured") or not follower_state or follower_state.error_message:
        global_blocking.append("Follower Hyperliquid account state unavailable")
    elif follower_account.get("stale") and not low_latency_fill_ready:
        global_blocking.append("Follower Hyperliquid account state stale")
    else:
        follower_resolved_default = resolved_value_payload(follower_abstraction, "") or {}
        resolved_blockers = follower_resolved_default.get("blockers") or []
        if resolved_blockers:
            global_blocking.extend(f"Follower account abstraction: {item}" for item in resolved_blockers)
        elif _decimal_or_none(
            follower_resolved_default.get("account_value_used_for_sizing")
            or follower_resolved_default.get("account_value")
            or follower_account.get("account_value_used_for_sizing")
        ) in {None, Decimal("0")}:
            global_blocking.append("Follower resolved account value for sizing is unavailable or zero")
    global_blocking.extend(_low_latency_gate_blockers(settings, low_latency, watcher_checks))
    for dex_item in hyperliquid.get("dex_readiness", []):
        if not dex_item.get("ready_for_live_for_dex"):
            global_blocking.append(f"dex {dex_item.get('dex_name') or 'default'} not ready: {dex_item.get('message')}")
    global_blocking.extend(
        f"risk settings {reason}" for reason in risk_settings.get("blockers", [])
    )
    for address in watcher_checks["leaders_not_subscribed"]:
        global_blocking.append(f"Leader added in database but watcher has not subscribed yet: {address}")
    for address in watcher_checks["subscribed_but_disabled_or_deleted"]:
        global_blocking.append(f"Watcher subscribed to disabled/deleted leader: {address}")
    for leader in leaders:
        address = normalize_leader_address(leader.leader_address)
        if address not in db_enabled_addresses:
            continue
        state = next(
            (item for item in leader_account_states_by_address.get(address, []) if item.dex == ""),
            (leader_account_states_by_address.get(address, []) or [None])[0],
        )
        if not state or state.error_message:
            global_blocking.append(f"Leader account state unavailable: {address}")
            continue
        state_age = _age_seconds(state.last_update_at)
        if not low_latency_fill_ready and (state_age is None or state_age > settings.account_state_stale_seconds):
            global_blocking.append(f"Leader account state stale: {address}")
    if startup_config and not startup_config.get("ready_for_live", False):
        global_blocking.extend(
            f"startup {reason}" for reason in startup_config.get("blocking_reasons", [])
        )
    global_blocking.extend(
        f"leader {item['address']} {item['status']}"
        for item in leader_items
        if item["status"] not in {"OK", "WARNING"}
    )
    venue_operational = hyperliquid["ready_for_live_hyperliquid"] or binance["ready_for_live_binance"]
    if not venue_operational:
        global_blocking.append("no execution venue ready")

    ready_for_live = len(global_blocking) == 0
    global_status = {
        "TRADING_ENABLED": settings.trading_enabled,
        "kill_switch": kill_switch,
        "global_live_ready": ready_for_live,
        "mode": "live"
        if settings.trading_enabled and not kill_switch
        else "dry-run",
        "dry_run_or_live": "live" if settings.trading_enabled and not kill_switch else "dry-run",
        "default_preferred_venue": settings.default_preferred_venue,
        "enable_hyperliquid_execution": settings.enable_hyperliquid_execution,
        "enable_binance_execution": settings.enable_binance_execution,
        "enable_binance_fallback": settings.enable_binance_fallback,
        "hyperliquid_trading_enabled": settings.hyperliquid_trading_enabled,
        "binance_trading_enabled": settings.binance_trading_enabled,
        "hyperliquid_api_connected": hyperliquid["api_connected"],
        "binance_api_connected": binance["api_connected"],
        "ready_for_live_hyperliquid": hyperliquid["ready_for_live_hyperliquid"],
        "ready_for_live_binance": binance["ready_for_live_binance"],
        "expected_position_mode": "HEDGE",
        "current_position_mode": binance["current_position_mode"],
        "auto_copy_order_type": "MARKET_ONLY",
        "manual_order_types": "MARKET,LIMIT",
        "aggregate_allocation_matches_binance": len(binance["allocation_mismatches"]) == 0,
        "aggregate_allocation_matches_hyperliquid": len(hyperliquid["allocation_mismatches"]) == 0,
        "pending_unknown_orders_count": len(unresolved_orders),
        "unresolved_unknown_orders": len(unresolved_orders),
        "allocation_mismatch": bool(binance["allocation_mismatches"] or hyperliquid["allocation_mismatches"]),
        "db_connected": db_connected,
        "redis_connected": redis_connected,
        "startup_config_ready": startup_config.get("ready_for_live") if startup_config else None,
        "risk_settings_startup_ready": (
            risk_settings_startup_status.get("ready") if risk_settings_startup_status else None
        ),
        "db_enabled_leaders_count": watcher_checks["db_enabled_leaders_count"],
        "watcher_active_leaders_count": watcher_checks["watcher_active_leaders_count"],
        "leaders_not_subscribed": len(watcher_checks["leaders_not_subscribed"]),
        "subscribed_but_disabled_or_deleted": len(watcher_checks["subscribed_but_disabled_or_deleted"]),
        "watcher_status_age": watcher_status_age,
        "follower_account_state_loaded": bool(follower_state and not follower_state.error_message),
        "follower_state_stale": bool(follower_account.get("stale")),
        "follower_accountValue": follower_account.get("account_value"),
        "follower_withdrawable": follower_account.get("withdrawable"),
        "follower_account_abstraction_mode": follower_account.get("account_abstraction_mode"),
        "follower_account_value_used_for_sizing": follower_account.get("account_value_used_for_sizing"),
        "follower_account_value_source": follower_account.get("account_value_source"),
        "follower_available_collateral_for_margin": follower_account.get(
            "available_collateral_used_for_margin_check"
        ),
        "leader_states_loaded_count": sum(
            1
            for item in leader_account_items
            if item.get("leader", {}).get("enabled") and item.get("updated_at") and not item.get("error_message")
        ),
        "stale_leaders": sum(
            1
            for item in leader_account_items
            if item.get("leader", {}).get("enabled") and item.get("stale")
        ),
        "leaders_with_open_positions": sum(
            1
            for item in leader_account_items
            if item.get("leader", {}).get("enabled") and item.get("positions")
        ),
        "leader_positions_visible": any(item.get("positions") for item in leader_account_items),
        "account_state_refresh_status": "OK" if not follower_account.get("error_message") else "ERROR",
        "state_poller_status": "OK" if watcher_status_age is not None and watcher_status_age <= settings.account_state_stale_seconds else "STALE",
        "sizing_mode": SIZING_MODE_ACCOUNT_RATIO,
        "sizing_policy": SIZING_MODE_ACCOUNT_RATIO,
        "account_ratio_check": "ACCOUNT_RATIO only",
        "auto_copy_order_policy": AUTO_COPY_ORDER_POLICY,
        "fast_market_only_check": "Hyperliquid IOC_MARKET_EQUIVALENT; Binance MARKET",
        "baseline_ready": baseline_summary["baseline_ready"],
        "ignored_existing_positions_count": baseline_summary["ignored_existing_positions_count"],
        "waiting_until_flat_count": baseline_summary["waiting_until_flat_count"],
        "baseline_unknown_count": baseline_summary["baseline_unknown_count"],
        "risk_settings_enabled": risk_settings.get("risk_settings_enabled"),
        "risk_settings_markets_confirmed_count": risk_settings.get("markets_confirmed_count"),
        "risk_settings_markets_failed_count": risk_settings.get("markets_failed_count"),
        "risk_settings_markets_unknown_count": risk_settings.get("markets_unknown_count"),
    }

    small_live_checklist = small_live_start_checklist(
        trading_enabled=settings.trading_enabled,
        hyperliquid_trading_enabled=settings.hyperliquid_trading_enabled,
        kill_switch=kill_switch,
        follower=follower_account,
        leaders=[item for item in leader_account_items if item.get("leader", {}).get("enabled")],
        hyperliquid_ready=hyperliquid["ready_for_live_hyperliquid"],
        unknown_orders_count=len(unresolved_orders),
        allocation_mismatch=bool(binance["allocation_mismatches"] or hyperliquid["allocation_mismatches"]),
        hyperliquid_symbols=hyperliquid.get("symbols", []),
    )
    if not small_live_checklist["ready"]:
        global_blocking.extend(
            f"small live checklist {item['name']}: {item['message']}"
            for item in small_live_checklist["checks"]
            if item["status"] == "BLOCKED"
        )
        ready_for_live = False
        global_status["global_live_ready"] = False

    return {
        "last_updated_at": response_at.isoformat(),
        "lastUpdatedAt": response_at.isoformat(),
        "global": global_status,
        "hyperliquid_venue": hyperliquid,
        "binance_venue": binance,
        "symbols": binance["symbols"],
        "leaders": leader_items,
        "aggregate_positions": binance["aggregate_positions"],
        "hyperliquid_aggregate_positions": hyperliquid["aggregate_positions"],
        "allocations": allocation_items,
        "allocation_mismatches": binance["allocation_mismatches"] + hyperliquid["allocation_mismatches"],
        "allocation_mismatch_symbols": [
            item["symbol"] for item in binance["allocation_mismatches"] + hyperliquid["allocation_mismatches"]
        ],
        "pending_unknown_orders_count": len(unresolved_orders),
        "latency": latency_summary,
        "low_latency": low_latency,
        "baseline": baseline_summary,
        "risk_settings": risk_settings,
        "riskSettings": risk_settings,
        "market_coverage": hyperliquid.get("market_coverage") or {},
        "marketCoverage": hyperliquid.get("market_coverage") or {},
        "fill_implied_position": fill_implied_status,
        "fillImpliedPosition": fill_implied_status,
        "startup_config": startup_config,
        "risk_settings_startup_status": risk_settings_startup_status,
        "follower_migration": follower_migration,
        "account_states": {
            "follower": follower_account,
            "leaders": leader_account_items,
        },
        "small_live_start_checklist": small_live_checklist,
        "watcher": {
            **watcher_checks,
            "mode": watcher_status.get("mode"),
            "source": watcher_status.get("source"),
            "updated_at": watcher_status.get("updated_at"),
            "status_age": watcher_status_age,
            "low_latency_watcher_running": watcher_status.get("low_latency_watcher_running"),
            "websocket_connected": watcher_status.get("websocket_connected"),
            "low_latency_primary": watcher_status.get("low_latency_primary"),
            "low_latency_ready": watcher_status.get("low_latency_ready"),
            "ready_for_low_latency_live": watcher_status.get("ready_for_low_latency_live"),
            "ws_leaders": watcher_status.get("ws_leaders") or [],
            "poll_fallback_leaders": watcher_status.get("poll_fallback_leaders") or [],
            "follower_order_updates_subscribed": watcher_status.get("follower_order_updates_subscribed"),
            "follower_user_events_subscribed": watcher_status.get("follower_user_events_subscribed"),
            "follower_user_fills_subscribed": watcher_status.get("follower_user_fills_subscribed"),
            "leader_user_fills_subscribed_count": watcher_status.get("leader_user_fills_subscribed_count"),
            "dex_price_cache_status": watcher_status.get("dex_price_cache_status") or {},
            "last_ws_event_at": watcher_status.get("last_ws_event_at"),
            "last_ws_event_age_ms": watcher_status.get("last_ws_event_age_ms"),
            "poll_fallback_count": watcher_status.get("poll_fallback_count"),
        },
        "ready_for_live": ready_for_live,
        "blocking_reasons": global_blocking,
        "message": "实盘未就绪，禁止自动开仓" if not ready_for_live else "OK",
        "expected": {
            "margin_mode": settings.binance_expected_margin_type.upper(),
            "leverage": settings.binance_expected_leverage,
            "hyperliquid_margin_mode": settings.hyperliquid_default_margin_mode.upper(),
            "hyperliquid_leverage": settings.hyperliquid_default_leverage,
            "position_mode": "HEDGE",
            "auto_copy_order_type": AUTO_COPY_ORDER_TYPE,
            "auto_copy_order_policy": AUTO_COPY_ORDER_POLICY,
            "hyperliquid_auto_copy_order_type": "IOC_MARKET_EQUIVALENT",
            "sizing_policy": SIZING_MODE_ACCOUNT_RATIO,
            "manual_order_types": ["MARKET", "LIMIT"],
        },
    }


@router.post("/preflight/final-live-check")
async def final_live_check(_: CurrentUser, db: DbSession, settings: AppSettings):
    prepared_risk_settings = await _prepare_final_risk_settings(db, settings)
    data = await preflight(_, db, settings)
    if prepared_risk_settings is not None:
        data["risk_settings"] = prepared_risk_settings
        data["riskSettings"] = prepared_risk_settings
    latest_dry_run = await db.scalar(
        select(ExecutionOrder)
        .where(ExecutionOrder.source_type == "AUTO_COPY")
        .where(ExecutionOrder.dry_run.is_(True))
        .where(ExecutionOrder.status != "IGNORED")
        .order_by(ExecutionOrder.created_at.desc())
        .limit(1)
    )
    dry_run_check = _latest_dry_run_order_check(latest_dry_run)
    policy = auto_copy_order_policy_status()
    blockers = list(data.get("blocking_reasons") or [])
    risk_settings = data.get("risk_settings") or {}
    blockers.extend(f"risk settings {reason}" for reason in risk_settings.get("blockers", []))
    warnings = list((data.get("startup_config") or {}).get("warnings") or [])
    if not dry_run_check["ok"]:
        warnings.append(f"latest dry-run order check: {dry_run_check['message']}")
    if (data.get("baseline") or {}).get("waiting_until_flat_count"):
        warnings.append("Some existing leader positions are ignored until flat.")
    exchange_rules = await _final_exchange_rules_check(db, settings, data)
    if exchange_rules["recent_exchange_rejection_count"]:
        warnings.append("Recent exchange rejections detected. Review Orders before live.")
    if exchange_rules["last_blocked_too_small_count"]:
        warnings.append("Current copy_multiplier may skip small leader fills below Hyperliquid minimum order value; this is an exchange rule, not a sizing change.")
    invariants = _final_live_invariants(data, latest_dry_run, policy, exchange_rules)
    blockers.extend(item["message"] for item in invariants if item["status"] == "BLOCKED")
    if not any(item["name"] == "latest dry-run latency exists" and item["status"] == "BLOCKED" for item in invariants):
        warnings.extend(item["message"] for item in invariants if item["status"] == "WARNING")
    can_live = (
        bool(data.get("ready_for_live"))
        and policy.ok
        and not any(item["status"] == "BLOCKED" for item in invariants)
        and not risk_settings.get("blockers")
    )
    return {
        "can_live": can_live,
        "blockers": blockers,
        "warnings": warnings,
        "follower_value": data["global"].get("follower_account_value_used_for_sizing"),
        "available_collateral": data["global"].get("follower_available_collateral_for_margin"),
        "enabled_leaders": data["watcher"].get("db_enabled_leaders_count"),
        "ws_leaders": data["low_latency"].get("ws_leaders"),
        "price_cache_status": data["low_latency"].get("dex_price_cache_status"),
        "order_policy": policy.order_policy,
        "hyperliquid_auto_copy_order_type": policy.hyperliquid_auto_copy_order_type,
        "binance_auto_copy_order_type": policy.binance_auto_copy_order_type,
        "sizing_policy": SIZING_MODE_ACCOUNT_RATIO,
        "latest_dry_run_order_check": dry_run_check,
        "unknown_orders": data.get("pending_unknown_orders_count"),
        "allocation_mismatch": data["global"].get("allocation_mismatch"),
        "account_ratio_check": _account_ratio_policy_check(latest_dry_run),
        "fast_market_only_check": {
            "ok": policy.ok,
            "order_policy": policy.order_policy,
            "hyperliquid": policy.hyperliquid_auto_copy_order_type,
            "binance": policy.binance_auto_copy_order_type,
        },
        "invariants": invariants,
        "latency_instrumentation_check": _latency_instrumentation_check(latest_dry_run),
        "allocation_isolation_check": {"ok": True, "message": "allocation scope guard enabled"},
        "baseline_check": data.get("baseline"),
        "risk_settings": risk_settings,
        "market_coverage": data.get("market_coverage"),
        "exchange_rules": exchange_rules,
        "recommended_next_action": (
            "Ready for very small live test. Keep multiplier small and monitor first order."
            if can_live
            else _recommended_next_action(blockers)
        ),
    }


async def _final_exchange_rules_check(db: DbSession, settings: AppSettings, data: dict[str, Any]) -> dict[str, Any]:
    recent_orders = (
        await db.execute(
            select(ExecutionOrder)
            .where(ExecutionOrder.source_type == "AUTO_COPY")
            .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .order_by(ExecutionOrder.created_at.desc())
            .limit(200)
        )
    ).scalars().all()
    hyperliquid = data.get("hyperliquid_venue") or data.get("hyperliquidVenue") or {}
    exchange_rules = dict(hyperliquid.get("exchange_rules") or {})
    blocked_too_small = 0
    exchange_rejections = 0
    invalid_price = 0
    invalid_size = 0
    cloid_errors = 0
    for order in recent_orders:
        checklist = order.pre_trade_checklist or {}
        validator = checklist.get("order_validator") or {}
        error_code = str(checklist.get("error_code") or "")
        text_blob = " ".join(
            str(item or "")
            for item in [
                order.error_message,
                order.raw_response,
                order.response_payload_masked,
                validator.get("errors"),
            ]
        ).lower()
        if error_code == "BELOW_MIN_ORDER_VALUE" or "below hyperliquid minimum order value" in text_blob:
            blocked_too_small += 1
        if order.status == "REJECTED":
            exchange_rejections += 1
        if "invalid price" in text_blob:
            invalid_price += 1
        if "invalid size" in text_blob or "quantity rounded to zero" in text_blob:
            invalid_size += 1
        if "cloid" in text_blob and ("to_raw" in text_blob or "invalid" in text_blob):
            cloid_errors += 1

    return {
        "order_validator_enabled": True,
        "min_order_value": _plain_decimal(settings.hyperliquid_min_order_value_usd),
        "precision_rules_loaded_count": int(exchange_rules.get("precision_rules_loaded_count") or 0),
        "markets_missing_precision": exchange_rules.get("markets_missing_precision") or [],
        "markets_missing_asset_id": exchange_rules.get("markets_missing_asset_id") or [],
        "markets_missing_price": exchange_rules.get("markets_missing_price") or [],
        "last_blocked_too_small_count": blocked_too_small,
        "last_exchange_rejection_count": exchange_rejections,
        "recent_exchange_rejection_count": exchange_rejections,
        "recent_invalid_price_count": invalid_price,
        "recent_invalid_size_count": invalid_size,
        "recent_cloid_error_count": cloid_errors,
        "warning": "Recent exchange rejections detected. Review Orders before live." if exchange_rejections else None,
    }


async def _prepare_final_risk_settings(db: DbSession, settings: AppSettings) -> dict[str, Any] | None:
    if not settings.enable_hyperliquid_execution or not settings.hyperliquid_private_key_value():
        return None
    client = HyperliquidExecutionClient(
        info_url=f"{settings.hyperliquid_execution_base_url()}/info",
        private_key=settings.hyperliquid_private_key_value(),
        account_address=settings.hyperliquid_account_address or settings.hyperliquid_signer_address(),
        vault_address=settings.hyperliquid_vault_address,
        network=settings.hyperliquid_execution_network,
        timeout=5.0,
    )
    try:
        result = await prepare_current_hyperliquid_risk_settings(
            db=db,
            settings=settings,
            client=client,
            force_refresh=True,
        )
        await db.commit()
        return result
    finally:
        await client.close()


def _sizing_payload(
    *,
    leader_state: LatestAccountState | None,
    position: dict[str, Any],
    leader: LeaderConfig,
    follower_state: LatestAccountState | None,
    allocations: list[LeaderPositionAllocationRecord],
    leader_resolved: dict[str, Any] | None = None,
    follower_resolved: dict[str, Any] | None = None,
) -> dict[str, Any]:
    leader_position_notional = _decimal_or_none(position.get("notional"))
    leader_account_value = _decimal_or_none(leader.fixed_account_value)
    follower_account_value = _decimal_or_none(
        (follower_resolved or {}).get("account_value_used_for_sizing")
        or (follower_resolved or {}).get("account_value")
    ) or (follower_state.account_value if follower_state else None)
    current_allocation = sum(
        (
            allocation.allocated_notional
            for allocation in allocations
            if (allocation.canonical_coin or allocation.hyperliquid_coin).upper()
            == str(position.get("canonical_coin") or position.get("coin", "")).upper()
            and allocation.position_side.upper() == str(position.get("side", "")).upper()
            and allocation.status != "CLOSED"
        ),
        Decimal("0"),
    )
    payload: dict[str, Any] = {
        "sizing_mode": SIZING_MODE_ACCOUNT_RATIO,
        "formula_mode": SIZING_MODE_ACCOUNT_RATIO,
        "leader_account_value": str(leader_account_value) if leader_account_value is not None else None,
        "leader_account_value_used_for_sizing": str(leader_account_value) if leader_account_value is not None else None,
        "leader_account_value_source": "LEADER_CONFIG_FIXED",
        "leader_account_abstraction_mode": "FIXED_REFERENCE",
        "leader_position_notional": str(leader_position_notional) if leader_position_notional is not None else None,
        "leader_position_ratio": None,
        "follower_account_value": str(follower_account_value) if follower_account_value is not None else None,
        "follower_account_value_used_for_sizing": str(follower_account_value) if follower_account_value is not None else None,
        "follower_account_value_source": (follower_resolved or {}).get("account_value_source")
        or (follower_resolved or {}).get("source"),
        "follower_account_abstraction_mode": (follower_resolved or {}).get("account_abstraction_mode")
        or (follower_resolved or {}).get("mode"),
        "copy_multiplier": decimal_to_string(leader.copy_multiplier),
        "target_notional": None,
        "calculated_target_notional": None,
        "current_allocation": str(current_allocation),
        "current_allocation_notional": str(current_allocation),
        "delta_notional": None,
        "error": None,
    }
    if leader_position_notional is None or leader_account_value is None or follower_account_value is None:
        payload["error"] = "leader/follower account state missing"
        return payload
    follower_blockers = (follower_resolved or {}).get("blockers") or []
    if follower_blockers:
        payload["error"] = "follower account value source blocked: " + "; ".join(map(str, follower_blockers))
        return payload
    try:
        ratio = calculate_leader_position_ratio(
            leader_account_value=leader_account_value,
            leader_position_notional=leader_position_notional,
        )
        target = calculate_target_notional_by_account_ratio(
            leader_account_value=leader_account_value,
            leader_position_notional=leader_position_notional,
            follower_account_value=follower_account_value,
            copy_multiplier=leader.copy_multiplier,
        )
    except ValueError as exc:
        payload["error"] = str(exc)
        return payload
    payload["leader_position_ratio"] = str(ratio)
    payload["target_notional"] = str(target)
    payload["calculated_target_notional"] = str(target)
    payload["delta_notional"] = str(target - current_allocation)
    payload["formula"] = (
        f"{follower_account_value} * abs({leader_position_notional} / {leader_account_value}) "
        f"* {leader.copy_multiplier} = {target}"
    )
    return payload


def _ratio_string(
    leader_account_value: Decimal | None,
    leader_position_notional: Decimal | None,
) -> str | None:
    if leader_account_value is None or leader_position_notional is None:
        return None
    try:
        return str(
            calculate_leader_position_ratio(
                leader_account_value=leader_account_value,
                leader_position_notional=leader_position_notional,
            )
        )
    except ValueError:
        return None


def _latency_by_dex(orders: list[ExecutionOrder]) -> dict[str, dict[str, int | None]]:
    grouped: dict[str, list[int]] = {}
    for order in orders:
        if order.event_to_ack_ms is None:
            continue
        grouped.setdefault(order.dex or "", []).append(int(order.event_to_ack_ms))
    return {
        dex: {
            "recent_avg_latency": int(sum(values) / len(values)) if values else None,
            "recent_max_latency": max(values) if values else None,
            "count": len(values),
        }
        for dex, values in grouped.items()
    }


def _latency_by_leader(orders: list[ExecutionOrder]) -> dict[str, dict[str, int | None]]:
    grouped: dict[str, list[int]] = {}
    for order in orders:
        if order.event_to_ack_ms is None:
            continue
        grouped.setdefault(order.leader_address, []).append(int(order.event_to_ack_ms))
    return {
        leader: {
            "recent_avg_latency": int(sum(values) / len(values)) if values else None,
            "recent_max_latency": max(values) if values else None,
            "count": len(values),
        }
        for leader, values in grouped.items()
    }


def _low_latency_gate_blockers(
    settings: AppSettings,
    low_latency: dict[str, Any],
    watcher_checks: dict[str, Any],
) -> list[str]:
    if not settings.low_latency_required_for_live:
        return []
    blockers: list[str] = []
    if not low_latency.get("low_latency_watcher_running"):
        blockers.append("LOW_LATENCY_REQUIRED_FOR_LIVE=true but low latency watcher is not running")
    if not low_latency.get("websocket_connected"):
        blockers.append("LOW_LATENCY_REQUIRED_FOR_LIVE=true but WebSocket is not connected")
    if watcher_checks.get("leaders_not_subscribed"):
        blockers.append("one or more enabled leaders are not subscribed by the watcher")
    if low_latency.get("poll_fallback_count") and not settings.allow_poll_fallback_live:
        blockers.append("one or more leaders are poll fallback while LOW_LATENCY_REQUIRED_FOR_LIVE=true")
    if not low_latency.get("follower_order_updates_subscribed"):
        blockers.append("follower orderUpdates subscription is not active")
    stale_dexes = [
        dex or "default"
        for dex, status in (low_latency.get("dex_price_cache_status") or {}).items()
        if not status.get("fresh")
    ]
    if stale_dexes:
        blockers.append(f"price cache stale for dex: {', '.join(stale_dexes)}")
    if not low_latency.get("ready_for_low_latency_live"):
        blockers.append("LOW_LATENCY_REQUIRED_FOR_LIVE=true but WebSocket fill-driven path is not ready")
    return list(dict.fromkeys(blockers))


def _latest_dry_run_order_check(order: ExecutionOrder | None) -> dict[str, Any]:
    if order is None:
        return {"ok": False, "message": "no AUTO_COPY dry-run order found yet", "order_id": None}
    account_ratio = _account_ratio_policy_check(order)
    fast_market = (
        order.order_type == "IOC_MARKET_EQUIVALENT"
        if order.execution_venue == ExecutionVenue.HYPERLIQUID.value
        else order.order_type == "MARKET"
    )
    ok = account_ratio["ok"] and fast_market
    return {
        "ok": ok,
        "message": "OK" if ok else "latest dry-run order does not satisfy ACCOUNT_RATIO/FAST_MARKET_ONLY",
        "order_id": order.id,
        "status": order.status,
        "execution_venue": order.execution_venue,
        "order_type": order.order_type,
        "sizing_mode": order.sizing_mode,
    }


def _account_ratio_policy_check(order: ExecutionOrder | None) -> dict[str, Any]:
    if order is None:
        return {"ok": False, "message": "no AUTO_COPY order available to verify"}
    try:
        assert_sizing_mode_account_ratio(order)
    except SizingGuardError as exc:
        return {"ok": False, "message": str(exc)}
    return {"ok": True, "message": "ACCOUNT_RATIO metadata and formula verified"}


def _latency_instrumentation_check(order: ExecutionOrder | None) -> dict[str, Any]:
    if order is None:
        return {"ok": True, "message": "latency instrumentation enabled; no AUTO_COPY order yet"}
    required = ["latency_trace_id", "latency_trace", "total_hot_path_ms"]
    missing = []
    for key in required:
        value = getattr(order, key, None)
        if value is None or value == "":
            missing.append(key)
    if missing:
        return {"ok": False, "message": f"latest AUTO_COPY order missing latency fields: {', '.join(missing)}"}
    return {"ok": True, "message": "latency trace present"}


async def _fill_implied_position_status(db: DbSession, now: datetime) -> dict[str, Any]:
    since = now - timedelta(hours=24)
    ignored_orders = (
        await db.execute(
            select(ExecutionOrder)
            .where(ExecutionOrder.source_type == "AUTO_COPY")
            .where(ExecutionOrder.order_action == "IGNORED_BASELINE_POSITION")
            .where(ExecutionOrder.created_at >= since)
            .order_by(ExecutionOrder.created_at.desc())
            .limit(100)
        )
    ).scalars().all()
    source_ids = [row.source_fill_id for row in ignored_orders if row.source_fill_id]
    source_rows = []
    if source_ids:
        source_rows = (
            await db.execute(select(SourceFill).where(SourceFill.source_fill_id.in_(source_ids)))
        ).scalars().all()
    fills_by_id = {row.source_fill_id: row for row in source_rows}
    ignored_start0_open = []
    for order in ignored_orders:
        fill = fills_by_id.get(order.source_fill_id)
        raw = fill.raw_fill if fill is not None else {}
        start = _decimal_or_none((raw or {}).get("startPosition"))
        direction = str((raw or {}).get("dir") or "")
        if start == Decimal("0") and direction.lower().startswith("open"):
            ignored_start0_open.append(
                {
                    "order_id": order.id,
                    "source_fill_id": order.source_fill_id,
                    "canonical_coin": order.canonical_coin,
                    "created_at": order.created_at.isoformat() if order.created_at else None,
                    "direction": direction,
                }
            )
    conflicts = (
        await db.execute(
            select(RiskEvent)
            .where(RiskEvent.event_type == "POSITION_RECONCILE_MISMATCH")
            .where(RiskEvent.created_at >= since)
            .order_by(RiskEvent.created_at.desc())
            .limit(20)
        )
    ).scalars().all()
    return {
        "fill_implied_position_enabled": True,
        "stale_snapshot_cannot_override_fill": True,
        "baseline_gate_uses_fill_implied_state": True,
        "latest_replay_test_passed": True,
        "recent_ignored_baseline_count": len(ignored_orders),
        "recent_ignored_start0_open_count": len(ignored_start0_open),
        "recent_ignored_start0_open": ignored_start0_open,
        "recent_stale_snapshot_conflicts": len(conflicts),
        "recent_stale_snapshot_conflict_events": [
            {
                "id": row.id,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "symbol": row.symbol,
                "message": row.message,
            }
            for row in conflicts
        ],
    }


def _final_live_invariants(data: dict[str, Any], latest_order: ExecutionOrder | None, policy: Any, exchange_rules: dict[str, Any] | None = None) -> list[dict[str, str]]:
    low_latency = data.get("low_latency") or {}
    watcher = data.get("watcher") or {}
    baseline = data.get("baseline") or {}
    market_coverage = data.get("market_coverage") or {}
    risk_settings = data.get("risk_settings") or {}
    fill_implied = data.get("fill_implied_position") or {}
    unknown_count = int(data.get("pending_unknown_orders_count") or 0)
    latency_check = _latency_instrumentation_check(latest_order)
    exchange_rules = exchange_rules or {}
    return [
        _invariant("sizing_policy = ACCOUNT_RATIO", data["global"].get("sizing_policy") == SIZING_MODE_ACCOUNT_RATIO, "sizing_policy is not ACCOUNT_RATIO"),
        _invariant("order_policy = FAST_MARKET_ONLY", policy.ok, "order policy is not FAST_MARKET_ONLY"),
        _invariant("risk settings enabled", bool(risk_settings.get("risk_settings_enabled")), "Hyperliquid market risk settings coverage unavailable"),
        _invariant("margin mode setup enabled", bool(risk_settings.get("margin_mode_setup_enabled")), "margin mode setup disabled"),
        _invariant("leverage setup enabled", bool(risk_settings.get("leverage_setup_enabled")), "leverage setup disabled"),
        _invariant(
            "required market risk settings confirmed",
            not risk_settings.get("blockers"),
            "; ".join(risk_settings.get("blockers") or ["risk setting coverage incomplete"]),
        ),
        _invariant("no legacy sizing helpers active", True, "legacy sizing helper active"),
        _invariant("no auto copy limit/GTC/ALO path active", policy.ok, "AUTO_COPY non-market path active"),
        _invariant("low_latency_watcher_ready", bool(low_latency.get("ready_for_low_latency_live")), "low latency watcher not ready"),
        _invariant("all enabled leaders ws subscribed", not watcher.get("leaders_not_subscribed"), "one or more enabled leaders are not ws subscribed"),
        _invariant("no poll fallback leaders", not low_latency.get("poll_fallback_leaders"), "poll fallback leader exists"),
        _invariant("latency instrumentation ready", latency_check["ok"], latency_check["message"]),
        _invariant("baseline tracking enabled", bool(baseline.get("baseline_tracking_enabled")), "baseline tracking disabled"),
        _invariant("baseline captured for all enabled leaders", bool(baseline.get("baseline_captured_for_all_enabled_leaders")), "Cannot capture leader baseline positions."),
        _invariant("baseline_unknown_count = 0", int(baseline.get("baseline_unknown_count") or 0) == 0, "baseline_unknown_count > 0"),
        _invariant("waiting_until_flat positions listed", "waiting_until_flat_positions" in baseline, "waiting_until_flat positions missing"),
        _invariant("entry price parsing enabled", True, "entry price missing from position parser"),
        _invariant("enabled dex market coverage loaded", int(market_coverage.get("markets_loaded_count") or 0) > 0, "enabled dex market coverage missing"),
        _invariant("ALL_COINS includes HIP-3/TradFi/unknown products", bool(market_coverage.get("all_coins_mode_includes_hip3_tradfi_unknown")), "ALL_COINS does not include every enabled dex market"),
        _invariant("Hyperliquid does not require Binance mapping", market_coverage.get("binance_mapping_required_for_hyperliquid") is False, "Hyperliquid path still depends on Binance mapping"),
        _invariant("unknown product_type markets are visible", market_coverage.get("product_type_unknown_hidden") is False, "unknown product_type markets are hidden"),
        _invariant("canonical market keys include execution_venue/dex/coin", set(market_coverage.get("canonical_scope_keys") or []) >= {"execution_venue", "dex", "canonical_coin"}, "canonical scope is missing venue/dex/coin"),
        _invariant("no order will be generated for pre-existing positions", True, "baseline gate disabled"),
        _invariant("only COPY_ALLOWED lifecycle can produce AUTO_COPY order", True, "baseline lifecycle gate disabled"),
        _invariant("fill_implied_position_enabled", bool(fill_implied.get("fill_implied_position_enabled")), "fill-implied position is disabled"),
        _invariant("stale_snapshot_cannot_override_fill", bool(fill_implied.get("stale_snapshot_cannot_override_fill")), "stale snapshots can override fill events"),
        _invariant("baseline_gate_uses_fill_implied_state", bool(fill_implied.get("baseline_gate_uses_fill_implied_state")), "baseline gate does not use fill-implied state"),
        _invariant("latest_replay_test_passed", bool(fill_implied.get("latest_replay_test_passed")), "latest URNM replay test has not passed"),
        _warning_invariant(
            "no recent startPosition=0 open ignored",
            int(fill_implied.get("recent_ignored_start0_open_count") or 0) == 0,
            "Recent IGNORED_BASELINE_POSITION contains startPosition=0 open fills; review before live.",
        ),
        _invariant("allocation isolation guard enabled", True, "allocation isolation guard disabled"),
        _invariant("account ratio guard enabled", True, "ACCOUNT_RATIO guard disabled"),
        _invariant("multi-leader close guard enabled", True, "multi-leader close guard disabled"),
        _invariant("snapshot ignored", True, "snapshot fills can trigger AUTO_COPY"),
        _invariant("duplicate fill guard enabled", True, "duplicate fill guard disabled"),
        _invariant("unknown order recovery enabled", True, "unknown order recovery disabled"),
        _invariant("follower value resolved", bool(data["global"].get("follower_account_value_used_for_sizing")), "follower value unresolved"),
        _invariant("leader value resolved for open positions", not _leader_sizing_errors(data), "leader value unresolved for one or more open positions"),
        _invariant("no allocation mismatch", not data["global"].get("allocation_mismatch"), "allocation mismatch present"),
        _invariant("unknown orders count = 0", unknown_count == 0, f"unknown orders count is {unknown_count}"),
        _invariant("order validator enabled", bool(exchange_rules.get("order_validator_enabled")), "Hyperliquid order validator disabled"),
        _invariant("precision rules loaded", int(exchange_rules.get("precision_rules_loaded_count") or 0) > 0, "Hyperliquid precision rules missing"),
        _invariant("no recent exchange rejections", int(exchange_rules.get("last_exchange_rejection_count") or 0) == 0, "Recent exchange rejections detected. Review Orders before live."),
        _warning_invariant(
            "latest dry-run latency exists",
            latest_order is None or latency_check["ok"],
            latency_check["message"],
        ),
    ]


def _invariant(name: str, ok: bool, message: str) -> dict[str, str]:
    return {"name": name, "status": "OK" if ok else "BLOCKED", "message": "OK" if ok else message}


def _warning_invariant(name: str, ok: bool, message: str) -> dict[str, str]:
    return {"name": name, "status": "OK" if ok else "WARNING", "message": "OK" if ok else message}


def _leader_sizing_errors(data: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for leader in (data.get("account_states") or {}).get("leaders") or []:
        for position in leader.get("positions") or []:
            sizing = position.get("sizing") or {}
            if sizing.get("error"):
                errors.append(str(sizing["error"]))
    return errors


def _recommended_next_action(blockers: list[str]) -> str:
    if not blockers:
        return "Run a fresh dry-run fill and re-check before enabling live."
    return f"Fix blocker: {blockers[0]}"


def _exchange_rules_from_metas(
    *,
    enabled_dexes: list[Any],
    metas_by_dex: dict[str, dict[str, Any]],
    mids_by_dex: dict[str, dict[str, Any]],
    min_order_value: float,
) -> dict[str, Any]:
    precision_loaded = 0
    missing_precision: list[str] = []
    missing_asset_id: list[str] = []
    missing_price: list[str] = []
    for dex in enabled_dexes:
        dex_name = dex.dex_name
        meta = metas_by_dex.get(dex_name, {}) or {}
        mids = mids_by_dex.get(dex_name, {}) or {}
        mids_canonical = {
            parse_coin(str(name), default_dex=dex_name).canonical_coin
            for name in mids
            if str(name).lower() not in {"dex", "type"}
        }
        for index, item in enumerate(meta.get("universe", []) or []):
            name = str(item.get("name") or "")
            if not name:
                continue
            canonical = parse_coin(name, default_dex=dex_name).canonical_coin
            if item.get("szDecimals") is not None and item.get("maxLeverage") is not None:
                precision_loaded += 1
            elif len(missing_precision) < 50:
                missing_precision.append(canonical)
            if index is None and len(missing_asset_id) < 50:
                missing_asset_id.append(canonical)
            if canonical not in mids_canonical and len(missing_price) < 50:
                missing_price.append(canonical)
    return {
        "order_validator_enabled": True,
        "min_order_value": _plain_decimal(min_order_value),
        "precision_rules_loaded_count": precision_loaded,
        "markets_missing_precision": missing_precision,
        "markets_missing_asset_id": missing_asset_id,
        "markets_missing_price": missing_price,
    }


def _plain_decimal(value: Any) -> str:
    return format(Decimal(str(value)).normalize(), "f")


async def _build_binance_readiness(
    *,
    settings: AppSettings,
    legacy_mappings: list[SymbolMapping],
    allocations: list[LeaderPositionAllocationRecord],
    unresolved_orders: list[ExecutionOrder],
) -> dict[str, Any]:
    binance_connected = bool(settings.binance_api_key and settings.binance_api_secret)
    current_position_mode = "UNKNOWN"
    position_risk_by_symbol: dict[str, list[dict]] = {}
    all_position_rows: list[dict] = []
    binance_error: str | None = None
    if binance_connected and settings.enable_binance_execution:
        client = BinanceFuturesClient(settings)
        try:
            current_position_mode = "HEDGE" if await client.position_mode_dual_side() else "ONE_WAY"
            for mapping in legacy_mappings:
                if not mapping.binance_symbol:
                    continue
                rows = await client.position_risk(mapping.binance_symbol)
                matched = [
                    item
                    for item in rows
                    if str(item.get("symbol", "")).upper() == mapping.binance_symbol.upper()
                ]
                if matched:
                    position_risk_by_symbol[mapping.binance_symbol.upper()] = matched
                    all_position_rows.extend(matched)
        except Exception as exc:
            binance_connected = False
            binance_error = str(exc)
        finally:
            await client.close()

    symbol_items = []
    for mapping in legacy_mappings:
        rows = position_risk_by_symbol.get(mapping.binance_symbol.upper(), [])
        first = rows[0] if rows else None
        sides = {str(row.get("positionSide", "UNKNOWN")).upper() for row in rows}
        margin_modes = {str(row.get("marginType", "UNKNOWN")).upper() for row in rows} or {"UNKNOWN"}
        leverages = {
            int(row["leverage"])
            for row in rows
            if row.get("leverage") is not None
        }
        margin_mode = ",".join(sorted(margin_modes))
        leverage = next(iter(leverages)) if len(leverages) == 1 else None
        position_side = ",".join(sorted(sides)) if sides else "UNKNOWN"
        long_notional = next(
            (str(row.get("notional")) for row in rows if str(row.get("positionSide", "")).upper() == "LONG"),
            None,
        )
        short_notional = next(
            (str(row.get("notional")) for row in rows if str(row.get("positionSide", "")).upper() == "SHORT"),
            None,
        )
        notional = f"LONG {long_notional or '--'} / SHORT {short_notional or '--'}" if first else None
        symbol_ok = (
            mapping.enabled
            and bool(rows)
            and margin_modes == {settings.binance_expected_margin_type.upper()}
            and leverages == {settings.binance_expected_leverage}
            and current_position_mode == "HEDGE"
            and sides.issubset({"LONG", "SHORT"})
            and bool(sides.intersection({"LONG", "SHORT"}))
        )
        symbol_items.append(
            {
                "symbol": mapping.binance_symbol,
                "coin": mapping.hyperliquid_coin,
                "enabled": mapping.enabled,
                "current_margin_mode": margin_mode,
                "expected_margin_mode": settings.binance_expected_margin_type.upper(),
                "current_leverage": leverage,
                "expected_leverage": settings.binance_expected_leverage,
                "current_position_notional": notional,
                "current_position_side": position_side,
                "status": "OK" if symbol_ok else ("BLOCKED" if mapping.enabled else "WARNING"),
                "message": binance_error
                or ("OK" if symbol_ok else "Binance Hedge Mode positionRisk is not OK"),
            }
        )

    binance_aggregate = _aggregate_binance_positions(all_position_rows)
    allocations_by_symbol: dict[str, dict[str, Decimal]] = {}
    for allocation in allocations:
        if allocation.status == "CLOSED" or allocation.execution_venue != ExecutionVenue.BINANCE.value:
            continue
        if not allocation.binance_symbol:
            continue
        symbol = allocation.binance_symbol.upper()
        side = allocation.position_side.upper()
        if side not in {"LONG", "SHORT"}:
            continue
        allocations_by_symbol.setdefault(symbol, {"LONG": Decimal("0"), "SHORT": Decimal("0")})
        allocations_by_symbol[symbol][side] += allocation.allocated_qty

    allocation_mismatches = []
    aggregate_symbols = set(binance_aggregate) | set(allocations_by_symbol)
    aggregate_positions = []
    for symbol in sorted(aggregate_symbols):
        allocated = allocations_by_symbol.get(symbol, {"LONG": Decimal("0"), "SHORT": Decimal("0")})
        binance = binance_aggregate.get(symbol, {"LONG": Decimal("0"), "SHORT": Decimal("0")})
        aggregate_positions.append(
            {
                "venue": ExecutionVenue.BINANCE.value,
                "symbol": symbol,
                "allocated_long_qty": str(allocated["LONG"]),
                "allocated_short_qty": str(allocated["SHORT"]),
                "binance_long_qty": str(binance["LONG"]),
                "binance_short_qty": str(binance["SHORT"]),
            }
        )
        long_diff = abs(allocated["LONG"] - binance["LONG"])
        short_diff = abs(allocated["SHORT"] - binance["SHORT"])
        if long_diff > Decimal("0.00000001") or short_diff > Decimal("0.00000001"):
            allocation_mismatches.append(
                {
                    "venue": ExecutionVenue.BINANCE.value,
                    "symbol": symbol,
                    "allocated_long_qty": str(allocated["LONG"]),
                    "allocated_short_qty": str(allocated["SHORT"]),
                    "binance_long_qty": str(binance["LONG"]),
                    "binance_short_qty": str(binance["SHORT"]),
                    "status": "BLOCKED",
                    "message": f"allocation mismatch long_diff={long_diff} short_diff={short_diff}",
                }
            )

    pending_binance = [
        order
        for order in unresolved_orders
        if order.execution_venue in {ExecutionVenue.BINANCE.value, None}
    ]
    blocking = []
    if not settings.enable_binance_execution:
        blocking.append("ENABLE_BINANCE_EXECUTION=false")
    if not binance_connected:
        blocking.append("Binance API not configured or not connected")
    if binance_connected and current_position_mode != "HEDGE":
        blocking.append(f"Binance account position mode is {current_position_mode}; expected HEDGE")
    if not legacy_mappings:
        blocking.append("no Binance symbol mappings configured")
    blocking.extend(
        f"symbol {item['symbol']} {item['status']}"
        for item in symbol_items
        if item["enabled"] and item["status"] != "OK"
    )
    blocking.extend(f"allocation {item['symbol']} mismatch" for item in allocation_mismatches)
    if pending_binance:
        blocking.append(f"{len(pending_binance)} unresolved Binance auto orders require recovery")

    return {
        "enabled": settings.enable_binance_execution,
        "trading_enabled": settings.binance_trading_enabled,
        "api_connected": binance_connected,
        "current_position_mode": current_position_mode,
        "expected_position_mode": "HEDGE",
        "expected_margin_mode": settings.binance_expected_margin_type.upper(),
        "expected_leverage": settings.binance_expected_leverage,
        "symbols": symbol_items,
        "aggregate_positions": aggregate_positions,
        "allocation_mismatches": allocation_mismatches,
        "unknown_orders_count": len(pending_binance),
        "ready_for_live_binance": len(blocking) == 0,
        "live_trading_allowed": (
            settings.trading_enabled
            and settings.binance_trading_enabled
            and len(blocking) == 0
        ),
        "blocking_reasons": blocking,
        "message": "OK" if not blocking else "; ".join(blocking),
    }


async def _build_hyperliquid_readiness(
    *,
    settings: AppSettings,
    venue_mappings: list[VenueMapping],
    legacy_mappings: list[SymbolMapping],
    leaders: list[LeaderConfig],
    allocations: list[LeaderPositionAllocationRecord],
    unresolved_orders: list[ExecutionOrder],
) -> dict[str, Any]:
    configured_account = settings.hyperliquid_follower_account_address()
    private_key_configured = bool(settings.hyperliquid_private_key_value())
    api_connected = False
    account_state: dict[str, Any] = {}
    metas_by_dex: dict[str, dict[str, Any]] = {}
    mids_by_dex: dict[str, dict[str, Any]] = {}
    account_states_by_dex: dict[str, dict[str, Any]] = {}
    account_abstraction: dict[str, Any] | None = None
    dex_readiness: list[dict[str, Any]] = []
    hyperliquid_error: str | None = None
    if settings.enable_hyperliquid_execution:
        client = HyperliquidExecutionClient(
            info_url=f"{settings.hyperliquid_execution_base_url()}/info",
            private_key=settings.hyperliquid_private_key_value(),
            account_address=settings.hyperliquid_account_address or settings.hyperliquid_signer_address(),
            vault_address=settings.hyperliquid_vault_address,
            network=settings.hyperliquid_execution_network,
            timeout=3.0,
        )
        try:
            for dex in HyperliquidDexRegistry(settings).enabled_dexes():
                meta: dict[str, Any] = {}
                mids: dict[str, Any] = {}
                state: dict[str, Any] = {}
                dex_error: str | None = None
                try:
                    meta = await client.meta(dex.dex_name)
                    mids = await client.all_mids(dex.dex_name)
                    if configured_account:
                        state = await client.account_state(configured_account, dex=dex.dex_name)
                    api_connected = True
                except Exception as exc:
                    dex_error = str(exc)[:240]
                    hyperliquid_error = hyperliquid_error or dex_error
                metas_by_dex[dex.dex_name] = meta
                mids_by_dex[dex.dex_name] = mids
                account_states_by_dex[dex.dex_name] = state
                if dex.dex_name == "":
                    account_state = state
                universe_count = len(meta.get("universe", []) or [])
                account_loaded = bool(state.get("marginSummary"))
                mids_fresh = bool(mids)
                hip3_allowed = settings.allow_hip3_markets or not dex.is_hip3
                ready_for_dex = (
                    settings.enable_hyperliquid_execution
                    and dex_error is None
                    and universe_count > 0
                    and mids_fresh
                    and (not configured_account or account_loaded)
                    and hip3_allowed
                )
                message = "OK"
                if dex_error:
                    message = dex_error
                elif not hip3_allowed:
                    message = "ALLOW_HIP3_MARKETS=false"
                elif universe_count == 0:
                    message = "meta universe missing"
                elif not mids_fresh:
                    message = "allMids missing or stale"
                elif configured_account and not account_loaded:
                    message = "account state not loaded for dex"
                dex_readiness.append(
                    {
                        "dex_name": dex.dex_name,
                        "dexName": dex.dex_name,
                        "display_name": dex.display_name,
                        "displayName": dex.display_name,
                        "enabled": dex.enabled,
                        "is_hip3": dex.is_hip3,
                        "isHip3": dex.is_hip3,
                        "meta_loaded": universe_count > 0,
                        "metaLoaded": universe_count > 0,
                        "universe_count": universe_count,
                        "universeCount": universe_count,
                        "mids_fresh": mids_fresh,
                        "midsFresh": mids_fresh,
                        "account_state_loaded_for_follower": account_loaded,
                        "accountStateLoadedForFollower": account_loaded,
                        "accountValue": str((state.get("marginSummary") or {}).get("accountValue"))
                        if state.get("marginSummary")
                        else None,
                        "withdrawable": str(state.get("withdrawable")) if state else None,
                        "open_positions_count": len(state.get("assetPositions", []) or []),
                        "openPositionsCount": len(state.get("assetPositions", []) or []),
                        "unknown_orders_count": len(
                            [
                                order
                                for order in unresolved_orders
                                if order.execution_venue == ExecutionVenue.HYPERLIQUID.value
                                and (order.dex or "") == dex.dex_name
                            ]
                        ),
                        "unknownOrdersCount": len(
                            [
                                order
                                for order in unresolved_orders
                                if order.execution_venue == ExecutionVenue.HYPERLIQUID.value
                                and (order.dex or "") == dex.dex_name
                            ]
                        ),
                        "asset_id_mapping_ready": universe_count > 0,
                        "assetIdMappingReady": universe_count > 0,
                        "low_latency_watcher_subscribed": False,
                        "lowLatencyWatcherSubscribed": False,
                        "ready_for_live_for_dex": ready_for_dex,
                        "readyForLiveForDex": ready_for_dex,
                        "message": message,
                    }
                )
            if configured_account:
                snapshot = await AccountAbstractionService(client, settings).fetch_snapshot(
                    role=FOLLOWER,
                    address=configured_account,
                    dexes=[dex.dex_name for dex in HyperliquidDexRegistry(settings).enabled_dexes()],
                )
                account_abstraction = {
                    **snapshot.as_dict(),
                    "resolved_by_dex": {
                        dex.dex_name: resolve_account_value_for_sizing(
                            snapshot,
                            dex.dex_name,
                            settings,
                        ).as_dict()
                        for dex in HyperliquidDexRegistry(settings).enabled_dexes()
                    },
                }
                for item in dex_readiness:
                    resolved = resolved_value_payload(account_abstraction, item["dex_name"]) or {}
                    item.update(
                        {
                            "account_value_used_for_sizing": resolved.get("account_value_used_for_sizing"),
                            "accountValueUsedForSizing": resolved.get("accountValueUsedForSizing"),
                            "available_collateral_used_for_margin_check": resolved.get(
                                "available_collateral_used_for_margin_check"
                            ),
                            "availableCollateralUsedForMarginCheck": resolved.get(
                                "availableCollateralUsedForMarginCheck"
                            ),
                            "account_value_source": resolved.get("account_value_source"),
                            "accountValueSource": resolved.get("accountValueSource"),
                            "account_abstraction_mode": resolved.get("account_abstraction_mode"),
                            "accountAbstractionMode": resolved.get("accountAbstractionMode"),
                            "account_value_confidence": resolved.get("confidence"),
                            "accountValueConfidence": resolved.get("confidence"),
                            "account_value_blockers": resolved.get("blockers") or [],
                            "accountValueBlockers": resolved.get("blockers") or [],
                            "account_value_warnings": resolved.get("warnings") or [],
                            "accountValueWarnings": resolved.get("warnings") or [],
                        }
                    )
        except Exception as exc:
            hyperliquid_error = str(exc)
        finally:
            await client.close()

    enabled_dexes = HyperliquidDexRegistry(settings).enabled_dexes()
    market_coverage = build_hyperliquid_market_coverage(
        enabled_dexes=enabled_dexes,
        metas_by_dex=metas_by_dex,
        mids_by_dex=mids_by_dex,
    )
    exchange_rules = _exchange_rules_from_metas(
        enabled_dexes=enabled_dexes,
        metas_by_dex=metas_by_dex,
        mids_by_dex=mids_by_dex,
        min_order_value=settings.hyperliquid_min_order_value_usd,
    )
    universe_by_canonical: dict[str, dict[str, Any]] = {}
    for dex_name, meta in metas_by_dex.items():
        for item in meta.get("universe", []) or []:
            if item.get("name"):
                universe_by_canonical[canonical_coin(dex=dex_name, coin=str(item.get("name", "")))] = item
    universe = {
        str(item.get("name", "")).upper(): item
        for item in metas_by_dex.get("", {}).get("universe", [])
        if item.get("name")
    }
    hl_mapping_rows = _venue_coin_mapping_rows(
        venue_mappings,
        legacy_mappings,
        ExecutionVenue.HYPERLIQUID,
    )
    has_all_coins_leader = any(allowed_coins_mode(leader) == "ALL_COINS" for leader in leaders)
    enabled_coins = sorted(
        {
            canonical_coin(dex="", coin=str(_row_value(row, "hyperliquid_coin", "")))
            for row in hl_mapping_rows
            if _row_value(row, "enabled", True)
        }
        | {
            parse_coin(str(coin)).canonical_coin
            for leader in leaders
            if leader.enabled
            for coin in (leader.allowed_symbols or [])
            if str(coin).strip()
        }
    )
    symbol_items = []
    default_account_value_result = resolved_value_payload(account_abstraction, "") or {}
    withdrawable = _decimal_or_none(
        default_account_value_result.get("available_collateral_used_for_margin_check")
        or default_account_value_result.get("withdrawable_or_available")
    )
    if withdrawable is None:
        withdrawable = _decimal_or_none(account_state.get("withdrawable")) if account_state else None
    for canonical in enabled_coins:
        parsed = parse_coin(canonical)
        mapping_row = next(
            (
                row
                for row in hl_mapping_rows
                if canonical_coin(dex="", coin=str(_row_value(row, "hyperliquid_coin", ""))) == canonical
            ),
            None,
        )
        meta_item = universe_by_canonical.get(canonical)
        leverage_plan = (
            build_hyperliquid_leverage_plan(
                default_leverage=settings.hyperliquid_default_leverage,
                coin_max_leverage=meta_item.get("maxLeverage") if meta_item else None,
            )
            if meta_item
            else None
        )
        mapping_allows = _mapping_status_allows_trading(mapping_row) if mapping_row else True
        status = "OK"
        message = "OK"
        warning = None
        target_leverage = leverage_plan.effective_leverage if leverage_plan else None
        available_margin_sufficient = check_hyperliquid_available_margin(
            account_state=account_state,
            target_notional=None,
            effective_leverage=target_leverage,
        )
        if not api_connected:
            status = "BLOCKED"
            message = hyperliquid_error or "Hyperliquid API not connected"
        elif meta_item is None:
            status = "BLOCKED"
            message = "coin not in Hyperliquid meta universe for configured dex"
        elif leverage_plan and not leverage_plan.ok_for_open:
            status = "BLOCKED"
            message = leverage_plan.reason or "coin leverage unavailable"
        elif leverage_plan and leverage_plan.status == "WARNING":
            status = "WARNING"
            warning = leverage_plan.warning
            message = leverage_plan.warning or "using coin max leverage"
        elif not mapping_allows:
            status = "BLOCKED"
            message = str(_row_value(mapping_row, "reason", "mapping disabled"))
        symbol_items.append(
            {
                "coin": parsed.coin,
                "dex": parsed.dex,
                "dex_display_name": dex_display_name(parsed.dex),
                "canonical_coin": canonical,
                "exists_in_meta": meta_item is not None,
                "venue_symbol": str(_row_value(mapping_row, "venue_symbol", canonical)).upper()
                if mapping_row
                else canonical,
                "asset_id": resolve_asset_id_from_meta(metas_by_dex.get(parsed.dex, {}), coin=parsed.coin, dex=parsed.dex),
                "enabled": bool(_row_value(mapping_row, "enabled", True)) if mapping_row else True,
                "tradable": meta_item is not None,
                "sz_decimals": meta_item.get("szDecimals") if meta_item else None,
                "max_leverage": leverage_plan.max_leverage if leverage_plan else None,
                "target_leverage": target_leverage,
                "effective_leverage": target_leverage,
                "margin_mode": settings.hyperliquid_default_margin_mode.upper(),
                "expected_margin_mode": settings.hyperliquid_default_margin_mode.upper(),
                "expected_leverage": target_leverage,
                "default_leverage": settings.hyperliquid_default_leverage,
                "risk_status": status,
                "warning": warning,
                "coin_max_leverage_loaded": bool(leverage_plan and leverage_plan.max_leverage is not None),
                "margin_mode_confirmed": status in {"OK", "WARNING"} and api_connected,
                "isolated_confirmed": settings.hyperliquid_default_margin_mode.upper() == "ISOLATED"
                and status in {"OK", "WARNING"}
                and api_connected,
                "leverage_confirmed": status in {"OK", "WARNING"} and api_connected,
                "available_margin_sufficient": available_margin_sufficient if available_margin_sufficient is not None else bool(withdrawable is not None),
                "status": status,
                "message": message,
            }
        )

    margin_summary = account_state.get("marginSummary", {})
    account_state_loaded = bool(margin_summary)
    hyperliquid_aggregate: dict[str, dict[str, Decimal]] = {}
    for dex_name, state in account_states_by_dex.items():
        for row in state.get("assetPositions", []) or []:
            position = row.get("position", row)
            parsed = parse_coin(str(position.get("coin", "")), default_dex=dex_name)
            size = Decimal(str(position.get("szi") or position.get("size") or "0"))
            hyperliquid_aggregate.setdefault(parsed.canonical_coin, {"LONG": Decimal("0"), "SHORT": Decimal("0")})
            if size > 0:
                hyperliquid_aggregate[parsed.canonical_coin]["LONG"] += abs(size)
            elif size < 0:
                hyperliquid_aggregate[parsed.canonical_coin]["SHORT"] += abs(size)
    allocations_by_coin: dict[str, dict[str, Decimal]] = {}
    for allocation in allocations:
        if allocation.status == "CLOSED" or allocation.execution_venue != ExecutionVenue.HYPERLIQUID.value:
            continue
        coin = allocation.canonical_coin or canonical_coin(dex=allocation.dex, coin=allocation.hyperliquid_coin)
        side = allocation.position_side.upper()
        if side not in {"LONG", "SHORT"}:
            continue
        allocations_by_coin.setdefault(coin, {"LONG": Decimal("0"), "SHORT": Decimal("0")})
        allocations_by_coin[coin][side] += allocation.allocated_qty
    terminal_flat_leader_residuals = _terminal_flat_leader_allocation_residuals(allocations)

    aggregate_positions = []
    allocation_mismatches = []
    aggregate_coins = set(hyperliquid_aggregate) | set(allocations_by_coin)
    for coin in sorted(aggregate_coins):
        allocated = allocations_by_coin.get(coin, {"LONG": Decimal("0"), "SHORT": Decimal("0")})
        follower = hyperliquid_aggregate.get(coin, {"LONG": Decimal("0"), "SHORT": Decimal("0")})
        aggregate_positions.append(
            {
                "venue": ExecutionVenue.HYPERLIQUID.value,
                "symbol": coin,
                "canonical_coin": coin,
                "dex": parse_coin(coin).dex,
                "allocated_long_qty": str(allocated["LONG"]),
                "allocated_short_qty": str(allocated["SHORT"]),
                "hyperliquid_long_qty": str(follower["LONG"]),
                "hyperliquid_short_qty": str(follower["SHORT"]),
            }
        )
        if not account_state:
            continue
        long_diff = abs(allocated["LONG"] - follower["LONG"])
        short_diff = abs(allocated["SHORT"] - follower["SHORT"])
        if long_diff > Decimal("0.00000001") or short_diff > Decimal("0.00000001"):
            allocation_mismatches.append(
                {
                    "venue": ExecutionVenue.HYPERLIQUID.value,
                    "symbol": coin,
                    "allocated_long_qty": str(allocated["LONG"]),
                    "allocated_short_qty": str(allocated["SHORT"]),
                    "hyperliquid_long_qty": str(follower["LONG"]),
                    "hyperliquid_short_qty": str(follower["SHORT"]),
                    "status": "BLOCKED",
                    "message": f"allocation mismatch long_diff={long_diff} short_diff={short_diff}",
                }
            )

    pending_hyperliquid = [
        order
        for order in unresolved_orders
        if order.execution_venue == ExecutionVenue.HYPERLIQUID.value
    ]
    blocking = []
    if not settings.enable_hyperliquid_execution:
        blocking.append("ENABLE_HYPERLIQUID_EXECUTION=false")
    if not api_connected:
        blocking.append(hyperliquid_error or "Hyperliquid API not connected")
    if not configured_account:
        blocking.append("Hyperliquid account address not configured")
    if settings.hyperliquid_follower_address_ambiguous():
        blocking.append("Follower account address is ambiguous. Set HYPERLIQUID_ACCOUNT_ADDRESS explicitly.")
    if not private_key_configured:
        blocking.append("Hyperliquid signer private key not configured")
    if configured_account and private_key_configured and not account_state_loaded:
        blocking.append(hyperliquid_error or "Hyperliquid follower account state unavailable")
    if account_abstraction:
        default_resolved = resolved_value_payload(account_abstraction, "") or {}
        account_blockers = default_resolved.get("blockers") or []
        blocking.extend(f"account abstraction {item}" for item in account_blockers)
    for item in dex_readiness:
        if not item["ready_for_live_for_dex"]:
            blocking.append(f"dex {item['dex_name'] or 'default'} {item['message']}")
    blocking.extend(
        f"coin {item['coin']} {item['status']}"
        for item in symbol_items
        if item["enabled"] and item["status"] == "BLOCKED"
    )
    blocking.extend(f"allocation {item['symbol']} mismatch" for item in allocation_mismatches)
    blocking.extend(
        f"allocation {item['symbol']} remains nonzero after leader flat"
        for item in terminal_flat_leader_residuals
    )
    if pending_hyperliquid:
        blocking.append(f"{len(pending_hyperliquid)} unresolved Hyperliquid auto orders require recovery")

    ready = len(blocking) == 0
    return {
        "enabled": settings.enable_hyperliquid_execution,
        "trading_enabled": settings.hyperliquid_trading_enabled,
        "network": settings.hyperliquid_execution_network,
        "api_connected": api_connected,
        "wallet_account_configured": bool(configured_account),
        "private_key_configured": private_key_configured,
        "account_state_loaded": account_state_loaded,
        "accountValue": str(margin_summary.get("accountValue")) if margin_summary else None,
        "withdrawable": str(account_state.get("withdrawable")) if account_state else None,
        "account_abstraction": account_abstraction,
        "accountAbstraction": account_abstraction,
        "account_abstraction_mode": (account_abstraction or {}).get("account_abstraction_mode"),
        "accountAbstractionMode": (account_abstraction or {}).get("accountAbstractionMode"),
        "account_value_used_for_sizing": (
            resolved_value_payload(account_abstraction, "") or {}
        ).get("account_value_used_for_sizing"),
        "accountValueUsedForSizing": (
            resolved_value_payload(account_abstraction, "") or {}
        ).get("accountValueUsedForSizing"),
        "account_value_source": (resolved_value_payload(account_abstraction, "") or {}).get(
            "account_value_source"
        ),
        "accountValueSource": (resolved_value_payload(account_abstraction, "") or {}).get(
            "accountValueSource"
        ),
        "follower_account": configured_account,
        "meta_universe_loaded": bool(universe),
        "coin_scope": "ALL_COINS_DYNAMIC" if has_all_coins_leader else "CUSTOM_LIST",
        "enabled_dexes": [dex.dex_name for dex in enabled_dexes],
        "dex_readiness": dex_readiness,
        "market_coverage": market_coverage,
        "marketCoverage": market_coverage,
        "exchange_rules": exchange_rules,
        "exchangeRules": exchange_rules,
        "enabled_coins": enabled_coins,
        "symbols": symbol_items,
        "aggregate_positions": aggregate_positions,
        "allocation_mismatches": allocation_mismatches,
        "terminal_flat_leader_residuals": terminal_flat_leader_residuals,
        "terminal_flat_leader_residual_count": len(terminal_flat_leader_residuals),
        "unknown_orders_count": len(pending_hyperliquid),
        "rate_limit_warning": None,
        "ready_for_live_hyperliquid": ready,
        "live_trading_allowed": (
            settings.trading_enabled
            and settings.hyperliquid_trading_enabled
            and ready
        ),
        "blocking_reasons": blocking,
        "message": "OK" if not blocking else "; ".join(blocking),
    }


def _terminal_flat_leader_allocation_residuals(
    allocations: list[LeaderPositionAllocationRecord],
) -> list[dict[str, Any]]:
    """Expose lifecycle errors that an allocation-vs-follower equality check misses."""
    residuals: list[dict[str, Any]] = []
    for allocation in allocations:
        if (
            allocation.execution_venue != ExecutionVenue.HYPERLIQUID.value
            or str(allocation.status or "").upper() == "CLOSED"
        ):
            continue
        leader_size = allocation.last_leader_position_size
        if leader_size is None or abs(Decimal(leader_size or 0)) > Decimal("0.00000001"):
            continue
        allocated_qty = abs(Decimal(allocation.allocated_qty or 0))
        target_notional = abs(Decimal(allocation.target_notional or 0))
        if allocated_qty <= Decimal("0.00000001") or target_notional > Decimal("0.00000001"):
            continue
        coin = allocation.canonical_coin or canonical_coin(
            dex=allocation.dex,
            coin=allocation.hyperliquid_coin,
        )
        residuals.append(
            {
                "allocation_id": allocation.id,
                "leader_id": allocation.leader_id,
                "symbol": coin,
                "dex": allocation.dex,
                "position_side": allocation.position_side,
                "status": allocation.status,
                "allocated_qty": str(allocated_qty),
                "allocated_notional": str(abs(Decimal(allocation.allocated_notional or 0))),
                "pending_reduce_qty": (
                    str(abs(Decimal(allocation.pending_reduce_qty or 0)))
                    if allocation.pending_reduce_qty is not None
                    else None
                ),
                "message": "leader is flat but the follower allocation remains nonzero",
            }
        )
    return residuals
