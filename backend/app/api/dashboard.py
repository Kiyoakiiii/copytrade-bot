from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from fastapi import APIRouter
from sqlalchemy import case, func, select

from app.api.deps import AppSettings, CurrentUser, DbSession
from app.api.risk import get_risk_setting
from app.models import (
    AppSetting,
    ExecutionOrder,
    LatestAccountPosition,
    LatestAccountState,
    LeaderConfig,
    LeaderPositionAllocationRecord,
    RiskEvent,
)
from app.services.account_state import FOLLOWER, LEADER, account_state_payload, load_account_state_with_positions, position_payload
from app.api.account_states import (
    _account_abstraction_fields,
    _account_state_payloads_for_address,
    _follower_config_debug,
    _load_follower_spot_debug,
    _sizing_payload,
)
from app.services.account_abstraction import load_account_abstraction_state, resolved_value_payload
from app.services.auto_copy import RECOVERY_ORDER_STATUSES
from app.services.baseline import (
    baseline_readiness_summary,
    baselines_by_scope_for_leaders,
    baseline_scope_key,
    baseline_status_for_position,
)
from app.services.copy_execution_status import (
    copy_order_key,
    copy_order_status_payload,
    latest_copy_orders_by_market,
)
from app.services.execution_router import ExecutionVenue
from app.services.follower_migration import (
    FOLLOWER_RUNTIME_IDENTITY_KEY,
    public_follower_migration_payload,
)
from app.services.leader_config import (
    active_leaders_statement,
    allowed_coins_mode,
    decimal_to_string,
    is_coin_allowed,
    normalize_leader_address,
)
from app.services.live_readiness import small_live_start_checklist
from app.services.watcher_status import (
    watcher_active_leaders_by_scope,
    watcher_execution_scope,
    watcher_statuses_by_scope,
)
from app.tasks.leader_state_poller import (
    account_state_cache_status,
    monitoring_account_state_stale_seconds,
)

router = APIRouter(tags=["dashboard"])


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/dashboard")
async def dashboard(_: CurrentUser, db: DbSession, settings: AppSettings):
    leader_count = await db.scalar(select(func.count()).select_from(LeaderConfig))
    order_count = await db.scalar(select(func.count()).select_from(ExecutionOrder))
    error_count = await db.scalar(
        select(func.count()).select_from(RiskEvent).where(RiskEvent.severity.in_(["error", "critical"]))
    )
    allocation_count = await db.scalar(
        select(func.count()).select_from(LeaderPositionAllocationRecord).where(
            LeaderPositionAllocationRecord.status != "CLOSED"
        )
    )
    return {
        "trading_enabled_env": settings.trading_enabled,
        "binance_testnet": settings.binance_testnet,
        "leaders": leader_count or 0,
        "orders": order_count or 0,
        "active_allocations": allocation_count or 0,
        "errors": error_count or 0,
        "websocket_status": "not_started",
        "equity": None,
        "positions": [],
    }


@router.get("/dashboard/realtime")
async def dashboard_realtime(_: CurrentUser, db: DbSession, settings: AppSettings):
    # The operator dashboard is a read-only projection of watcher-persisted
    # state. Browser refreshes must never schedule exchange Info API traffic.
    state_refresh = await account_state_cache_status(
        settings,
        max_age_seconds=monitoring_account_state_stale_seconds(settings),
    )
    return await build_dashboard_realtime_payload(db=db, settings=settings, state_refresh=state_refresh)


@router.get("/dashboard/overview")
async def dashboard_overview(_: CurrentUser, db: DbSession, settings: AppSettings):
    """Return the small, DB-only projection consumed by Command Center.

    The legacy realtime payload also serves diagnostics and preflight clients,
    so it intentionally contains rich follower/leader state.  Command Center
    only needs execution accounts, active leader configuration, recent orders,
    and the applied kill-switch state.  Keeping this projection separate avoids
    rebuilding and transferring several megabytes whenever the operator changes
    tabs, and it never schedules exchange I/O.
    """

    return await build_dashboard_overview_payload(db=db, settings=settings)


async def build_dashboard_overview_payload(
    *,
    db: DbSession,
    settings: AppSettings,
) -> dict[str, object]:
    response_at = datetime.now(timezone.utc)
    risk = await get_risk_setting(db)
    kill_switch = bool(risk.get("kill_switch", False))

    main_address = str(
        settings.hyperliquid_account_address
        or settings.hyperliquid_signer_address()
        or ""
    ).strip().lower()
    subaccounts = [
        address
        for address in settings.hyperliquid_execution_subaccount_address_list()
        if address and address != main_address
    ]
    configured_accounts = [
        ("", main_address or None, "MAIN", "Main account"),
        *[
            (address, address, "SUBACCOUNT", f"Subaccount · {address[-4:]}")
            for address in subaccounts
        ],
    ]
    addresses = [address for _, address, _, _ in configured_accounts if address]

    status_keys = [
        "watcher_status" if not scope else f"watcher_status:{scope}"
        for scope, _, _, _ in configured_accounts
    ]
    watcher_rows = (
        await db.execute(select(AppSetting).where(AppSetting.key.in_(status_keys)))
    ).scalars().all()
    watcher_values = {
        row.key: row.value if isinstance(row.value, dict) else {}
        for row in watcher_rows
    }
    watcher_status_by_scope = watcher_statuses_by_scope(watcher_rows)
    watcher_active_by_scope = watcher_active_leaders_by_scope(watcher_status_by_scope)

    state_rows = []
    position_rows = []
    if addresses:
        state_rows = (
            await db.execute(
                select(LatestAccountState)
                .where(LatestAccountState.role == FOLLOWER)
                .where(LatestAccountState.address.in_(addresses))
                .order_by(LatestAccountState.address, LatestAccountState.dex)
            )
        ).scalars().all()
        position_rows = (
            await db.execute(
                select(LatestAccountPosition)
                .where(LatestAccountPosition.role == FOLLOWER)
                .where(LatestAccountPosition.address.in_(addresses))
                .where(LatestAccountPosition.active.is_(True))
                .order_by(
                    LatestAccountPosition.address,
                    LatestAccountPosition.dex,
                    LatestAccountPosition.coin,
                )
            )
        ).scalars().all()

    states_by_address: dict[str, list[LatestAccountState]] = {}
    states_by_id: dict[int, LatestAccountState] = {}
    for row in state_rows:
        states_by_address.setdefault(row.address.lower(), []).append(row)
        states_by_id[row.id] = row
    positions_by_address: dict[str, list[LatestAccountPosition]] = {}
    for row in position_rows:
        positions_by_address.setdefault(row.address.lower(), []).append(row)

    # Attribute follower positions from the durable allocation ledger.  This is
    # intentionally a DB-only overview projection: opening the dashboard never
    # reaches Hyperliquid and cannot contend with the watcher hot path.  A
    # detached liquidation lifecycle is operator-owned, so it must not be
    # presented as an actively followed leader position.
    execution_scopes = [scope for scope, _, _, _ in configured_accounts]
    active_allocations = (
        (
            await db.execute(
                select(LeaderPositionAllocationRecord)
                .where(
                    LeaderPositionAllocationRecord.execution_venue
                    == ExecutionVenue.HYPERLIQUID.value
                )
                .where(LeaderPositionAllocationRecord.venue_account.in_(execution_scopes))
                .where(LeaderPositionAllocationRecord.status != "CLOSED")
                .where(LeaderPositionAllocationRecord.status != "LIQUIDATION_DETACHED")
                .where(LeaderPositionAllocationRecord.allocated_qty > Decimal("0"))
            )
        )
        .scalars()
        .all()
        if execution_scopes
        else []
    )
    position_owners: dict[tuple[str, str, str, str], set[str]] = {}
    for allocation in active_allocations:
        key = _overview_position_owner_key(
            execution_scope=allocation.venue_account,
            dex=allocation.dex,
            canonical_coin=(
                allocation.canonical_coin or allocation.hyperliquid_coin
            ),
            side=allocation.position_side,
        )
        position_owners.setdefault(key, set()).add(
            normalize_leader_address(allocation.leader_address)
        )

    accounts: list[dict[str, object]] = []
    for scope, address, account_type, label in configured_accounts:
        status_key = "watcher_status" if not scope else f"watcher_status:{scope}"
        watcher = watcher_values.get(status_key, {})
        if not address:
            accounts.append(
                {
                    "execution_scope": scope,
                    "account_type": account_type,
                    "account_label": label,
                    "address": None,
                    "account_value": None,
                    "account_value_used_for_sizing": None,
                    "available_collateral_used_for_margin_check": None,
                    "withdrawable": None,
                    "total_margin_used": None,
                    "data_age_ms": None,
                    "stale": True,
                    "error_message": "account address is not configured",
                    "watcher_running": bool(watcher.get("low_latency_watcher_running")),
                    "watcher_ready": bool(watcher.get("ready_for_low_latency_live")),
                    "active_leaders": watcher.get("active_leaders") or [],
                    "positions": [],
                }
            )
            continue

        address_states = states_by_address.get(address, [])
        primary_state = next(
            (row for row in address_states if str(row.dex or "") == ""),
            address_states[0] if address_states else None,
        )
        primary = account_state_payload(
            primary_state,
            [],
            stale_seconds=int(settings.account_state_stale_seconds),
        )
        abstraction = await load_account_abstraction_state(
            db,
            role=FOLLOWER,
            address=address,
        )
        resolved = resolved_value_payload(abstraction, "") or {}
        account_value_used = (
            resolved.get("account_value_used_for_sizing")
            or resolved.get("accountValueUsedForSizing")
            or resolved.get("account_value")
            or resolved.get("accountValue")
            or primary.get("account_value")
        )
        available_collateral = (
            resolved.get("available_collateral_used_for_margin_check")
            or resolved.get("availableCollateralUsedForMarginCheck")
            or resolved.get("withdrawable_or_available")
            or resolved.get("withdrawableOrAvailable")
            or primary.get("withdrawable")
        )
        positions = []
        for row in positions_by_address.get(address, []):
            owner_candidates = position_owners.get(
                _overview_position_owner_key(
                    execution_scope=scope,
                    dex=row.dex,
                    canonical_coin=row.canonical_coin or row.coin,
                    side=row.side,
                ),
                set(),
            )
            leader_address = (
                next(iter(owner_candidates))
                if len(owner_candidates) == 1
                else None
            )
            attribution = (
                "LEADER"
                if leader_address
                else "AMBIGUOUS"
                if owner_candidates
                else "MANUAL"
            )
            positions.append(
                _overview_position_payload(
                    row,
                    account_state=states_by_id.get(row.account_state_id),
                    leader_address=leader_address,
                    attribution=attribution,
                )
            )
        accounts.append(
            {
                "execution_scope": scope,
                "account_type": account_type,
                "account_label": label,
                "address": address,
                "account_value": primary.get("account_value"),
                "account_value_used_for_sizing": account_value_used,
                "available_collateral_used_for_margin_check": available_collateral,
                "withdrawable": primary.get("withdrawable"),
                "total_margin_used": primary.get("total_margin_used"),
                "data_age_ms": primary.get("data_age_ms"),
                "stale": bool(primary.get("stale")),
                "error_message": primary.get("error_message"),
                "watcher_running": bool(watcher.get("low_latency_watcher_running")),
                "watcher_ready": bool(watcher.get("ready_for_low_latency_live")),
                "active_leaders": watcher.get("active_leaders") or [],
                "positions": positions,
            }
        )

    leader_rows = (await db.execute(active_leaders_statement())).scalars().all()
    leaders = [
        {
            "id": row.id,
            "enabled": row.enabled,
            "deleted_at": row.deleted_at.isoformat() if row.deleted_at else None,
            "leader_address": row.leader_address,
            "copy_multiplier": decimal_to_string(row.copy_multiplier),
            "fixed_account_value": decimal_to_string(row.fixed_account_value),
            "hyperliquid_vault_address": row.hyperliquid_vault_address,
            "watcher_status": (
                "active"
                if normalize_leader_address(row.leader_address)
                in watcher_active_by_scope.get(
                    watcher_execution_scope(row.hyperliquid_vault_address),
                    set(),
                )
                else "not_subscribed"
            ),
        }
        for row in leader_rows
    ]

    recent_orders = (
        await db.execute(select(ExecutionOrder).order_by(ExecutionOrder.created_at.desc()).limit(20))
    ).scalars().all()
    auto_copy_orders = [row for row in recent_orders if row.source_type == "AUTO_COPY"]
    return {
        "last_updated_at": response_at.isoformat(),
        "runtime": {
            "dry_run_or_live": "live" if settings.trading_enabled and not kill_switch else "dry-run",
            "kill_switch": kill_switch,
            "kill_switch_updated_at": risk.get("kill_switch_updated_at"),
            "live_opens_enabled": bool(
                settings.trading_enabled
                and settings.hyperliquid_trading_enabled
                and not kill_switch
            ),
        },
        "accounts": accounts,
        "leaders": leaders,
        "recent_orders": [_dashboard_order_payload(row) for row in recent_orders],
        "latency": _dashboard_latency_summary(auto_copy_orders),
    }


async def build_dashboard_realtime_payload(
    *,
    db: DbSession,
    settings: AppSettings,
    state_refresh: dict[str, object] | None = None,
) -> dict[str, object]:
    if state_refresh is None:
        state_refresh = await account_state_cache_status(
            settings,
            max_age_seconds=monitoring_account_state_stale_seconds(settings),
        )
    response_at = datetime.now(timezone.utc)
    monitoring_stale_seconds = monitoring_account_state_stale_seconds(settings)
    risk = await get_risk_setting(db)
    kill_switch = bool(risk.get("kill_switch", False))
    watcher_rows = (
        await db.execute(
            select(AppSetting).where(AppSetting.key.like("watcher_status%"))
        )
    ).scalars().all()
    watcher_status_by_scope = watcher_statuses_by_scope(watcher_rows)
    watcher_active_by_scope = watcher_active_leaders_by_scope(watcher_status_by_scope)
    watcher_status = watcher_status_by_scope.get("", {})
    follower_migration_row = await db.get(AppSetting, FOLLOWER_RUNTIME_IDENTITY_KEY)
    follower_migration = public_follower_migration_payload(
        follower_migration_row.value if follower_migration_row else None
    )
    live_price_cache = (watcher_status.get("price_cache") or {}).get("prices") or {}
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
            stale_seconds=monitoring_stale_seconds,
        )
        if follower_address
        else []
    )
    follower_state_rows = (
        (
            await db.execute(
                select(LatestAccountState)
                .where(LatestAccountState.role == FOLLOWER)
                .where(LatestAccountState.address == follower_address)
            )
        )
        .scalars()
        .all()
        if follower_address
        else []
    )
    follower_states_by_dex = {row.dex: row for row in follower_state_rows}
    follower = account_state_payload(
        follower_state,
        follower_positions,
        stale_seconds=monitoring_stale_seconds,
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
    follower["positions"] = [
        _position_with_live_price(position, live_price_cache=live_price_cache)
        for item in follower_dex_states
        for position in item.get("positions", [])
    ]

    leaders = (await db.execute(active_leaders_statement())).scalars().all()
    baseline_by_scope = await baselines_by_scope_for_leaders(db, [leader.id for leader in leaders])
    baseline_summary = await baseline_readiness_summary(db, leaders)
    leader_state_rows = (
        await db.execute(select(LatestAccountState).where(LatestAccountState.role == LEADER))
    ).scalars().all()
    leader_states: dict[str, list[LatestAccountState]] = {}
    for row in leader_state_rows:
        leader_states.setdefault(row.address.lower(), []).append(row)
    state_ids = [row.id for row in leader_state_rows]
    positions_by_state: dict[int, list[LatestAccountPosition]] = {}
    if state_ids:
        for position in (
            await db.execute(
                select(LatestAccountPosition)
                .where(LatestAccountPosition.account_state_id.in_(state_ids))
                .where(LatestAccountPosition.active.is_(True))
                .order_by(LatestAccountPosition.coin)
            )
        ).scalars().all():
            positions_by_state.setdefault(position.account_state_id, []).append(position)

    allocations = (
        await db.execute(
            select(LeaderPositionAllocationRecord)
            .where(LeaderPositionAllocationRecord.status != "CLOSED")
            .order_by(
                case(
                    (
                        LeaderPositionAllocationRecord.status
                        == "LIQUIDATION_DETACHED",
                        0,
                    ),
                    else_=1,
                ),
                LeaderPositionAllocationRecord.updated_at.desc(),
            )
            .limit(25)
        )
    ).scalars().all()

    latest_copy_orders = await latest_copy_orders_by_market(db, leaders)
    leader_payloads = []
    for leader in leaders:
        address = normalize_leader_address(leader.leader_address)
        leader_abstraction = await load_account_abstraction_state(db, role=LEADER, address=address)
        states = sorted(leader_states.get(address, []), key=lambda row: row.dex)
        state = next((row for row in states if row.dex == ""), states[0] if states else None)
        dex_state_payloads = [
            account_state_payload(
                row,
                positions_by_state.get(row.id, []),
                stale_seconds=monitoring_stale_seconds,
                extra=_account_abstraction_fields(leader_abstraction, dex=row.dex),
            )
            for row in states
        ]
        payload = account_state_payload(
            state,
            [],
            stale_seconds=monitoring_stale_seconds,
            extra={
                "leader": {
                    "id": leader.id,
                    "leader_address": leader.leader_address,
                    "enabled": leader.enabled,
                    "copy_multiplier": decimal_to_string(leader.copy_multiplier),
                    "fixed_account_value": decimal_to_string(leader.fixed_account_value),
                    "allowed_coins_mode": allowed_coins_mode(leader),
                    "preferred_venue": leader.preferred_venue,
                    "fallback_venue": leader.fallback_venue,
                    "max_notional_per_trade": decimal_to_string(leader.max_notional_per_trade),
                    "max_total_notional": decimal_to_string(leader.max_total_notional),
                    "execution_account": str(leader.hyperliquid_vault_address or "").lower(),
                },
                "dex_states": dex_state_payloads,
                "dexStates": dex_state_payloads,
                "watcher_status": "active"
                if address
                in watcher_active_by_scope.get(
                    watcher_execution_scope(leader.hyperliquid_vault_address),
                    set(),
                )
                else "not_subscribed",
                **_account_abstraction_fields(leader_abstraction, dex=state.dex if state else ""),
            },
        )
        leader_allocations = [
            allocation
            for allocation in allocations
            if normalize_leader_address(allocation.leader_address) == address
        ]
        payload["positions"] = []
        for row in states:
            for position_row in positions_by_state.get(row.id, []):
                position = position_payload(position_row, account_state=row)
                enriched = _dashboard_position_with_baseline(
                    leader=leader,
                    dex=row.dex,
                    position=position,
                    baseline_by_scope=baseline_by_scope,
                    latest_copy_orders=latest_copy_orders,
                )
                enriched["sizing"] = (
                    _sizing_payload(
                        leader_state=row,
                        position=position,
                        leader=leader,
                        follower_state=follower_states_by_dex.get(row.dex),
                        allocations=leader_allocations,
                    )
                    if enriched.get("copyable")
                    else None
                )
                payload["positions"].append(_position_with_live_price(enriched, live_price_cache=live_price_cache))
        leader_payloads.append(payload)

    allocation_keys = {
        (row.hyperliquid_coin.upper(), row.position_side.upper())
        for row in allocations
        if row.hyperliquid_coin and row.position_side
    }
    for position in follower.get("positions", []):
        matched = (
            str(position.get("coin", "")).upper(),
            str(position.get("side", "")).upper(),
        ) in allocation_keys
        position["allocation_matched"] = matched
        position["allocationMatched"] = matched
    recent_orders = (
        await db.execute(select(ExecutionOrder).order_by(ExecutionOrder.created_at.desc()).limit(20))
    ).scalars().all()
    latency_summary = _dashboard_latency_summary([row for row in recent_orders if row.source_type == "AUTO_COPY"])
    unresolved_orders = (
        await db.execute(
            select(ExecutionOrder).where(
                ExecutionOrder.source_type == "AUTO_COPY",
                ExecutionOrder.status.in_(RECOVERY_ORDER_STATUSES),
            )
        )
    ).scalars().all()
    startup_config_row = await db.get(AppSetting, "startup_config")
    startup_blockers = (startup_config_row.value or {}).get("blocking_reasons", []) if startup_config_row else []
    hyperliquid_startup_blockers = [
        reason for reason in startup_blockers if str(reason).startswith("hyperliquid.")
    ]
    allocation_mismatch = any(allocation.status == "NEEDS_MANUAL_REVIEW" for allocation in allocations)
    checklist = small_live_start_checklist(
        trading_enabled=settings.trading_enabled,
        hyperliquid_trading_enabled=settings.hyperliquid_trading_enabled,
        kill_switch=kill_switch,
        follower=follower,
        leaders=leader_payloads,
        hyperliquid_ready=not hyperliquid_startup_blockers,
        unknown_orders_count=len(unresolved_orders),
        allocation_mismatch=allocation_mismatch,
        hyperliquid_symbols=[],
    )
    preflight_blockers = list(startup_blockers)
    preflight_blockers.extend(item["message"] for item in checklist["checks"] if item["status"] == "BLOCKED")
    data_age_ms = max(
        [
            value
            for value in [
                follower.get("data_age_ms"),
                *[leader.get("data_age_ms") for leader in leader_payloads],
            ]
            if isinstance(value, int)
        ],
        default=None,
    )
    response_stale = bool(state_refresh.get("stale")) or any(
        bool(item.get("stale")) for item in [follower, *leader_payloads]
    )

    return {
        "last_updated_at": response_at.isoformat(),
        "lastUpdatedAt": response_at.isoformat(),
        "data_source": "db_latest_state",
        "dataSource": "db_latest_state",
        "updated_at": response_at.isoformat(),
        "updatedAt": response_at.isoformat(),
        "data_age_ms": data_age_ms,
        "dataAgeMs": data_age_ms,
        "stale": response_stale,
        "refresh_in_progress": bool(state_refresh.get("refresh_in_progress")),
        "refreshInProgress": bool(state_refresh.get("refresh_in_progress")),
        "last_error": state_refresh.get("last_error"),
        "lastError": state_refresh.get("last_error"),
        "runtime": {
            "trading_enabled": settings.trading_enabled,
            "hyperliquid_trading_enabled": settings.hyperliquid_trading_enabled,
            "binance_trading_enabled": settings.binance_trading_enabled,
            "dry_run_or_live": "live" if settings.trading_enabled and not kill_switch else "dry-run",
            "kill_switch": kill_switch,
            "kill_switch_updated_at": risk.get("kill_switch_updated_at"),
            "live_opens_enabled": bool(
                settings.trading_enabled
                and settings.hyperliquid_trading_enabled
                and not kill_switch
            ),
            "hyperliquid_default_leverage": str(settings.hyperliquid_default_leverage),
            "hyperliquid_default_margin_mode": settings.hyperliquid_default_margin_mode,
            "follower_migration": follower_migration,
        },
        "state_refresh": state_refresh,
        "stateRefresh": state_refresh,
        "follower": follower,
        "leaders": leader_payloads,
        "active_allocations": [
            {
                "leader_address": row.leader_address,
                "coin": row.hyperliquid_coin,
                "symbol": row.binance_symbol or row.venue_symbol,
                "execution_venue": row.execution_venue,
                "execution_account": row.venue_account,
                "dex": row.dex,
                "canonical_coin": row.canonical_coin,
                "position_side": row.position_side,
                "target_notional": str(row.target_notional),
                "allocated_notional": str(row.allocated_notional),
                "allocated_qty": str(row.allocated_qty),
                "avg_entry_price": str(row.avg_entry_price) if row.avg_entry_price is not None else None,
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
            }
            for row in allocations
        ],
        "recent_orders": [_dashboard_order_payload(row) for row in recent_orders],
        "latency": latency_summary,
        "baseline": baseline_summary,
        "preflight_blockers": preflight_blockers,
        "small_live_start_checklist": checklist,
    }


def _overview_position_payload(
    row: LatestAccountPosition,
    *,
    account_state: LatestAccountState | None,
    leader_address: str | None = None,
    attribution: str = "MANUAL",
) -> dict[str, object]:
    """Strip raw exchange payloads and compatibility aliases from Overview."""

    payload = position_payload(row, account_state=account_state)
    raw_position = row.raw_payload_masked if isinstance(row.raw_payload_masked, dict) else {}
    cumulative_funding = raw_position.get("cumFunding")
    funding_since_open = (
        cumulative_funding.get("sinceOpen")
        if isinstance(cumulative_funding, dict)
        else None
    )
    if funding_since_open is None:
        funding_since_open = raw_position.get("fundingSinceOpen")
    if funding_since_open is None:
        funding_since_open = raw_position.get("funding_since_open")
    funding_pnl_since_open = None
    if funding_since_open is not None:
        try:
            # Hyperliquid cumFunding is recorded from the account debit
            # perspective.  Operator-facing PnL uses the inverse convention:
            # funding received is positive and funding paid is negative.
            funding_pnl_since_open = -Decimal(str(funding_since_open))
        except Exception:
            funding_pnl_since_open = None
    return_on_equity = raw_position.get("returnOnEquity")
    if return_on_equity is None:
        return_on_equity = raw_position.get("return_on_equity")
    if return_on_equity is None:
        margin_used = row.margin_used
        if margin_used is not None and margin_used != 0 and row.unrealized_pnl is not None:
            return_on_equity = row.unrealized_pnl / abs(margin_used)
    return {
        "dex": payload.get("dex"),
        "canonical_coin": payload.get("canonical_coin"),
        "coin": payload.get("coin"),
        "side": payload.get("side"),
        "size": payload.get("size"),
        "notional": payload.get("notional"),
        "entry_px": payload.get("entry_px"),
        "mark_px": payload.get("mark_px"),
        "unrealized_pnl": payload.get("unrealized_pnl"),
        "return_on_equity": str(return_on_equity) if return_on_equity is not None else None,
        "funding_since_open": (
            str(funding_pnl_since_open)
            if funding_pnl_since_open is not None
            else None
        ),
        "leverage": payload.get("leverage"),
        "margin_used": payload.get("margin_used"),
        "margin_mode": payload.get("margin_mode"),
        "liquidation_px": payload.get("liquidation_px"),
        "data_age_ms": payload.get("data_age_ms"),
        "leader_address": leader_address,
        "attribution": attribution,
    }


def _overview_position_owner_key(
    *,
    execution_scope: str | None,
    dex: str | None,
    canonical_coin: str | None,
    side: str | None,
) -> tuple[str, str, str, str]:
    return (
        str(execution_scope or "").strip().lower(),
        str(dex or "").strip().lower(),
        str(canonical_coin or "").strip().upper(),
        str(side or "").strip().upper(),
    )


def _dashboard_order_payload(row: ExecutionOrder) -> dict[str, object]:
    return {
        "id": row.id,
        "allocation_id": row.allocation_id,
        "leader_address": row.leader_address,
        "source_coin": row.source_coin,
        "execution_venue": row.execution_venue,
        "execution_account": row.venue_account,
        "dex": row.dex,
        "canonical_coin": row.canonical_coin,
        "venue_symbol": row.venue_symbol,
        "side": row.side,
        "order_action": row.order_action,
        "status": row.status,
        "dry_run": row.dry_run,
        "reason": row.error_message,
        "avg_fill_price": str(row.avg_fill_price) if row.avg_fill_price is not None else None,
        "leader_entry_px": str(row.leader_entry_px) if row.leader_entry_px is not None else None,
        "follower_avg_entry_px": (
            str(row.follower_avg_entry_px)
            if row.follower_avg_entry_px is not None
            else None
        ),
        "event_to_ack_ms": row.event_to_ack_ms,
        "ws_to_submit_ms": row.ws_to_submit_ms,
        "submit_to_ack_ms": row.submit_to_ack_ms,
        "total_hot_path_ms": row.total_hot_path_ms,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _dashboard_position_with_baseline(
    *,
    leader: LeaderConfig,
    dex: str,
    position: dict[str, object],
    baseline_by_scope: dict[tuple[int, str, str, str], object],
    latest_copy_orders: dict[tuple[str, str, str], ExecutionOrder],
) -> dict[str, object]:
    canonical = str(position.get("canonical_coin") or position.get("coin") or "").upper()
    baseline = baseline_by_scope.get(
        baseline_scope_key(
            leader_id=leader.id,
            execution_venue=ExecutionVenue.HYPERLIQUID.value,
            dex=dex,
            canonical_coin=canonical,
        )
    )
    decision = baseline_status_for_position(
        baseline=baseline,
        copy_allowed_by_config=is_coin_allowed(leader, canonical),
    )
    latest_order = latest_copy_orders.get(
        copy_order_key(leader_address=leader.leader_address, dex=dex, canonical_coin=canonical)
    )
    return {**position, **decision, **copy_order_status_payload(latest_order)}


def _dashboard_latency_summary(orders: list[ExecutionOrder]) -> dict[str, object]:
    last_10 = orders[:10]
    event_to_ack = [int(row.event_to_ack_ms) for row in last_10 if row.event_to_ack_ms is not None]
    stage_values: dict[str, list[int]] = {
        "ws_to_submit_ms": [int(row.ws_to_submit_ms) for row in last_10 if row.ws_to_submit_ms is not None],
        "submit_to_ack_ms": [int(row.submit_to_ack_ms) for row in last_10 if row.submit_to_ack_ms is not None],
        "decision_ms": [int(row.decision_ms) for row in last_10 if row.decision_ms is not None],
        "total_hot_path_ms": [int(row.total_hot_path_ms) for row in last_10 if row.total_hot_path_ms is not None],
    }
    worst_stage = None
    worst_value = None
    for stage, values in stage_values.items():
        if not values:
            continue
        value = max(values)
        if worst_value is None or value > worst_value:
            worst_stage = stage
            worst_value = value
    latest = last_10[0] if last_10 else None
    return {
        "latest_event_to_ack_ms": latest.event_to_ack_ms if latest else None,
        "latest_ws_to_submit_ms": latest.ws_to_submit_ms if latest else None,
        "latest_submit_to_ack_ms": latest.submit_to_ack_ms if latest else None,
        "latest_total_hot_path_ms": latest.total_hot_path_ms if latest else None,
        "last_10_avg_event_to_ack_ms": int(sum(event_to_ack) / len(event_to_ack)) if event_to_ack else None,
        "last_10_max_event_to_ack_ms": max(event_to_ack) if event_to_ack else None,
        "worst_stage": worst_stage,
        "worst_stage_ms": worst_value,
    }


def _position_with_live_price(position: dict[str, object], *, live_price_cache: dict[str, object]) -> dict[str, object]:
    canonical_raw = str(position.get("canonical_coin") or position.get("canonicalCoin") or position.get("coin") or "")
    canonical = canonical_raw.upper()
    price_row = live_price_cache.get(canonical_raw) or live_price_cache.get(canonical)
    if price_row is None:
        upper_prices = {str(key).upper(): value for key, value in live_price_cache.items()}
        price_row = upper_prices.get(canonical) or upper_prices.get(canonical.split(":", 1)[-1])
    if not isinstance(price_row, dict) or price_row.get("stale"):
        return position
    price = price_row.get("price")
    try:
        live_price = Decimal(str(price))
        size = Decimal(str(position.get("size") or "0"))
        current_notional = Decimal(str(position.get("notional") or "0"))
    except Exception:
        return position
    side_sign = Decimal("-1") if current_notional < 0 or str(position.get("side", "")).upper() == "SHORT" else Decimal("1")
    estimated_notional = abs(size) * live_price * side_sign
    next_position = dict(position)
    price_text = str(live_price)
    notional_text = str(estimated_notional.quantize(Decimal("0.00000001")))
    next_position.update(
        {
            "mark_px": price_text,
            "markPx": price_text,
            "mid_px": price_text,
            "midPx": price_text,
            "mark_px_source": "LIVE_PRICE_CACHE",
            "markPxSource": "LIVE_PRICE_CACHE",
            "mark_price_estimated": True,
            "markPriceEstimated": True,
            "mark_price_stale": False,
            "markPriceStale": False,
            "notional": notional_text,
            "notional_estimated": True,
            "notionalEstimated": True,
            "price_cache_age_ms": price_row.get("age_ms"),
            "priceCacheAgeMs": price_row.get("age_ms"),
        }
    )
    return next_position
