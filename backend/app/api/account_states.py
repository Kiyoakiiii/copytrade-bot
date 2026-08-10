from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from app.api.deps import AppSettings, CurrentUser, DbSession
from app.models import AppSetting, ExecutionOrder, LatestAccountPosition, LatestAccountState, LeaderConfig, LeaderPositionAllocationRecord
from app.services.baseline import baselines_by_scope_for_leaders, baseline_scope_key, baseline_status_for_position
from app.services.account_abstraction import load_account_abstraction_state, resolved_value_payload
from app.services.account_state import FOLLOWER, LEADER, account_state_payload, load_account_state_with_positions, position_payload
from app.services.calculator import (
    SIZING_MODE_ACCOUNT_RATIO,
    calculate_leader_position_ratio,
    calculate_target_notional_by_account_ratio,
)
from app.services.copy_execution_status import (
    copy_order_key,
    copy_order_status_payload,
    latest_copy_orders_by_market,
    latest_copy_orders_for_markets,
)
from app.services.leader_config import (
    active_leaders_statement,
    allowed_coins_mode,
    decimal_to_string,
    is_coin_allowed,
    normalize_leader_address,
)
from app.services.execution_router import ExecutionVenue
from app.services.hyperliquid_dex import HyperliquidDexRegistry, canonical_coin, dex_display_name, mask_address
from app.services.watcher_status import (
    watcher_active_leaders_by_scope,
    watcher_execution_scope,
    watcher_statuses_by_scope,
)
from app.tasks.leader_state_poller import (
    monitoring_account_state_stale_seconds,
    schedule_account_state_refresh_if_stale,
)

router = APIRouter(prefix="/account-states", tags=["account-states"])


@router.get("/follower")
async def follower_account_state(
    _: CurrentUser,
    db: DbSession,
    settings: AppSettings,
    include_closed: bool = Query(False),
):
    refresh_status = await schedule_account_state_refresh_if_stale(settings)
    address = settings.hyperliquid_follower_account_address()
    if not address:
        return {
            "role": FOLLOWER,
            "address": None,
            "account_label": "My Hyperliquid Follower Account",
            "account_value": None,
            "withdrawable": None,
            "total_ntl_pos": None,
            "total_raw_usd": None,
            "total_margin_used": None,
            "positions": [],
            "updated_at": None,
            "data_age_ms": None,
            "source": None,
            "stale": True,
            "data_source": "db_latest_state",
            "dataSource": "db_latest_state",
            "refresh_in_progress": bool(refresh_status.get("refresh_in_progress")),
            "refreshInProgress": bool(refresh_status.get("refresh_in_progress")),
            "last_error": refresh_status.get("last_error"),
            "lastError": refresh_status.get("last_error"),
            "error_message": "Hyperliquid follower account is not configured",
            "live_ready_impact": "BLOCKED",
            "debug": _follower_config_debug(settings, None, []),
        }
    account_abstraction = await load_account_abstraction_state(db, role=FOLLOWER, address=address)
    dex_states = await _account_state_payloads_for_address(
        db,
        role=FOLLOWER,
        address=address,
        settings=settings,
        extra_for_each={"configured": True},
        account_abstraction=account_abstraction,
        include_closed=include_closed,
    )
    state, positions = await load_account_state_with_positions(
        db,
        role=FOLLOWER,
        address=address,
        dex="",
        include_closed=include_closed,
    )
    spot_debug = await _load_follower_spot_debug(db)
    payload = account_state_payload(
        state,
        positions,
        stale_seconds=settings.account_state_stale_seconds,
        include_closed=include_closed,
        extra={
            "live_ready_impact": "BLOCKED" if state is None else "OK",
            "dex_states": dex_states,
            "dexStates": dex_states,
            "debug": _follower_config_debug(
                settings,
                state,
                dex_states,
                spot_debug=spot_debug,
                account_abstraction=account_abstraction,
            ),
            **_account_abstraction_fields(account_abstraction, dex=""),
        },
    )
    if payload["stale"] or payload["error_message"]:
        payload["live_ready_impact"] = "BLOCKED"
    payload["positions"] = [position for item in dex_states for position in item.get("positions", [])]
    payload.update(
        {
            "data_source": "db_latest_state",
            "dataSource": "db_latest_state",
            "refresh_in_progress": bool(refresh_status.get("refresh_in_progress")),
            "refreshInProgress": bool(refresh_status.get("refresh_in_progress")),
            "last_error": refresh_status.get("last_error"),
            "lastError": refresh_status.get("last_error"),
        }
    )
    return payload


@router.get("/follower/debug")
async def follower_account_debug(_: CurrentUser, db: DbSession, settings: AppSettings):
    address = settings.hyperliquid_follower_account_address()
    state = None
    dex_states: list[dict[str, Any]] = []
    account_abstraction = None
    if address:
        account_abstraction = await load_account_abstraction_state(db, role=FOLLOWER, address=address)
        dex_states = await _account_state_payloads_for_address(
            db,
            role=FOLLOWER,
            address=address,
            settings=settings,
            extra_for_each={"configured": True},
            account_abstraction=account_abstraction,
        )
        state, _ = await load_account_state_with_positions(db, role=FOLLOWER, address=address, dex="")
    return _follower_config_debug(
        settings,
        state,
        dex_states,
        spot_debug=await _load_follower_spot_debug(db),
        account_abstraction=account_abstraction,
    )


@router.get("/leaders")
async def leader_account_states(
    _: CurrentUser,
    db: DbSession,
    settings: AppSettings,
    include_closed: bool = Query(False),
    compact: bool = Query(True),
):
    await schedule_account_state_refresh_if_stale(
        settings,
        max_age_seconds=monitoring_account_state_stale_seconds(settings),
    )
    leaders = (await db.execute(active_leaders_statement())).scalars().all()
    return await _leader_state_payloads(
        db,
        leaders,
        settings=settings,
        include_closed=include_closed,
        compact=compact,
    )


@router.get("/leaders/{leader_id}")
async def leader_account_state(
    leader_id: int,
    _: CurrentUser,
    db: DbSession,
    settings: AppSettings,
    include_closed: bool = Query(False),
):
    await schedule_account_state_refresh_if_stale(
        settings,
        max_age_seconds=monitoring_account_state_stale_seconds(settings),
    )
    leader = await db.get(LeaderConfig, leader_id)
    if not leader:
        raise HTTPException(status_code=404, detail="leader not found")
    payloads = await _leader_state_payloads(
        db,
        [leader],
        settings=settings,
        include_allocations=True,
        include_closed=include_closed,
    )
    return payloads[0]


async def _leader_state_payloads(
    db: DbSession,
    leaders: list[LeaderConfig],
    *,
    settings: AppSettings,
    include_allocations: bool = False,
    include_closed: bool = False,
    compact: bool = False,
) -> list[dict[str, Any]]:
    monitoring_stale_seconds = monitoring_account_state_stale_seconds(settings)
    leader_addresses = [
        normalize_leader_address(leader.leader_address)
        for leader in leaders
    ]
    if not leader_addresses:
        return []
    state_rows = (
        await db.execute(
            select(LatestAccountState)
            .where(LatestAccountState.role == LEADER)
            .where(LatestAccountState.address.in_(leader_addresses))
        )
    ).scalars().all()
    states_by_address: dict[str, list[LatestAccountState]] = defaultdict(list)
    for row in state_rows:
        states_by_address[row.address.lower()].append(row)
    state_ids = [row.id for row in state_rows]
    positions_by_state: dict[int, list[LatestAccountPosition]] = defaultdict(list)
    if state_ids:
        for position in (
            await db.execute(
                select(LatestAccountPosition)
                .where(LatestAccountPosition.account_state_id.in_(state_ids))
                .where(True if include_closed else LatestAccountPosition.active.is_(True))
                .order_by(LatestAccountPosition.coin)
            )
        ).scalars().all():
            positions_by_state[position.account_state_id].append(position)

    allocations_by_leader: dict[str, list[LeaderPositionAllocationRecord]] = defaultdict(list)
    if include_allocations:
        for allocation in (
            await db.execute(
                select(LeaderPositionAllocationRecord)
                .where(LeaderPositionAllocationRecord.status != "CLOSED")
                .order_by(LeaderPositionAllocationRecord.hyperliquid_coin)
            )
        ).scalars().all():
            allocations_by_leader[allocation.leader_address.lower()].append(allocation)
    follower_address = (
        settings.hyperliquid_follower_account_address()
        if include_allocations
        else None
    )
    follower_states_by_dex: dict[str, LatestAccountState] = {}
    follower_abstraction: dict[str, Any] | None = None
    if follower_address:
        follower_abstraction = await load_account_abstraction_state(db, role=FOLLOWER, address=follower_address)
        follower_rows = (
            await db.execute(
                select(LatestAccountState)
                .where(LatestAccountState.role == FOLLOWER)
                .where(LatestAccountState.address == follower_address.lower())
            )
        ).scalars().all()
        follower_states_by_dex = {row.dex: row for row in follower_rows}

    watcher_rows = (
        await db.execute(
            select(AppSetting).where(AppSetting.key.like("watcher_status%"))
        )
    ).scalars().all()
    watcher_active_by_scope = watcher_active_leaders_by_scope(
        watcher_statuses_by_scope(watcher_rows)
    )
    baseline_by_scope = await baselines_by_scope_for_leaders(db, [leader.id for leader in leaders])
    if compact:
        visible_market_keys = {
            copy_order_key(
                leader_address=position.address,
                dex=position.dex,
                canonical_coin=(
                    position.canonical_coin or position.coin
                ),
            )
            for positions in positions_by_state.values()
            for position in positions
        }
        latest_copy_orders = await latest_copy_orders_for_markets(
            db,
            visible_market_keys,
        )
    else:
        latest_copy_orders = await latest_copy_orders_by_market(db, leaders)

    payloads = []
    for leader in leaders:
        address = normalize_leader_address(leader.leader_address)
        leader_abstraction = (
            None
            if compact
            else await load_account_abstraction_state(
                db,
                role=LEADER,
                address=address,
            )
        )
        leader_states = sorted(states_by_address.get(address, []), key=lambda row: row.dex)
        state = next((row for row in leader_states if row.dex == ""), leader_states[0] if leader_states else None)
        positions_with_state = [
            (row, position)
            for row in leader_states
            for position in positions_by_state.get(row.id, [])
        ]
        if compact:
            dex_state_payloads = [
                _compact_account_state_summary(
                    row,
                    positions_by_state.get(row.id, []),
                    stale_seconds=monitoring_stale_seconds,
                    include_closed=include_closed,
                )
                for row in leader_states
            ]
            base = _compact_account_state_summary(
                state,
                [],
                stale_seconds=monitoring_stale_seconds,
                include_closed=include_closed,
            )
            base.update(
                {
                    "leader": _leader_config_payload(leader),
                    "watcher_status": "active"
                    if address
                    in watcher_active_by_scope.get(
                        watcher_execution_scope(leader.hyperliquid_vault_address),
                        set(),
                    )
                    else "not_subscribed",
                    "dex_states": dex_state_payloads,
                }
            )
        else:
            dex_state_payloads = [
                account_state_payload(
                    row,
                    positions_by_state.get(row.id, []),
                    stale_seconds=monitoring_stale_seconds,
                    include_closed=include_closed,
                    extra=_account_abstraction_fields(leader_abstraction, dex=row.dex),
                )
                for row in leader_states
            ]
            base = account_state_payload(
                state,
                [],
                stale_seconds=monitoring_stale_seconds,
                include_closed=include_closed,
                extra={
                    "leader": _leader_config_payload(leader),
                    "watcher_status": "active"
                    if address
                    in watcher_active_by_scope.get(
                        watcher_execution_scope(leader.hyperliquid_vault_address),
                        set(),
                    )
                    else "not_subscribed",
                    "dex_states": dex_state_payloads,
                    "dexStates": dex_state_payloads,
                    **_account_abstraction_fields(leader_abstraction, dex=state.dex if state else ""),
                },
            )
        position_payloads = [
            _position_with_baseline_payload(
                leader=leader,
                state_row=state_row,
                row=row,
                follower_state=follower_states_by_dex.get(state_row.dex),
                allocations=allocations_by_leader.get(address, []),
                leader_resolved=resolved_value_payload(leader_abstraction, state_row.dex),
                follower_resolved=resolved_value_payload(follower_abstraction, state_row.dex),
                baseline_by_scope=baseline_by_scope,
                latest_copy_orders=latest_copy_orders,
                include_allocations=include_allocations,
            )
            for state_row, row in positions_with_state
        ]
        base["positions"] = (
            [_compact_leader_position_payload(item) for item in position_payloads]
            if compact
            else position_payloads
        )
        if include_allocations:
            base["allocations"] = [
                _allocation_payload(allocation)
                for allocation in allocations_by_leader.get(address, [])
            ]
        payloads.append(base)
    return payloads


_COMPACT_ACCOUNT_STATE_KEYS = (
    "role",
    "address",
    "dex",
    "dex_display_name",
    "account_label",
    "account_value",
    "withdrawable",
    "total_ntl_pos",
    "total_raw_usd",
    "total_margin_used",
    "updated_at",
    "data_age_ms",
    "source",
    "stale",
    "error_message",
)

_COMPACT_LEADER_POSITION_KEYS = (
    "dex",
    "dex_display_name",
    "coin",
    "canonical_coin",
    "product_type",
    "side",
    "size",
    "notional",
    "entry_px",
    "mark_px",
    "mid_px",
    "mark_price_stale",
    "open_time",
    "first_seen_at",
    "open_time_source",
    "updated_at",
    "data_age_ms",
    "unrealized_pnl",
    "copyable",
    "coin_allowed",
    "venue_route",
    "copy_reason",
    "copy_status",
    "baseline_status",
    "baseline_id",
    "last_copy_order_display_status",
    "last_copy_order_reason",
)


def _compact_account_state_summary(
    state: LatestAccountState | None,
    positions: list[LatestAccountPosition],
    *,
    stale_seconds: int,
    include_closed: bool,
) -> dict[str, Any]:
    """Return only fields consumed by the multi-leader overview.

    The rich account-abstraction and raw position payloads remain available on
    the single-leader detail endpoint.  Repeating them for every DEX and again
    under camelCase aliases made the overview response multi-megabyte and
    caused UI refreshes to contend with latency-sensitive processes.
    """

    payload = account_state_payload(
        state,
        [],
        stale_seconds=stale_seconds,
        include_closed=include_closed,
    )
    summary = {key: payload.get(key) for key in _COMPACT_ACCOUNT_STATE_KEYS}
    position_count = sum(
        1
        for position in positions
        if include_closed or bool(getattr(position, "active", False))
    )
    # Preserve the old overview client's only use of nested positions
    # (``positions.length``) while omitting every heavy position field.
    summary["positions"] = [{} for _ in range(position_count)]
    summary["position_count"] = position_count
    return summary


def _compact_leader_position_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: payload.get(key)
        for key in _COMPACT_LEADER_POSITION_KEYS
    }


def _position_with_baseline_payload(
    *,
    leader: LeaderConfig,
    state_row: LatestAccountState,
    row: LatestAccountPosition,
    follower_state: LatestAccountState | None,
    allocations: list[LeaderPositionAllocationRecord],
    leader_resolved: dict[str, Any] | None,
    follower_resolved: dict[str, Any] | None,
    baseline_by_scope: dict[tuple[int, str, str, str], Any],
    latest_copy_orders: dict[tuple[str, str, str], ExecutionOrder],
    include_allocations: bool,
) -> dict[str, Any]:
    position = position_payload(row, account_state=state_row)
    copy_decision = _copy_decision(leader, position["canonical_coin"])
    baseline = baseline_by_scope.get(
        baseline_scope_key(
            leader_id=leader.id,
            execution_venue=ExecutionVenue.HYPERLIQUID.value,
            dex=state_row.dex,
            canonical_coin=position["canonical_coin"],
        )
    )
    baseline_decision = baseline_status_for_position(
        baseline=baseline,
        copy_allowed_by_config=bool(copy_decision["copyable"]),
    )
    merged_decision = {
        **copy_decision,
        **baseline_decision,
        "venue_route": copy_decision.get("venue_route"),
    }
    sizing = None
    if include_allocations and merged_decision["copyable"]:
        sizing = _sizing_payload(
            leader_state=state_row,
            position=position,
            leader=leader,
            follower_state=follower_state,
            allocations=allocations,
            leader_resolved=leader_resolved,
            follower_resolved=follower_resolved,
        )
    latest_order = latest_copy_orders.get(
        copy_order_key(
            leader_address=leader.leader_address,
            dex=state_row.dex,
            canonical_coin=position["canonical_coin"],
        )
    )
    return {
        **position,
        **merged_decision,
        **copy_order_status_payload(latest_order),
        "sizing": sizing,
        "allocation": _allocation_payload_for_coin(
            allocations,
            position["canonical_coin"],
        )
        if include_allocations
        else None,
    }


def _leader_config_payload(leader: LeaderConfig) -> dict[str, Any]:
    return {
        "id": leader.id,
        "leader_address": leader.leader_address,
        "enabled": leader.enabled,
        "deleted_at": leader.deleted_at.isoformat() if leader.deleted_at else None,
        "copy_multiplier": decimal_to_string(leader.copy_multiplier),
        "fixed_account_value": decimal_to_string(leader.fixed_account_value),
        "allowed_symbols": leader.allowed_symbols,
        "blocked_symbols": leader.blocked_symbols or [],
        "allowed_coins_mode": allowed_coins_mode(leader),
        "preferred_venue": leader.preferred_venue,
        "fallback_venue": leader.fallback_venue,
        "max_notional_per_trade": decimal_to_string(leader.max_notional_per_trade),
        "max_total_notional": decimal_to_string(leader.max_total_notional),
    }


def _copy_decision(leader: LeaderConfig, coin: str) -> dict[str, Any]:
    allowed = is_coin_allowed(leader, coin)
    if not allowed:
        return {
            "copyable": False,
            "coin_allowed": False,
            "venue_route": None,
            "copy_reason": "coin blocked by leader allowlist/blocklist or leader disabled",
        }
    preferred = str(leader.preferred_venue or "HYPERLIQUID").upper()
    return {
        "copyable": True,
        "coin_allowed": True,
        "venue_route": preferred,
        "copy_reason": "coin allowed; Hyperliquid meta/preflight still gates execution",
    }


def _allocation_payload_for_coin(
    allocations: list[LeaderPositionAllocationRecord],
    coin: str,
) -> dict[str, Any] | None:
    matches = [
        allocation
        for allocation in allocations
        if (allocation.canonical_coin or allocation.hyperliquid_coin).upper() == coin.upper()
    ]
    if not matches:
        return None
    target = sum((allocation.target_notional for allocation in matches), Decimal("0"))
    allocated = sum((allocation.allocated_notional for allocation in matches), Decimal("0"))
    return {
        "target_notional": str(target),
        "allocated_notional": str(allocated),
        "sides": sorted({allocation.position_side for allocation in matches}),
        "statuses": sorted({allocation.status for allocation in matches}),
    }


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
        "follower_account_value": str(follower_account_value) if follower_account_value is not None else None,
        "follower_account_value_used_for_sizing": str(follower_account_value) if follower_account_value is not None else None,
        "follower_account_value_source": (follower_resolved or {}).get("account_value_source")
        or (follower_resolved or {}).get("source"),
        "follower_account_abstraction_mode": (follower_resolved or {}).get("account_abstraction_mode")
        or (follower_resolved or {}).get("mode"),
        "copy_multiplier": decimal_to_string(leader.copy_multiplier),
        "current_allocation": str(current_allocation),
        "current_allocation_notional": str(current_allocation),
        "target_notional": None,
        "calculated_target_notional": None,
        "delta_notional": None,
        "leader_position_ratio": None,
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


def _allocation_payload(allocation: LeaderPositionAllocationRecord) -> dict[str, Any]:
    return {
        "leader_address": allocation.leader_address,
        "coin": allocation.hyperliquid_coin,
        "dex": allocation.dex,
        "dex_display_name": dex_display_name(allocation.dex),
        "canonical_coin": allocation.canonical_coin or canonical_coin(dex=allocation.dex, coin=allocation.hyperliquid_coin),
        "symbol": allocation.binance_symbol or allocation.venue_symbol,
        "execution_venue": allocation.execution_venue,
        "venue_symbol": allocation.venue_symbol,
        "position_side": allocation.position_side,
        "target_notional": str(allocation.target_notional),
        "allocated_notional": str(allocation.allocated_notional),
        "allocated_qty": str(allocation.allocated_qty),
        "avg_entry_price": str(allocation.avg_entry_price) if allocation.avg_entry_price is not None else None,
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
        "status": allocation.status,
    }


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or str(value) == "":
        return None
    return Decimal(str(value))


async def _account_state_payloads_for_address(
    db: DbSession,
    *,
    role: str,
    address: str,
    settings: AppSettings,
    extra_for_each: dict[str, Any] | None = None,
    account_abstraction: dict[str, Any] | None = None,
    include_closed: bool = False,
    stale_seconds: int | None = None,
) -> list[dict[str, Any]]:
    effective_stale_seconds = (
        int(stale_seconds)
        if stale_seconds is not None
        else int(settings.account_state_stale_seconds)
    )
    states = (
        await db.execute(
            select(LatestAccountState)
            .where(LatestAccountState.role == role.upper())
            .where(LatestAccountState.address == address.lower())
            .order_by(LatestAccountState.dex)
        )
    ).scalars().all()
    positions_by_state: dict[int, list[LatestAccountPosition]] = defaultdict(list)
    if states:
        for position in (
            await db.execute(
                select(LatestAccountPosition)
                .where(LatestAccountPosition.account_state_id.in_([row.id for row in states]))
                .where(True if include_closed else LatestAccountPosition.active.is_(True))
                .order_by(LatestAccountPosition.dex, LatestAccountPosition.coin)
            )
        ).scalars().all():
            positions_by_state[position.account_state_id].append(position)
    payloads = [
        account_state_payload(
            state,
            positions_by_state.get(state.id, []),
            stale_seconds=effective_stale_seconds,
            include_closed=include_closed,
            extra={**(extra_for_each or {}), **_account_abstraction_fields(account_abstraction, dex=state.dex)},
        )
        for state in states
    ]
    loaded_dexes = {item.get("dex") for item in payloads}
    for dex in HyperliquidDexRegistry(settings).enabled_dexes():
        if dex.dex_name not in loaded_dexes:
            payloads.append(
                {
                    "role": role.upper(),
                    "address": address.lower(),
                    "dex": dex.dex_name,
                    "dex_display_name": dex.display_name,
                    "dexDisplayName": dex.display_name,
                    "account_label": None,
                    "account_value": None,
                    "accountValue": None,
                    "withdrawable": None,
                    "total_ntl_pos": None,
                    "totalNtlPos": None,
                    "total_margin_used": None,
                    "totalMarginUsed": None,
                    "positions": [],
                    "updated_at": None,
                    "updatedAt": None,
                    "data_age_ms": None,
                    "dataAgeMs": None,
                    "source": None,
                    "stale": True,
                    "error_message": "account state unavailable",
                    "errorMessage": "account state unavailable",
                    **(extra_for_each or {}),
                    **_account_abstraction_fields(account_abstraction, dex=dex.dex_name),
                }
            )
    return sorted(payloads, key=lambda item: str(item.get("dex") or ""))


def _follower_config_debug(
    settings: AppSettings,
    state: LatestAccountState | None,
    dex_states: list[dict[str, Any]],
    *,
    spot_debug: dict[str, Any] | None = None,
    account_abstraction: dict[str, Any] | None = None,
) -> dict[str, Any]:
    account_value = state.account_value if state else None
    spot_usdc_total = spot_debug.get("usdc_total") if spot_debug else None
    resolved = resolved_value_payload(account_abstraction, "") or {}
    resolved_value = _decimal_or_none(
        resolved.get("account_value_used_for_sizing") or resolved.get("account_value")
    )
    resolved_blockers = resolved.get("blockers") or []
    resolved_mode = str(
        resolved.get("account_abstraction_mode")
        or resolved.get("mode")
        or (account_abstraction or {}).get("account_abstraction_mode")
        or ""
    ).upper()
    likely_issue = "OK"
    if settings.hyperliquid_follower_address_ambiguous():
        likely_issue = "Follower account address is ambiguous. Set HYPERLIQUID_ACCOUNT_ADDRESS explicitly."
    elif resolved_blockers:
        likely_issue = "; ".join(str(item) for item in resolved_blockers)
    elif account_value is not None and account_value == 0:
        if resolved_value is not None and resolved_value > 0:
            likely_issue = "OK" if resolved_mode in {"UNIFIED", "PORTFOLIO", "INFERRED_UNIFIED"} else (
                "accountValue is 0, but another account value source is available. Confirm account abstraction before live."
            )
        elif spot_usdc_total and spot_usdc_total != "0":
            likely_issue = (
                f"USDC balance {spot_usdc_total} is visible in spotClearinghouseState while per-dex "
                "clearinghouse accountValue is 0. Account abstraction is not confirmed, so live opening stays blocked."
            )
        else:
            likely_issue = (
                "accountValue is 0. Check HYPERLIQUID_ACCOUNT_ADDRESS, API wallet vs main account, "
                "mainnet/testnet, vault/subaccount, and whether funds are in another Hyperliquid account state."
            )
    elif state is None or state.error_message:
        likely_issue = "account state not loaded"
    return {
        "network": settings.hyperliquid_execution_network,
        "configured_account_address_masked": mask_address(settings.hyperliquid_account_address),
        "derived_signer_address_masked": mask_address(settings.hyperliquid_signer_address()),
        "api_wallet_address_masked": mask_address(settings.hyperliquid_api_wallet_address),
        "vault_address_masked": mask_address(settings.hyperliquid_vault_address),
        "subaccount_address_masked": mask_address(settings.hyperliquid_subaccount_address),
        "account_state_query_address_masked": mask_address(settings.hyperliquid_follower_account_address()),
        "signer_type": settings.hyperliquid_signer_type(),
        "accountValue": str(account_value) if account_value is not None else None,
        "withdrawable": str(state.withdrawable) if state and state.withdrawable is not None else None,
        "spot_usdc_total": spot_usdc_total,
        "spot_source": spot_debug.get("source") if spot_debug else None,
        "spot_updated_at": spot_debug.get("updated_at") if spot_debug else None,
        "account_abstraction": account_abstraction,
        **_account_abstraction_fields(account_abstraction, dex=""),
        "assetPositions_count": sum(len(item.get("positions", [])) for item in dex_states),
        "state_loaded": bool(state and not state.error_message),
        "state_error": state.error_message if state else None,
        "address_ambiguous": settings.hyperliquid_follower_address_ambiguous(),
        "dex_states": dex_states,
        "likely_issue": likely_issue,
    }


async def _load_follower_spot_debug(db: DbSession) -> dict[str, Any] | None:
    row = await db.get(AppSetting, "follower_spot_state")
    if not row or not isinstance(row.value, dict):
        return None
    payload = dict(row.value)
    payload.pop("address", None)
    return payload


def _account_abstraction_fields(payload: dict[str, Any] | None, dex: str = "") -> dict[str, Any]:
    if not payload:
        return {}
    resolved = resolved_value_payload(payload, dex) or {}
    spot_balances = payload.get("spot_balances") or payload.get("spotBalances") or {}
    spot_usdc = spot_balances.get("USDC") if isinstance(spot_balances, dict) else {}
    clearing = payload.get("clearinghouse_by_dex") or payload.get("clearinghouseByDex") or {}
    default_clearing = clearing.get("") if isinstance(clearing, dict) else {}
    xyz_clearing = clearing.get("xyz") if isinstance(clearing, dict) else {}
    return {
        "account_abstraction": payload,
        "accountAbstraction": payload,
        "account_abstraction_mode": resolved.get("account_abstraction_mode")
        or resolved.get("mode")
        or payload.get("account_abstraction_mode"),
        "accountAbstractionMode": resolved.get("accountAbstractionMode")
        or resolved.get("mode")
        or payload.get("accountAbstractionMode"),
        "account_value_used_for_sizing": resolved.get("account_value_used_for_sizing")
        or resolved.get("account_value"),
        "accountValueUsedForSizing": resolved.get("accountValueUsedForSizing")
        or resolved.get("accountValue"),
        "available_collateral_used_for_margin_check": resolved.get(
            "available_collateral_used_for_margin_check"
        )
        or resolved.get("withdrawable_or_available"),
        "availableCollateralUsedForMarginCheck": resolved.get(
            "availableCollateralUsedForMarginCheck"
        )
        or resolved.get("withdrawableOrAvailable"),
        "balance_source": resolved.get("account_value_source")
        or resolved.get("source")
        or payload.get("balance_source"),
        "balanceSource": resolved.get("accountValueSource")
        or resolved.get("source")
        or payload.get("balanceSource"),
        "margin_source": payload.get("margin_source"),
        "marginSource": payload.get("marginSource"),
        "confidence": resolved.get("confidence"),
        "account_value_source": resolved.get("account_value_source") or resolved.get("source"),
        "accountValueSource": resolved.get("accountValueSource") or resolved.get("source"),
        "portfolio_account_value": payload.get("portfolio_account_value"),
        "portfolioAccountValue": payload.get("portfolioAccountValue"),
        "portfolio_state_available": payload.get("portfolio_state_available"),
        "portfolioStateAvailable": payload.get("portfolioStateAvailable"),
        "spot_state_available": payload.get("spot_state_available"),
        "spotStateAvailable": payload.get("spotStateAvailable"),
        "clearinghouse_state_available": payload.get("clearinghouse_state_available"),
        "clearinghouseStateAvailable": payload.get("clearinghouseStateAvailable"),
        "spot_usdc_total": spot_usdc.get("total") if isinstance(spot_usdc, dict) else None,
        "spotUsdcTotal": spot_usdc.get("total") if isinstance(spot_usdc, dict) else None,
        "spot_usdc_hold": spot_usdc.get("hold") if isinstance(spot_usdc, dict) else None,
        "spotUsdcHold": spot_usdc.get("hold") if isinstance(spot_usdc, dict) else None,
        "spot_usdc_available": spot_usdc.get("available") if isinstance(spot_usdc, dict) else None,
        "spotUsdcAvailable": spot_usdc.get("available") if isinstance(spot_usdc, dict) else None,
        "clearinghouse_default_account_value": default_clearing.get("account_value")
        if isinstance(default_clearing, dict)
        else None,
        "clearinghouseDefaultAccountValue": default_clearing.get("accountValue")
        if isinstance(default_clearing, dict)
        else None,
        "clearinghouse_xyz_account_value": xyz_clearing.get("account_value")
        if isinstance(xyz_clearing, dict)
        else None,
        "clearinghouseXyzAccountValue": xyz_clearing.get("accountValue")
        if isinstance(xyz_clearing, dict)
        else None,
        "warnings": resolved.get("warnings") or payload.get("warnings") or [],
        "blockers": resolved.get("blockers") or [],
    }
