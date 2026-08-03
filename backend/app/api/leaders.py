from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from sqlalchemy import func, select

from app.api.deps import AppSettings, CurrentUser, DbSession
from app.models import AppSetting, LatestLeaderState, LeaderConfig, LeaderPositionAllocationRecord, LeaderPositionBaseline
from app.schemas.api import LeaderCreate, LeaderDelete, LeaderPatch, LeaderReplace
from app.services.baseline import (
    BASELINE_WAIT_UNTIL_FLAT,
    capture_leader_position_baselines,
)
from app.services.hyperliquid import HyperliquidInfoClient
from app.services.leader_performance import (
    load_leader_performance_cache,
    load_single_leader_performance_cache,
)
from app.services.leader_config import (
    ADDRESS_RE,
    allowed_coins_mode,
    decimal_to_string,
    disable_leader,
    enable_leader,
    normalize_allowed_symbols_for_storage,
    normalize_blocked_symbols_for_storage,
    normalize_leader_address,
    soft_delete_leader,
)

router = APIRouter(prefix="/leaders", tags=["leaders"])


@router.get("")
async def list_leaders(
    _: CurrentUser,
    db: DbSession,
    include_deleted: bool = Query(default=False),
):
    leaders = await _load_leaders(db, include_deleted=include_deleted)
    return await _leaders_payload(db, leaders)


@router.get("/performance")
async def get_leader_performance(_: CurrentUser, db: DbSession):
    """Return the cached, sanitized since-join analytics for all current leaders."""
    return await load_leader_performance_cache(db)


@router.get("/execution-accounts")
async def get_execution_accounts(_: CurrentUser, db: DbSession, settings: AppSettings):
    """Return public, operator-selectable execution accounts.

    The route value is deliberately empty for the main account because the
    durable trading tables use the historical empty scope for main-account
    activity.  Only public account addresses are returned; signing material is
    never part of this payload.
    """
    subaccounts = settings.hyperliquid_execution_subaccount_address_list()
    main_address = str(
        settings.hyperliquid_account_address
        or settings.hyperliquid_signer_address()
        or ""
    ).lower() or None
    if main_address:
        subaccounts = [address for address in subaccounts if address != main_address]

    status_keys = ["watcher_status", *(f"watcher_status:{address}" for address in subaccounts)]
    status_rows = (
        await db.execute(select(AppSetting).where(AppSetting.key.in_(status_keys)))
    ).scalars().all()
    statuses = {
        row.key: row.value if isinstance(row.value, dict) else {}
        for row in status_rows
    }

    def item(*, route_value: str, account_address: str | None, account_type: str) -> dict[str, Any]:
        status_key = f"watcher_status:{route_value}" if route_value else "watcher_status"
        status = statuses.get(status_key, {})
        suffix = account_address[-4:] if account_address else "unknown"
        label = "Main account" if account_type == "MAIN" else f"Subaccount · {suffix}"
        return {
            "route_value": route_value,
            "account_address": account_address,
            "account_type": account_type,
            "label": label,
            "watcher_running": bool(status.get("low_latency_watcher_running")),
            "watcher_ready": bool(status.get("ready_for_low_latency_live")),
            "active_leaders": status.get("active_leaders") or [],
        }

    return [
        item(route_value="", account_address=main_address, account_type="MAIN"),
        *[
            item(route_value=address, account_address=address, account_type="SUBACCOUNT")
            for address in subaccounts
        ],
    ]


@router.post("")
async def create_leader(_: CurrentUser, payload: LeaderCreate, db: DbSession, settings: AppSettings):
    address = _validate_leader_address(payload.leader_address)
    existing = await _leader_by_address(db, address)
    data = _normalized_payload(payload.model_dump())
    _validate_patch(data)
    _validate_execution_account_route(data, settings=settings)
    activation_time = datetime.now(timezone.utc)

    if existing and existing.deleted_at is None:
        raise HTTPException(status_code=409, detail="leader address already exists")
    if existing:
        await _ensure_execution_route_change_safe(
            db,
            leader=existing,
            new_route=data.get("hyperliquid_vault_address"),
        )
        for key, value in data.items():
            setattr(existing, key, value)
        existing.leader_address = address
        existing.deleted_at = None
        existing.delete_reason = None
        existing.performance_started_at = activation_time
        leader = existing
    else:
        leader = LeaderConfig(**data, performance_started_at=activation_time)
        leader.leader_address = address
        db.add(leader)

    await db.flush()
    if leader.enabled and leader.deleted_at is None:
        await _capture_baseline_for_leader(
            db,
            leader=leader,
            settings=settings,
            reason="leader add/restore baseline capture",
            force_reset=True,
        )
    await db.commit()
    await db.refresh(leader)
    return (await _leaders_payload(db, [leader]))[0]


@router.post("/replace-active")
async def replace_active_leader(_: CurrentUser, payload: LeaderReplace, db: DbSession, settings: AppSettings):
    address = _validate_leader_address(payload.leader_address)
    overrides = _normalized_payload(payload.model_dump(exclude_unset=True))
    overrides.pop("leader_address", None)
    overrides.pop("allow_unmanaged_existing_allocations", None)
    requested_route = str(overrides.get("hyperliquid_vault_address") or "").lower()
    _validate_patch(overrides)
    _validate_execution_account_route(overrides, settings=settings)

    route_filter = func.lower(func.coalesce(LeaderConfig.hyperliquid_vault_address, "")) == requested_route
    active_leaders = (
        await db.execute(
            select(LeaderConfig)
            .where(LeaderConfig.enabled.is_(True))
            .where(LeaderConfig.deleted_at.is_(None))
            .where(route_filter)
            .order_by(LeaderConfig.created_at.desc())
        )
    ).scalars().all()
    active_addresses = [normalize_leader_address(leader.leader_address) for leader in active_leaders]
    if active_addresses and not payload.allow_unmanaged_existing_allocations:
        open_allocations_count = await db.scalar(
            select(func.count())
            .select_from(LeaderPositionAllocationRecord)
            .where(LeaderPositionAllocationRecord.leader_address.in_(active_addresses))
            .where(LeaderPositionAllocationRecord.status != "CLOSED")
        )
        if open_allocations_count:
            raise HTTPException(
                status_code=409,
                detail=(
                    "active leader has open copied allocations; close them or confirm replacement with "
                    "allow_unmanaged_existing_allocations=true"
                ),
            )

    existing = await _leader_by_address(db, address)
    if existing:
        await _ensure_execution_route_change_safe(
            db,
            leader=existing,
            new_route=requested_route or None,
        )
    template = active_leaders[0] if active_leaders else existing
    data = _replacement_data(template)
    data.update(overrides)
    # Omitting the field means main-account replacement. Do not accidentally
    # inherit an old/deleted leader's subaccount route through the template.
    data["hyperliquid_vault_address"] = requested_route or None
    data["enabled"] = True
    _validate_patch(data)
    _validate_execution_account_route(data, settings=settings)
    activation_time = datetime.now(timezone.utc)

    if existing:
        leader = existing
        for key, value in data.items():
            setattr(leader, key, value)
        leader.leader_address = address
        leader.deleted_at = None
        leader.delete_reason = None
        leader.performance_started_at = activation_time
    else:
        leader = LeaderConfig(**data, performance_started_at=activation_time)
        leader.leader_address = address
        db.add(leader)

    for old_leader in active_leaders:
        if existing and int(old_leader.id) == int(existing.id):
            continue
        disable_leader(old_leader)

    await db.flush()
    baseline = await _capture_baseline_for_leader(
        db,
        leader=leader,
        settings=settings,
        reason="leader atomic replacement baseline capture",
        force_reset=True,
    )
    if not baseline.ready:
        raise HTTPException(status_code=502, detail=f"replacement baseline capture failed: {baseline.error}")
    await db.commit()
    await db.refresh(leader)
    return (await _leaders_payload(db, [leader]))[0]


@router.get("/{leader_id}")
async def get_leader(leader_id: int, _: CurrentUser, db: DbSession):
    leader = await _get_leader_or_404(db, leader_id)
    return (await _leaders_payload(db, [leader]))[0]


@router.get("/{leader_id}/performance")
async def get_single_leader_performance(leader_id: int, _: CurrentUser, db: DbSession):
    await _get_leader_or_404(db, leader_id)
    payload = await load_single_leader_performance_cache(db, leader_id)
    if payload is not None:
        return payload
    overview = await load_leader_performance_cache(db)
    return {
        "schema_version": overview.get("schema_version", 1),
        "status": "warming",
        "generated_at": overview.get("generated_at"),
        "methodology": overview.get("methodology") or {},
        "leader": None,
    }


@router.patch("/{leader_id}")
async def patch_leader(leader_id: int, _: CurrentUser, payload: LeaderPatch, db: DbSession, settings: AppSettings):
    leader = await _get_leader_or_404(db, leader_id)
    was_active = leader.enabled and leader.deleted_at is None
    old_allowed = list(leader.allowed_symbols or []) if leader.allowed_symbols is not None else None
    old_preferred = leader.preferred_venue
    old_enabled_venues = list(leader.enabled_venues or [])
    data = _normalized_payload(payload.model_dump(exclude_unset=True))
    _validate_patch(data)
    _validate_execution_account_route(
        data,
        settings=settings,
        current_route=leader.hyperliquid_vault_address,
    )
    if "hyperliquid_vault_address" in data:
        await _ensure_execution_route_change_safe(
            db,
            leader=leader,
            new_route=data["hyperliquid_vault_address"],
        )
    for key, value in data.items():
        setattr(leader, key, value)
    is_active = leader.enabled and leader.deleted_at is None
    if is_active and not was_active:
        leader.performance_started_at = datetime.now(timezone.utc)
    if is_active and (
        (not was_active)
        or _baseline_relevant_patch(
            data,
            old_allowed=old_allowed,
            old_preferred=old_preferred,
            old_enabled_venues=old_enabled_venues,
        )
    ):
        await db.flush()
        await _capture_baseline_for_leader(
            db,
            leader=leader,
            settings=settings,
            reason="leader config changed baseline capture",
            force_reset=not was_active,
        )
    await db.commit()
    await db.refresh(leader)
    return (await _leaders_payload(db, [leader]))[0]


@router.delete("/{leader_id}")
async def delete_leader(
    leader_id: int,
    _: CurrentUser,
    db: DbSession,
    payload: LeaderDelete | None = Body(default=None),
):
    leader = await _get_leader_or_404(db, leader_id)
    soft_delete_leader(leader, reason=payload.delete_reason if payload else None)
    await db.commit()
    await db.refresh(leader)
    return (await _leaders_payload(db, [leader]))[0]


@router.post("/{leader_id}/enable")
async def enable_leader_endpoint(leader_id: int, _: CurrentUser, db: DbSession, settings: AppSettings):
    leader = await _get_leader_or_404(db, leader_id)
    _validate_execution_account_route(
        {"hyperliquid_vault_address": leader.hyperliquid_vault_address},
        settings=settings,
    )
    enable_leader(leader)
    await db.flush()
    await _capture_baseline_for_leader(
        db,
        leader=leader,
        settings=settings,
        reason="leader enable baseline capture",
        force_reset=True,
    )
    await db.commit()
    await db.refresh(leader)
    return (await _leaders_payload(db, [leader]))[0]


@router.post("/{leader_id}/disable")
async def disable_leader_endpoint(leader_id: int, _: CurrentUser, db: DbSession):
    leader = await _get_leader_or_404(db, leader_id)
    disable_leader(leader)
    await db.commit()
    await db.refresh(leader)
    return (await _leaders_payload(db, [leader]))[0]


async def _load_leaders(db: DbSession, *, include_deleted: bool) -> list[LeaderConfig]:
    query = select(LeaderConfig).order_by(LeaderConfig.created_at.desc())
    if not include_deleted:
        query = query.where(LeaderConfig.deleted_at.is_(None))
    return (await db.execute(query)).scalars().all()


async def _get_leader_or_404(db: DbSession, leader_id: int) -> LeaderConfig:
    leader = await db.get(LeaderConfig, leader_id)
    if not leader:
        raise HTTPException(status_code=404, detail="leader not found")
    return leader


async def _leader_by_address(db: DbSession, address: str) -> LeaderConfig | None:
    return (
        await db.execute(
            select(LeaderConfig).where(func.lower(LeaderConfig.leader_address) == address.lower())
        )
    ).scalar_one_or_none()


def _validate_leader_address(address: str) -> str:
    normalized = normalize_leader_address(address)
    if not ADDRESS_RE.fullmatch(normalized):
        raise HTTPException(status_code=400, detail="leader_address must be a 0x-prefixed EVM address")
    return normalized


def _normalized_payload(data: dict[str, Any]) -> dict[str, Any]:
    result = dict(data)
    if "leader_address" in result:
        result["leader_address"] = _validate_leader_address(result["leader_address"])
    if "allowed_symbols" in result:
        result["allowed_symbols"] = normalize_allowed_symbols_for_storage(result["allowed_symbols"])
    if "blocked_symbols" in result:
        result["blocked_symbols"] = normalize_blocked_symbols_for_storage(result["blocked_symbols"])
    if "preferred_venue" in result and result["preferred_venue"] is not None:
        result["preferred_venue"] = str(result["preferred_venue"]).upper()
    if "fallback_venue" in result and result["fallback_venue"] is not None:
        result["fallback_venue"] = str(result["fallback_venue"]).upper()
    if "enabled_venues" in result and result["enabled_venues"] is not None:
        result["enabled_venues"] = [str(item).upper() for item in result["enabled_venues"]]
    if "hyperliquid_vault_address" in result:
        value = str(result["hyperliquid_vault_address"] or "").strip().lower()
        result["hyperliquid_vault_address"] = value or None
    return result


def _validate_patch(data: dict[str, Any]) -> None:
    if "copy_multiplier" in data and data["copy_multiplier"] is not None and data["copy_multiplier"] <= 0:
        raise HTTPException(status_code=400, detail="copy_multiplier must be > 0")
    if "fixed_account_value" in data and (
        data["fixed_account_value"] is None or data["fixed_account_value"] <= 0
    ):
        raise HTTPException(status_code=400, detail="fixed_account_value must be > 0")
    if "preferred_venue" in data and data["preferred_venue"] not in {None, "HYPERLIQUID", "BINANCE", "AUTO"}:
        raise HTTPException(status_code=400, detail="preferred_venue must be HYPERLIQUID, BINANCE, or AUTO")
    if "fallback_venue" in data and data["fallback_venue"] not in {None, "NONE", "BINANCE", "HYPERLIQUID"}:
        raise HTTPException(status_code=400, detail="fallback_venue must be NONE, BINANCE, or HYPERLIQUID")
    if (
        "hyperliquid_vault_address" in data
        and data["hyperliquid_vault_address"] is not None
        and not ADDRESS_RE.fullmatch(data["hyperliquid_vault_address"])
    ):
        raise HTTPException(
            status_code=400,
            detail="execution account must be a 0x-prefixed EVM address",
        )


def _validate_execution_account_route(
    data: dict[str, Any],
    *,
    settings: AppSettings,
    current_route: str | None = None,
) -> None:
    if "hyperliquid_vault_address" not in data:
        return
    route = str(data.get("hyperliquid_vault_address") or "").strip().lower()
    if not route:
        return
    if current_route and route == str(current_route).strip().lower():
        return

    main_address = str(settings.hyperliquid_account_address or "").strip().lower()
    if main_address and route == main_address:
        raise HTTPException(
            status_code=400,
            detail="select Main account instead of entering the main-account address as a subaccount",
        )
    allowed_routes = set(settings.hyperliquid_execution_subaccount_address_list())
    if route not in allowed_routes:
        raise HTTPException(
            status_code=400,
            detail="execution account is not a configured and verified subaccount",
        )


async def _ensure_execution_route_change_safe(
    db: DbSession,
    *,
    leader: LeaderConfig,
    new_route: str | None,
) -> None:
    old_route = str(leader.hyperliquid_vault_address or "").strip().lower()
    normalized_new_route = str(new_route or "").strip().lower()
    if old_route == normalized_new_route:
        return
    if leader.enabled and leader.deleted_at is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "disable the leader before changing its execution account; "
                "this prevents overlapping watcher subscriptions"
            ),
        )
    open_allocations = int(
        await db.scalar(
            select(func.count())
            .select_from(LeaderPositionAllocationRecord)
            .where(LeaderPositionAllocationRecord.leader_address == leader.leader_address.lower())
            .where(LeaderPositionAllocationRecord.status != "CLOSED")
        )
        or 0
    )
    if open_allocations:
        raise HTTPException(
            status_code=409,
            detail="execution account cannot change while copied allocations are open",
        )


async def _leaders_payload(db: DbSession, leaders: list[LeaderConfig]) -> list[dict[str, Any]]:
    state_rows = {
        row.leader_address.lower(): row
        for row in (await db.execute(select(LatestLeaderState))).scalars().all()
    }
    allocations = (
        await db.execute(select(LeaderPositionAllocationRecord))
    ).scalars().all()
    baselines = (
        await db.execute(select(LeaderPositionBaseline))
    ).scalars().all()
    allocation_stats: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"count": 0, "notional": Decimal("0")}
    )
    for allocation in allocations:
        if str(allocation.status).upper() == "CLOSED":
            continue
        address = allocation.leader_address.lower()
        allocation_stats[address]["count"] += 1
        allocation_stats[address]["notional"] += abs(allocation.allocated_notional or Decimal("0"))
    baseline_stats: dict[int, dict[str, int]] = defaultdict(
        lambda: {"waiting_until_flat_count": 0, "baseline_rows_count": 0}
    )
    for baseline in baselines:
        if baseline.leader_id is None:
            continue
        baseline_stats[int(baseline.leader_id)]["baseline_rows_count"] += 1
        if str(baseline.baseline_status).upper() == BASELINE_WAIT_UNTIL_FLAT:
            baseline_stats[int(baseline.leader_id)]["waiting_until_flat_count"] += 1

    watcher_rows = (
        await db.execute(
            select(AppSetting).where(AppSetting.key.like("watcher_status%"))
        )
    ).scalars().all()
    watcher_active_by_scope: dict[str, set[str]] = {}
    for watcher_row in watcher_rows:
        scope = (
            watcher_row.key.removeprefix("watcher_status:").lower()
            if watcher_row.key.startswith("watcher_status:")
            else ""
        )
        value = watcher_row.value if isinstance(watcher_row.value, dict) else {}
        watcher_active_by_scope[scope] = {
            normalize_leader_address(address)
            for address in value.get("active_leaders", [])
        }

    return [
        _leader_payload(
            leader,
            latest_state=state_rows.get(leader.leader_address.lower()),
            allocation_stats=allocation_stats.get(leader.leader_address.lower(), {"count": 0, "notional": Decimal("0")}),
            baseline_stats=baseline_stats.get(int(leader.id), {"waiting_until_flat_count": 0, "baseline_rows_count": 0}),
            watcher_active=watcher_active_by_scope.get(
                str(leader.hyperliquid_vault_address or "").lower(),
                set(),
            ),
        )
        for leader in leaders
    ]


def _leader_payload(
    leader: LeaderConfig,
    *,
    latest_state: LatestLeaderState | None,
    allocation_stats: dict[str, Any],
    watcher_active: set[str],
    baseline_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    address = leader.leader_address.lower()
    baseline_stats = baseline_stats or {"waiting_until_flat_count": 0, "baseline_rows_count": 0}
    deleted = leader.deleted_at is not None
    if deleted:
        watcher_status = "deleted"
    elif not leader.enabled:
        watcher_status = "disabled"
    elif address in watcher_active:
        watcher_status = "active"
    else:
        watcher_status = "not_subscribed"
    return {
        "id": leader.id,
        "enabled": leader.enabled,
        "deleted_at": leader.deleted_at.isoformat() if leader.deleted_at else None,
        "delete_reason": leader.delete_reason,
        "performance_started_at": leader.performance_started_at.isoformat(),
        "leader_address": leader.leader_address,
        "copy_multiplier": decimal_to_string(leader.copy_multiplier),
        "fixed_account_value": decimal_to_string(leader.fixed_account_value),
        "allowed_symbols": leader.allowed_symbols,
        "blocked_symbols": leader.blocked_symbols or [],
        "allowed_coins_mode": allowed_coins_mode(leader),
        "max_notional_per_trade": decimal_to_string(leader.max_notional_per_trade),
        "max_total_notional": decimal_to_string(leader.max_total_notional),
        "max_leverage": leader.max_leverage,
        "slippage_bps": leader.slippage_bps,
        "preferred_venue": leader.preferred_venue,
        "fallback_venue": leader.fallback_venue,
        "enabled_venues": leader.enabled_venues or [],
        "hyperliquid_account_id": leader.hyperliquid_account_id,
        "hyperliquid_vault_address": leader.hyperliquid_vault_address,
        "hyperliquid_vault_address_configured": bool(leader.hyperliquid_vault_address),
        "watcher_status": watcher_status,
        "last_state_update": latest_state.last_update_at.isoformat() if latest_state else None,
        "positions_loaded": bool(latest_state and latest_state.positions),
        "current_allocations_count": allocation_stats["count"],
        "open_allocations_notional": decimal_to_string(allocation_stats["notional"]),
        "baseline_rows_count": baseline_stats["baseline_rows_count"],
        "waiting_until_flat_count": baseline_stats["waiting_until_flat_count"],
    }


async def _capture_baseline_for_leader(
    db: DbSession,
    *,
    leader: LeaderConfig,
    settings: AppSettings,
    reason: str,
    force_reset: bool,
) -> Any:
    client = HyperliquidInfoClient(settings.hyperliquid_info_url)
    try:
        return await capture_leader_position_baselines(
            db,
            leader=leader,
            settings=settings,
            info_client=client,
            reason=reason,
            force_reset=force_reset,
        )
    finally:
        await client.close()


def _replacement_data(template: LeaderConfig | None) -> dict[str, Any]:
    if template is None:
        return LeaderCreate(leader_address="0x" + "0" * 40).model_dump(exclude={"leader_address"})
    return {
        "enabled": True,
        "copy_multiplier": template.copy_multiplier,
        "fixed_account_value": template.fixed_account_value,
        "allowed_symbols": list(template.allowed_symbols or []) if template.allowed_symbols is not None else None,
        "blocked_symbols": list(template.blocked_symbols or []),
        "max_notional_per_trade": template.max_notional_per_trade,
        "max_total_notional": template.max_total_notional,
        "max_leverage": template.max_leverage,
        "slippage_bps": template.slippage_bps,
        "preferred_venue": template.preferred_venue,
        "fallback_venue": template.fallback_venue,
        "enabled_venues": list(template.enabled_venues or ["HYPERLIQUID"]),
        "hyperliquid_account_id": template.hyperliquid_account_id,
        "hyperliquid_vault_address": template.hyperliquid_vault_address,
    }


def _baseline_relevant_patch(
    data: dict[str, Any],
    *,
    old_allowed: list[str] | None,
    old_preferred: str,
    old_enabled_venues: list[str],
) -> bool:
    if "allowed_symbols" in data and data["allowed_symbols"] != old_allowed:
        return True
    # Coin blocks are lifecycle-aware in the fill watcher: existing
    # allocations continue, while a position without an allocation cannot be
    # joined midway through its lifecycle. A slow external baseline capture is
    # therefore unnecessary for a blocked-list-only edit.
    if "preferred_venue" in data and data["preferred_venue"] != old_preferred:
        return True
    if "enabled_venues" in data and data["enabled_venues"] != old_enabled_venues:
        return True
    return False
