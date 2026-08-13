from __future__ import annotations

import asyncio
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Any, Iterable

import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.core.logging import redact_text
from app.db.session import SessionLocal
from app.models import (
    AllocationEvent,
    AppSetting,
    ExecutionOrder,
    LatestAccountPosition,
    LatestAccountState,
    LeaderConfig,
    LeaderPositionAllocationRecord,
    SourceFill,
    SourceFillOutcome,
    UnmatchedFollowerFill,
)
from app.services.account_state import FOLLOWER, LEADER
from app.services.hyperliquid import HyperliquidInfoClient
from app.services.hyperliquid_dex import parse_coin
from app.services.task_status import store_task_status


log = structlog.get_logger(__name__)

LEADER_PERFORMANCE_CACHE_KEY = "leader_performance:v1"
LEADER_PERFORMANCE_EXCHANGE_CACHE_KEY = "leader_performance:exchange_cache:v1"
LEADER_PERFORMANCE_TASK_NAME = "leader_performance"
PERFORMANCE_SCHEMA_VERSION = 1
PERFORMANCE_EXCHANGE_CACHE_SCHEMA_VERSION = 1
PERFORMANCE_INCREMENTAL_OVERLAP_MS = 60_000
MAIN_EXECUTION_SCOPE_CACHE_KEY = "__main__"
USER_FILLS_PAGE_SIZE = 2000
USER_FUNDING_PAGE_SIZE = 500
ZERO = Decimal("0")
POSITION_TOLERANCE = Decimal("0.00000001")


async def load_leader_performance_cache(db: Any) -> dict[str, Any]:
    row = await db.get(AppSetting, LEADER_PERFORMANCE_CACHE_KEY)
    if row is None or not isinstance(row.value, dict):
        return _warming_payload()
    return dict(row.value)


async def load_single_leader_performance_cache(db: Any, leader_id: int) -> dict[str, Any] | None:
    payload = await load_leader_performance_cache(db)
    for item in payload.get("leaders") or []:
        if int(item.get("leader_id") or 0) == int(leader_id):
            return {
                "schema_version": payload.get("schema_version", PERFORMANCE_SCHEMA_VERSION),
                "status": payload.get("status", "ready"),
                "generated_at": payload.get("generated_at"),
                "methodology": payload.get("methodology") or _methodology_payload(),
                "leader": item,
            }
    return None


async def refresh_leader_performance(settings: Any) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    async with SessionLocal() as db:
        leaders = (
            await db.execute(
                select(LeaderConfig)
                .where(LeaderConfig.deleted_at.is_(None))
                .order_by(LeaderConfig.id)
            )
        ).scalars().all()
        if not leaders:
            payload = _empty_payload(now)
            await _store_performance_payload(db, payload)
            await db.commit()
            return payload

        leader_ids = [int(leader.id) for leader in leaders]
        addresses = [str(leader.leader_address).lower() for leader in leaders]
        performance_start = {
            int(leader.id): _as_utc(
                leader.performance_started_at or leader.created_at
            )
            for leader in leaders
        }
        joined_ms = {
            leader_id: int(started_at.timestamp() * 1000)
            for leader_id, started_at in performance_start.items()
        }
        earliest_join_ms = min(joined_ms.values())
        source_fills = (
            await db.execute(
                select(SourceFill)
                .where(SourceFill.leader_address.in_(addresses))
                .where(SourceFill.source_time_ms >= earliest_join_ms)
                .where(SourceFill.is_snapshot.is_(False))
                .order_by(SourceFill.source_time_ms, SourceFill.id)
            )
        ).scalars().all()
        source_ids = [row.source_fill_id for row in source_fills]
        outcomes = (
            await db.execute(
                select(SourceFillOutcome).where(SourceFillOutcome.source_fill_id.in_(source_ids))
            )
        ).scalars().all() if source_ids else []
        orders = (
            await db.execute(
                select(ExecutionOrder)
                .where(ExecutionOrder.leader_id.in_(leader_ids))
                .where(ExecutionOrder.source_type == "AUTO_COPY")
                .where(ExecutionOrder.execution_venue == "HYPERLIQUID")
                .order_by(ExecutionOrder.created_at, ExecutionOrder.id)
            )
        ).scalars().all()
        allocation_events = (
            await db.execute(
                select(AllocationEvent)
                .where(AllocationEvent.leader_id.in_(leader_ids))
                .order_by(AllocationEvent.created_at, AllocationEvent.id)
            )
        ).scalars().all()
        active_allocations = (
            await db.execute(
                select(LeaderPositionAllocationRecord)
                .where(LeaderPositionAllocationRecord.leader_id.in_(leader_ids))
                .where(LeaderPositionAllocationRecord.execution_venue == "HYPERLIQUID")
                .where(LeaderPositionAllocationRecord.status != "CLOSED")
                .where(LeaderPositionAllocationRecord.allocated_qty > ZERO)
            )
        ).scalars().all()
        account_states = (
            await db.execute(
                select(LatestAccountState).where(
                    LatestAccountState.role.in_([LEADER, FOLLOWER])
                )
            )
        ).scalars().all()
        state_ids = [row.id for row in account_states]
        account_positions = (
            await db.execute(
                select(LatestAccountPosition)
                .where(LatestAccountPosition.account_state_id.in_(state_ids))
                .where(LatestAccountPosition.active.is_(True))
                .where(LatestAccountPosition.status == "OPEN")
            )
        ).scalars().all() if state_ids else []
        unmatched_fills = (
            await db.execute(select(UnmatchedFollowerFill))
        ).scalars().all()
        exchange_cache = await _load_performance_exchange_cache(db)

    fills_by_address: dict[str, list[SourceFill]] = defaultdict(list)
    for row in source_fills:
        fills_by_address[str(row.leader_address).lower()].append(row)
    outcomes_by_source = {row.source_fill_id: row for row in outcomes}
    orders_by_leader: dict[int, list[ExecutionOrder]] = defaultdict(list)
    for row in orders:
        if row.leader_id is not None:
            orders_by_leader[int(row.leader_id)].append(row)
    allocation_events_by_leader: dict[int, list[AllocationEvent]] = defaultdict(list)
    for row in allocation_events:
        if row.leader_id is not None:
            allocation_events_by_leader[int(row.leader_id)].append(row)

    states_by_id = {row.id: row for row in account_states}
    leader_positions_by_address: dict[str, list[LatestAccountPosition]] = defaultdict(list)
    follower_positions_by_account: dict[str, list[LatestAccountPosition]] = defaultdict(list)
    # Analytics spans every execution account.  The historical empty scope is
    # always the main account even if another process is configured with an
    # explicit subaccount route.
    default_follower_address = str(
        settings.hyperliquid_account_address
        or settings.hyperliquid_follower_account_address()
        or ""
    ).lower()
    for position in account_positions:
        state = states_by_id.get(position.account_state_id)
        if state is None:
            continue
        if state.role == LEADER:
            leader_positions_by_address[str(state.address).lower()].append(position)
        elif state.role == FOLLOWER:
            follower_positions_by_account[str(state.address).lower()].append(position)

    execution_accounts = _performance_execution_scopes(
        leaders=leaders,
        orders=orders,
        active_allocations=active_allocations,
    )
    follower_address_by_scope = {
        scope: (scope if scope else default_follower_address)
        for scope in execution_accounts | {""}
    }
    joined_ms_by_scope: dict[str, int] = {}
    for leader in leaders:
        scope = str(leader.hyperliquid_vault_address or "").strip().lower()
        leader_joined_ms = joined_ms[int(leader.id)]
        joined_ms_by_scope[scope] = min(
            joined_ms_by_scope.get(scope, leader_joined_ms),
            leader_joined_ms,
        )
    follower_open_by_leader: dict[int, dict[str, Any]] = defaultdict(_empty_open_attribution)
    for execution_scope, follower_address in follower_address_by_scope.items():
        if not follower_address:
            continue
        scoped_open = _follower_open_attribution(
            active_allocations=[
                row
                for row in active_allocations
                if str(row.venue_account or "").lower() == execution_scope
            ],
            follower_positions=follower_positions_by_account.get(follower_address, []),
            manual_scopes={
                (str(row.dex or "").lower(), str(row.canonical_coin or "").upper())
                for row in unmatched_fills
                if str(row.execution_account or "").lower() == execution_scope
            },
        )
        for leader_id, values in scoped_open.items():
            target = follower_open_by_leader[leader_id]
            target["unrealized_pnl"] += values["unrealized_pnl"]
            target["notional"] += values["notional"]
            target["manual_sync"] = target["manual_sync"] or values["manual_sync"]

    updated_exchange_cache = deepcopy(exchange_cache)
    updated_exchange_cache["schema_version"] = PERFORMANCE_EXCHANGE_CACHE_SCHEMA_VERSION
    follower_cache_by_scope = updated_exchange_cache.setdefault("follower_scopes", {})
    funding_cache_by_leader = updated_exchange_cache.setdefault("leader_funding", {})
    portfolio_cache_by_leader = updated_exchange_cache.setdefault("leader_portfolios", {})
    cache_stats = {
        "follower_fill_api_rows": 0,
        "follower_fill_cached_rows": 0,
        "funding_api_rows": 0,
        "funding_cached_rows": 0,
        "portfolio_api_requests": 0,
        "portfolio_cache_fallbacks": 0,
    }

    info = HyperliquidInfoClient(
        settings.hyperliquid_info_url,
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
    try:
        end_ms = int(now.timestamp() * 1000)
        actual_fills_by_order: dict[int, list[dict[str, Any]]] = {}
        for execution_scope, follower_address in follower_address_by_scope.items():
            if not follower_address:
                continue
            scope_cache_key = _performance_scope_cache_key(execution_scope)
            required_start_ms = joined_ms_by_scope.get(execution_scope, earliest_join_ms)
            cached_scope = follower_cache_by_scope.get(scope_cache_key)
            if not _valid_incremental_cache_entry(
                cached_scope,
                identity=follower_address,
                required_start_ms=required_start_ms,
            ):
                cached_scope = {
                    "identity": follower_address,
                    "coverage_start_ms": required_start_ms,
                    "cursor_ms": None,
                    "fills_by_order": {},
                }
            scope_orders = [
                order
                for order in orders
                if str(order.venue_account or "").lower() == execution_scope
            ]
            valid_order_ids = {
                int(order.id)
                for order in scope_orders
                if order.id is not None
            }
            cached_fills_by_order = _cached_order_fills(
                cached_scope.get("fills_by_order"),
                valid_order_ids=valid_order_ids,
            )
            incremental_start_ms = _performance_incremental_start_ms(
                cached_scope.get("cursor_ms"),
                required_start_ms=required_start_ms,
                end_ms=end_ms,
            )
            account_fills = await _fetch_user_fills_complete(
                info,
                follower_address,
                incremental_start_ms,
                end_ms,
            )
            cache_stats["follower_fill_api_rows"] += len(account_fills)
            new_fills_by_order = _actual_follower_fills_by_order(
                scope_orders,
                [
                    {**fill, "_copytrade_execution_scope": execution_scope}
                    for fill in account_fills
                ],
            )
            merged_fills_by_order = _merge_order_fill_cache(
                cached_fills_by_order,
                new_fills_by_order,
            )
            for order_id, fills in merged_fills_by_order.items():
                actual_fills_by_order[order_id] = fills
            cache_stats["follower_fill_cached_rows"] += sum(
                len(fills) for fills in merged_fills_by_order.values()
            )
            follower_cache_by_scope[scope_cache_key] = {
                "identity": follower_address,
                "execution_scope": execution_scope,
                "coverage_start_ms": min(
                    int(cached_scope.get("coverage_start_ms") or required_start_ms),
                    required_start_ms,
                ),
                "cursor_ms": end_ms,
                "fills_by_order": {
                    str(order_id): [
                        _compact_follower_fill(fill) for fill in fills
                    ]
                    for order_id, fills in merged_fills_by_order.items()
                },
            }
        portfolio_by_leader: dict[int, Any] = {}
        funding_by_leader: dict[int, list[dict[str, Any]]] = {}
        for leader in leaders:
            leader_id = int(leader.id)
            leader_address = str(leader.leader_address).lower()
            portfolio_cache_key = str(leader_id)
            cached_portfolio = portfolio_cache_by_leader.get(portfolio_cache_key)
            cache_stats["portfolio_api_requests"] += 1
            try:
                portfolio = await info.post_info(
                    {"type": "portfolio", "user": leader.leader_address}
                )
                portfolio_cache_by_leader[portfolio_cache_key] = {
                    "identity": leader_address,
                    "fetched_at_ms": end_ms,
                    "payload": portfolio,
                }
            except Exception:
                if not (
                    isinstance(cached_portfolio, dict)
                    and str(cached_portfolio.get("identity") or "").lower()
                    == leader_address
                    and cached_portfolio.get("payload") is not None
                ):
                    raise
                portfolio = cached_portfolio.get("payload")
                cache_stats["portfolio_cache_fallbacks"] += 1
                log.warning(
                    "leader_performance_portfolio_cache_fallback",
                    leader_id=leader_id,
                    leader_address=str(leader.leader_address)[-4:],
                )
            portfolio_by_leader[leader_id] = portfolio

            funding_cache_key = str(leader_id)
            cached_funding = funding_cache_by_leader.get(funding_cache_key)
            if not _valid_incremental_cache_entry(
                cached_funding,
                identity=leader_address,
                required_start_ms=joined_ms[leader_id],
            ):
                cached_funding = {
                    "identity": leader_address,
                    "coverage_start_ms": joined_ms[leader_id],
                    "cursor_ms": None,
                    "items": [],
                }
            incremental_funding_start_ms = _performance_incremental_start_ms(
                cached_funding.get("cursor_ms"),
                required_start_ms=joined_ms[leader_id],
                end_ms=end_ms,
            )
            new_funding = await _fetch_user_funding_complete(
                info,
                str(leader.leader_address),
                incremental_funding_start_ms,
                end_ms,
            )
            cache_stats["funding_api_rows"] += len(new_funding)
            funding = _merge_funding_cache(
                cached_funding.get("items"),
                new_funding,
                required_start_ms=joined_ms[leader_id],
            )
            funding_by_leader[leader_id] = funding
            cache_stats["funding_cached_rows"] += len(funding)
            funding_cache_by_leader[funding_cache_key] = {
                "identity": leader_address,
                "coverage_start_ms": min(
                    int(cached_funding.get("coverage_start_ms") or joined_ms[leader_id]),
                    joined_ms[leader_id],
                ),
                "cursor_ms": end_ms,
                "items": [_compact_funding_item(item) for item in funding],
            }
    finally:
        await info.close()

    updated_exchange_cache["updated_at"] = now.isoformat()
    updated_exchange_cache["last_refresh_stats"] = cache_stats
    leaders_payload: list[dict[str, Any]] = []
    for leader in leaders:
        leader_id = int(leader.id)
        leader_fills = [
            row
            for row in fills_by_address.get(str(leader.leader_address).lower(), [])
            if int(row.source_time_ms) >= joined_ms[leader_id]
        ]
        leader_orders = [
            row
            for row in orders_by_leader.get(leader_id, [])
            if _as_utc(row.created_at) >= performance_start[leader_id]
        ]
        leader_allocation_events = [
            row
            for row in allocation_events_by_leader.get(leader_id, [])
            if _as_utc(row.created_at) >= performance_start[leader_id]
        ]
        payload = _build_leader_payload(
            leader=leader,
            now=now,
            source_fills=leader_fills,
            outcomes_by_source=outcomes_by_source,
            orders=leader_orders,
            actual_fills_by_order=actual_fills_by_order,
            allocation_events=leader_allocation_events,
            leader_positions=leader_positions_by_address.get(
                str(leader.leader_address).lower(), []
            ),
            follower_open=follower_open_by_leader.get(leader_id, _empty_open_attribution()),
            portfolio=portfolio_by_leader.get(leader_id),
            funding=funding_by_leader.get(leader_id, []),
        )
        leaders_payload.append(payload)

    leaders_payload.sort(
        key=lambda item: (
            -int((item.get("scores") or {}).get("overall") or 0),
            int(item.get("leader_id") or 0),
        )
    )
    payload = {
        "schema_version": PERFORMANCE_SCHEMA_VERSION,
        "status": "ready",
        "generated_at": now.isoformat(),
        "window": "SINCE_JOINED",
        "leaders": leaders_payload,
        "methodology": _methodology_payload(),
    }
    async with SessionLocal() as db:
        await _store_performance_exchange_cache(db, updated_exchange_cache)
        await _store_performance_payload(db, payload)
        await store_task_status(
            db,
            task_name=LEADER_PERFORMANCE_TASK_NAME,
            status="running",
            metadata={
                "generated_at": now.isoformat(),
                "leader_count": len(leaders_payload),
                "window": "SINCE_JOINED",
                "refresh_seconds": float(
                    getattr(settings, "leader_performance_refresh_seconds", 21600.0)
                    or 21600.0
                ),
                "exchange_cache": cache_stats,
            },
        )
        await db.commit()
    return payload


async def run_leader_performance_refresher(settings: Any) -> None:
    interval = max(
        60.0,
        float(getattr(settings, "leader_performance_refresh_seconds", 21600.0) or 21600.0),
    )
    while True:
        async with SessionLocal() as db:
            cached = await load_leader_performance_cache(db)
        delay = performance_refresh_delay_seconds(
            cached,
            now=datetime.now(timezone.utc),
            interval_seconds=interval,
        )
        if delay > 0:
            await asyncio.sleep(max(5.0, delay))
            continue

        started = asyncio.get_running_loop().time()
        try:
            await refresh_leader_performance(settings)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            safe_error = redact_text(exc)
            log.exception("leader_performance_refresh_failed", error=safe_error)
            async with SessionLocal() as db:
                await store_task_status(
                    db,
                    task_name=LEADER_PERFORMANCE_TASK_NAME,
                    status="degraded_retrying",
                    last_error=safe_error[:500],
                    metadata={"window": "SINCE_JOINED"},
                )
                await db.commit()
            await asyncio.sleep(min(interval, 3600.0))
            continue
        elapsed = asyncio.get_running_loop().time() - started
        await asyncio.sleep(max(5.0, interval - elapsed))


def performance_refresh_delay_seconds(
    payload: dict[str, Any] | None,
    *,
    now: datetime,
    interval_seconds: float,
) -> float:
    """Return how long a cache-only worker can wait before its next API refresh."""

    raw_generated_at = (payload or {}).get("generated_at")
    if not raw_generated_at:
        return 0.0
    try:
        generated_at = datetime.fromisoformat(str(raw_generated_at).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return 0.0
    generated_at = _as_utc(generated_at)
    current = _as_utc(now)
    age_seconds = max(0.0, (current - generated_at).total_seconds())
    return max(0.0, float(interval_seconds) - age_seconds)


async def _store_performance_payload(db: Any, payload: dict[str, Any]) -> None:
    now = datetime.now(timezone.utc)
    await db.execute(
        insert(AppSetting)
        .values(key=LEADER_PERFORMANCE_CACHE_KEY, value=payload, updated_at=now)
        .on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={"value": payload, "updated_at": now},
        )
    )


async def _load_performance_exchange_cache(db: Any) -> dict[str, Any]:
    row = await db.get(AppSetting, LEADER_PERFORMANCE_EXCHANGE_CACHE_KEY)
    if row is None or not isinstance(row.value, dict):
        return _empty_performance_exchange_cache()
    payload = dict(row.value)
    if int(payload.get("schema_version") or 0) != PERFORMANCE_EXCHANGE_CACHE_SCHEMA_VERSION:
        return _empty_performance_exchange_cache()
    return payload


async def _store_performance_exchange_cache(
    db: Any,
    payload: dict[str, Any],
) -> None:
    now = datetime.now(timezone.utc)
    await db.execute(
        insert(AppSetting)
        .values(
            key=LEADER_PERFORMANCE_EXCHANGE_CACHE_KEY,
            value=payload,
            updated_at=now,
        )
        .on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={"value": payload, "updated_at": now},
        )
    )


def _empty_performance_exchange_cache() -> dict[str, Any]:
    return {
        "schema_version": PERFORMANCE_EXCHANGE_CACHE_SCHEMA_VERSION,
        "updated_at": None,
        "follower_scopes": {},
        "leader_funding": {},
        "leader_portfolios": {},
    }


def _performance_scope_cache_key(execution_scope: str | None) -> str:
    return str(execution_scope or "").strip().lower() or MAIN_EXECUTION_SCOPE_CACHE_KEY


def _performance_execution_scopes(
    *,
    leaders: Iterable[Any],
    orders: Iterable[Any],
    active_allocations: Iterable[Any],
) -> set[str]:
    scopes = {
        str(getattr(leader, "hyperliquid_vault_address", "") or "")
        .strip()
        .lower()
        for leader in leaders
    }
    scopes.update(
        str(getattr(row, "venue_account", "") or "").strip().lower()
        for row in [*orders, *active_allocations]
    )
    scopes.add("")
    return scopes


def _valid_incremental_cache_entry(
    entry: Any,
    *,
    identity: str,
    required_start_ms: int,
) -> bool:
    if not isinstance(entry, dict):
        return False
    if str(entry.get("identity") or "").strip().lower() != str(identity or "").strip().lower():
        return False
    try:
        coverage_start_ms = int(entry.get("coverage_start_ms"))
    except (TypeError, ValueError):
        return False
    return coverage_start_ms <= int(required_start_ms)


def _performance_incremental_start_ms(
    cursor_ms: Any,
    *,
    required_start_ms: int,
    end_ms: int,
) -> int:
    try:
        cursor = int(cursor_ms)
    except (TypeError, ValueError):
        return int(required_start_ms)
    if cursor <= 0:
        return int(required_start_ms)
    return max(
        int(required_start_ms),
        min(cursor, int(end_ms)) - PERFORMANCE_INCREMENTAL_OVERLAP_MS,
    )


def _cached_order_fills(
    value: Any,
    *,
    valid_order_ids: set[int],
) -> dict[int, list[dict[str, Any]]]:
    if not isinstance(value, dict):
        return {}
    result: dict[int, list[dict[str, Any]]] = {}
    for raw_order_id, raw_fills in value.items():
        try:
            order_id = int(raw_order_id)
        except (TypeError, ValueError):
            continue
        if order_id not in valid_order_ids or not isinstance(raw_fills, list):
            continue
        unique = {
            _exchange_fill_key(fill): dict(fill)
            for fill in raw_fills
            if isinstance(fill, dict)
        }
        result[order_id] = sorted(
            unique.values(),
            key=lambda fill: (int(fill.get("time") or 0), _exchange_fill_key(fill)),
        )
    return result


def _merge_order_fill_cache(
    cached: dict[int, list[dict[str, Any]]],
    fresh: dict[int, list[dict[str, Any]]],
) -> dict[int, list[dict[str, Any]]]:
    result: dict[int, list[dict[str, Any]]] = {}
    for order_id in sorted(set(cached) | set(fresh)):
        unique = {
            _exchange_fill_key(fill): dict(fill)
            for fill in [*cached.get(order_id, []), *fresh.get(order_id, [])]
            if isinstance(fill, dict)
        }
        if unique:
            result[order_id] = sorted(
                unique.values(),
                key=lambda fill: (
                    int(fill.get("time") or 0),
                    _exchange_fill_key(fill),
                ),
            )
    return result


def _compact_follower_fill(fill: dict[str, Any]) -> dict[str, Any]:
    return {
        key: fill.get(key)
        for key in (
            "cloid",
            "oid",
            "time",
            "px",
            "sz",
            "closedPnl",
            "fee",
            "hash",
            "tid",
            "coin",
        )
        if fill.get(key) is not None
    }


def _funding_key(item: dict[str, Any]) -> str:
    delta = item.get("delta") if isinstance(item.get("delta"), dict) else {}
    return "|".join(
        str(value or "")
        for value in (
            item.get("hash"),
            item.get("time"),
            delta.get("coin"),
            delta.get("usdc"),
        )
    )


def _merge_funding_cache(
    cached: Any,
    fresh: list[dict[str, Any]],
    *,
    required_start_ms: int,
) -> list[dict[str, Any]]:
    cached_items = cached if isinstance(cached, list) else []
    unique = {
        _funding_key(item): dict(item)
        for item in [*cached_items, *fresh]
        if isinstance(item, dict)
        and int(item.get("time") or 0) >= int(required_start_ms)
    }
    return sorted(
        unique.values(),
        key=lambda item: (int(item.get("time") or 0), _funding_key(item)),
    )


def _compact_funding_item(item: dict[str, Any]) -> dict[str, Any]:
    delta = item.get("delta") if isinstance(item.get("delta"), dict) else {}
    return {
        "hash": item.get("hash"),
        "time": item.get("time"),
        "delta": {
            "coin": delta.get("coin"),
            "usdc": delta.get("usdc"),
        },
    }


async def _fetch_user_fills_complete(
    info: HyperliquidInfoClient,
    user: str,
    start_ms: int,
    end_ms: int,
    *,
    depth: int = 0,
) -> list[dict[str, Any]]:
    page = await info.user_fills_by_time(
        user,
        start_ms,
        end_time_ms=end_ms,
        aggregate_by_time=False,
    )
    page = [
        fill
        for fill in page
        if start_ms <= int(fill.get("time") or 0) <= end_ms
    ]
    if len(page) < USER_FILLS_PAGE_SIZE:
        return page
    if start_ms >= end_ms or depth >= 32:
        raise RuntimeError("performance user fills range saturated the exchange page limit")
    midpoint = (start_ms + end_ms) // 2
    left = await _fetch_user_fills_complete(
        info, user, start_ms, midpoint, depth=depth + 1
    )
    right = await _fetch_user_fills_complete(
        info, user, midpoint + 1, end_ms, depth=depth + 1
    )
    unique = {_exchange_fill_key(fill): fill for fill in [*left, *right]}
    return list(unique.values())


async def _fetch_user_funding_complete(
    info: HyperliquidInfoClient,
    user: str,
    start_ms: int,
    end_ms: int,
    *,
    depth: int = 0,
) -> list[dict[str, Any]]:
    page = list(
        await info.post_info(
            {
                "type": "userFunding",
                "user": user,
                "startTime": start_ms,
                "endTime": end_ms,
            }
        )
        or []
    )
    page = [
        item
        for item in page
        if start_ms <= int(item.get("time") or 0) <= end_ms
    ]
    if len(page) < USER_FUNDING_PAGE_SIZE:
        return page
    if start_ms >= end_ms or depth >= 32:
        raise RuntimeError("performance user funding range saturated the exchange page limit")
    midpoint = (start_ms + end_ms) // 2
    left = await _fetch_user_funding_complete(
        info, user, start_ms, midpoint, depth=depth + 1
    )
    right = await _fetch_user_funding_complete(
        info, user, midpoint + 1, end_ms, depth=depth + 1
    )
    unique = {_funding_key(item): item for item in [*left, *right]}
    return list(unique.values())


def _build_leader_payload(
    *,
    leader: LeaderConfig,
    now: datetime,
    source_fills: list[SourceFill],
    outcomes_by_source: dict[str, SourceFillOutcome],
    orders: list[ExecutionOrder],
    actual_fills_by_order: dict[int, list[dict[str, Any]]],
    allocation_events: list[AllocationEvent],
    leader_positions: list[LatestAccountPosition],
    follower_open: dict[str, Any],
    portfolio: Any,
    funding: list[dict[str, Any]],
) -> dict[str, Any]:
    joined_at = _as_utc(leader.performance_started_at or leader.created_at)
    observed_days = max(0.0, (now - joined_at).total_seconds() / 86400)
    logical_events = _logical_leader_events(source_fills)
    behavior = _leader_behavior(logical_events, leader_positions)
    pipeline = _pipeline_metrics(logical_events, outcomes_by_source)
    funding_total = sum(
        (_decimal((item.get("delta") or {}).get("usdc")) for item in funding),
        ZERO,
    )
    leader_realized_gross = sum(
        (_decimal((row.raw_fill or {}).get("closedPnl")) for row in source_fills),
        ZERO,
    )
    leader_fees = sum(
        (_decimal((row.raw_fill or {}).get("fee")) for row in source_fills),
        ZERO,
    )
    leader_volume = sum((abs(_decimal(row.price) * _decimal(row.size)) for row in source_fills), ZERO)
    leader_upnl = sum((_decimal(row.unrealized_pnl) for row in leader_positions), ZERO)
    leader_realized_net = leader_realized_gross - leader_fees + funding_total
    portfolio_metrics = _portfolio_metrics_since_join(portfolio, joined_at)
    portfolio_curve = portfolio_metrics.pop("curve", [])

    follower = _follower_contribution(orders, actual_fills_by_order)
    follower["current_unrealized_pnl"] = _string(follower_open["unrealized_pnl"])
    follower["current_copied_notional"] = _string(follower_open["notional"])
    follower["known_total_pnl_ex_funding"] = _string(
        _decimal(follower["realized_net_ex_funding"])
        + _decimal(follower_open["unrealized_pnl"])
    )
    follower["includes_manual_synced_exposure"] = bool(follower_open["manual_sync"])
    peak_allocated = _peak_allocated_notional(allocation_events)
    follower["peak_allocated_notional"] = _string(peak_allocated)
    follower_total = _decimal(follower["known_total_pnl_ex_funding"])
    follower["return_on_peak_allocated_pct"] = _optional_string(
        follower_total / peak_allocated * Decimal("100") if peak_allocated > ZERO else None
    )

    copyability = _copyability_metrics(orders, actual_fills_by_order, follower, pipeline)
    leader_account = {
        **portfolio_metrics,
        "fill_realized_gross": _string(leader_realized_gross),
        "fill_fees": _string(leader_fees),
        "funding": _string(funding_total),
        "fill_realized_net_including_funding": _string(leader_realized_net),
        "current_unrealized_pnl": _string(leader_upnl),
        "known_trading_pnl": _string(leader_realized_net + leader_upnl),
        "trading_volume": _string(leader_volume),
        "open_positions_count": len(leader_positions),
    }
    scores, recommendation = _score_leader(
        observed_days=observed_days,
        leader_account=leader_account,
        follower=follower,
        copyability=copyability,
        behavior=behavior,
        pipeline=pipeline,
    )
    suffix = _address_suffix(leader.leader_address)
    caveats = [
        "Follower funding is not assigned to a leader because manual size changes share the same exchange position.",
        "Current follower unrealized PnL includes any manual quantity already synchronized into that leader allocation.",
        "No-cloid manual fills are excluded, but manual size changes can alter the shared average entry and therefore the closedPnl reported on a later copied close.",
        "Portfolio drawdown uses Hyperliquid mark-to-market history sampled from the leader join time; intrapoint spikes can be larger.",
    ]
    if copyability["exchange_match_coverage_pct"] is not None and float(
        copyability["exchange_match_coverage_pct"]
    ) < 90:
        caveats.append(
            "Some historical database FILLED rows have no fill on the currently configured follower account; exchange fills remain the attribution source of truth."
        )
    return {
        "leader_id": int(leader.id),
        "leader_address": str(leader.leader_address),
        "address_suffix": suffix,
        "label": f"Leader · {suffix}",
        "enabled": bool(leader.enabled),
        "joined_at": joined_at.isoformat(),
        "observed_days": round(observed_days, 2),
        "window": "SINCE_JOINED",
        "scores": scores,
        "recommendation": recommendation,
        "leader_account": leader_account,
        "follower_account": follower,
        "copyability": copyability,
        "behavior": behavior,
        "pipeline": pipeline,
        "data_quality": {
            "source": "exchange_fills_plus_db_attribution",
            "caveats": caveats,
            "portfolio_history_points": portfolio_metrics["history_points"],
            "logical_leader_events": len(logical_events),
        },
        "history": {"leader_pnl": portfolio_curve},
    }


def _logical_leader_events(source_fills: list[SourceFill]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, int, str], dict[str, Any]] = {}
    for row in source_fills:
        raw = row.raw_fill or {}
        canonical = str(
            row.canonical_coin
            or parse_coin(row.raw_coin or row.coin, default_dex=row.dex).canonical_coin
        ).upper()
        time_ms = int(row.source_time_ms)
        direction = str(raw.get("dir") or "")
        order_key = str(raw.get("oid") or row.source_fill_id)
        key = (canonical, order_key, time_ms, direction)
        side = str(raw.get("side") or row.side or "").upper()
        signed_size = abs(_decimal(row.size)) if side in {"B", "BUY"} else -abs(_decimal(row.size))
        event = groups.setdefault(
            key,
            {
                "key": key,
                "canonical_coin": canonical,
                "time_ms": time_ms,
                "direction": direction,
                "side": side,
                "start_positions": [],
                "signed_size": ZERO,
                "size": ZERO,
                "quote": ZERO,
                "closed_pnl": ZERO,
                "fee": ZERO,
                "source_fill_ids": [],
            },
        )
        start_position = _decimal_or_none(raw.get("startPosition"))
        if start_position is not None:
            event["start_positions"].append(start_position)
        size = abs(_decimal(row.size))
        event["signed_size"] += signed_size
        event["size"] += size
        event["quote"] += abs(_decimal(row.price) * size)
        event["closed_pnl"] += _decimal(raw.get("closedPnl"))
        event["fee"] += _decimal(raw.get("fee"))
        event["source_fill_ids"].append(row.source_fill_id)
    result = []
    for event in groups.values():
        starts = event.pop("start_positions")
        if starts:
            event["start_position"] = min(starts) if event["signed_size"] >= ZERO else max(starts)
        else:
            event["start_position"] = None
        event["price"] = event["quote"] / event["size"] if event["size"] > ZERO else ZERO
        result.append(event)
    return sorted(
        result,
        key=lambda item: (
            item["time_ms"],
            0 if str(item["direction"]).lower().startswith("close") else 1,
            str(item["key"]),
        ),
    )


def _leader_behavior(
    events: list[dict[str, Any]],
    current_positions: list[LatestAccountPosition],
) -> dict[str, Any]:
    active: dict[str, dict[str, Any] | None] = {}
    lifecycles: list[dict[str, Any]] = []
    market_volume: dict[str, Decimal] = defaultdict(Decimal)
    for event in events:
        coin = event["canonical_coin"]
        market_volume[coin] += event["quote"]
        direction = str(event["direction"] or "").lower()
        start = event["start_position"]
        delta = event["signed_size"]
        end = start + delta if start is not None else None
        is_open = direction.startswith("open")
        is_close = direction.startswith("close")
        if is_open and start is not None and abs(start) <= POSITION_TOLERANCE:
            active[coin] = {
                "opened_at_ms": event["time_ms"],
                "net": event["closed_pnl"] - event["fee"],
                "peak_notional": max(event["quote"], abs((end or ZERO) * event["price"])),
            }
            continue
        current = active.get(coin)
        if is_open:
            if coin not in active:
                active[coin] = None
            elif current is not None:
                current["net"] += event["closed_pnl"] - event["fee"]
                current["peak_notional"] = max(
                    current["peak_notional"],
                    event["quote"],
                    abs((end or ZERO) * event["price"]),
                )
            continue
        if is_close and current is not None:
            current["net"] += event["closed_pnl"] - event["fee"]
            current["peak_notional"] = max(
                current["peak_notional"],
                abs((start or ZERO) * event["price"]),
            )
        full_close = bool(
            is_close
            and start is not None
            and (
                abs(event["size"]) >= abs(start) - POSITION_TOLERANCE
                or (end is not None and abs(end) <= POSITION_TOLERANCE)
            )
        )
        if full_close:
            if current is not None:
                peak_notional = max(_decimal(current["peak_notional"]), POSITION_TOLERANCE)
                lifecycles.append(
                    {
                        "canonical_coin": coin,
                        "duration_hours": max(
                            0.0,
                            (event["time_ms"] - int(current["opened_at_ms"])) / 3_600_000,
                        ),
                        "net_pnl": _decimal(current["net"]),
                        "return_bps": _decimal(current["net"]) / peak_notional * Decimal("10000"),
                    }
                )
            active.pop(coin, None)

    wins = [item for item in lifecycles if item["net_pnl"] > ZERO]
    losses = [item for item in lifecycles if item["net_pnl"] < ZERO]
    gross_profit = sum((item["net_pnl"] for item in wins), ZERO)
    gross_loss = abs(sum((item["net_pnl"] for item in losses), ZERO))
    total_volume = sum(market_volume.values(), ZERO)
    top_markets = sorted(market_volume.items(), key=lambda item: item[1], reverse=True)
    winner_hold = _median([item["duration_hours"] for item in wins])
    loser_hold = _median([item["duration_hours"] for item in losses])
    return {
        "complete_lifecycles": len(lifecycles),
        "winning_lifecycles": len(wins),
        "losing_lifecycles": len(losses),
        "lifecycle_win_rate_pct": _round_or_none(
            len(wins) / len(lifecycles) * 100 if lifecycles else None
        ),
        "profit_factor": _round_or_none(
            float(gross_profit / gross_loss) if gross_loss > ZERO else (10.0 if gross_profit > ZERO else None)
        ),
        "median_lifecycle_return_bps": _round_or_none(
            _median([float(item["return_bps"]) for item in lifecycles])
        ),
        "worst_lifecycle_return_bps": _round_or_none(
            min((float(item["return_bps"]) for item in lifecycles), default=None)
        ),
        "median_hold_hours": _round_or_none(
            _median([item["duration_hours"] for item in lifecycles])
        ),
        "winner_median_hold_hours": _round_or_none(winner_hold),
        "loser_median_hold_hours": _round_or_none(loser_hold),
        "loser_to_winner_hold_ratio": _round_or_none(
            loser_hold / winner_hold if loser_hold not in {None, 0} and winner_hold not in {None, 0} else None
        ),
        "p90_hold_hours": _round_or_none(
            _percentile([item["duration_hours"] for item in lifecycles], 0.90)
        ),
        "max_hold_hours": _round_or_none(
            max((item["duration_hours"] for item in lifecycles), default=None)
        ),
        "current_open_positions": len(current_positions),
        "top_market_volume_pct": _round_or_none(
            float(top_markets[0][1] / total_volume * 100) if top_markets and total_volume > ZERO else None
        ),
        "top_three_market_volume_pct": _round_or_none(
            float(sum((item[1] for item in top_markets[:3]), ZERO) / total_volume * 100)
            if total_volume > ZERO
            else None
        ),
        "top_markets": [
            {
                "canonical_coin": coin,
                "volume": _string(volume),
                "share_pct": _round_or_none(float(volume / total_volume * 100)) if total_volume > ZERO else None,
            }
            for coin, volume in top_markets[:8]
        ],
    }


def _pipeline_metrics(
    events: list[dict[str, Any]],
    outcomes_by_source: dict[str, SourceFillOutcome],
) -> dict[str, Any]:
    counts = defaultdict(int)
    for event in events:
        outcomes = [
            outcomes_by_source[source_id]
            for source_id in event["source_fill_ids"]
            if source_id in outcomes_by_source
        ]
        dispositions = {str(item.disposition or "").upper() for item in outcomes}
        reasons = " | ".join(str(item.reason or "") for item in outcomes).lower()
        if "EXECUTED" in dispositions:
            counts["executed"] += 1
        elif "min" in reasons and ("order" in reasons or "notional" in reasons):
            counts["min_notional"] += 1
        elif "MIN_NOTIONAL_EXEMPT" in dispositions:
            counts["min_notional"] += 1
        elif "market_owner" in reasons or "owner blocked" in reasons:
            counts["fcfs"] += 1
        elif "ignored_old_lifecycle" in reasons or "existing position from before" in reasons:
            counts["old_lifecycle"] += 1
        elif "legacy processed fill" in reasons:
            counts["legacy"] += 1
        elif "MANUAL_REVIEW" in dispositions:
            counts["manual_review"] += 1
        elif outcomes:
            counts["no_action"] += 1
        else:
            counts["missing_outcome"] += 1
    return {
        "logical_events": len(events),
        "executed_events": counts["executed"],
        "minimum_10u_exempt_events": counts["min_notional"],
        "fcfs_blocked_events": counts["fcfs"],
        "ignored_old_lifecycle_events": counts["old_lifecycle"],
        "legacy_outcome_events": counts["legacy"],
        "manual_review_events": counts["manual_review"],
        "other_no_action_events": counts["no_action"],
        "missing_outcome_events": counts["missing_outcome"],
    }


def _actual_follower_fills_by_order(
    orders: list[ExecutionOrder],
    follower_fills: list[dict[str, Any]],
) -> dict[int, list[dict[str, Any]]]:
    by_cloid = {
        (str(row.venue_account or "").lower(), str(row.cloid).lower()): row
        for row in orders
        if row.id is not None and row.cloid
    }
    by_oid: dict[tuple[str, str], ExecutionOrder] = {}
    for row in orders:
        for value in (row.order_id, row.venue_order_id):
            if value:
                by_oid[(str(row.venue_account or "").lower(), str(value))] = row
    result: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for fill in follower_fills:
        execution_scope = str(fill.get("_copytrade_execution_scope") or "").lower()
        cloid = str(fill.get("cloid") or "").lower()
        order = by_cloid.get((execution_scope, cloid)) if cloid else None
        if order is None and fill.get("oid") is not None:
            order = by_oid.get((execution_scope, str(fill.get("oid"))))
        if order is None or order.id is None or order.leader_id is None:
            continue
        if int(fill.get("time") or 0) < int(_as_utc(order.created_at).timestamp() * 1000) - 60_000:
            continue
        result[int(order.id)].append(fill)
    return result


def _follower_contribution(
    orders: list[ExecutionOrder],
    actual_fills_by_order: dict[int, list[dict[str, Any]]],
) -> dict[str, Any]:
    gross = ZERO
    fees = ZERO
    volume = ZERO
    curve: list[tuple[int, Decimal]] = []
    actual_orders = 0
    fill_fragments = 0
    for order in orders:
        fills = actual_fills_by_order.get(int(order.id or 0), [])
        if not fills:
            continue
        actual_orders += 1
        fill_fragments += len(fills)
        for fill in fills:
            pnl = _decimal(fill.get("closedPnl"))
            fee = _decimal(fill.get("fee"))
            gross += pnl
            fees += fee
            volume += abs(_decimal(fill.get("px")) * _decimal(fill.get("sz")))
            curve.append((int(fill.get("time") or 0), pnl - fee))
    max_drawdown = _realized_curve_max_drawdown(curve)
    net = gross - fees
    return {
        "realized_gross": _string(gross),
        "fees": _string(fees),
        "realized_net_ex_funding": _string(net),
        "trading_volume": _string(volume),
        "realized_edge_bps": _optional_string(net / volume * Decimal("10000") if volume > ZERO else None),
        "realized_curve_max_drawdown": _string(max_drawdown),
        "matched_exchange_orders": actual_orders,
        "exchange_fill_fragments": fill_fragments,
    }


def _copyability_metrics(
    orders: list[ExecutionOrder],
    actual_fills_by_order: dict[int, list[dict[str, Any]]],
    follower: dict[str, Any],
    pipeline: dict[str, Any],
) -> dict[str, Any]:
    slippages: list[float] = []
    weighted_cost = ZERO
    weighted_notional = ZERO
    adverse = 0
    latencies: list[int] = []
    matched = 0
    for order in orders:
        fills = actual_fills_by_order.get(int(order.id or 0), [])
        if not fills:
            continue
        qty = sum((_decimal(fill.get("sz")) for fill in fills), ZERO)
        if qty <= ZERO:
            continue
        follower_price = sum(
            (_decimal(fill.get("px")) * _decimal(fill.get("sz")) for fill in fills),
            ZERO,
        ) / qty
        leader_price = _decimal_or_none(order.leader_entry_px)
        if leader_price is None or leader_price <= ZERO:
            continue
        if str(order.side or "").upper() == "BUY":
            slippage = (follower_price - leader_price) / leader_price * Decimal("10000")
        else:
            slippage = (leader_price - follower_price) / leader_price * Decimal("10000")
        slippages.append(float(slippage))
        weighted_notional += leader_price * qty
        weighted_cost += slippage / Decimal("10000") * leader_price * qty
        adverse += slippage > ZERO
        matched += 1
        if order.event_to_final_ms is not None:
            latencies.append(int(order.event_to_final_ms))
        elif order.event_to_ack_ms is not None:
            latencies.append(int(order.event_to_ack_ms))
    db_filled = sum(1 for order in orders if str(order.status or "").upper() == "FILLED")
    db_filled_exchange_matched = sum(
        1
        for order in orders
        if str(order.status or "").upper() == "FILLED"
        and bool(actual_fills_by_order.get(int(order.id or 0), []))
    )
    database_status_disagreements = sum(
        1
        for order in orders
        if str(order.status or "").upper() != "FILLED"
        and bool(actual_fills_by_order.get(int(order.id or 0), []))
    )
    coverage = db_filled_exchange_matched / db_filled * 100 if db_filled else None
    return {
        "matched_priced_orders": matched,
        "db_filled_orders": db_filled,
        "db_filled_exchange_matched_orders": db_filled_exchange_matched,
        "database_status_disagreement_orders": database_status_disagreements,
        "exchange_match_coverage_pct": _round_or_none(coverage),
        "weighted_adverse_slippage_bps": _round_or_none(
            float(weighted_cost / weighted_notional * Decimal("10000"))
            if weighted_notional > ZERO
            else None
        ),
        "median_slippage_bps": _round_or_none(_median(slippages)),
        "p90_slippage_bps": _round_or_none(_percentile(slippages, 0.90)),
        "p95_slippage_bps": _round_or_none(_percentile(slippages, 0.95)),
        "max_slippage_bps": _round_or_none(max(slippages, default=None)),
        "adverse_slippage_order_pct": _round_or_none(
            adverse / len(slippages) * 100 if slippages else None
        ),
        "median_event_to_final_ms": _round_or_none(_median(latencies)),
        "p95_event_to_final_ms": _round_or_none(_percentile(latencies, 0.95)),
        "realized_edge_bps": _round_or_none(_float_or_none(follower.get("realized_edge_bps"))),
        "minimum_10u_exempt_events": pipeline["minimum_10u_exempt_events"],
        "fcfs_blocked_events": pipeline["fcfs_blocked_events"],
        "manual_review_events": pipeline["manual_review_events"],
    }


def _portfolio_metrics_since_join(portfolio: Any, joined_at: datetime) -> dict[str, Any]:
    periods = dict(portfolio or []) if isinstance(portfolio, list) else dict(portfolio or {})
    perp = periods.get("perpAllTime") or periods.get("allTime") or {}
    total = periods.get("allTime") or perp
    pnl_points = sorted(
        [(int(item[0]), _decimal(item[1])) for item in (perp.get("pnlHistory") or [])],
        key=lambda item: item[0],
    )
    account_points = sorted(
        [(int(item[0]), _decimal(item[1])) for item in (total.get("accountValueHistory") or [])],
        key=lambda item: item[0],
    )
    joined_ms = int(joined_at.timestamp() * 1000)
    if not pnl_points:
        return {
            "portfolio_pnl_since_join": None,
            "portfolio_return_pct": None,
            "max_drawdown": None,
            "max_drawdown_pct": None,
            "current_drawdown": None,
            "current_drawdown_pct": None,
            "start_account_value": None,
            "current_account_value": None,
            "history_points": 0,
            "curve": [],
        }
    baseline = _value_at_or_before(pnl_points, joined_ms)
    if baseline is None:
        baseline = pnl_points[0][1]
    window = [(time_ms, value - baseline) for time_ms, value in pnl_points if time_ms >= joined_ms]
    if not window:
        window = [(joined_ms, ZERO), (pnl_points[-1][0], pnl_points[-1][1] - baseline)]
    elif window[0][0] > joined_ms:
        window.insert(0, (joined_ms, ZERO))
    start_account = _value_at_or_before(account_points, joined_ms)
    if start_account is None:
        start_account = next((value for time_ms, value in account_points if time_ms >= joined_ms), None)
    current_account = account_points[-1][1] if account_points else None
    peak = ZERO
    max_drawdown = ZERO
    for _, value in window:
        peak = max(peak, value)
        max_drawdown = max(max_drawdown, peak - value)
    final = window[-1][1]
    current_drawdown = peak - final
    denominator = start_account if start_account is not None and start_account > ZERO else None
    curve = _downsample_curve(window, 160)
    return {
        "portfolio_pnl_since_join": _string(final),
        "portfolio_return_pct": _optional_string(final / denominator * Decimal("100") if denominator else None),
        "max_drawdown": _string(max_drawdown),
        "max_drawdown_pct": _optional_string(max_drawdown / denominator * Decimal("100") if denominator else None),
        "current_drawdown": _string(current_drawdown),
        "current_drawdown_pct": _optional_string(current_drawdown / denominator * Decimal("100") if denominator else None),
        "start_account_value": _optional_string(start_account),
        "current_account_value": _optional_string(current_account),
        "history_points": len(window),
        "curve": [
            {"time": datetime.fromtimestamp(time_ms / 1000, timezone.utc).isoformat(), "pnl": _string(value)}
            for time_ms, value in curve
        ],
    }


def _peak_allocated_notional(events: list[AllocationEvent]) -> Decimal:
    current: dict[int, Decimal] = {}
    peak = ZERO
    for event in sorted(events, key=lambda row: (_as_utc(row.created_at), int(row.id or 0))):
        if event.allocation_id is None:
            continue
        current[int(event.allocation_id)] = abs(_decimal(event.after_notional))
        peak = max(peak, sum(current.values(), ZERO))
    return peak


def _follower_open_attribution(
    *,
    active_allocations: list[LeaderPositionAllocationRecord],
    follower_positions: list[LatestAccountPosition],
    manual_scopes: set[tuple[str, str]],
) -> dict[int, dict[str, Any]]:
    allocations_by_scope: dict[tuple[str, str, str], list[LeaderPositionAllocationRecord]] = defaultdict(list)
    for allocation in active_allocations:
        canonical = str(
            allocation.canonical_coin
            or parse_coin(allocation.hyperliquid_coin, default_dex=allocation.dex).canonical_coin
        ).upper()
        key = (str(allocation.dex or "").lower(), canonical, str(allocation.position_side).upper())
        allocations_by_scope[key].append(allocation)
    result: dict[int, dict[str, Any]] = defaultdict(_empty_open_attribution)
    for position in follower_positions:
        canonical = str(position.canonical_coin or position.coin or "").upper()
        key = (str(position.dex or "").lower(), canonical, str(position.side or "").upper())
        allocations = allocations_by_scope.get(key, [])
        total_qty = sum((abs(_decimal(row.allocated_qty)) for row in allocations), ZERO)
        if total_qty <= ZERO:
            continue
        for allocation in allocations:
            if allocation.leader_id is None:
                continue
            share = abs(_decimal(allocation.allocated_qty)) / total_qty
            target = result[int(allocation.leader_id)]
            target["unrealized_pnl"] += _decimal(position.unrealized_pnl) * share
            target["notional"] += abs(_decimal(position.notional)) * share
            target["manual_sync"] = target["manual_sync"] or (
                (key[0], key[1]) in manual_scopes
            )
    return result


def _empty_open_attribution() -> dict[str, Any]:
    return {"unrealized_pnl": ZERO, "notional": ZERO, "manual_sync": False}


def _score_leader(
    *,
    observed_days: float,
    leader_account: dict[str, Any],
    follower: dict[str, Any],
    copyability: dict[str, Any],
    behavior: dict[str, Any],
    pipeline: dict[str, Any],
) -> tuple[dict[str, int], dict[str, Any]]:
    follower_roi = _float_or_none(follower.get("return_on_peak_allocated_pct"))
    follower_edge = _float_or_none(follower.get("realized_edge_bps"))
    leader_return = _float_or_none(leader_account.get("portfolio_return_pct"))
    profitability = round(
        0.50 * _linear_score(follower_roi, [(-20, 0), (-5, 20), (0, 45), (5, 70), (15, 88), (30, 100)])
        + 0.30 * _linear_score(follower_edge, [(-10, 0), (0, 40), (5, 58), (15, 75), (40, 92), (80, 100)])
        + 0.20 * _linear_score(leader_return, [(-30, 0), (-10, 20), (0, 45), (10, 68), (30, 85), (70, 100)])
    )

    max_dd = _float_or_none(leader_account.get("max_drawdown_pct"))
    current_dd = _float_or_none(leader_account.get("current_drawdown_pct"))
    concentration = _float_or_none(behavior.get("top_market_volume_pct"))
    hold_ratio = _float_or_none(behavior.get("loser_to_winner_hold_ratio"))
    risk_control = round(
        0.50 * _inverse_score(max_dd, [(5, 95), (10, 82), (20, 62), (30, 42), (40, 20), (60, 0)])
        + 0.20 * _inverse_score(current_dd, [(3, 95), (10, 75), (20, 48), (30, 25), (50, 0)])
        + 0.15 * _inverse_score(concentration, [(20, 95), (40, 75), (60, 50), (80, 25), (100, 5)])
        + 0.15 * _inverse_score(hold_ratio, [(1, 95), (2, 82), (5, 60), (10, 35), (20, 10), (40, 0)])
    )

    weighted_slippage = _float_or_none(copyability.get("weighted_adverse_slippage_bps"))
    p95_slippage = _float_or_none(copyability.get("p95_slippage_bps"))
    coverage = _float_or_none(copyability.get("exchange_match_coverage_pct"))
    copyability_score = round(
        0.45 * _inverse_score(weighted_slippage, [(2, 100), (5, 90), (10, 75), (20, 55), (40, 30), (80, 5)])
        + 0.25 * _inverse_score(p95_slippage, [(10, 100), (25, 85), (50, 65), (100, 40), (200, 10), (400, 0)])
        + 0.20 * _linear_score(follower_edge, [(-10, 0), (0, 35), (5, 55), (15, 75), (40, 92), (80, 100)])
        + 0.10 * _linear_score(coverage, [(50, 0), (75, 45), (90, 75), (98, 95), (100, 100)])
    )

    profit_factor = _float_or_none(behavior.get("profit_factor"))
    median_return = _float_or_none(behavior.get("median_lifecycle_return_bps"))
    complete_lifecycles = int(behavior.get("complete_lifecycles") or 0)
    consistency = round(
        0.45 * _linear_score(profit_factor, [(0.5, 0), (1, 40), (1.5, 65), (2.5, 82), (5, 95), (10, 100)])
        + 0.30 * _linear_score(median_return, [(-50, 0), (0, 40), (20, 60), (75, 82), (200, 100)])
        + 0.25 * _linear_score(float(complete_lifecycles), [(0, 0), (10, 35), (30, 65), (75, 88), (150, 100)])
    )
    data_confidence = round(
        min(observed_days / 30, 1) * 40
        + min(complete_lifecycles / 50, 1) * 40
        + _linear_score(coverage, [(50, 0), (80, 10), (95, 18), (100, 20)])
    )
    overall = round(
        0.27 * profitability
        + 0.28 * risk_control
        + 0.28 * copyability_score
        + 0.12 * consistency
        + 0.05 * data_confidence
    )

    flags: list[dict[str, str]] = []
    follower_total = _decimal(follower.get("known_total_pnl_ex_funding"))
    if follower_total < ZERO:
        flags.append(_flag("NEGATIVE_CONTRIBUTION", "danger", "自加入以来的已知跟单贡献为负。"))
    if max_dd is not None and max_dd >= 30:
        flags.append(_flag("HIGH_DRAWDOWN", "danger", f"Leader 加入后最大回撤约 {max_dd:.1f}%。"))
    if current_dd is not None and current_dd >= 20:
        flags.append(_flag("CURRENT_DRAWDOWN", "danger", f"Leader 当前仍距阶段高点约 {current_dd:.1f}%。"))
    if weighted_slippage is not None and weighted_slippage >= 20:
        flags.append(_flag("POOR_COPYABILITY", "danger", f"方向调整后加权滑点约 {weighted_slippage:.1f}bps。"))
    if p95_slippage is not None and p95_slippage >= 80:
        flags.append(_flag("SLIPPAGE_TAIL", "warn", f"P95 滑点约 {p95_slippage:.1f}bps，存在插针/低流动性尾部。"))
    if hold_ratio is not None and hold_ratio >= 5 and int(behavior.get("losing_lifecycles") or 0) >= 3:
        flags.append(_flag("LOSS_HOLDING_ASYMMETRY", "warn", f"亏损仓持有时间约为盈利仓的 {hold_ratio:.1f} 倍。"))
    if concentration is not None and concentration >= 50:
        flags.append(_flag("MARKET_CONCENTRATION", "warn", f"最大单一市场成交占比约 {concentration:.1f}%。"))
    if follower_edge is not None and follower_edge < 5 and _decimal(follower.get("trading_volume")) > Decimal("50000"):
        flags.append(_flag("THIN_EDGE", "warn", "成交量很大但扣费后有效收益边际很薄。"))
    if observed_days < 14 or complete_lifecycles < 20:
        flags.append(_flag("LIMITED_SAMPLE", "info", "观察时间或完整生命周期样本仍不足。"))
    if coverage is not None and coverage < 90:
        flags.append(_flag("HISTORICAL_PIPELINE_GAPS", "info", "旧版机器人部分 FILLED 记录无法在当前 follower 账户成交中验证。"))
    if int(pipeline.get("manual_review_events") or 0) > 0:
        flags.append(_flag("HISTORICAL_EXECUTION_ISSUES", "info", "加入以来包含旧版机器人产生的人工复核/执行异常。"))

    danger_codes = {item["code"] for item in flags if item["severity"] == "danger"}
    if observed_days < 7 or complete_lifecycles < 10:
        status = "OBSERVE"
        label = "样本不足"
    elif "NEGATIVE_CONTRIBUTION" in danger_codes or "HIGH_DRAWDOWN" in danger_codes:
        status = "HIGH_RISK"
        label = "高风险"
    elif "POOR_COPYABILITY" in danger_codes:
        status = "POOR_COPYABILITY"
        label = "不易复制"
    elif overall >= 78:
        status = "STRONG"
        label = "表现强"
    elif overall >= 62:
        status = "KEEP"
        label = "可保留"
    else:
        status = "WATCH"
        label = "重点观察"
    return (
        {
            "overall": _bounded_score(overall),
            "profitability": _bounded_score(profitability),
            "risk_control": _bounded_score(risk_control),
            "copyability": _bounded_score(copyability_score),
            "consistency": _bounded_score(consistency),
            "data_confidence": _bounded_score(data_confidence),
        },
        {"status": status, "label": label, "flags": flags},
    )


def _flag(code: str, severity: str, message: str) -> dict[str, str]:
    return {"code": code, "severity": severity, "message": message}


def _linear_score(value: float | None, points: list[tuple[float, float]]) -> float:
    if value is None:
        return 40.0
    return _interpolate(value, points)


def _inverse_score(value: float | None, points: list[tuple[float, float]]) -> float:
    if value is None:
        return 45.0
    return _interpolate(value, points)


def _interpolate(value: float, points: list[tuple[float, float]]) -> float:
    ordered = sorted(points, key=lambda item: item[0])
    if value <= ordered[0][0]:
        return ordered[0][1]
    if value >= ordered[-1][0]:
        return ordered[-1][1]
    for (left_x, left_y), (right_x, right_y) in zip(ordered, ordered[1:]):
        if left_x <= value <= right_x:
            ratio = (value - left_x) / (right_x - left_x)
            return left_y + ratio * (right_y - left_y)
    return ordered[-1][1]


def _realized_curve_max_drawdown(curve: list[tuple[int, Decimal]]) -> Decimal:
    cumulative = ZERO
    peak = ZERO
    drawdown = ZERO
    for _, delta in sorted(curve, key=lambda item: item[0]):
        cumulative += delta
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return drawdown


def _value_at_or_before(points: list[tuple[int, Decimal]], time_ms: int) -> Decimal | None:
    value = None
    for point_time, point_value in points:
        if point_time > time_ms:
            break
        value = point_value
    return value


def _downsample_curve(
    points: list[tuple[int, Decimal]],
    limit: int,
) -> list[tuple[int, Decimal]]:
    if len(points) <= limit:
        return points
    step = (len(points) - 1) / (limit - 1)
    indexes = sorted({round(index * step) for index in range(limit)})
    return [points[index] for index in indexes]


def _exchange_fill_key(fill: dict[str, Any]) -> str:
    return "|".join(
        str(fill.get(key) or "")
        for key in ("hash", "tid", "oid", "time", "coin")
    )


def _address_suffix(address: str | None) -> str:
    value = str(address or "")
    return value[-4:].upper() if len(value) >= 4 else "----"


def _decimal(value: Any) -> Decimal:
    if value in {None, ""}:
        return ZERO
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return ZERO


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _string(value: Any) -> str:
    return str(_decimal(value))


def _optional_string(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _round_or_none(value: float | int | None, digits: int = 3) -> float | None:
    return round(float(value), digits) if value is not None else None


def _median(values: Iterable[float | int]) -> float | None:
    items = [float(value) for value in values]
    return float(median(items)) if items else None


def _percentile(values: Iterable[float | int], percentile: float) -> float | None:
    items = sorted(float(value) for value in values)
    if not items:
        return None
    index = min(len(items) - 1, max(0, round((len(items) - 1) * percentile)))
    return items[index]


def _bounded_score(value: int | float) -> int:
    return max(0, min(100, round(value)))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _methodology_payload() -> dict[str, Any]:
    return {
        "window": "Each leader's own database join timestamp through the latest refresh.",
        "overall_weights": {
            "profitability": 27,
            "risk_control": 28,
            "copyability": 28,
            "consistency": 12,
            "data_confidence": 5,
        },
        "principles": [
            "Win rate is displayed but is not directly used as a dominant score.",
            "Exchange follower fills, not database status alone, are the contribution source of truth.",
            "Slippage is direction-adjusted against the leader fill price; positive means adverse to the follower.",
            "Minimum-10U exemptions, FCFS conflicts, old lifecycles, and historical execution issues are reported separately.",
            "Scores never enable, disable, or resize a leader automatically.",
        ],
    }


def _warming_payload() -> dict[str, Any]:
    return {
        "schema_version": PERFORMANCE_SCHEMA_VERSION,
        "status": "warming",
        "generated_at": None,
        "window": "SINCE_JOINED",
        "leaders": [],
        "methodology": _methodology_payload(),
    }


def _empty_payload(now: datetime) -> dict[str, Any]:
    return {
        **_warming_payload(),
        "status": "ready",
        "generated_at": now.isoformat(),
    }
