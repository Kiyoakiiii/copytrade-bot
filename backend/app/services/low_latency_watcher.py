from __future__ import annotations

import asyncio
import hashlib
import json
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from types import SimpleNamespace
from typing import Any, Callable

import structlog
import websockets
from sqlalchemy import case, delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.core.logging import redact_text
from app.db.session import SessionLocal
from app.models import (
    AppSetting,
    AllocationEvent,
    ExecutionOrder,
    FollowerMarketGuard,
    LatestAccountPosition,
    LatestAccountState,
    LeaderConfig,
    LeaderFillCursor,
    LeaderPositionAllocationRecord,
    LeaderPositionBaseline,
    MarketRiskSetting,
    RiskEvent,
    SourceFill,
    SourceFillOutcome,
    SignerNonceState,
    UnmatchedFollowerFill,
)
from app.services.account_abstraction import (
    AccountAbstractionService,
    MODE_DEX_ABSTRACTION,
    MODE_UNIFIED,
    SOURCE_ACCOUNT_TOTAL,
    SOURCE_CLEARINGHOUSE,
    account_abstraction_setting_key,
    available_collateral_sufficient,
    load_account_abstraction_state,
    resolved_value_payload,
    resolve_account_value_for_sizing,
    save_account_abstraction_state,
)
from app.services.account_state import (
    FOLLOWER,
    LEADER,
    AccountStateService,
    parse_account_state,
    save_account_state,
)
from app.services.allocations import (
    ALLOCATION_TRANSITION_TOLERANCE,
    LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK,
    MAX_POSITION_NOTIONAL_CAP_APPLIED,
    MAX_POSITION_NOTIONAL_CAP_EXCEEDED,
    AllocationScopeError,
    AllocationTransitionAction,
    assert_allocation_scope,
    plan_leader_allocation_transition,
)
from app.services.allocation_fill import (
    apply_filled_order_to_allocation_state,
    clear_deferred_reduce_state,
)
from app.services.auto_copy import RECOVERY_ORDER_STATUSES
from app.services.baseline import BASELINE_COPY_ALLOWED, BASELINE_WAIT_UNTIL_FLAT
from app.services.calculator import (
    SIZING_MODE_ACCOUNT_RATIO,
    calculate_leader_position_ratio,
    calculate_target_notional_by_account_ratio,
)
from app.services.execution_router import ExecutionVenue
from app.services.execution_alerts import (
    HYPERLIQUID_NETWORK_UPGRADE_POST_ONLY_REJECTION,
    is_hyperliquid_network_upgrade_post_only_error,
)
from app.services.hyperliquid import HyperliquidInfoClient, fill_unique_id
from app.services.hyperliquid_dex import (
    HyperliquidDexRegistry,
    ParsedCoin,
    canonical_coin,
    dex_display_name,
    mask_address,
    parse_coin,
)
from app.services.hyperliquid_execution import (
    HyperliquidExecutionClient,
    ValidatedOrderParams,
    build_hyperliquid_cloid,
    build_hyperliquid_leverage_plan,
    resolve_asset_id_from_meta,
    validate_hyperliquid_order_params,
)
from app.services.hyperliquid_risk_settings import (
    DESIRED_MARGIN_MODE,
    FALLBACK_MARGIN_MODE,
    RiskSettingResult,
    STATUS_CONFIRMED,
    desired_leverage_for_margin_mode,
    effective_leverage_for_margin_mode,
    ensure_hyperliquid_market_risk_settings,
    market_requires_isolated_margin,
    one_x_leverage_required,
)
from app.services.leader_config import active_leaders_statement, is_coin_allowed, normalize_leader_address
from app.services.order_policy import (
    AutoCopyOrderPolicyError,
    HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
    assert_hyperliquid_auto_copy_order,
)
from app.services.runtime_control import acquire_copy_trading_control_lock
from app.services.sizing_guard import SizingGuardError, assert_sizing_mode_account_ratio
from app.services.task_status import store_task_status
from app.services.target_position import PositionSide
from app.tasks.leader_state_poller import schedule_account_state_refresh_if_stale

log = structlog.get_logger(__name__)

PENDING_OPEN_STATUS = "PENDING_OPEN"
PENDING_OPEN_REASON = "BELOW_MIN_ORDER_VALUE_PENDING_OPEN"
ACCOUNT_VALUE_PENDING_OPEN_REASON = "ACCOUNT_VALUE_UNAVAILABLE_PENDING_OPEN"
MINIMUM_RESIDUAL_ECONOMIC_FLAT_REASON = "MINIMUM_RESIDUAL_ECONOMIC_FLAT"
MINIMUM_RESIDUAL_ECONOMIC_FLAT_PENDING_REASON = (
    "MINIMUM_RESIDUAL_ECONOMIC_FLAT_PENDING"
)
_COALESCED_FILL_NOTIONAL_KEY = "_copytrade_coalesced_fill_notional"
_COALESCED_FILL_SIZE_KEY = "_copytrade_coalesced_fill_size"
_COALESCED_FILL_COUNT_KEY = "_copytrade_coalesced_fill_count"
_COALESCED_SOURCE_IDS_KEY = "_copytrade_coalesced_source_fill_ids"
_SNAPSHOT_RECOVERY_KEY = "_copytrade_snapshot_recovery"
_SNAPSHOT_RECOVERY_REASON_KEY = "_copytrade_snapshot_recovery_reason"
HYPERLIQUID_WS_APP_PING_SECONDS = 30.0
HYPERLIQUID_WS_MAX_QUEUE = 256
HYPERLIQUID_WS_MAX_MESSAGE_BYTES = 2 * 1024 * 1024
LEADER_FILL_PRICE_FALLBACK_SOURCE = "LEADER_FILL_PRICE_FALLBACK"
RECENT_CLOSED_ALLOCATION_STATE_LAG_WINDOW = timedelta(minutes=5)
FIXED_LEADER_ACCOUNT_VALUE_SOURCE = "LEADER_CONFIG_FIXED"
FIXED_LEADER_ACCOUNT_VALUE_MODE = "FIXED_REFERENCE"
LOCAL_POSITION_PROJECTION_SOURCES = {"LOCAL_FILL_PROJECTION", "ORDER_RECOVERY_PROJECTION"}
UNRESOLVED_SAME_MARKET_ORDER_BLOCKER = "unresolved UNKNOWN/PENDING auto order exists for this leader/market"
UNRESOLVED_SAME_MARKET_RETRY_ATTEMPTS = 3
UNRESOLVED_SAME_MARKET_RETRY_SLEEP_SECONDS = 0.01
TRANSIENT_UNRESOLVED_ORDER_STATUSES = {"PENDING_SUBMIT", "SUBMITTING"}
FILL_OUTCOME_ORDER_PLANNED = "ORDER_PLANNED"
FILL_OUTCOME_EXECUTED = "EXECUTED"
FILL_OUTCOME_MIN_NOTIONAL_EXEMPT = "MIN_NOTIONAL_EXEMPT"
FILL_OUTCOME_SUBMISSION_UNKNOWN = "SUBMISSION_UNKNOWN"
FILL_OUTCOME_MANUAL_REVIEW = "MANUAL_REVIEW"
FILL_OUTCOME_NO_ACTION_REQUIRED = "NO_ACTION_REQUIRED"
EMERGENCY_KILL_SWITCH_ERROR = "EMERGENCY_KILL_SWITCH: copy trading disabled before exchange submit"
MANUAL_MARKET_OWNER_BLOCKED = "MANUAL_MARKET_OWNER_BLOCKED"
LEADER_FILL_BACKFILL_PAGE_SIZE = 2000
LEADER_FILL_BACKFILL_OVERLAP_MS = 1000
LEADER_FILL_BACKFILL_RETRY_BASE_SECONDS = 1.0
LEADER_FILL_BACKFILL_RETRY_MAX_SECONDS = 15.0
PER_FILL_INTERNAL_LATENCY_WARN_MS = 200
RECENT_COMPLETED_SOURCE_FILL_MAX = 100_000
DURABLE_REPLAY_MAX_HOT_PATH_DEFER_SECONDS = 0.25
FOLLOWER_POSITION_STREAM_TRUST_SECONDS = 15.0
PRICE_FALLBACK_RECENT_EVENT_SECONDS = 60.0
# Hyperliquid clearinghouseState can briefly lag a just-observed user fill even
# though the REST response itself is timestamped after the websocket event.
# Keep an unmatched-fill guard through at least one full refresh interval so a
# pre-fill snapshot cannot be mistaken for a reconciled post-fill position.
UNMATCHED_FOLLOWER_FILL_STATE_SETTLE_SECONDS = 1.0
MANUAL_FILL_POSITION_AT_LEAST = "AT_LEAST"
MANUAL_FILL_POSITION_AT_MOST = "AT_MOST"
MANUAL_FILL_POSITION_EXACT = "EXACT"
SHARED_MARKET_META_CACHE_VERSION = "v1"
SHARED_MARKET_META_LOCK_PREFIX = "copytrade:hyperliquid:market-meta"
SHARED_PERP_DEX_DIRECTORY_CACHE_VERSION = "v1"
SHARED_PERP_DEX_DIRECTORY_LOCK_PREFIX = "copytrade:hyperliquid:perp-dex-directory"


def _durable_replay_wait_seconds(
    *,
    active_interval: float,
    idle_interval: float,
    order_resume_interval: float,
    now: float,
    next_order_resume_at: float,
    replayed: int,
    resumed: int,
) -> float:
    """Choose a recovery poll delay without slowing a known pending item.

    A non-empty replay/resume pass stays on the short active cadence.  Empty
    scans back off, capped by the next outbox recovery scan.  Exact fill retry
    deadlines bypass this timeout through ``_durable_replay_wakeup``.
    """
    active = max(0.01, float(active_interval))
    if int(replayed or 0) > 0 or int(resumed or 0) > 0:
        return active
    idle = max(active, float(idle_interval))
    resume = max(active, float(order_resume_interval))
    until_resume = max(active, float(next_order_resume_at) - float(now))
    return min(idle, resume, until_resume)


def _durable_replay_should_scan(
    *,
    hot_path_busy: bool,
    wakeup_requested: bool,
    now: float,
    last_scan_at: float,
    max_hot_path_defer_seconds: float = DURABLE_REPLAY_MAX_HOT_PATH_DEFER_SECONDS,
) -> bool:
    """Bound recovery starvation without making every hot-path tick hit SQL."""

    if not hot_path_busy or wakeup_requested:
        return True
    max_defer = max(0.01, float(max_hot_path_defer_seconds))
    return float(now) - float(last_scan_at) >= max_defer


def _leader_fill_backfill_retry_delay_seconds(consecutive_failures: int) -> float:
    exponent = min(max(int(consecutive_failures), 1) - 1, 10)
    return min(
        LEADER_FILL_BACKFILL_RETRY_BASE_SECONDS * (2**exponent),
        LEADER_FILL_BACKFILL_RETRY_MAX_SECONDS,
    )


@dataclass(frozen=True)
class MarketKey:
    dex: str
    coin: str
    canonical_coin: str
    raw_coin: str
    asset_id: int | None
    venue_symbol: str


@dataclass(frozen=True)
class FillEvent:
    source_fill_id: str
    leader_address: str
    market: MarketKey
    side: str
    price: Decimal
    size: Decimal
    time_ms: int
    raw: dict[str, Any]
    is_snapshot: bool
    ws_received_at: datetime
    parse_started_at: datetime | None = None
    parse_done_at: datetime | None = None
    queue_enqueued_at: datetime | None = None
    fill_worker_started_at: datetime | None = None
    ingress_channel: str | None = None

    @property
    def hyperliquid_event_time(self) -> datetime | None:
        if not self.time_ms:
            return None
        return datetime.fromtimestamp(self.time_ms / 1000, timezone.utc)


class RetryableFillProcessingError(RuntimeError):
    """The fill remains in the durable inbox and must be planned again."""


class MarketFillFifoWait(RetryableFillProcessingError):
    """An earlier durable fill for this follower market must finish first."""


class MarketOwnershipHandoffPending(RetryableFillProcessingError):
    """The current owner is releasing/confirming the market; retry without dropping the fill."""


class RetryablePreExchangeSubmitError(RuntimeError):
    """The exchange was not called, so the durable outbox is safe to retry."""


class OrderSubmitClaimLost(RuntimeError):
    """Another worker/recovery path won the durable pre-send order CAS."""


class StaleFollowerMarketPlanInvalidated(RuntimeError):
    """An unsubmitted stale plan was returned to the durable fill inbox."""


def _follower_position_freshness_retry(value: Any) -> bool:
    return "follower position state is stale" in str(value or "").lower()


def _follower_position_state_is_fresh(
    *,
    state_at: datetime | None,
    now: datetime,
    stale_seconds: float,
    stream_trusted: bool,
) -> bool:
    """Accept either a recent snapshot or a live causally ordered state feed."""

    if stream_trusted:
        return True
    return bool(
        state_at is not None
        and state_at >= now - timedelta(seconds=max(0.5, float(stale_seconds)))
    )


def _market_serialization_retry(value: Any) -> bool:
    return isinstance(value, (MarketFillFifoWait, MarketOwnershipHandoffPending)) or str(
        value or ""
    ).startswith("MARKET_FILL_FIFO_WAIT")


def _durable_fill_retry_delay_seconds(
    value: Any,
    *,
    attempt: int,
    base: float,
    cap: float,
) -> float:
    """Choose a retry deadline that preserves liveness without hot-spinning.

    A follower-position refresh happens after a stale-state failure.  Letting
    the ordinary exponential retry reach five seconds while position freshness
    expires after two seconds creates a phase-locked livelock: every retry sees
    the refresh only after it has become stale again.  Retry this condition in
    a short fixed window so the next plan observes the completed refresh.

    Market FIFO/handoff waits are normally very short, but a genuinely blocked
    predecessor must not make its successors write the database twenty times a
    second forever.  Back off those waits only to 500 ms; normal in-memory FIFO
    processing and explicit completion wakeups remain unchanged.
    """
    retry_base = max(0.01, float(base))
    retry_cap = max(retry_base, float(cap))
    if _follower_position_freshness_retry(value):
        return min(retry_cap, max(retry_base, 0.25))
    if _market_serialization_retry(value):
        adaptive = retry_base * (2 ** min(max(int(attempt) - 1, 0), 16))
        return min(retry_cap, max(retry_base, min(0.5, adaptive)))
    return min(
        retry_cap,
        retry_base * (2 ** min(max(int(attempt) - 1, 0), 16)),
    )


def _submit_barrier_poll_delay_seconds(waited_ms: int | float) -> float:
    """Poll aggressively for normal sub-second sequencing, then back off."""
    elapsed = max(0.0, float(waited_ms))
    if elapsed < 100:
        return 0.005
    if elapsed < 1_000:
        return 0.02
    return 0.1


def _submit_barrier_reconcile_interval_seconds(waited_ms: int | float) -> float:
    """Keep the normal self-heal fast without polling SQL forever.

    A completed predecessor releases its in-memory barrier immediately.  The
    database query is only a fallback for a lost callback, so progressively
    reducing that fallback's frequency cannot add latency to the normal path
    and prevents an unresolved exchange outcome from becoming a SQL livelock.
    """

    elapsed = max(0.0, float(waited_ms))
    if elapsed < 1_000:
        return 0.25
    if elapsed < 10_000:
        return 1.0
    return 5.0


def _durable_submit_retry_delay_seconds(retry_count: int) -> float:
    """Back off persistent pre-send failures after three immediate attempts."""
    deferred_attempt = max(0, int(retry_count) - 4)
    return min(5.0, 0.25 * (2 ** min(deferred_attempt, 16)))


def _expected_fill_retry(value: Any) -> bool:
    return (
        _market_serialization_retry(value)
        or _follower_position_freshness_retry(value)
        or str(value or "").startswith("MANUAL_FOLLOWER_POSITION_GUARD:")
    )


@dataclass(frozen=True)
class FillImpliedPosition:
    dex: str
    canonical_coin: str
    side_after: PositionSide
    size_after: Decimal
    signed_size_after: Decimal
    notional_after_estimate: Decimal
    fill_size: Decimal
    start_position: Decimal | None
    direction: str
    is_open: bool
    is_increase: bool
    is_reduce: bool
    is_close: bool
    is_flip: bool
    confidence: str
    reason: str
    entry_px: Decimal | None

    @property
    def side(self) -> PositionSide:
        return self.side_after

    @property
    def size(self) -> Decimal:
        return self.size_after

    @property
    def notional(self) -> Decimal:
        return self.notional_after_estimate


@dataclass(frozen=True)
class LiveLeaderPositionSnapshot:
    side: PositionSide
    size: Decimal
    signed_size: Decimal
    notional: Decimal
    entry_px: Decimal | None
    mark_px: Decimal | None
    raw_payload_masked: dict[str, Any]
    last_update_at: datetime


@dataclass
class PriceEntry:
    price: Decimal
    updated_at: datetime
    source: str


def _execution_price_entry_for_fill(
    *,
    cached_entry: PriceEntry | None,
    cache_fresh: bool,
    fill: FillEvent,
) -> tuple[PriceEntry | None, bool, bool, str]:
    if cached_entry is not None and cache_fresh:
        return cached_entry, True, False, cached_entry.source

    fill_price = Decimal(str(fill.price or "0"))
    if fill_price > 0:
        return (
            PriceEntry(
                price=fill_price,
                updated_at=fill.ws_received_at,
                source=LEADER_FILL_PRICE_FALLBACK_SOURCE,
            ),
            True,
            True,
            LEADER_FILL_PRICE_FALLBACK_SOURCE,
        )

    return cached_entry, False, False, cached_entry.source if cached_entry is not None else "missing"


class LowLatencyPriceCache:
    def __init__(self, *, stale_ms: int) -> None:
        self.stale_ms = stale_ms
        self._prices: dict[str, PriceEntry] = {}
        # Keep the normalized market grouping alongside the price map.  The
        # watcher receives roughly 1,100 mids across several DEXes; reparsing
        # every canonical coin once per DEX in each health/freshness pass used
        # unnecessary CPU on the same event loop that receives leader fills.
        self._markets_by_dex: dict[str, set[str]] = {}

    def set_price(
        self,
        *,
        dex: str,
        coin: str,
        price: Decimal | str,
        source: str = "REST",
    ) -> str | None:
        parsed = parse_coin(coin, default_dex=dex)
        value = Decimal(str(price))
        if value <= 0:
            return None
        self._prices[parsed.canonical_coin] = PriceEntry(
            price=value,
            updated_at=datetime.now(timezone.utc),
            source=source,
        )
        self._markets_by_dex.setdefault(parsed.dex, set()).add(parsed.canonical_coin)
        return parsed.canonical_coin

    def update_mids(self, *, dex: str, mids: dict[str, Any], source: str, replace: bool = False) -> None:
        updated: set[str] = set()
        for raw_coin, value in mids.items():
            try:
                canonical = self.set_price(
                    dex=dex,
                    coin=str(raw_coin),
                    price=Decimal(str(value)),
                    source=source,
                )
                if canonical is not None:
                    updated.add(canonical)
            except Exception:
                continue
        if replace and updated:
            target_dex = str(dex or "").lower()
            known = self._markets_by_dex.setdefault(target_dex, set())
            for canonical in known - updated:
                self._prices.pop(canonical, None)
            self._markets_by_dex[target_dex] = set(updated)

    def get(self, canonical: str) -> PriceEntry | None:
        return self._prices.get(parse_coin(canonical).canonical_coin)

    def fresh_mids_for_dex(self, dex: str, *, now: datetime | None = None) -> dict[str, Decimal]:
        now = now or datetime.now(timezone.utc)
        result: dict[str, Decimal] = {}
        target_dex = str(dex or "").lower()
        for canonical in self._markets_by_dex.get(target_dex, set()):
            entry = self._prices.get(canonical)
            if entry is None:
                continue
            parsed = parse_coin(canonical)
            if int((now - entry.updated_at).total_seconds() * 1000) > self.stale_ms:
                continue
            result[parsed.canonical_coin] = entry.price
            result[parsed.raw_coin] = entry.price
            result[parsed.coin] = entry.price

        return result

    def is_fresh(self, canonical: str, *, now: datetime | None = None) -> bool:
        entry = self.get(canonical)
        if entry is None:
            return False
        return self.age_ms(canonical, now=now) <= self.stale_ms

    def age_ms(self, canonical: str, *, now: datetime | None = None) -> int | None:
        entry = self.get(canonical)
        if entry is None:
            return None
        now = now or datetime.now(timezone.utc)
        return int((now - entry.updated_at).total_seconds() * 1000)

    def status_by_dex(self, dexes: list[str]) -> dict[str, dict[str, Any]]:
        now = datetime.now(timezone.utc)
        result: dict[str, dict[str, Any]] = {}
        for dex in dexes:
            target_dex = str(dex or "").lower()
            entries = [
                (coin, entry)
                for coin in self._markets_by_dex.get(target_dex, set())
                if (entry := self._prices.get(coin)) is not None
            ]
            ages = [int((now - entry.updated_at).total_seconds() * 1000) for _, entry in entries]
            stale = [coin for coin, entry in entries if int((now - entry.updated_at).total_seconds() * 1000) > self.stale_ms]
            result[target_dex] = {
                "markets_count": len(entries),
                "fresh": bool(entries) and not stale,
                "stale_markets_count": len(stale),
                "last_price_update_age_ms": min(ages) if ages else None,
            }
        return result

    def snapshot(
        self,
        dexes: list[str],
        *,
        canonical_coins: set[str] | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        allowed = {str(dex or "").lower() for dex in dexes}
        selected = (
            {parse_coin(value).canonical_coin for value in canonical_coins if value}
            if canonical_coins is not None
            else None
        )
        prices: dict[str, dict[str, Any]] = {}
        candidates = (
            selected
            if selected is not None
            else set().union(*(self._markets_by_dex.get(dex, set()) for dex in allowed))
        )
        for canonical in candidates:
            entry = self._prices.get(canonical)
            if entry is None:
                continue
            parsed = parse_coin(canonical)
            if parsed.dex not in allowed:
                continue
            age_ms = int((now - entry.updated_at).total_seconds() * 1000)
            prices[parsed.canonical_coin] = {
                "dex": parsed.dex,
                "coin": parsed.coin,
                "canonical_coin": parsed.canonical_coin,
                "price": str(entry.price),
                "updated_at": entry.updated_at.isoformat(),
                "age_ms": age_ms,
                "source": entry.source,
                "stale": age_ms > self.stale_ms,
            }
        return {
            "source": "low_latency_price_cache",
            "updated_at": now.isoformat(),
            "stale_ms": self.stale_ms,
            "prices": prices,
        }


@dataclass(frozen=True)
class PendingIntentScope:
    leader_id: int
    execution_venue: str
    dex: str
    canonical_coin: str
    position_side: str


@dataclass(frozen=True)
class PendingIntent:
    scope: PendingIntentScope
    allocation_id: int | None
    order_id: int | None
    source_fill_id: str | None
    cloid: str | None
    order_action: str
    reduce_only: bool
    quantity: Decimal
    notional: Decimal
    estimated_price: Decimal | None
    target_notional_after: Decimal | None
    leader_account_value: Decimal | None
    leader_position_notional: Decimal | None
    leader_position_size: Decimal | None
    copy_multiplier: Decimal | None
    created_at: datetime


class PendingIntentLedger:
    """Runtime overlay for orders planned but not finalized yet."""

    def __init__(self) -> None:
        self._by_order_id: dict[int, PendingIntent] = {}
        self._by_source_fill_id: dict[str, PendingIntent] = {}
        self._by_cloid: dict[str, PendingIntent] = {}
        self._by_scope: dict[PendingIntentScope, OrderedDict[int | str, PendingIntent]] = {}

    def reserve(self, order: ExecutionOrder, allocation: LeaderPositionAllocationRecord | None) -> None:
        if order.status != "PENDING_SUBMIT" or allocation is None or not order.position_side:
            return
        scope = PendingIntentScope(
            leader_id=int(order.leader_id or allocation.leader_id or 0),
            execution_venue=str(order.execution_venue or ExecutionVenue.HYPERLIQUID.value).upper(),
            dex=str(order.dex or "").lower(),
            canonical_coin=str(order.canonical_coin or allocation.canonical_coin or "").upper(),
            position_side=str(order.position_side or allocation.position_side or "").upper(),
        )
        key: int | str = int(order.id) if order.id is not None else (order.cloid or order.source_fill_id or str(id(order)))
        if isinstance(key, int) and key in self._by_order_id:
            return
        if order.source_fill_id and order.source_fill_id in self._by_source_fill_id:
            return
        if order.cloid and order.cloid in self._by_cloid:
            return
        intent = PendingIntent(
            scope=scope,
            allocation_id=int(allocation.id) if allocation.id is not None else None,
            order_id=int(order.id) if order.id is not None else None,
            source_fill_id=order.source_fill_id,
            cloid=order.cloid,
            order_action=str(order.order_action or "").upper(),
            reduce_only=bool(order.reduce_only),
            quantity=abs(Decimal(order.quantity or 0)),
            notional=abs(Decimal(order.notional or 0)),
            estimated_price=Decimal(order.estimated_price) if order.estimated_price is not None else None,
            target_notional_after=Decimal(order.target_notional) if order.target_notional is not None else None,
            leader_account_value=Decimal(order.leader_account_value) if order.leader_account_value is not None else None,
            leader_position_notional=Decimal(order.leader_position_notional)
            if order.leader_position_notional is not None
            else None,
            leader_position_size=Decimal(allocation.last_leader_position_size)
            if allocation.last_leader_position_size is not None
            else None,
            copy_multiplier=Decimal(order.copy_multiplier) if order.copy_multiplier is not None else None,
            created_at=datetime.now(timezone.utc),
        )
        if intent.order_id is not None:
            self._by_order_id[int(intent.order_id)] = intent
        if intent.source_fill_id:
            self._by_source_fill_id[intent.source_fill_id] = intent
        if intent.cloid:
            self._by_cloid[intent.cloid] = intent
        self._by_scope.setdefault(scope, OrderedDict())[key] = intent

    def release(self, order: ExecutionOrder) -> None:
        intent = None
        if order.id is not None:
            intent = self._by_order_id.pop(int(order.id), None)
        if intent is None and order.source_fill_id:
            intent = self._by_source_fill_id.get(order.source_fill_id)
        if intent is None and order.cloid:
            intent = self._by_cloid.get(order.cloid)
        if intent is None:
            return
        if intent.order_id is not None:
            self._by_order_id.pop(int(intent.order_id), None)
        if intent.source_fill_id:
            self._by_source_fill_id.pop(intent.source_fill_id, None)
        if intent.cloid:
            self._by_cloid.pop(intent.cloid, None)
        bucket = self._by_scope.get(intent.scope)
        if bucket is not None:
            for key, value in list(bucket.items()):
                if value is intent:
                    bucket.pop(key, None)
            if not bucket:
                self._by_scope.pop(intent.scope, None)

    def release_order_id(self, order_id: int) -> None:
        intent = self._by_order_id.get(int(order_id))
        if intent is None:
            return
        order = SimpleNamespace(id=int(order_id), source_fill_id=intent.source_fill_id, cloid=intent.cloid)
        self.release(order)

    def order_ids_for_market(self, *, dex: str, canonical_coin: str) -> list[int]:
        ids: list[int] = []
        dex_key = str(dex or "").lower()
        coin_key = str(canonical_coin or "").upper()
        for scope, intents in self._by_scope.items():
            if scope.dex != dex_key or scope.canonical_coin != coin_key:
                continue
            for intent in intents.values():
                if intent.order_id is not None:
                    ids.append(int(intent.order_id))
        return ids

    def has_active_order(self, order: ExecutionOrder | None) -> bool:
        if order is None:
            return False
        if order.id is not None and int(order.id) in self._by_order_id:
            return True
        if order.source_fill_id and order.source_fill_id in self._by_source_fill_id:
            return True
        if order.cloid and order.cloid in self._by_cloid:
            return True
        return False

    def has_active_order_id(self, order_id: int) -> bool:
        return int(order_id) in self._by_order_id

    def has_cloid(self, cloid: str | None) -> bool:
        return bool(cloid and str(cloid) in self._by_cloid)

    def has_pending_allocation(self, allocation: LeaderPositionAllocationRecord | None) -> bool:
        scope = self._scope_from_allocation(allocation)
        return bool(scope and self._by_scope.get(scope))

    def submit_barriers_before(self, order: ExecutionOrder | None) -> list[PendingIntent]:
        current = self.intent_for_order(order)
        return self._submit_barriers_before_intent(current)

    def submit_barriers_before_order_id(self, order_id: int) -> list[PendingIntent]:
        return self._submit_barriers_before_intent(self._by_order_id.get(int(order_id)))

    def _submit_barriers_before_intent(self, current: PendingIntent | None) -> list[PendingIntent]:
        if current is None:
            return []
        barriers: list[PendingIntent] = []
        for scope, intents in self._by_scope.items():
            if not self._same_market_scope(scope, current.scope):
                continue
            for intent in intents.values():
                if intent is current:
                    continue
                if intent.order_id is not None and intent.order_id == current.order_id:
                    continue
                if intent.created_at > current.created_at:
                    continue
                if self._intent_blocks_submit(intent, current):
                    barriers.append(intent)
        barriers.sort(key=lambda intent: intent.created_at)
        return barriers

    def reduce_quantity_before(self, order: ExecutionOrder | None) -> Decimal:
        current = self.intent_for_order(order)
        if current is None:
            return Decimal("0")
        qty = Decimal("0")
        for scope, intents in self._by_scope.items():
            if scope != current.scope:
                continue
            for intent in intents.values():
                if intent is current:
                    continue
                if intent.created_at > current.created_at:
                    continue
                if (
                    current.allocation_id is not None
                    and intent.allocation_id is not None
                    and intent.allocation_id != current.allocation_id
                ):
                    continue
                if self._intent_reduces(intent):
                    qty += max(Decimal("0"), intent.quantity)
        return _q(qty)

    def intent_for_order(self, order: ExecutionOrder | None) -> PendingIntent | None:
        if order is None:
            return None
        if order.id is not None:
            intent = self._by_order_id.get(int(order.id))
            if intent is not None:
                return intent
        if order.source_fill_id:
            intent = self._by_source_fill_id.get(order.source_fill_id)
            if intent is not None:
                return intent
        if order.cloid:
            intent = self._by_cloid.get(order.cloid)
            if intent is not None:
                return intent
        return None

    def overlay_allocation(self, allocation: LeaderPositionAllocationRecord | None) -> Any | None:
        if allocation is None:
            return None
        scope = self._scope_from_allocation(allocation)
        intents = list(self._by_scope.get(scope, {}).values()) if scope else []
        if not intents:
            return allocation
        snapshot = SimpleNamespace(
            **{column.name: getattr(allocation, column.name) for column in LeaderPositionAllocationRecord.__table__.columns}
        )
        qty = abs(Decimal(snapshot.allocated_qty or 0))
        notional = abs(Decimal(snapshot.allocated_notional or 0))
        avg_entry = Decimal(snapshot.avg_entry_price) if snapshot.avg_entry_price is not None else None
        for intent in intents:
            if self._intent_increases(intent):
                add_qty = max(Decimal("0"), intent.quantity)
                add_notional = self._intent_notional(intent)
                new_qty = qty + add_qty
                new_notional = notional + add_notional
                avg_entry = _q(new_notional / new_qty) if new_qty > 0 else avg_entry
                qty = new_qty
                notional = new_notional
            else:
                reduce_qty = min(qty, max(Decimal("0"), intent.quantity))
                if qty > ALLOCATION_TRANSITION_TOLERANCE:
                    remaining_ratio = max(Decimal("0"), (qty - reduce_qty) / qty)
                    qty = max(Decimal("0"), qty - reduce_qty)
                    notional = _q(notional * remaining_ratio)
                if intent.target_notional_after is not None:
                    notional = min(notional, max(Decimal("0"), intent.target_notional_after))
        snapshot.allocated_qty = _q(qty)
        snapshot.allocated_notional = _q(notional)
        snapshot.avg_entry_price = avg_entry
        latest = intents[-1]
        snapshot.target_notional = (
            latest.target_notional_after if latest.target_notional_after is not None else snapshot.target_notional
        )
        snapshot.last_leader_account_value = latest.leader_account_value or snapshot.last_leader_account_value
        snapshot.last_leader_position_notional = (
            latest.leader_position_notional
            if latest.leader_position_notional is not None
            else snapshot.last_leader_position_notional
        )
        snapshot.last_leader_position_size = (
            latest.leader_position_size if latest.leader_position_size is not None else snapshot.last_leader_position_size
        )
        snapshot.copy_multiplier = latest.copy_multiplier or snapshot.copy_multiplier
        snapshot.last_source_fill_id = latest.source_fill_id or snapshot.last_source_fill_id
        snapshot.last_reconcile_at = latest.created_at
        return snapshot

    def effective_qty(
        self,
        *,
        dex: str,
        canonical_coin: str,
        side: PositionSide,
        base_qty: Decimal | None,
    ) -> Decimal:
        qty = Decimal(base_qty or 0)
        for scope, intents in self._by_scope.items():
            if scope.dex != str(dex or "").lower():
                continue
            if scope.canonical_coin != str(canonical_coin or "").upper():
                continue
            if scope.position_side != side.value:
                continue
            for intent in intents.values():
                if self._intent_increases(intent):
                    qty += intent.quantity
                else:
                    qty = max(Decimal("0"), qty - intent.quantity)
        return _q(max(Decimal("0"), qty))

    def effective_qtys(
        self,
        *,
        dex: str,
        canonical_coin: str,
        base_qtys: dict[PositionSide, Decimal],
    ) -> dict[PositionSide, Decimal]:
        qtys = dict(base_qtys)
        for side in (PositionSide.LONG, PositionSide.SHORT):
            qtys[side] = self.effective_qty(
                dex=dex,
                canonical_coin=canonical_coin,
                side=side,
                base_qty=qtys.get(side, Decimal("0")),
            )
        return qtys

    @staticmethod
    def _intent_increases(intent: PendingIntent) -> bool:
        return not intent.reduce_only and intent.order_action in {
            AllocationTransitionAction.OPEN.value,
            AllocationTransitionAction.INCREASE.value,
            AllocationTransitionAction.FLIP_OPEN_SECOND.value,
        }

    @staticmethod
    def _intent_notional(intent: PendingIntent) -> Decimal:
        if intent.notional > 0:
            return intent.notional
        if intent.estimated_price is not None and intent.estimated_price > 0:
            return _q(intent.quantity * intent.estimated_price)
        return Decimal("0")

    @staticmethod
    def _same_market_scope(left: PendingIntentScope, right: PendingIntentScope) -> bool:
        return (
            left.leader_id == right.leader_id
            and left.execution_venue == right.execution_venue
            and left.dex == right.dex
            and left.canonical_coin == right.canonical_coin
        )

    @staticmethod
    def _intent_reduces(intent: PendingIntent) -> bool:
        return bool(intent.reduce_only) or _reduce_like_action(intent.order_action)

    @classmethod
    def _intent_blocks_submit(cls, prior: PendingIntent, current: PendingIntent) -> bool:
        if cls._intent_reduces(current):
            return cls._intent_increases(prior)
        if cls._intent_increases(current):
            return cls._intent_reduces(prior)
        return True

    @staticmethod
    def _scope_from_allocation(allocation: LeaderPositionAllocationRecord | None) -> PendingIntentScope | None:
        if allocation is None or not allocation.position_side:
            return None
        return PendingIntentScope(
            leader_id=int(allocation.leader_id or 0),
            execution_venue=str(allocation.execution_venue or ExecutionVenue.HYPERLIQUID.value).upper(),
            dex=str(allocation.dex or "").lower(),
            canonical_coin=str(allocation.canonical_coin or "").upper(),
            position_side=str(allocation.position_side or "").upper(),
        )


@dataclass
class FollowerManualPositionGuardEntry:
    dex: str
    canonical_coin: str
    created_at: datetime
    reason: str
    position_version: int = 0
    expected_position_side: PositionSide | None = None
    expected_position_qty: Decimal | None = None
    expected_position_relation: str | None = None
    position_change_confirmed_at: datetime | None = None

    def blocker_message(self) -> str:
        return (
            "MANUAL_FOLLOWER_POSITION_GUARD: follower has an unmatched "
            f"{self.canonical_coin} fill; copy actions are disabled until the follower position "
            "and allocation ledger are reconciled"
        )


class FollowerManualPositionGuard:
    def __init__(self) -> None:
        self._by_market: dict[tuple[str, str], FollowerManualPositionGuardEntry] = {}

    def mark(
        self,
        market: MarketKey,
        *,
        reason: str,
        observed_at: datetime | None = None,
        position_version: int = 0,
        expected_position_side: PositionSide | str | None = None,
        expected_position_qty: Decimal | None = None,
        expected_position_relation: str | None = None,
        position_change_confirmed_at: datetime | None = None,
    ) -> None:
        key = self._key(market)
        self._by_market[key] = FollowerManualPositionGuardEntry(
            dex=key[0],
            canonical_coin=key[1],
            created_at=_datetime_or_none(observed_at) or datetime.now(timezone.utc),
            reason=reason[:500],
            position_version=max(int(position_version or 0), 0),
            expected_position_side=_position_side_or_none(expected_position_side),
            expected_position_qty=(
                abs(Decimal(expected_position_qty))
                if expected_position_qty is not None
                else None
            ),
            expected_position_relation=str(expected_position_relation or "").upper() or None,
            position_change_confirmed_at=_datetime_or_none(position_change_confirmed_at),
        )

    def active_entry(self, market: MarketKey) -> FollowerManualPositionGuardEntry | None:
        return self._by_market.get(self._key(market))

    def clear(self, market: MarketKey) -> None:
        self._by_market.pop(self._key(market), None)

    def entries(self) -> list[FollowerManualPositionGuardEntry]:
        return list(self._by_market.values())

    def confirm_if_observed(
        self,
        market: MarketKey,
        *,
        follower_qty_by_side: dict[PositionSide, Decimal],
        allocation_qty_by_side: dict[PositionSide, Decimal] | None = None,
        follower_state_at: datetime | None,
    ) -> datetime | None:
        entry = self.active_entry(market)
        if entry is None:
            return None
        if entry.position_change_confirmed_at is not None:
            return entry.position_change_confirmed_at
        observed_state_at = _datetime_or_none(follower_state_at)
        trustworthy_after = entry.created_at + timedelta(
            seconds=UNMATCHED_FOLLOWER_FILL_STATE_SETTLE_SECONDS
        )
        if observed_state_at is None or observed_state_at < trustworthy_after:
            return None
        absolute_checkpoint_seen = _manual_fill_position_checkpoint_satisfied(
            entry, follower_qty_by_side=follower_qty_by_side
        )
        allocation_delta_seen = _manual_fill_allocation_delta_satisfied(
            entry,
            follower_qty_by_side=follower_qty_by_side,
            allocation_qty_by_side=allocation_qty_by_side,
        )
        if not absolute_checkpoint_seen and not allocation_delta_seen:
            return None
        entry.position_change_confirmed_at = observed_state_at
        return observed_state_at

    def reconcile(
        self,
        market: MarketKey,
        *,
        unmanaged_qty_by_side: dict[PositionSide, Decimal],
        follower_state_at: datetime | None,
        allocation_mismatch: bool = False,
        follower_qty_by_side: dict[PositionSide, Decimal] | None = None,
        allocation_qty_by_side: dict[PositionSide, Decimal] | None = None,
    ) -> FollowerManualPositionGuardEntry | None:
        entry = self.active_entry(market)
        if entry is None:
            return None
        self.confirm_if_observed(
            market,
            follower_qty_by_side=follower_qty_by_side or _empty_position_side_qtys(),
            allocation_qty_by_side=allocation_qty_by_side,
            follower_state_at=follower_state_at,
        )
        follower_qtys = follower_qty_by_side or _empty_position_side_qtys()
        allocation_qtys = allocation_qty_by_side or _empty_position_side_qtys()
        follower_flat = all(
            abs(Decimal(follower_qtys.get(side, 0))) <= ALLOCATION_TRANSITION_TOLERANCE
            for side in (PositionSide.LONG, PositionSide.SHORT)
        )
        allocation_flat = all(
            abs(Decimal(allocation_qtys.get(side, 0))) <= ALLOCATION_TRANSITION_TOLERANCE
            for side in (PositionSide.LONG, PositionSide.SHORT)
        )
        # A restart can restore a guard after the fill-implied checkpoint has
        # already come and gone.  Literal follower-flat plus allocation-flat is
        # an authoritative convergence point, so keeping the guard in that
        # state can only deadlock the next lifecycle.
        if (
            follower_qty_by_side is not None
            and allocation_qty_by_side is not None
            and follower_flat
            and allocation_flat
        ):
            self.clear(market)
            return None
        if entry.position_change_confirmed_at is None:
            return entry
        has_unmanaged = any(qty > ALLOCATION_TRANSITION_TOLERANCE for qty in unmanaged_qty_by_side.values())
        if has_unmanaged or allocation_mismatch:
            return entry
        self.clear(market)
        return None

    def snapshot(self) -> list[dict[str, str]]:
        return [
            {
                "dex": entry.dex,
                "canonical_coin": entry.canonical_coin,
                "created_at": entry.created_at.isoformat(),
                "reason": entry.reason,
            }
            for entry in self._by_market.values()
        ]

    @staticmethod
    def _key(market: MarketKey) -> tuple[str, str]:
        return str(market.dex or "").lower(), str(market.canonical_coin or "").upper()


def _manual_fill_position_confirmation_spec(
    fill: dict[str, Any],
) -> tuple[PositionSide, Decimal, str] | None:
    start = _decimal_from_value(fill.get("startPosition"))
    size = _decimal_from_value(fill.get("sz"))
    side = str(fill.get("side") or "").upper()
    if start is None or size is None or size <= 0 or side not in {"A", "B"}:
        return None
    after = start + size if side == "B" else start - size
    before_abs = abs(start)
    after_abs = abs(after)
    before_side = PositionSide.LONG if start > 0 else PositionSide.SHORT if start < 0 else None
    after_side = PositionSide.LONG if after > 0 else PositionSide.SHORT if after < 0 else None
    if before_side is not None and after_side is not None and before_side != after_side:
        # A manual flip cannot safely be attributed to the old copy lifecycle.
        return None
    if after_abs > before_abs + ALLOCATION_TRANSITION_TOLERANCE:
        return (
            after_side or before_side or PositionSide.FLAT,
            _q(after_abs),
            MANUAL_FILL_POSITION_AT_LEAST,
        )
    if after_abs < before_abs - ALLOCATION_TRANSITION_TOLERANCE:
        return (
            before_side or after_side or PositionSide.FLAT,
            _q(after_abs),
            MANUAL_FILL_POSITION_AT_MOST,
        )
    checkpoint_side = after_side or before_side
    if checkpoint_side is None:
        return None
    return checkpoint_side, _q(after_abs), MANUAL_FILL_POSITION_EXACT


def _manual_fill_position_checkpoint_satisfied(
    entry: FollowerManualPositionGuardEntry,
    *,
    follower_qty_by_side: dict[PositionSide, Decimal],
) -> bool:
    side = entry.expected_position_side
    expected = entry.expected_position_qty
    relation = str(entry.expected_position_relation or "").upper()
    if side not in {PositionSide.LONG, PositionSide.SHORT} or expected is None:
        return False
    opposite = _opposite_side(side)
    if Decimal(follower_qty_by_side.get(opposite, 0)) > ALLOCATION_TRANSITION_TOLERANCE:
        return False
    actual = abs(Decimal(follower_qty_by_side.get(side, 0)))
    if relation == MANUAL_FILL_POSITION_AT_LEAST:
        return actual >= expected - ALLOCATION_TRANSITION_TOLERANCE
    if relation == MANUAL_FILL_POSITION_AT_MOST:
        return actual <= expected + ALLOCATION_TRANSITION_TOLERANCE
    if relation == MANUAL_FILL_POSITION_EXACT:
        return abs(actual - expected) <= ALLOCATION_TRANSITION_TOLERANCE
    return False


def _manual_fill_allocation_delta_satisfied(
    entry: FollowerManualPositionGuardEntry,
    *,
    follower_qty_by_side: dict[PositionSide, Decimal],
    allocation_qty_by_side: dict[PositionSide, Decimal] | None,
) -> bool:
    if allocation_qty_by_side is None:
        return False
    side = entry.expected_position_side
    relation = str(entry.expected_position_relation or "").upper()
    if side not in {PositionSide.LONG, PositionSide.SHORT}:
        return False
    opposite = _opposite_side(side)
    if Decimal(follower_qty_by_side.get(opposite, 0)) > ALLOCATION_TRANSITION_TOLERANCE:
        return False
    actual = abs(Decimal(follower_qty_by_side.get(side, 0)))
    allocated = abs(Decimal(allocation_qty_by_side.get(side, 0)))
    if relation == MANUAL_FILL_POSITION_AT_LEAST:
        return actual > allocated + ALLOCATION_TRANSITION_TOLERANCE
    if relation == MANUAL_FILL_POSITION_AT_MOST:
        return actual < allocated - ALLOCATION_TRANSITION_TOLERANCE
    if relation == MANUAL_FILL_POSITION_EXACT:
        return abs(actual - allocated) <= ALLOCATION_TRANSITION_TOLERANCE
    return False


def parse_fill_to_market_key(fill: dict[str, Any], *, default_dex: str = "") -> MarketKey:
    raw_coin = str(fill.get("coin") or fill.get("name") or fill.get("symbol") or "")
    dex_hint = str(fill.get("dex") or fill.get("perpDex") or fill.get("dexName") or default_dex or "")
    parsed = parse_coin(raw_coin, default_dex=dex_hint)
    asset_id = _int_or_none(_first_present(fill.get("asset"), fill.get("assetId"), fill.get("a")))
    if not parsed.coin:
        raise ValueError("fill coin missing")
    return MarketKey(
        dex=parsed.dex,
        coin=parsed.coin,
        canonical_coin=parsed.canonical_coin,
        raw_coin=raw_coin,
        asset_id=asset_id,
        venue_symbol=parsed.canonical_coin,
    )


class FillDrivenExecutionEngine:
    def __init__(
        self,
        *,
        settings: Settings,
        info_client: HyperliquidInfoClient,
        account_info_client: HyperliquidInfoClient | None = None,
        execution_client: HyperliquidExecutionClient,
        price_cache: LowLatencyPriceCache,
        manual_position_guard: FollowerManualPositionGuard | None = None,
        follower_position_stream_trusted: Callable[[str], bool] | None = None,
        shared_db_session_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.settings = settings
        execution_scope = getattr(settings, "low_latency_execution_scope", None)
        self.execution_scope = (
            str(execution_scope() or "").lower()
            if callable(execution_scope)
            else ""
        )
        self.info_client = info_client
        # Balance polling has its own HTTP connection pool and rate-limit
        # queue. It can never occupy the info-client slots needed by a fill
        # that encounters a genuinely new market or needs live recovery data.
        self.account_info_client = account_info_client or info_client
        self.execution_client = execution_client
        self.price_cache = price_cache
        self.manual_position_guard = manual_position_guard
        self._follower_position_stream_trusted = follower_position_stream_trusted
        self._shared_db_session_factory = shared_db_session_factory or SessionLocal
        self.market_leverage_plan_cache: dict[tuple[str, str], Any] = {}
        self._asset_id_cache: dict[tuple[str, str], int] = {}
        self._market_asset_offsets: dict[str, int] = {}
        self._perp_dex_directory_refreshed_at: datetime | None = None
        self._market_meta_cache: dict[str, tuple[datetime, dict[str, Any]]] = {}
        self._market_meta_refresh_locks: dict[str, asyncio.Lock] = {}
        self._market_meta_force_refresh_at: dict[str, datetime] = {}
        self._risk_settings_ok_cache: dict[tuple[str, str, int | None, str], RiskSettingResult] = {}
        self._risk_settings_locks: dict[tuple[str, str, int | None], asyncio.Lock] = {}
        self._account_abstraction_cache: dict[str, tuple[datetime, dict[str, Any]]] = {}
        self._account_abstraction_refresh_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._account_abstraction_full_refresh_at: dict[tuple[str, str], datetime] = {}
        self.pending_intents = PendingIntentLedger()
        self._state_refresh_task: asyncio.Task | None = None
        self._account_abstraction_refresh_tasks: dict[tuple[str, str, str], asyncio.Task] = {}

    async def warm_risk_settings_cache(self, db: Any) -> int:
        account = self.settings.hyperliquid_follower_account_address()
        if not account:
            return 0
        rows = (
            await db.execute(
                select(MarketRiskSetting)
                .where(MarketRiskSetting.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(MarketRiskSetting.account_address == account.lower())
                .where(MarketRiskSetting.status == STATUS_CONFIRMED)
                .where(MarketRiskSetting.effective_leverage.is_not(None))
            )
        ).scalars().all()
        warmed = 0
        for row in rows:
            persisted_effective = _int_or_none(row.effective_leverage)
            if one_x_leverage_required(
                margin_mode=row.actual_margin_mode or row.desired_margin_mode,
                canonical_coin_value=row.canonical_coin,
            ):
                policy_effective = 1
            else:
                policy_effective = (
                    _int_or_none(row.market_max_leverage)
                    or int(self.settings.hyperliquid_default_leverage or 10)
                )
            if (
                persisted_effective != policy_effective
                or _int_or_none(row.actual_leverage) != policy_effective
            ):
                # Never reuse a legacy >1x isolated/CXMT confirmation.  The
                # first risk-increasing order must reconfirm the 1x policy at
                # the exchange before it can submit.
                continue
            result = _risk_setting_result_from_row(row)
            self._risk_settings_ok_cache[
                (
                    str(row.dex or "").lower(),
                    str(row.canonical_coin or "").upper(),
                    policy_effective,
                    row.desired_margin_mode or DESIRED_MARGIN_MODE,
                )
            ] = result
            if row.actual_margin_mode and row.actual_margin_mode != row.desired_margin_mode:
                self._risk_settings_ok_cache[
                    (
                        str(row.dex or "").lower(),
                        str(row.canonical_coin or "").upper(),
                        policy_effective,
                        row.actual_margin_mode,
                    )
                ] = result
            warmed += 1
        return warmed

    async def warm_account_abstraction_cache(self, db: Any, *, leader_addresses: list[str]) -> int:
        warmed = 0
        follower = self.settings.hyperliquid_follower_account_address()
        requests: list[tuple[str, str]] = []
        if follower:
            requests.append((FOLLOWER, follower))
        for address in leader_addresses:
            normalized = normalize_leader_address(address)
            if normalized:
                requests.append((LEADER, normalized))

        seen: set[tuple[str, str]] = set()
        for role, address in requests:
            key = (role.upper(), address.lower())
            if key in seen:
                continue
            seen.add(key)
            await self._refresh_account_abstraction(db, role, address)
            warmed += 1
        return warmed

    async def handle_fill(
        self,
        db: Any,
        fill: FillEvent,
        leader: LeaderConfig,
        *,
        submit_order: bool = True,
    ) -> ExecutionOrder | None:
        if fill.is_snapshot:
            recovery_reason = await self._snapshot_recovery_reason(db, fill, leader)
            if recovery_reason is None:
                await self._record_source_fill(db, fill, processed=False)
                return None
            fill = _snapshot_recovery_fill(fill, reason=recovery_reason)
        dedupe_started_at = datetime.now(timezone.utc)
        ws_received_at = fill.ws_received_at
        inserted = await self._claim_source_fill_group(db, fill)
        dedupe_done_at = datetime.now(timezone.utc)
        if not inserted:
            return None
        await self._assert_market_fill_fifo(db, fill)
        debounce_ms = min(max(int(self.settings.max_fill_debounce_ms), 0), 1000)
        debounce_started_at = datetime.now(timezone.utc)
        if debounce_ms:
            await asyncio.sleep(debounce_ms / 1000)
        debounce_released_at = datetime.now(timezone.utc)

        no_lock_at = datetime.now(timezone.utc)
        return await self.reconcile_leader_symbol_allocation(
            db,
            fill=fill,
            leader=leader,
            dedupe_started_at=dedupe_started_at,
            dedupe_done_at=dedupe_done_at,
            debounce_started_at=debounce_started_at,
            debounce_released_at=debounce_released_at,
            lock_wait_started_at=no_lock_at,
            lock_acquired_at=no_lock_at,
            ws_received_at=ws_received_at,
            submit_order=submit_order,
        )

    async def reconcile_leader_symbol_allocation(
        self,
        db: Any,
        *,
        fill: FillEvent,
        leader: LeaderConfig,
        dedupe_started_at: datetime,
        dedupe_done_at: datetime,
        debounce_started_at: datetime,
        debounce_released_at: datetime,
        lock_wait_started_at: datetime,
        lock_acquired_at: datetime,
        ws_received_at: datetime,
        submit_order: bool = True,
    ) -> ExecutionOrder | None:
        decision_started_at = datetime.now(timezone.utc)
        event_time = fill.hyperliquid_event_time
        blockers: list[str] = []
        lifecycle_checkpoint_flip_open = False
        lifecycle_economic_dust_reopen = False
        lifecycle_economic_dust_reopen_mode: str | None = None
        lifecycle_allocation: LeaderPositionAllocationRecord | None = None
        minimum_tradeable_notional = Decimal(
            str(self.settings.hyperliquid_min_order_value_usd)
        )

        # Ownership acquisition has a deliberately dependency-free fast gate.
        # Once a market is released, an unrelated leader's add/reduce/close/flip
        # belongs to a lifecycle that began while somebody else owned the market.
        # Complete it as IGNORED before metadata, prices, account values, follower
        # guards, or handoff waits so it cannot head-of-line block a later genuine
        # startPosition=0 open.  A same-side increase from an economically flat
        # (< venue minimum) leader remainder is also a genuine new lifecycle once
        # it crosses back into tradeable size; the authoritative follower-flat and
        # ownership checks still run later before any order can be submitted.
        if isinstance(db, AsyncSession):
            await self._release_resolved_pending_intents_for_market(db, fill.market)
            lifecycle_allocation = await self._peek_allocation_for_lifecycle_gate(
                db,
                leader,
                fill.market,
            )
            lifecycle_planning_allocation = self.pending_intents.overlay_allocation(
                lifecycle_allocation
            )
            early_fill_implied_position = derive_leader_post_position_from_fill(fill)
            pending_checkpoint_dust_reopen = (
                _fill_is_minimum_residual_pending_checkpoint_reopen(
                    lifecycle_allocation,
                    early_fill_implied_position,
                )
            )
            lifecycle_has_durable_unresolved_order = bool(
                lifecycle_allocation is not None
                and (
                    not _allocation_lifecycle_active(
                        lifecycle_planning_allocation
                    )
                    or pending_checkpoint_dust_reopen
                )
                and await self._allocation_has_durable_unresolved_order(
                    db,
                    lifecycle_allocation,
                )
            )
            if (
                pending_checkpoint_dust_reopen
                and lifecycle_has_durable_unresolved_order
            ):
                raise MarketOwnershipHandoffPending(
                    "MINIMUM_RESIDUAL_HANDOFF_PENDING: the follower economic "
                    "close is unresolved; this dust reopen remains durable and "
                    "will compete from its original FIFO position after release"
                )
            absolute_dust_reopen = _fill_is_economic_dust_reopen(
                early_fill_implied_position,
                reference_price=fill.price,
                min_order_value=minimum_tradeable_notional,
            )
            checkpoint_dust_reopen = (
                _fill_is_minimum_residual_checkpoint_reopen(
                    lifecycle_planning_allocation,
                    early_fill_implied_position,
                )
            )
            lifecycle_economic_dust_reopen = bool(
                not _allocation_lifecycle_active(lifecycle_planning_allocation)
                and (absolute_dust_reopen or checkpoint_dust_reopen)
            )
            lifecycle_economic_dust_reopen_mode = (
                "FOLLOWER_MINIMUM_RESIDUAL_CHECKPOINT"
                if lifecycle_economic_dust_reopen and checkpoint_dust_reopen
                else "LEADER_ABSOLUTE_DUST"
                if lifecycle_economic_dust_reopen and absolute_dust_reopen
                else None
            )
            early_lifecycle_ignore_reason = (
                None
                if lifecycle_has_durable_unresolved_order
                else _ignore_without_allocation_reason(
                    lifecycle_planning_allocation,
                    early_fill_implied_position,
                    allow_economic_dust_reopen=lifecycle_economic_dust_reopen,
                )
            )
            lifecycle_checkpoint_flip_open = _fill_is_checkpoint_contiguous_flip_open(
                lifecycle_planning_allocation,
                early_fill_implied_position,
            )
            if early_lifecycle_ignore_reason:
                early_now = datetime.now(timezone.utc)
                early_use_fill_position = _should_use_fill_derived_position(
                    None,
                    fill,
                    early_fill_implied_position,
                )
                early_leader_notional = (
                    early_fill_implied_position.notional_after_estimate
                    if early_use_fill_position
                    else None
                )
                early_side = (
                    PositionSide.LONG
                    if early_leader_notional is not None and early_leader_notional > 0
                    else PositionSide.SHORT
                    if early_leader_notional is not None and early_leader_notional < 0
                    else PositionSide.FLAT
                )
                return await self._record_lifecycle_ignored_order(
                    db,
                    fill=fill,
                    leader=leader,
                    reason=early_lifecycle_ignore_reason,
                    target_side_hint=early_side,
                    leader_position_notional=early_leader_notional,
                    leader_entry_px=(
                        early_fill_implied_position.entry_px
                        if early_use_fill_position
                        else None
                    ),
                    leader_account_value=_configured_leader_account_value(leader),
                    follower_account_value=None,
                    dedupe_started_at=dedupe_started_at,
                    dedupe_done_at=dedupe_done_at,
                    debounce_started_at=debounce_started_at,
                    debounce_released_at=debounce_released_at,
                    lock_wait_started_at=lock_wait_started_at,
                    lock_acquired_at=lock_acquired_at,
                    ws_received_at=ws_received_at,
                    decision_started_at=decision_started_at,
                    account_cache_read_at=early_now,
                    account_cache_read_done_at=early_now,
                    price_cache_read_at=early_now,
                    price_cache_read_done_at=early_now,
                )

        manual_owner_reason = await self._standalone_manual_market_owner_blocker(
            db,
            fill.market,
        )
        if manual_owner_reason:
            manual_owner_now = datetime.now(timezone.utc)
            manual_owner_implied = derive_leader_post_position_from_fill(fill)
            manual_owner_use_fill_position = _should_use_fill_derived_position(
                None,
                fill,
                manual_owner_implied,
            )
            manual_owner_notional = (
                manual_owner_implied.notional_after_estimate
                if manual_owner_use_fill_position
                else None
            )
            manual_owner_side = (
                PositionSide.LONG
                if manual_owner_notional is not None and manual_owner_notional > 0
                else PositionSide.SHORT
                if manual_owner_notional is not None and manual_owner_notional < 0
                else PositionSide.FLAT
            )
            return await self._record_lifecycle_ignored_order(
                db,
                fill=fill,
                leader=leader,
                reason=manual_owner_reason,
                target_side_hint=manual_owner_side,
                leader_position_notional=manual_owner_notional,
                leader_entry_px=(
                    manual_owner_implied.entry_px
                    if manual_owner_use_fill_position
                    else None
                ),
                leader_account_value=_configured_leader_account_value(leader),
                follower_account_value=None,
                dedupe_started_at=dedupe_started_at,
                dedupe_done_at=dedupe_done_at,
                debounce_started_at=debounce_started_at,
                debounce_released_at=debounce_released_at,
                lock_wait_started_at=lock_wait_started_at,
                lock_acquired_at=lock_acquired_at,
                ws_received_at=ws_received_at,
                decision_started_at=decision_started_at,
                account_cache_read_at=manual_owner_now,
                account_cache_read_done_at=manual_owner_now,
                price_cache_read_at=manual_owner_now,
                price_cache_read_done_at=manual_owner_now,
            )

        follower_market_position_version = await self._follower_market_position_version_for_plan(
            db,
            fill.market,
        )

        coin_allowed_by_config = is_coin_allowed(leader, fill.market.canonical_coin)
        asset_id_hydrate_started_at = None
        asset_id_hydrate_done_at = None
        asset_id_source = "fill"
        if fill.market.asset_id is None:
            asset_id_hydrate_started_at = datetime.now(timezone.utc)
            cached_asset_id = await self._load_cached_asset_id(db, fill.market)
            if cached_asset_id is not None:
                fill = self._fill_with_asset_id(fill, cached_asset_id)
                asset_id_source = "market_risk_settings_cache"
            else:
                fill = await self._hydrate_asset_id(fill)
                asset_id_source = "meta_rest" if fill.market.asset_id is not None else "unresolved"
            asset_id_hydrate_done_at = datetime.now(timezone.utc)
            if fill.market.asset_id is None:
                raise RetryableFillProcessingError(
                    "could not resolve Hyperliquid asset id for fill market"
                )
        await self._ensure_market_execution_metadata(fill.market)

        price_cache_read_at = datetime.now(timezone.utc)
        cached_price_entry = await self._load_hot_path_price(fill.market)
        price_cache_read_done_at = datetime.now(timezone.utc)
        price_cache_fresh = cached_price_entry is not None and self.price_cache.is_fresh(fill.market.canonical_coin)
        price_cache_age_ms = self.price_cache.age_ms(fill.market.canonical_coin)
        price_entry, order_price_fresh, price_reference_fallback, price_reference_source = (
            _execution_price_entry_for_fill(
                cached_entry=cached_price_entry,
                cache_fresh=price_cache_fresh,
                fill=fill,
            )
        )
        if price_entry is None or not order_price_fresh:
            raise RetryableFillProcessingError("price cache stale or missing for fill market")

        blocking_unresolved = await self._blocking_unresolved_same_market_orders(
            db,
            leader_address=leader.leader_address,
            market=fill.market,
        )
        if blocking_unresolved:
            raise RetryableFillProcessingError(UNRESOLVED_SAME_MARKET_ORDER_BLOCKER)

        account_cache_read_at = datetime.now(timezone.utc)
        follower_value = await self._resolved_account_value(
            db,
            FOLLOWER,
            self.settings.hyperliquid_follower_account_address(),
            fill.market.dex,
        )

        leader_state: LatestAccountState | None = None
        leader_position: LatestAccountPosition | LiveLeaderPositionSnapshot | None = None
        fill_implied_position = derive_leader_post_position_from_fill(fill)
        force_live_reduce_refresh = _is_snapshot_recovery_fill(fill) and bool(
            fill_implied_position.is_reduce or fill_implied_position.is_close or fill_implied_position.is_flip
        )
        live_leader_position_refresh: dict[str, Any] | None = None
        live_leader_position_source: str | None = None
        if force_live_reduce_refresh:
            live_refresh_started_at = datetime.now(timezone.utc)
            live_ok, live_position, live_error = await self._load_live_leader_position_snapshot(
                leader=leader,
                market=fill.market,
                price_entry=price_entry,
            )
            live_refresh_done_at = datetime.now(timezone.utc)
            live_leader_position_refresh = {
                "attempted": True,
                "ok": live_ok,
                "error": live_error,
                "started_at": live_refresh_started_at.isoformat(),
                "done_at": live_refresh_done_at.isoformat(),
                "duration_ms": _delta_ms(live_refresh_started_at, live_refresh_done_at),
                "live_side": live_position.side.value if live_position is not None else "FLAT" if live_ok else None,
                "live_size": str(live_position.size) if live_position is not None else "0" if live_ok else None,
                "live_notional": str(live_position.notional) if live_position is not None else "0" if live_ok else None,
            }
            if live_ok:
                adjusted = _live_adjusted_fill_implied_position(
                    fill=fill,
                    implied=fill_implied_position,
                    live_position=live_position,
                    observed_at=live_refresh_done_at,
                    force_reduce=force_live_reduce_refresh,
                )
                if adjusted is not None:
                    fill_implied_position = adjusted
                    leader_position = live_position or _flat_live_leader_position_snapshot(
                        observed_at=live_refresh_done_at
                    )
                    live_leader_position_source = (
                        "live_account_state_flat_reduce"
                        if live_position is None
                        else "live_account_state_reduce"
                    )
                    live_leader_position_refresh["used"] = True
                    live_leader_position_refresh["adjusted_fill_implied_position"] = _fill_implied_payload(adjusted)
                else:
                    live_leader_position_refresh["used"] = False
        account_cache_read_done_at = datetime.now(timezone.utc)

        snapshot_conflict = False
        use_fill_implied_position = _should_use_fill_derived_position(None, fill, fill_implied_position)
        leader_position_source = "fill_start_position" if use_fill_implied_position else "fill_position_unknown"
        leader_position_notional = None
        leader_position_size = None
        if use_fill_implied_position:
            leader_position_notional = fill_implied_position.notional_after_estimate
            leader_position_size = fill_implied_position.size_after
            if live_leader_position_source:
                leader_position_source = live_leader_position_source
        leader_account_value = _configured_leader_account_value(leader)
        leader_account_value_source = FIXED_LEADER_ACCOUNT_VALUE_SOURCE
        leader_account_abstraction_mode = FIXED_LEADER_ACCOUNT_VALUE_MODE
        follower_account_value = _decimal_from_payload(follower_value, "account_value_used_for_sizing")
        account_value_blockers: list[str] = []
        if leader_account_value is None or leader_account_value <= 0:
            account_value_blockers.append("fixed leader account value unavailable")
        if follower_account_value is None or follower_account_value <= 0:
            account_value_blockers.append("follower resolved account value unavailable")
        account_value_blockers.extend(str(item) for item in (follower_value or {}).get("blockers") or [])

        target_notional: Decimal | None = None
        leader_ratio: Decimal | None = None
        target_side_hint = PositionSide.FLAT
        if leader_position_notional is not None:
            target_side_hint = PositionSide.LONG if leader_position_notional > 0 else PositionSide.SHORT if leader_position_notional < 0 else PositionSide.FLAT
        leader_entry_px_for_order = (
            leader_position.entry_px
            if leader_position is not None and not use_fill_implied_position
            else fill_implied_position.entry_px
            if use_fill_implied_position
            else None
        )
        allocation_read_at = datetime.now(timezone.utc)
        if target_side_hint == PositionSide.FLAT:
            existing_allocation = await self._load_allocation(db, leader, fill.market, None)
        else:
            same_side_allocation = await self._load_allocation(db, leader, fill.market, target_side_hint)
            opposite_side_allocation = await self._load_allocation(db, leader, fill.market, _opposite_side(target_side_hint))
            existing_allocation = opposite_side_allocation if _allocation_active(opposite_side_allocation) else same_side_allocation
        await self._release_resolved_pending_intents_for_market(db, fill.market)
        planning_allocation = self.pending_intents.overlay_allocation(existing_allocation)
        allocation_read_done_at = datetime.now(timezone.utc)
        stale_zero_reason = (
            None
            if self.pending_intents.has_pending_allocation(existing_allocation)
            else _stale_zero_allocation_reason(existing_allocation)
        )
        if stale_zero_reason:
            _close_zero_allocation_lifecycle(
                existing_allocation,
                reason=stale_zero_reason,
                now=datetime.now(timezone.utc),
            )
            db.add(
                RiskEvent(
                    severity="warning",
                    event_type="STALE_ZERO_ALLOCATION_CLOSED",
                    symbol=fill.market.canonical_coin,
                    leader_address=leader.leader_address,
                    message=stale_zero_reason,
                    metadata_json={
                        "allocation_id": existing_allocation.id,
                        "source_fill_id": fill.source_fill_id,
                        "dex": fill.market.dex,
                        "canonical_coin": fill.market.canonical_coin,
                    },
                )
            )
            existing_allocation = None
            planning_allocation = None
        absolute_dust_reopen = _fill_is_economic_dust_reopen(
            fill_implied_position,
            reference_price=fill.price,
            min_order_value=minimum_tradeable_notional,
        )
        checkpoint_dust_reopen = _fill_is_minimum_residual_checkpoint_reopen(
            lifecycle_allocation,
            fill_implied_position,
        )
        lifecycle_economic_dust_reopen = bool(
            not _allocation_lifecycle_active(planning_allocation)
            and (absolute_dust_reopen or checkpoint_dust_reopen)
        )
        lifecycle_economic_dust_reopen_mode = (
            "FOLLOWER_MINIMUM_RESIDUAL_CHECKPOINT"
            if lifecycle_economic_dust_reopen and checkpoint_dust_reopen
            else "LEADER_ABSOLUTE_DUST"
            if lifecycle_economic_dust_reopen and absolute_dust_reopen
            else None
        )
        lifecycle_ignore_reason = _ignore_without_allocation_reason(
            planning_allocation,
            fill_implied_position,
            allow_economic_dust_reopen=lifecycle_economic_dust_reopen,
        )
        if lifecycle_ignore_reason:
            return await self._record_lifecycle_ignored_order(
                db,
                fill=fill,
                leader=leader,
                reason=lifecycle_ignore_reason,
                target_side_hint=target_side_hint,
                leader_position_notional=leader_position_notional,
                leader_entry_px=leader_entry_px_for_order,
                leader_account_value=leader_account_value,
                follower_account_value=follower_account_value,
                dedupe_started_at=dedupe_started_at,
                dedupe_done_at=dedupe_done_at,
                debounce_started_at=debounce_started_at,
                debounce_released_at=debounce_released_at,
                lock_wait_started_at=lock_wait_started_at,
                lock_acquired_at=lock_acquired_at,
                ws_received_at=ws_received_at,
                decision_started_at=decision_started_at,
                account_cache_read_at=account_cache_read_at,
                account_cache_read_done_at=account_cache_read_done_at,
                price_cache_read_at=price_cache_read_at,
                price_cache_read_done_at=price_cache_read_done_at,
            )
        market_owner_allocation = await self._load_market_owner_allocation(db, fill.market)
        market_owner_blocker = _market_owner_blocker(
            market_owner_allocation,
            leader=leader,
            current_allocation=planning_allocation,
        )
        if market_owner_blocker:
            if await self._market_owner_handoff_pending(db, market_owner_allocation):
                raise MarketOwnershipHandoffPending(
                    "MARKET_OWNER_HANDOFF_PENDING: the prior owner is closing or its first order "
                    "is still unresolved; this fill remains durable and will claim the market "
                    "immediately after the prior lifecycle is finalized"
                )
            blockers.append(market_owner_blocker)
        if leader_position_notional is None and planning_allocation is not None:
            leader_position_notional = Decimal("0")
            leader_position_size = Decimal("0")
        if _allocation_needs_manual_review(existing_allocation):
            blockers.append("allocation mismatch needs manual review for this leader/market")
        if leader_position_notional is None and planning_allocation is None:
            blockers.append("leader position not loaded for fill market")
        if leader_position_notional is not None and leader_account_value and leader_account_value > 0:
            try:
                leader_ratio = calculate_leader_position_ratio(
                    leader_account_value=leader_account_value,
                    leader_position_notional=leader_position_notional,
                )
            except ValueError as exc:
                blockers.append(str(exc))

        leader_fill_is_reduce_or_close = _fill_is_reduce_or_close(fill_implied_position)
        leader_previous_position_size = _fill_previous_position_size(fill_implied_position)
        if _snapshot_recovery_should_use_allocation_checkpoint(
            fill=fill,
            planning_allocation=planning_allocation,
            leader_previous_position_size=leader_previous_position_size,
            leader_position_size=leader_position_size,
        ):
            leader_previous_position_size = None
        try:
            transition_plan = plan_leader_allocation_transition(
                leader_id=leader.id,
                execution_venue=ExecutionVenue.HYPERLIQUID.value,
                dex=fill.market.dex,
                canonical_coin=fill.market.canonical_coin,
                leader_side=target_side_hint,
                leader_position_notional=leader_position_notional,
                leader_position_size=leader_position_size,
                leader_account_value_used=leader_account_value,
                follower_account_value_used=follower_account_value,
                copy_multiplier=leader.copy_multiplier,
                current_allocation=planning_allocation,
                max_position_notional=leader.max_notional_per_trade,
                leader_fill_notional=_fill_notional_for_sizing(fill),
                leader_previous_position_size=leader_previous_position_size,
                leader_fill_is_reduce_or_close=leader_fill_is_reduce_or_close,
            )
        except ValueError as exc:
            transition_plan = None
            blockers.append(str(exc))
        if lifecycle_economic_dust_reopen and transition_plan is not None:
            formula_inputs = dict(transition_plan.formula_inputs or {})
            formula_inputs.update(
                {
                    "economic_dust_reopen": True,
                    "economic_dust_reopen_mode": lifecycle_economic_dust_reopen_mode,
                    "economic_dust_start_notional": str(
                        abs(
                            Decimal(fill_implied_position.start_position or 0)
                            * Decimal(fill.price)
                        )
                    ),
                    "economic_dust_post_notional": str(
                        abs(fill_implied_position.notional_after_estimate)
                    ),
                    "minimum_tradeable_notional": str(
                        minimum_tradeable_notional
                    ),
                    "economic_dust_reopen_formula": (
                        "leader startPosition was below the venue minimum and "
                        "the same-side increase crossed back into tradeable size; "
                        "treat follower-flat state as a new account-ratio lifecycle"
                    ),
                }
            )
            transition_plan = replace(
                transition_plan,
                formula_inputs=formula_inputs,
            )
        sizing_done_at = datetime.now(timezone.utc)

        coin_allowed_for_fill = _coin_config_allows_fill(
            config_allowed=coin_allowed_by_config,
            allocation=planning_allocation,
            fill_implied_position=fill_implied_position,
            transition_plan=transition_plan,
        )
        if not coin_allowed_for_fill:
            blockers.append("coin not allowed for leader")

        if transition_plan is not None and transition_plan.action == AllocationTransitionAction.BLOCK:
            blockers.append(transition_plan.reason)
        position_cap_rejected = bool(
            transition_plan is not None
            and transition_plan.action == AllocationTransitionAction.BLOCK
            and str(transition_plan.reason or "").startswith(
                MAX_POSITION_NOTIONAL_CAP_EXCEEDED
            )
        )
        if _transition_requires_account_value(transition_plan):
            if account_value_blockers:
                raise RetryableFillProcessingError(
                    "account value required for sizing is temporarily unavailable: "
                    + "; ".join(account_value_blockers)
                )
        plan_warnings = _allocation_plan_warnings(transition_plan)
        transition_plan, minimum_residual_early_close = (
            _close_instead_of_leaving_untradeable_residual(
                transition_plan=transition_plan,
                current_allocation=planning_allocation,
                min_order_value=Decimal(str(self.settings.hyperliquid_min_order_value_usd)),
            )
        )
        current_allocation = existing_allocation
        pending_open_activation_reason = _pending_open_activation_block_reason(
            planning_allocation,
            fill_implied_position,
            transition_plan,
        )
        if pending_open_activation_reason:
            blockers.append(pending_open_activation_reason)
        missed_reduce_catchup = _missed_reduce_catchup_allows_direction_mismatch(
            fill_implied_position=fill_implied_position,
            transition_plan=transition_plan,
            planning_allocation=planning_allocation,
        )
        fill_direction_guard_reason = _fill_direction_action_block_reason(
            fill_implied_position,
            transition_plan,
            allow_missed_reduce_catchup=missed_reduce_catchup,
            allow_checkpoint_flip_open=lifecycle_checkpoint_flip_open,
        )
        if fill_direction_guard_reason:
            blockers.append(fill_direction_guard_reason)
        target_notional = transition_plan.target_notional if transition_plan is not None else Decimal("0")
        delta_notional = transition_plan.delta_notional if transition_plan is not None else Decimal("0")
        reduce_only = bool(transition_plan.reduce_only) if transition_plan is not None else False
        order_action = transition_plan.action.value if transition_plan is not None else "BLOCK"
        order_position_side = (
            transition_plan.old_side
            if transition_plan is not None and transition_plan.reduce_only and transition_plan.old_side is not None
            else transition_plan.new_side
            if transition_plan is not None
            else PositionSide.FLAT
        )
        target_delta_abs = abs(delta_notional)
        mark_price = price_entry.price if price_entry else Decimal("0")
        aggregate_side = order_position_side if order_position_side != PositionSide.FLAT else target_side_hint
        follower_qty_by_side: dict[PositionSide, Decimal] | None = None
        allocation_qty_by_side: dict[PositionSide, Decimal] | None = None
        aggregate_follower_qty: Decimal | None = None
        aggregate_follower_state_at: datetime | None = None
        allocation_sum_qty: Decimal | None = None
        allocation_latest_reconcile_at: datetime | None = None
        persisted_allocation_qty_by_side: dict[PositionSide, Decimal] | None = None
        aggregate_allocation_read_started_at: datetime | None = None
        aggregate_allocation_read_done_at: datetime | None = None
        follower_position_read_started_at: datetime | None = None
        follower_position_read_done_at: datetime | None = None
        opposite_allocation_guard_started_at: datetime | None = None
        opposite_allocation_guard_done_at: datetime | None = None
        market_leverage_plan_started_at: datetime | None = None
        market_leverage_plan_done_at: datetime | None = None
        sizing_guard_started_at: datetime | None = None
        sizing_guard_done_at: datetime | None = None
        allocation_mismatch = False
        allocation_mismatch_state_lag = False
        allocation_mismatch_reduce_safe = False
        follower_below_allocation = False
        unmanaged_follower_position = False
        unmanaged_follower_position_state_lag = False
        unmanaged_follower_position_reduce_safe = False
        unmanaged_follower_position_qty_by_side: dict[str, str] = {}
        manual_follower_position_guard = False
        manual_follower_position_guard_reason: str | None = None
        manual_follower_position_guard_created_at: datetime | None = None
        follower_position_state_fresh: bool | None = None
        follower_position_stream_trusted = False
        economic_dust_reopen_follower_flat: bool | None = None
        if aggregate_side != PositionSide.FLAT:
            aggregate_allocation_read_started_at = datetime.now(timezone.utc)
            allocation_qty_by_side, allocation_latest_reconcile_at = await self._allocation_sum_qtys_with_latest_reconcile(
                db,
                fill.market,
            )
            aggregate_allocation_read_done_at = datetime.now(timezone.utc)
            persisted_allocation_qty_by_side = dict(allocation_qty_by_side)
            follower_position_read_started_at = aggregate_allocation_read_done_at
            follower_qty_by_side, aggregate_follower_state_at = await self._follower_position_qtys_with_state_at(
                db,
                fill.market,
            )
            follower_position_read_done_at = datetime.now(timezone.utc)
            follower_qty_by_side = self.pending_intents.effective_qtys(
                dex=fill.market.dex,
                canonical_coin=fill.market.canonical_coin,
                base_qtys=follower_qty_by_side,
            )
            allocation_qty_by_side = self.pending_intents.effective_qtys(
                dex=fill.market.dex,
                canonical_coin=fill.market.canonical_coin,
                base_qtys=allocation_qty_by_side,
            )
            aggregate_follower_qty = follower_qty_by_side.get(aggregate_side, Decimal("0"))
            allocation_sum_qty = allocation_qty_by_side.get(aggregate_side, Decimal("0"))
            allocation_mismatch = any(
                abs(
                    Decimal(follower_qty_by_side.get(side, 0))
                    - Decimal(allocation_qty_by_side.get(side, 0))
                )
                > ALLOCATION_TRANSITION_TOLERANCE
                for side in (PositionSide.LONG, PositionSide.SHORT)
            )
            allocation_mismatch_state_lag = _allocation_mismatch_from_stale_follower_state(
                allocation_mismatch=allocation_mismatch,
                follower_state_at=aggregate_follower_state_at,
                allocation_latest_reconcile_at=allocation_latest_reconcile_at,
            )
            unmanaged_qty_by_side = _unmanaged_follower_position_qtys(
                follower_qty_by_side=follower_qty_by_side,
                allocation_qty_by_side=allocation_qty_by_side,
            )
            unmanaged_follower_position = any(
                qty > ALLOCATION_TRANSITION_TOLERANCE for qty in unmanaged_qty_by_side.values()
            )
            unmanaged_follower_position_state_lag = _unmanaged_follower_position_from_stale_follower_state(
                unmanaged_follower_position=unmanaged_follower_position,
                follower_state_at=aggregate_follower_state_at,
                allocation_latest_reconcile_at=allocation_latest_reconcile_at,
            )
            unmanaged_follower_position_reduce_safe = _unmanaged_follower_position_reduce_safe(
                transition_plan=transition_plan,
                unmanaged_qty_by_side=unmanaged_qty_by_side,
            )
            follower_below_allocation = any(
                Decimal(follower_qty_by_side.get(side, 0))
                < Decimal(allocation_qty_by_side.get(side, 0)) - ALLOCATION_TRANSITION_TOLERANCE
                for side in (PositionSide.LONG, PositionSide.SHORT)
            )
            allocation_mismatch_reduce_safe = bool(
                allocation_mismatch
                and not follower_below_allocation
                and unmanaged_follower_position_reduce_safe
            )
            unmanaged_follower_position_qty_by_side = _position_side_qtys_payload(unmanaged_qty_by_side)
            stale_seconds = max(
                0.5,
                float(getattr(self.settings, "account_state_stale_seconds", 2) or 2),
            )
            if self._follower_position_stream_trusted is not None:
                try:
                    follower_position_stream_trusted = bool(
                        self._follower_position_stream_trusted(fill.market.dex)
                    )
                except Exception:
                    follower_position_stream_trusted = False
            follower_position_state_fresh = _follower_position_state_is_fresh(
                state_at=aggregate_follower_state_at,
                now=datetime.now(timezone.utc),
                stale_seconds=stale_seconds,
                stream_trusted=follower_position_stream_trusted,
            )
            economic_dust_reopen_flat_blocker = (
                _economic_dust_reopen_follower_flat_blocker(
                    economic_dust_reopen=lifecycle_economic_dust_reopen,
                    follower_qty_by_side=follower_qty_by_side,
                )
            )
            if lifecycle_economic_dust_reopen:
                economic_dust_reopen_follower_flat = (
                    economic_dust_reopen_flat_blocker is None
                )
            if economic_dust_reopen_flat_blocker:
                blockers.append(economic_dust_reopen_flat_blocker)

            actionable_transition = bool(
                transition_plan is not None
                and transition_plan.action not in {AllocationTransitionAction.NOOP, AllocationTransitionAction.BLOCK}
            )
            if actionable_transition and not follower_position_state_fresh:
                if _allocation_lifecycle_active(planning_allocation):
                    raise RetryableFillProcessingError(
                        "follower position state is stale while an active allocation is being changed"
                    )
                raise RetryableFillProcessingError(
                    "follower position state is stale before a released market can be opened"
                )
            if actionable_transition and _allocation_lifecycle_active(planning_allocation):
                if allocation_mismatch and not allocation_mismatch_state_lag and follower_below_allocation:
                    raise RetryableFillProcessingError(
                        "follower actual position is below the allocation ledger; waiting for allocation reconciliation"
                    )

            unmanaged_blocker = _unmanaged_follower_position_blocker(
                transition_plan=transition_plan,
                unmanaged_qty_by_side=unmanaged_qty_by_side,
                unmanaged_position_state_lag=unmanaged_follower_position_state_lag,
                canonical_coin=fill.market.canonical_coin,
            )
            if unmanaged_blocker:
                blockers.append(unmanaged_blocker)
            if (
                self.manual_position_guard is not None
                and transition_plan is not None
                and transition_plan.action not in {AllocationTransitionAction.NOOP, AllocationTransitionAction.BLOCK}
            ):
                guard_entry = self.manual_position_guard.active_entry(fill.market)
                if guard_entry is not None:
                    manual_follower_position_guard = True
                    manual_follower_position_guard_reason = guard_entry.reason
                    manual_follower_position_guard_created_at = guard_entry.created_at
                    blockers.append(guard_entry.blocker_message())
        scope_aggregate_follower_qty = _effective_aggregate_follower_qty_for_reduce_scope(
            reduce_only=reduce_only,
            allocation_mismatch_state_lag=allocation_mismatch_state_lag,
            aggregate_follower_qty=aggregate_follower_qty,
            allocation_sum_qty=allocation_sum_qty,
        )
        if transition_plan is not None and transition_plan.action in {
            AllocationTransitionAction.OPEN,
            AllocationTransitionAction.INCREASE,
            AllocationTransitionAction.FLIP_OPEN_SECOND,
        }:
            opposite_allocation_guard_started_at = datetime.now(timezone.utc)
            if persisted_allocation_qty_by_side is not None:
                opposite_allocation_exists = _opposite_allocation_exists_in_snapshot(
                    allocation_qty_by_side=persisted_allocation_qty_by_side,
                    current_allocation=planning_allocation,
                    current_leader_id=leader.id,
                    new_side=transition_plan.new_side,
                )
            else:
                opposite_allocation_exists = await self._opposite_aggregate_allocation_exists(
                    db,
                    leader,
                    fill.market,
                    transition_plan.new_side,
                )
            opposite_allocation_guard_done_at = datetime.now(timezone.utc)
            if opposite_allocation_exists:
                blockers.append("Opposite aggregate allocation exists on same Hyperliquid account")
        if transition_plan is not None and transition_plan.action in {
            AllocationTransitionAction.REDUCE,
            AllocationTransitionAction.CLOSE,
            AllocationTransitionAction.FLIP_CLOSE_FIRST,
        }:
            try:
                assert_allocation_scope(
                    {
                        "action": transition_plan.action.value,
                        "leader_id": leader.id,
                        "execution_venue": ExecutionVenue.HYPERLIQUID.value,
                        "dex": fill.market.dex,
                        "canonical_coin": fill.market.canonical_coin,
                        "old_side": transition_plan.old_side.value if transition_plan.old_side else None,
                        "close_qty_limit": transition_plan.close_qty_limit,
                    },
                    planning_allocation,
                    aggregate_follower_qty=None,
                    allocation_sum_qty=allocation_sum_qty,
                )
            except AllocationScopeError as exc:
                blockers.append(str(exc))
        market_leverage_plan_started_at = datetime.now(timezone.utc)
        leverage_plan = await self._load_market_leverage_plan(
            db,
            fill.market,
            leader_position,
            reduce_only=reduce_only,
        )
        market_leverage_plan_done_at = datetime.now(timezone.utc)
        market_only_isolated = market_requires_isolated_margin(leverage_plan.market_meta)
        required_margin_mode = FALLBACK_MARGIN_MODE if market_only_isolated else DESIRED_MARGIN_MODE
        effective_leverage = _resolved_market_effective_leverage(
            leverage_plan,
            configured_default_leverage=self.settings.hyperliquid_default_leverage,
            required_margin_mode=required_margin_mode,
            canonical_coin_value=fill.market.canonical_coin,
        )
        if target_delta_abs > 0 and not reduce_only and not leverage_plan.ok_for_open:
            blockers.append(leverage_plan.reason or "market max leverage not confirmed")
        leader_margin_mode_observed = _observed_margin_mode(leader_position)
        margin_ok = True
        required_margin: Decimal | None = None
        if target_delta_abs > 0 and not reduce_only and follower_value:
            margin_ok, required_margin = available_collateral_sufficient(
                _account_value_result_like(follower_value),
                target_delta_notional=target_delta_abs,
                effective_leverage=effective_leverage,
            )
            if not margin_ok:
                blockers.append("insufficient available collateral for target delta")

        # The authoritative emergency-switch check is serialized with /off at
        # the final exchange-submit boundary.  Reading it here as well added a
        # database round trip to every open/increase without making the race any
        # safer: a switch can change immediately after this planning read.  Let
        # the final guard alone decide whether an order may reach the exchange.
        kill_switch_active = False

        sizing_guard_error: str | None = None
        sizing_guard_started_at = datetime.now(timezone.utc)
        if (
            target_delta_abs > Decimal("0.00000001")
            and not reduce_only
            and transition_plan is not None
            and transition_plan.action not in {AllocationTransitionAction.NOOP, AllocationTransitionAction.BLOCK}
        ):
            formula_inputs = transition_plan.formula_inputs or {}
            try:
                assert_sizing_mode_account_ratio(
                    {
                        "sizing_mode": SIZING_MODE_ACCOUNT_RATIO,
                        "leader_account_value": leader_account_value,
                        "leader_account_value_source": leader_account_value_source,
                        "leader_position_notional": leader_position_notional,
                        "follower_account_value": follower_account_value,
                        "follower_account_value_source": (follower_value or {}).get("account_value_source") or (follower_value or {}).get("source"),
                        "leader_position_ratio": leader_ratio,
                        "copy_multiplier": leader.copy_multiplier,
                        "target_notional": target_notional,
                        "delta_notional": delta_notional,
                        "current_allocation_notional": transition_plan.current_allocation_notional,
                        "leader_fill_notional": formula_inputs.get("leader_fill_notional"),
                        "leader_position_size": formula_inputs.get("leader_position_size"),
                        "previous_leader_position_size": formula_inputs.get("previous_leader_position_size"),
                        "leader_fill_previous_position_size": formula_inputs.get("leader_fill_previous_position_size"),
                        "previous_leader_position_notional": formula_inputs.get("previous_leader_position_notional"),
                        "leader_position_increase_ratio": formula_inputs.get("leader_position_increase_ratio"),
                        "increase_delta_source": formula_inputs.get("increase_delta_source"),
                        "fill_delta_target_notional": formula_inputs.get("fill_delta_target_notional"),
                        "pending_reduce_offset_notional": formula_inputs.get("pending_reduce_offset_notional"),
                        "max_position_notional_cap": formula_inputs.get("max_position_notional_cap"),
                        "target_notional_before_cap": formula_inputs.get("target_notional_before_cap"),
                    }
                )
            except SizingGuardError as exc:
                sizing_guard_error = str(exc)
                blockers.append(f"ACCOUNT_RATIO guard failed: {exc}")
        sizing_guard_done_at = datetime.now(timezone.utc)

        order_side_value = _order_side(target_side=order_position_side, reduce_only=reduce_only)
        quantity = _order_quantity_for_transition(
            mark_price=mark_price,
            target_delta_abs=target_delta_abs,
            reduce_only=reduce_only,
            transition_plan=transition_plan,
            aggregate_follower_qty=scope_aggregate_follower_qty,
        )
        if reduce_only and transition_plan is not None:
            try:
                assert_allocation_scope(
                    {
                        "action": transition_plan.action.value,
                        "leader_id": leader.id,
                        "execution_venue": ExecutionVenue.HYPERLIQUID.value,
                        "dex": fill.market.dex,
                        "canonical_coin": fill.market.canonical_coin,
                        "old_side": transition_plan.old_side.value if transition_plan.old_side else None,
                        "quantity": quantity,
                    },
                    planning_allocation,
                    aggregate_follower_qty=scope_aggregate_follower_qty,
                    allocation_sum_qty=allocation_sum_qty,
                )
            except AllocationScopeError as exc:
                blockers.append(str(exc))
        if (
            quantity <= Decimal("0")
            and transition_plan is not None
            and transition_plan.action not in {AllocationTransitionAction.NOOP, AllocationTransitionAction.BLOCK}
        ):
            blockers.append("order quantity is zero")
        cloid = build_hyperliquid_cloid(
            leader_address=leader.leader_address,
            coin=fill.market.coin,
            dex=fill.market.dex,
            side=order_position_side.value,
            action=order_action,
            source_fill_id=fill.source_fill_id,
            timestamp_ms=int(decision_started_at.timestamp() * 1000),
        )
        allow_target_notional_price_drift = _allow_target_notional_price_drift_for_transition(transition_plan)
        validator_started_at = datetime.now(timezone.utc)
        validator_result = validate_hyperliquid_order_params(
            dex=fill.market.dex,
            canonical_coin=fill.market.canonical_coin,
            asset_id=fill.market.asset_id,
            action=order_action,
            side=order_side_value,
            target_delta_notional=target_delta_abs,
            raw_size=quantity,
            raw_price=mark_price,
            market_meta=leverage_plan.market_meta,
            order_policy={
                "cloid": cloid,
                "is_buy": order_side_value == "BUY",
                "reduce_only": reduce_only,
                "tif": "Ioc",
                "order_type": {"limit": {"tif": "Ioc"}},
                "aggressive_market": True,
                "price_fresh": order_price_fresh,
                "min_order_value": self.settings.hyperliquid_min_order_value_usd,
                "effective_leverage": effective_leverage,
                "allow_target_notional_price_drift": allow_target_notional_price_drift,
            },
        )
        final_close_min_order_override = False
        reduce_quantity_guard_blockers = _reduce_quantity_guard_blockers(
            reduce_only=reduce_only,
            transition_plan=transition_plan,
            current_allocation=planning_allocation,
            rounded_size=validator_result.rounded_size,
            aggregate_follower_qty=scope_aggregate_follower_qty,
        )
        blockers.extend(reduce_quantity_guard_blockers)
        validator_done_at = datetime.now(timezone.utc)
        if not validator_result.ok:
            validator_message = _validator_blocker_message(validator_result)
            if validator_message not in blockers:
                blockers.append(validator_message)
        deferred_reduce = _is_deferred_reduce_block(
            reduce_only=reduce_only,
            transition_plan=transition_plan,
            validator_result=validator_result,
        )
        pending_open = _is_pending_open_block(
            reduce_only=reduce_only,
            transition_plan=transition_plan,
            validator_result=validator_result,
            blockers=blockers,
        )
        account_value_pending_open = _is_account_value_pending_open(
            allocation=current_allocation,
            fill_implied_position=fill_implied_position,
            transition_plan=transition_plan,
            blockers=blockers,
        )
        pending_open = pending_open or account_value_pending_open
        pending_open_reason = (
            ACCOUNT_VALUE_PENDING_OPEN_REASON
            if account_value_pending_open
            else PENDING_OPEN_REASON
            if pending_open
            else None
        )
        direction_guard_preserve_allocation = _direction_guard_preserves_allocation(
            fill_direction_guard_reason,
            current_allocation,
        )
        state_target_notional = _allocation_state_target_notional(
            target_notional=target_notional,
            transition_plan=transition_plan,
            pending_open_reason=pending_open_reason,
            reduce_only=reduce_only,
        )
        if direction_guard_preserve_allocation and current_allocation is not None:
            state_target_notional = Decimal(current_allocation.allocated_notional or 0)
        checklist_done_at = datetime.now(timezone.utc)
        decision_done_at = datetime.now(timezone.utc)
        noop = transition_plan is not None and transition_plan.action == AllocationTransitionAction.NOOP
        dry_run = noop or not (
            self.settings.trading_enabled
            and self.settings.hyperliquid_trading_enabled
            and not blockers
        )
        status = "NOOP" if noop and not blockers else "BLOCKED" if blockers else ("DRY_RUN" if dry_run else "PENDING_SUBMIT")
        blocked_order_preserves_allocation = _blocked_order_preserves_allocation_state(
            allocation=current_allocation,
            blockers=blockers,
            pending_open_activation_reason=pending_open_activation_reason,
            pending_open=pending_open,
            deferred_reduce=deferred_reduce,
            direction_guard_preserve_allocation=direction_guard_preserve_allocation,
        )
        if _should_fast_forward_below_min_pending_open_lifecycle(
            allocation=current_allocation,
            transition_plan=transition_plan,
            reduce_only=reduce_only,
            pending_open=pending_open,
            pending_open_activation_reason=pending_open_activation_reason,
            deferred_reduce=deferred_reduce,
        ):
            _fast_forward_below_min_pending_open_lifecycle(
                current_allocation,
                leader_account_value=leader_account_value,
                leader_position_notional=leader_position_notional,
                leader_position_size=leader_position_size,
                copy_multiplier=leader.copy_multiplier,
                source_fill_id=fill.source_fill_id,
                now=datetime.now(timezone.utc),
            )
            await self._record_source_fill_outcomes(
                db,
                fill,
                order=None,
                disposition=FILL_OUTCOME_MIN_NOTIONAL_EXEMPT,
                reason="follower order is below the Hyperliquid minimum order value",
            )
            await db.commit()
            return None
        if _should_fast_forward_below_min_active_increase(
            allocation=current_allocation,
            transition_plan=transition_plan,
            reduce_only=reduce_only,
            pending_open=pending_open,
            pending_open_activation_reason=pending_open_activation_reason,
            deferred_reduce=deferred_reduce,
        ):
            _fast_forward_below_min_active_increase(
                current_allocation,
                leader_account_value=leader_account_value,
                leader_position_notional=leader_position_notional,
                leader_position_size=leader_position_size,
                copy_multiplier=leader.copy_multiplier,
                source_fill_id=fill.source_fill_id,
                now=datetime.now(timezone.utc),
            )
            await self._record_source_fill_outcomes(
                db,
                fill,
                order=None,
                disposition=FILL_OUTCOME_MIN_NOTIONAL_EXEMPT,
                reason="follower order is below the Hyperliquid minimum order value",
            )
            await db.commit()
            return None
        if (
            status == "BLOCKED"
            and target_delta_abs > Decimal("0.00000001")
            and not reduce_only
            and current_allocation is None
            and leader_position is not None
            and validator_result.block_reason != "BLOCKED_TOO_SMALL"
        ):
            await self._mark_current_position_wait_until_flat(
                db,
                leader=leader,
                leader_state=leader_state,
                leader_position=leader_position,
                market=fill.market,
                reason=f"Auto-copy open blocked before execution: {'; '.join(blockers)}",
            )
        order_submit_started_at = None
        pending_submit_write_started_at = datetime.now(timezone.utc) if status == "PENDING_SUBMIT" else None
        order = ExecutionOrder(
            leader_id=leader.id,
            allocation_id=current_allocation.id if current_allocation else None,
            leader_address=leader.leader_address,
            source_fill_id=fill.source_fill_id,
            source_type="AUTO_COPY",
            source_coin=fill.market.coin,
            execution_venue=ExecutionVenue.HYPERLIQUID.value,
            venue_account=self.execution_scope,
            dex=fill.market.dex,
            canonical_coin=fill.market.canonical_coin,
            raw_coin_from_fill=fill.market.raw_coin,
            asset_id=fill.market.asset_id,
            venue_symbol=fill.market.venue_symbol,
            hyperliquid_coin=fill.market.coin,
            binance_symbol=None,
            side=order_side_value,
            position_side=order_position_side.value if order_position_side != PositionSide.FLAT else None,
            order_action=order_action,
            order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
            cloid=cloid,
            quantity=validator_result.rounded_size,
            price=mark_price if mark_price > 0 else None,
            estimated_price=mark_price if mark_price > 0 else None,
            leader_entry_px=leader_entry_px_for_order,
            follower_avg_entry_px=current_allocation.avg_entry_price if current_allocation else None,
            notional=target_delta_abs,
            status=status,
            dry_run=dry_run,
            reduce_only=reduce_only,
            is_close_intent=reduce_only,
            error_message="; ".join(blockers) if blockers else None,
            sizing_mode=SIZING_MODE_ACCOUNT_RATIO,
            leader_account_value=leader_account_value,
            leader_account_value_source=leader_account_value_source,
            leader_account_abstraction_mode=leader_account_abstraction_mode,
            leader_position_notional=leader_position_notional,
            follower_account_value=follower_account_value,
            follower_account_value_source=(follower_value or {}).get("account_value_source") or (follower_value or {}).get("source"),
            follower_account_abstraction_mode=(follower_value or {}).get("account_abstraction_mode") or (follower_value or {}).get("mode"),
            leader_position_ratio=leader_ratio,
            copy_multiplier=leader.copy_multiplier,
            target_notional=target_notional,
            delta_notional=delta_notional,
            hyperliquid_event_time=event_time,
            event_received_at=ws_received_at,
            ws_received_at=ws_received_at,
            dedupe_done_at=dedupe_done_at,
            debounce_released_at=debounce_released_at,
            decision_started_at=decision_started_at,
            decision_done_at=decision_done_at,
            order_submit_started_at=order_submit_started_at,
            binance_order_submit_at=order_submit_started_at,
            latency_trace_id=cloid,
            latency_trace=_latency_trace_payload(
                fill=fill,
                dedupe_started_at=dedupe_started_at,
                dedupe_done_at=dedupe_done_at,
                debounce_started_at=debounce_started_at,
                debounce_released_at=debounce_released_at,
                lock_wait_started_at=lock_wait_started_at,
                lock_acquired_at=lock_acquired_at,
                decision_started_at=decision_started_at,
                account_cache_read_at=account_cache_read_at,
                account_cache_read_done_at=account_cache_read_done_at,
                price_cache_read_at=price_cache_read_at,
                price_cache_read_done_at=price_cache_read_done_at,
                allocation_read_at=allocation_read_at,
                allocation_read_done_at=allocation_read_done_at,
                sizing_done_at=sizing_done_at,
                validator_started_at=validator_started_at,
                validator_done_at=validator_done_at,
                checklist_done_at=checklist_done_at,
                decision_done_at=decision_done_at,
                order_plan_created_at=decision_done_at,
                order_submit_started_at=order_submit_started_at,
            ),
            missing_latency_fields=[],
            pre_trade_checklist={
                "trading_enabled": self.settings.trading_enabled,
                "hyperliquid_trading_enabled": self.settings.hyperliquid_trading_enabled,
                "kill_switch_off": not kill_switch_active,
                "kill_switch_check_deferred_to_serialized_submit_boundary": not reduce_only,
                "leader_enabled": leader.enabled and leader.deleted_at is None,
                "coin_allowed": coin_allowed_for_fill,
                "coin_allowed_by_config": coin_allowed_by_config,
                "blocked_coin_existing_lifecycle_continuation": bool(
                    not coin_allowed_by_config and coin_allowed_for_fill
                ),
                "price_fresh": order_price_fresh,
                "price_cache_fresh": price_cache_fresh,
                "price_cache_age_ms": price_cache_age_ms,
                "price_reference_source": price_reference_source,
                "price_reference_fallback": price_reference_fallback,
                "leader_account_state_fresh": leader_state is not None,
                "leader_position_source": leader_position_source,
                "fill_implied_position_enabled": True,
                "fill_implied_position": _fill_implied_payload(fill_implied_position),
                "live_leader_position_refresh": live_leader_position_refresh,
                "stale_snapshot_cannot_override_fill": True,
                "snapshot_conflict": snapshot_conflict,
                "follower_account_state_fresh": follower_position_state_fresh,
                "follower_position_stream_trusted": follower_position_stream_trusted,
                "follower_account_state_hot_path": True,
                "effective_leverage": effective_leverage,
                "effective_leverage_source": "CONFIRMED_MARKET_TARGET",
                "market_max_leverage": leverage_plan.max_leverage,
                "market_sz_decimals": leverage_plan.sz_decimals,
                "market_asset_id_from_meta": leverage_plan.asset_id,
                "market_only_isolated": market_only_isolated,
                "effective_leverage_confirmed": leverage_plan.ok_for_open,
                "leader_margin_mode_observed": leader_margin_mode_observed,
                "follower_margin_mode_required": required_margin_mode,
                "follower_margin_mode_confirmed": False,
                "follower_isolated_required": required_margin_mode == FALLBACK_MARGIN_MODE,
                "follower_isolated_confirmed": False,
                "follower_leverage_confirmed": False,
                "margin_sufficient": margin_ok,
                "required_initial_margin": str(required_margin) if required_margin is not None else None,
                "order_submit_transport": self.settings.order_submit_transport,
                "order_policy": "FAST_MARKET_ONLY",
                "transition_action": transition_plan.action.value if transition_plan else "BLOCK",
                "transition_reason": transition_plan.reason if transition_plan else None,
                "allocation_scope_guard": bool(
                    transition_plan
                    and transition_plan.action
                    in {
                        AllocationTransitionAction.REDUCE,
                        AllocationTransitionAction.CLOSE,
                        AllocationTransitionAction.FLIP_CLOSE_FIRST,
                    }
                ),
                "allocation_mismatch": allocation_mismatch,
                "allocation_mismatch_state_lag": allocation_mismatch_state_lag,
                "allocation_mismatch_reduce_safe": allocation_mismatch_reduce_safe,
                "follower_below_allocation": follower_below_allocation,
                "aggregate_follower_qty": str(aggregate_follower_qty) if aggregate_follower_qty is not None else None,
                "aggregate_follower_state_at": _iso_or_none(aggregate_follower_state_at),
                "scope_aggregate_follower_qty": (
                    str(scope_aggregate_follower_qty) if scope_aggregate_follower_qty is not None else None
                ),
                "allocation_sum_qty": str(allocation_sum_qty) if allocation_sum_qty is not None else None,
                "allocation_latest_reconcile_at": _iso_or_none(allocation_latest_reconcile_at),
                "follower_qty_by_side": _position_side_qtys_payload(follower_qty_by_side),
                "allocation_qty_by_side": _position_side_qtys_payload(allocation_qty_by_side),
                "unmanaged_follower_position": unmanaged_follower_position,
                "unmanaged_follower_position_state_lag": unmanaged_follower_position_state_lag,
                "unmanaged_follower_position_qty_by_side": unmanaged_follower_position_qty_by_side,
                "unmanaged_follower_position_reduce_safe": unmanaged_follower_position_reduce_safe,
                "manual_follower_position_guard": manual_follower_position_guard,
                "manual_follower_position_guard_reason": manual_follower_position_guard_reason,
                "manual_follower_position_guard_created_at": _iso_or_none(manual_follower_position_guard_created_at),
                "follower_market_position_version": follower_market_position_version,
                "follower_market_position_version_checked_at": decision_started_at.isoformat(),
                "pending_open": pending_open,
                "pending_open_reason": pending_open_reason,
                "pending_open_activation_blocked": bool(pending_open_activation_reason),
                "pending_open_activation_reason": pending_open_activation_reason,
                "missed_reduce_catchup": missed_reduce_catchup,
                "fill_direction_guard_reason": fill_direction_guard_reason,
                "blocked_order_preserves_allocation_state": blocked_order_preserves_allocation,
                "position_cap_rejected": position_cap_rejected,
                "final_close_min_order_override": final_close_min_order_override,
                "minimum_residual_early_close": minimum_residual_early_close,
                "sizing_guard_error": sizing_guard_error,
                "reduce_quantity_guard": {
                    "ok": not reduce_quantity_guard_blockers,
                    "blockers": reduce_quantity_guard_blockers,
                    "planned_close_qty_limit": str(getattr(transition_plan, "close_qty_limit", ""))
                    if transition_plan is not None
                    else None,
                    "rounded_order_size": str(validator_result.rounded_size),
                    "allocation_qty": str(current_allocation.allocated_qty)
                    if current_allocation is not None
                    else None,
                    "aggregate_follower_qty": str(scope_aggregate_follower_qty)
                    if scope_aggregate_follower_qty is not None
                    else None,
                },
                "max_position_notional_cap": (transition_plan.formula_inputs or {}).get("max_position_notional_cap")
                if transition_plan
                else None,
                "allocation_ratio_warnings": plan_warnings,
                "market_owner_guard": {
                    "blocked": bool(market_owner_blocker),
                    "reason": market_owner_blocker,
                    "owner_allocation_id": market_owner_allocation.id if market_owner_allocation is not None else None,
                    "owner_leader_id": market_owner_allocation.leader_id if market_owner_allocation is not None else None,
                    "owner_leader_address": mask_address(market_owner_allocation.leader_address)
                    if market_owner_allocation is not None
                    else None,
                },
                "market_ownership_acquisition_required": bool(
                    current_allocation is None
                    and transition_plan is not None
                    and transition_plan.action == AllocationTransitionAction.OPEN
                ),
                "market_ownership_new_open_from_flat": _fill_is_new_open_from_flat(
                    fill_implied_position
                ),
                "market_ownership_checkpoint_flip_open": lifecycle_checkpoint_flip_open,
                "market_ownership_economic_dust_reopen": lifecycle_economic_dust_reopen,
                "economic_dust_reopen_mode": lifecycle_economic_dust_reopen_mode,
                "economic_dust_reopen_follower_flat": economic_dust_reopen_follower_flat,
                "error_code": _validator_error_code(validator_result),
                "order_validator": validator_result.to_dict(),
                "blockers": blockers,
            },
        )
        for trace_key, trace_value in {
            "aggregate_allocation_read_started_at": aggregate_allocation_read_started_at,
            "aggregate_allocation_read_done_at": aggregate_allocation_read_done_at,
            "follower_position_read_started_at": follower_position_read_started_at,
            "follower_position_read_done_at": follower_position_read_done_at,
            "opposite_allocation_guard_started_at": opposite_allocation_guard_started_at,
            "opposite_allocation_guard_done_at": opposite_allocation_guard_done_at,
            "market_leverage_plan_started_at": market_leverage_plan_started_at,
            "market_leverage_plan_done_at": market_leverage_plan_done_at,
            "sizing_guard_started_at": sizing_guard_started_at,
            "sizing_guard_done_at": sizing_guard_done_at,
        }.items():
            if trace_value is not None:
                _trace_set(order, trace_key, trace_value)
        if pending_submit_write_started_at is not None:
            _trace_set(order, "pending_submit_write_started_at", pending_submit_write_started_at)
        if asset_id_hydrate_started_at is not None:
            _trace_set(order, "asset_id_hydrate_started_at", asset_id_hydrate_started_at)
            _trace_set(order, "asset_id_hydrate_done_at", asset_id_hydrate_done_at)
            _trace_set_detail(order, "asset_id_source", asset_id_source)
        if transition_plan is not None:
            _trace_set_detail(order, "formula_inputs", transition_plan.formula_inputs)
        for warning in plan_warnings:
            db.add(
                RiskEvent(
                    severity="warning",
                    event_type="ALLOCATION_RATIO_FALLBACK_WARNING",
                    symbol=fill.market.canonical_coin,
                    leader_address=leader.leader_address,
                    message=_allocation_plan_warning_message(warning),
                    metadata_json=_json_safe({
                        "source_fill_id": fill.source_fill_id,
                        "dex": fill.market.dex,
                        "canonical_coin": fill.market.canonical_coin,
                        "warning": warning,
                        "formula_inputs": (transition_plan.formula_inputs if transition_plan else {}),
                    }),
                )
            )
        if minimum_residual_early_close and transition_plan is not None:
            db.add(
                RiskEvent(
                    severity="warning",
                    event_type="MINIMUM_RESIDUAL_EARLY_CLOSE",
                    symbol=fill.market.canonical_coin,
                    leader_address=leader.leader_address,
                    message=(
                        "proportional follower remainder was below the venue minimum; "
                        "the full allocation close was selected to prevent terminal dust"
                    ),
                    metadata_json=_json_safe({
                        "source_fill_id": fill.source_fill_id,
                        "dex": fill.market.dex,
                        "canonical_coin": fill.market.canonical_coin,
                        "allocation_id": current_allocation.id if current_allocation else None,
                        "formula_target_notional_before_close": (
                            transition_plan.formula_inputs or {}
                        ).get("formula_target_notional_before_minimum_residual_close"),
                        "minimum_tradeable_notional": (
                            transition_plan.formula_inputs or {}
                        ).get("minimum_tradeable_notional"),
                        "close_qty_limit": str(transition_plan.close_qty_limit),
                    }),
                )
            )
        if lifecycle_economic_dust_reopen and transition_plan is not None:
            db.add(
                RiskEvent(
                    severity="info",
                    event_type="ECONOMIC_DUST_REOPEN",
                    symbol=fill.market.canonical_coin,
                    leader_address=leader.leader_address,
                    message=(
                        "leader increased a sub-minimum same-side remainder back "
                        "into tradeable size; follower-flat state was treated as "
                        "a new account-ratio lifecycle"
                    ),
                    metadata_json=_json_safe(
                        {
                            "source_fill_id": fill.source_fill_id,
                            "dex": fill.market.dex,
                            "canonical_coin": fill.market.canonical_coin,
                            "start_position": str(
                                fill_implied_position.start_position
                            ),
                            "post_position": str(
                                fill_implied_position.signed_size_after
                            ),
                            "reference_price": str(fill.price),
                            "start_notional": (
                                transition_plan.formula_inputs or {}
                            ).get("economic_dust_start_notional"),
                            "post_notional": (
                                transition_plan.formula_inputs or {}
                            ).get("economic_dust_post_notional"),
                            "minimum_tradeable_notional": str(
                                minimum_tradeable_notional
                            ),
                            "reopen_mode": lifecycle_economic_dust_reopen_mode,
                            "prior_allocation_id": (
                                lifecycle_allocation.id
                                if lifecycle_allocation is not None
                                else None
                            ),
                            "transition_action": transition_plan.action.value,
                            "order_status": status,
                            "blockers": blockers,
                        }
                    ),
                )
            )
        _set_latency_fields(order)
        db.add(order)
        await db.flush()
        await self._record_source_fill_outcomes(
            db,
            fill,
            order=order,
            disposition=(
                FILL_OUTCOME_ORDER_PLANNED
                if status == "PENDING_SUBMIT"
                else FILL_OUTCOME_MIN_NOTIONAL_EXEMPT
                if _validator_error_code(validator_result) == "BELOW_MIN_ORDER_VALUE"
                else FILL_OUTCOME_NO_ACTION_REQUIRED
                if status in {"NOOP", "IGNORED"} or _expected_no_action_block(order.error_message)
                else FILL_OUTCOME_MANUAL_REVIEW
            ),
            reason=order.error_message,
        )
        if pending_submit_write_started_at is not None:
            _trace_set(order, "pending_submit_write_done_at", datetime.now(timezone.utc))
            _set_latency_fields(order)

        if (
            current_allocation is None
            and target_notional is not None
            and transition_plan is not None
            and (
                transition_plan.action
                in {
                    AllocationTransitionAction.OPEN,
                    AllocationTransitionAction.INCREASE,
                    AllocationTransitionAction.FLIP_OPEN_SECOND,
                }
                or account_value_pending_open
            )
            and (not blockers or pending_open)
        ):
            allocation_status = PENDING_OPEN_STATUS if pending_open else "OPEN"
            pending_reason = pending_open_reason if pending_open else None
            pending_since = datetime.now(timezone.utc) if pending_open else None
            current_allocation = LeaderPositionAllocationRecord(
                leader_id=leader.id,
                leader_address=leader.leader_address,
                hyperliquid_coin=fill.market.coin,
                dex=fill.market.dex,
                canonical_coin=fill.market.canonical_coin,
                binance_symbol=None,
                execution_venue=ExecutionVenue.HYPERLIQUID.value,
                venue_account=self.execution_scope,
                venue_symbol=fill.market.venue_symbol,
                position_side=order_position_side.value,
                target_notional=state_target_notional,
                allocated_notional=Decimal("0"),
                allocated_qty=Decimal("0"),
                avg_entry_price=mark_price if mark_price > 0 else None,
                last_leader_account_value=leader_account_value,
                last_leader_position_notional=leader_position_notional,
                last_leader_position_size=leader_position_size,
                copy_multiplier=leader.copy_multiplier,
                pending_reduce_qty=None,
                pending_reduce_notional=None,
                pending_reduce_reason=pending_reason,
                pending_reduce_since=pending_since,
                pending_reduce_source_fill_id=fill.source_fill_id if pending_open else None,
                status=allocation_status,
                last_source_fill_id=fill.source_fill_id,
                last_reconcile_at=datetime.now(timezone.utc),
            )
            db.add(current_allocation)
            await db.flush()
            order.allocation_id = current_allocation.id
        elif current_allocation is not None and target_notional is not None:
            if _allocation_needs_manual_review(current_allocation):
                current_allocation.status = "NEEDS_MANUAL_REVIEW"
                current_allocation.pending_reduce_reason = (
                    "; ".join(blockers) or "allocation needs manual review"
                )[:1000]
            elif direction_guard_preserve_allocation:
                _preserve_allocation_state_after_direction_guard(
                    current_allocation,
                    leader_account_value=leader_account_value,
                    leader_position_notional=leader_position_notional,
                    leader_position_size=leader_position_size,
                    copy_multiplier=leader.copy_multiplier,
                    source_fill_id=fill.source_fill_id,
                    now=datetime.now(timezone.utc),
                )
            elif blocked_order_preserves_allocation:
                if position_cap_rejected:
                    _preserve_allocation_state_after_position_cap_rejection(
                        current_allocation,
                        leader_account_value=leader_account_value,
                        leader_position_notional=leader_position_notional,
                        leader_position_size=leader_position_size,
                        copy_multiplier=leader.copy_multiplier,
                        source_fill_id=fill.source_fill_id,
                        now=datetime.now(timezone.utc),
                    )
                else:
                    _preserve_allocation_state_after_blocked_order(
                        current_allocation,
                        now=datetime.now(timezone.utc),
                    )
            else:
                current_allocation.target_notional = state_target_notional
                current_allocation.last_leader_account_value = leader_account_value
                current_allocation.last_leader_position_notional = leader_position_notional
                current_allocation.last_leader_position_size = leader_position_size
                current_allocation.copy_multiplier = leader.copy_multiplier
                current_allocation.last_source_fill_id = fill.source_fill_id
                current_allocation.last_reconcile_at = datetime.now(timezone.utc)
                if blockers:
                    if pending_open_activation_reason:
                        current_allocation.status = PENDING_OPEN_STATUS
                        current_allocation.pending_reduce_reason = pending_open_activation_reason
                        current_allocation.pending_reduce_since = (
                            current_allocation.pending_reduce_since or current_allocation.last_reconcile_at
                        )
                        current_allocation.pending_reduce_source_fill_id = fill.source_fill_id
                    elif pending_open:
                        if _pending_open_should_remain_pending(current_allocation):
                            current_allocation.status = PENDING_OPEN_STATUS
                            current_allocation.pending_reduce_reason = pending_open_reason or PENDING_OPEN_REASON
                            current_allocation.pending_reduce_since = (
                                current_allocation.pending_reduce_since or current_allocation.last_reconcile_at
                            )
                            current_allocation.pending_reduce_source_fill_id = fill.source_fill_id
                        else:
                            current_allocation.status = "OPEN"
                            _clear_deferred_reduce(current_allocation)
                    elif deferred_reduce:
                        _mark_deferred_reduce(
                            current_allocation,
                            order=order,
                            transition_plan=transition_plan,
                            quantity=quantity,
                            reason="; ".join(blockers),
                            now=current_allocation.last_reconcile_at,
                        )
                    else:
                        current_allocation.status = "BLOCKED"
                elif _pending_open_allocation_flat(current_allocation, transition_plan):
                    current_allocation.status = "CLOSED"
                    current_allocation.target_notional = Decimal("0")
                    current_allocation.pending_reduce_qty = None
                    current_allocation.pending_reduce_notional = None
                    current_allocation.pending_reduce_reason = None
                    current_allocation.pending_reduce_since = None
                    current_allocation.pending_reduce_source_fill_id = None
                elif transition_plan is not None and transition_plan.action not in {
                    AllocationTransitionAction.REDUCE,
                    AllocationTransitionAction.CLOSE,
                    AllocationTransitionAction.FLIP_CLOSE_FIRST,
                }:
                    if not _apply_pending_reduce_offset_from_plan(current_allocation, transition_plan):
                        _clear_deferred_reduce(current_allocation)

        if (
            current_allocation is not None
            and status == "PENDING_SUBMIT"
            and minimum_residual_early_close
        ):
            _mark_minimum_residual_release_pending(
                current_allocation,
                order=order,
                quantity=quantity,
                now=datetime.now(timezone.utc),
            )

        if current_allocation is not None and transition_plan is not None:
            _record_allocation_event(
                db,
                allocation=current_allocation,
                order=order,
                action="DIRECTION_GUARD_BLOCKED"
                if direction_guard_preserve_allocation
                else "POSITION_CAP_REJECTED"
                if position_cap_rejected
                else "BLOCKED_PRESERVE"
                if blocked_order_preserves_allocation
                else "DEFERRED_REDUCE"
                if deferred_reduce
                else transition_plan.action.value,
                before_notional=transition_plan.current_allocation_notional,
                after_notional=Decimal(current_allocation.allocated_notional or 0)
                if blocked_order_preserves_allocation
                else state_target_notional,
                before_qty=Decimal(current_allocation.allocated_qty or 0),
                after_qty=Decimal(current_allocation.allocated_qty or 0),
                metadata={
                    "reason": transition_plan.reason,
                    "reduce_only": reduce_only,
                    "deferred_reduce": deferred_reduce,
                    "pending_reduce_qty": str(current_allocation.pending_reduce_qty)
                    if deferred_reduce and current_allocation.pending_reduce_qty is not None
                    else None,
                    "formula_inputs": transition_plan.formula_inputs,
                    "allocation_mismatch": allocation_mismatch,
                    "allocation_mismatch_state_lag": allocation_mismatch_state_lag,
                    "allocation_state_preserved": direction_guard_preserve_allocation
                    or blocked_order_preserves_allocation,
                },
            )

        if status == "PENDING_SUBMIT":
            self.pending_intents.reserve(order, current_allocation)
            if submit_order:
                await self.submit_planned_order(db, order, fill)
        elif blockers:
            db.add(
                RiskEvent(
                    severity="warning",
                    event_type="FILL_DRIVEN_ORDER_BLOCKED",
                    symbol=fill.market.canonical_coin,
                    leader_address=leader.leader_address,
                    message="; ".join(blockers),
                    metadata_json={"source_fill_id": fill.source_fill_id, "dex": fill.market.dex},
                )
            )
        if status == "PENDING_SUBMIT":
            _trace_set(order, "order_plan_commit_started_at", datetime.now(timezone.utc))
        await db.commit()
        if status == "PENDING_SUBMIT" and submit_order:
            self._release_pending_intent_if_resolved(order)
        return order

    async def _follower_market_position_version_for_plan(
        self,
        db: Any,
        market: MarketKey,
    ) -> int:
        memory_entry = (
            self.manual_position_guard.active_entry(market)
            if self.manual_position_guard is not None
            else None
        )
        if memory_entry is not None:
            raise RetryableFillProcessingError(memory_entry.blocker_message())
        if not isinstance(db, AsyncSession):
            return 0
        row = await db.scalar(
            _follower_market_guard_query(
                market,
                execution_scope=self.execution_scope,
            ).with_for_update()
        )
        if row is None:
            return 0
        if bool(row.active):
            if self.manual_position_guard is not None:
                self.manual_position_guard.mark(
                    market,
                    reason=row.reason or "durable unmatched follower fill guard",
                    observed_at=row.observed_at,
                    position_version=int(row.position_version or 0),
                    expected_position_side=row.expected_position_side,
                    expected_position_qty=row.expected_position_qty,
                    expected_position_relation=row.expected_position_relation,
                    position_change_confirmed_at=row.position_change_confirmed_at,
                )
            raise RetryableFillProcessingError(
                "DURABLE_FOLLOWER_MARKET_GUARD: an unmatched follower fill is being reconciled; "
                "the source fill remains pending and will be replanned"
            )
        return int(row.position_version or 0)

    async def _standalone_manual_market_owner_blocker(
        self,
        db: Any,
        market: MarketKey,
    ) -> str | None:
        """Reject fills that arrived while a standalone manual position owned a market.

        A manual fill against an existing copy allocation is different: that
        allocation remains the owner while the guard pauses planning until the
        actual follower quantity is synchronized.  With no active allocation,
        the manual position itself is the owner, so retaining leader fills for
        replay would submit stale lifecycle actions after the manual position
        is eventually closed.
        """

        memory_entry = (
            self.manual_position_guard.active_entry(market)
            if self.manual_position_guard is not None
            else None
        )
        durable_guard = None
        if memory_entry is None and isinstance(db, AsyncSession):
            durable_guard = await db.scalar(
                _follower_market_guard_query(
                    market,
                    execution_scope=self.execution_scope,
                ).with_for_update()
            )
            if durable_guard is not None and bool(durable_guard.active):
                if self.manual_position_guard is not None:
                    self.manual_position_guard.mark(
                        market,
                        reason=durable_guard.reason or "durable unmatched follower fill guard",
                        observed_at=durable_guard.observed_at,
                        position_version=int(durable_guard.position_version or 0),
                        expected_position_side=durable_guard.expected_position_side,
                        expected_position_qty=durable_guard.expected_position_qty,
                        expected_position_relation=durable_guard.expected_position_relation,
                        position_change_confirmed_at=durable_guard.position_change_confirmed_at,
                    )
                memory_entry = (
                    self.manual_position_guard.active_entry(market)
                    if self.manual_position_guard is not None
                    else None
                )
        if memory_entry is None and not bool(
            durable_guard is not None and durable_guard.active
        ):
            return None
        if await self._load_market_owner_allocation(db, market) is not None:
            return None
        return (
            f"{MANUAL_MARKET_OWNER_BLOCKED}: {market.canonical_coin} is owned "
            "by a standalone manual follower position; the leader fill that "
            "arrived during manual ownership is intentionally ignored and "
            "will not be replayed after release"
        )

    async def _assert_follower_market_plan_current(
        self,
        db: Any,
        order: ExecutionOrder,
        fill: FillEvent,
    ) -> None:
        if not isinstance(db, AsyncSession):
            return
        await db.execute(
            text("SELECT pg_advisory_xact_lock(:market_key)"),
            {"market_key": _market_transaction_key(fill.market, self.execution_scope)},
        )
        guard = await db.scalar(
            _follower_market_guard_query(
                fill.market,
                execution_scope=self.execution_scope,
            ).with_for_update()
        )
        current_version = int(guard.position_version or 0) if guard is not None else 0
        checklist = order.pre_trade_checklist or {}
        planned_version = _int_or_none(checklist.get("follower_market_position_version"))
        guard_active = bool(guard is not None and guard.active)
        memory_guard_active = bool(
            self.manual_position_guard is not None
            and self.manual_position_guard.active_entry(fill.market) is not None
        )
        if planned_version == current_version and not guard_active and not memory_guard_active:
            order.pre_trade_checklist = _json_safe({
                **checklist,
                "follower_market_position_cas": {
                    "ok": True,
                    "planned_version": planned_version,
                    "current_version": current_version,
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                },
            })
            return

        reason = (
            "unmatched follower fill changed the market position after order planning"
            if planned_version != current_version
            else "follower market guard became active after order planning"
        )
        invalidated = await self._invalidate_unsubmitted_market_plans_for_replay(
            db,
            market=fill.market,
            current_version=current_version,
            invalidate_current_version=guard_active or memory_guard_active,
            reason=reason,
        )
        if invalidated:
            raise StaleFollowerMarketPlanInvalidated(reason)
        raise RetryablePreExchangeSubmitError(
            "follower market position changed before exchange submit; waiting for durable reconciliation"
        )

    async def _invalidate_unsubmitted_market_plans_for_replay(
        self,
        db: AsyncSession,
        *,
        market: MarketKey,
        current_version: int,
        invalidate_current_version: bool,
        reason: str,
    ) -> int:
        rows = (
            await db.execute(
                select(ExecutionOrder)
                .where(ExecutionOrder.source_type == "AUTO_COPY")
                .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(ExecutionOrder.venue_account == self.execution_scope)
                .where(ExecutionOrder.dex == market.dex)
                .where(func.upper(ExecutionOrder.canonical_coin) == market.canonical_coin.upper())
                .where(ExecutionOrder.status.in_(["PENDING_SUBMIT", "SUBMITTING"]))
                .where(ExecutionOrder.order_submit_started_at.is_(None))
                .with_for_update()
            )
        ).scalars().all()
        stale_rows = [
            row
            for row in rows
            if _planned_follower_market_position_version(row) != current_version
            or invalidate_current_version
        ]
        if not stale_rows:
            return 0

        stale_order_ids = [int(row.id) for row in stale_rows if row.id is not None]
        outcome_source_fill_ids = (
            await db.execute(
                select(SourceFillOutcome.source_fill_id).where(
                    SourceFillOutcome.execution_order_id.in_(stale_order_ids)
                )
            )
        ).scalars().all()
        source_fill_ids = {
            str(source_fill_id)
            for source_fill_id in outcome_source_fill_ids
            if source_fill_id
        }
        source_fill_ids.update(
            str(row.source_fill_id) for row in stale_rows if row.source_fill_id
        )
        allocation_ids = {
            int(row.allocation_id) for row in stale_rows if row.allocation_id is not None
        }
        now = datetime.now(timezone.utc)
        if stale_order_ids:
            await db.execute(
                delete(SourceFillOutcome).where(
                    SourceFillOutcome.execution_order_id.in_(stale_order_ids)
                )
            )
        if source_fill_ids:
            await db.execute(
                update(SourceFill)
                .where(SourceFill.source_fill_id.in_(source_fill_ids))
                .values(
                    processed_at=None,
                    next_retry_at=now,
                    last_processing_error=(
                        "stale unsubmitted order plan invalidated after follower market position change"
                    ),
                    updated_at=now,
                )
            )
        for row in stale_rows:
            original_source_fill_id = row.source_fill_id
            original_cloid = row.cloid
            row.pre_trade_checklist = _json_safe({
                **(row.pre_trade_checklist or {}),
                "follower_market_position_cas": {
                    "ok": False,
                    "planned_version": _planned_follower_market_position_version(row),
                    "current_version": current_version,
                    "invalidated_at": now.isoformat(),
                    "reason": reason,
                    "source_fill_id": original_source_fill_id,
                    "cloid": original_cloid,
                },
            })
            row.status = "STALE_PLAN"
            row.dry_run = True
            row.error_message = (
                "STALE_FOLLOWER_MARKET_PLAN: no exchange request was sent; "
                "the source fill was returned to the durable inbox for replanning"
            )
            row.order_finalized_at = now
            # Release both uniqueness keys only because the exchange submit-start
            # marker proves this logical plan was never sent.
            row.source_fill_id = None
            row.cloid = None
            self.pending_intents.release(row)

        if allocation_ids:
            allocations = (
                await db.execute(
                    select(LeaderPositionAllocationRecord)
                    .where(LeaderPositionAllocationRecord.id.in_(allocation_ids))
                    .with_for_update()
                )
            ).scalars().all()
            for allocation in allocations:
                allocated_qty = abs(Decimal(allocation.allocated_qty or 0))
                allocated_notional = abs(Decimal(allocation.allocated_notional or 0))
                if allocated_qty <= ALLOCATION_TRANSITION_TOLERANCE:
                    _close_zero_allocation_lifecycle(
                        allocation,
                        reason="stale unsubmitted market plan invalidated before exchange submit",
                        now=now,
                    )
                else:
                    allocation.target_notional = allocated_notional
                    allocation.status = "OPEN"
                    if allocation.last_source_fill_id in source_fill_ids:
                        allocation.last_source_fill_id = None
                    if allocation.pending_reduce_source_fill_id in source_fill_ids:
                        _clear_deferred_reduce(allocation)

        db.add(
            RiskEvent(
                severity="warning",
                event_type="STALE_FOLLOWER_MARKET_PLANS_REQUEUED",
                symbol=market.canonical_coin,
                leader_address=None,
                message=(
                    "unsubmitted plans were invalidated after a follower position version change; "
                    "source fills returned to durable replay"
                ),
                metadata_json={
                    "execution_venue": ExecutionVenue.HYPERLIQUID.value,
                    "dex": market.dex,
                    "canonical_coin": market.canonical_coin,
                    "current_version": current_version,
                    "invalidated_order_ids": stale_order_ids,
                    "source_fill_count": len(source_fill_ids),
                    "reason": reason,
                },
            )
        )
        await db.flush()
        return len(stale_rows)

    async def submit_planned_order(self, db: Any, order: ExecutionOrder, fill: FillEvent) -> None:
        try:
            status = str(order.status or "").upper()
            if status == "PENDING_SUBMIT":
                # Acquire market serialization before the order-row claim so
                # every transaction keeps the global market -> row lock order.
                # Holding it through the submit-marker commit lets the later
                # submit function reuse this single CAS instead of reacquiring
                # and rereading the same follower-market guard.
                _trace_set(order, "submit_plan_guard_started_at", datetime.now(timezone.utc))
                await self._assert_follower_market_plan_current(db, order, fill)
                _trace_set(order, "submit_plan_guard_done_at", datetime.now(timezone.utc))
                _trace_set(order, "submit_claim_started_at", datetime.now(timezone.utc))
                claimed = await self._claim_order_for_submit(db, order)
                _trace_set(order, "submit_claim_done_at", datetime.now(timezone.utc))
                if not claimed:
                    return
            elif status == "SUBMITTING":
                return
            else:
                self.pending_intents.release(order)
                return
            await self._submit_hyperliquid_order(
                db,
                order,
                fill,
                reduce_only=bool(order.reduce_only),
                market_plan_guard_held=True,
                durable_claim_pending=isinstance(db, AsyncSession),
            )
        except OrderSubmitClaimLost:
            # The signed submit marker is the single durable PENDING->SUBMITTING
            # CAS. A competing worker/recovery path already changed this order,
            # so this transaction must discard every pre-send mutation and must
            # never call the exchange.
            if hasattr(db, "rollback"):
                await db.rollback()
            return
        except StaleFollowerMarketPlanInvalidated:
            await db.commit()
            self.pending_intents.release(order)
            return
        await self._prefer_concurrently_recovered_fill(db, order)
        await self._apply_allocation_fill(db, order)
        await self._update_order_source_fill_outcomes(db, order)
        await db.commit()
        self._release_pending_intent_if_resolved(order)

    async def _block_order_for_active_kill_switch(
        self,
        db: Any,
        order: ExecutionOrder,
        fill: FillEvent,
        *,
        reduce_only: bool,
        serialize_with_control: bool = False,
    ) -> bool:
        # Unit-level fake sessions do not represent a live runtime switch. Tests
        # that exercise this gate opt in explicitly.
        enforce_runtime_switch = isinstance(db, AsyncSession) or bool(
            getattr(db, "enforce_runtime_kill_switch", False)
        )
        if reduce_only or not enforce_runtime_switch:
            return False
        if serialize_with_control:
            await acquire_copy_trading_control_lock(db)
        kill_switch_active = await self._kill_switch_active(db)
        checked_at = datetime.now(timezone.utc)
        order.pre_trade_checklist = _json_safe({
            **(order.pre_trade_checklist or {}),
            "runtime_kill_switch_guard": {
                "ok": not kill_switch_active,
                "checked_at": checked_at.isoformat(),
                "serialized_with_control": serialize_with_control,
                "reduce_close_allowed": True,
            },
            "kill_switch_off": not kill_switch_active,
        })
        if not kill_switch_active:
            return False

        order.status = "BLOCKED"
        order.dry_run = True
        order.error_message = EMERGENCY_KILL_SWITCH_ERROR
        order.order_finalized_at = checked_at
        allocation = None
        if order.allocation_id is not None:
            allocation = await self._load_allocation_for_update(db, order.allocation_id)
        if _allocation_active(allocation):
            _preserve_allocation_state_after_blocked_order(allocation, now=checked_at)
        else:
            await self._close_zero_allocation_after_unsubmitted_open(
                db,
                order,
                fill,
                reason=order.error_message,
            )
        db.add(
            RiskEvent(
                severity="warning",
                event_type="EMERGENCY_KILL_SWITCH_BLOCKED_QUEUED_ORDER",
                symbol=fill.market.canonical_coin,
                leader_address=order.leader_address,
                message=order.error_message,
                metadata_json={
                    "order_id": order.id,
                    "source_fill_id": order.source_fill_id,
                    "allocation_id": order.allocation_id,
                    "cloid": order.cloid,
                    "order_action": order.order_action,
                    "serialized_with_control": serialize_with_control,
                },
            )
        )
        _set_latency_fields(order)
        return True

    async def _block_open_increase_above_latest_position_cap(
        self,
        db: Any,
        order: ExecutionOrder,
        fill: FillEvent,
        *,
        reduce_only: bool,
    ) -> bool:
        """Recheck the latest leader cap at the final reversible boundary.

        Planning rejects the whole logical fill instead of clipping it. This
        second check serializes with frontend config updates and protects a
        queued plan from a lower cap, allocation reconciliation, rounded size,
        or a fresher market price observed after planning. Reduce/close orders
        bypass the guard unconditionally.
        """

        action = str(order.order_action or "").upper()
        if reduce_only or action not in {
            AllocationTransitionAction.OPEN.value,
            AllocationTransitionAction.INCREASE.value,
            AllocationTransitionAction.FLIP_OPEN_SECOND.value,
        }:
            return False
        enforce = isinstance(db, AsyncSession) or bool(
            getattr(db, "enforce_runtime_position_cap", False)
        )
        if not enforce:
            return False

        if isinstance(db, AsyncSession):
            cap = await db.scalar(
                select(LeaderConfig.max_notional_per_trade)
                .where(LeaderConfig.id == order.leader_id)
                # Serialize the final decision with PATCH /leaders/{id}. If the
                # save commits first we see the new cap; otherwise that save
                # takes effect immediately after this already-authorized send.
                .with_for_update(read=True, of=LeaderConfig)
                .limit(1)
            )
        else:
            cap = getattr(db, "runtime_position_cap", None)
        cap_value = _decimal_from_value(cap)
        checked_at = datetime.now(timezone.utc)
        if cap_value is None or cap_value <= 0:
            order.pre_trade_checklist = _json_safe({
                **(order.pre_trade_checklist or {}),
                "final_position_cap_guard": {
                    "ok": True,
                    "cap": None,
                    "checked_at": checked_at.isoformat(),
                    "reduce_close_exempt": True,
                },
            })
            return False

        allocation = None
        if order.allocation_id is not None:
            allocation = await self._load_allocation_for_update(db, order.allocation_id)
        current_qty = abs(Decimal(getattr(allocation, "allocated_qty", 0) or 0))
        current_notional = abs(Decimal(getattr(allocation, "allocated_notional", 0) or 0))
        order_qty = abs(Decimal(order.quantity or 0))
        order_notional = abs(Decimal(order.notional or order.delta_notional or 0))
        planned_target = abs(Decimal(order.target_notional or 0))
        price_candidates = [
            Decimal(order.estimated_price or 0),
            Decimal(order.price or 0),
        ]
        final_limit_price = _decimal_from_value(
            (order.request_payload_masked or {}).get("limit_px")
        )
        if final_limit_price is not None and final_limit_price > 0:
            price_candidates.append(final_limit_price)
        live_price_entry = self.price_cache.get(fill.market.canonical_coin)
        if live_price_entry is not None:
            price_candidates.append(Decimal(live_price_entry.price or 0))
        live_price = max(price_candidates)
        projected_qty_notional = (
            _q((current_qty + order_qty) * live_price)
            if live_price > 0
            else Decimal("0")
        )
        projected_ledger_notional = _q(current_notional + order_notional)
        projected_notional = max(
            _q(planned_target),
            projected_ledger_notional,
            projected_qty_notional,
        )
        cap_value = _q(cap_value)
        blocked = projected_notional > cap_value + ALLOCATION_TRANSITION_TOLERANCE
        guard_payload = {
            "ok": not blocked,
            "cap": str(cap_value),
            "planned_target_notional": str(_q(planned_target)),
            "current_allocation_notional": str(_q(current_notional)),
            "order_notional": str(_q(order_notional)),
            "projected_ledger_notional": str(projected_ledger_notional),
            "current_allocation_qty": str(_q(current_qty)),
            "order_qty": str(_q(order_qty)),
            "final_limit_price": str(_q(final_limit_price))
            if final_limit_price is not None
            else None,
            "live_price": str(_q(live_price)),
            "projected_qty_notional": str(projected_qty_notional),
            "projected_notional": str(projected_notional),
            "policy": "REJECT_WHOLE_OPEN_OR_INCREASE",
            "checked_at": checked_at.isoformat(),
            "reduce_close_exempt": True,
        }
        order.pre_trade_checklist = _json_safe({
            **(order.pre_trade_checklist or {}),
            "final_position_cap_guard": guard_payload,
        })
        if not blocked:
            return False

        order.status = "BLOCKED"
        order.dry_run = True
        order.error_message = (
            f"{MAX_POSITION_NOTIONAL_CAP_EXCEEDED}: final projected position "
            f"{projected_notional} exceeds latest configured cap {cap_value}; "
            "whole open/increase rejected before exchange submit"
        )
        order.order_finalized_at = checked_at
        if _allocation_active(allocation):
            _preserve_allocation_state_after_blocked_order(allocation, now=checked_at)
        else:
            await self._close_zero_allocation_after_unsubmitted_open(
                db,
                order,
                fill,
                reason=order.error_message,
            )
        db.add(
            RiskEvent(
                severity="warning",
                event_type="MAX_POSITION_NOTIONAL_CAP_BLOCKED_ORDER",
                symbol=fill.market.canonical_coin,
                leader_address=order.leader_address,
                message=order.error_message,
                metadata_json={
                    "order_id": order.id,
                    "source_fill_id": order.source_fill_id,
                    "allocation_id": order.allocation_id,
                    "order_action": order.order_action,
                    **guard_payload,
                },
            )
        )
        _set_latency_fields(order)
        return True

    async def _prefer_concurrently_recovered_fill(self, db: Any, order: ExecutionOrder) -> None:
        """Do not let a late submit response overwrite a recovery-confirmed fill."""
        if not isinstance(db, AsyncSession) or order.id is None:
            return
        with db.no_autoflush:
            row = (
                await db.execute(
                    select(
                        ExecutionOrder.status,
                        ExecutionOrder.executed_qty,
                        ExecutionOrder.avg_fill_price,
                        ExecutionOrder.cum_quote,
                        ExecutionOrder.order_id,
                        ExecutionOrder.venue_order_id,
                        ExecutionOrder.raw_response,
                        ExecutionOrder.response_payload_masked,
                        ExecutionOrder.error_message,
                        ExecutionOrder.order_finalized_at,
                    ).where(ExecutionOrder.id == order.id)
                )
            ).one_or_none()
        if row is None or Decimal(row.executed_qty or 0) <= Decimal(order.executed_qty or 0):
            return
        order.status = row.status
        order.executed_qty = row.executed_qty
        order.avg_fill_price = row.avg_fill_price
        order.cum_quote = row.cum_quote
        order.order_id = row.order_id
        order.venue_order_id = row.venue_order_id
        order.raw_response = row.raw_response
        order.response_payload_masked = row.response_payload_masked
        order.error_message = row.error_message
        order.order_finalized_at = row.order_finalized_at

    async def _claim_order_for_submit(self, db: Any, order: ExecutionOrder) -> bool:
        if str(order.status or "").upper() != "PENDING_SUBMIT":
            return False
        if order.id is None or not isinstance(db, AsyncSession):
            claim_time = datetime.now(timezone.utc)
            order.status = "SUBMITTING"
            order.updated_at = claim_time
            if hasattr(db, "commit"):
                await db.commit()
            return True
        # Do not issue a redundant UPDATE here.  The signed submit marker below
        # performs the only durable PENDING_SUBMIT -> SUBMITTING compare-and-set
        # and commits the cloid/signer/nonce/envelope atomically before transport.
        # Until that CAS succeeds, a failure is provably pre-exchange and leaves
        # the durable order replayable. Duplicate workers can prepare in parallel,
        # but only the marker winner is allowed to send.
        return True

    def _release_pending_intent_if_resolved(self, order: ExecutionOrder) -> None:
        if str(order.status or "").upper() not in RECOVERY_ORDER_STATUSES:
            self.pending_intents.release(order)

    async def _release_resolved_pending_intents_for_market(self, db: Any, market: MarketKey) -> None:
        order_ids = self.pending_intents.order_ids_for_market(
            dex=market.dex,
            canonical_coin=market.canonical_coin,
        )
        if not order_ids:
            return
        rows = (
            await db.execute(
                select(ExecutionOrder.id, ExecutionOrder.status).where(ExecutionOrder.id.in_(order_ids))
            )
        ).all()
        active = {int(order_id) for order_id, status in rows if str(status or "").upper() in RECOVERY_ORDER_STATUSES}
        for order_id in order_ids:
            if int(order_id) not in active:
                self.pending_intents.release_order_id(int(order_id))

    async def _blocking_unresolved_same_market_orders(
        self,
        db: Any,
        *,
        leader_address: str,
        market: MarketKey,
    ) -> list[ExecutionOrder]:
        blocking: list[ExecutionOrder] = []
        for attempt in range(UNRESOLVED_SAME_MARKET_RETRY_ATTEMPTS):
            unresolved_rows = (
                await db.execute(
                    unresolved_same_market_order_query(
                        leader_address=leader_address,
                        dex=market.dex,
                        canonical_coin=market.canonical_coin,
                        execution_scope=self.execution_scope,
                    )
                )
            ).scalars().all()
            blocking = [
                row
                for row in unresolved_rows
                if not (
                    str(row.status or "").upper() in TRANSIENT_UNRESOLVED_ORDER_STATUSES
                    and self.pending_intents.has_active_order(row)
                )
            ]
            if not blocking:
                return []
            if not _unresolved_blockers_retryable(blocking):
                return blocking
            if attempt >= UNRESOLVED_SAME_MARKET_RETRY_ATTEMPTS - 1:
                return blocking
            await asyncio.sleep(UNRESOLVED_SAME_MARKET_RETRY_SLEEP_SECONDS)
        return blocking

    async def _record_lifecycle_ignored_order(
        self,
        db: Any,
        *,
        fill: FillEvent,
        leader: LeaderConfig,
        reason: str,
        target_side_hint: PositionSide,
        leader_position_notional: Decimal | None,
        leader_entry_px: Decimal | None,
        leader_account_value: Decimal | None,
        follower_account_value: Decimal | None,
        dedupe_started_at: datetime,
        dedupe_done_at: datetime,
        debounce_started_at: datetime,
        debounce_released_at: datetime,
        lock_wait_started_at: datetime,
        lock_acquired_at: datetime,
        ws_received_at: datetime,
        decision_started_at: datetime,
        account_cache_read_at: datetime,
        account_cache_read_done_at: datetime,
        price_cache_read_at: datetime,
        price_cache_read_done_at: datetime,
    ) -> ExecutionOrder:
        now = datetime.now(timezone.utc)
        action = "IGNORED_OLD_LIFECYCLE"
        cloid = build_hyperliquid_cloid(
            leader_address=leader.leader_address,
            coin=fill.market.coin,
            dex=fill.market.dex,
            side=target_side_hint.value,
            action=action,
            source_fill_id=fill.source_fill_id,
            timestamp_ms=int(decision_started_at.timestamp() * 1000),
        )
        order = ExecutionOrder(
            leader_id=leader.id,
            allocation_id=None,
            leader_address=leader.leader_address,
            source_fill_id=fill.source_fill_id,
            source_type="AUTO_COPY",
            source_coin=fill.market.coin,
            execution_venue=ExecutionVenue.HYPERLIQUID.value,
            venue_account=self.execution_scope,
            dex=fill.market.dex,
            canonical_coin=fill.market.canonical_coin,
            raw_coin_from_fill=fill.market.raw_coin,
            asset_id=fill.market.asset_id,
            venue_symbol=fill.market.venue_symbol,
            hyperliquid_coin=fill.market.coin,
            binance_symbol=None,
            side="IGNORED",
            position_side=target_side_hint.value if target_side_hint != PositionSide.FLAT else None,
            order_action=action,
            order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
            cloid=cloid,
            quantity=Decimal("0"),
            price=fill.price if fill.price > 0 else None,
            estimated_price=fill.price if fill.price > 0 else None,
            leader_entry_px=leader_entry_px,
            follower_avg_entry_px=None,
            notional=abs(Decimal(leader_position_notional or 0)),
            status="IGNORED",
            dry_run=True,
            reduce_only=False,
            is_close_intent=False,
            error_message=reason,
            sizing_mode=None,
            leader_account_value=leader_account_value,
            leader_position_notional=leader_position_notional,
            follower_account_value=follower_account_value,
            copy_multiplier=leader.copy_multiplier,
            target_notional=None,
            delta_notional=None,
            hyperliquid_event_time=fill.hyperliquid_event_time,
            event_received_at=ws_received_at,
            ws_received_at=ws_received_at,
            dedupe_done_at=dedupe_done_at,
            debounce_released_at=debounce_released_at,
            decision_started_at=decision_started_at,
            decision_done_at=now,
            latency_trace_id=cloid,
            latency_trace=_latency_trace_payload(
                fill=fill,
                dedupe_started_at=dedupe_started_at,
                dedupe_done_at=dedupe_done_at,
                debounce_started_at=debounce_started_at,
                debounce_released_at=debounce_released_at,
                lock_wait_started_at=lock_wait_started_at,
                lock_acquired_at=lock_acquired_at,
                decision_started_at=decision_started_at,
                account_cache_read_at=account_cache_read_at,
                account_cache_read_done_at=account_cache_read_done_at,
                price_cache_read_at=price_cache_read_at,
                price_cache_read_done_at=price_cache_read_done_at,
                allocation_read_at=now,
                allocation_read_done_at=now,
                sizing_done_at=now,
                validator_started_at=now,
                validator_done_at=now,
                checklist_done_at=now,
                decision_done_at=now,
                order_plan_created_at=now,
                order_submit_started_at=None,
            ),
            missing_latency_fields=[],
            pre_trade_checklist={
                "lifecycle_gate": "IGNORED_OLD_LIFECYCLE",
                "lifecycle_reason": reason,
                "fill_implied_position": _fill_implied_payload(derive_leader_post_position_from_fill(fill)),
                "order_policy": "FAST_MARKET_ONLY",
                "transition_action": action,
                "blockers": [],
            },
        )
        _set_latency_fields(order)
        db.add(order)
        await db.flush()
        await self._record_source_fill_outcomes(
            db,
            fill,
            order=order,
            disposition=FILL_OUTCOME_NO_ACTION_REQUIRED,
            reason=reason,
        )
        await db.commit()
        return order

    async def _submit_risk_settings(
        self,
        db: Any,
        order: ExecutionOrder,
        fill: FillEvent,
        *,
        reduce_only: bool,
    ) -> tuple[RiskSettingResult, str]:
        checklist = order.pre_trade_checklist or {}
        validator_checklist = (
            checklist.get("order_validator")
            if isinstance(checklist.get("order_validator"), dict)
            else {}
        )
        market_max_leverage = _int_or_none(
            checklist.get("market_max_leverage")
            or validator_checklist.get("market_max_leverage")
            or validator_checklist.get("max_leverage")
        )
        # Leverage/margin settings do not affect a reduce-only action. Reuse the
        # leverage that was already validated while planning instead of issuing
        # another database query on every reduction.
        if reduce_only:
            desired_leverage = (
                _int_or_none(checklist.get("effective_leverage"))
                or _int_or_none(checklist.get("follower_effective_leverage"))
                or int(self.settings.hyperliquid_default_leverage or 10)
            )
            effective_leverage = min(
                value
                for value in (desired_leverage, market_max_leverage)
                if value is not None and value > 0
            )
            return (
                RiskSettingResult(
                    is_ok=True,
                    status="SKIPPED_REDUCE_ONLY_FAST_PATH",
                    account_address=self.settings.hyperliquid_follower_account_address() or "",
                    dex=fill.market.dex,
                    canonical_coin=fill.market.canonical_coin,
                    desired_margin_mode=DESIRED_MARGIN_MODE,
                    desired_leverage=desired_leverage,
                    market_max_leverage=market_max_leverage,
                    effective_leverage=effective_leverage,
                    actual_margin_mode=DESIRED_MARGIN_MODE,
                    actual_leverage=effective_leverage,
                    asset_id=fill.market.asset_id,
                    cache_used=True,
                ),
                "reduce_only_skip",
            )

        checklist_effective = _int_or_none(checklist.get("effective_leverage"))
        # Planning already resolved and validated the market's effective
        # leverage. If that exact market/leverage/margin tuple is confirmed in
        # the process cache, avoid querying MarketRiskSetting again on every
        # burst fill. A cache miss still follows the authoritative DB/exchange
        # path below, so newly seen markets and changed leverage remain safe.
        planned_margin_mode = str(
            checklist.get("follower_margin_mode_required")
            or validator_checklist.get("follower_margin_mode_required")
            or (
                FALLBACK_MARGIN_MODE
                if (
                    checklist.get("market_only_isolated")
                    or validator_checklist.get("market_only_isolated")
                )
                else DESIRED_MARGIN_MODE
            )
        ).upper()
        market_only_isolated_explicit = (
            "market_only_isolated" in checklist
            or "market_only_isolated" in validator_checklist
        )
        if checklist_effective is not None and checklist_effective > 0:
            planned_cache_keys = [
                (
                    str(fill.market.dex or "").lower(),
                    str(fill.market.canonical_coin or "").upper(),
                    checklist_effective,
                    planned_margin_mode,
                )
            ]
            for cache_key in planned_cache_keys:
                cached = self._risk_settings_ok_cache.get(cache_key)
                if cached is not None:
                    return cached, "process_cache_planned_leverage"

        # Planning resolves the policy from the already-prewarmed authoritative
        # market metadata: cross-capable=max leverage, isolated-only=1x.  Reuse
        # that exact value here instead of querying a stale per-market DB target
        # or taking the minimum with it.
        if planned_margin_mode == FALLBACK_MARGIN_MODE:
            effective_leverage = 1
        else:
            effective_leverage = (
                checklist_effective
                or _int_or_none(checklist.get("follower_effective_leverage"))
                or market_max_leverage
                or int(self.settings.hyperliquid_default_leverage or 10)
            )
            if market_max_leverage is not None and market_max_leverage > 0:
                effective_leverage = min(effective_leverage, market_max_leverage)
        desired_leverage = effective_leverage
        account_address = self.settings.hyperliquid_follower_account_address() or ""

        cache_keys = [
            (
                str(fill.market.dex or "").lower(),
                str(fill.market.canonical_coin or "").upper(),
                effective_leverage,
                planned_margin_mode,
            ),
        ]
        for cache_key in cache_keys:
            cached = self._risk_settings_ok_cache.get(cache_key)
            if cached is not None:
                return cached, "process_cache"

        lock_key = (
            str(fill.market.dex or "").lower(),
            str(fill.market.canonical_coin or "").upper(),
            effective_leverage,
        )
        risk_lock = self._risk_settings_locks.setdefault(lock_key, asyncio.Lock())
        async with risk_lock:
            for cache_key in cache_keys:
                cached = self._risk_settings_ok_cache.get(cache_key)
                if cached is not None:
                    return cached, "process_cache"
            risk_settings = await ensure_hyperliquid_market_risk_settings(
                db=db,
                client=self.execution_client,
                settings=self.settings,
                account_address=account_address,
                dex=fill.market.dex,
                canonical_coin_value=fill.market.canonical_coin,
                asset_id=fill.market.asset_id,
                market_max_leverage=market_max_leverage,
                market_only_isolated=(
                    planned_margin_mode == FALLBACK_MARGIN_MODE
                    if market_only_isolated_explicit
                    else None
                ),
                desired_default_leverage=desired_leverage,
                action_type=order.order_action or "OPEN",
                reduce_only=False,
                allow_stale_confirmed_cache=True,
            )
        if risk_settings.is_ok:
            self._risk_settings_ok_cache[
                (
                    str(risk_settings.dex or "").lower(),
                    str(risk_settings.canonical_coin or "").upper(),
                    risk_settings.effective_leverage,
                    risk_settings.desired_margin_mode,
                )
            ] = risk_settings
            if risk_settings.actual_margin_mode and risk_settings.actual_margin_mode != risk_settings.desired_margin_mode:
                self._risk_settings_ok_cache[
                    (
                        str(risk_settings.dex or "").lower(),
                        str(risk_settings.canonical_coin or "").upper(),
                        risk_settings.effective_leverage,
                        risk_settings.actual_margin_mode,
                    )
                ] = risk_settings
        return risk_settings, "confirmed_cache" if risk_settings.cache_used else "exchange_update"

    async def _known_cross_margin_unsupported(
        self,
        db: Any,
        *,
        account_address: str,
        dex: str,
        canonical_coin_value: str,
        desired_leverage: int,
        market_max_leverage: int | None,
        effective_leverage: int | None,
        asset_id: int | None,
    ) -> RiskSettingResult | None:
        if not account_address:
            return None
        row = await db.scalar(
            select(MarketRiskSetting)
            .where(MarketRiskSetting.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(MarketRiskSetting.account_address == account_address.lower())
            .where(MarketRiskSetting.dex == str(dex or "").lower())
            .where(func.upper(MarketRiskSetting.canonical_coin) == str(canonical_coin_value).upper())
            .limit(1)
        )
        if row is None or not _risk_setting_row_says_cross_margin_unsupported(row):
            return None
        return RiskSettingResult(
            is_ok=False,
            status=row.status or "FAILED",
            account_address=account_address.lower(),
            dex=str(dex or "").lower(),
            canonical_coin=canonical_coin_value,
            desired_margin_mode=DESIRED_MARGIN_MODE,
            desired_leverage=desired_leverage,
            market_max_leverage=row.market_max_leverage or market_max_leverage,
            effective_leverage=row.effective_leverage or effective_leverage,
            actual_margin_mode=row.actual_margin_mode,
            actual_leverage=row.actual_leverage,
            asset_id=row.asset_id or asset_id,
            reason_code="CROSS_MARGIN_NOT_SUPPORTED",
            reason=row.error_message or "Hyperliquid reports cross margin is not allowed for this asset",
            cache_used=True,
            row_id=row.id,
            last_confirmed_at=row.last_confirmed_at,
        )

    async def _submit_hyperliquid_order(
        self,
        db: Any,
        order: ExecutionOrder,
        fill: FillEvent,
        *,
        reduce_only: bool,
        market_plan_guard_held: bool = False,
        durable_claim_pending: bool = False,
    ) -> None:
        already_submitted = (
            order.order_submit_started_at is not None
            or order.order_submit_done_at is not None
            or order.order_ack_at is not None
            or bool(order.order_id)
            or bool(order.venue_order_id)
            or bool(order.raw_response)
        )
        allowed_submit_statuses = (
            {"PENDING_SUBMIT", "SUBMITTING"}
            if durable_claim_pending
            else {"SUBMITTING"}
        )
        if already_submitted or (
            isinstance(db, AsyncSession)
            and str(order.status or "").upper() not in allowed_submit_statuses
        ):
            order.status = "BLOCKED"
            order.dry_run = True
            order.error_message = (
                "INTERNAL_SUBMIT_GUARD: order already has submit markers"
                if already_submitted
                else "INTERNAL_SUBMIT_GUARD: order was not claimed before submit"
            )
            db.add(
                RiskEvent(
                    severity="error",
                    event_type="INTERNAL_SUBMIT_GUARD_BLOCKED_ORDER",
                    symbol=fill.market.canonical_coin,
                    leader_address=order.leader_address,
                    message=order.error_message,
                    metadata_json={
                        "order_id": order.id,
                        "source_fill_id": order.source_fill_id,
                        "cloid": order.cloid,
                        "status": order.status,
                        "already_submitted": already_submitted,
                    },
                )
            )
            await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message)
            if not already_submitted:
                await self._restore_unfilled_minimum_residual_release(
                    db,
                    order,
                    fill,
                )
            _set_latency_fields(order)
            return
        if not order.cloid or not order.estimated_price or order.quantity <= 0:
            order.status = "BLOCKED"
            order.dry_run = True
            order.error_message = "invalid order payload"
            await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message)
            await self._restore_unfilled_minimum_residual_release(
                db,
                order,
                fill,
            )
            return
        internal_blockers = self._pre_submit_internal_blockers(
            order=order,
            fill=fill,
            reduce_only=reduce_only,
        )
        if internal_blockers:
            order.status = "BLOCKED"
            order.dry_run = True
            order.error_message = "; ".join(internal_blockers)
            order.pre_trade_checklist = _json_safe({
                **(order.pre_trade_checklist or {}),
                "internal_submit_guard": {
                    "ok": False,
                    "blockers": internal_blockers,
                },
            })
            db.add(
                RiskEvent(
                    severity="error",
                    event_type="INTERNAL_SUBMIT_GUARD_BLOCKED_ORDER",
                    symbol=fill.market.canonical_coin,
                    leader_address=order.leader_address,
                    message=order.error_message,
                    metadata_json={
                        "source_fill_id": fill.source_fill_id,
                        "order_id": order.id,
                        "order_action": order.order_action,
                        "side": order.side,
                        "position_side": order.position_side,
                        "reduce_only": order.reduce_only,
                    },
                )
            )
            _set_latency_fields(order)
            await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message)
            await self._restore_unfilled_minimum_residual_release(
                db,
                order,
                fill,
            )
            return
        if await self._signer_has_ambiguous_submitted_order(db, order):
            raise RetryablePreExchangeSubmitError(
                "SIGNER_SUBMISSION_UNKNOWN_BARRIER: an earlier signed action is still ambiguous; "
                "this order remains durable until recovery resolves it"
            )
        # Every transaction that can touch both the market serialization lock
        # and an allocation row must acquire them in that order.  Fill planning
        # already uses market -> allocation.  Taking the market lock before the
        # live-allocation submit guard prevents a parallel fill planner and
        # submit worker from forming the inverse allocation -> market deadlock.
        # The submit-start commit releases this lock before the exchange call,
        # so exchange round-trip latency is not serialized.
        if not market_plan_guard_held:
            await self._assert_follower_market_plan_current(db, order, fill)
        if await self._guard_reduce_submit_against_live_allocation(db, order, fill):
            await self._restore_unfilled_minimum_residual_release(
                db,
                order,
                fill,
            )
            return
        submit_trace: dict[str, Any] = {}
        _trace_set(order, "submit_flow_started_at", datetime.now(timezone.utc))
        try:
            validator_payload = ((order.pre_trade_checklist or {}).get("order_validator") or {})
            if not validator_payload.get("ok"):
                order.status = "BLOCKED"
                order.dry_run = True
                order.error_message = _validator_payload_message(validator_payload) or "Hyperliquid order validator blocked order"
                _set_latency_fields(order)
                await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message)
                await self._restore_unfilled_minimum_residual_release(
                    db,
                    order,
                    fill,
                )
                return

            _trace_set(order, "risk_setting_started_at", datetime.now(timezone.utc))
            try:
                risk_settings, risk_setting_source = await self._submit_risk_settings(
                    db,
                    order,
                    fill,
                    reduce_only=reduce_only,
                )
            finally:
                _trace_set(order, "risk_setting_done_at", datetime.now(timezone.utc))
            _trace_set_detail(order, "risk_setting_source", risk_setting_source)
            if not risk_settings.is_ok:
                if str(risk_settings.reason_code or "").upper() != "CROSS_MARGIN_NOT_SUPPORTED":
                    raise RetryablePreExchangeSubmitError(
                        risk_settings.reason_code
                        or risk_settings.reason
                        or "Hyperliquid margin settings are temporarily unconfirmed"
                    )
                order.status = "BLOCKED"
                order.dry_run = True
                order.error_message = risk_settings.reason_code or risk_settings.reason or "could not confirm Hyperliquid margin settings"
                order.pre_trade_checklist = _json_safe({
                    **(order.pre_trade_checklist or {}),
                    "follower_margin_mode_required": risk_settings.desired_margin_mode or DESIRED_MARGIN_MODE,
                    "follower_margin_mode_primary": DESIRED_MARGIN_MODE,
                    "follower_margin_mode_fallback": FALLBACK_MARGIN_MODE,
                    "follower_margin_mode_confirmed": False,
                    "follower_isolated_required": DESIRED_MARGIN_MODE == "ISOLATED",
                    "follower_isolated_confirmed": False,
                    "follower_leverage_confirmed": False,
                    "follower_margin_mode": risk_settings.actual_margin_mode,
                    "follower_effective_leverage": risk_settings.effective_leverage,
                    "follower_risk_setting_status": risk_settings.status,
                    "follower_risk_setting_reason_code": risk_settings.reason_code,
                    "follower_risk_settings_reason": risk_settings.reason,
                    "follower_risk_setting_cache_used": risk_settings.cache_used,
                    "follower_risk_setting_source": risk_setting_source,
                })
                db.add(
                    RiskEvent(
                        severity="warning",
                        event_type="RISK_SETTING_BLOCKED_ORDER",
                        symbol=fill.market.canonical_coin,
                        leader_address=order.leader_address,
                        message=order.error_message,
                        metadata_json=risk_settings.payload(),
                    )
                )
                _set_latency_fields(order)
                await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message)
                return
            if risk_settings.warning:
                db.add(
                    RiskEvent(
                        severity="warning",
                        event_type="RISK_SETTING_REDUCE_WARNING",
                        symbol=fill.market.canonical_coin,
                        leader_address=order.leader_address,
                        message=risk_settings.warning,
                        metadata_json=risk_settings.payload(),
                    )
                )
            order.pre_trade_checklist = _json_safe({
                **(order.pre_trade_checklist or {}),
                "follower_margin_mode_required": risk_settings.desired_margin_mode or DESIRED_MARGIN_MODE,
                "follower_margin_mode_primary": DESIRED_MARGIN_MODE,
                "follower_margin_mode_fallback": FALLBACK_MARGIN_MODE,
                "follower_margin_mode_confirmed": True,
                "follower_isolated_required": DESIRED_MARGIN_MODE == "ISOLATED",
                "follower_isolated_confirmed": risk_settings.actual_margin_mode == "ISOLATED",
                "follower_leverage_confirmed": True,
                "follower_margin_mode": risk_settings.actual_margin_mode,
                "follower_effective_leverage": risk_settings.effective_leverage,
                "follower_risk_setting_status": risk_settings.status,
                "follower_risk_setting_cache_used": risk_settings.cache_used,
                "follower_risk_setting_warning": risk_settings.warning,
                "follower_risk_setting_source": risk_setting_source,
            })
            payload_masked = validator_payload.get("payload_masked") or {}
            payload = {
                "coin": payload_masked.get("coin") or fill.market.canonical_coin,
                "dex": payload_masked.get("dex") or fill.market.dex,
                "is_buy": bool(payload_masked.get("is_buy")),
                "sz": Decimal(str(payload_masked.get("sz"))),
                "limit_px": Decimal(str(payload_masked.get("limit_px"))),
                "order_type": {"limit": {"tif": "Ioc"}},
                "reduce_only": bool(payload_masked.get("reduce_only")),
                "cloid": payload_masked.get("cloid") or order.cloid,
            }
            assert_hyperliquid_auto_copy_order(order_type=order.order_type, payload=payload)
            order.request_payload_masked = _json_safe({
                **payload,
                "quantity": payload["sz"],
                "limit_px": payload["limit_px"],
            })
            if await self._block_open_increase_above_latest_position_cap(
                db,
                order,
                fill,
                reduce_only=reduce_only,
            ):
                return
            # This is the final reversible boundary. The shared advisory lock
            # orders this check against Telegram/frontend /off updates; the
            # submit-start marker commit below releases it immediately before
            # the exchange transport call.
            if await self._block_order_for_active_kill_switch(
                db,
                order,
                fill,
                reduce_only=reduce_only,
                serialize_with_control=True,
            ):
                return
            durable_signed_submit = (
                isinstance(db, AsyncSession)
                and callable(getattr(self.execution_client, "prepare_market_order_envelope", None))
                and callable(getattr(self.execution_client, "submit_market_order_envelope", None))
                and bool(getattr(self.execution_client, "signer_scope", None))
            )
            if durable_signed_submit:
                try:
                    submit_nonce = await self._allocate_submit_nonce(db)
                    signed_envelope = self.execution_client.prepare_market_order_envelope(
                        nonce=submit_nonce,
                        latency_trace=submit_trace,
                        **payload,
                    )
                    signed_hash = _signed_envelope_hash(signed_envelope)
                except Exception as exc:
                    raise RetryablePreExchangeSubmitError(
                        f"could not durably prepare signed exchange action: {redact_text(exc)}"
                    ) from exc
                order.submit_signer_scope = self.execution_client.signer_scope
                order.submit_nonce = submit_nonce
                order.signed_action_envelope = _json_safe(signed_envelope)
                order.signed_action_hash = signed_hash
            exchange_submit_started_at = datetime.now(timezone.utc)
            await self._persist_submit_started_marker(
                db,
                order,
                exchange_submit_started_at,
                claim_pending=durable_claim_pending,
            )
            if durable_signed_submit:
                response = await self.execution_client.submit_market_order_envelope(
                    signed_envelope,
                    latency_trace=submit_trace,
                )
            else:
                response = await self.execution_client.place_market_order(**payload, _latency_trace=submit_trace)
            exchange_submit_done_at = datetime.now(timezone.utc)
            _trace_merge_submit_trace(order, submit_trace)
            order.order_submit_done_at = exchange_submit_done_at
            order.order_ack_at = exchange_submit_done_at
            order.binance_order_ack_at = exchange_submit_done_at
            _trace_set(order, "exchange_submit_call_done_at", exchange_submit_done_at)
            _trace_set(order, "order_submit_done_at", exchange_submit_done_at)
            _trace_set(order, "order_ack_at", exchange_submit_done_at)
            safe_response = _json_safe(response)
            order.raw_response = safe_response
            order.response_payload_masked = safe_response
            status = _hyperliquid_status(response)
            order.status = status
            hyperliquid_error = _hyperliquid_error(response)
            if hyperliquid_error:
                order.error_message = f"Hyperliquid order rejected: {hyperliquid_error}"
            order.dry_run = False
            fill_qty, avg_px = _hyperliquid_fill_qty_price(response)
            if fill_qty is not None:
                order.executed_qty = fill_qty
            if avg_px is not None:
                order.avg_fill_price = avg_px
            if fill_qty is not None and avg_px is not None:
                order.cum_quote = _q(fill_qty * avg_px)
            oid = _hyperliquid_oid(response)
            if oid:
                order.order_id = oid
                order.venue_order_id = oid
            if (
                hyperliquid_error
                and fill_qty is None
                and is_hyperliquid_network_upgrade_post_only_error(hyperliquid_error)
            ):
                db.add(
                    RiskEvent(
                        severity="critical",
                        event_type=HYPERLIQUID_NETWORK_UPGRADE_POST_ONLY_REJECTION,
                        symbol=str(fill.market.canonical_coin)[:32],
                        leader_address=order.leader_address,
                        message=(
                            "Hyperliquid explicitly rejected the order during the "
                            "network-upgrade post-only period; manual action required"
                        ),
                        metadata_json={
                            "order_id": order.id,
                            "source_fill_id": order.source_fill_id,
                            "canonical_coin": fill.market.canonical_coin,
                            "order_action": order.order_action,
                            "position_side": order.position_side,
                            "quantity": str(order.quantity),
                            "reduce_only": bool(order.reduce_only),
                            "exchange_error": redact_text(hyperliquid_error),
                        },
                    )
                )
            response_parsed_at = datetime.now(timezone.utc)
            _trace_set(order, "exchange_response_parsed_at", response_parsed_at)
            if status in {"OPEN", "RESTING", "SUBMITTED"}:
                try:
                    await self.execution_client.cancel_by_cloid(coin=fill.market.venue_symbol, cloid=order.cloid)
                    db.add(
                        RiskEvent(
                            severity="warning",
                            event_type="IOC_RESTING_CANCEL_REQUESTED",
                            symbol=fill.market.canonical_coin,
                            leader_address=order.leader_address,
                            message="IOC order returned resting/open status; cancel requested",
                            metadata_json={"cloid": order.cloid, "oid": oid},
                        )
                    )
                except Exception as exc:
                    db.add(
                        RiskEvent(
                            severity="warning",
                            event_type="IOC_CANCEL_FAILED",
                            symbol=fill.market.canonical_coin,
                            leader_address=order.leader_address,
                            message=f"IOC resting/open cancel failed: {redact_text(exc)}",
                            metadata_json={"cloid": order.cloid},
                        )
                    )
                order.status = "UNKNOWN"
                order.error_message = "IOC order returned resting/open status; cancel requested"
            if order.status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                fill_confirmed_at = datetime.now(timezone.utc)
                order.order_finalized_at = fill_confirmed_at
                _trace_set(order, "fill_confirmed_at", fill_confirmed_at)
                _trace_set(order, "order_finalized_at", fill_confirmed_at)
        except AutoCopyOrderPolicyError as exc:
            order.status = "BLOCKED"
            order.dry_run = True
            order.error_message = str(exc)
            await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message)
        except RetryablePreExchangeSubmitError:
            raise
        except OrderSubmitClaimLost:
            raise
        except StaleFollowerMarketPlanInvalidated:
            raise
        except Exception as exc:
            # A database deadlock/serialization/connection failure before the
            # durable submit-start marker is provably pre-exchange.  Let the
            # queue worker roll the failed transaction back and retry the same
            # durable order; querying from this aborted transaction would only
            # mask the root error with InFailedSQLTransaction and delay replay.
            if order.order_submit_started_at is None and _is_transient_submit_exception(exc):
                raise
            _trace_merge_submit_trace(order, submit_trace)
            now = datetime.now(timezone.utc)
            if order.order_submit_started_at is not None:
                order.order_submit_done_at = now
                order.order_ack_at = now
                order.binance_order_ack_at = now
                _trace_set(order, "exchange_submit_call_done_at", now)
                _trace_set(order, "order_submit_done_at", now)
                _trace_set(order, "order_ack_at", now)
            if _is_definitely_not_submitted_hyperliquid_error(exc):
                order.status = "FAILED"
                order.order_finalized_at = now
                _trace_set(order, "order_finalized_at", now)
                order.error_message = f"Hyperliquid order not submitted: {redact_text(exc)}"
                await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message)
            else:
                order.status = "UNKNOWN"
                order.error_message = f"Hyperliquid order status unknown: {redact_text(exc)}"
            order.dry_run = False
        if order.status in {"REJECTED", "CANCELED", "EXPIRED"} and not order.executed_qty:
            await self._close_allocation_after_absent_reduce_rejection(db, order, fill)
            await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message or order.status)
        if (
            str(order.status or "").upper()
            in {"FAILED", "REJECTED", "CANCELED", "EXPIRED", "BLOCKED"}
            and not order.executed_qty
        ):
            await self._restore_unfilled_minimum_residual_release(
                db,
                order,
                fill,
            )
        _set_latency_fields(order)
        actual_send_ms = ((order.latency_trace or {}).get("metrics") or {}).get(
            "ws_to_actual_send_ms"
        )
        if actual_send_ms is not None and actual_send_ms > PER_FILL_INTERNAL_LATENCY_WARN_MS:
            metrics = (order.latency_trace or {}).get("metrics") or {}
            log.warning(
                "per_fill_actual_send_latency_slo_missed",
                order_id=order.id,
                source_fill_id=order.source_fill_id,
                dex=order.dex,
                canonical_coin=order.canonical_coin,
                order_action=order.order_action,
                ws_to_actual_send_ms=actual_send_ms,
                ws_to_submit_ms=order.ws_to_submit_ms,
                parse_to_dedupe_ms=metrics.get("parse_to_dedupe_ms"),
                dedupe_ms=metrics.get("dedupe_ms"),
                decision_ms=metrics.get("decision_ms"),
                order_plan_commit_ms=metrics.get("order_plan_commit_ms"),
                submit_scheduler_lag_ms=metrics.get("submit_scheduler_lag_ms"),
                submit_order_load_ms=metrics.get("submit_order_load_ms"),
                submit_plan_guard_ms=metrics.get("submit_plan_guard_ms"),
                submit_claim_ms=metrics.get("submit_claim_ms"),
                risk_setting_ms=metrics.get("risk_setting_ms"),
                sdk_prepare_sign_ms=metrics.get("sdk_prepare_sign_ms"),
                submit_marker_to_actual_send_ms=metrics.get(
                    "submit_marker_to_actual_send_ms"
                ),
            )

    async def _signer_has_ambiguous_submitted_order(
        self,
        db: Any,
        order: ExecutionOrder,
    ) -> bool:
        if not isinstance(db, AsyncSession) or order.id is None:
            return False
        signer_scope = str(getattr(self.execution_client, "signer_scope", "") or "")
        if not signer_scope:
            return False
        return bool(
            await db.scalar(
                _ambiguous_signer_order_query(
                    signer_scope=signer_scope,
                    current_order_id=int(order.id),
                )
            )
        )

    async def _allocate_submit_nonce(self, db: AsyncSession) -> int:
        signer_scope = str(getattr(self.execution_client, "signer_scope", "") or "")
        if not signer_scope:
            raise RuntimeError("execution signer scope is unavailable")
        now = datetime.now(timezone.utc)
        nonce_floor = int(now.timestamp() * 1000)
        nonce_insert = insert(SignerNonceState).values(
            signer_scope=signer_scope,
            last_nonce=nonce_floor,
            updated_at=now,
        )
        allocated = await db.scalar(
            nonce_insert.on_conflict_do_update(
                index_elements=[SignerNonceState.signer_scope],
                set_={
                    "last_nonce": func.greatest(
                        SignerNonceState.last_nonce + 1,
                        nonce_floor,
                    ),
                    "updated_at": now,
                },
            ).returning(SignerNonceState.last_nonce)
        )
        if allocated is None:
            raise RuntimeError("database did not allocate an exchange nonce")
        allocated_nonce = int(allocated)
        reserve_in_process = getattr(
            self.execution_client,
            "reserve_action_nonce_at_least",
            None,
        )
        if callable(reserve_in_process):
            allocated_nonce = int(reserve_in_process(allocated_nonce))
            await db.execute(
                update(SignerNonceState)
                .where(SignerNonceState.signer_scope == signer_scope)
                .values(
                    last_nonce=func.greatest(
                        SignerNonceState.last_nonce,
                        allocated_nonce,
                    ),
                    updated_at=now,
                )
            )
        return allocated_nonce

    async def _persist_submit_started_marker(
        self,
        db: Any,
        order: ExecutionOrder,
        started_at: datetime,
        *,
        claim_pending: bool = False,
    ) -> None:
        if isinstance(db, AsyncSession) and order.id is not None:
            expected_status = "PENDING_SUBMIT" if claim_pending else "SUBMITTING"
            _trace_set(order, "submit_marker_write_started_at", datetime.now(timezone.utc))
            result = await db.execute(
                update(ExecutionOrder)
                .where(ExecutionOrder.id == order.id)
                .where(ExecutionOrder.venue_account == self.execution_scope)
                .where(ExecutionOrder.status == expected_status)
                .where(ExecutionOrder.order_submit_started_at.is_(None))
                .values(
                    status="SUBMITTING",
                    order_submit_started_at=started_at,
                    binance_order_submit_at=started_at,
                    request_payload_masked=order.request_payload_masked,
                    signed_action_envelope=order.signed_action_envelope,
                    signed_action_hash=order.signed_action_hash,
                    submit_signer_scope=order.submit_signer_scope,
                    submit_nonce=order.submit_nonce,
                    pre_trade_checklist=order.pre_trade_checklist,
                    updated_at=started_at,
                )
                .returning(ExecutionOrder.id)
            )
            if result.scalar_one_or_none() is None:
                raise OrderSubmitClaimLost(
                    "durable submit-start marker CAS was already claimed"
                )
            _trace_set(order, "submit_marker_cas_done_at", datetime.now(timezone.utc))
            await db.commit()
            _trace_set(order, "submit_marker_commit_done_at", datetime.now(timezone.utc))
        order.status = "SUBMITTING"
        order.order_submit_started_at = started_at
        order.binance_order_submit_at = started_at
        _trace_set(order, "order_submit_started_at", started_at)
        _trace_set(order, "exchange_submit_call_started_at", started_at)
        _set_latency_fields(order)
        if (
            order.ws_to_submit_ms is not None
            and order.ws_to_submit_ms > PER_FILL_INTERNAL_LATENCY_WARN_MS
        ):
            metrics = (order.latency_trace or {}).get("metrics") or {}
            log.warning(
                "per_fill_internal_latency_slo_missed",
                order_id=order.id,
                source_fill_id=order.source_fill_id,
                dex=order.dex,
                canonical_coin=order.canonical_coin,
                order_action=order.order_action,
                ws_to_submit_ms=order.ws_to_submit_ms,
                dedupe_ms=metrics.get("dedupe_ms"),
                decision_ms=metrics.get("decision_ms"),
                pending_submit_db_write_ms=metrics.get("pending_submit_db_write_ms"),
            )

    async def _guard_reduce_submit_against_live_allocation(
        self,
        db: Any,
        order: ExecutionOrder,
        fill: FillEvent,
    ) -> bool:
        if not bool(order.reduce_only) or not _reduce_like_action(order.order_action) or order.allocation_id is None:
            return False
        allocation = await self._load_allocation_for_submit_guard(db, order.allocation_id)
        if allocation is None:
            order.status = "BLOCKED"
            order.dry_run = True
            order.error_message = "INTERNAL_SUBMIT_GUARD: reduce/close allocation missing before submit"
            _set_latency_fields(order)
            return True
        remaining_qty = max(Decimal("0"), Decimal(allocation.allocated_qty or 0))
        prior_reduce_qty = self.pending_intents.reduce_quantity_before(order)
        safe_remaining_qty = max(Decimal("0"), remaining_qty - prior_reduce_qty)
        requested_qty = max(Decimal("0"), Decimal(order.quantity or 0))
        order.pre_trade_checklist = _json_safe({
            **(order.pre_trade_checklist or {}),
            "submit_parallel_reduce_guard": {
                "remaining_allocation_qty": str(remaining_qty),
                "prior_unresolved_reduce_qty": str(prior_reduce_qty),
                "safe_remaining_allocation_qty": str(safe_remaining_qty),
                "requested_qty": str(requested_qty),
            },
        })
        if safe_remaining_qty <= ALLOCATION_TRANSITION_TOLERANCE:
            order.status = "BLOCKED"
            order.dry_run = True
            order.error_message = "INTERNAL_SUBMIT_GUARD: reduce/close allocation already flat before submit"
            order.pre_trade_checklist = _json_safe({
                **(order.pre_trade_checklist or {}),
                "submit_reduce_allocation_guard": {
                    "ok": False,
                    "reason": "allocation already flat before submit",
                    "requested_qty": requested_qty,
                    "remaining_allocation_qty": remaining_qty,
                    "prior_unresolved_reduce_qty": prior_reduce_qty,
                    "safe_remaining_allocation_qty": safe_remaining_qty,
                },
            })
            db.add(
                RiskEvent(
                    severity="warning",
                    event_type="REDUCE_SUBMIT_BLOCKED_FLAT_ALLOCATION",
                    symbol=fill.market.canonical_coin,
                    leader_address=order.leader_address,
                    message=order.error_message,
                    metadata_json={
                        "order_id": order.id,
                        "allocation_id": order.allocation_id,
                        "source_fill_id": order.source_fill_id,
                        "requested_qty": str(requested_qty),
                        "remaining_allocation_qty": str(remaining_qty),
                        "prior_unresolved_reduce_qty": str(prior_reduce_qty),
                        "safe_remaining_allocation_qty": str(safe_remaining_qty),
                    },
                )
            )
            _set_latency_fields(order)
            return True
        action = str(order.order_action or "").upper()
        if (
            action in {
                AllocationTransitionAction.CLOSE.value,
                AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
            }
            and requested_qty + ALLOCATION_TRANSITION_TOLERANCE < safe_remaining_qty
        ):
            expanded_qty = safe_remaining_qty
            estimated_price = Decimal(order.estimated_price or 0)
            expanded_notional = (
                _q(expanded_qty * estimated_price)
                if estimated_price > 0
                else Decimal(order.notional or 0)
            )
            original_qty = requested_qty
            original_notional = Decimal(order.notional or 0)
            order.quantity = expanded_qty
            order.notional = expanded_notional
            order.delta_notional = -expanded_notional
            order.pre_trade_checklist = _json_safe(
                _with_expanded_final_close_validator_payload(
                    order.pre_trade_checklist or {},
                    expanded_qty=expanded_qty,
                    expanded_notional=expanded_notional,
                    original_qty=original_qty,
                    original_notional=original_notional,
                    remaining_allocation_qty=safe_remaining_qty,
                )
            )
            db.add(
                RiskEvent(
                    severity="warning",
                    event_type="FINAL_CLOSE_SUBMIT_QTY_EXPANDED_TO_ALLOCATION",
                    symbol=fill.market.canonical_coin,
                    leader_address=order.leader_address,
                    message="final close submit quantity expanded to the remaining leader allocation",
                    metadata_json={
                        "order_id": order.id,
                        "allocation_id": order.allocation_id,
                        "source_fill_id": order.source_fill_id,
                        "original_qty": str(original_qty),
                        "expanded_qty": str(expanded_qty),
                        "remaining_allocation_qty": str(remaining_qty),
                        "prior_unresolved_reduce_qty": str(prior_reduce_qty),
                        "safe_remaining_allocation_qty": str(safe_remaining_qty),
                    },
                )
            )
            return False
        if requested_qty <= safe_remaining_qty + ALLOCATION_TRANSITION_TOLERANCE:
            return False

        clamped_qty = safe_remaining_qty
        estimated_price = Decimal(order.estimated_price or 0)
        clamped_notional = _q(clamped_qty * estimated_price) if estimated_price > 0 else Decimal(order.notional or 0)
        original_qty = requested_qty
        original_notional = Decimal(order.notional or 0)
        order.quantity = clamped_qty
        order.notional = clamped_notional
        order.delta_notional = -clamped_notional
        order.pre_trade_checklist = _json_safe(
            _with_clamped_reduce_validator_payload(
                order.pre_trade_checklist or {},
                clamped_qty=clamped_qty,
                clamped_notional=clamped_notional,
                original_qty=original_qty,
                original_notional=original_notional,
                remaining_allocation_qty=safe_remaining_qty,
            )
        )
        db.add(
            RiskEvent(
                severity="warning",
                event_type="REDUCE_SUBMIT_QTY_CLAMPED_TO_ALLOCATION",
                symbol=fill.market.canonical_coin,
                leader_address=order.leader_address,
                message="reduce/close submit qty clamped to remaining allocation",
                metadata_json={
                    "order_id": order.id,
                    "allocation_id": order.allocation_id,
                    "source_fill_id": order.source_fill_id,
                    "original_qty": str(original_qty),
                    "clamped_qty": str(clamped_qty),
                    "remaining_allocation_qty": str(remaining_qty),
                    "prior_unresolved_reduce_qty": str(prior_reduce_qty),
                    "safe_remaining_allocation_qty": str(safe_remaining_qty),
                },
            )
        )
        return False

    async def _load_allocation_for_submit_guard(
        self,
        db: Any,
        allocation_id: int,
    ) -> LeaderPositionAllocationRecord | None:
        flush = getattr(db, "flush", None)
        if flush is not None:
            maybe = flush()
            if hasattr(maybe, "__await__"):
                await maybe
        return await db.get(LeaderPositionAllocationRecord, allocation_id)

    async def _close_zero_allocation_after_unsubmitted_open(
        self,
        db: Any,
        order: ExecutionOrder,
        fill: FillEvent,
        *,
        reason: str | None,
    ) -> None:
        if not _open_like_action(order.order_action) or order.reduce_only or order.allocation_id is None:
            return
        allocation = await self._load_allocation_for_update(db, order.allocation_id)
        stale_reason = _stale_zero_allocation_reason(allocation)
        if not stale_reason:
            return
        close_reason = f"{stale_reason}: {reason or 'open was not submitted'}"
        _close_zero_allocation_lifecycle(allocation, reason=close_reason, now=datetime.now(timezone.utc))
        db.add(
            RiskEvent(
                severity="warning",
                event_type="ZERO_ALLOCATION_OPEN_ABORTED",
                symbol=fill.market.canonical_coin,
                leader_address=order.leader_address,
                message=close_reason,
                metadata_json={
                    "allocation_id": allocation.id,
                    "order_id": order.id,
                    "source_fill_id": fill.source_fill_id,
                    "dex": fill.market.dex,
                    "canonical_coin": fill.market.canonical_coin,
                    "order_status": order.status,
                    "order_action": order.order_action,
                },
            )
        )

    async def _close_allocation_after_absent_reduce_rejection(
        self,
        db: Any,
        order: ExecutionOrder,
        fill: FillEvent,
    ) -> None:
        if (
            order.status != "REJECTED"
            or not bool(order.reduce_only)
            or not _reduce_like_action(order.order_action)
            or order.allocation_id is None
            or not _reduce_only_rejected_position_absent(order.error_message)
        ):
            return
        allocation = await self._load_allocation_for_update(db, order.allocation_id)
        if allocation is None or str(allocation.status or "").upper() == "CLOSED":
            return
        before_qty = Decimal(allocation.allocated_qty or 0)
        before_notional = Decimal(allocation.allocated_notional or 0)
        now = datetime.now(timezone.utc)
        allocation.allocated_qty = Decimal("0")
        allocation.allocated_notional = Decimal("0")
        allocation.target_notional = Decimal("0")
        allocation.status = "CLOSED"
        allocation.pending_reduce_qty = None
        allocation.pending_reduce_notional = None
        allocation.pending_reduce_reason = (
            MINIMUM_RESIDUAL_ECONOMIC_FLAT_REASON
            if bool(
                (order.pre_trade_checklist or {}).get(
                    "minimum_residual_early_close"
                )
            )
            else "reduce-only order rejected because follower position was already absent"
        )
        allocation.pending_reduce_since = None
        allocation.pending_reduce_source_fill_id = None
        allocation.last_reconcile_at = now
        db.add(
            AllocationEvent(
                allocation_id=allocation.id,
                execution_order_id=order.id,
                leader_id=allocation.leader_id,
                leader_address=allocation.leader_address,
                source_fill_id=order.source_fill_id,
                execution_venue=allocation.execution_venue,
                dex=allocation.dex,
                canonical_coin=allocation.canonical_coin,
                position_side=allocation.position_side,
                action="ABSENT_REDUCE_REJECT_CLOSE",
                before_notional=before_notional,
                after_notional=Decimal("0"),
                before_qty=before_qty,
                after_qty=Decimal("0"),
                metadata_json=_json_safe({
                    "order_id": order.id,
                    "order_status": order.status,
                    "order_action": order.order_action,
                    "error_message": order.error_message,
                }),
            )
        )
        db.add(
            RiskEvent(
                severity="warning",
                event_type="ABSENT_REDUCE_REJECT_CLOSE",
                symbol=fill.market.canonical_coin,
                leader_address=order.leader_address,
                message=(
                    "reduce-only copy order was rejected because the follower position was already absent; "
                    "closed allocation to prevent stale lifecycle reuse"
                ),
                metadata_json=_json_safe({
                    "allocation_id": allocation.id,
                    "order_id": order.id,
                    "source_fill_id": order.source_fill_id,
                    "dex": fill.market.dex,
                    "canonical_coin": fill.market.canonical_coin,
                    "before_qty": str(before_qty),
                    "before_notional": str(before_notional),
                }),
            )
        )

    async def _restore_unfilled_minimum_residual_release(
        self,
        db: Any,
        order: ExecutionOrder,
        fill: FillEvent,
    ) -> None:
        """Cancel a provisional release when no follower close was executed."""
        if (
            order.allocation_id is None
            or not bool(
                (order.pre_trade_checklist or {}).get(
                    "minimum_residual_early_close"
                )
            )
        ):
            return
        allocation = await self._load_allocation_for_update(
            db,
            order.allocation_id,
        )
        if (
            allocation is None
            or str(allocation.status or "").upper() == "CLOSED"
            or str(allocation.pending_reduce_reason or "")
            != MINIMUM_RESIDUAL_ECONOMIC_FLAT_PENDING_REASON
        ):
            return
        before_target = Decimal(allocation.target_notional or 0)
        now = datetime.now(timezone.utc)
        allocation.target_notional = Decimal(
            allocation.allocated_notional or 0
        )
        allocation.status = (
            "OPEN" if _allocation_active(allocation) else "CLOSED"
        )
        _clear_deferred_reduce(allocation)
        allocation.last_reconcile_at = now
        db.add(
            AllocationEvent(
                allocation_id=allocation.id,
                execution_order_id=order.id,
                leader_id=allocation.leader_id,
                leader_address=allocation.leader_address,
                source_fill_id=order.source_fill_id,
                execution_venue=allocation.execution_venue,
                dex=allocation.dex,
                canonical_coin=allocation.canonical_coin,
                position_side=allocation.position_side,
                action="MINIMUM_RESIDUAL_RELEASE_RESTORED",
                before_notional=before_target,
                after_notional=Decimal(
                    allocation.allocated_notional or 0
                ),
                before_qty=Decimal(allocation.allocated_qty or 0),
                after_qty=Decimal(allocation.allocated_qty or 0),
                metadata_json=_json_safe(
                    {
                        "order_id": order.id,
                        "order_status": order.status,
                        "error_message": order.error_message,
                    }
                ),
            )
        )
        db.add(
            RiskEvent(
                severity="warning",
                event_type="MINIMUM_RESIDUAL_RELEASE_RESTORED",
                symbol=fill.market.canonical_coin,
                leader_address=order.leader_address,
                message=(
                    "minimum-residual close reached a terminal state without "
                    "execution; follower allocation remains owner and the "
                    "provisional market release was canceled"
                ),
                metadata_json=_json_safe(
                    {
                        "allocation_id": allocation.id,
                        "order_id": order.id,
                        "source_fill_id": order.source_fill_id,
                        "order_status": order.status,
                        "dex": fill.market.dex,
                        "canonical_coin": fill.market.canonical_coin,
                    }
                ),
            )
        )

    def _pre_submit_internal_blockers(
        self,
        *,
        order: ExecutionOrder,
        fill: FillEvent,
        reduce_only: bool,
    ) -> list[str]:
        blockers = _order_intent_blockers(
            order,
            derive_leader_post_position_from_fill(fill),
            reduce_only=reduce_only,
            allow_missed_reduce_catchup=bool((order.pre_trade_checklist or {}).get("missed_reduce_catchup")),
        )
        action = str(order.order_action or "").upper()
        checklist = order.pre_trade_checklist or {}
        manual_guard_blocked = False
        if checklist.get("manual_follower_position_guard"):
            blockers.append("INTERNAL_SUBMIT_GUARD: manual follower position guard active")
            manual_guard_blocked = True
        if (
            not manual_guard_blocked
            and self.manual_position_guard is not None
            and self.manual_position_guard.active_entry(fill.market) is not None
        ):
            blockers.append("INTERNAL_SUBMIT_GUARD: manual follower position guard active")
        if (
            checklist.get("market_ownership_economic_dust_reopen")
            and checklist.get("economic_dust_reopen_follower_flat") is not True
        ):
            blockers.append(
                "INTERNAL_SUBMIT_GUARD: economic dust reopen requires "
                "confirmed follower-flat planning state"
            )
        if action in {
            AllocationTransitionAction.REDUCE.value,
            AllocationTransitionAction.CLOSE.value,
            AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
        }:
            if order.allocation_id is None:
                blockers.append("INTERNAL_SUBMIT_GUARD: reduce/close requires allocation_id")
            if not checklist.get("allocation_scope_guard"):
                blockers.append("INTERNAL_SUBMIT_GUARD: reduce/close missing allocation scope guard")
            if (
                checklist.get("allocation_mismatch")
                and not checklist.get("allocation_mismatch_state_lag")
                and not checklist.get("allocation_mismatch_reduce_safe")
            ):
                blockers.append("INTERNAL_SUBMIT_GUARD: allocation mismatch present")
            if (
                checklist.get("unmanaged_follower_position")
                and not checklist.get("unmanaged_follower_position_state_lag")
                and not checklist.get("unmanaged_follower_position_reduce_safe")
            ):
                blockers.append("INTERNAL_SUBMIT_GUARD: unmanaged follower position present")
        elif action in {
            AllocationTransitionAction.OPEN.value,
            AllocationTransitionAction.INCREASE.value,
            AllocationTransitionAction.FLIP_OPEN_SECOND.value,
        }:
            if order.allocation_id is None:
                blockers.append("INTERNAL_SUBMIT_GUARD: open/increase requires allocation_id")
            if (
                checklist.get("allocation_mismatch")
                and not checklist.get("allocation_mismatch_state_lag")
            ):
                blockers.append("INTERNAL_SUBMIT_GUARD: allocation mismatch present")
            if checklist.get("unmanaged_follower_position") and not checklist.get("unmanaged_follower_position_state_lag"):
                blockers.append("INTERNAL_SUBMIT_GUARD: unmanaged follower position present")
        return blockers

    async def _apply_allocation_fill(self, db: Any, order: ExecutionOrder) -> None:
        if order.allocation_id is None or order.status != "FILLED":
            return
        if order.executed_qty is None or order.executed_qty <= 0:
            return
        if await self._allocation_fill_already_applied(db, order):
            return
        allocation = await self._load_allocation_for_update(db, order.allocation_id)
        if allocation is None:
            return
        if await self._allocation_fill_already_applied(db, order):
            return
        allocation_update_started_at = datetime.now(timezone.utc)
        _trace_set(order, "allocation_update_started_at", allocation_update_started_at)
        result = apply_filled_order_to_allocation_state(
            allocation,
            order,
            fill_qty=Decimal(order.executed_qty),
            update_target_notional=False,
            clear_deferred_reduce=True,
        )
        if result is None:
            return
        minimum_residual_economic_flat_intent = bool(
            (order.pre_trade_checklist or {}).get(
                "minimum_residual_early_close"
            )
        )
        if minimum_residual_economic_flat_intent:
            # This closed allocation is the durable causal boundary that lets a
            # later same-side leader increase be treated as a new follower
            # lifecycle.  Preserve it after any executed close quantity so an
            # eventual follower-flat reconciliation also retains the boundary.
            # The helper additionally requires status=CLOSED and the exact
            # startPosition checkpoint, so a partial fill cannot reopen early.
            allocation.pending_reduce_reason = (
                MINIMUM_RESIDUAL_ECONOMIC_FLAT_REASON
            )
        allocation_update_done_at = result.applied_at
        await self._promote_baseline_for_active_allocation(
            db,
            allocation=allocation,
            order=order,
            now=allocation_update_done_at,
        )
        _trace_set(order, "allocation_update_done_at", allocation_update_done_at)
        _trace_set(order, "state_refresh_scheduled_at", allocation_update_done_at)
        _record_allocation_event(
            db,
            allocation=allocation,
            order=order,
            action="FILL_APPLIED",
            before_notional=result.before_notional,
            after_notional=result.after_notional,
            before_qty=result.before_qty,
            after_qty=result.after_qty,
            metadata={
                "order_action": order.order_action,
                "minimum_residual_economic_flat_intent": (
                    minimum_residual_economic_flat_intent
                ),
                "minimum_residual_economic_flat_closed": bool(
                    minimum_residual_economic_flat_intent
                    and str(allocation.status or "").upper() == "CLOSED"
                ),
            },
        )
        _set_latency_fields(order)

    async def _allocation_fill_already_applied(self, db: Any, order: ExecutionOrder) -> bool:
        if order.id is None:
            return False
        existing = await db.scalar(
            select(AllocationEvent.id)
            .where(AllocationEvent.execution_order_id == order.id)
            .where(AllocationEvent.action == "FILL_APPLIED")
            .limit(1)
        )
        return existing is not None

    async def _load_allocation_for_update(
        self,
        db: Any,
        allocation_id: int,
    ) -> LeaderPositionAllocationRecord | None:
        flush = getattr(db, "flush", None)
        if flush is not None:
            maybe = flush()
            if hasattr(maybe, "__await__"):
                await maybe
        allocation = await db.scalar(
            select(LeaderPositionAllocationRecord)
            .where(LeaderPositionAllocationRecord.id == allocation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
            .limit(1)
        )
        if allocation is not None:
            return allocation
        return await db.get(LeaderPositionAllocationRecord, allocation_id)

    async def _promote_baseline_for_active_allocation(
        self,
        db: Any,
        *,
        allocation: LeaderPositionAllocationRecord,
        order: ExecutionOrder,
        now: datetime,
    ) -> None:
        baseline = await db.scalar(
            select(LeaderPositionBaseline)
            .where(LeaderPositionBaseline.leader_id == allocation.leader_id)
            .where(LeaderPositionBaseline.execution_venue == allocation.execution_venue)
            .where(LeaderPositionBaseline.dex == allocation.dex)
            .where(func.upper(LeaderPositionBaseline.canonical_coin) == str(allocation.canonical_coin or "").upper())
            .limit(1)
        )
        if baseline is None or str(baseline.baseline_status).upper() != BASELINE_WAIT_UNTIL_FLAT:
            return
        baseline.baseline_status = BASELINE_COPY_ALLOWED
        baseline.copy_allowed_at = baseline.copy_allowed_at or now
        baseline.last_checked_at = now
        baseline.last_leader_size = (
            abs(Decimal(allocation.last_leader_position_size))
            if allocation.last_leader_position_size is not None
            else baseline.last_leader_size
        )
        baseline.last_leader_notional = (
            abs(Decimal(order.leader_position_notional))
            if order.leader_position_notional is not None
            else baseline.last_leader_notional
        )
        baseline.reason = "Active follower allocation exists; baseline no longer blocks this copied lifecycle."
        db.add(
            RiskEvent(
                severity="info",
                event_type="BASELINE_PROMOTED_FOR_ACTIVE_ALLOCATION",
                symbol=allocation.canonical_coin,
                leader_address=allocation.leader_address,
                message=baseline.reason,
                metadata_json={
                    "allocation_id": allocation.id,
                    "source_fill_id": order.source_fill_id,
                    "previous_baseline_status": BASELINE_WAIT_UNTIL_FLAT,
                    "new_baseline_status": BASELINE_COPY_ALLOWED,
                },
            )
        )
        await db.flush()

    async def _source_fill_seen(self, db: Any, source_fill_id: str) -> bool:
        return await db.scalar(select(SourceFill).where(SourceFill.source_fill_id == source_fill_id).limit(1)) is not None

    async def _snapshot_recovery_reason(
        self,
        db: Any,
        fill: FillEvent,
        leader: LeaderConfig,
    ) -> str | None:
        implied = derive_leader_post_position_from_fill(fill)
        if str(implied.confidence or "").upper() not in {"HIGH", "MEDIUM"}:
            return None
        if not (implied.is_open or implied.is_increase or implied.is_reduce or implied.is_close or implied.is_flip):
            return None
        side = _snapshot_recovery_allocation_side(implied)
        if side is None:
            return None
        allocation = await self._load_allocation(db, leader, fill.market, side)
        if not _allocation_active(allocation):
            return None
        if not _snapshot_event_after_allocation_checkpoint(fill, allocation):
            return None
        return "recover snapshot fill after active allocation checkpoint"

    async def _record_source_fill(self, db: Any, fill: FillEvent, *, processed: bool) -> bool:
        attempt_at = datetime.now(timezone.utc) if processed else None
        insert_stmt = insert(SourceFill).values(
            source_fill_id=fill.source_fill_id,
            execution_account=self.execution_scope,
            leader_address=fill.leader_address,
            coin=fill.market.coin,
            dex=fill.market.dex,
            canonical_coin=fill.market.canonical_coin,
            raw_coin=fill.market.raw_coin,
            asset_id=fill.market.asset_id,
            side=fill.side,
            price=fill.price,
            size=fill.size,
            source_time_ms=fill.time_ms,
            ws_received_at=fill.ws_received_at,
            raw_fill=fill.raw,
            is_snapshot=fill.is_snapshot,
            processed_at=attempt_at,
            last_attempt_at=attempt_at,
            next_retry_at=None,
            last_processing_error=None,
        )
        if processed:
            # The durable inbox normally inserted this fill already with
            # processed_at=NULL. Claim it atomically in one round trip. The
            # WHERE clause is the exactly-once guard: an already-processed fill
            # returns no row and therefore cannot be submitted again. Keep the
            # original inbox payload immutable: a coalesced planning event may
            # represent several raw fills and must never overwrite one of them.
            stmt = insert_stmt.on_conflict_do_update(
                index_elements=[SourceFill.source_fill_id],
                set_={
                    "processed_at": attempt_at,
                    "last_attempt_at": attempt_at,
                    "next_retry_at": None,
                    "updated_at": attempt_at,
                },
                where=(
                    SourceFill.processed_at.is_(None)
                    & (SourceFill.execution_account == self.execution_scope)
                ),
            ).returning(SourceFill.id)
        else:
            stmt = insert_stmt.on_conflict_do_nothing(
                index_elements=[SourceFill.source_fill_id]
            ).returning(SourceFill.id)
        result = await db.execute(stmt)
        await db.flush()
        return result.scalar_one_or_none() is not None

    async def _claim_source_fill_group(self, db: Any, fill: FillEvent) -> bool:
        if isinstance(db, AsyncSession):
            await db.execute(
                text("SELECT pg_advisory_xact_lock(:market_key)"),
                {"market_key": _market_transaction_key(fill.market, self.execution_scope)},
            )
        claimed = await self._record_source_fill(db, fill, processed=True)
        if not claimed:
            return False
        source_fill_ids = _coalesced_source_fill_ids(fill)
        if len(source_fill_ids) <= 1 or not isinstance(db, AsyncSession):
            await self._mark_coalesced_source_fills_processed(db, fill)
            return True
        rows = (
            await db.execute(
                select(SourceFill.source_fill_id, SourceFill.processed_at)
                .where(SourceFill.execution_account == self.execution_scope)
                .where(SourceFill.source_fill_id.in_(source_fill_ids))
                .with_for_update()
            )
        ).all()
        if len(rows) != len(source_fill_ids):
            raise RetryableFillProcessingError(
                "coalesced fill group is incomplete in the durable inbox"
            )
        representative_id = fill.source_fill_id
        already_processed_members = [
            source_fill_id
            for source_fill_id, processed_at in rows
            if source_fill_id != representative_id and processed_at is not None
        ]
        if already_processed_members:
            raise RuntimeError(
                "coalesced fill group overlaps a previously processed source fill"
            )
        await self._mark_coalesced_source_fills_processed(db, fill)
        return True

    async def _assert_market_fill_fifo(self, db: Any, fill: FillEvent) -> None:
        """Prevent retry timing from changing first-arrival ownership order.

        ``source_fills.id`` is allocated when the websocket fill is durably
        inserted, so it is the restart-safe arrival sequence.  A later fill may
        not pass an earlier unprocessed fill for the same follower market even
        if exponential retry timing or per-leader websocket scheduling differs.
        The market advisory transaction lock acquired by ``_claim_source_fill_group``
        serializes this check with every competing planner.
        """
        if not isinstance(db, AsyncSession):
            return
        current_id_query = (
            select(SourceFill.id)
            .where(SourceFill.execution_account == self.execution_scope)
            .where(SourceFill.source_fill_id == fill.source_fill_id)
            .limit(1)
            .scalar_subquery()
        )
        earlier_id_query = (
            select(SourceFill.id)
            .where(SourceFill.execution_account == self.execution_scope)
            .where(SourceFill.id < current_id_query)
            .where(SourceFill.is_snapshot.is_(False))
            .where(SourceFill.processed_at.is_(None))
            .where(SourceFill.dex == str(fill.market.dex or "").lower())
            .where(func.upper(SourceFill.canonical_coin) == fill.market.canonical_coin.upper())
            .order_by(SourceFill.id.asc())
            .limit(1)
            .scalar_subquery()
        )
        current_id, earlier_id = (
            await db.execute(
                select(
                    current_id_query.label("current_id"),
                    earlier_id_query.label("earlier_id"),
                )
            )
        ).one()
        if current_id is None:
            raise RetryableFillProcessingError(
                "durable source fill row is missing during market FIFO claim"
            )
        if earlier_id is not None:
            raise MarketFillFifoWait(
                "MARKET_FILL_FIFO_WAIT: an earlier durable fill for this market "
                "must finish before this fill can compete for ownership"
            )

    async def _mark_coalesced_source_fills_processed(self, db: Any, fill: FillEvent) -> None:
        source_fill_ids = _coalesced_source_fill_ids(fill)
        if len(source_fill_ids) <= 1:
            return
        now = datetime.now(timezone.utc)
        await db.execute(
            update(SourceFill)
            .where(SourceFill.execution_account == self.execution_scope)
            .where(SourceFill.source_fill_id.in_(source_fill_ids))
            .where(SourceFill.processed_at.is_(None))
            .values(
                processed_at=now,
                last_attempt_at=now,
                next_retry_at=None,
                last_processing_error=None,
                updated_at=now,
            )
        )
        await db.flush()

    async def _record_source_fill_outcomes(
        self,
        db: Any,
        fill: FillEvent,
        *,
        order: ExecutionOrder | None,
        disposition: str,
        reason: str | None,
    ) -> None:
        source_fill_ids = _coalesced_source_fill_ids(fill)
        if not source_fill_ids or not hasattr(db, "execute"):
            return
        values = [
            {
                "source_fill_id": source_fill_id,
                "execution_order_id": order.id if order is not None else None,
                "disposition": disposition,
                "reason": redact_text(reason)[:2000] if reason else None,
            }
            for source_fill_id in source_fill_ids
        ]
        result = await db.execute(
            insert(SourceFillOutcome)
            .values(values)
            .on_conflict_do_nothing(index_elements=[SourceFillOutcome.source_fill_id])
            .returning(SourceFillOutcome.source_fill_id)
        )
        inserted = set(result.scalars().all())
        if len(inserted) == len(source_fill_ids):
            await db.flush()
            return
        existing = (
            await db.execute(
                select(SourceFillOutcome).where(
                    SourceFillOutcome.source_fill_id.in_(source_fill_ids)
                )
            )
        ).scalars().all()
        expected_order_id = order.id if order is not None else None
        for outcome in existing:
            if outcome.execution_order_id != expected_order_id:
                raise RuntimeError(
                    "source fill outcome is already linked to a different logical order"
                )
        await db.flush()

    async def _update_order_source_fill_outcomes(self, db: Any, order: ExecutionOrder) -> None:
        if order.id is None:
            return
        status = str(order.status or "").upper()
        if status == "FILLED" and Decimal(order.executed_qty or 0) > 0:
            disposition = FILL_OUTCOME_EXECUTED
        elif status in RECOVERY_ORDER_STATUSES or status in {"OPEN", "RESTING", "SUBMITTED"}:
            disposition = FILL_OUTCOME_SUBMISSION_UNKNOWN
        elif status in {"NOOP", "IGNORED"}:
            disposition = FILL_OUTCOME_NO_ACTION_REQUIRED
        elif status == "BLOCKED" and str(order.error_message or "").startswith("EMERGENCY_KILL_SWITCH:"):
            disposition = FILL_OUTCOME_NO_ACTION_REQUIRED
        elif status == "BLOCKED" and _expected_no_action_block(order.error_message):
            disposition = FILL_OUTCOME_NO_ACTION_REQUIRED
        else:
            disposition = FILL_OUTCOME_MANUAL_REVIEW
        await db.execute(
            update(SourceFillOutcome)
            .where(SourceFillOutcome.execution_order_id == order.id)
            .values(
                disposition=disposition,
                reason=redact_text(order.error_message)[:2000] if order.error_message else None,
                updated_at=datetime.now(timezone.utc),
            )
        )

    async def warm_market_meta_cache(self, dexes: list[str] | tuple[str, ...] | set[str]) -> None:
        # Market capabilities are exchange-global, not account-specific. Use a
        # PostgreSQL-backed snapshot so the main and sub-account workers copy
        # the same data into their own in-process hot caches instead of each
        # pulling meta/perpDexs independently. Startup only; no fill waits on
        # this shared database cache in the normal path.
        for dex in dexes:
            try:
                meta, refreshed_at, asset_offset = await self._load_shared_market_meta(dex)
                self._install_market_meta(
                    dex,
                    meta,
                    refreshed_at=refreshed_at,
                    asset_offset=asset_offset,
                )
            except Exception as exc:
                log.warning(
                    "shared_market_meta_warmup_failed",
                    dex=str(dex or ""),
                    error=redact_text(exc)[:160],
                )
                try:
                    await self._get_market_meta(dex)
                except Exception as fallback_exc:
                    log.warning(
                        "market_meta_warmup_failed",
                        dex=str(dex or ""),
                        error=redact_text(fallback_exc)[:160],
                    )

    async def load_shared_perp_dex_directory(self) -> dict[str, int]:
        """Load one exchange-global DEX/asset-offset directory for all workers.

        The main/default worker owns periodic refreshes.  An explicit
        sub-account worker always reuses a valid snapshot, so adding another
        execution account does not duplicate Hyperliquid metadata traffic.
        This method is startup-only and is never awaited by a live fill.
        """

        network = str(self.settings.hyperliquid_execution_network or "mainnet").lower()
        setting_key = _shared_perp_dex_directory_setting_key(network=network)
        lock_key = f"{SHARED_PERP_DEX_DIRECTORY_LOCK_PREFIX}:{network}"
        ttl_seconds = max(
            int(getattr(self.settings, "hyperliquid_risk_settings_ttl_seconds", 300) or 300),
            30,
        )
        now = datetime.now(timezone.utc)
        async with self._shared_db_session_factory() as db:
            if isinstance(db, AsyncSession):
                await db.execute(text("SET LOCAL synchronous_commit = OFF"))
            await db.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                {"lock_key": lock_key},
            )
            row = await db.get(AppSetting, setting_key)
            payload = dict(row.value) if row is not None and isinstance(row.value, dict) else None
            parsed = _parse_shared_perp_dex_directory_payload(
                payload,
                expected_network=network,
            )
            explicit_route = bool(self.settings.low_latency_uses_explicit_leader_route())
            if parsed is not None:
                cached_offsets, cached_at = parsed
                if explicit_route or cached_at >= now - timedelta(seconds=ttl_seconds):
                    self._market_asset_offsets.update(cached_offsets)
                    self._perp_dex_directory_refreshed_at = cached_at
                    await db.commit()
                    return dict(cached_offsets)

            raw_directory = await self.info_client.post_info({"type": "perpDexs"})
            offsets = _perp_dex_asset_offsets_from_payload(raw_directory)
            if not offsets or "" not in offsets:
                raise RuntimeError("invalid Hyperliquid perp DEX directory")
            refreshed_at = datetime.now(timezone.utc)
            value = {
                "version": SHARED_PERP_DEX_DIRECTORY_CACHE_VERSION,
                "network": network,
                "refreshed_at": refreshed_at.isoformat(),
                "asset_offsets": offsets,
            }
            await db.execute(
                insert(AppSetting)
                .values(key=setting_key, value=value, updated_at=refreshed_at)
                .on_conflict_do_update(
                    index_elements=[AppSetting.key],
                    set_={"value": value, "updated_at": refreshed_at},
                )
            )
            await db.commit()
        self._market_asset_offsets.update(offsets)
        self._perp_dex_directory_refreshed_at = refreshed_at
        return dict(offsets)

    async def _load_shared_market_meta(
        self,
        dex: str,
    ) -> tuple[dict[str, Any], datetime, int]:
        dex_key = str(dex or "").lower()
        setting_key = _shared_market_meta_setting_key(
            network=self.settings.hyperliquid_execution_network,
            dex=dex_key,
        )
        lock_key = (
            f"{SHARED_MARKET_META_LOCK_PREFIX}:"
            f"{self.settings.hyperliquid_execution_network.lower()}:{dex_key or 'default'}"
        )
        ttl_seconds = max(
            int(getattr(self.settings, "hyperliquid_risk_settings_ttl_seconds", 300) or 300),
            30,
        )
        now = datetime.now(timezone.utc)
        async with self._shared_db_session_factory() as db:
            if isinstance(db, AsyncSession):
                await db.execute(text("SET LOCAL synchronous_commit = OFF"))
            await db.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                {"lock_key": lock_key},
            )
            row = await db.get(AppSetting, setting_key)
            payload = dict(row.value) if row is not None and isinstance(row.value, dict) else None
            parsed = _parse_shared_market_meta_payload(payload, expected_dex=dex_key)
            explicit_route = bool(self.settings.low_latency_uses_explicit_leader_route())
            if parsed is not None:
                cached_meta, cached_at, cached_offset = parsed
                # The default/main worker is the designated refresher. The
                # explicit sub-account worker always reuses an existing valid
                # snapshot; a genuinely new market still follows the existing
                # miss refresh path.
                if explicit_route or cached_at >= now - timedelta(seconds=ttl_seconds):
                    await db.commit()
                    return cached_meta, cached_at, cached_offset

            try:
                meta = await self.info_client.meta(dex_key)
            except TypeError:
                meta = await self.info_client.meta()
            meta = dict(meta or {})
            if not isinstance(meta.get("universe"), list):
                raise RuntimeError(f"invalid Hyperliquid market metadata for {dex_key or 'default'}")
            cached_offset = parsed[2] if parsed is not None else None
            asset_offset = (
                cached_offset
                if cached_offset is not None
                else await self._fetch_perp_dex_asset_offset(dex_key)
            )
            refreshed_at = datetime.now(timezone.utc)
            value = {
                "version": SHARED_MARKET_META_CACHE_VERSION,
                "network": self.settings.hyperliquid_execution_network.lower(),
                "dex": dex_key,
                "asset_offset": asset_offset,
                "refreshed_at": refreshed_at.isoformat(),
                "meta": meta,
            }
            stmt = (
                insert(AppSetting)
                .values(key=setting_key, value=value, updated_at=refreshed_at)
                .on_conflict_do_update(
                    index_elements=[AppSetting.key],
                    set_={"value": value, "updated_at": refreshed_at},
                )
            )
            await db.execute(stmt)
            await db.commit()
            return meta, refreshed_at, asset_offset

    async def _fetch_perp_dex_asset_offset(self, dex: str) -> int:
        dex_key = str(dex or "").lower()
        if not dex_key:
            return 0
        cached_offset = self._market_asset_offsets.get(dex_key)
        if cached_offset is not None:
            return int(cached_offset)
        payload = await self.info_client.post_info({"type": "perpDexs"})
        offsets = _perp_dex_asset_offsets_from_payload(payload)
        self._market_asset_offsets.update(offsets)
        if dex_key in offsets:
            return int(offsets[dex_key])
        raise RuntimeError(f"Hyperliquid perp dex offset unavailable for {dex_key}")

    def _install_market_meta(
        self,
        dex: str,
        meta: dict[str, Any],
        *,
        refreshed_at: datetime,
        asset_offset: int,
    ) -> None:
        dex_key = str(dex or "").lower()
        self._market_meta_cache[dex_key] = (refreshed_at, dict(meta))
        self._market_asset_offsets[dex_key] = int(asset_offset)
        self._prime_market_plan_cache_from_meta(dex_key, meta)
        prime_sdk_meta = getattr(self.execution_client, "prime_sdk_market_metadata", None)
        if callable(prime_sdk_meta):
            prime_sdk_meta(
                dex=dex_key,
                meta=meta,
                asset_offset=asset_offset,
            )

    async def _get_market_meta(self, dex: str, *, force_refresh: bool = False) -> dict[str, Any]:
        dex_key = str(dex or "").lower()
        now = datetime.now(timezone.utc)
        ttl_seconds = max(int(getattr(self.settings, "hyperliquid_risk_settings_ttl_seconds", 300) or 300), 30)
        cached = self._market_meta_cache.get(dex_key)
        observed_cached_at = cached[0] if cached is not None else None
        if cached is not None and not force_refresh:
            cached_at, meta = cached
            if cached_at >= now - timedelta(seconds=ttl_seconds):
                return meta
        refresh_lock = self._market_meta_refresh_locks.setdefault(dex_key, asyncio.Lock())
        async with refresh_lock:
            now = datetime.now(timezone.utc)
            cached = self._market_meta_cache.get(dex_key)
            if force_refresh:
                if cached is not None and (
                    observed_cached_at is None or cached[0] > observed_cached_at
                ):
                    return cached[1]
                cooldown_seconds = max(
                    0.0,
                    float(
                        getattr(
                            self.settings,
                            "market_meta_miss_refresh_cooldown_seconds",
                            0.25,
                        )
                        or 0
                    ),
                )
                last_force_refresh_at = self._market_meta_force_refresh_at.get(dex_key)
                if (
                    cached is not None
                    and last_force_refresh_at is not None
                    and last_force_refresh_at >= now - timedelta(seconds=cooldown_seconds)
                ):
                    return cached[1]
            elif cached is not None and cached[0] >= now - timedelta(seconds=ttl_seconds):
                return cached[1]
            try:
                meta = await self.info_client.meta(dex_key)
            except TypeError:
                meta = await self.info_client.meta()
            meta = dict(meta or {})
            refreshed_at = datetime.now(timezone.utc)
            asset_offset = self._market_asset_offsets.get(dex_key)
            prime_sdk_meta = getattr(self.execution_client, "prime_sdk_market_metadata", None)
            if asset_offset is None and callable(prime_sdk_meta):
                asset_offset = await self._fetch_perp_dex_asset_offset(dex_key)
            self._install_market_meta(
                dex_key,
                meta,
                refreshed_at=refreshed_at,
                asset_offset=asset_offset or 0,
            )
            if force_refresh:
                self._market_meta_force_refresh_at[dex_key] = refreshed_at
            return meta

    def _prime_market_plan_cache_from_meta(self, dex: str, meta: dict[str, Any]) -> None:
        for index, item in enumerate(meta.get("universe", []) or []):
            if not isinstance(item, dict):
                continue
            parsed = parse_coin(str(item.get("name", "")), default_dex=dex)
            if not parsed.coin:
                continue
            market_meta = {**item, "asset_id": index, "index": index}
            policy_leverage = _market_policy_effective_leverage(
                market_meta,
                canonical_coin_value=parsed.canonical_coin,
                configured_default_leverage=self.settings.hyperliquid_default_leverage,
            )
            plan = build_hyperliquid_leverage_plan(
                default_leverage=policy_leverage,
                coin_max_leverage=item.get("maxLeverage"),
                sz_decimals=item.get("szDecimals"),
                asset_id=index,
                market_meta=market_meta,
            )
            cache_key = (str(parsed.dex or "").lower(), str(parsed.canonical_coin or "").upper())
            self.market_leverage_plan_cache[cache_key] = plan
            if plan.asset_id is not None:
                self._asset_id_cache[cache_key] = plan.asset_id

    async def _hydrate_asset_id(self, fill: FillEvent) -> FillEvent:
        try:
            meta = await self._get_market_meta(fill.market.dex)
            asset_id = resolve_asset_id_from_meta(meta, coin=fill.market.coin, dex=fill.market.dex)
            if asset_id is None:
                meta = await self._get_market_meta(fill.market.dex, force_refresh=True)
                asset_id = resolve_asset_id_from_meta(meta, coin=fill.market.coin, dex=fill.market.dex)
        except Exception:
            asset_id = None
        if asset_id is not None:
            self._asset_id_cache[(str(fill.market.dex or "").lower(), str(fill.market.canonical_coin or "").upper())] = asset_id
        return self._fill_with_asset_id(fill, asset_id)

    async def _ensure_market_execution_metadata(self, market: MarketKey) -> dict[str, Any]:
        cache_key = (str(market.dex or "").lower(), str(market.canonical_coin or "").upper())
        cached_plan = self.market_leverage_plan_cache.get(cache_key)
        cached_market_meta = getattr(cached_plan, "market_meta", None) if cached_plan is not None else None
        retry_reason = self._market_metadata_retry_reason(market, cached_market_meta)
        if retry_reason is None:
            return dict(cached_market_meta)

        meta = await self._get_market_meta(market.dex)
        market_meta = self._market_meta_item(meta, market)
        retry_reason = self._market_metadata_retry_reason(market, market_meta)
        if retry_reason is not None:
            meta = await self._get_market_meta(market.dex, force_refresh=True)
            market_meta = self._market_meta_item(meta, market)
            retry_reason = self._market_metadata_retry_reason(market, market_meta)
        if retry_reason is not None:
            raise RetryableFillProcessingError(retry_reason)

        asset_id = int(market_meta["asset_id"])
        self._asset_id_cache[cache_key] = asset_id
        policy_leverage = _market_policy_effective_leverage(
            market_meta,
            canonical_coin_value=market.canonical_coin,
            configured_default_leverage=self.settings.hyperliquid_default_leverage,
        )
        plan = build_hyperliquid_leverage_plan(
            default_leverage=policy_leverage,
            coin_max_leverage=market_meta.get("maxLeverage"),
            sz_decimals=market_meta.get("szDecimals"),
            asset_id=asset_id,
            market_meta=market_meta,
        )
        self.market_leverage_plan_cache[cache_key] = plan
        return market_meta

    @staticmethod
    def _market_meta_item(meta: dict[str, Any], market: MarketKey) -> dict[str, Any] | None:
        target = parse_coin(market.canonical_coin, default_dex=market.dex)
        for index, item in enumerate(meta.get("universe", []) or []):
            if not isinstance(item, dict):
                continue
            parsed = parse_coin(str(item.get("name", "")), default_dex=market.dex)
            if parsed.canonical_coin == target.canonical_coin:
                return {**item, "asset_id": index, "index": index}
        return None

    @staticmethod
    def _market_metadata_retry_reason(
        market: MarketKey,
        market_meta: dict[str, Any] | None,
    ) -> str | None:
        if not market_meta:
            return f"market metadata unavailable for {market.canonical_coin}"
        meta_asset_id = _int_or_none(
            _first_present(
                market_meta.get("asset_id"),
                market_meta.get("assetId"),
                market_meta.get("index"),
            )
        )
        if market.asset_id is None or meta_asset_id is None:
            return f"market asset id unavailable for {market.canonical_coin}"
        if int(market.asset_id) != meta_asset_id:
            return f"market asset id mismatch for {market.canonical_coin}"
        sz_decimals = _int_or_none(market_meta.get("szDecimals"))
        if sz_decimals is None or sz_decimals < 0:
            return f"market size precision unavailable for {market.canonical_coin}"
        max_leverage = _int_or_none(market_meta.get("maxLeverage"))
        if max_leverage is None or max_leverage <= 0:
            return f"market max leverage unavailable for {market.canonical_coin}"
        return None

    async def _load_cached_asset_id(self, db: Any, market: MarketKey) -> int | None:
        cache_key = (str(market.dex or "").lower(), str(market.canonical_coin or "").upper())
        cached = self._asset_id_cache.get(cache_key)
        if cached is not None:
            return cached
        value = await db.scalar(
            select(MarketRiskSetting.asset_id)
            .where(MarketRiskSetting.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(MarketRiskSetting.dex == market.dex)
            .where(func.upper(MarketRiskSetting.canonical_coin) == str(market.canonical_coin).upper())
            .where(MarketRiskSetting.asset_id.is_not(None))
            .order_by(MarketRiskSetting.last_confirmed_at.desc().nulls_last(), MarketRiskSetting.updated_at.desc())
            .limit(1)
        )
        asset_id = _int_or_none(value)
        if asset_id is not None:
            self._asset_id_cache[cache_key] = asset_id
        return asset_id

    async def _load_hot_path_price(self, market: MarketKey) -> PriceEntry | None:
        return self.price_cache.get(market.canonical_coin)

    def _fill_with_asset_id(self, fill: FillEvent, asset_id: int | None) -> FillEvent:
        market = MarketKey(
            dex=fill.market.dex,
            coin=fill.market.coin,
            canonical_coin=fill.market.canonical_coin,
            raw_coin=fill.market.raw_coin,
            asset_id=asset_id,
            venue_symbol=fill.market.venue_symbol,
        )
        return replace(fill, market=market)

    async def _load_live_leader_position_snapshot(
        self,
        *,
        leader: LeaderConfig,
        market: MarketKey,
        price_entry: PriceEntry | None,
    ) -> tuple[bool, LiveLeaderPositionSnapshot | None, str | None]:
        try:
            state = await self.info_client.clearinghouse_state(leader.leader_address, dex=market.dex)
        except Exception as exc:
            log.warning(
                "live_leader_position_refresh_failed",
                leader_address=mask_address(leader.leader_address),
                dex=market.dex,
                canonical_coin=market.canonical_coin,
                error=redact_text(exc)[:160],
            )
            return False, None, str(exc)[:500]

        target = parse_coin(market.canonical_coin, default_dex=market.dex)
        for item in state.get("assetPositions") or []:
            position = item.get("position") if isinstance(item, dict) else None
            if not isinstance(position, dict):
                continue
            parsed = parse_coin(str(position.get("coin") or ""), default_dex=market.dex)
            if parsed.canonical_coin != target.canonical_coin:
                continue
            signed_size = _decimal_from_value(position.get("szi") or position.get("size")) or Decimal("0")
            if abs(signed_size) <= ALLOCATION_TRANSITION_TOLERANCE:
                return True, None, None
            raw_notional = _decimal_from_value(position.get("positionValue") or position.get("notional")) or Decimal("0")
            notional = abs(raw_notional) if signed_size > 0 else -abs(raw_notional)
            mark_px = (
                _decimal_from_value(position.get("markPx"))
                or _decimal_from_value(position.get("midPx"))
                or (price_entry.price if price_entry is not None else None)
            )
            if notional == 0 and mark_px is not None and mark_px > 0:
                notional = signed_size * mark_px
            side = PositionSide.LONG if signed_size > 0 else PositionSide.SHORT
            return (
                True,
                LiveLeaderPositionSnapshot(
                    side=side,
                    size=abs(signed_size),
                    signed_size=signed_size,
                    notional=notional,
                    entry_px=_decimal_from_value(position.get("entryPx")),
                    mark_px=mark_px,
                    raw_payload_masked=_json_safe(position),
                    last_update_at=datetime.now(timezone.utc),
                ),
                None,
            )
        return True, None, None

    async def _mark_current_position_wait_until_flat(
        self,
        db: Any,
        *,
        leader: LeaderConfig,
        leader_state: LatestAccountState | None,
        leader_position: LatestAccountPosition,
        market: MarketKey,
        reason: str,
    ) -> None:
        baseline = await db.scalar(
            select(LeaderPositionBaseline)
            .where(LeaderPositionBaseline.leader_id == leader.id)
            .where(LeaderPositionBaseline.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(LeaderPositionBaseline.dex == market.dex)
            .where(func.upper(LeaderPositionBaseline.canonical_coin) == str(market.canonical_coin).upper())
            .limit(1)
        )
        now = datetime.now(timezone.utc)
        if baseline is None:
            baseline = LeaderPositionBaseline(
                leader_id=leader.id,
                leader_address=normalize_leader_address(leader.leader_address),
                execution_venue=ExecutionVenue.HYPERLIQUID.value,
                dex=market.dex,
                canonical_coin=market.canonical_coin,
            )
            db.add(baseline)
            await db.flush()
        baseline.side_at_enable = leader_position.side
        baseline.size_at_enable = abs(Decimal(leader_position.size or 0))
        baseline.notional_at_enable = abs(Decimal(leader_position.notional or 0))
        baseline.account_value_at_enable = leader_state.account_value if leader_state else None
        baseline.entry_px_at_enable = leader_position.entry_px
        baseline.mark_px_at_enable = leader_position.mark_px
        baseline.baseline_status = BASELINE_WAIT_UNTIL_FLAT
        baseline.first_seen_at = baseline.first_seen_at or now
        baseline.flat_confirmed_at = None
        baseline.copy_allowed_at = None
        baseline.last_leader_size = abs(Decimal(leader_position.size or 0))
        baseline.last_leader_notional = abs(Decimal(leader_position.notional or 0))
        baseline.last_leader_entry_px = leader_position.entry_px
        baseline.last_leader_mark_px = leader_position.mark_px
        baseline.last_checked_at = now
        baseline.reason = reason[:1000]
        await db.flush()

    async def _load_market_leverage_plan(
        self,
        db: Any,
        market: MarketKey,
        leader_position: LatestAccountPosition | None,
        *,
        reduce_only: bool = False,
    ) -> Any:
        cache_key = (str(market.dex or "").lower(), str(market.canonical_coin or "").upper())
        cached = self.market_leverage_plan_cache.get(cache_key)
        # A reduce-only order needs the cached asset/size precision but cannot
        # increase margin exposure. Avoid reloading per-market leverage for the
        # dominant reduction path; missing metadata still falls through to the
        # authoritative loader below.
        if (
            reduce_only
            and cached is not None
            and cached.max_leverage is not None
            and cached.sz_decimals is not None
        ):
            return cached
        if (
            cached is not None
            and cached.max_leverage is not None
            and cached.sz_decimals is not None
            and cached.effective_leverage
            == _market_policy_effective_leverage(
                cached.market_meta,
                canonical_coin_value=market.canonical_coin,
                configured_default_leverage=self.settings.hyperliquid_default_leverage,
            )
        ):
            return cached
        # Risk-increasing orders require current capability metadata. The full
        # universe is prewarmed at startup, so this is an in-memory hit in the
        # normal path; only a genuinely new/unseen market performs the existing
        # metadata refresh.
        market_meta = await self._ensure_market_execution_metadata(market)
        policy_leverage = _market_policy_effective_leverage(
            market_meta,
            canonical_coin_value=market.canonical_coin,
            configured_default_leverage=self.settings.hyperliquid_default_leverage,
        )
        meta_plan = build_hyperliquid_leverage_plan(
            default_leverage=policy_leverage,
            coin_max_leverage=market_meta.get("maxLeverage"),
            sz_decimals=market_meta.get("szDecimals"),
            asset_id=market_meta.get("asset_id"),
            market_meta=market_meta,
        )
        self.market_leverage_plan_cache[cache_key] = meta_plan
        return meta_plan

    def _schedule_state_refresh_if_stale(self) -> None:
        if self._state_refresh_task is not None and not self._state_refresh_task.done():
            return
        self._state_refresh_task = asyncio.create_task(
            schedule_account_state_refresh_if_stale(self.settings)
        )

    def _schedule_account_abstraction_refresh(self, *, role: str, address: str, dex: str) -> None:
        key = (str(role or "").upper(), str(address or "").lower(), str(dex or "").lower())
        existing = self._account_abstraction_refresh_tasks.get(key)
        if existing is not None and not existing.done():
            return
        self._account_abstraction_refresh_tasks[key] = asyncio.create_task(
            self._refresh_account_abstraction_with_new_session(role=role, address=address, extra_dex=dex)
        )

    async def _refresh_account_abstraction_with_new_session(self, *, role: str, address: str, extra_dex: str) -> None:
        try:
            async with SessionLocal() as db:
                await self._refresh_account_abstraction(db, role, address, extra_dex=extra_dex)
                await db.commit()
        except Exception as exc:
            log.warning(
                "account_abstraction_background_refresh_failed",
                role=role,
                address=address,
                dex=extra_dex,
                error=redact_text(exc)[:160],
            )

    async def _refresh_account_abstraction(self, db: Any, role: str, address: str, *, extra_dex: str | None = None) -> None:
        refresh_key = (str(role or "").upper(), str(address or "").lower())
        refresh_lock = self._account_abstraction_refresh_locks.setdefault(
            refresh_key,
            asyncio.Lock(),
        )
        async with refresh_lock:
            dexes = [dex.dex_name for dex in HyperliquidDexRegistry(self.settings).enabled_dexes()]
            normalized_extra = str(extra_dex or "").lower()
            if normalized_extra not in dexes:
                dexes.append(normalized_extra)
            cached_entry = self._account_abstraction_cache.get(
                account_abstraction_setting_key(role, address)
            )
            cached_payload = cached_entry[1] if cached_entry is not None else {}
            cached_mode = str(
                cached_payload.get("account_abstraction_mode")
                or cached_payload.get("mode")
                or ""
            ).upper()
            confirmed_unified = bool(
                cached_mode == MODE_UNIFIED
                and (
                    cached_payload.get("user_abstraction_available")
                    or cached_payload.get("userAbstractionAvailable")
                )
            )
            now = datetime.now(timezone.utc)
            last_full_refresh = self._account_abstraction_full_refresh_at.get(refresh_key)
            full_refresh_interval = max(
                30.0,
                float(
                    getattr(self.settings, "account_value_full_refresh_seconds", 300.0)
                    or 300.0
                ),
            )
            use_confirmed_unified_fast = bool(
                confirmed_unified
                and last_full_refresh is not None
                and last_full_refresh >= now - timedelta(seconds=full_refresh_interval)
            )
            snapshot = await AccountAbstractionService(
                self.account_info_client,
                self.settings,
            ).fetch_snapshot(
                role=role,
                address=address,
                dexes=dexes,
                confirmed_unified_fast=use_confirmed_unified_fast,
            )
            resolved_by_dex = {
                dex: resolve_account_value_for_sizing(snapshot, dex, self.settings)
                for dex in dexes
            }
            cached_usable = _account_abstraction_payload_has_usable_value(
                cached_payload,
                dexes=dexes,
            )
            refreshed_usable = any(
                result.account_value is not None
                and result.account_value > 0
                and not result.blockers
                for result in resolved_by_dex.values()
            )
            if snapshot.error_message and cached_usable and not refreshed_usable:
                # A transient info/API failure must never replace a usable
                # sizing value with an unavailable one. Keep the last known
                # good snapshot; the independent background loop will retry.
                raise RuntimeError(
                    "account value refresh returned no usable value; preserving last known good snapshot"
                )
            payload = await save_account_abstraction_state(
                db,
                snapshot=snapshot,
                resolved_by_dex=resolved_by_dex,
            )
            if not use_confirmed_unified_fast:
                self._account_abstraction_full_refresh_at[refresh_key] = datetime.now(timezone.utc)
            # One dictionary assignment atomically replaces the prior snapshot
            # for every fill coroutine running on this event loop.
            self._cache_account_abstraction_payload(snapshot.role, snapshot.address, payload)

    def _cache_account_abstraction_payload(
        self,
        role: str,
        address: str,
        payload: dict[str, Any] | None,
        *,
        cached_at: datetime | None = None,
    ) -> None:
        if not payload:
            return
        key = account_abstraction_setting_key(role, address)
        self._account_abstraction_cache[key] = (cached_at or datetime.now(timezone.utc), dict(payload))

    def cache_account_abstraction_payloads(self, payloads: dict[str, dict[str, Any]]) -> None:
        now = datetime.now(timezone.utc)
        for key, payload in payloads.items():
            if isinstance(payload, dict):
                self._account_abstraction_cache[str(key)] = (now, dict(payload))

    def _cached_account_abstraction_payload(self, key: str, *, now: datetime | None = None) -> dict[str, Any] | None:
        cached = self._account_abstraction_cache.get(key)
        if cached is None:
            return None
        _, payload = cached
        # The fill path always keeps the last known good snapshot in memory.
        # Freshness is maintained by the independent poller, not by expiring
        # this entry and forcing a database/REST read on an arriving fill.
        return dict(payload)

    async def _resolved_account_values(
        self,
        db: Any,
        *,
        leader_address: str,
        dex: str,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        follower_address = self.settings.hyperliquid_follower_account_address()
        requests = [
            (FOLLOWER, follower_address),
            (LEADER, leader_address),
        ]
        keys = {
            account_abstraction_setting_key(role, address): (role, address)
            for role, address in requests
            if address
        }
        if not keys:
            return None, None
        now = datetime.now(timezone.utc)
        payloads: dict[str, dict[str, Any]] = {}
        missing_keys: list[str] = []
        for key in keys:
            payload = self._cached_account_abstraction_payload(key, now=now)
            if payload is None:
                missing_keys.append(key)
            else:
                payloads[key] = payload
        if missing_keys:
            rows = (
                await db.execute(
                    select(AppSetting)
                    .where(AppSetting.key.in_(missing_keys))
                )
            ).scalars().all()
            for row in rows:
                if isinstance(row.value, dict):
                    payload = dict(row.value)
                    payloads[row.key] = payload
                    self._account_abstraction_cache[row.key] = (now, payload)

        def resolved(role: str, address: str | None) -> dict[str, Any] | None:
            if not address:
                return None
            key = account_abstraction_setting_key(role, address)
            payload = payloads.get(key)
            if payload is None:
                self._schedule_state_refresh_if_stale()
                return None
            value = _resolved_account_value_payload(payload, dex)
            if _account_value_payload_needs_refresh(value):
                self._schedule_state_refresh_if_stale()
                self._schedule_account_abstraction_refresh(role=role, address=address, dex=dex)
            return value

        return resolved(FOLLOWER, follower_address), resolved(LEADER, leader_address)

    async def _resolved_account_value(self, db: Any, role: str, address: str | None, dex: str) -> dict[str, Any] | None:
        if not address:
            return None
        key = account_abstraction_setting_key(role, address)
        payload = self._cached_account_abstraction_payload(key)
        if payload is None:
            payload = await load_account_abstraction_state(db, role=role, address=address)
            if payload is not None:
                self._cache_account_abstraction_payload(role, address, payload)
        if payload is None:
            self._schedule_state_refresh_if_stale()
            return None
        result = _resolved_account_value_payload(payload, dex)
        if _account_value_payload_needs_refresh(result):
            self._schedule_state_refresh_if_stale()
            self._schedule_account_abstraction_refresh(role=role, address=address, dex=dex)
        return result

    async def _load_allocation(
        self,
        db: Any,
        leader: LeaderConfig,
        market: MarketKey,
        side: PositionSide | None,
    ) -> LeaderPositionAllocationRecord | None:
        stmt = (
            select(LeaderPositionAllocationRecord)
            .where(LeaderPositionAllocationRecord.leader_address == leader.leader_address.lower())
            .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
            .where(LeaderPositionAllocationRecord.dex == market.dex)
            .where(func.upper(LeaderPositionAllocationRecord.canonical_coin) == str(market.canonical_coin).upper())
            .where(LeaderPositionAllocationRecord.status != "CLOSED")
        )
        if side is not None and side != PositionSide.FLAT:
            stmt = stmt.where(LeaderPositionAllocationRecord.position_side == side.value)
        return await db.scalar(
            stmt.order_by(LeaderPositionAllocationRecord.updated_at.desc())
            .with_for_update()
            .execution_options(populate_existing=True)
            .limit(1)
        )

    async def _load_market_owner_allocation(
        self,
        db: Any,
        market: MarketKey,
    ) -> LeaderPositionAllocationRecord | None:
        rows = (
            await db.execute(
                select(LeaderPositionAllocationRecord)
                .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
                .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
                .where(LeaderPositionAllocationRecord.dex == market.dex)
                .where(func.upper(LeaderPositionAllocationRecord.canonical_coin) == str(market.canonical_coin).upper())
                .where(LeaderPositionAllocationRecord.status != "CLOSED")
                .where(LeaderConfig.enabled.is_(True))
                .where(LeaderConfig.deleted_at.is_(None))
                .order_by(LeaderPositionAllocationRecord.created_at.asc(), LeaderPositionAllocationRecord.id.asc())
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
        for allocation in rows:
            if _allocation_market_owner_active(allocation, self.pending_intents):
                return allocation
            if await self._allocation_has_durable_unresolved_order(db, allocation):
                # In-memory pending intents are intentionally only an acceleration
                # layer.  After a restart, the durable outbox remains authoritative:
                # a zero-fill OPEN allocation with an UNKNOWN/SUBMITTING order still
                # owns the market until recovery proves whether it filled.
                return allocation
        return None

    async def _peek_allocation_for_lifecycle_gate(
        self,
        db: Any,
        leader: LeaderConfig,
        market: MarketKey,
    ) -> LeaderPositionAllocationRecord | None:
        """Read lifecycle presence without holding an allocation row lock.

        The market transaction lock already serializes competing planners.  This
        lightweight read exists only to discard old non-owner lifecycle fills
        before hot-path dependencies; the authoritative allocation is still
        loaded with ``FOR UPDATE`` later for every actionable fill.
        """
        return await db.scalar(
            select(LeaderPositionAllocationRecord)
            .where(LeaderPositionAllocationRecord.leader_address == leader.leader_address.lower())
            .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
            .where(LeaderPositionAllocationRecord.dex == market.dex)
            .where(
                func.upper(LeaderPositionAllocationRecord.canonical_coin)
                == str(market.canonical_coin).upper()
            )
            .order_by(
                case((LeaderPositionAllocationRecord.status != "CLOSED", 0), else_=1),
                LeaderPositionAllocationRecord.updated_at.desc(),
                LeaderPositionAllocationRecord.id.desc(),
            )
            .execution_options(populate_existing=True)
            .limit(1)
        )

    async def _allocation_has_durable_unresolved_order(
        self,
        db: Any,
        allocation: LeaderPositionAllocationRecord | None,
    ) -> bool:
        allocation_id = _int_or_none(getattr(allocation, "id", None))
        if allocation_id is None:
            return False
        if self.pending_intents.has_pending_allocation(allocation):
            return True
        return bool(
            await db.scalar(
                select(ExecutionOrder.id)
                .where(ExecutionOrder.source_type == "AUTO_COPY")
                .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(ExecutionOrder.venue_account == self.execution_scope)
                .where(ExecutionOrder.allocation_id == allocation_id)
                .where(ExecutionOrder.status.in_(RECOVERY_ORDER_STATUSES))
                .limit(1)
            )
        )

    async def _market_owner_handoff_pending(
        self,
        db: Any,
        owner_allocation: LeaderPositionAllocationRecord | None,
    ) -> bool:
        if owner_allocation is None:
            return False
        # A leader-flat close intent is a release in progress.  The next leader
        # must wait for the follower close confirmation, not be permanently
        # recorded as BLOCKED and not submit concurrently with that close.
        if _allocation_has_flat_leader_close_intent(owner_allocation):
            return True
        if (
            str(
                getattr(owner_allocation, "pending_reduce_reason", "") or ""
            )
            == MINIMUM_RESIDUAL_ECONOMIC_FLAT_PENDING_REASON
        ):
            # The leader intentionally remains non-flat at dust size, but the
            # follower close is a full lifecycle release. Keep the next
            # leader's fill durable until that close reaches a terminal state.
            return await self._allocation_has_durable_unresolved_order(
                db,
                owner_allocation,
            )
        # Initial ownership is also provisional while its first exchange action
        # is unresolved.  Waiting here prevents a restart from letting another
        # leader open the same market before UNKNOWN recovery completes.
        if not _allocation_active(owner_allocation):
            return await self._allocation_has_durable_unresolved_order(db, owner_allocation)
        return False

    async def _allocation_sum_qty(self, db: Any, market: MarketKey, side: PositionSide) -> Decimal:
        qty, _latest_reconcile_at = await self._allocation_sum_qty_with_latest_reconcile(db, market, side)
        return qty

    async def _allocation_sum_qty_with_latest_reconcile(
        self,
        db: Any,
        market: MarketKey,
        side: PositionSide,
    ) -> tuple[Decimal, datetime | None]:
        recent_closed_cutoff = datetime.now(timezone.utc) - RECENT_CLOSED_ALLOCATION_STATE_LAG_WINDOW
        rows = (
            await db.execute(
                select(LeaderPositionAllocationRecord)
                .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
                .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
                .where(LeaderPositionAllocationRecord.dex == market.dex)
                .where(func.upper(LeaderPositionAllocationRecord.canonical_coin) == str(market.canonical_coin).upper())
                .where(LeaderPositionAllocationRecord.position_side == side.value)
                .where(
                    or_(
                        LeaderPositionAllocationRecord.status != "CLOSED",
                        LeaderPositionAllocationRecord.last_reconcile_at >= recent_closed_cutoff,
                    )
                )
                .where(LeaderConfig.enabled.is_(True))
                .where(LeaderConfig.deleted_at.is_(None))
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
        active_rows = [row for row in rows if not _allocation_row_closed(row)]
        latest_reconcile_at = _latest_allocation_reconcile_at(rows)
        return (
            sum((Decimal(row.allocated_qty or 0) for row in active_rows), Decimal("0")),
            latest_reconcile_at,
        )

    async def _allocation_sum_qtys_with_latest_reconcile(
        self,
        db: Any,
        market: MarketKey,
    ) -> tuple[dict[PositionSide, Decimal], datetime | None]:
        recent_closed_cutoff = datetime.now(timezone.utc) - RECENT_CLOSED_ALLOCATION_STATE_LAG_WINDOW
        rows = (
            await db.execute(
                select(LeaderPositionAllocationRecord)
                .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
                .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
                .where(LeaderPositionAllocationRecord.dex == market.dex)
                .where(func.upper(LeaderPositionAllocationRecord.canonical_coin) == str(market.canonical_coin).upper())
                .where(
                    or_(
                        LeaderPositionAllocationRecord.status != "CLOSED",
                        LeaderPositionAllocationRecord.last_reconcile_at >= recent_closed_cutoff,
                    )
                )
                .where(LeaderConfig.enabled.is_(True))
                .where(LeaderConfig.deleted_at.is_(None))
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
        qtys = _empty_position_side_qtys()
        latest_reconcile_at = _latest_allocation_reconcile_at(rows)
        for row in rows:
            if _allocation_row_closed(row):
                continue
            side = _position_side_or_none(row.position_side)
            if side is None:
                continue
            qtys[side] += abs(Decimal(row.allocated_qty or 0))
        return qtys, latest_reconcile_at

    async def _follower_position_qty(self, db: Any, market: MarketKey, side: PositionSide) -> Decimal:
        qty, _state_at = await self._follower_position_qty_with_state_at(db, market, side)
        return qty

    async def _follower_position_qty_with_state_at(
        self,
        db: Any,
        market: MarketKey,
        side: PositionSide,
    ) -> tuple[Decimal, datetime | None]:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return Decimal("0"), None
        state = await db.scalar(
            select(LatestAccountState)
            .where(LatestAccountState.role == FOLLOWER)
            .where(LatestAccountState.address == follower.lower())
            .where(LatestAccountState.dex == market.dex)
            .limit(1)
        )
        if state is None:
            return Decimal("0"), None
        positions = (
            await db.execute(
                select(LatestAccountPosition)
                .where(LatestAccountPosition.account_state_id == state.id)
                .where(func.upper(LatestAccountPosition.canonical_coin) == str(market.canonical_coin).upper())
                .where(LatestAccountPosition.side == side.value)
                .where(LatestAccountPosition.active.is_(True))
            )
        ).scalars().all()
        position = _preferred_actual_position(positions)
        qty = abs(Decimal(position.size or 0)) if position is not None else Decimal("0")
        return qty, _latest_datetime(
            _state_updated_at(state),
            _datetime_or_none(getattr(position, "last_update_at", None)) if position is not None else None,
        )

    async def _follower_position_qtys_with_state_at(
        self,
        db: Any,
        market: MarketKey,
    ) -> tuple[dict[PositionSide, Decimal], datetime | None]:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return _empty_position_side_qtys(), None
        if isinstance(db, AsyncSession):
            # Load the state timestamp and both possible position sides in one
            # round trip.  Keeping the outer join is important: a genuinely
            # flat market still needs the fresh account-state timestamp.
            state_id = (
                select(LatestAccountState.id)
                .where(LatestAccountState.role == FOLLOWER)
                .where(LatestAccountState.address == follower.lower())
                .where(LatestAccountState.dex == market.dex)
                .order_by(
                    LatestAccountState.last_update_at.desc().nulls_last(),
                    LatestAccountState.id.desc(),
                )
                .limit(1)
                .scalar_subquery()
            )
            rows = (
                await db.execute(
                    select(LatestAccountState, LatestAccountPosition)
                    .outerjoin(
                        LatestAccountPosition,
                        (LatestAccountPosition.account_state_id == LatestAccountState.id)
                        & (
                            func.upper(LatestAccountPosition.canonical_coin)
                            == str(market.canonical_coin).upper()
                        )
                        & (LatestAccountPosition.side.in_(["LONG", "SHORT"]))
                        & (LatestAccountPosition.active.is_(True)),
                    )
                    .where(LatestAccountState.id == state_id)
                )
            ).all()
            if not rows:
                return _empty_position_side_qtys(), None
            state = rows[0][0]
            positions = [position for _state, position in rows if position is not None]
            return self._position_qtys_and_latest_state_at(state, positions)
        state = await db.scalar(
            select(LatestAccountState)
            .where(LatestAccountState.role == FOLLOWER)
            .where(LatestAccountState.address == follower.lower())
            .where(LatestAccountState.dex == market.dex)
            .limit(1)
        )
        if state is None:
            return _empty_position_side_qtys(), None
        positions = (
            await db.execute(
                select(LatestAccountPosition)
                .where(LatestAccountPosition.account_state_id == state.id)
                .where(func.upper(LatestAccountPosition.canonical_coin) == str(market.canonical_coin).upper())
                .where(LatestAccountPosition.side.in_(["LONG", "SHORT"]))
                .where(LatestAccountPosition.active.is_(True))
            )
        ).scalars().all()
        return self._position_qtys_and_latest_state_at(state, positions)

    @staticmethod
    def _position_qtys_and_latest_state_at(
        state: LatestAccountState,
        positions: list[LatestAccountPosition],
    ) -> tuple[dict[PositionSide, Decimal], datetime | None]:
        positions_by_side: dict[PositionSide, list[LatestAccountPosition]] = {
            PositionSide.LONG: [],
            PositionSide.SHORT: [],
        }
        for position in positions:
            side = _position_side_or_none(position.side)
            if side is None:
                continue
            positions_by_side[side].append(position)
        qtys = _empty_position_side_qtys()
        latest_position_at: datetime | None = None
        for side, side_positions in positions_by_side.items():
            position = _preferred_actual_position(side_positions)
            if position is None:
                continue
            qtys[side] += abs(Decimal(position.size or 0))
            latest_position_at = _latest_datetime(
                latest_position_at,
                _datetime_or_none(getattr(position, "last_update_at", None)),
            )
        return qtys, _latest_datetime(_state_updated_at(state), latest_position_at)

    async def _opposite_aggregate_allocation_exists(
        self,
        db: Any,
        leader: LeaderConfig,
        market: MarketKey,
        new_side: PositionSide,
    ) -> bool:
        opposite = _opposite_side(new_side)
        row = await db.scalar(
            select(LeaderPositionAllocationRecord)
            .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
            .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
            .where(LeaderPositionAllocationRecord.dex == market.dex)
            .where(func.upper(LeaderPositionAllocationRecord.canonical_coin) == str(market.canonical_coin).upper())
            .where(LeaderPositionAllocationRecord.position_side == opposite.value)
            .where(LeaderPositionAllocationRecord.status != "CLOSED")
            .where(LeaderPositionAllocationRecord.leader_id != leader.id)
            .where(LeaderPositionAllocationRecord.allocated_qty > Decimal("0"))
            .where(LeaderConfig.enabled.is_(True))
            .where(LeaderConfig.deleted_at.is_(None))
            .execution_options(populate_existing=True)
            .limit(1)
        )
        return row is not None

    async def _kill_switch_active(self, db: Any) -> bool:
        if isinstance(db, AsyncSession):
            row = await db.scalar(
                select(AppSetting)
                .where(AppSetting.key == "risk")
                .execution_options(populate_existing=True)
                .limit(1)
            )
        else:
            row = await db.get(AppSetting, "risk")
        if row is None:
            return True
        return bool((row.value or {}).get("kill_switch", True))


@dataclass
class LowLatencyRuntimeState:
    websocket_connected: bool = False
    market_ws_connected: bool = False
    leader_fills_ws_connected: bool = False
    low_latency_primary: bool = False
    low_latency_ready: bool = False
    active_leaders: dict[str, LeaderConfig] = field(default_factory=dict)
    ws_leaders: set[str] = field(default_factory=set)
    poll_fallback_leaders: set[str] = field(default_factory=set)
    subscribed_leaders_by_dex: dict[str, int] = field(default_factory=dict)
    last_event_time_by_dex: dict[str, str | None] = field(default_factory=dict)
    active_allocation_dexes: set[str] = field(default_factory=set)
    follower_order_updates_subscribed: bool = False
    follower_user_events_subscribed: bool = False
    follower_user_fills_subscribed: bool = False
    follower_clearinghouse_subscribed: bool = False
    follower_all_dexs_clearinghouse_subscribed: bool = False
    leader_user_fills_subscribed_count: int = 0
    last_ws_event_at: datetime | None = None
    last_allocation_sync_at: datetime | None = None
    allocation_sync_count: int = 0
    allocation_sync_skipped_count: int = 0
    allocation_sync_last_error: str | None = None
    account_value_refresh_count: int = 0
    account_value_last_refresh_at: datetime | None = None
    account_value_last_refresh_duration_ms: int | None = None
    account_value_refresh_last_error: str | None = None
    event_loop_lag_ms: int = 0
    max_event_loop_lag_ms: int = 0
    durable_replay_scan_count: int = 0
    durable_order_resume_scan_count: int = 0
    durable_replay_idle_wait_count: int = 0
    background_cycles_deferred_for_hot_path: int = 0
    reconnect_count: int = 0
    last_error: str | None = None


class HyperliquidLowLatencyWatcher:
    def __init__(
        self,
        *,
        settings: Settings,
        info_client: HyperliquidInfoClient,
        account_info_client: HyperliquidInfoClient | None = None,
        execution_client: HyperliquidExecutionClient,
        db_session_factory: Any,
        submit_db_session_factory: Any | None = None,
        price_cache: LowLatencyPriceCache | None = None,
    ) -> None:
        self.settings = settings
        execution_scope = getattr(settings, "low_latency_execution_scope", None)
        self.execution_scope = (
            str(execution_scope() or "").lower()
            if callable(execution_scope)
            else ""
        )
        self.info_client = info_client
        self.account_info_client = account_info_client or info_client
        self.execution_client = execution_client
        self.db_session_factory = db_session_factory
        self.submit_db_session_factory = submit_db_session_factory or db_session_factory
        self._submit_database_pool_isolated = submit_db_session_factory is not None
        self.price_cache = price_cache or LowLatencyPriceCache(stale_ms=settings.price_cache_stale_ms)
        self.manual_position_guard = FollowerManualPositionGuard()
        self.state = LowLatencyRuntimeState()
        self._follower_position_stream_observed_at: dict[str, datetime] = {}
        self.engine = FillDrivenExecutionEngine(
            settings=settings,
            info_client=info_client,
            account_info_client=self.account_info_client,
            execution_client=execution_client,
            price_cache=self.price_cache,
            manual_position_guard=self.manual_position_guard,
            follower_position_stream_trusted=self._follower_position_stream_is_trusted,
            shared_db_session_factory=db_session_factory,
        )
        self._stopped = asyncio.Event()
        self._leader_lock = asyncio.Lock()
        self._subscribed: set[str] = set()
        self._leader_fill_subscription_event = asyncio.Event()
        self._account_abstraction_refresh_event = asyncio.Event()
        self._leader_fill_backfill_start_ms: int | None = None
        self._leader_fill_backfill_lock = asyncio.Lock()
        self._leader_fill_startup_backfill_done = False
        self._leader_snapshot_backfilled_through_ms: dict[str, int] = {}
        self._fill_ingress_guard = asyncio.Lock()
        self._fill_ingress_locks: dict[tuple[str, str, str], asyncio.Lock] = {}
        self._fill_queue_guard = asyncio.Lock()
        self._fill_queues: dict[
            tuple[str, str, str],
            asyncio.Queue[tuple[FillEvent, LeaderConfig, bool]],
        ] = {}
        self._fill_workers: dict[tuple[str, str, str], asyncio.Task] = {}
        self._queued_source_fill_ids: set[str] = set()
        self._submit_queue_guard = asyncio.Lock()
        self._submit_queues: dict[str, asyncio.Queue[tuple[int, FillEvent]]] = {}
        self._submit_workers: dict[str, asyncio.Task] = {}
        self._queued_submit_order_ids: set[int] = set()
        # A freshly committed plan is already the authoritative payload the
        # submit worker needs. Hand the detached ORM object across in memory so
        # the normal live path does not re-read a ~7KB order row before its
        # final durable CAS. Recovery/retry paths intentionally fall back to an
        # authoritative database load when only a durable order id is available.
        self._committed_submit_orders: dict[int, ExecutionOrder] = {}
        self._submit_plan_committed_at: dict[int, datetime] = {}
        self._submit_retry_counts: dict[int, int] = {}
        self._submit_retry_not_before: dict[int, float] = {}
        self._recent_submit_latency_samples: list[dict[str, Any]] = []
        self._durable_replay_wakeup = asyncio.Event()
        self._durable_replay_wakeup_handle: asyncio.TimerHandle | None = None
        self._durable_replay_wakeup_at: float | None = None
        self._suppressed_source_fill_guard = asyncio.Lock()
        self._suppressed_source_fill_ids: OrderedDict[str, datetime] = OrderedDict()
        self._recently_completed_source_fill_ids: OrderedDict[str, datetime] = OrderedDict()
        self._background_tasks: set[asyncio.Task] = set()
        self._follower_state_refresh_pending_dexes: set[str] = set()
        self._follower_state_refresh_reasons: dict[str, str] = {}
        self._follower_state_refresh_tasks: dict[str, asyncio.Task] = {}
        self._follower_rest_refresh_failures: dict[str, int] = {}
        self._follower_rest_refresh_next_at: dict[str, float] = {}
        self._liveness_stuck_signature: str | None = None
        self._writer_lease_connection: Any | None = None
        self._writer_lease_key = _writer_lease_key(
            self.settings.hyperliquid_follower_account_address() or "default"
        )

    async def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        while not self._stopped.is_set() and not await self._acquire_writer_lease():
            await self._store_standby_status()
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
        if self._stopped.is_set():
            return
        await self._enable_discovered_perp_dexes()
        await self._store_starting_status()
        await self.refresh_leaders()
        # A persisted last-known-good value is sufficient to start ingestion.
        # Do not hold websocket subscription/backfill behind a balance REST
        # request merely because that value is old. If no valid value has ever
        # been captured, one blocking refresh is unavoidable because the
        # configured sizing formula cannot otherwise be evaluated.
        cached_account_value_ready = await self._load_follower_account_abstraction_cache()
        if cached_account_value_ready:
            self._account_abstraction_refresh_event.set()
        else:
            await self._refresh_follower_account_abstraction_once()
            self._account_abstraction_refresh_event.clear()
        await self._restore_persistent_manual_position_guards()
        await self._refresh_active_follower_positions_once(reason="STARTUP_ACTIVE_REFRESH")
        await self._replay_unprocessed_fills_once()
        await self._resume_unstarted_orders_once()
        self._schedule_background_task(self._warm_latency_caches())
        tasks = [
            asyncio.create_task(self._leader_fill_ws_loop()),
            asyncio.create_task(self._ws_loop()),
            asyncio.create_task(self._leader_refresh_loop()),
            asyncio.create_task(self._price_poll_loop()),
            asyncio.create_task(self._active_follower_position_refresh_loop()),
            asyncio.create_task(self._account_abstraction_refresh_loop()),
            asyncio.create_task(self._allocation_sync_loop()),
            asyncio.create_task(self._durable_replay_loop()),
            asyncio.create_task(self._leader_fill_backfill_retry_loop()),
            asyncio.create_task(self._writer_lease_watch_loop()),
            asyncio.create_task(self._event_loop_lag_watch_loop()),
            asyncio.create_task(self._status_loop()),
        ]
        try:
            await self._stopped.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._cancel_background_tasks()
            await self._cancel_fill_workers()
            await self._cancel_submit_workers()
            await self._release_writer_lease()

    async def _enable_discovered_perp_dexes(self) -> list[str]:
        configured = self.settings.enabled_hyperliquid_dex_list()
        if not bool(
            getattr(self.settings, "enable_all_hyperliquid_perp_dexes", True)
        ):
            return configured
        try:
            offsets = await asyncio.wait_for(
                self.engine.load_shared_perp_dex_directory(),
                timeout=max(
                    0.25,
                    float(
                        getattr(
                            self.settings,
                            "hyperliquid_perp_dex_discovery_timeout_seconds",
                            3.0,
                        )
                        or 3.0
                    ),
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The checked-in/current environment list remains a safe startup
            # fallback.  Discovery is an availability enhancement and must not
            # prevent websocket ingestion or durable replay after an API outage.
            log.warning(
                "hyperliquid_perp_dex_directory_discovery_failed",
                error=redact_text(exc)[:200],
                configured_dexes=configured,
            )
            return configured
        enabled = list(configured)
        for dex in offsets:
            normalized = str(dex or "").lower()
            if normalized not in enabled:
                enabled.append(normalized)
        self.settings.enabled_hyperliquid_dexes = ",".join(enabled)
        log.info(
            "hyperliquid_all_perp_dexes_enabled",
            dexes=enabled,
            directory_refreshed_at=(
                self.engine._perp_dex_directory_refreshed_at.isoformat()
                if self.engine._perp_dex_directory_refreshed_at is not None
                else None
            ),
        )
        return enabled

    async def _acquire_writer_lease(self) -> bool:
        if self._writer_lease_connection is not None:
            return True
        bind = getattr(self.db_session_factory, "kw", {}).get("bind")
        if bind is None:
            return True
        connection = await bind.connect()
        try:
            acquired = bool(
                await connection.scalar(
                    text("SELECT pg_try_advisory_lock(:lease_key)"),
                    {"lease_key": self._writer_lease_key},
                )
            )
            await connection.commit()
        except Exception:
            await connection.close()
            raise
        if not acquired:
            await connection.close()
            return False
        self._writer_lease_connection = connection
        return True

    async def _release_writer_lease(self) -> None:
        connection = self._writer_lease_connection
        self._writer_lease_connection = None
        if connection is None:
            return
        try:
            await connection.execute(
                text("SELECT pg_advisory_unlock(:lease_key)"),
                {"lease_key": self._writer_lease_key},
            )
            await connection.commit()
        finally:
            await connection.close()

    async def _writer_lease_watch_loop(self) -> None:
        while not self._stopped.is_set():
            connection = self._writer_lease_connection
            if connection is None:
                self.state.last_error = "writer lease lost"
                self._stopped.set()
                return
            try:
                await connection.scalar(text("SELECT 1"))
                await connection.commit()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = f"writer lease lost: {redact_text(exc)[:160]}"
                self._stopped.set()
                return
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def _event_loop_lag_watch_loop(self) -> None:
        interval = 0.05
        loop = asyncio.get_running_loop()
        expected = loop.time() + interval
        last_warning_at = 0.0
        while not self._stopped.is_set():
            await asyncio.sleep(interval)
            observed = loop.time()
            lag_ms = max(0, int((observed - expected) * 1000))
            self.state.event_loop_lag_ms = lag_ms
            self.state.max_event_loop_lag_ms = max(self.state.max_event_loop_lag_ms, lag_ms)
            if lag_ms >= 100 and observed - last_warning_at >= 1.0:
                last_warning_at = observed
                log.warning(
                    "low_latency_event_loop_lag",
                    lag_ms=lag_ms,
                    fill_queue_count=len(self._queued_source_fill_ids),
                    submit_queue_count=len(self._queued_submit_order_ids),
                )
            expected = observed + interval

    async def _warm_latency_caches(self) -> None:
        dexes = [dex.dex_name for dex in HyperliquidDexRegistry(self.settings).enabled_dexes()]
        await self.engine.warm_market_meta_cache(dexes)
        try:
            # SDK object construction performs CPU-heavy wallet/mapping setup.
            # Metadata is already in memory, so move this one-time work off the
            # event loop and keep websocket ingestion responsive.
            warmed = await asyncio.to_thread(
                self.execution_client.warm_exchanges,
                dexes,
            )
            if warmed:
                log.info("hyperliquid_exchange_cache_warmed", dexes=warmed)
        except Exception as exc:
            self.state.last_error = f"exchange warmup: {redact_text(exc)[:160]}"
        warm_transport = getattr(self.execution_client, "warm_order_transport", None)
        if callable(warm_transport):
            try:
                if await warm_transport(""):
                    log.info("hyperliquid_order_transport_warmed")
            except Exception as exc:
                # This is an optional latency optimization. A failed read-only
                # warmup must never affect durable order processing.
                log.warning(
                    "hyperliquid_order_transport_warmup_failed",
                    error=redact_text(exc)[:200],
                )
        try:
            async with self.db_session_factory() as db:
                warmed_risk = await self.engine.warm_risk_settings_cache(db)
                await db.commit()
            if warmed_risk:
                log.info("hyperliquid_risk_settings_cache_warmed", markets=warmed_risk)
        except Exception as exc:
            self.state.last_error = f"latency cache warmup: {redact_text(exc)[:160]}"
        try:
            confirmed, failed = await self._reconcile_one_x_market_risk_settings()
            if confirmed or failed:
                log.info(
                    "hyperliquid_one_x_risk_settings_reconciled",
                    confirmed=confirmed,
                    failed=failed,
                )
        except Exception as exc:
            self.state.last_error = f"1x risk reconciliation: {redact_text(exc)[:160]}"
            log.warning(
                "hyperliquid_one_x_risk_settings_reconcile_failed",
                error=redact_text(exc)[:200],
            )

    async def _reconcile_one_x_market_risk_settings(self) -> tuple[int, int]:
        account = self.settings.hyperliquid_follower_account_address()
        if not account:
            return 0, 0
        async with self.db_session_factory() as db:
            rows = (
                await db.execute(
                    select(MarketRiskSetting)
                    .where(MarketRiskSetting.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                    .where(MarketRiskSetting.account_address == account.lower())
                    .order_by(MarketRiskSetting.dex, MarketRiskSetting.canonical_coin)
                )
            ).scalars().all()
            active_position_scopes = {
                (str(dex or "").lower(), str(canonical or coin or "").upper())
                for dex, canonical, coin in (
                    await db.execute(
                        select(
                            LatestAccountPosition.dex,
                            LatestAccountPosition.canonical_coin,
                            LatestAccountPosition.coin,
                        )
                        .where(LatestAccountPosition.role == FOLLOWER)
                        .where(LatestAccountPosition.address == account.lower())
                        .where(LatestAccountPosition.active.is_(True))
                    )
                ).all()
            }
        targets = [
            (
                str(row.dex or "").lower(),
                str(row.canonical_coin or ""),
                row.asset_id,
                row.market_max_leverage,
            )
            for row in rows
            if one_x_leverage_required(
                margin_mode=row.actual_margin_mode or row.desired_margin_mode,
                canonical_coin_value=row.canonical_coin,
            )
            and any(
                _int_or_none(value) != 1
                for value in (row.desired_leverage, row.effective_leverage, row.actual_leverage)
            )
        ]
        targets.sort(
            key=lambda item: (
                (item[0], item[1].upper()) not in active_position_scopes,
                item[0],
                item[1].upper(),
            )
        )
        confirmed = 0
        failed = 0
        for dex, canonical, asset_id, market_max_leverage in targets:
            cache_scope = (dex, canonical.upper())
            lock_key = (*cache_scope, 1)
            risk_lock = self.engine._risk_settings_locks.setdefault(lock_key, asyncio.Lock())
            async with risk_lock:
                async with self.db_session_factory() as db:
                    result = await ensure_hyperliquid_market_risk_settings(
                        db=db,
                        client=self.execution_client,
                        settings=self.settings,
                        account_address=account,
                        dex=dex,
                        canonical_coin_value=canonical,
                        asset_id=asset_id,
                        market_max_leverage=market_max_leverage,
                        desired_default_leverage=1,
                        action_type="PREPARE_OPEN",
                        force_refresh=True,
                    )
                    await db.commit()
            for cache_key in list(self.engine._risk_settings_ok_cache):
                if cache_key[:2] == cache_scope:
                    self.engine._risk_settings_ok_cache.pop(cache_key, None)
            if not result.is_ok or result.effective_leverage != 1 or result.actual_leverage != 1:
                failed += 1
                log.warning(
                    "hyperliquid_one_x_market_reconcile_failed",
                    dex=dex,
                    canonical_coin=canonical,
                    reason=redact_text(result.reason_code or result.reason or result.status)[:160],
                )
                continue
            self.engine._risk_settings_ok_cache[
                (dex, canonical.upper(), 1, result.desired_margin_mode)
            ] = result
            if result.actual_margin_mode:
                self.engine._risk_settings_ok_cache[
                    (dex, canonical.upper(), 1, result.actual_margin_mode)
                ] = result
            confirmed += 1
        return confirmed, failed

    async def refresh_leaders(self) -> None:
        async with self.db_session_factory() as db:
            leaders = (
                await db.execute(
                    active_leaders_statement(
                        execution_scope=self.execution_scope,
                        explicit_route=self.settings.low_latency_uses_explicit_leader_route(),
                    )
                )
            ).scalars().all()
        active = {
            normalize_leader_address(leader.leader_address): leader for leader in leaders
        }
        ws_leaders, poll_fallback = self._partition_ws_leaders(list(active))
        async with self._leader_lock:
            previous_active_leaders = set(self.state.active_leaders)
            previous_ws_leaders = set(self.state.ws_leaders)
            self.state.active_leaders = active
            self.state.ws_leaders = set(ws_leaders)
            self.state.poll_fallback_leaders = set(poll_fallback)
            self.state.leader_user_fills_subscribed_count = len(self.state.ws_leaders)
            self.state.subscribed_leaders_by_dex = {
                dex.dex_name: len(self.state.ws_leaders)
                for dex in HyperliquidDexRegistry(self.settings).enabled_dexes()
            }
            changed = previous_ws_leaders != self.state.ws_leaders
            active_changed = previous_active_leaders != set(active)
            self._update_websocket_connected_state()
        if changed:
            self._leader_fill_subscription_event.set()
        if active_changed:
            # Enabling a leader often follows a collateral transfer. Wake the
            # out-of-band balance poller immediately instead of waiting for its
            # normal interval.
            self._account_abstraction_refresh_event.set()

    def _partition_ws_leaders(self, leaders: list[str]) -> tuple[list[str], list[str]]:
        limit = int(getattr(self.settings, "hyperliquid_ws_leader_subscription_limit", 0) or 0)
        leaders_sorted = sorted(leaders)
        if limit <= 0 or len(leaders_sorted) <= limit:
            return leaders_sorted, []
        return leaders_sorted[:limit], leaders_sorted[limit:]

    async def _leader_refresh_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.refresh_leaders()
            except Exception as exc:
                self.state.last_error = redact_text(exc)[:200]
            await asyncio.sleep(float(self.settings.low_latency_leader_refresh_seconds))

    async def _refresh_follower_account_abstraction_once(self) -> bool:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            self.state.account_value_refresh_last_error = "follower account address unavailable"
            return False
        started_at = datetime.now(timezone.utc)
        try:
            async with self.db_session_factory() as db:
                if isinstance(db, AsyncSession):
                    # This snapshot is reconstructable. It must not force a
                    # WAL fsync ahead of a fill plan or an order submit marker.
                    await db.execute(text("SET LOCAL synchronous_commit = OFF"))
                await self.engine._refresh_account_abstraction(db, FOLLOWER, follower)
                await db.commit()
            finished_at = datetime.now(timezone.utc)
            self.state.account_value_refresh_count += 1
            self.state.account_value_last_refresh_at = finished_at
            self.state.account_value_last_refresh_duration_ms = _delta_ms(started_at, finished_at)
            self.state.account_value_refresh_last_error = None
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            self.state.account_value_last_refresh_duration_ms = _delta_ms(started_at, finished_at)
            self.state.account_value_refresh_last_error = redact_text(exc)[:200]
            log.warning(
                "follower_account_value_background_refresh_failed",
                execution_account=mask_address(follower),
                error=self.state.account_value_refresh_last_error,
            )
            return False

    async def _load_follower_account_abstraction_cache(self) -> bool:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return False
        async with self.db_session_factory() as db:
            payload = await load_account_abstraction_state(
                db,
                role=FOLLOWER,
                address=follower,
            )
        if not payload:
            return False
        self.engine._cache_account_abstraction_payload(FOLLOWER, follower, payload)
        dexes = [
            dex.dex_name
            for dex in HyperliquidDexRegistry(self.settings).enabled_dexes()
        ]
        return _account_abstraction_payload_has_usable_value(
            payload,
            dexes=dexes,
        )

    async def _account_abstraction_refresh_loop(self) -> None:
        interval = max(
            1.0,
            float(getattr(self.settings, "account_value_refresh_seconds", 5.0) or 5.0),
        )
        while not self._stopped.is_set():
            try:
                await asyncio.wait_for(
                    self._account_abstraction_refresh_event.wait(),
                    timeout=interval,
                )
            except asyncio.TimeoutError:
                pass
            self._account_abstraction_refresh_event.clear()
            while self._hot_path_busy() and not self._stopped.is_set():
                self.state.background_cycles_deferred_for_hot_path += 1
                await asyncio.sleep(0.05)
            if self._stopped.is_set():
                return
            await self._refresh_follower_account_abstraction_once()

    async def _price_poll_loop(self) -> None:
        while not self._stopped.is_set():
            dexes = HyperliquidDexRegistry(self.settings).enabled_dexes()
            price_status = self.price_cache.status_by_dex([dex.dex_name for dex in dexes])
            fallback_dexes = _price_fallback_dexes(
                enabled_dexes=[dex.dex_name for dex in dexes],
                active_allocation_dexes=self.state.active_allocation_dexes,
                last_event_time_by_dex=self.state.last_event_time_by_dex,
                now=datetime.now(timezone.utc),
            )
            had_error = False
            for dex in dexes:
                if str(dex.dex_name or "").lower() not in fallback_dexes:
                    continue
                status = price_status.get(str(dex.dex_name or "").lower()) or {}
                if self.state.websocket_connected and status.get("fresh"):
                    continue
                try:
                    mids = await self.info_client.all_mids(dex.dex_name)
                    self.price_cache.update_mids(dex=dex.dex_name, mids=mids, source="REST_POLL_FALLBACK", replace=True)
                except Exception as exc:
                    had_error = True
                    self.state.last_error = f"price cache {dex.dex_name or 'default'}: {redact_text(exc)[:160]}"
            if not had_error and str(self.state.last_error or "").startswith("price cache "):
                self.state.last_error = None
            await asyncio.sleep(float(self.settings.price_cache_poll_seconds))

    async def _allocation_sync_loop(self) -> None:
        while not self._stopped.is_set():
            if self._hot_path_busy():
                self.state.background_cycles_deferred_for_hot_path += 1
                await asyncio.sleep(0.05)
                continue
            try:
                async with self.db_session_factory() as db:
                    synced, skipped = await self._sync_allocations_to_actual_follower_positions(db)
                    await self._reconcile_manual_position_guards(db)
                    await db.commit()
                self.state.last_allocation_sync_at = datetime.now(timezone.utc)
                self.state.allocation_sync_count = synced
                self.state.allocation_sync_skipped_count = skipped
                self.state.allocation_sync_last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.allocation_sync_last_error = redact_text(exc)[:200]
                self.state.last_error = f"allocation sync: {redact_text(exc)[:160]}"
                log.warning("allocation_sync_failed", error=redact_text(exc))
            await asyncio.sleep(float(self.settings.allocation_sync_poll_seconds))

    async def _active_follower_position_refresh_loop(self) -> None:
        interval = float(getattr(self.settings, "follower_active_position_refresh_seconds", 1.0) or 0)
        if interval <= 0:
            return
        while not self._stopped.is_set():
            started = asyncio.get_running_loop().time()
            if self._hot_path_busy():
                self.state.background_cycles_deferred_for_hot_path += 1
                await asyncio.sleep(min(0.05, max(0.01, interval)))
                continue
            try:
                dexes = [
                    dex
                    for dex in await self._active_allocation_dexes()
                    if not self._follower_position_stream_is_trusted(dex)
                ]
                if dexes:
                    self._schedule_follower_state_refresh(dexes, reason="ACTIVE_ALLOCATION_REFRESH")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = f"active follower position refresh: {redact_text(exc)[:160]}"
                log.warning("active_follower_position_refresh_failed", error=redact_text(exc))
            elapsed = asyncio.get_running_loop().time() - started
            await asyncio.sleep(max(0.05, interval - elapsed))

    async def _refresh_active_follower_positions_once(self, *, reason: str) -> int:
        dexes = await self._active_allocation_dexes()
        if not dexes:
            return 0
        return await self._refresh_follower_positions_for_dexes(dexes, source=reason)

    async def _active_allocation_dexes(self) -> list[str]:
        follower = self.settings.hyperliquid_follower_account_address()
        allocation_dexes = (
            select(LeaderPositionAllocationRecord.dex)
            .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
            .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
            .where(LeaderPositionAllocationRecord.status != "CLOSED")
            .where(LeaderConfig.enabled.is_(True))
            .where(LeaderConfig.deleted_at.is_(None))
        )
        dex_query = allocation_dexes
        if follower:
            # Keep refreshing a DEX while a follower position is still OPEN or
            # awaiting its second missing-position confirmation.  Otherwise the
            # last allocation can close after the first flat snapshot and leave
            # a stale MISSING position behind forever, causing the next genuine
            # open to be mistaken for an unmanaged/manual position.
            follower_position_dexes = (
                select(LatestAccountPosition.dex)
                .where(LatestAccountPosition.role == FOLLOWER)
                .where(LatestAccountPosition.address == follower.lower())
                .where(LatestAccountPosition.active.is_(True))
            )
            dex_query = allocation_dexes.union(follower_position_dexes)
        async with self.db_session_factory() as db:
            rows = (
                await db.execute(dex_query)
            ).scalars().all()
        active_dexes = {str(row or "").lower() for row in rows}
        self.state.active_allocation_dexes = active_dexes
        return sorted(active_dexes)

    async def _refresh_follower_positions_for_dexes(self, dexes: list[str], *, source: str) -> int:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return 0
        service = AccountStateService(self.info_client)
        refreshed = 0
        async with self.db_session_factory() as db:
            for dex in dexes:
                dex_name = str(dex or "").lower()
                loop_time = asyncio.get_running_loop().time()
                if loop_time < self._follower_rest_refresh_next_at.get(dex_name, 0.0):
                    continue
                try:
                    state = await service.fetch_state(
                        role=FOLLOWER,
                        address=follower,
                        dex=dex_name,
                        account_label=f"My Hyperliquid Follower Account / {dex_display_name(dex_name)}",
                        source=source,
                        price_mids=self.price_cache.fresh_mids_for_dex(dex_name),
                    )
                    await save_account_state(db, state)
                    refreshed += 1
                    self._follower_rest_refresh_failures.pop(dex_name, None)
                    refresh_error_prefix = (
                        f"follower position refresh {dex_name or 'default'}:"
                    )
                    if str(self.state.last_error or "").startswith(refresh_error_prefix):
                        self.state.last_error = None
                    self._follower_rest_refresh_next_at[dex_name] = loop_time + max(
                        0.5,
                        float(
                            getattr(
                                self.settings,
                                "follower_active_position_refresh_seconds",
                                0.5,
                            )
                            or 0.5
                        ),
                    )
                except Exception as exc:
                    failures = self._follower_rest_refresh_failures.get(dex_name, 0) + 1
                    self._follower_rest_refresh_failures[dex_name] = failures
                    self._follower_rest_refresh_next_at[dex_name] = loop_time + min(
                        30.0,
                        0.5 * (2 ** min(failures, 6)),
                    )
                    self.state.last_error = f"follower position refresh {dex_name or 'default'}: {redact_text(exc)[:160]}"
                    log.warning(
                        "follower_position_refresh_failed",
                        dex=dex_name,
                        reason=source,
                        error=redact_text(exc),
                    )
            await db.commit()
        return refreshed

    async def _sync_allocations_to_actual_follower_positions(self, db: Any) -> tuple[int, int]:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return 0, 0
        synced, skipped = await self._close_deleted_or_disabled_leader_allocations(db)
        allocation_rows = (
            await db.execute(
                select(LeaderPositionAllocationRecord)
                .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
                .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
                .where(LeaderPositionAllocationRecord.status != "CLOSED")
                .where(LeaderConfig.enabled.is_(True))
                .where(LeaderConfig.deleted_at.is_(None))
                .with_for_update(of=LeaderPositionAllocationRecord)
                .execution_options(populate_existing=True)
            )
        ).scalars().all()
        if not allocation_rows:
            return synced, skipped
        state_rows = (
            await db.execute(
                select(LatestAccountState)
                .where(LatestAccountState.role == FOLLOWER)
                .where(LatestAccountState.address == follower.lower())
            )
        ).scalars().all()
        state_by_dex = {str(row.dex or "").lower(): row for row in state_rows}
        position_rows = (
            await db.execute(
                select(LatestAccountPosition)
                .join(LatestAccountState, LatestAccountState.id == LatestAccountPosition.account_state_id)
                .where(LatestAccountState.role == FOLLOWER)
                .where(LatestAccountState.address == follower.lower())
                .where(LatestAccountPosition.active.is_(True))
            )
        ).scalars().all()
        actual_positions_by_scope: dict[tuple[str, str, str], list[LatestAccountPosition]] = {}
        for position in position_rows:
            side = _position_side_or_none(position.side)
            if side is None:
                continue
            actual_positions_by_scope.setdefault(
                (
                    str(position.dex or "").lower(),
                    str(position.canonical_coin or position.coin or "").upper(),
                    side.value,
                ),
                [],
            ).append(position)

        allocations_by_scope: dict[tuple[str, str, str], list[LeaderPositionAllocationRecord]] = {}
        for allocation in allocation_rows:
            side = _position_side_or_none(allocation.position_side)
            if side is None:
                continue
            allocations_by_scope.setdefault(
                (
                    str(allocation.dex or "").lower(),
                    str(allocation.canonical_coin or allocation.hyperliquid_coin or "").upper(),
                    side.value,
                ),
                [],
            ).append(allocation)

        for scope, rows in allocations_by_scope.items():
            dex, canonical_coin, side_value = scope
            side = _position_side_or_none(side_value)
            if side is None:
                skipped += len(rows)
                continue
            parsed_coin = parse_coin(canonical_coin, default_dex=dex)
            market = MarketKey(
                dex=dex,
                coin=parsed_coin.coin,
                canonical_coin=canonical_coin,
                raw_coin=canonical_coin,
                asset_id=None,
                venue_symbol=canonical_coin,
            )
            # Order recovery can finalize an UNKNOWN/SUBMITTING order in the
            # backend process after the watcher has already returned from the
            # submit attempt.  Reconcile the watcher-only overlay from the
            # durable order state before it is allowed to suppress allocation
            # sync.  Otherwise a recovered order can leave an immortal pending
            # intent which prevents a later manual fill from being absorbed and
            # keeps the durable follower-market guard active indefinitely.
            await self.engine._release_resolved_pending_intents_for_market(db, market)
            state = state_by_dex.get(dex)
            state_at = _state_updated_at(state)
            if state_at is None:
                skipped += len(rows)
                continue
            position = _preferred_actual_position(actual_positions_by_scope.get(scope, []))
            actual_qty = abs(Decimal(getattr(position, "size", 0) or 0)) if position is not None else Decimal("0")
            actual_notional = abs(Decimal(getattr(position, "notional", 0) or 0)) if position is not None else Decimal("0")
            mark_price = _position_sync_mark_price(position, actual_qty, actual_notional)
            if len(rows) != 1:
                if actual_qty <= ALLOCATION_TRANSITION_TOLERANCE:
                    for allocation in rows:
                        allocation_reconcile_at = _datetime_or_none(getattr(allocation, "last_reconcile_at", None))
                        if self.engine.pending_intents.has_pending_allocation(allocation):
                            skipped += 1
                            continue
                        if allocation_reconcile_at is not None and state_at < allocation_reconcile_at:
                            skipped += 1
                            continue
                        await self._close_allocation_after_follower_flat(
                            db,
                            allocation=allocation,
                            event_type="AUTO_CLOSE_MULTI_SCOPE_FLAT",
                            message=(
                                "multiple allocations shared a follower scope, but follower actual position is flat; "
                                "closed stale allocation to prevent residual lifecycle reuse"
                            ),
                            state_at=state_at,
                        )
                        if (
                            _allocation_leader_snapshot_nonflat(allocation)
                            and str(allocation.pending_reduce_reason or "")
                            != MINIMUM_RESIDUAL_ECONOMIC_FLAT_REASON
                        ):
                            await self._mark_allocation_wait_until_flat_after_follower_flat(
                                db,
                                allocation=allocation,
                                reason=(
                                    "Follower actual position is flat across multiple allocations while leader lifecycle "
                                    "is still non-flat; allocation closed and lifecycle waits until leader flat."
                                ),
                            )
                        synced += 1
                    continue
                skipped += len(rows)
                continue
            allocation = rows[0]
            manual_guard_entry = self.manual_position_guard.active_entry(market)
            manual_guard_active = manual_guard_entry is not None
            if self.engine.pending_intents.has_pending_allocation(allocation):
                skipped += 1
                continue
            allocation_reconcile_at = _datetime_or_none(getattr(allocation, "last_reconcile_at", None))
            actual_scope_qty_by_side = {
                PositionSide.LONG: _actual_scope_qty(
                    actual_positions_by_scope, dex, canonical_coin, PositionSide.LONG
                ),
                PositionSide.SHORT: _actual_scope_qty(
                    actual_positions_by_scope, dex, canonical_coin, PositionSide.SHORT
                ),
            }
            allocation_scope_qty_by_side = {
                PositionSide.LONG: (
                    abs(Decimal(allocation.allocated_qty or 0))
                    if side == PositionSide.LONG
                    else Decimal("0")
                ),
                PositionSide.SHORT: (
                    abs(Decimal(allocation.allocated_qty or 0))
                    if side == PositionSide.SHORT
                    else Decimal("0")
                ),
            }
            confirmation_before = (
                manual_guard_entry.position_change_confirmed_at
                if manual_guard_entry is not None
                else None
            )
            manual_guard_confirmation_at = self.manual_position_guard.confirm_if_observed(
                market,
                follower_qty_by_side=actual_scope_qty_by_side,
                allocation_qty_by_side=allocation_scope_qty_by_side,
                follower_state_at=state_at,
            )
            manual_guard_confirms_state = manual_guard_confirmation_at is not None
            if (
                isinstance(db, AsyncSession)
                and manual_guard_entry is not None
                and confirmation_before is None
                and manual_guard_confirmation_at is not None
            ):
                await db.execute(
                    update(FollowerMarketGuard)
                    .where(FollowerMarketGuard.execution_account == self.execution_scope)
                    .where(FollowerMarketGuard.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                    .where(FollowerMarketGuard.dex == market.dex)
                    .where(func.upper(FollowerMarketGuard.canonical_coin) == market.canonical_coin.upper())
                    .where(FollowerMarketGuard.active.is_(True))
                    .where(FollowerMarketGuard.position_version == manual_guard_entry.position_version)
                    .values(
                        position_change_confirmed_at=manual_guard_confirmation_at,
                        updated_at=datetime.now(timezone.utc),
                    )
                )
            if (
                allocation_reconcile_at is not None
                and state_at < allocation_reconcile_at
                and not manual_guard_confirms_state
            ):
                skipped += 1
                continue
            current_allocation_qty = abs(Decimal(allocation.allocated_qty or 0))
            latest_fill_event: Any | None = None
            if abs(actual_qty - current_allocation_qty) > ALLOCATION_TRANSITION_TOLERANCE:
                latest_fill_event = await self._latest_allocation_fill_applied_event(db, allocation)
                if (
                    actual_qty < current_allocation_qty - ALLOCATION_TRANSITION_TOLERANCE
                    and not manual_guard_active
                    and _allocation_sync_in_post_fill_snapshot_lag_guard(
                        allocation_reconcile_at=allocation_reconcile_at,
                        latest_fill_event_at=_datetime_or_none(getattr(latest_fill_event, "created_at", None)),
                        guard_seconds=float(self.settings.allocation_post_fill_snapshot_lag_guard_seconds),
                    )
                ):
                    skipped += 1
                    continue
            if manual_guard_active and not manual_guard_confirms_state:
                # Never mutate the allocation from a merely time-new snapshot.
                # It must either cross the fill-implied position checkpoint or
                # show the expected directional delta versus the allocation.
                skipped += 1
                continue
            if _allocation_has_flat_leader_close_intent(allocation):
                applied = await self._sync_flat_leader_close_intent_allocation(
                    db,
                    allocation=allocation,
                    actual_qty=actual_qty,
                    actual_notional=actual_notional,
                    mark_price=mark_price,
                    state_at=state_at,
                )
                if applied:
                    synced += 1
                else:
                    skipped += 1
                continue
            sync = _manual_same_side_position_sync(
                allocation=allocation,
                planning_allocation=allocation,
                transition_plan=SimpleNamespace(action=AllocationTransitionAction.INCREASE),
                aggregate_side=side,
                follower_qty_by_side=actual_scope_qty_by_side,
                allocation_qty_by_side=allocation_scope_qty_by_side,
                follower_state_at=state_at,
                allocation_latest_reconcile_at=allocation_reconcile_at,
                has_pending_allocation=False,
                mark_price=mark_price,
                # An unmatched/no-cloid same-side follower add is an explicit
                # manual position change.  Once a post-fill account snapshot is
                # old enough to be trustworthy, absorb the actual quantity into
                # the sole active allocation just as we already do for manual
                # reductions.  Otherwise the manual guard would forbid the
                # upward sync while also requiring that sync before it can
                # clear, permanently blocking every later leader fill.
                allow_actual_qty_increase=(
                    not manual_guard_active or manual_guard_confirms_state
                ),
                trusted_actual_qty_ceiling=(
                    actual_qty
                    if manual_guard_confirms_state
                    else _decimal_from_value(getattr(latest_fill_event, "after_qty", None))
                ),
            )
            if not sync.get("applied"):
                if abs(actual_qty - current_allocation_qty) <= ALLOCATION_TRANSITION_TOLERANCE:
                    allocation.last_reconcile_at = state_at
                    allocation.updated_at = datetime.now(timezone.utc)
                    synced += 1
                continue
            before_qty = Decimal(allocation.allocated_qty or 0)
            before_notional = Decimal(allocation.allocated_notional or 0)
            if sync.get("closed"):
                minimum_residual_release_pending = (
                    str(allocation.pending_reduce_reason or "")
                    == MINIMUM_RESIDUAL_ECONOMIC_FLAT_PENDING_REASON
                )
                allocation.allocated_qty = Decimal("0")
                allocation.allocated_notional = Decimal("0")
                allocation.target_notional = Decimal("0")
                allocation.status = "CLOSED"
                if (
                    _allocation_leader_snapshot_nonflat(allocation)
                    and not minimum_residual_release_pending
                ):
                    await self._mark_allocation_wait_until_flat_after_follower_flat(
                        db,
                        allocation=allocation,
                        reason="Follower actual position is flat while leader lifecycle is still non-flat; allocation closed and lifecycle waits until leader flat.",
                    )
            else:
                allocation.allocated_qty = sync["actual_qty"]
                allocation.allocated_notional = actual_notional if actual_notional > 0 else sync["actual_notional"]
                allocation.target_notional = allocation.allocated_notional
                allocation.avg_entry_price = _position_entry_price(position) or mark_price or allocation.avg_entry_price
                allocation.status = "OPEN"
            allocation.last_reconcile_at = datetime.now(timezone.utc)
            # Do not probe this server-onupdate ORM attribute with hasattr after
            # an autoflush. SQLAlchemy may have expired it, and probing performs
            # forbidden implicit async IO (MissingGreenlet), rolling back the
            # whole allocation-sync transaction.
            allocation.updated_at = allocation.last_reconcile_at
            if sync.get("closed"):
                _clear_deferred_reduce_after_follower_flat(allocation)
            else:
                _clear_deferred_reduce(allocation)
            db.add(
                RiskEvent(
                    severity="info",
                    event_type="ALLOCATION_AUTO_SYNCED_TO_FOLLOWER_POSITION",
                    symbol=allocation.canonical_coin,
                    leader_address=allocation.leader_address,
                    message="allocation ledger auto-synced to actual follower position",
                    metadata_json=_json_safe({
                        "allocation_id": allocation.id,
                        "dex": allocation.dex,
                        "canonical_coin": allocation.canonical_coin,
                        "position_side": allocation.position_side,
                        "before_qty": str(before_qty),
                        "before_notional": str(before_notional),
                        "actual_qty": str(allocation.allocated_qty),
                        "actual_notional": str(allocation.allocated_notional),
                        "closed": bool(sync.get("closed")),
                        "state_at": state_at.isoformat(),
                    }),
                )
            )
            synced += 1
        return synced, skipped

    async def _close_deleted_or_disabled_leader_allocations(self, db: Any) -> tuple[int, int]:
        rows = (
            await db.execute(
                select(
                    LeaderPositionAllocationRecord,
                    LeaderConfig.enabled,
                    LeaderConfig.deleted_at,
                )
                .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
                .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
                .where(LeaderPositionAllocationRecord.status != "CLOSED")
                .where(or_(LeaderConfig.enabled.is_(False), LeaderConfig.deleted_at.is_not(None)))
                .with_for_update(of=LeaderPositionAllocationRecord)
                .execution_options(populate_existing=True)
            )
        ).all()
        synced = 0
        skipped = 0
        now = datetime.now(timezone.utc)
        for row in rows:
            allocation = row[0]
            leader_enabled = bool(row[1])
            leader_deleted_at = row[2]
            if self.engine.pending_intents.has_pending_allocation(allocation):
                skipped += 1
                continue
            before_qty = Decimal(allocation.allocated_qty or 0)
            before_notional = Decimal(allocation.allocated_notional or 0)
            allocation.allocated_qty = Decimal("0")
            allocation.allocated_notional = Decimal("0")
            allocation.target_notional = Decimal("0")
            allocation.status = "CLOSED"
            allocation.pending_reduce_reason = "closed because leader was disabled or deleted"
            allocation.pending_reduce_qty = None
            allocation.pending_reduce_notional = None
            allocation.pending_reduce_since = None
            allocation.pending_reduce_source_fill_id = None
            allocation.last_leader_position_size = Decimal("0")
            allocation.last_leader_position_notional = Decimal("0")
            allocation.last_reconcile_at = now
            allocation.updated_at = now
            _clear_deferred_reduce(allocation)
            db.add(
                AllocationEvent(
                    allocation_id=allocation.id,
                    execution_order_id=None,
                    leader_id=allocation.leader_id,
                    leader_address=allocation.leader_address,
                    source_fill_id=None,
                    execution_venue=allocation.execution_venue,
                    dex=allocation.dex,
                    canonical_coin=allocation.canonical_coin,
                    position_side=allocation.position_side,
                    action="DELETED_LEADER_ALLOCATION_CLOSED",
                    before_notional=before_notional,
                    after_notional=Decimal("0"),
                    before_qty=before_qty,
                    after_qty=Decimal("0"),
                    metadata_json=_json_safe({
                        "reason": "leader disabled/deleted; allocation detached from copy lifecycle",
                        "leader_enabled": leader_enabled,
                        "leader_deleted_at": _iso_or_none(leader_deleted_at),
                    }),
                )
            )
            db.add(
                RiskEvent(
                    severity="warning",
                    event_type="DELETED_LEADER_ALLOCATION_CLOSED",
                    symbol=allocation.canonical_coin,
                    leader_address=allocation.leader_address,
                    message="deleted/disabled leader allocation closed so it cannot remain stale or own a market",
                    metadata_json=_json_safe({
                        "allocation_id": allocation.id,
                        "leader_id": allocation.leader_id,
                        "dex": allocation.dex,
                        "canonical_coin": allocation.canonical_coin,
                        "position_side": allocation.position_side,
                        "before_qty": str(before_qty),
                        "before_notional": str(before_notional),
                        "leader_enabled": leader_enabled,
                        "leader_deleted_at": _iso_or_none(leader_deleted_at),
                    }),
                )
            )
            synced += 1
        return synced, skipped

    async def _latest_allocation_fill_applied_event(
        self,
        db: Any,
        allocation: LeaderPositionAllocationRecord,
    ) -> AllocationEvent | None:
        allocation_id = getattr(allocation, "id", None)
        if allocation_id is None:
            return None
        return await db.scalar(
            select(AllocationEvent)
            .where(AllocationEvent.allocation_id == allocation_id)
            .where(AllocationEvent.action == "FILL_APPLIED")
            .order_by(AllocationEvent.created_at.desc(), AllocationEvent.id.desc())
            .limit(1)
        )

    async def _close_allocation_after_follower_flat(
        self,
        db: Any,
        *,
        allocation: LeaderPositionAllocationRecord,
        event_type: str,
        message: str,
        state_at: datetime,
    ) -> None:
        before_qty = Decimal(allocation.allocated_qty or 0)
        before_notional = Decimal(allocation.allocated_notional or 0)
        now = datetime.now(timezone.utc)
        allocation.allocated_qty = Decimal("0")
        allocation.allocated_notional = Decimal("0")
        allocation.target_notional = Decimal("0")
        allocation.status = "CLOSED"
        allocation.last_reconcile_at = now
        allocation.updated_at = now
        _clear_deferred_reduce_after_follower_flat(allocation)
        db.add(
            AllocationEvent(
                allocation_id=allocation.id,
                execution_order_id=None,
                leader_id=allocation.leader_id,
                leader_address=allocation.leader_address,
                source_fill_id=None,
                execution_venue=allocation.execution_venue,
                dex=allocation.dex,
                canonical_coin=allocation.canonical_coin,
                position_side=allocation.position_side,
                action=event_type,
                before_notional=before_notional,
                after_notional=Decimal("0"),
                before_qty=before_qty,
                after_qty=Decimal("0"),
                metadata_json=_json_safe({
                    "state_at": state_at.isoformat(),
                    "actual_qty": "0",
                    "target_notional": "0",
                }),
            )
        )
        db.add(
            RiskEvent(
                severity="warning",
                event_type=event_type,
                symbol=allocation.canonical_coin,
                leader_address=allocation.leader_address,
                message=message,
                metadata_json=_json_safe({
                    "allocation_id": allocation.id,
                    "leader_id": allocation.leader_id,
                    "dex": allocation.dex,
                    "canonical_coin": allocation.canonical_coin,
                    "position_side": allocation.position_side,
                    "before_qty": str(before_qty),
                    "before_notional": str(before_notional),
                    "state_at": state_at.isoformat(),
                }),
            )
        )

    async def _sync_flat_leader_close_intent_allocation(
        self,
        db: Any,
        *,
        allocation: LeaderPositionAllocationRecord,
        actual_qty: Decimal,
        actual_notional: Decimal,
        mark_price: Decimal,
        state_at: datetime,
    ) -> bool:
        before_qty = Decimal(allocation.allocated_qty or 0)
        before_notional = Decimal(allocation.allocated_notional or 0)
        now = datetime.now(timezone.utc)
        if actual_qty <= ALLOCATION_TRANSITION_TOLERANCE:
            allocation.allocated_qty = Decimal("0")
            allocation.allocated_notional = Decimal("0")
            allocation.target_notional = Decimal("0")
            allocation.status = "CLOSED"
            _clear_deferred_reduce_after_follower_flat(allocation)
            event_type = "AUTO_CLOSE_LEADER_FOLLOWER_FLAT"
            message = "flat leader close-intent allocation auto-closed after follower actual position became flat"
        elif abs(actual_qty - before_qty) > ALLOCATION_TRANSITION_TOLERANCE:
            allocation.allocated_qty = _q(actual_qty)
            if actual_notional > 0:
                allocation.allocated_notional = _q(actual_notional)
            elif mark_price > 0:
                allocation.allocated_notional = _q(actual_qty * mark_price)
            else:
                allocation.allocated_notional = before_notional
            allocation.target_notional = Decimal("0")
            allocation.status = "REDUCING"
            event_type = "AUTO_SYNC_FLAT_LEADER_CLOSE"
            message = "flat leader close-intent allocation synced down to actual follower residual; target remains zero"
        else:
            return False
        allocation.last_reconcile_at = now
        allocation.updated_at = now
        db.add(
            AllocationEvent(
                allocation_id=allocation.id,
                execution_order_id=None,
                leader_id=allocation.leader_id,
                leader_address=allocation.leader_address,
                source_fill_id=None,
                execution_venue=allocation.execution_venue,
                dex=allocation.dex,
                canonical_coin=allocation.canonical_coin,
                position_side=allocation.position_side,
                action=event_type,
                before_notional=before_notional,
                after_notional=Decimal(allocation.allocated_notional or 0),
                before_qty=before_qty,
                after_qty=Decimal(allocation.allocated_qty or 0),
                metadata_json=_json_safe({
                    "state_at": state_at.isoformat(),
                    "actual_qty": str(actual_qty),
                    "actual_notional": str(actual_notional),
                    "target_notional": str(allocation.target_notional),
                }),
            )
        )
        db.add(
            RiskEvent(
                severity="info" if allocation.status == "CLOSED" else "warning",
                event_type=event_type,
                symbol=allocation.canonical_coin,
                leader_address=allocation.leader_address,
                message=message,
                metadata_json=_json_safe({
                    "allocation_id": allocation.id,
                    "dex": allocation.dex,
                    "canonical_coin": allocation.canonical_coin,
                    "position_side": allocation.position_side,
                    "before_qty": str(before_qty),
                    "before_notional": str(before_notional),
                    "after_qty": str(allocation.allocated_qty),
                    "after_notional": str(allocation.allocated_notional),
                    "state_at": state_at.isoformat(),
                }),
            )
        )
        return True

    async def _mark_allocation_wait_until_flat_after_follower_flat(
        self,
        db: Any,
        *,
        allocation: LeaderPositionAllocationRecord,
        reason: str,
    ) -> None:
        leader_size = _allocation_last_leader_abs_size(allocation)
        leader_notional = _allocation_last_leader_abs_notional(allocation)
        if leader_size <= ALLOCATION_TRANSITION_TOLERANCE and leader_notional <= ALLOCATION_TRANSITION_TOLERANCE:
            return
        canonical = str(allocation.canonical_coin or allocation.hyperliquid_coin or "").upper()
        dex = str(allocation.dex or "").lower()
        baseline = await db.scalar(
            select(LeaderPositionBaseline)
            .where(LeaderPositionBaseline.leader_id == allocation.leader_id)
            .where(LeaderPositionBaseline.execution_venue == allocation.execution_venue)
            .where(LeaderPositionBaseline.dex == dex)
            .where(func.upper(LeaderPositionBaseline.canonical_coin) == canonical)
            .limit(1)
        )
        now = datetime.now(timezone.utc)
        if baseline is None:
            baseline = LeaderPositionBaseline(
                leader_id=allocation.leader_id,
                leader_address=normalize_leader_address(allocation.leader_address),
                execution_venue=allocation.execution_venue,
                dex=dex,
                canonical_coin=canonical,
            )
            db.add(baseline)
            await db.flush()
        baseline.leader_address = normalize_leader_address(allocation.leader_address)
        baseline.side_at_enable = allocation.position_side or PositionSide.FLAT.value
        baseline.size_at_enable = leader_size
        baseline.notional_at_enable = leader_notional
        baseline.account_value_at_enable = allocation.last_leader_account_value
        baseline.baseline_status = BASELINE_WAIT_UNTIL_FLAT
        baseline.first_seen_at = baseline.first_seen_at or now
        baseline.flat_confirmed_at = None
        baseline.copy_allowed_at = None
        baseline.last_leader_size = leader_size
        baseline.last_leader_notional = leader_notional
        baseline.last_checked_at = now
        baseline.reason = reason[:1000]
        db.add(
            RiskEvent(
                severity="warning",
                event_type="ALLOCATION_CLOSED_FOLLOWER_FLAT_WAIT_UNTIL_LEADER_FLAT",
                symbol=allocation.canonical_coin,
                leader_address=allocation.leader_address,
                message=reason,
                metadata_json=_json_safe({
                    "allocation_id": allocation.id,
                    "leader_id": allocation.leader_id,
                    "dex": allocation.dex,
                    "canonical_coin": allocation.canonical_coin,
                    "leader_size": str(leader_size),
                    "leader_notional": str(leader_notional),
                }),
            )
        )

    async def _ws_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                async with websockets.connect(
                    self.settings.hyperliquid_ws_url,
                    ping_interval=20,
                    compression=None,
                    max_queue=HYPERLIQUID_WS_MAX_QUEUE,
                    max_size=HYPERLIQUID_WS_MAX_MESSAGE_BYTES,
                ) as ws:
                    self.state.market_ws_connected = True
                    self.state.low_latency_primary = True
                    self.state.follower_clearinghouse_subscribed = False
                    self.state.follower_all_dexs_clearinghouse_subscribed = False
                    self._follower_position_stream_observed_at.clear()
                    self._update_websocket_connected_state()
                    self.state.last_error = None
                    await self._subscribe_follower(ws)
                    await self._subscribe_market_data(ws)
                    next_app_ping_at = asyncio.get_running_loop().time() + HYPERLIQUID_WS_APP_PING_SECONDS
                    while not self._stopped.is_set():
                        if asyncio.get_running_loop().time() >= next_app_ping_at:
                            await self._send_ws_ping(ws)
                            next_app_ping_at = asyncio.get_running_loop().time() + HYPERLIQUID_WS_APP_PING_SECONDS
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        ws_received_at = datetime.now(timezone.utc)
                        self.state.last_ws_event_at = ws_received_at
                        await self._handle_ws_message(
                            raw,
                            ws_received_at=ws_received_at,
                            defer_leader_fill_persist=True,
                        )
            except Exception as exc:
                self.state.market_ws_connected = False
                self._update_websocket_connected_state()
                self.state.low_latency_primary = False
                self.state.follower_order_updates_subscribed = False
                self.state.follower_user_events_subscribed = False
                self.state.follower_user_fills_subscribed = False
                self.state.follower_clearinghouse_subscribed = False
                self.state.follower_all_dexs_clearinghouse_subscribed = False
                self._follower_position_stream_observed_at.clear()
                self.state.reconnect_count += 1
                self.state.last_error = redact_text(exc)[:200]
                log.warning("low_latency_ws_reconnect", error=redact_text(exc))
                await asyncio.sleep(3)

    async def _leader_fill_ws_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                async with websockets.connect(
                    self.settings.hyperliquid_ws_url,
                    ping_interval=20,
                    compression=None,
                    max_queue=HYPERLIQUID_WS_MAX_QUEUE,
                    max_size=HYPERLIQUID_WS_MAX_MESSAGE_BYTES,
                ) as ws:
                    self.state.leader_fills_ws_connected = True
                    self._update_websocket_connected_state()
                    self.state.last_error = None
                    self._subscribed.clear()
                    self._leader_fill_subscription_event.clear()
                    await self._subscribe_active_leaders(ws)
                    next_app_ping_at = asyncio.get_running_loop().time() + HYPERLIQUID_WS_APP_PING_SECONDS
                    start_time_ms = await self._durable_leader_fill_backfill_start_ms()
                    if self._leader_fill_backfill_start_ms is not None:
                        start_time_ms = min(start_time_ms, self._leader_fill_backfill_start_ms)
                    self._leader_fill_backfill_start_ms = None
                    async with self._leader_fill_backfill_lock:
                        await self._backfill_leader_fills_since(start_time_ms)
                    while not self._stopped.is_set():
                        if asyncio.get_running_loop().time() >= next_app_ping_at:
                            await self._send_ws_ping(ws)
                            next_app_ping_at = asyncio.get_running_loop().time() + HYPERLIQUID_WS_APP_PING_SECONDS
                        if self._leader_fill_subscription_event.is_set():
                            self._leader_fill_subscription_event.clear()
                            await self._subscribe_active_leaders(ws)
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        ws_received_at = datetime.now(timezone.utc)
                        self.state.last_ws_event_at = ws_received_at
                        await self._handle_ws_message(
                            raw,
                            ws_received_at=ws_received_at,
                            defer_leader_fill_persist=True,
                            leader_ingress_source="primary_userFills",
                        )
            except Exception as exc:
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                backfill_start_ms = max(now_ms - 5_000, 0)
                if self._leader_fill_backfill_start_ms is None:
                    self._leader_fill_backfill_start_ms = backfill_start_ms
                else:
                    self._leader_fill_backfill_start_ms = min(
                        self._leader_fill_backfill_start_ms,
                        backfill_start_ms,
                    )
                self.state.leader_fills_ws_connected = False
                self._update_websocket_connected_state()
                self.state.reconnect_count += 1
                self.state.last_error = redact_text(exc)[:200]
                log.warning("low_latency_leader_fill_ws_reconnect", error=redact_text(exc))
                await asyncio.sleep(3)

    async def _durable_leader_fill_backfill_start_ms(self) -> int:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        fallback = self._startup_leader_fill_backfill_start_ms(now_ms)
        fallback = fallback if fallback is not None else max(now_ms - 60_000, 0)
        async with self._leader_lock:
            leaders = sorted(self.state.ws_leaders)
        if not leaders:
            return fallback
        async with self.db_session_factory() as db:
            rows = (
                await db.execute(
                    select(LeaderFillCursor.leader_address, LeaderFillCursor.backfilled_through_ms)
                    .where(LeaderFillCursor.leader_address.in_(leaders))
                )
            ).all()
        by_address = {
            normalize_leader_address(address): (
                int(backfilled_through_ms)
                if int(backfilled_through_ms or 0) > 0
                else fallback
            )
            for address, backfilled_through_ms in rows
        }
        starts = [
            max(by_address.get(address, fallback) - LEADER_FILL_BACKFILL_OVERLAP_MS, 0)
            for address in leaders
        ]
        self._leader_fill_startup_backfill_done = True
        return min(starts or [fallback])

    def _startup_leader_fill_backfill_start_ms(self, now_ms: int | None = None) -> int | None:
        window_seconds = max(float(getattr(self.settings, "leader_fill_startup_backfill_seconds", 0) or 0), 0.0)
        if window_seconds <= 0:
            return None
        if now_ms is None:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return max(int(now_ms) - int(window_seconds * 1000), 0)

    def _schedule_startup_leader_fill_backfill(self, now_ms: int | None = None) -> bool:
        if self._leader_fill_startup_backfill_done:
            return False
        self._leader_fill_startup_backfill_done = True
        start_time_ms = self._startup_leader_fill_backfill_start_ms(now_ms)
        if start_time_ms is None:
            return False
        self._schedule_background_task(self._backfill_leader_fills_since(start_time_ms))
        return True

    async def _subscribe_active_leaders(self, ws: Any) -> None:
        async with self._leader_lock:
            leaders = list(self.state.ws_leaders)
        for address in leaders:
            key = f"userFills:{address}"
            if key in self._subscribed:
                continue
            await self._subscribe(ws, {"type": "userFills", "user": address})
            self._subscribed.add(key)

    def _update_websocket_connected_state(self) -> None:
        leader_fill_ready = not self.state.ws_leaders or self.state.leader_fills_ws_connected
        self.state.websocket_connected = bool(self.state.market_ws_connected and leader_fill_ready)

    async def _backfill_leader_fills_since(self, start_time_ms: int) -> bool:
        async with self._leader_lock:
            leaders = sorted(self.state.ws_leaders)
        if not leaders:
            return True
        end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        all_succeeded = True
        for address in leaders:
            try:
                fills = await self._fetch_leader_fills_range(
                    address,
                    start_time_ms,
                    end_time_ms,
                )
                fills = _causal_sort_fill_payloads(fills)
                if fills:
                    await self._handle_ws_message(
                        json.dumps({"channel": "userFills", "data": {"user": address, "fills": fills}}),
                        ws_received_at=datetime.now(timezone.utc),
                    )
                await self._mark_leader_backfill_complete(address, end_time_ms)
                normalized_address = normalize_leader_address(address)
                self._leader_snapshot_backfilled_through_ms[normalized_address] = max(
                    end_time_ms,
                    self._leader_snapshot_backfilled_through_ms.get(normalized_address, 0),
                )
            except Exception as exc:
                all_succeeded = False
                if self._leader_fill_backfill_start_ms is None:
                    self._leader_fill_backfill_start_ms = start_time_ms
                else:
                    self._leader_fill_backfill_start_ms = min(
                        self._leader_fill_backfill_start_ms,
                        start_time_ms,
                    )
                safe_error = redact_text(exc)
                self.state.last_error = (
                    f"leader fill backfill {mask_address(address)}: {safe_error[:160]}"
                )
                log.warning(
                    "low_latency_leader_fill_backfill_failed",
                    leader_address=mask_address(address),
                    start_time_ms=start_time_ms,
                    error=safe_error,
                )
        if all_succeeded and str(self.state.last_error or "").startswith(
            "leader fill backfill "
        ):
            self.state.last_error = None
        return all_succeeded

    async def _leader_fill_backfill_retry_loop(self) -> None:
        next_reconcile_at = 0.0
        retry_not_before = 0.0
        consecutive_failures = 0
        while not self._stopped.is_set():
            start_time_ms = self._leader_fill_backfill_start_ms
            loop_time = asyncio.get_running_loop().time()
            retry_due = start_time_ms is not None and loop_time >= retry_not_before
            periodic_due = start_time_ms is None and loop_time >= next_reconcile_at
            if self.state.leader_fills_ws_connected and (retry_due or periodic_due):
                if start_time_ms is None:
                    start_time_ms = await self._durable_leader_fill_backfill_start_ms()
                self._leader_fill_backfill_start_ms = None
                async with self._leader_fill_backfill_lock:
                    succeeded = await self._backfill_leader_fills_since(start_time_ms)
                loop_time = asyncio.get_running_loop().time()
                if succeeded:
                    consecutive_failures = 0
                    retry_not_before = 0.0
                else:
                    consecutive_failures += 1
                    retry_not_before = loop_time + _leader_fill_backfill_retry_delay_seconds(
                        consecutive_failures
                    )
                next_reconcile_at = loop_time + max(
                    5.0,
                    float(getattr(self.settings, "leader_fill_reconcile_seconds", 15.0) or 15.0),
                )
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def _fetch_leader_fills_range(
        self,
        address: str,
        start_time_ms: int,
        end_time_ms: int,
        *,
        depth: int = 0,
    ) -> list[dict[str, Any]]:
        page = await self.info_client.user_fills_by_time(
            address,
            start_time_ms,
            end_time_ms=end_time_ms,
            aggregate_by_time=False,
        )
        page = [
            fill
            for fill in list(page or [])
            if int(start_time_ms) <= (_int_or_none(fill.get("time")) or 0) <= int(end_time_ms)
        ]
        if len(page) < LEADER_FILL_BACKFILL_PAGE_SIZE:
            return page
        if start_time_ms >= end_time_ms or depth >= 32:
            raise RuntimeError(
                "leader fill backfill saturated the 2000-fill API limit in one time window"
            )
        midpoint = (int(start_time_ms) + int(end_time_ms)) // 2
        left = await self._fetch_leader_fills_range(
            address,
            start_time_ms,
            midpoint,
            depth=depth + 1,
        )
        right = await self._fetch_leader_fills_range(
            address,
            midpoint + 1,
            end_time_ms,
            depth=depth + 1,
        )
        by_source_fill_id: dict[str, dict[str, Any]] = {}
        for fill in [*left, *right]:
            by_source_fill_id[fill_unique_id(address, fill)] = fill
        return list(by_source_fill_id.values())

    async def _mark_leader_backfill_complete(self, address: str, end_time_ms: int) -> None:
        now = datetime.now(timezone.utc)
        async with self.db_session_factory() as db:
            cursor_insert = insert(LeaderFillCursor).values(
                leader_address=normalize_leader_address(address),
                last_fill_time_ms=0,
                last_fill_tid=None,
                backfilled_through_ms=end_time_ms,
                updated_at=now,
            )
            await db.execute(
                cursor_insert.on_conflict_do_update(
                    index_elements=[LeaderFillCursor.leader_address],
                    set_={
                        "backfilled_through_ms": func.greatest(
                            LeaderFillCursor.backfilled_through_ms,
                            cursor_insert.excluded.backfilled_through_ms,
                        ),
                        "updated_at": now,
                    },
                )
            )
            await db.commit()

    async def _subscribe_follower(self, ws: Any) -> None:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return
        subscriptions = {
            "orderUpdates": "follower_order_updates_subscribed",
            "userFills": "follower_user_fills_subscribed",
            "allDexsClearinghouseState": "follower_all_dexs_clearinghouse_subscribed",
        }
        for sub_type, attr in subscriptions.items():
            await self._subscribe(ws, {"type": sub_type, "user": follower})
            setattr(self.state, attr, True)
            if sub_type == "allDexsClearinghouseState":
                # Preserve the existing status contract while exposing the
                # more precise all-DEX subscription field separately.
                self.state.follower_clearinghouse_subscribed = True

    async def _subscribe_market_data(self, ws: Any) -> None:
        for dex in HyperliquidDexRegistry(self.settings).enabled_dexes():
            payload: dict[str, Any] = {"type": "allMids"}
            if dex.dex_name:
                payload["dex"] = dex.dex_name
            await self._subscribe(ws, payload)

    async def _subscribe(self, ws: Any, subscription: dict[str, Any]) -> None:
        await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))

    async def _send_ws_ping(self, ws: Any) -> None:
        await ws.send(json.dumps({"method": "ping"}))

    async def _handle_ws_message(
        self,
        raw_message: str | bytes,
        *,
        ws_received_at: datetime | None = None,
        defer_leader_fill_persist: bool = False,
        leader_ingress_source: str | None = None,
    ) -> None:
        try:
            message = json.loads(raw_message)
        except Exception:
            return
        channel = message.get("channel")
        data = message.get("data")
        if channel == "allMids":
            dex = str(data.get("dex") or "") if isinstance(data, dict) else ""
            mids = data.get("mids") if isinstance(data, dict) else None
            if mids is None and isinstance(data, dict):
                mids = {
                    key: value
                    for key, value in data.items()
                    if key not in {"dex", "type"} and isinstance(value, (str, int, float))
                }
            if isinstance(mids, dict):
                self.price_cache.update_mids(dex=dex, mids=mids, source="WEBSOCKET")
            return
        if not isinstance(data, dict):
            return
        user_address = normalize_leader_address(data.get("user", ""))
        follower_address = normalize_leader_address(self.settings.hyperliquid_follower_account_address() or "")
        if follower_address and user_address == follower_address:
            if channel == "clearinghouseState":
                await self._handle_follower_clearinghouse_state(data, ws_received_at=ws_received_at)
                return
            if channel == "allDexsClearinghouseState":
                await self._handle_follower_all_dexs_clearinghouse_state(
                    data,
                    ws_received_at=ws_received_at,
                )
                return
            if channel in {"userFills", "userEvents"}:
                await self._handle_follower_user_fills(channel, data, ws_received_at=ws_received_at)
                return
            return
        if channel != "userFills":
            return
        fills = _fills_from_message(channel, data)
        if not fills:
            return
        fills = _causal_sort_fill_payloads(fills)
        leader_address = user_address
        async with self._leader_lock:
            leader = self.state.active_leaders.get(leader_address)
            ws_leader = leader_address in self.state.ws_leaders
        if leader is None or not ws_leader:
            return
        is_snapshot = bool(data.get("isSnapshot"))
        snapshot_covered_through_ms = (
            self._leader_snapshot_backfilled_through_ms.get(leader_address)
            if is_snapshot
            else None
        )
        events: list[FillEvent] = []
        parse_failures: list[str] = []
        for fill in fills:
            fill_time_ms = _int_or_none(fill.get("time"))
            if (
                is_snapshot
                and snapshot_covered_through_ms is not None
                and fill_time_ms is not None
                and fill_time_ms <= snapshot_covered_through_ms
            ):
                # A successful REST backfill already ran this fill through the
                # durable exactly-once path.  Redundant subscription history
                # must not compete with a new live fill during reconnect.
                continue
            event_is_snapshot = bool(
                is_snapshot
                and (snapshot_covered_through_ms is None or fill_time_ms is None)
            )
            try:
                event = build_fill_event(
                    leader_address,
                    fill,
                    is_snapshot=event_is_snapshot,
                    ws_received_at=ws_received_at,
                    ingress_channel=leader_ingress_source or str(channel or ""),
                )
            except Exception as exc:
                safe_error = redact_text(exc)
                parse_failures.append(safe_error)
                await self._record_parse_error(leader_address, fill, safe_error)
                continue
            if not event.is_snapshot:
                self.state.last_event_time_by_dex[event.market.dex] = (
                    event.ws_received_at.isoformat()
                )
            events.append(event)
        if events and not all(event.is_snapshot for event in events):
            events = await self._filter_suppressed_events(events)
        if events:
            await self._enqueue_fill_events(
                events,
                leader,
                persist=not defer_leader_fill_persist,
                ensure_persist_in_worker=defer_leader_fill_persist,
            )
        if parse_failures:
            raise RetryableFillProcessingError(
                f"{len(parse_failures)} leader fill(s) could not be parsed; backfill cursor not advanced: "
                f"{parse_failures[0][:160]}"
            )

    async def _handle_follower_user_fills(
        self,
        channel: str | None,
        data: dict[str, Any],
        *,
        ws_received_at: datetime | None,
    ) -> None:
        fills = _fills_from_message(channel, data)
        if not fills:
            return
        fills = _causal_sort_fill_payloads(fills)
        affected_dexes: set[str] = set()
        is_snapshot = bool(data.get("isSnapshot"))
        if is_snapshot:
            for fill in fills:
                try:
                    market = parse_fill_to_market_key(fill)
                except Exception:
                    continue
                affected_dexes.add(market.dex)
            if affected_dexes:
                self._schedule_follower_state_refresh(
                    affected_dexes,
                    reason="FOLLOWER_USER_FILL_SNAPSHOT",
                )
            return
        durable_guards: dict[tuple[str, str], tuple[MarketKey, FollowerMarketGuard]] = {}
        async with self.db_session_factory() as db:
            for fill in fills:
                try:
                    market = parse_fill_to_market_key(fill)
                except Exception:
                    continue
                affected_dexes.add(market.dex)
                if await self._follower_fill_matches_auto_copy_order(db, fill):
                    continue
                confirmation_spec = _manual_fill_position_confirmation_spec(fill)
                self.manual_position_guard.mark(
                    market,
                    reason="follower fill was not linked to an AUTO_COPY order",
                    observed_at=ws_received_at,
                    expected_position_side=(confirmation_spec or (None, None, None))[0],
                    expected_position_qty=(confirmation_spec or (None, None, None))[1],
                    expected_position_relation=(confirmation_spec or (None, None, None))[2],
                )
                guard = await self._persist_unmatched_follower_fill_guard(
                    db,
                    market=market,
                    fill=fill,
                    observed_at=ws_received_at,
                )
                durable_guards[(market.dex, market.canonical_coin.upper())] = (market, guard)
            await db.commit()
        for market, guard in durable_guards.values():
            if bool(guard.active):
                self.manual_position_guard.mark(
                    market,
                    reason=guard.reason or "follower fill was not linked to an AUTO_COPY order",
                    observed_at=guard.observed_at,
                    position_version=int(guard.position_version or 0),
                    expected_position_side=guard.expected_position_side,
                    expected_position_qty=guard.expected_position_qty,
                    expected_position_relation=guard.expected_position_relation,
                    position_change_confirmed_at=guard.position_change_confirmed_at,
                )
            else:
                # A duplicated websocket delivery of an already reconciled
                # fill must not reactivate the guard.
                self.manual_position_guard.clear(market)
        if affected_dexes:
            self._schedule_follower_state_refresh(
                affected_dexes,
                reason="FOLLOWER_USER_FILL",
            )

    async def _follower_fill_matches_auto_copy_order(self, db: Any, fill: dict[str, Any]) -> bool:
        cloid, oid = _fill_order_identifiers(fill)
        if cloid and self.engine.pending_intents.has_cloid(cloid):
            return True
        filters = []
        if cloid:
            filters.append(ExecutionOrder.cloid == cloid)
        if oid:
            filters.extend([ExecutionOrder.order_id == oid, ExecutionOrder.venue_order_id == oid])
        if not filters:
            return False
        row = await db.scalar(
            select(ExecutionOrder.id)
            .where(ExecutionOrder.source_type == "AUTO_COPY")
            .where(ExecutionOrder.venue_account == self.execution_scope)
            .where(or_(*filters))
            .limit(1)
        )
        return row is not None

    async def _persist_unmatched_follower_fill_guard(
        self,
        db: Any,
        *,
        market: MarketKey,
        fill: dict[str, Any],
        observed_at: datetime | None,
    ) -> FollowerMarketGuard:
        confirmation_spec = _manual_fill_position_confirmation_spec(fill)
        expected_position_side = (confirmation_spec or (None, None, None))[0]
        expected_position_qty = (confirmation_spec or (None, None, None))[1]
        expected_position_relation = (confirmation_spec or (None, None, None))[2]
        if not isinstance(db, AsyncSession):
            return FollowerMarketGuard(
                execution_account=self.execution_scope,
                execution_venue=ExecutionVenue.HYPERLIQUID.value,
                dex=market.dex,
                canonical_coin=market.canonical_coin,
                position_version=1,
                active=True,
                reason="follower fill was not linked to an AUTO_COPY order",
                observed_at=observed_at or datetime.now(timezone.utc),
                expected_position_side=(
                    expected_position_side.value if expected_position_side is not None else None
                ),
                expected_position_qty=expected_position_qty,
                expected_position_relation=expected_position_relation,
                position_change_confirmed_at=None,
            )
        await db.execute(
            text("SELECT pg_advisory_xact_lock(:market_key)"),
            {"market_key": _market_transaction_key(market, self.execution_scope)},
        )
        row = await db.scalar(
            _follower_market_guard_query(
                market,
                execution_scope=self.execution_scope,
            ).with_for_update()
        )
        follower = self.settings.hyperliquid_follower_account_address() or "follower"
        unmatched_fill_id = fill_unique_id(follower, fill)
        cloid, oid = _fill_order_identifiers(fill)
        now = datetime.now(timezone.utc)
        event_at = _datetime_or_none(observed_at) or now
        inserted_fill_id = await db.scalar(
            insert(UnmatchedFollowerFill)
            .values(
                follower_fill_id=unmatched_fill_id,
                execution_account=self.execution_scope,
                execution_venue=ExecutionVenue.HYPERLIQUID.value,
                dex=str(market.dex or "").lower(),
                canonical_coin=market.canonical_coin.upper(),
                observed_at=event_at,
                cloid=cloid,
                order_id=oid,
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=[UnmatchedFollowerFill.follower_fill_id])
            .returning(UnmatchedFollowerFill.follower_fill_id)
        )
        if inserted_fill_id is None:
            if row is None:
                raise RuntimeError(
                    "unmatched follower fill was deduplicated without a durable market guard"
                )
            return row
        if row is None:
            row = FollowerMarketGuard(
                execution_account=self.execution_scope,
                execution_venue=ExecutionVenue.HYPERLIQUID.value,
                dex=str(market.dex or "").lower(),
                canonical_coin=market.canonical_coin.upper(),
                position_version=1,
                active=True,
                reason="follower fill was not linked to an AUTO_COPY order",
                observed_at=event_at,
                reconciled_at=None,
                last_unmatched_fill_id=unmatched_fill_id,
                last_cloid=cloid,
                last_order_id=oid,
                expected_position_side=(
                    expected_position_side.value if expected_position_side is not None else None
                ),
                expected_position_qty=expected_position_qty,
                expected_position_relation=expected_position_relation,
                position_change_confirmed_at=None,
            )
            db.add(row)
            await db.flush()
            return row
        row.position_version = int(row.position_version or 0) + 1
        row.active = True
        row.reason = "follower fill was not linked to an AUTO_COPY order"
        row.observed_at = event_at
        row.reconciled_at = None
        row.last_unmatched_fill_id = unmatched_fill_id
        row.last_cloid = cloid
        row.last_order_id = oid
        row.expected_position_side = (
            expected_position_side.value if expected_position_side is not None else None
        )
        row.expected_position_qty = expected_position_qty
        row.expected_position_relation = expected_position_relation
        row.position_change_confirmed_at = None
        row.updated_at = now
        await db.flush()
        return row

    async def _restore_persistent_manual_position_guards(self) -> int:
        async with self.db_session_factory() as db:
            if not isinstance(db, AsyncSession):
                return 0
            rows = (
                await db.execute(
                    select(FollowerMarketGuard)
                    .where(FollowerMarketGuard.execution_account == self.execution_scope)
                    .where(FollowerMarketGuard.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                    .where(FollowerMarketGuard.active.is_(True))
                )
            ).scalars().all()
        for row in rows:
            parsed = parse_coin(row.canonical_coin, default_dex=row.dex)
            market = MarketKey(
                dex=str(row.dex or "").lower(),
                coin=parsed.coin,
                canonical_coin=str(row.canonical_coin or "").upper(),
                raw_coin=str(row.canonical_coin or ""),
                asset_id=None,
                venue_symbol=str(row.canonical_coin or ""),
            )
            self.manual_position_guard.mark(
                market,
                reason=row.reason or "durable unmatched follower fill guard",
                observed_at=row.observed_at,
                position_version=int(row.position_version or 0),
                expected_position_side=row.expected_position_side,
                expected_position_qty=row.expected_position_qty,
                expected_position_relation=row.expected_position_relation,
                position_change_confirmed_at=row.position_change_confirmed_at,
            )
        return len(rows)

    async def _handle_follower_clearinghouse_state(
        self,
        data: dict[str, Any],
        *,
        ws_received_at: datetime | None,
    ) -> None:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return
        payload = _clearinghouse_state_payload_from_ws(data)
        dex = _dex_from_ws_account_state(data)
        if payload is None:
            self._schedule_follower_state_refresh([dex], reason="FOLLOWER_CLEARINGHOUSE_WS_EMPTY")
            return
        observed_at = ws_received_at or datetime.now(timezone.utc)
        state = parse_account_state(
            role=FOLLOWER,
            address=follower,
            dex=dex,
            clearinghouse_state=payload,
            account_label=f"My Hyperliquid Follower Account / {dex_display_name(dex)}",
            source="FOLLOWER_CLEARINGHOUSE_WS",
            updated_at=observed_at,
            price_mids=self.price_cache.fresh_mids_for_dex(dex),
        )
        async with self.db_session_factory() as db:
            await save_account_state(db, state)
            await self._reconcile_manual_position_guards(db, dexes={dex})
            await db.commit()
        self._follower_position_stream_observed_at[dex] = observed_at

    async def _handle_follower_all_dexs_clearinghouse_state(
        self,
        data: dict[str, Any],
        *,
        ws_received_at: datetime | None,
    ) -> None:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return
        states = _all_dexs_clearinghouse_states(data)
        enabled_dexes = {
            str(item.dex_name or "").lower()
            for item in HyperliquidDexRegistry(self.settings).enabled_dexes()
        }
        states = {
            dex: payload
            for dex, payload in states.items()
            if dex in enabled_dexes
        }
        if not states:
            self._schedule_follower_state_refresh(
                enabled_dexes,
                reason="FOLLOWER_ALL_DEXS_CLEARINGHOUSE_WS_EMPTY",
            )
            return
        observed_at = ws_received_at or datetime.now(timezone.utc)
        parsed_states = []
        for dex, payload in states.items():
            try:
                parsed_states.append(
                    parse_account_state(
                        role=FOLLOWER,
                        address=follower,
                        dex=dex,
                        clearinghouse_state=payload,
                        account_label=(
                            f"My Hyperliquid Follower Account / {dex_display_name(dex)}"
                        ),
                        source="FOLLOWER_ALL_DEXS_CLEARINGHOUSE_WS",
                        updated_at=observed_at,
                        price_mids=self.price_cache.fresh_mids_for_dex(dex),
                    )
                )
            except Exception as exc:
                log.warning(
                    "follower_all_dexs_state_parse_failed",
                    dex=dex,
                    error=redact_text(exc)[:200],
                )
        if not parsed_states:
            self._schedule_follower_state_refresh(
                enabled_dexes,
                reason="FOLLOWER_ALL_DEXS_CLEARINGHOUSE_WS_PARSE_FAILED",
            )
            return
        parsed_dexes = {str(state.dex or "").lower() for state in parsed_states}
        async with self.db_session_factory() as db:
            for state in parsed_states:
                await save_account_state(db, state)
            await self._reconcile_manual_position_guards(db, dexes=parsed_dexes)
            await db.commit()
        # Trust is published only after every saved state is durable. A failed
        # transaction therefore falls back to REST rather than blessing a
        # memory-only snapshot.
        for dex in parsed_dexes:
            self._follower_position_stream_observed_at[dex] = observed_at

    def _schedule_follower_state_refresh(self, dexes: list[str] | set[str], *, reason: str) -> None:
        normalized = {str(dex or "").lower() for dex in dexes}
        if not normalized:
            normalized = {dex.dex_name for dex in HyperliquidDexRegistry(self.settings).enabled_dexes()}
        self._follower_state_refresh_pending_dexes.update(normalized)
        for dex in normalized:
            self._follower_state_refresh_reasons[dex] = reason
            task = self._follower_state_refresh_tasks.get(dex)
            if task is not None and not task.done():
                continue
            self._start_follower_state_refresh_task(dex)

    def _follower_position_stream_is_trusted(self, dex: str) -> bool:
        """Use the all-DEX state stream only while its liveness is proven.

        The subscription delivers an authoritative initial snapshot and
        causally ordered updates.  Follower fills are handled on the same
        connection and still install their durable manual-position guard.
        Disconnects clear this trust immediately; a quiet/stalled stream
        expires quickly and falls back to the existing REST refresh path.
        """

        if (
            not self.state.market_ws_connected
            or not self.state.follower_all_dexs_clearinghouse_subscribed
        ):
            return False
        observed_at = self._follower_position_stream_observed_at.get(
            str(dex or "").lower()
        )
        if observed_at is None:
            return False
        return observed_at >= datetime.now(timezone.utc) - timedelta(
            seconds=FOLLOWER_POSITION_STREAM_TRUST_SECONDS
        )

    def _start_follower_state_refresh_task(self, dex: str) -> None:
        task = asyncio.create_task(self._follower_state_refresh_worker(dex=dex))
        self._follower_state_refresh_tasks[dex] = task
        self._background_tasks.add(task)
        task.add_done_callback(self._background_task_done)
        task.add_done_callback(
            lambda completed, dex=dex: self._follower_state_refresh_done(dex, completed)
        )

    def _follower_state_refresh_done(self, dex: str, task: asyncio.Task) -> None:
        if self._follower_state_refresh_tasks.get(dex) is not task:
            return
        self._follower_state_refresh_tasks.pop(dex, None)
        # A request can arrive after the worker observes an empty pending set
        # but before its done callback runs.  Restart here so that narrow race
        # cannot strand a manual-fill confirmation or a leader-fill prefetch.
        if dex in self._follower_state_refresh_pending_dexes and not self._stopped.is_set():
            self._start_follower_state_refresh_task(dex)

    async def _follower_state_refresh_worker(self, *, dex: str) -> None:
        while dex in self._follower_state_refresh_pending_dexes:
            # Coalesce every request that arrives during the REST cooldown into
            # this single sleeping task.  Previously a sustained fill burst
            # could repeatedly open/commit empty DB sessions while the actual
            # request was rate-limited, an avoidable scheduler/SQL livelock.
            retry_at = self._follower_rest_refresh_next_at.get(dex, 0.0)
            delay = retry_at - asyncio.get_running_loop().time()
            if delay > 0:
                try:
                    await asyncio.wait_for(self._stopped.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                if self._stopped.is_set():
                    return
            self._follower_state_refresh_pending_dexes.discard(dex)
            reason = self._follower_state_refresh_reasons.pop(dex, "FOLLOWER_STATE_REFRESH")
            refreshed = await self._refresh_follower_positions_for_dexes([dex], source=reason)
            if refreshed:
                # A stale-position fill used to wait for the fixed durable retry
                # deadline even when this refresh had already completed.  Move
                # only those durable freshness retries back to "ready now" and
                # wake the inbox.  The fill is still claimed through the normal
                # market FIFO/exactly-once path; this only removes blind sleep.
                await self._wake_stale_follower_position_retries(dex)

    async def _wake_stale_follower_position_retries(self, dex: str) -> int:
        now = datetime.now(timezone.utc)
        async with self.db_session_factory() as db:
            if not isinstance(db, AsyncSession):
                return 0
            result = await db.execute(
                update(SourceFill)
                .where(SourceFill.execution_account == self.execution_scope)
                .where(SourceFill.processed_at.is_(None))
                .where(SourceFill.is_snapshot.is_(False))
                .where(SourceFill.dex == str(dex or "").lower())
                .where(SourceFill.last_processing_error.ilike("%follower position state is stale%"))
                .values(next_retry_at=now, updated_at=now)
            )
            await db.commit()
        awakened = max(0, int(getattr(result, "rowcount", 0) or 0))
        if awakened:
            self._schedule_durable_replay_wakeup(0.0)
        return awakened

    async def _await_fresh_follower_state_for_retry(self, dex: str) -> bool:
        """Wait for the already-prefetched DEX snapshot, then verify freshness.

        This wait runs in the per-market fill worker, never in either websocket
        receive loop.  Other markets and leader sockets therefore remain fully
        concurrent.  A timeout leaves the fill in the durable inbox and uses the
        existing bounded retry path.
        """
        dex_name = str(dex or "").lower()
        self._schedule_follower_state_refresh(
            {dex_name},
            reason="STALE_FOLLOWER_POSITION_RETRY",
        )
        task = self._follower_state_refresh_tasks.get(dex_name)
        if task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=0.75)
            except asyncio.TimeoutError:
                return False
            except asyncio.CancelledError:
                raise
            except Exception:
                return False

        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return False
        async with self.db_session_factory() as db:
            state = await db.scalar(
                select(LatestAccountState)
                .where(LatestAccountState.role == FOLLOWER)
                .where(LatestAccountState.address == follower.lower())
                .where(LatestAccountState.dex == dex_name)
                .limit(1)
            )
        refreshed_at = _state_updated_at(state)
        stale_seconds = max(
            0.5,
            float(getattr(self.settings, "account_state_stale_seconds", 2) or 2),
        )
        return bool(
            refreshed_at is not None
            and refreshed_at >= datetime.now(timezone.utc) - timedelta(seconds=stale_seconds)
        )

    async def _reconcile_manual_position_guards(self, db: Any, *, dexes: set[str] | None = None) -> int:
        persistent_by_scope: dict[tuple[str, str], FollowerMarketGuard] = {}
        if isinstance(db, AsyncSession):
            stmt = (
                select(FollowerMarketGuard)
                .where(FollowerMarketGuard.execution_account == self.execution_scope)
                .where(FollowerMarketGuard.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(FollowerMarketGuard.active.is_(True))
            )
            if dexes is not None:
                stmt = stmt.where(
                    FollowerMarketGuard.dex.in_({str(dex or "").lower() for dex in dexes})
                )
            persistent_rows = (await db.execute(stmt)).scalars().all()
            for row in persistent_rows:
                key = (str(row.dex or "").lower(), str(row.canonical_coin or "").upper())
                persistent_by_scope[key] = row
                parsed = parse_coin(key[1], default_dex=key[0])
                self.manual_position_guard.mark(
                    MarketKey(
                        dex=key[0],
                        coin=parsed.coin,
                        canonical_coin=key[1],
                        raw_coin=key[1],
                        asset_id=None,
                        venue_symbol=key[1],
                    ),
                    reason=row.reason or "durable unmatched follower fill guard",
                    observed_at=row.observed_at,
                    position_version=int(row.position_version or 0),
                    expected_position_side=row.expected_position_side,
                    expected_position_qty=row.expected_position_qty,
                    expected_position_relation=row.expected_position_relation,
                    position_change_confirmed_at=row.position_change_confirmed_at,
                )
        entries = self.manual_position_guard.entries()
        if not entries:
            return 0
        allowed_dexes = {str(dex or "").lower() for dex in dexes} if dexes is not None else None
        cleared = 0
        for entry in entries:
            if allowed_dexes is not None and entry.dex not in allowed_dexes:
                continue
            parsed = parse_coin(entry.canonical_coin, default_dex=entry.dex)
            market = MarketKey(
                dex=entry.dex,
                coin=parsed.coin,
                canonical_coin=entry.canonical_coin,
                raw_coin=entry.canonical_coin,
                asset_id=None,
                venue_symbol=entry.canonical_coin,
            )
            follower_qty_by_side, follower_state_at = await self.engine._follower_position_qtys_with_state_at(db, market)
            allocation_qty_by_side, _allocation_latest_reconcile_at = (
                await self.engine._allocation_sum_qtys_with_latest_reconcile(db, market)
            )
            follower_qty_by_side = self.engine.pending_intents.effective_qtys(
                dex=market.dex,
                canonical_coin=market.canonical_coin,
                base_qtys=follower_qty_by_side,
            )
            allocation_qty_by_side = self.engine.pending_intents.effective_qtys(
                dex=market.dex,
                canonical_coin=market.canonical_coin,
                base_qtys=allocation_qty_by_side,
            )
            allocation_mismatch = any(
                abs(
                    Decimal(follower_qty_by_side.get(side, 0))
                    - Decimal(allocation_qty_by_side.get(side, 0))
                )
                > ALLOCATION_TRANSITION_TOLERANCE
                for side in (PositionSide.LONG, PositionSide.SHORT)
            )
            before = self.manual_position_guard.active_entry(market)
            confirmation_before = before.position_change_confirmed_at if before is not None else None
            confirmation_at = self.manual_position_guard.confirm_if_observed(
                market,
                follower_qty_by_side=follower_qty_by_side,
                allocation_qty_by_side=allocation_qty_by_side,
                follower_state_at=follower_state_at,
            )
            persistent = persistent_by_scope.get((entry.dex, entry.canonical_coin.upper()))
            if persistent is not None and confirmation_before is None and confirmation_at is not None:
                persistent.position_change_confirmed_at = confirmation_at
                persistent.updated_at = datetime.now(timezone.utc)
            self.manual_position_guard.reconcile(
                market,
                unmanaged_qty_by_side=_unmanaged_follower_position_qtys(
                    follower_qty_by_side=follower_qty_by_side,
                    allocation_qty_by_side=allocation_qty_by_side,
                ),
                follower_state_at=follower_state_at,
                allocation_mismatch=allocation_mismatch,
                follower_qty_by_side=follower_qty_by_side,
                allocation_qty_by_side=allocation_qty_by_side,
            )
            if before is not None and self.manual_position_guard.active_entry(market) is None:
                if persistent is not None:
                    persistent.active = False
                    persistent.reconciled_at = datetime.now(timezone.utc)
                    persistent.updated_at = persistent.reconciled_at
                cleared += 1
        return cleared

    async def _persist_fill_inbox(self, events: list[FillEvent]) -> None:
        """Commit fills before they depend on an in-memory worker queue."""
        if not events:
            return
        async with self.db_session_factory() as db:
            await self._persist_fill_inbox_rows(db, events)
            await db.commit()

    def _schedule_unpersisted_fill_backfill(self, events: list[FillEvent]) -> None:
        """Make a rolled-back live batch eligible for the one-second backfill loop."""
        event_times = [int(event.time_ms) for event in events if int(event.time_ms or 0) > 0]
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        earliest_ms = min(event_times) if event_times else now_ms
        start_time_ms = max(earliest_ms - LEADER_FILL_BACKFILL_OVERLAP_MS, 0)
        if self._leader_fill_backfill_start_ms is None:
            self._leader_fill_backfill_start_ms = start_time_ms
        else:
            self._leader_fill_backfill_start_ms = min(
                self._leader_fill_backfill_start_ms,
                start_time_ms,
            )

    async def _make_deferred_fill_batch_durable(self, events: list[FillEvent]) -> bool:
        """Persist a rolled-back live batch or explicitly hand it to backfill.

        A database outage must not terminate the per-market worker.  If even the
        fallback inbox commit fails, the durable cursor is still unchanged; the
        websocket backfill loop will therefore fetch the batch again.
        """
        if not events:
            return True
        try:
            await self._persist_fill_inbox(events)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._schedule_unpersisted_fill_backfill(events)
            self.state.last_error = redact_text(exc)[:200]
            log.exception(
                "low_latency_deferred_fill_persist_failed_backfill_scheduled",
                source_fill_id=events[0].source_fill_id,
                fill_count=len(events),
                error=redact_text(exc),
            )
            return False
        return True

    async def _persist_fill_inbox_rows(self, db: Any, events: list[FillEvent]) -> None:
        """Insert inbox/cursor rows into the caller's current transaction.

        Live websocket fills use this inside the same transaction that creates
        their logical order, source outcomes and allocation transition.  A
        retryable planning failure rolls the transaction back and the worker
        falls back to ``_persist_fill_inbox`` before scheduling durable replay.
        """
        if not events:
            return
        values = [
            {
                "source_fill_id": event.source_fill_id,
                "execution_account": self.execution_scope,
                "leader_address": event.leader_address,
                "coin": event.market.coin,
                "dex": event.market.dex,
                "canonical_coin": event.market.canonical_coin,
                "raw_coin": event.market.raw_coin,
                "asset_id": event.market.asset_id,
                "side": event.side,
                "price": event.price,
                "size": event.size,
                "source_time_ms": event.time_ms,
                "ws_received_at": event.ws_received_at,
                "raw_fill": event.raw,
                "is_snapshot": event.is_snapshot,
                "processed_at": None,
                "processing_attempts": 0,
            }
            for event in events
        ]
        if isinstance(db, AsyncSession):
            # Serialize durable arrival assignment per follower market before
            # inserting rows. This keeps the source-fill id sequence aligned
            # with the later FIFO claim across concurrent leader streams.
            market_lock_keys = sorted(
                {_market_arrival_key(event.market, self.execution_scope) for event in events}
            )
            for market_lock_key in market_lock_keys:
                await db.execute(
                    text("SELECT pg_advisory_xact_lock(:market_key)"),
                    {"market_key": market_lock_key},
                )
        await db.execute(
            insert(SourceFill)
            .values(values)
            .on_conflict_do_nothing(index_elements=[SourceFill.source_fill_id])
        )
        cursor_candidates: dict[str, tuple[int, int | None]] = {}
        for event in events:
            if event.is_snapshot or event.time_ms <= 0:
                continue
            address = normalize_leader_address(event.leader_address)
            candidate = (int(event.time_ms), _event_tid(event))
            current = cursor_candidates.get(address)
            if current is None or _fill_cursor_key(candidate) > _fill_cursor_key(current):
                cursor_candidates[address] = candidate
        for address, (time_ms, tid) in cursor_candidates.items():
            cursor_insert = insert(LeaderFillCursor).values(
                leader_address=address,
                last_fill_time_ms=time_ms,
                last_fill_tid=tid,
                backfilled_through_ms=0,
                updated_at=datetime.now(timezone.utc),
            )
            excluded = cursor_insert.excluded
            await db.execute(
                cursor_insert.on_conflict_do_update(
                    index_elements=[LeaderFillCursor.leader_address],
                    set_={
                        "last_fill_tid": case(
                            (
                                excluded.last_fill_time_ms > LeaderFillCursor.last_fill_time_ms,
                                excluded.last_fill_tid,
                            ),
                            (
                                excluded.last_fill_time_ms == LeaderFillCursor.last_fill_time_ms,
                                func.greatest(
                                    func.coalesce(LeaderFillCursor.last_fill_tid, 0),
                                    func.coalesce(excluded.last_fill_tid, 0),
                                ),
                            ),
                            else_=LeaderFillCursor.last_fill_tid,
                        ),
                        "last_fill_time_ms": func.greatest(
                            LeaderFillCursor.last_fill_time_ms,
                            excluded.last_fill_time_ms,
                        ),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )
            )

    async def _durable_replay_loop(self) -> None:
        active_interval = max(
            0.01,
            float(getattr(self.settings, "durable_fill_replay_interval_seconds", 0.1) or 0.1),
        )
        idle_interval = max(
            active_interval,
            float(getattr(self.settings, "durable_fill_replay_idle_seconds", 1.0) or 1.0),
        )
        order_resume_interval = max(
            active_interval,
            float(getattr(self.settings, "durable_order_resume_scan_seconds", 1.0) or 1.0),
        )
        loop = asyncio.get_running_loop()
        next_order_resume_at = 0.0
        last_replay_scan_at = loop.time()
        while not self._stopped.is_set():
            replayed = 0
            resumed = 0
            try:
                loop_time = loop.time()
                hot_path_busy = self._hot_path_busy()
                replay_scan_due = _durable_replay_should_scan(
                    hot_path_busy=hot_path_busy,
                    wakeup_requested=self._durable_replay_wakeup.is_set(),
                    now=loop_time,
                    last_scan_at=last_replay_scan_at,
                )
                if not replay_scan_due:
                    self.state.background_cycles_deferred_for_hot_path += 1
                else:
                    replayed = await self._replay_unprocessed_fills_once()
                    self.state.durable_replay_scan_count += 1
                    last_replay_scan_at = loop.time()
                    now = loop.time()
                    if replayed or now >= next_order_resume_at:
                        resumed = await self._resume_unstarted_orders_once()
                        self.state.durable_order_resume_scan_count += 1
                        next_order_resume_at = loop.time() + order_resume_interval
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = f"durable replay: {redact_text(exc)[:160]}"
                log.exception("low_latency_durable_replay_failed", error=redact_text(exc))
            now = loop.time()
            wait_seconds = (
                active_interval
                if self._hot_path_busy()
                else _durable_replay_wait_seconds(
                    active_interval=active_interval,
                    idle_interval=idle_interval,
                    order_resume_interval=order_resume_interval,
                    now=now,
                    next_order_resume_at=next_order_resume_at,
                    replayed=replayed,
                    resumed=resumed,
                )
            )
            if self._hot_path_busy() and not self._durable_replay_wakeup.is_set():
                # A continuously busy live queue must not starve a durable
                # inbox row.  Shorten the final hot-path sleep so the next
                # scan happens at the deadline rather than one whole polling
                # interval after it.
                until_forced_scan = (
                    DURABLE_REPLAY_MAX_HOT_PATH_DEFER_SECONDS
                    - (now - last_replay_scan_at)
                )
                wait_seconds = min(wait_seconds, max(0.01, until_forced_scan))
            if not replayed and not resumed and wait_seconds > active_interval:
                self.state.durable_replay_idle_wait_count += 1
            try:
                await asyncio.wait_for(
                    self._durable_replay_wakeup.wait(),
                    timeout=wait_seconds,
                )
            except asyncio.TimeoutError:
                pass
            else:
                # Clear immediately after consuming the wakeup. Any retry
                # discovered by the following replay pass can then schedule a
                # new precise deadline without spawning helper tasks per tick.
                self._durable_replay_wakeup.clear()

    def _hot_path_busy(self) -> bool:
        """Return true while a live fill or exchange submit needs priority."""
        return bool(self._queued_source_fill_ids or self._queued_submit_order_ids)

    def _schedule_durable_replay_wakeup(self, delay_seconds: float) -> None:
        """Wake the durable inbox at an exact retry deadline.

        The periodic replay scan remains the crash-safe fallback. This timer
        only removes up to one polling interval of avoidable latency from a
        fill that is already durable and waiting on a transient condition.
        """
        loop = asyncio.get_running_loop()
        delay = max(float(delay_seconds), 0.0)
        wake_at = loop.time() + delay
        if self._durable_replay_wakeup.is_set():
            return
        if (
            self._durable_replay_wakeup_handle is not None
            and not self._durable_replay_wakeup_handle.cancelled()
            and self._durable_replay_wakeup_at is not None
            and self._durable_replay_wakeup_at <= wake_at
        ):
            return
        if self._durable_replay_wakeup_handle is not None:
            self._durable_replay_wakeup_handle.cancel()

        def wake() -> None:
            self._durable_replay_wakeup_handle = None
            self._durable_replay_wakeup_at = None
            self._durable_replay_wakeup.set()

        self._durable_replay_wakeup_at = wake_at
        self._durable_replay_wakeup_handle = loop.call_later(delay, wake)

    async def _replay_unprocessed_fills_once(self) -> int:
        now = datetime.now(timezone.utc)
        limit = max(1, int(getattr(self.settings, "durable_fill_replay_batch_size", 1000) or 1000))
        async with self.db_session_factory() as db:
            rows = (
                await db.execute(
                    _durable_market_head_fill_query(
                        execution_scope=self.execution_scope,
                        now=now,
                        limit=limit,
                    )
                )
            ).scalars().all()
            if not rows:
                return 0
            addresses = sorted({normalize_leader_address(row.leader_address) for row in rows})
            leaders = (
                await db.execute(
                    select(LeaderConfig).where(func.lower(LeaderConfig.leader_address).in_(addresses))
                )
            ).scalars().all()

        leader_by_address = {
            normalize_leader_address(item.leader_address): item
            for item in leaders
        }
        missing_leader_events: list[FillEvent] = []
        replay_items: list[tuple[FillEvent, LeaderConfig]] = []
        for row in rows:
            event = _fill_event_from_source_row(row)
            address = normalize_leader_address(row.leader_address)
            if address not in leader_by_address:
                missing_leader_events.append(event)
                continue
            replay_items.append((event, leader_by_address[address]))
        if missing_leader_events:
            await self._record_fill_processing_failure(
                missing_leader_events,
                RuntimeError("leader configuration missing for durable fill replay"),
            )
        replayed = 0
        # Do not regroup by leader here: regrouping A1, B1, A2 as A1, A2, B1
        # would change first-arrival order for a shared market after restart.
        for event, replay_leader in replay_items:
            # The database query above is authoritative: this row is
            # unprocessed and due now. Never let a stale in-memory
            # completed/suppressed cache entry hide it. The queue membership
            # set plus the durable claim still prevent concurrent duplicates.
            await self._enqueue_fill_events(
                [event],
                replay_leader,
                persist=False,
                authoritative_replay=True,
            )
            replayed += 1
        return replayed

    async def _resume_unstarted_orders_once(self) -> int:
        now = datetime.now(timezone.utc)
        stale_after = max(
            2.0,
            float(getattr(self.settings, "order_recovery_stale_pending_submit_seconds", 10.0) or 10.0),
        )
        stale_before = now - timedelta(seconds=stale_after)
        async with self.db_session_factory() as db:
            await db.execute(
                update(ExecutionOrder)
                .where(ExecutionOrder.source_type == "AUTO_COPY")
                .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(ExecutionOrder.venue_account == self.execution_scope)
                .where(ExecutionOrder.status == "SUBMITTING")
                .where(ExecutionOrder.order_submit_started_at.is_(None))
                .where(ExecutionOrder.updated_at < stale_before)
                .values(status="PENDING_SUBMIT", updated_at=now)
            )
            rows = (
                await db.execute(
                    select(ExecutionOrder)
                    .where(ExecutionOrder.source_type == "AUTO_COPY")
                    .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                    .where(ExecutionOrder.venue_account == self.execution_scope)
                    .where(ExecutionOrder.status == "PENDING_SUBMIT")
                    .where(ExecutionOrder.order_submit_started_at.is_(None))
                    .order_by(ExecutionOrder.created_at, ExecutionOrder.id)
                    .limit(max(1, int(getattr(self.settings, "durable_fill_replay_batch_size", 1000) or 1000)))
                )
            ).scalars().all()
            source_ids = [row.source_fill_id for row in rows if row.source_fill_id]
            allocation_ids = [row.allocation_id for row in rows if row.allocation_id is not None]
            source_rows = (
                await db.execute(
                    select(SourceFill)
                    .where(SourceFill.execution_account == self.execution_scope)
                    .where(SourceFill.source_fill_id.in_(source_ids))
                )
            ).scalars().all() if source_ids else []
            allocations = (
                await db.execute(
                    select(LeaderPositionAllocationRecord).where(
                        LeaderPositionAllocationRecord.id.in_(allocation_ids)
                    ).where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
                )
            ).scalars().all() if allocation_ids else []
            await db.commit()

        source_by_id = {row.source_fill_id: row for row in source_rows}
        allocation_by_id = {row.id: row for row in allocations}
        resumed = 0
        for order in rows:
            source_row = source_by_id.get(order.source_fill_id)
            allocation = allocation_by_id.get(order.allocation_id)
            if source_row is None or allocation is None:
                continue
            self.engine.pending_intents.reserve(order, allocation)
            if not self.engine.pending_intents.has_active_order(order):
                continue
            await self._enqueue_submit_order(order, _fill_event_from_source_row(source_row))
            resumed += 1
        return resumed

    async def _record_fill_processing_failure(
        self,
        events: list[FillEvent],
        exc: Exception,
        *,
        retry_delay_override_seconds: float | None = None,
        follower_refresh_already_requested: bool = False,
    ) -> None:
        if _follower_position_freshness_retry(exc) and not follower_refresh_already_requested:
            self._schedule_follower_state_refresh(
                {event.market.dex for event in events},
                reason="STALE_FOLLOWER_POSITION_RETRY",
            )
        source_fill_ids = _flatten_source_fill_ids(events)
        if not source_fill_ids:
            return
        now = datetime.now(timezone.utc)
        base = max(0.01, float(getattr(self.settings, "durable_fill_retry_base_seconds", 0.05) or 0.05))
        cap = max(base, float(getattr(self.settings, "durable_fill_retry_max_seconds", 5.0) or 5.0))
        earliest_retry_delay: float | None = None
        async with self.db_session_factory() as db:
            rows = (
                await db.execute(
                    select(SourceFill.source_fill_id, SourceFill.processing_attempts)
                    .where(SourceFill.execution_account == self.execution_scope)
                    .where(SourceFill.source_fill_id.in_(source_fill_ids))
                    .where(SourceFill.processed_at.is_(None))
                )
            ).all()
            for source_fill_id, attempts in rows:
                next_attempt = int(attempts or 0) + 1
                delay = _durable_fill_retry_delay_seconds(
                    exc,
                    attempt=next_attempt,
                    base=base,
                    cap=cap,
                )
                if retry_delay_override_seconds is not None:
                    delay = max(0.0, float(retry_delay_override_seconds))
                earliest_retry_delay = (
                    delay
                    if earliest_retry_delay is None
                    else min(earliest_retry_delay, delay)
                )
                await db.execute(
                    update(SourceFill)
                    .where(SourceFill.execution_account == self.execution_scope)
                    .where(SourceFill.source_fill_id == source_fill_id)
                    .where(SourceFill.processed_at.is_(None))
                    .values(
                        processing_attempts=next_attempt,
                        last_attempt_at=now,
                        next_retry_at=now + timedelta(seconds=delay),
                        last_processing_error=redact_text(exc)[:2000],
                        updated_at=now,
                    )
                )
            await db.commit()
        if earliest_retry_delay is not None:
            self._schedule_durable_replay_wakeup(earliest_retry_delay)

    def _fill_queue_key(self, event: FillEvent, leader: LeaderConfig) -> tuple[str, str, str]:
        return (
            ExecutionVenue.HYPERLIQUID.value,
            str(event.market.dex or "").lower(),
            event.market.canonical_coin.upper(),
        )

    async def _acquire_fill_ingress_locks(
        self,
        events: list[FillEvent],
        leader: LeaderConfig,
    ) -> list[asyncio.Lock]:
        keys = sorted({self._fill_queue_key(event, leader) for event in events})
        async with self._fill_ingress_guard:
            locks = [self._fill_ingress_locks.setdefault(key, asyncio.Lock()) for key in keys]
        acquired: list[asyncio.Lock] = []
        try:
            for lock in locks:
                await lock.acquire()
                acquired.append(lock)
            return acquired
        except BaseException:
            # Cancellation between the first and last acquisition used to leak
            # the already-held prefix forever.  Sorted acquisition prevents
            # cycles; this rollback makes partial acquisition cancellation-safe.
            for lock in reversed(acquired):
                lock.release()
            raise

    async def _enqueue_fill_event(self, event: FillEvent, leader: LeaderConfig) -> None:
        await self._enqueue_fill_events([event], leader)

    async def _enqueue_fill_events(
        self,
        events: list[FillEvent],
        leader: LeaderConfig,
        *,
        persist: bool = True,
        ensure_persist_in_worker: bool = False,
        authoritative_replay: bool = False,
    ) -> None:
        if not events:
            return
        ingress_locks = await self._acquire_fill_ingress_locks(events, leader)
        queued_any = False
        refresh_dexes: set[str] = set()
        try:
            # Both websocket handlers can pass the optimistic filter before
            # either one acquires this market's ingress lock. Recheck here so a
            # very fast winning channel cannot finish and leave the loser doing
            # a redundant durable claim/database round trip.
            if not events[0].is_snapshot:
                if authoritative_replay:
                    # PostgreSQL has proven these rows are unfinished. Erase
                    # all contradictory memory-only hints before queueing; the
                    # queue membership set and durable DB claims remain the
                    # duplicate-prevention authorities.
                    await self._forget_suppressed_events(events)
                    await self._forget_completed_events(events)
                else:
                    events = await self._filter_suppressed_events(events)
                    if not events:
                        return
            fill_engine = isinstance(self.engine, FillDrivenExecutionEngine)
            if persist and fill_engine:
                await self._persist_fill_inbox(events)
            grouped: OrderedDict[tuple[str, str, str], list[FillEvent]] = OrderedDict()
            for event in events:
                grouped.setdefault(self._fill_queue_key(event, leader), []).append(event)
            async with self._fill_queue_guard:
                for key, key_events in grouped.items():
                    queue = self._fill_queues.get(key)
                    if queue is None:
                        queue = asyncio.Queue()
                        self._fill_queues[key] = queue
                    for event in key_events:
                        if event.source_fill_id in self._queued_source_fill_ids:
                            continue
                        self._queued_source_fill_ids.add(event.source_fill_id)
                        if not event.is_snapshot:
                            refresh_dexes.add(event.market.dex)
                        queued_event = replace(
                            event,
                            queue_enqueued_at=datetime.now(timezone.utc),
                        )
                        queue.put_nowait(
                            (
                                queued_event,
                                leader,
                                bool(ensure_persist_in_worker and fill_engine),
                            )
                        )
                        queued_any = True
                    worker = self._fill_workers.get(key)
                    if worker is None or worker.done():
                        worker = asyncio.create_task(self._fill_worker(key, queue))
                        self._fill_workers[key] = worker
            if (persist or ensure_persist_in_worker) and fill_engine and refresh_dexes:
                # Only the channel that actually queued a new fill starts the
                # prefetch. The duplicate channel must not double REST traffic.
                refresh_dexes = {
                    dex
                    for dex in refresh_dexes
                    if not self._follower_position_stream_is_trusted(dex)
                }
                if refresh_dexes:
                    self._schedule_follower_state_refresh(
                        refresh_dexes,
                        reason="LEADER_FILL_POSITION_PRECHECK",
                    )
        finally:
            for lock in reversed(ingress_locks):
                lock.release()
        if queued_any:
            # Give the market worker an immediate scheduling point instead of
            # letting a burst of websocket callbacks monopolize the event loop.
            await asyncio.sleep(0)

    async def _fill_worker(
        self,
        key: tuple[str, str, str],
        queue: asyncio.Queue[tuple[FillEvent, LeaderConfig, bool]],
    ) -> None:
        while not self._stopped.is_set():
            first_event, first_leader, first_needs_persist = await queue.get()
            first_event = replace(
                first_event,
                fill_worker_started_at=datetime.now(timezone.utc),
            )
            batch: list[tuple[FillEvent, LeaderConfig, bool]] = [
                (first_event, first_leader, first_needs_persist)
            ]
            while True:
                try:
                    event, leader, needs_persist = queue.get_nowait()
                    batch.append(
                        (
                            replace(
                                event,
                                fill_worker_started_at=datetime.now(timezone.utc),
                            ),
                            leader,
                            needs_persist,
                        )
                    )
                except asyncio.QueueEmpty:
                    break
            events = [event for event, _leader, _needs_persist in batch]
            deferred_persist_events = [
                event for event, _leader, needs_persist in batch if needs_persist
            ]
            leader_by_source_fill_id = {
                event.source_fill_id: leader for event, leader, _needs_persist in batch
            }
            skipped_events: list[FillEvent] = []
            try:
                selected_events = events
                if not first_event.is_snapshot:
                    selected_events, fragment_skipped, _has_order_keys = _aggregate_same_order_fills(events)
                    selected_events, lifecycle_skipped = _coalesce_queued_lifecycle_fills(selected_events)
                    skipped_events = [*fragment_skipped, *lifecycle_skipped]
                    if skipped_events:
                        await self._remember_suppressed_events(skipped_events)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                durable = await self._make_deferred_fill_batch_durable(
                    deferred_persist_events
                )
                self.state.last_error = redact_text(exc)[:200]
                log.exception(
                    "low_latency_fill_worker_batch_prepare_failed",
                    queue_key=":".join(key),
                    dex=key[1],
                    canonical_coin=key[2],
                    source_fill_id=first_event.source_fill_id,
                    error=redact_text(exc),
                )
                if durable:
                    await self._record_fill_processing_failure(events, exc)
            else:
                selected_ok = True
                for selected_index, event in enumerate(selected_events):
                    leader = leader_by_source_fill_id.get(event.source_fill_id, first_leader)
                    try:
                        async with self.db_session_factory() as db:
                            if selected_index == 0 and deferred_persist_events:
                                # Common live path: one atomic commit contains
                                # source fills, cursor, source outcomes, order plan
                                # and allocation transition. This removes the
                                # former inbox-commit round trip without weakening
                                # crash recovery or exactly-once constraints.
                                await self._persist_fill_inbox_rows(db, deferred_persist_events)
                            if isinstance(self.engine, FillDrivenExecutionEngine):
                                order = await self.engine.handle_fill(db, event, leader, submit_order=False)
                            else:
                                order = await self.engine.handle_fill(db, event, leader)
                            if order is None and selected_index == 0 and deferred_persist_events:
                                await db.commit()
                            if order is not None and order.status == "PENDING_SUBMIT" and order.id is not None:
                                # handle_fill has already committed the durable
                                # fill outcome, cloid and order plan. Publish the
                                # committed object before AsyncSession context
                                # cleanup so the submit worker can start its CAS
                                # and signed-envelope preparation immediately.
                                # Detaching is synchronous and makes it safe to
                                # attach to the submit worker's session without
                                # an intervening full-row database read.  The
                                # durable SUBMITTING marker CAS remains the sole
                                # authority that permits an exchange send.
                                if isinstance(db, AsyncSession):
                                    db.expunge(order)
                                await self._enqueue_submit_order(order, event)
                        durable_order_confirmed = order is not None or not isinstance(
                            self.engine,
                            FillDrivenExecutionEngine,
                        )
                        if durable_order_confirmed and not event.is_snapshot:
                            # The transaction above has committed (or found the
                            # fill already committed with a durable logical
                            # outcome). Suppress the losing WS delivery in
                            # memory so it does not add a second database round
                            # trip. An order=None result is not proof of a
                            # durable outcome and must never hide an unprocessed
                            # inbox row.
                            await self._remember_completed_events([event])
                        if selected_index == 0:
                            deferred_persist_events = []
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        if selected_index == 0 and deferred_persist_events:
                            # The combined transaction rolled back. Commit the
                            # raw source rows/cursor first so the normal durable
                            # retry record below can never lose this fill.
                            durable = await self._make_deferred_fill_batch_durable(
                                deferred_persist_events
                            )
                            deferred_persist_events = []
                            if not durable:
                                selected_ok = False
                                break
                        selected_ok = False
                        follower_refresh_ready = False
                        if _follower_position_freshness_retry(exc):
                            follower_refresh_ready = await self._await_fresh_follower_state_for_retry(
                                event.market.dex
                            )
                        if not _expected_fill_retry(exc):
                            self.state.last_error = redact_text(exc)[:200]
                            log.exception(
                                "low_latency_fill_worker_failed",
                                queue_key=":".join(key),
                                leader_id=getattr(leader, "id", None),
                                dex=key[1],
                                canonical_coin=key[2],
                                source_fill_id=event.source_fill_id,
                                error=redact_text(exc),
                            )
                        await self._record_fill_processing_failure(
                            [event],
                            exc,
                            retry_delay_override_seconds=0.0 if follower_refresh_ready else None,
                            follower_refresh_already_requested=_follower_position_freshness_retry(exc),
                        )
                    else:
                        if _expected_fill_retry(self.state.last_error):
                            self.state.last_error = None
                if skipped_events and selected_ok:
                    await self._record_skipped_source_fills(skipped_events)
            finally:
                for queued_event, _leader, _needs_persist in batch:
                    self._queued_source_fill_ids.discard(queued_event.source_fill_id)
                    queue.task_done()

    def _submit_queue_key(
        self,
        event: FillEvent,
        *,
        order: ExecutionOrder | None = None,
        order_id: int | None = None,
    ) -> str:
        leader_id = str(order.leader_id or "unknown") if order is not None else "unknown"
        dex = str((order.dex if order is not None else event.market.dex) or "").lower()
        canonical_coin = str(
            (order.canonical_coin if order is not None else event.market.canonical_coin) or ""
        ).upper()
        side = str((order.position_side if order is not None else "") or "").upper()
        base = f"{leader_id}:{dex}:{canonical_coin}:{side}"
        if order is not None and (
            (_open_like_action(order.order_action) and not bool(order.reduce_only))
            or _reduce_like_action(order.order_action)
        ):
            parallelism = max(
                1,
                int(getattr(self.settings, "max_parallel_order_submits_per_market", 8) or 8),
            )
            shard = int(order.id or order_id or 0) % parallelism
            return f"{base}:parallel:{shard}"
        unique = order_id if order is None else None
        return f"{base}:serial" if unique is None else f"{base}:fast:{unique}"

    async def _enqueue_submit_order(self, order_or_id: ExecutionOrder | int, event: FillEvent) -> None:
        order = order_or_id if isinstance(order_or_id, ExecutionOrder) else None
        if order is not None:
            if order.id is None:
                raise ValueError("cannot enqueue submit order without id")
            order_id = int(order.id)
        else:
            order_id = int(order_or_id)
        key = self._submit_queue_key(event, order=order, order_id=order_id)
        published = False
        async with self._submit_queue_guard:
            if order_id in self._queued_submit_order_ids:
                return
            retry_not_before = self._submit_retry_not_before.get(order_id)
            loop_time = asyncio.get_running_loop().time()
            if retry_not_before is not None and retry_not_before > loop_time:
                self._schedule_durable_replay_wakeup(retry_not_before - loop_time)
                return
            self._submit_retry_not_before.pop(order_id, None)
            if order is not None:
                # Store only objects that are actually published.  A duplicate
                # or deferred enqueue must not leave an unowned detached ORM
                # object behind in the live handoff table.
                self._committed_submit_orders.setdefault(order_id, order)
                # handle_fill returns only after its exactly-once plan/outcome/
                # allocation transaction commits. Preserve that handoff instant
                # only for the item we actually publish, without another write.
                self._submit_plan_committed_at.setdefault(
                    order_id,
                    datetime.now(timezone.utc),
                )
            self._queued_submit_order_ids.add(order_id)
            queue = self._submit_queues.get(key)
            if queue is None:
                queue = asyncio.Queue()
                self._submit_queues[key] = queue
            worker = self._submit_workers.get(key)
            if worker is None or worker.done():
                worker = asyncio.create_task(self._submit_worker(key, queue))
                self._submit_workers[key] = worker
            # Publishing the item and selecting/starting its worker must be one
            # atomic queue-lifecycle operation.  Otherwise an idle worker can
            # time out, remove this queue after the producer releases the guard,
            # and strand the newly published item until durable replay runs.
            queue.put_nowait((order_id, event))
            published = True
        if published:
            # Start the exchange-submit worker before the fill planner continues
            # through another burst item. This bounds plan-commit -> worker-start
            # scheduler lag without serializing on the exchange response.
            await asyncio.sleep(0)

    async def _submit_worker(
        self,
        key: str,
        queue: asyncio.Queue[tuple[int, FillEvent]],
    ) -> None:
        while not self._stopped.is_set():
            try:
                order_id, event = await asyncio.wait_for(queue.get(), timeout=10.0)
            except asyncio.TimeoutError:
                # Producers publish while holding this same guard.  Rechecking
                # under the guard closes the idle-retirement race: either the
                # item is already visible and this worker keeps running, or the
                # queue is retired before a producer can select it.
                async with self._submit_queue_guard:
                    if not queue.empty():
                        continue
                    if self._submit_queues.get(key) is queue:
                        self._submit_queues.pop(key, None)
                    if self._submit_workers.get(key) is asyncio.current_task():
                        self._submit_workers.pop(key, None)
                    return
            requeued = False
            try:
                await self._process_submit_queue_item(order_id, event)
                self._submit_retry_counts.pop(int(order_id), None)
                self._submit_retry_not_before.pop(int(order_id), None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # The object used by the failed attempt belonged to the submit
                # session that has just rolled back/closed.  Never carry that
                # ORM state into another attempt: a safe retry must reload the
                # authoritative durable row before it can reach the final CAS.
                self._committed_submit_orders.pop(int(order_id), None)
                retry_safe = False
                if _is_transient_submit_exception(exc):
                    retry_safe = await self._prepare_submit_retry_if_safe(order_id)
                retry_count = self._submit_retry_counts.get(int(order_id), 0)
                if retry_safe:
                    next_retry_count = retry_count + 1
                    self._submit_retry_counts[int(order_id)] = next_retry_count
                    if next_retry_count <= 3:
                        log.warning(
                            "low_latency_submit_worker_retrying",
                            dex=key,
                            order_id=order_id,
                            source_fill_id=event.source_fill_id,
                            retry_count=next_retry_count,
                            error=redact_text(exc)[:300],
                        )
                        queue.put_nowait((order_id, event))
                        requeued = True
                    else:
                        delay = _durable_submit_retry_delay_seconds(next_retry_count)
                        self._submit_retry_not_before[int(order_id)] = (
                            asyncio.get_running_loop().time() + delay
                        )
                        self._schedule_durable_replay_wakeup(delay)
                        log.warning(
                            "low_latency_submit_worker_retry_deferred",
                            dex=key,
                            order_id=order_id,
                            source_fill_id=event.source_fill_id,
                            retry_count=next_retry_count,
                            retry_delay_seconds=delay,
                            error=redact_text(exc)[:300],
                        )
                else:
                    self._submit_retry_counts.pop(int(order_id), None)
                    self._submit_retry_not_before.pop(int(order_id), None)
                    self.state.last_error = redact_text(exc)[:200]
                    log.exception(
                        "low_latency_submit_worker_failed",
                        dex=key,
                        order_id=order_id,
                        source_fill_id=event.source_fill_id,
                        error=redact_text(exc),
                    )
            finally:
                if not requeued:
                    self._queued_submit_order_ids.discard(int(order_id))
                    self._committed_submit_orders.pop(int(order_id), None)
                    self._submit_plan_committed_at.pop(int(order_id), None)
                queue.task_done()

    async def _process_submit_queue_item(self, order_id: int, event: FillEvent) -> None:
        wake_market_waiters = False
        submit_worker_started_at = datetime.now(timezone.utc)
        order_plan_committed_at = self._submit_plan_committed_at.get(int(order_id))
        # Barrier waiting can last until an earlier exchange response arrives. Do not
        # pin a database transaction/connection during that network-dependent wait.
        # The durable plan is mirrored in PendingIntentLedger, so the normal path
        # does not need to open and discard a first session merely to read it.
        barrier_wait_started_at, barrier_wait_done_at = await self._wait_for_submit_barrier(order_id)
        submit_session_started_at = datetime.now(timezone.utc)
        async with self.submit_db_session_factory() as db:
            submit_db_connection_acquire_started_at = datetime.now(timezone.utc)
            if isinstance(db, AsyncSession):
                await db.connection()
            submit_db_connection_acquired_at = datetime.now(timezone.utc)
            submit_order_query_started_at = datetime.now(timezone.utc)
            order = self._committed_submit_orders.get(int(order_id))
            submit_order_source = "committed_memory_handoff"
            if order is not None:
                db.add(order)
            else:
                submit_order_source = "durable_database_reload"
                order = await db.get(ExecutionOrder, order_id)
            submit_order_query_done_at = datetime.now(timezone.utc)
            if order is None:
                return
            if str(order.venue_account or "").lower() != self.execution_scope:
                raise RuntimeError("submit worker refused an order for another execution account")
            submit_order_loaded_at = datetime.now(timezone.utc)
            _trace_set(order, "submit_worker_started_at", submit_worker_started_at)
            if order_plan_committed_at is not None:
                _trace_set(order, "order_plan_committed_at", order_plan_committed_at)
            _trace_set(order, "submit_session_started_at", submit_session_started_at)
            _trace_set(
                order,
                "submit_db_connection_acquire_started_at",
                submit_db_connection_acquire_started_at,
            )
            _trace_set(
                order,
                "submit_db_connection_acquired_at",
                submit_db_connection_acquired_at,
            )
            _trace_set(order, "submit_order_query_started_at", submit_order_query_started_at)
            _trace_set(order, "submit_order_query_done_at", submit_order_query_done_at)
            _trace_set(order, "submit_order_loaded_at", submit_order_loaded_at)
            _trace_set_detail(order, "submit_order_source", submit_order_source)
            _trace_set_detail(
                order,
                "submit_database_pool",
                "isolated_low_latency" if self._submit_database_pool_isolated else "shared_default",
            )
            if barrier_wait_started_at is not None:
                _trace_set(order, "submit_barrier_wait_started_at", barrier_wait_started_at)
                _trace_set(order, "submit_barrier_wait_done_at", barrier_wait_done_at)
                _trace_set_detail(order, "submit_barrier_waited", True)
            status = str(order.status or "").upper()
            if status == "SUBMITTING":
                return
            if status != "PENDING_SUBMIT":
                self.engine.pending_intents.release(order)
                return
            if not self.engine.pending_intents.has_active_order_id(order_id):
                order.status = "BLOCKED"
                order.dry_run = True
                order.error_message = "pending intent missing before submit; blocked to prevent duplicate order"
                db.add(
                    RiskEvent(
                        severity="error",
                        event_type="PENDING_INTENT_MISSING_BLOCKED_ORDER",
                        symbol=order.canonical_coin or order.source_coin,
                        leader_address=order.leader_address,
                        message=order.error_message,
                        metadata_json={
                            "order_id": order.id,
                            "source_fill_id": order.source_fill_id,
                            "cloid": order.cloid,
                        },
                    )
                )
                await db.commit()
                return
            await self.engine.submit_planned_order(db, order, event)
            self._record_submit_latency_sample(order)
            wake_market_waiters = bool(
                _reduce_like_action(order.order_action)
                and str(order.status or "").upper() not in RECOVERY_ORDER_STATUSES
            )
        if wake_market_waiters:
            # A close/reduce finalization may have changed the allocation to
            # CLOSED. Wake durable ownership waiters immediately instead of
            # making the next leader wait for the periodic replay tick.
            await self._replay_unprocessed_fills_once()

    def _record_submit_latency_sample(self, order: ExecutionOrder) -> None:
        metrics = (order.latency_trace or {}).get("metrics") or {}
        actual_send_ms = _int_or_none(metrics.get("ws_to_actual_send_ms"))
        marker_ms = _int_or_none(order.ws_to_submit_ms)
        if actual_send_ms is None and marker_ms is None:
            return
        self._recent_submit_latency_samples.append(
            {
                "order_id": int(order.id) if order.id is not None else None,
                "canonical_coin": order.canonical_coin,
                "order_action": order.order_action,
                "ws_to_submit_ms": marker_ms,
                "ws_to_actual_send_ms": actual_send_ms,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        del self._recent_submit_latency_samples[:-100]

    async def _prepare_submit_retry_if_safe(self, order_id: int) -> bool:
        try:
            async with self.db_session_factory() as db:
                order = await db.get(ExecutionOrder, order_id)
                if order is None:
                    return False
                if str(order.venue_account or "").lower() != self.execution_scope:
                    return False
                status = str(order.status or "").upper()
                if status == "PENDING_SUBMIT":
                    return self.engine.pending_intents.has_active_order(order)
                if status != "SUBMITTING":
                    return False
                if (
                    order.order_submit_started_at is not None
                    or order.order_submit_done_at is not None
                    or order.order_ack_at is not None
                    or order.order_id
                    or order.venue_order_id
                    or order.raw_response
                ):
                    return False
                if not self.engine.pending_intents.has_active_order(order):
                    return False
                order.status = "PENDING_SUBMIT"
                order.updated_at = datetime.now(timezone.utc)
                await db.commit()
                return True
        except Exception as exc:
            self.state.last_error = redact_text(exc)[:200]
            log.warning(
                "low_latency_submit_worker_retry_prepare_failed",
                order_id=order_id,
                error=redact_text(exc),
            )
            return False

    async def _wait_for_submit_barrier(
        self,
        order: ExecutionOrder | int,
    ) -> tuple[datetime | None, datetime | None]:
        wait_started_at: datetime | None = None
        logged = False
        last_durable_reconcile_at = 0.0
        while True:
            barriers = (
                self.engine.pending_intents.submit_barriers_before_order_id(order)
                if isinstance(order, int)
                else self.engine.pending_intents.submit_barriers_before(order)
            )
            if not barriers:
                if wait_started_at is not None:
                    wait_done_at = datetime.now(timezone.utc)
                    if not isinstance(order, int):
                        _trace_set(order, "submit_barrier_wait_done_at", wait_done_at)
                        _trace_set_detail(order, "submit_barrier_waited", True)
                    return wait_started_at, wait_done_at
                return None, None
            if wait_started_at is None:
                wait_started_at = datetime.now(timezone.utc)
                if not isinstance(order, int):
                    _trace_set(order, "submit_barrier_wait_started_at", wait_started_at)
            waited_ms = _delta_ms(wait_started_at, datetime.now(timezone.utc)) or 0
            if not logged and waited_ms >= 100:
                logged = True
                log.warning(
                    "low_latency_submit_barrier_waiting",
                    order_id=order if isinstance(order, int) else order.id,
                    source_fill_id=None if isinstance(order, int) else order.source_fill_id,
                    waited_ms=waited_ms,
                    blockers=[
                        {
                            "order_id": intent.order_id,
                            "source_fill_id": intent.source_fill_id,
                            "order_action": intent.order_action,
                            "reduce_only": intent.reduce_only,
                        }
                        for intent in barriers[:5]
                    ],
                )
            loop_time = asyncio.get_running_loop().time()
            reconcile_interval = _submit_barrier_reconcile_interval_seconds(waited_ms)
            if (
                waited_ms >= 100
                and loop_time - last_durable_reconcile_at >= reconcile_interval
            ):
                # A completed prior order should release its in-memory intent in
                # the normal path.  Reconcile against the durable order state as
                # a self-healing fallback so a lost callback cannot leave the
                # next order behind an immortal memory-only barrier.
                await self._release_resolved_submit_barriers(barriers)
                last_durable_reconcile_at = loop_time
                continue
            await asyncio.sleep(_submit_barrier_poll_delay_seconds(waited_ms))

    async def _release_resolved_submit_barriers(
        self,
        barriers: list[PendingIntent],
    ) -> int:
        order_ids = sorted(
            {
                int(intent.order_id)
                for intent in barriers
                if intent.order_id is not None
            }
        )
        if not order_ids:
            return 0
        async with self.db_session_factory() as db:
            rows = (
                await db.execute(
                    select(ExecutionOrder.id, ExecutionOrder.status).where(
                        ExecutionOrder.id.in_(order_ids)
                    )
                )
            ).all()
        active_ids = {
            int(order_id)
            for order_id, status in rows
            if str(status or "").upper() in RECOVERY_ORDER_STATUSES
        }
        released = 0
        for order_id in order_ids:
            if order_id in active_ids:
                continue
            self.engine.pending_intents.release_order_id(order_id)
            released += 1
        return released

    async def _drain_fill_queues(self) -> None:
        queues = list(self._fill_queues.values())
        if queues:
            await asyncio.gather(*(queue.join() for queue in queues))
        await self._drain_submit_queues()

    async def _drain_submit_queues(self) -> None:
        queues = list(self._submit_queues.values())
        if queues:
            await asyncio.gather(*(queue.join() for queue in queues))

    async def _drain_background_tasks(self) -> None:
        while self._background_tasks:
            tasks = list(self._background_tasks)
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _cancel_fill_workers(self) -> None:
        workers = list(self._fill_workers.values())
        for worker in workers:
            worker.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

    async def _cancel_submit_workers(self) -> None:
        workers = list(self._submit_workers.values())
        for worker in workers:
            worker.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        # Orders left in queues were never handed to a submit session.  Their
        # plans remain durable and will be recovered from PostgreSQL on the
        # next run, so retaining detached objects here can only be stale/leaky.
        self._committed_submit_orders.clear()

    async def _cancel_background_tasks(self) -> None:
        tasks = list(self._background_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _filter_suppressed_events(self, events: list[FillEvent]) -> list[FillEvent]:
        if not events:
            return []
        now = datetime.now(timezone.utc)
        async with self._suppressed_source_fill_guard:
            self._prune_suppressed_source_fill_ids(now)
            return [
                event
                for event in events
                if event.source_fill_id not in self._suppressed_source_fill_ids
                and event.source_fill_id not in self._recently_completed_source_fill_ids
            ]

    async def _remember_completed_events(self, events: list[FillEvent]) -> None:
        source_fill_ids = _flatten_source_fill_ids(events)
        if not source_fill_ids:
            return
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=60)
        async with self._suppressed_source_fill_guard:
            self._prune_suppressed_source_fill_ids(now)
            for source_fill_id in source_fill_ids:
                self._recently_completed_source_fill_ids[source_fill_id] = expires_at
                self._recently_completed_source_fill_ids.move_to_end(source_fill_id)
            while len(self._recently_completed_source_fill_ids) > RECENT_COMPLETED_SOURCE_FILL_MAX:
                # Eviction only drops the memory fast path. The durable unique
                # claim remains authoritative, so this cannot permit a repeat.
                self._recently_completed_source_fill_ids.popitem(last=False)

    async def _remember_suppressed_events(self, events: list[FillEvent]) -> None:
        if not events:
            return
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=60)
        async with self._suppressed_source_fill_guard:
            self._prune_suppressed_source_fill_ids(now)
            for event in events:
                self._suppressed_source_fill_ids[event.source_fill_id] = expires_at
                self._suppressed_source_fill_ids.move_to_end(event.source_fill_id)

    async def _forget_suppressed_events(self, events: list[FillEvent]) -> None:
        if not events:
            return
        async with self._suppressed_source_fill_guard:
            for event in events:
                self._suppressed_source_fill_ids.pop(event.source_fill_id, None)

    async def _forget_completed_events(self, events: list[FillEvent]) -> None:
        """Remove memory-only completion hints for proven missing outcomes."""
        source_fill_ids = _flatten_source_fill_ids(events)
        if not source_fill_ids:
            return
        async with self._suppressed_source_fill_guard:
            for source_fill_id in source_fill_ids:
                self._recently_completed_source_fill_ids.pop(source_fill_id, None)

    def _prune_suppressed_source_fill_ids(self, now: datetime) -> None:
        for cache in (
            self._suppressed_source_fill_ids,
            self._recently_completed_source_fill_ids,
        ):
            while cache:
                _source_fill_id, expires_at = next(iter(cache.items()))
                if expires_at > now:
                    break
                cache.popitem(last=False)

    def _schedule_background_task(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_task_done)

    def _background_task_done(self, task: asyncio.Task) -> None:
        self._background_tasks.discard(task)
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc is not None:
            self.state.last_error = redact_text(exc)[:200]
            log.warning("low_latency_background_task_failed", error=redact_text(exc))

    async def _record_skipped_source_fills(self, events: list[FillEvent]) -> None:
        if not events:
            return
        recorded = False
        missing_outcome_events: list[FillEvent] = []
        try:
            async with self.db_session_factory() as db:
                outcome_source_fill_ids: set[str] | None = None
                if isinstance(db, AsyncSession) and isinstance(self.engine, FillDrivenExecutionEngine):
                    non_snapshot_ids = [
                        event.source_fill_id for event in events if not event.is_snapshot
                    ]
                    outcome_source_fill_ids = set(
                        (
                            await db.execute(
                                select(SourceFillOutcome.source_fill_id).where(
                                    SourceFillOutcome.source_fill_id.in_(non_snapshot_ids)
                                )
                            )
                        ).scalars().all()
                    ) if non_snapshot_ids else set()
                for event in events:
                    processed = not event.is_snapshot
                    if outcome_source_fill_ids is not None and processed:
                        processed = event.source_fill_id in outcome_source_fill_ids
                        if not processed:
                            missing_outcome_events.append(event)
                    await self.engine._record_source_fill(db, event, processed=processed)
                if missing_outcome_events:
                    missing_ids = [event.source_fill_id for event in missing_outcome_events]
                    await db.execute(
                        update(SourceFill)
                        .where(SourceFill.execution_account == self.execution_scope)
                        .where(SourceFill.source_fill_id.in_(missing_ids))
                        .where(
                            ~select(SourceFillOutcome.id)
                            .where(SourceFillOutcome.source_fill_id == SourceFill.source_fill_id)
                            .exists()
                        )
                        .values(
                            processed_at=None,
                            next_retry_at=None,
                            last_processing_error=None,
                            updated_at=datetime.now(timezone.utc),
                        )
                    )
                await db.commit()
            recorded = True
        except Exception as exc:
            self.state.last_error = redact_text(exc)[:200]
            log.warning(
                "low_latency_skipped_source_fill_record_failed",
                error=redact_text(exc),
                source_fill_ids=[event.source_fill_id for event in events[:10]],
                count=len(events),
            )
        if recorded:
            await self._forget_suppressed_events(events)
        if missing_outcome_events:
            # A durable row without an outcome is unfinished by definition.
            # Clear every memory-only completion hint before scheduling replay;
            # otherwise the former 60-second cache TTL can turn a safe replay
            # into a severe execution delay.
            await self._forget_completed_events(missing_outcome_events)
            await self._record_fill_processing_failure(
                missing_outcome_events,
                RetryableFillProcessingError(
                    "coalesced source fill has no durable outcome; returned to replay"
                ),
            )

    async def _record_parse_error(self, leader_address: str, fill: dict[str, Any], message: str) -> None:
        async with self.db_session_factory() as db:
            db.add(
                RiskEvent(
                    severity="warning",
                    event_type="FILL_PARSE_FAILED",
                    symbol=str(fill.get("coin") or ""),
                    leader_address=leader_address,
                    message=redact_text(message),
                    metadata_json={"fill": {k: str(v) for k, v in fill.items() if k in {"coin", "hash", "tid", "oid", "time"}}},
                )
            )
            await db.commit()

    async def _status_loop(self) -> None:
        interval = max(
            0.5,
            float(getattr(self.settings, "watcher_status_refresh_seconds", 2.0) or 2.0),
        )
        while not self._stopped.is_set():
            if self._hot_path_busy():
                self.state.background_cycles_deferred_for_hot_path += 1
                await asyncio.sleep(0.05)
                continue
            started = asyncio.get_running_loop().time()
            try:
                await self.store_status()
            except Exception as exc:
                self.state.last_error = redact_text(exc)[:200]
            elapsed = asyncio.get_running_loop().time() - started
            await asyncio.sleep(max(0.05, interval - elapsed))

    async def _account_value_readiness(
        self,
        db: Any,
        *,
        dexes: list[str],
        active_leaders: list[str],
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        follower = self.settings.hyperliquid_follower_account_address()
        requests: list[tuple[str, str]] = []
        if follower:
            requests.append((FOLLOWER, follower))
        for address in active_leaders:
            normalized = normalize_leader_address(address)
            if normalized:
                requests.append((LEADER, normalized))

        keys = {
            account_abstraction_setting_key(role, address): (role, address)
            for role, address in requests
            if address
        }
        if not keys:
            return {"ready": False, "blockers": ["account abstraction addresses missing"], "details": {}}

        rows = (
            await db.execute(
                select(AppSetting)
                .where(AppSetting.key.in_(list(keys)))
            )
        ).scalars().all()
        payloads = {row.key: dict(row.value) for row in rows if isinstance(row.value, dict)}
        self.engine.cache_account_abstraction_payloads(payloads)
        fixed_leader_values = {
            normalize_leader_address(row.leader_address): _configured_leader_account_value(row)
            for row in (
                await db.execute(
                    select(LeaderConfig)
                    .where(LeaderConfig.enabled.is_(True))
                    .where(LeaderConfig.deleted_at.is_(None))
                )
            ).scalars().all()
        }
        blockers: list[str] = []
        details: dict[str, Any] = {}
        for key, (role, address) in keys.items():
            payload = payloads.get(key)
            scope = f"{role}:{mask_address(address)}"
            details[scope] = {}
            if role == LEADER:
                fixed_value = fixed_leader_values.get(normalize_leader_address(address))
                ready = fixed_value is not None and fixed_value > 0
                reason = None if ready else "fixed leader account value unavailable"
                for dex in dexes:
                    details[scope][dex or "default"] = {
                        "ready": ready,
                        "source": FIXED_LEADER_ACCOUNT_VALUE_SOURCE,
                        "mode": FIXED_LEADER_ACCOUNT_VALUE_MODE,
                        "account_value": str(fixed_value) if fixed_value is not None else None,
                        "reason": reason,
                    }
                if not ready:
                    blockers.append(f"{scope}: {reason}")
                continue
            if payload is None:
                blockers.append(f"{scope} account abstraction cache missing")
                continue
            for dex in dexes:
                label = dex or "default"
                resolved = _resolved_account_value_payload(payload, dex)
                ok, reason = _account_value_payload_ready(resolved, role=role)
                stale = _account_value_payload_is_stale(
                    resolved,
                    max_age_seconds=float(
                        getattr(self.settings, "account_value_max_stale_seconds", 30.0) or 30.0
                    ),
                    now=now,
                )
                details[scope][label] = {
                    "ready": ok,
                    "stale": stale,
                    "source": (resolved or {}).get("account_value_source") or (resolved or {}).get("source"),
                    "mode": (resolved or {}).get("account_abstraction_mode") or (resolved or {}).get("mode"),
                    "account_value": (resolved or {}).get("account_value_used_for_sizing")
                    or (resolved or {}).get("accountValueUsedForSizing"),
                    "reason": reason,
                    "warning": "account value snapshot is stale; using last known good value"
                    if ok and stale
                    else None,
                }
                if not ok:
                    blockers.append(f"{scope} {label}: {reason}")
        return {"ready": not blockers, "blockers": blockers, "details": details}

    async def store_status(self) -> None:
        dexes = [dex.dex_name for dex in HyperliquidDexRegistry(self.settings).enabled_dexes()]
        price_status = self.price_cache.status_by_dex(dexes)
        active = sorted(self.state.active_leaders)
        now = datetime.now(timezone.utc)
        last_age = int((now - self.state.last_ws_event_at).total_seconds() * 1000) if self.state.last_ws_event_at else None
        async with self.db_session_factory() as db:
            # Health/status snapshots are reconstructable and must never force
            # an fsync ahead of a durable order-plan or submit-marker commit.
            # Trading transactions retain PostgreSQL synchronous_commit=on.
            if isinstance(db, AsyncSession):
                await db.execute(text("SET LOCAL synchronous_commit = OFF"))
            allocation_market_rows = (
                await db.execute(
                    select(
                        LeaderPositionAllocationRecord.dex,
                        LeaderPositionAllocationRecord.canonical_coin,
                        LeaderPositionAllocationRecord.hyperliquid_coin,
                    )
                    .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
                    .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                    .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
                    .where(LeaderPositionAllocationRecord.status != "CLOSED")
                    .where(LeaderConfig.enabled.is_(True))
                    .where(LeaderConfig.deleted_at.is_(None))
                    .distinct()
                )
            ).all()
            allocation_dexes = {
                str(dex or "").lower()
                for dex, _canonical, _coin in allocation_market_rows
            }
            status_price_markets = {
                str(canonical or canonical_coin(dex=str(dex or ""), coin=str(coin or "")))
                for dex, canonical, coin in allocation_market_rows
                if canonical or coin
            }
            active_position_markets = (
                await db.execute(
                    select(
                        LatestAccountPosition.dex,
                        LatestAccountPosition.canonical_coin,
                        LatestAccountPosition.coin,
                    )
                    .join(
                        LatestAccountState,
                        LatestAccountState.id == LatestAccountPosition.account_state_id,
                    )
                    .where(LatestAccountState.role == FOLLOWER)
                    .where(
                        LatestAccountState.address
                        == str(self.settings.hyperliquid_follower_account_address() or "").lower()
                    )
                    .where(LatestAccountPosition.active.is_(True))
                    .distinct()
                )
            ).all()
            status_price_markets.update(
                str(canonical or canonical_coin(dex=str(dex or ""), coin=str(coin or "")))
                for dex, canonical, coin in active_position_markets
                if canonical or coin
            )
            required_price_dexes = _required_price_status_dexes(
                enabled_dexes=dexes,
                event_dexes=set(self.state.last_event_time_by_dex),
                allocation_dexes=allocation_dexes,
            )
            price_fresh = _price_status_ready_for_low_latency_live(
                price_status,
                required_price_dexes,
                stale_ms=self.price_cache.stale_ms,
            )
            account_value_status = await self._account_value_readiness(db, dexes=dexes, active_leaders=active)
            follower_position_freshness = await self._active_follower_position_freshness(db, now=now)
            pending_fill_count = int(
                await db.scalar(
                    select(func.count(SourceFill.id))
                    .where(SourceFill.execution_account == self.execution_scope)
                    .where(SourceFill.processed_at.is_(None))
                    .where(SourceFill.is_snapshot.is_(False))
                )
                or 0
            )
            if pending_fill_count:
                # Status polling is an independent liveness watchdog. If a
                # wakeup was ever lost, observing durable unfinished work
                # immediately wakes the replay loop instead of merely
                # reporting the problem after the stuck threshold.
                self._schedule_durable_replay_wakeup(0.0)
            oldest_pending_fill_at = await db.scalar(
                select(func.min(SourceFill.created_at))
                .where(SourceFill.execution_account == self.execution_scope)
                .where(SourceFill.processed_at.is_(None))
                .where(SourceFill.is_snapshot.is_(False))
            )
            retrying_fill_count = int(
                await db.scalar(
                    select(func.count(SourceFill.id))
                    .where(SourceFill.execution_account == self.execution_scope)
                    .where(SourceFill.processed_at.is_(None))
                    .where(SourceFill.is_snapshot.is_(False))
                    .where(SourceFill.processing_attempts > 0)
                )
                or 0
            )
            pending_submit_count = int(
                await db.scalar(
                    select(func.count(ExecutionOrder.id))
                    .where(ExecutionOrder.source_type == "AUTO_COPY")
                    .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                    .where(ExecutionOrder.venue_account == self.execution_scope)
                    .where(ExecutionOrder.status == "PENDING_SUBMIT")
                )
                or 0
            )
            submitting_count = int(
                await db.scalar(
                    select(func.count(ExecutionOrder.id))
                    .where(ExecutionOrder.source_type == "AUTO_COPY")
                    .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                    .where(ExecutionOrder.venue_account == self.execution_scope)
                    .where(ExecutionOrder.status == "SUBMITTING")
                )
                or 0
            )
            unknown_submit_count = int(
                await db.scalar(
                    select(func.count(ExecutionOrder.id))
                    .where(ExecutionOrder.source_type == "AUTO_COPY")
                    .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                    .where(ExecutionOrder.venue_account == self.execution_scope)
                    .where(ExecutionOrder.status == "UNKNOWN")
                )
                or 0
            )
            oldest_unresolved_order_at = await db.scalar(
                select(func.min(ExecutionOrder.created_at))
                .where(ExecutionOrder.source_type == "AUTO_COPY")
                .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(ExecutionOrder.venue_account == self.execution_scope)
                .where(
                    ExecutionOrder.status.in_(["PENDING_SUBMIT", "SUBMITTING", "UNKNOWN"])
                )
            )
            processed_without_outcome_count = int(
                await db.scalar(
                    select(func.count(SourceFill.id))
                    .where(SourceFill.execution_account == self.execution_scope)
                    .where(SourceFill.processed_at.is_not(None))
                    .where(SourceFill.is_snapshot.is_(False))
                    .where(
                        ~select(SourceFillOutcome.id)
                        .where(SourceFillOutcome.source_fill_id == SourceFill.source_fill_id)
                        .exists()
                    )
                )
                or 0
            )
            unresolved_outcome_count = int(
                await db.scalar(
                    _unresolved_source_fill_outcomes_query(self.execution_scope)
                )
                or 0
            )
            stuck_threshold_ms = int(
                max(
                    1.0,
                    float(
                        getattr(self.settings, "durable_pipeline_stuck_seconds", 10.0)
                        or 10.0
                    ),
                )
                * 1000
            )
            oldest_pending_fill_age_ms = _delta_ms(oldest_pending_fill_at, now)
            oldest_unresolved_order_age_ms = _delta_ms(oldest_unresolved_order_at, now)
            durable_inbox_stuck = bool(
                pending_fill_count > 0
                and oldest_pending_fill_age_ms is not None
                and oldest_pending_fill_age_ms >= stuck_threshold_ms
            )
            unresolved_order_count = pending_submit_count + submitting_count + unknown_submit_count
            durable_outbox_stuck = bool(
                unresolved_order_count > 0
                and oldest_unresolved_order_age_ms is not None
                and oldest_unresolved_order_age_ms >= stuck_threshold_ms
            )
            liveness_ready = not durable_inbox_stuck and not durable_outbox_stuck
            stuck_parts: list[str] = []
            if durable_inbox_stuck:
                stuck_parts.append(
                    f"durable inbox has {pending_fill_count} pending fill(s), oldest "
                    f"{oldest_pending_fill_age_ms}ms"
                )
            if durable_outbox_stuck:
                stuck_parts.append(
                    f"durable outbox has {unresolved_order_count} unresolved order(s), oldest "
                    f"{oldest_unresolved_order_age_ms}ms"
                )
            liveness_reason = "; ".join(stuck_parts) or None
            # The human-readable age changes every status tick.  It must not be
            # the deduplication key or one stuck row emits an error plus an
            # expensive traceback/log write every two seconds indefinitely.
            liveness_signature_parts: list[str] = []
            if durable_inbox_stuck:
                liveness_signature_parts.append(f"inbox:{pending_fill_count}")
            if durable_outbox_stuck:
                liveness_signature_parts.append(f"outbox:{unresolved_order_count}")
            liveness_signature = ";".join(liveness_signature_parts) or None
            if liveness_signature != self._liveness_stuck_signature:
                if liveness_signature:
                    log.error(
                        "durable_pipeline_liveness_stuck",
                        reason=liveness_reason,
                        threshold_ms=stuck_threshold_ms,
                    )
                    self.state.last_error = f"DURABLE_PIPELINE_STUCK: {liveness_reason}"
                else:
                    if str(self.state.last_error or "").startswith("DURABLE_PIPELINE_STUCK:"):
                        self.state.last_error = None
                    if self._liveness_stuck_signature:
                        log.info("durable_pipeline_liveness_recovered")
                self._liveness_stuck_signature = liveness_signature
            if liveness_signature:
                # Other background diagnostics must not make a stuck durable
                # pipeline disappear from the primary health/error field.
                self.state.last_error = f"DURABLE_PIPELINE_STUCK: {liveness_reason}"
            ready = (
                self.state.websocket_connected
                and bool(active)
                and self.state.follower_order_updates_subscribed
                and (self.settings.allow_poll_fallback_live or not self.state.poll_fallback_leaders)
                and price_fresh
                and bool(account_value_status.get("ready"))
                and bool(follower_position_freshness.get("ready"))
                and processed_without_outcome_count == 0
                and liveness_ready
            )
            self.state.low_latency_ready = ready
            recent_submit_latency = _recent_submit_latency_summary(
                self._recent_submit_latency_samples,
                threshold_ms=PER_FILL_INTERNAL_LATENCY_WARN_MS,
            )
            payload = {
                "execution_account": self.settings.hyperliquid_follower_account_address(),
                "execution_scope": self.execution_scope,
                "mode": "websocket" if self.state.websocket_connected else "disconnected",
                "low_latency_watcher_running": True,
                "low_latency_primary": self.state.low_latency_primary,
                "low_latency_required_for_live": self.settings.low_latency_required_for_live,
                "websocket_connected": self.state.websocket_connected,
                "market_ws_connected": self.state.market_ws_connected,
                "leader_fills_ws_connected": self.state.leader_fills_ws_connected,
                "low_latency_ready": ready,
                "ready_for_low_latency_live": ready,
                "account_value_ready": bool(account_value_status.get("ready")),
                "account_value_blockers": account_value_status.get("blockers") or [],
                "account_value_readiness": account_value_status.get("details") or {},
                "account_value_refresh_count": self.state.account_value_refresh_count,
                "account_value_last_refresh_at": (
                    self.state.account_value_last_refresh_at.isoformat()
                    if self.state.account_value_last_refresh_at
                    else None
                ),
                "account_value_last_refresh_duration_ms": (
                    self.state.account_value_last_refresh_duration_ms
                ),
                "account_value_refresh_last_error": self.state.account_value_refresh_last_error,
                "follower_position_fresh": bool(follower_position_freshness.get("ready")),
                "follower_position_freshness": follower_position_freshness,
                "poll_fallback_count": len(self.state.poll_fallback_leaders),
                "source": "low_latency_watcher",
                "active_leaders": active,
                "ws_leaders": sorted(self.state.ws_leaders),
                "poll_fallback_leaders": sorted(self.state.poll_fallback_leaders),
                "enabled_dexes": dexes,
                "subscribed_leaders_by_dex": self.state.subscribed_leaders_by_dex,
                "last_event_time_by_dex": self.state.last_event_time_by_dex,
                "last_allocation_sync_at": self.state.last_allocation_sync_at.isoformat()
                if self.state.last_allocation_sync_at
                else None,
                "allocation_sync_count": self.state.allocation_sync_count,
                "allocation_sync_skipped_count": self.state.allocation_sync_skipped_count,
                "allocation_sync_last_error": self.state.allocation_sync_last_error,
                "event_loop_lag_ms": self.state.event_loop_lag_ms,
                "max_event_loop_lag_ms": self.state.max_event_loop_lag_ms,
                "durable_replay_scan_count": self.state.durable_replay_scan_count,
                "durable_order_resume_scan_count": self.state.durable_order_resume_scan_count,
                "durable_replay_idle_wait_count": self.state.durable_replay_idle_wait_count,
                "background_cycles_deferred_for_hot_path": self.state.background_cycles_deferred_for_hot_path,
                "manual_position_guards": self.manual_position_guard.snapshot(),
                "follower_order_updates_subscribed": self.state.follower_order_updates_subscribed,
                "follower_user_events_subscribed": self.state.follower_user_events_subscribed,
                "follower_user_fills_subscribed": self.state.follower_user_fills_subscribed,
                "follower_clearinghouse_subscribed": self.state.follower_clearinghouse_subscribed,
                "follower_all_dexs_clearinghouse_subscribed": (
                    self.state.follower_all_dexs_clearinghouse_subscribed
                ),
                "follower_position_stream_trusted_dexes": sorted(
                    dex
                    for dex in dexes
                    if self._follower_position_stream_is_trusted(dex)
                ),
                "follower_position_stream_age_ms_by_dex": {
                    dex: max(
                        0,
                        int(
                            (
                                now
                                - self._follower_position_stream_observed_at[dex]
                            ).total_seconds()
                            * 1000
                        ),
                    )
                    for dex in dexes
                    if dex in self._follower_position_stream_observed_at
                },
                "leader_user_fills_subscribed_count": self.state.leader_user_fills_subscribed_count,
                "dex_price_cache_status": price_status,
                "price_cache_required_dexes": required_price_dexes,
                "price_cache_all_dexes_fresh": all(item["fresh"] for item in price_status.values()) if price_status else False,
                # The dashboard only consumes prices for currently displayed
                # positions. Persisting every known market (~1,100 rows) once a
                # second produced a 228KB update and avoidable WAL/fsync load.
                "price_cache": self.price_cache.snapshot(
                    dexes,
                    canonical_coins=status_price_markets,
                ),
                "default_dex_price_cache_fresh": price_status.get("", {}).get("fresh", False),
                "xyz_price_cache_fresh": price_status.get("xyz", {}).get("fresh", False),
                "last_ws_event_at": self.state.last_ws_event_at.isoformat() if self.state.last_ws_event_at else None,
                "last_ws_event_age_ms": last_age,
                "reconnect_count": self.state.reconnect_count,
                "durable_inbox_pending_count": pending_fill_count,
                "durable_inbox_retrying_count": retrying_fill_count,
                "durable_inbox_oldest_age_ms": oldest_pending_fill_age_ms,
                "durable_inbox_stuck": durable_inbox_stuck,
                "durable_outbox_pending_submit_count": pending_submit_count,
                "durable_outbox_submitting_count": submitting_count,
                "durable_outbox_unknown_count": unknown_submit_count,
                "durable_outbox_oldest_age_ms": oldest_unresolved_order_age_ms,
                "durable_outbox_stuck": durable_outbox_stuck,
                "durable_pipeline_stuck_threshold_ms": stuck_threshold_ms,
                "durable_pipeline_liveness_ready": liveness_ready,
                "outcome_integrity_ready": processed_without_outcome_count == 0 and liveness_ready,
                "processed_without_outcome_count": processed_without_outcome_count,
                "unresolved_fill_outcome_count": unresolved_outcome_count,
                "writer_lease_acquired": self._writer_lease_connection is not None,
                "submit_database_pool_isolated": self._submit_database_pool_isolated,
                "in_memory_fill_queue_count": len(self._queued_source_fill_ids),
                "in_memory_submit_queue_count": len(self._queued_submit_order_ids),
                "recent_submit_latency": recent_submit_latency,
                "last_error": redact_text(self.state.last_error) if self.state.last_error else None,
                "updated_at": now.isoformat(),
            }
            stmt = (
                insert(AppSetting)
                .values(
                    key=self.settings.low_latency_watcher_status_key(),
                    value=payload,
                    updated_at=now,
                )
                .on_conflict_do_update(
                    index_elements=[AppSetting.key],
                    set_={"value": payload, "updated_at": now},
                )
            )
            await db.execute(stmt)
            await store_task_status(
                db,
                task_name="low_latency_watcher"
                if not self.execution_scope
                else f"low_latency_watcher:{self.execution_scope}",
                last_error=redact_text(self.state.last_error) if self.state.last_error else None,
                metadata={
                    "websocket_connected": self.state.websocket_connected,
                    "ready_for_low_latency_live": ready,
                    "active_leaders": active,
                    "durable_inbox_pending_count": pending_fill_count,
                    "durable_inbox_retrying_count": retrying_fill_count,
                    "durable_inbox_oldest_age_ms": oldest_pending_fill_age_ms,
                    "durable_inbox_stuck": durable_inbox_stuck,
                    "durable_outbox_pending_submit_count": pending_submit_count,
                    "durable_outbox_submitting_count": submitting_count,
                    "durable_outbox_unknown_count": unknown_submit_count,
                    "durable_outbox_oldest_age_ms": oldest_unresolved_order_age_ms,
                    "durable_outbox_stuck": durable_outbox_stuck,
                    "durable_pipeline_liveness_ready": liveness_ready,
                    "processed_without_outcome_count": processed_without_outcome_count,
                    "unresolved_fill_outcome_count": unresolved_outcome_count,
                    "writer_lease_acquired": self._writer_lease_connection is not None,
                    "event_loop_lag_ms": self.state.event_loop_lag_ms,
                    "max_event_loop_lag_ms": self.state.max_event_loop_lag_ms,
                    "durable_replay_scan_count": self.state.durable_replay_scan_count,
                    "durable_order_resume_scan_count": self.state.durable_order_resume_scan_count,
                    "durable_replay_idle_wait_count": self.state.durable_replay_idle_wait_count,
                    "background_cycles_deferred_for_hot_path": self.state.background_cycles_deferred_for_hot_path,
                    "recent_submit_latency": recent_submit_latency,
                },
            )
            await store_task_status(
                db,
                task_name="price_cache_updater",
                last_error=(
                    redact_text(self.state.last_error)
                    if not price_fresh and self.state.last_error
                    else None
                ),
                metadata={"dex_price_cache_status": price_status},
            )
            await db.commit()

    async def _store_starting_status(self) -> None:
        now = datetime.now(timezone.utc)
        payload = {
            "mode": "starting",
            "low_latency_watcher_running": True,
            "low_latency_ready": False,
            "ready_for_low_latency_live": False,
            "websocket_connected": False,
            "market_ws_connected": False,
            "leader_fills_ws_connected": False,
            "follower_user_fills_subscribed": False,
            "follower_position_fresh": False,
            "follower_position_freshness": {
                "ready": False,
                "reason": "watcher starting",
                "threshold_ms": int(float(getattr(self.settings, "account_state_stale_seconds", 2) or 2) * 1000),
            },
            "source": "low_latency_watcher",
            "last_error": None,
            "updated_at": now.isoformat(),
        }
        try:
            async with self.db_session_factory() as db:
                stmt = (
                    insert(AppSetting)
                    .values(
                        key=self.settings.low_latency_watcher_status_key(),
                        value=payload,
                        updated_at=now,
                    )
                    .on_conflict_do_update(
                        index_elements=[AppSetting.key],
                        set_={"value": payload, "updated_at": now},
                    )
                )
                await db.execute(stmt)
                await db.commit()
        except Exception as exc:
            safe_error = redact_text(exc)
            log.warning("starting_status_store_failed", error=safe_error[:160])

    async def _store_standby_status(self) -> None:
        now = datetime.now(timezone.utc)
        payload = {
            "mode": "standby",
            "low_latency_watcher_running": True,
            "low_latency_ready": False,
            "ready_for_low_latency_live": False,
            "writer_lease_acquired": False,
            "updated_at": now.isoformat(),
        }
        async with self.db_session_factory() as db:
            standby_key = (
                f"watcher_standby_status:{self.execution_scope}"
                if self.execution_scope
                else "watcher_standby_status"
            )
            await db.execute(
                insert(AppSetting)
                .values(key=standby_key, value=payload, updated_at=now)
                .on_conflict_do_update(
                    index_elements=[AppSetting.key],
                    set_={"value": payload, "updated_at": now},
                )
            )
            await store_task_status(
                db,
                task_name=(
                    f"low_latency_watcher_standby:{self.execution_scope}"
                    if self.execution_scope
                    else "low_latency_watcher_standby"
                ),
                status="standby_writer_lease",
                metadata={"writer_lease_acquired": False},
            )
            await db.commit()

    async def _active_follower_position_freshness(self, db: Any, *, now: datetime) -> dict[str, Any]:
        follower = normalize_leader_address(self.settings.hyperliquid_follower_account_address() or "")
        if not follower:
            return {"ready": False, "reason": "follower address missing"}
        allocation_rows = (
            await db.execute(
                select(
                    LeaderPositionAllocationRecord.dex,
                    LeaderPositionAllocationRecord.canonical_coin,
                    LeaderPositionAllocationRecord.hyperliquid_coin,
                    LeaderPositionAllocationRecord.position_side,
                )
                .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
                .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                .where(LeaderPositionAllocationRecord.venue_account == self.execution_scope)
                .where(LeaderPositionAllocationRecord.status != "CLOSED")
                .where(LeaderPositionAllocationRecord.allocated_qty > Decimal("0"))
                .where(LeaderConfig.enabled.is_(True))
                .where(LeaderConfig.deleted_at.is_(None))
                .distinct()
            )
        ).all()
        allocation_keys = {
            (
                str(dex or "").lower(),
                str(canonical or coin or "").upper(),
                str(side or "").upper(),
            )
            for dex, canonical, coin, side in allocation_rows
        }
        allocation_keys.discard(("", "", ""))
        if not allocation_keys:
            return {"ready": True, "active_allocation_count": 0}
        position_rows = (
            await db.execute(
                select(
                    LatestAccountPosition.dex,
                    LatestAccountPosition.canonical_coin,
                    LatestAccountPosition.coin,
                    LatestAccountPosition.side,
                    LatestAccountPosition.last_update_at,
                )
                .where(LatestAccountPosition.role == FOLLOWER)
                .where(LatestAccountPosition.address == follower)
                .where(LatestAccountPosition.active.is_(True))
            )
        ).all()
        positions_by_key: dict[tuple[str, str, str], datetime | None] = {}
        for dex, canonical, coin, side, last_update_at in position_rows:
            key = (
                str(dex or "").lower(),
                str(canonical or coin or "").upper(),
                str(side or "").upper(),
            )
            updated_at = _datetime_or_none(last_update_at)
            current = positions_by_key.get(key)
            positions_by_key[key] = _latest_datetime(current, updated_at)
        stale_seconds = float(getattr(self.settings, "account_state_stale_seconds", 2) or 2)
        threshold_ms = int(max(0.5, stale_seconds) * 1000)
        missing = sorted(f"{dex}:{coin}:{side}" for dex, coin, side in allocation_keys if positions_by_key.get((dex, coin, side)) is None)
        max_age_ms = 0
        stale: list[str] = []
        stream_trusted_dexes: set[str] = set()
        for key in allocation_keys:
            updated_at = positions_by_key.get(key)
            if updated_at is None:
                continue
            age_ms = max(0, int((now - updated_at).total_seconds() * 1000))
            max_age_ms = max(max_age_ms, age_ms)
            dex, coin, side = key
            stream_trusted = self._follower_position_stream_is_trusted(dex)
            if stream_trusted:
                stream_trusted_dexes.add(dex)
            # Use the exact same policy as the execution engine. Keeping the
            # readiness gate and the hot path on this shared function prevents
            # one from accepting a live stream while the other still applies
            # the legacy wall-clock-only rule.
            if not _follower_position_state_is_fresh(
                state_at=updated_at,
                now=now,
                stale_seconds=stale_seconds,
                stream_trusted=stream_trusted,
            ):
                stale.append(f"{dex}:{coin}:{side}")
        ready = not missing and not stale
        return {
            "ready": ready,
            "active_allocation_count": len(allocation_keys),
            "max_age_ms": max_age_ms,
            "threshold_ms": threshold_ms,
            "missing": missing,
            "stale": sorted(stale),
            "stream_trusted_dexes": sorted(stream_trusted_dexes),
        }


def _coalesce_same_batch_fills(events: list[FillEvent]) -> tuple[list[FillEvent], list[FillEvent]]:
    if len(events) <= 1:
        return events, []
    order_events, order_skipped, has_order_keys = _aggregate_same_order_fills(events)
    if has_order_keys:
        return order_events, order_skipped

    return _coalesce_legacy_same_batch_fills(events)


def _coalesce_queued_lifecycle_fills(events: list[FillEvent]) -> tuple[list[FillEvent], list[FillEvent]]:
    if len(events) <= 1:
        return events, []

    selected: list[FillEvent] = []
    skipped: list[FillEvent] = []
    idx = 0
    while idx < len(events):
        implied = _queued_chainable_implied(events[idx])
        if implied is None:
            selected.append(events[idx])
            idx += 1
            continue

        segment = [idx]
        chain_kind = _queued_chain_kind(implied)
        prev_event = events[idx]
        prev_implied = implied
        next_idx = idx + 1
        while next_idx < len(events):
            next_implied = _queued_lifecycle_chain_implied(
                chain_kind=chain_kind,
                prev_event=prev_event,
                prev_implied=prev_implied,
                next_event=events[next_idx],
            )
            if next_implied is None:
                break
            segment.append(next_idx)
            prev_event = events[next_idx]
            prev_implied = next_implied
            next_idx += 1

        if len(segment) == 1:
            selected.append(events[idx])
        else:
            representative_idx, synthetic = _aggregate_order_segment(events, segment)
            selected.append(synthetic)
            skipped.extend(events[segment_idx] for segment_idx in segment if segment_idx != representative_idx)
        idx = next_idx

    return selected, skipped


def _queued_lifecycle_chain_implied(
    *,
    chain_kind: str,
    prev_event: FillEvent,
    prev_implied: FillImpliedPosition,
    next_event: FillEvent,
) -> FillImpliedPosition | None:
    implied = _queued_chainable_implied(next_event)
    if implied is None:
        return None
    if _queued_chain_kind(implied) != chain_kind:
        return None
    if not _queued_events_same_market(prev_event, next_event):
        return None
    if implied.side_after != prev_implied.side_after:
        return None
    if str((prev_event.raw or {}).get("dir") or "").lower() != str((next_event.raw or {}).get("dir") or "").lower():
        return None
    if str(prev_event.side or "").upper() != str(next_event.side or "").upper():
        return None
    if prev_event.time_ms and next_event.time_ms and next_event.time_ms < prev_event.time_ms:
        return None
    if implied.start_position is None:
        return None
    if abs(implied.start_position - prev_implied.signed_size_after) > ALLOCATION_TRANSITION_TOLERANCE:
        return None
    return implied


def _queued_chainable_implied(event: FillEvent) -> FillImpliedPosition | None:
    implied = derive_leader_post_position_from_fill(event)
    if str(implied.confidence or "").upper() not in {"HIGH", "MEDIUM"}:
        return None
    if _queued_chain_kind(implied) is None:
        return None
    return implied


def _queued_chain_kind(implied: FillImpliedPosition) -> str | None:
    if implied.is_close or implied.is_flip:
        return None
    if implied.is_reduce:
        return "reduce"
    if implied.is_open or implied.is_increase:
        return "add"
    return None


def _queued_events_same_market(first: FillEvent, second: FillEvent) -> bool:
    return (
        str(first.leader_address or "").lower() == str(second.leader_address or "").lower()
        and str(first.market.dex or "").lower() == str(second.market.dex or "").lower()
        and str(first.market.canonical_coin or "").upper() == str(second.market.canonical_coin or "").upper()
    )


def _coalesce_legacy_same_batch_fills(events: list[FillEvent]) -> tuple[list[FillEvent], list[FillEvent]]:
    groups: dict[tuple[str, str], list[int]] = {}
    for idx, event in enumerate(events):
        key = (event.market.dex, event.market.canonical_coin.upper())
        groups.setdefault(key, []).append(idx)

    selected_events_by_index: dict[int, FillEvent] = {}
    for indexes in groups.values():
        if len(indexes) == 1:
            selected_events_by_index[indexes[0]] = events[indexes[0]]
            continue

        implied_by_index = {
            idx: derive_leader_post_position_from_fill(events[idx])
            for idx in indexes
        }
        group_selected_indexes: set[int] = {indexes[-1]}

        first = implied_by_index[indexes[0]]
        if _fill_is_new_open_from_flat(first):
            group_selected_indexes.add(indexes[0])

        for idx in indexes:
            implied = implied_by_index[idx]
            if implied.is_close or implied.is_flip or _fill_is_new_open_from_flat(implied):
                group_selected_indexes.add(idx)

        segment_start = 0
        for group_pos, idx in enumerate(indexes):
            if idx not in group_selected_indexes:
                continue
            segment = [events[indexes[pos]] for pos in range(segment_start, group_pos + 1)]
            selected_events_by_index[idx] = _with_coalesced_fill_segment(events[idx], segment)
            segment_start = group_pos + 1

    selected = [selected_events_by_index[idx] for idx in range(len(events)) if idx in selected_events_by_index]
    skipped = [event for idx, event in enumerate(events) if idx not in selected_events_by_index]
    return selected, skipped


def _aggregate_same_order_fills(events: list[FillEvent]) -> tuple[list[FillEvent], list[FillEvent], bool]:
    selected_by_index: dict[int, FillEvent] = {}
    skipped: list[FillEvent] = []
    handled_indexes: set[int] = set()
    has_order_keys = False

    time_action_groups: dict[tuple[str, ...], list[int]] = {}
    for idx, event in enumerate(events):
        key = _same_action_time_key(event)
        if key is not None:
            time_action_groups.setdefault(key, []).append(idx)

    for indexes in time_action_groups.values():
        if len(indexes) <= 1:
            continue
        unique_indexes = _unique_order_segment_indexes(events, indexes)
        if len(unique_indexes) <= 1 or not _can_aggregate_same_action_time_group(events, unique_indexes):
            continue
        has_order_keys = True
        handled_indexes.update(indexes)
        unique_index_set = set(unique_indexes)
        skipped.extend(events[idx] for idx in indexes if idx not in unique_index_set)
        for segment in _same_order_lifecycle_segments(events, unique_indexes):
            if len(segment) == 1:
                selected_by_index[segment[0]] = events[segment[0]]
                continue
            selected_idx, synthetic = _aggregate_order_segment(events, segment)
            selected_by_index[selected_idx] = synthetic
            skipped.extend(events[idx] for idx in segment if idx != selected_idx)

    groups: dict[tuple[str, ...], list[int]] = {}
    passthrough: dict[int, FillEvent] = {}
    for idx, event in enumerate(events):
        if idx in handled_indexes:
            continue
        key = _same_order_key(event)
        if key is None:
            passthrough[idx] = event
            continue
        groups.setdefault(key, []).append(idx)

    if not groups and not has_order_keys:
        return events, [], False

    selected_by_index.update(passthrough)
    if groups:
        has_order_keys = True
    for indexes in groups.values():
        unique_indexes = _unique_order_segment_indexes(events, indexes)
        unique_index_set = set(unique_indexes)
        duplicate_indexes = [idx for idx in indexes if idx not in unique_index_set]
        skipped.extend(events[idx] for idx in duplicate_indexes)
        for segment in _same_order_lifecycle_segments(events, unique_indexes):
            if len(segment) == 1:
                selected_by_index[segment[0]] = events[segment[0]]
                continue
            selected_idx, synthetic = _aggregate_order_segment(events, segment)
            selected_by_index[selected_idx] = synthetic
            skipped.extend(events[idx] for idx in segment if idx != selected_idx)

    selected = [selected_by_index[idx] for idx in range(len(events)) if idx in selected_by_index]
    return selected, skipped, has_order_keys


def _same_action_time_key(event: FillEvent) -> tuple[str, ...] | None:
    raw = event.raw or {}
    time_value = raw.get("time") or event.time_ms
    direction = str(raw.get("dir") or "").strip().lower()
    side = str(raw.get("side") or event.side or "").upper()
    if not time_value or not direction or not side:
        return None
    return (
        str(event.leader_address or "").lower(),
        str(event.market.dex or "").lower(),
        event.market.canonical_coin.upper(),
        str(raw.get("coin") or event.market.raw_coin or event.market.coin),
        side,
        direction,
        str(time_value),
    )


def _can_aggregate_same_action_time_group(events: list[FillEvent], indexes: list[int]) -> bool:
    segment = [events[idx] for idx in indexes]
    union = _position_interval_union(segment)
    return union is not None and union[1] == 0


def _same_order_key(event: FillEvent) -> tuple[str, ...] | None:
    raw = event.raw or {}
    oid = raw.get("oid")
    order_hash = raw.get("hash")
    if oid is not None:
        return (
            str(event.leader_address or "").lower(),
            event.market.dex,
            event.market.canonical_coin.upper(),
            str(raw.get("coin") or event.market.raw_coin or event.market.coin),
            str(raw.get("side") or event.side).upper(),
            "oid",
            str(oid),
        )
    if order_hash is None:
        return None
    return (
        str(event.leader_address or "").lower(),
        event.market.dex,
        event.market.canonical_coin.upper(),
        str(raw.get("coin") or event.market.raw_coin or event.market.coin),
        str(raw.get("side") or event.side).upper(),
        "hash",
        str(raw.get("time") or event.time_ms),
        str(order_hash or ""),
    )


def _same_order_lifecycle_segments(events: list[FillEvent], indexes: list[int]) -> list[list[int]]:
    segments: list[list[int]] = []
    start = 0
    for pos, idx in enumerate(indexes):
        if pos == 0:
            continue
        implied = derive_leader_post_position_from_fill(events[idx])
        if _fill_is_new_open_from_flat(implied):
            segments.append(indexes[start:pos])
            start = pos
    segments.append(indexes[start:])
    return [segment for segment in segments if segment]


def _unique_order_segment_indexes(events: list[FillEvent], indexes: list[int]) -> list[int]:
    seen: set[str] = set()
    unique: list[int] = []
    for idx in indexes:
        source_fill_id = events[idx].source_fill_id
        if source_fill_id in seen:
            continue
        seen.add(source_fill_id)
        unique.append(idx)
    return unique


def _aggregate_order_segment(events: list[FillEvent], indexes: list[int]) -> tuple[int, FillEvent]:
    segment = [events[idx] for idx in indexes]
    representative_idx = _order_segment_representative_index(events, indexes)
    representative = events[representative_idx]
    raw_total_size = sum(abs(event.size) for event in segment)
    raw_total_notional = sum(abs(event.price * event.size) for event in segment)
    total_size = _effective_segment_size(segment, raw_total_size)
    total_notional = (
        raw_total_notional * total_size / raw_total_size
        if raw_total_size > 0 and total_size > 0
        else raw_total_notional
    )
    vwap = total_notional / total_size if total_size > 0 else representative.price
    start_position = _order_segment_start_position(segment)

    raw = dict(representative.raw or {})
    raw["px"] = _decimal_string(vwap)
    raw["sz"] = _decimal_string(total_size)
    if start_position is not None:
        raw["startPosition"] = _decimal_string(start_position)
    raw["dir"] = _direction_from_start_side_size(start_position, representative.side, total_size) or raw.get("dir")
    raw[_COALESCED_FILL_NOTIONAL_KEY] = _decimal_string(total_notional)
    raw[_COALESCED_FILL_SIZE_KEY] = _decimal_string(total_size)
    raw[_COALESCED_FILL_COUNT_KEY] = len(segment)
    raw[_COALESCED_SOURCE_IDS_KEY] = _flatten_source_fill_ids(segment)

    return representative_idx, replace(
        representative,
        price=vwap,
        size=total_size,
        raw=raw,
    )


def _effective_segment_size(segment: list[FillEvent], fallback_size: Decimal) -> Decimal:
    union = _position_interval_union(segment)
    if union is None:
        return fallback_size
    union_size, _gaps = union
    if union_size > 0 and union_size <= fallback_size + ALLOCATION_TRANSITION_TOLERANCE:
        return union_size
    return fallback_size


def _position_interval_union(segment: list[FillEvent]) -> tuple[Decimal, int] | None:
    intervals: list[tuple[Decimal, Decimal]] = []
    for event in segment:
        interval = _fill_position_interval(event)
        if interval is None:
            return None
        lo, hi = interval
        if hi - lo <= ALLOCATION_TRANSITION_TOLERANCE:
            continue
        intervals.append((lo, hi))
    if not intervals:
        return None

    intervals.sort(key=lambda item: (item[0], item[1]))
    union_size = Decimal("0")
    gaps = 0
    current_lo, current_hi = intervals[0]
    for lo, hi in intervals[1:]:
        if lo > current_hi + ALLOCATION_TRANSITION_TOLERANCE:
            union_size += current_hi - current_lo
            gaps += 1
            current_lo, current_hi = lo, hi
            continue
        if hi > current_hi:
            current_hi = hi
    union_size += current_hi - current_lo
    return union_size, gaps


def _fill_position_interval(event: FillEvent) -> tuple[Decimal, Decimal] | None:
    start = _decimal_from_value((event.raw or {}).get("startPosition"))
    if start is None:
        return None
    size = abs(event.size)
    side = str((event.raw or {}).get("side") or event.side or "").upper()
    if side == "B":
        after = start + size
    elif side == "A":
        after = start - size
    else:
        return None
    return (min(start, after), max(start, after))


def _order_segment_representative_index(events: list[FillEvent], indexes: list[int]) -> int:
    starts: list[tuple[Decimal, int]] = []
    for idx in indexes:
        start = _decimal_from_value((events[idx].raw or {}).get("startPosition"))
        if start is not None:
            starts.append((abs(start), idx))
    if not starts:
        return indexes[0]
    first_direction = str((events[indexes[0]].raw or {}).get("dir") or "").lower()
    if first_direction.startswith("close"):
        return max(starts, key=lambda item: item[0])[1]
    return min(starts, key=lambda item: item[0])[1]


def _order_segment_start_position(segment: list[FillEvent]) -> Decimal | None:
    if not segment:
        return None
    representative_idx = _order_segment_representative_index(segment, list(range(len(segment))))
    return _decimal_from_value((segment[representative_idx].raw or {}).get("startPosition"))


def _direction_from_start_side_size(start_position: Decimal | None, side: str, size: Decimal) -> str | None:
    if start_position is None or size <= 0:
        return None
    side_value = str(side or "").upper()
    if side_value == "B":
        signed_after = start_position + size
    elif side_value == "A":
        signed_after = start_position - size
    else:
        return None
    if abs(signed_after) <= ALLOCATION_TRANSITION_TOLERANCE:
        return "Close Long" if start_position > 0 else "Close Short" if start_position < 0 else None
    after_side = "Long" if signed_after > 0 else "Short"
    if abs(start_position) <= ALLOCATION_TRANSITION_TOLERANCE:
        return f"Open {after_side}"
    if (start_position > 0) == (signed_after > 0):
        return f"Open {after_side}" if abs(signed_after) > abs(start_position) else f"Close {after_side}"
    return f"Flip {after_side}"


def _snapshot_recovery_fill(fill: FillEvent, *, reason: str) -> FillEvent:
    raw = dict(fill.raw or {})
    raw[_SNAPSHOT_RECOVERY_KEY] = True
    raw[_SNAPSHOT_RECOVERY_REASON_KEY] = reason
    return replace(fill, raw=raw, is_snapshot=False)


def _is_snapshot_recovery_fill(fill: FillEvent) -> bool:
    return bool((fill.raw or {}).get(_SNAPSHOT_RECOVERY_KEY))


def _snapshot_recovery_allocation_side(implied: FillImpliedPosition) -> PositionSide | None:
    if implied.is_open:
        return None
    if implied.is_reduce or implied.is_close or implied.is_flip:
        start_position = _decimal_from_value(implied.start_position)
        if start_position is None or abs(start_position) <= ALLOCATION_TRANSITION_TOLERANCE:
            return None
        return PositionSide.LONG if start_position > 0 else PositionSide.SHORT
    if implied.is_increase and implied.side_after in {PositionSide.LONG, PositionSide.SHORT}:
        return implied.side_after
    return None


def _snapshot_event_after_allocation_checkpoint(fill: FillEvent, allocation: Any) -> bool:
    event_time = fill.hyperliquid_event_time
    if event_time is None:
        return False
    checkpoint = (
        getattr(allocation, "last_reconcile_at", None)
        or getattr(allocation, "updated_at", None)
        or getattr(allocation, "created_at", None)
    )
    if checkpoint is None:
        return False
    if checkpoint.tzinfo is None:
        checkpoint = checkpoint.replace(tzinfo=timezone.utc)
    return event_time > checkpoint


def _snapshot_recovery_should_use_allocation_checkpoint(
    *,
    fill: FillEvent,
    planning_allocation: Any | None,
    leader_previous_position_size: Decimal | None,
    leader_position_size: Decimal | None,
) -> bool:
    if not _is_snapshot_recovery_fill(fill) or planning_allocation is None:
        return False
    previous_from_fill = _decimal_from_value(leader_previous_position_size)
    current_size = _decimal_from_value(leader_position_size)
    checkpoint_size = _decimal_from_value(getattr(planning_allocation, "last_leader_position_size", None))
    if previous_from_fill is None or current_size is None or checkpoint_size is None:
        return False
    if abs(current_size) >= abs(checkpoint_size) - ALLOCATION_TRANSITION_TOLERANCE:
        return False
    return abs(previous_from_fill) < abs(checkpoint_size) - ALLOCATION_TRANSITION_TOLERANCE


def _decimal_string(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _with_coalesced_fill_segment(event: FillEvent, segment: list[FillEvent]) -> FillEvent:
    if len(segment) <= 1:
        return event
    implied = derive_leader_post_position_from_fill(event)
    if not (implied.is_open or implied.is_increase):
        return event
    coalesced_notional = sum(abs(item.price * item.size) for item in segment)
    coalesced_size = sum(abs(item.size) for item in segment)
    raw = dict(event.raw or {})
    raw[_COALESCED_FILL_NOTIONAL_KEY] = str(coalesced_notional)
    raw[_COALESCED_FILL_SIZE_KEY] = str(coalesced_size)
    raw[_COALESCED_FILL_COUNT_KEY] = len(segment)
    raw[_COALESCED_SOURCE_IDS_KEY] = _flatten_source_fill_ids(segment)
    return replace(event, raw=raw)


def _coalesced_source_fill_ids(fill: FillEvent) -> list[str]:
    raw_ids = (fill.raw or {}).get(_COALESCED_SOURCE_IDS_KEY)
    values = list(raw_ids) if isinstance(raw_ids, list) else []
    values.insert(0, fill.source_fill_id)
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        source_fill_id = str(value or "").strip()
        if not source_fill_id or source_fill_id in seen:
            continue
        seen.add(source_fill_id)
        result.append(source_fill_id)
    return result


def _flatten_source_fill_ids(events: list[FillEvent]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for event in events:
        for source_fill_id in _coalesced_source_fill_ids(event):
            if source_fill_id in seen:
                continue
            seen.add(source_fill_id)
            result.append(source_fill_id)
    return result


def _fill_notional_for_sizing(fill: FillEvent) -> Decimal:
    coalesced_notional = _decimal_from_value((fill.raw or {}).get(_COALESCED_FILL_NOTIONAL_KEY))
    if coalesced_notional is not None and coalesced_notional > 0:
        return abs(coalesced_notional)
    return abs(fill.price * fill.size)


def build_fill_event(
    leader_address: str,
    fill: dict[str, Any],
    *,
    is_snapshot: bool,
    ws_received_at: datetime | None = None,
    ingress_channel: str | None = None,
) -> FillEvent:
    parse_started_at = datetime.now(timezone.utc)
    ws_received_at = ws_received_at or parse_started_at
    market = parse_fill_to_market_key(fill)
    parse_done_at = datetime.now(timezone.utc)
    return FillEvent(
        source_fill_id=fill_unique_id(leader_address, fill),
        leader_address=leader_address.lower(),
        market=market,
        side=str(fill.get("side") or fill.get("dir") or ""),
        price=Decimal(str(fill.get("px") or fill.get("price") or "0")),
        size=Decimal(str(fill.get("sz") or fill.get("size") or "0")),
        time_ms=int(fill.get("time") or fill.get("timeMs") or 0),
        raw=fill,
        is_snapshot=is_snapshot,
        ws_received_at=ws_received_at,
        parse_started_at=parse_started_at,
        parse_done_at=parse_done_at,
        ingress_channel=ingress_channel,
    )


def _fill_event_from_source_row(row: SourceFill) -> FillEvent:
    event = build_fill_event(
        row.leader_address,
        dict(row.raw_fill or {}),
        is_snapshot=bool(row.is_snapshot),
        ws_received_at=row.ws_received_at or row.created_at or datetime.now(timezone.utc),
        ingress_channel="durable_replay",
    )
    if event.source_fill_id == row.source_fill_id:
        return event
    return replace(event, source_fill_id=row.source_fill_id)


def unresolved_same_market_order_query(
    *,
    leader_address: str,
    dex: str,
    canonical_coin: str,
    execution_scope: str = "",
):
    return (
        select(ExecutionOrder)
        .where(ExecutionOrder.source_type == "AUTO_COPY")
        .where(ExecutionOrder.status.in_(RECOVERY_ORDER_STATUSES))
        .where(ExecutionOrder.venue_account == str(execution_scope or "").lower())
        .where(ExecutionOrder.leader_address == leader_address.lower())
        .where(ExecutionOrder.dex == str(dex or "").lower())
        .where(func.upper(ExecutionOrder.canonical_coin) == str(canonical_coin).upper())
    )


def _ambiguous_signer_order_query(*, signer_scope: str, current_order_id: int):
    return (
        select(ExecutionOrder.id)
        .where(ExecutionOrder.source_type == "AUTO_COPY")
        .where(ExecutionOrder.execution_venue == ExecutionVenue.HYPERLIQUID.value)
        .where(ExecutionOrder.id != int(current_order_id))
        .where(ExecutionOrder.status == "UNKNOWN")
        .where(ExecutionOrder.order_submit_started_at.is_not(None))
        .where(
            or_(
                ExecutionOrder.submit_signer_scope == str(signer_scope),
                ExecutionOrder.submit_signer_scope.is_(None),
            )
        )
        .order_by(ExecutionOrder.order_submit_started_at.asc(), ExecutionOrder.id.asc())
        .limit(1)
    )


def _earlier_unprocessed_market_fill_query(
    market: MarketKey,
    *,
    current_id: int,
    execution_scope: str = "",
):
    return (
        select(SourceFill.id)
        .where(SourceFill.id < int(current_id))
        .where(SourceFill.is_snapshot.is_(False))
        .where(SourceFill.processed_at.is_(None))
        .where(SourceFill.execution_account == str(execution_scope or "").lower())
        .where(SourceFill.dex == str(market.dex or "").lower())
        .where(func.upper(SourceFill.canonical_coin) == market.canonical_coin.upper())
        .order_by(SourceFill.id.asc())
        .limit(1)
    )


def _durable_market_head_fill_query(
    *,
    execution_scope: str,
    now: datetime,
    limit: int,
):
    """Return only each market's earliest unfinished fill when it is due.

    Filtering by ``next_retry_at`` before choosing the market head lets later
    rows leapfrog a blocked predecessor into the worker merely to fail the FIFO
    assertion.  Under a long manual-position guard that becomes one database
    write per successor every 500 ms.  Rank the complete pending set first,
    then apply the due-time filter to the head only.
    """

    market_heads = (
        select(
            SourceFill.id.label("source_fill_pk"),
            func.row_number()
            .over(
                partition_by=(
                    SourceFill.dex,
                    func.upper(func.coalesce(SourceFill.canonical_coin, SourceFill.coin)),
                ),
                order_by=SourceFill.id.asc(),
            )
            .label("market_rank"),
        )
        .where(SourceFill.execution_account == str(execution_scope or "").lower())
        .where(SourceFill.processed_at.is_(None))
        .where(SourceFill.is_snapshot.is_(False))
        .subquery()
    )
    return (
        select(SourceFill)
        .join(market_heads, market_heads.c.source_fill_pk == SourceFill.id)
        .where(market_heads.c.market_rank == 1)
        .where(or_(SourceFill.next_retry_at.is_(None), SourceFill.next_retry_at <= now))
        .order_by(SourceFill.id.asc())
        .limit(max(1, int(limit)))
    )


def _unresolved_source_fill_outcomes_query(execution_scope: str):
    """Keep main/sub-account outcome diagnostics completely independent."""

    return (
        select(func.count(SourceFillOutcome.id))
        .join(SourceFill, SourceFill.source_fill_id == SourceFillOutcome.source_fill_id)
        .where(SourceFill.execution_account == str(execution_scope or "").lower())
        .where(
            SourceFillOutcome.disposition.in_(
                [FILL_OUTCOME_SUBMISSION_UNKNOWN, FILL_OUTCOME_MANUAL_REVIEW]
            )
        )
    )


def _unresolved_blockers_retryable(rows: list[ExecutionOrder]) -> bool:
    if not rows:
        return False
    return all(str(row.status or "").upper() in TRANSIENT_UNRESOLVED_ORDER_STATUSES for row in rows)


def _fills_from_message(channel: str | None, data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    if channel == "userFills":
        return list(data.get("fills") or [])
    if channel == "userEvents" and isinstance(data.get("fills"), list):
        return list(data.get("fills") or [])
    return []


def _required_price_status_dexes(
    *,
    enabled_dexes: list[str],
    event_dexes: set[str],
    allocation_dexes: set[str],
) -> list[str]:
    enabled = {str(dex or "").lower() for dex in enabled_dexes}
    required = {""}
    required.update(str(dex or "").lower() for dex in event_dexes)
    required.update(str(dex or "").lower() for dex in allocation_dexes)
    return sorted(dex for dex in required if dex in enabled)


def _price_fallback_dexes(
    *,
    enabled_dexes: list[str],
    active_allocation_dexes: set[str],
    last_event_time_by_dex: dict[str, str | None],
    now: datetime,
    recent_event_seconds: float = PRICE_FALLBACK_RECENT_EVENT_SECONDS,
) -> set[str]:
    """Limit REST mids traffic without weakening first-fill execution.

    Every enabled DEX remains websocket-subscribed.  REST is only a liveness
    fallback for currently relevant DEXes; a genuinely new fill carries its own
    authoritative positive execution reference price and immediately makes its
    DEX recent for subsequent fills.
    """

    cutoff = now - timedelta(seconds=max(1.0, float(recent_event_seconds)))
    recent_event_dexes = {
        str(dex or "").lower()
        for dex, raw_observed_at in last_event_time_by_dex.items()
        if (observed_at := _datetime_or_none(raw_observed_at)) is not None
        and observed_at >= cutoff
    }
    return set(
        _required_price_status_dexes(
            enabled_dexes=enabled_dexes,
            event_dexes=recent_event_dexes,
            allocation_dexes={
                str(dex or "").lower() for dex in active_allocation_dexes
            },
        )
    )


def _price_status_ready_for_low_latency_live(
    price_status: dict[str, dict[str, Any]],
    required_dexes: list[str],
    *,
    stale_ms: int,
) -> bool:
    if not required_dexes:
        return False
    max_age_ms = max(int(stale_ms) * 2, 5_000)
    for dex in required_dexes:
        status = price_status.get(str(dex or "").lower()) or {}
        if int(status.get("markets_count") or 0) <= 0:
            return False
        age_ms = status.get("last_price_update_age_ms")
        if age_ms is None:
            return False
        try:
            if int(age_ms) > max_age_ms:
                return False
        except Exception:
            return False
    return True


def _fill_order_identifiers(fill: dict[str, Any]) -> tuple[str | None, str | None]:
    cloid = _str_or_none(
        _first_present(
            fill.get("cloid"),
            fill.get("clientOrderId"),
            fill.get("client_order_id"),
        )
    )
    oid = _str_or_none(
        _first_present(
            fill.get("oid"),
            fill.get("orderId"),
            fill.get("order_id"),
        )
    )
    return cloid, oid


def _clearinghouse_state_payload_from_ws(data: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("clearinghouseState", "clearinghouse_state", "state"):
        value = data.get(key)
        if isinstance(value, dict):
            return value
    if any(key in data for key in ("marginSummary", "assetPositions", "withdrawable")):
        return data
    return None


def _all_dexs_clearinghouse_states(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Normalize the documented map and the live API's pair-list encoding."""

    raw_states = data.get("clearinghouseStates")
    if raw_states is None:
        raw_states = data.get("clearinghouse_states")
    result: dict[str, dict[str, Any]] = {}
    if isinstance(raw_states, dict):
        items = raw_states.items()
    elif isinstance(raw_states, list):
        items = (
            (item[0], item[1])
            for item in raw_states
            if isinstance(item, (list, tuple)) and len(item) == 2
        )
    else:
        return result
    for raw_dex, payload in items:
        if not isinstance(payload, dict):
            continue
        result[str(raw_dex or "").lower()] = payload
    return result


def _dex_from_ws_account_state(data: dict[str, Any]) -> str:
    return str(data.get("dex") or data.get("perpDex") or data.get("dexName") or "").lower()


def _hyperliquid_status(response: dict[str, Any]) -> str:
    if not isinstance(response, dict):
        return "UNKNOWN"
    status = str(response.get("status") or "").upper()
    if status == "OK":
        statuses = []
        for item in response.get("response", {}).get("data", {}).get("statuses", []) or []:
            if isinstance(item, dict):
                statuses.extend(str(key).upper() for key in item)
        if "ERROR" in statuses:
            return "REJECTED"
        if "FILLED" in statuses:
            return "FILLED"
        if "RESTING" in statuses or "OPEN" in statuses:
            return "OPEN"
        return "SUBMITTED"
    return status or "UNKNOWN"


def _hyperliquid_error(response: dict[str, Any]) -> str | None:
    if not isinstance(response, dict):
        return None
    for item in response.get("response", {}).get("data", {}).get("statuses", []) or []:
        if isinstance(item, dict) and item.get("error") is not None:
            return str(item.get("error"))
    return None


def _validator_error_code(result: ValidatedOrderParams | None) -> str | None:
    if result is None or result.ok:
        return None
    if "BELOW_MIN_ORDER_VALUE" in result.errors:
        return "BELOW_MIN_ORDER_VALUE"
    return result.errors[0] if result.errors else result.block_reason


def _validator_blocker_message(result: ValidatedOrderParams) -> str:
    if "BELOW_MIN_ORDER_VALUE" in result.errors:
        return (
            "target/delta notional is below Hyperliquid minimum order value "
            f"(target={_fmt_decimal(result.target_delta_notional)}, "
            f"estimated={_fmt_decimal(result.estimated_notional)}, "
            f"min={_fmt_decimal(result.min_order_value)})"
        )
    if "BLOCKED_TOO_SMALL" in result.errors:
        return "rounded order size is zero after Hyperliquid market precision"
    if result.block_reason:
        return result.block_reason
    return "; ".join(result.errors) or "Hyperliquid order validator blocked order"


def _validator_payload_message(payload: dict[str, Any]) -> str | None:
    errors = list(payload.get("errors") or [])
    if "BELOW_MIN_ORDER_VALUE" in errors:
        return (
            "target/delta notional is below Hyperliquid minimum order value "
            f"(target={payload.get('target_delta_notional')}, "
            f"estimated={payload.get('estimated_notional')}, "
            f"min={payload.get('min_order_value')})"
        )
    return payload.get("block_reason") or ("; ".join(errors) if errors else None)


def _fmt_decimal(value: Decimal | None) -> str:
    if value is None:
        return "--"
    return format(value.normalize(), "f")


def _is_definitely_not_submitted_hyperliquid_error(exc: Exception) -> bool:
    message = str(exc)
    return isinstance(exc, (AttributeError, TypeError, ValueError)) and (
        "to_raw" in message or "quantity rounded to zero" in message
    )


def _is_transient_submit_exception(exc: Exception) -> bool:
    if isinstance(exc, RetryablePreExchangeSubmitError):
        return True
    text = f"{type(exc).__name__}: {exc}".lower()
    transient_fragments = (
        "deadlock detected",
        "deadlockdetectederror",
        "could not serialize access",
        "serializationfailure",
        "lock timeout",
        "pendingrollbackerror",
        "infailedsqltransactionerror",
        "current transaction is aborted",
        "connection reset",
        "connection refused",
        "server closed the connection",
        "connection was closed",
        "connection is closed",
    )
    return any(fragment in text for fragment in transient_fragments)


def _hyperliquid_oid(response: dict[str, Any]) -> str | None:
    for item in response.get("response", {}).get("data", {}).get("statuses", []) or []:
        if not isinstance(item, dict):
            continue
        for value in item.values():
            if isinstance(value, dict) and value.get("oid") is not None:
                return str(value.get("oid"))
    return None


def _hyperliquid_fill_qty_price(response: dict[str, Any]) -> tuple[Decimal | None, Decimal | None]:
    for item in response.get("response", {}).get("data", {}).get("statuses", []) or []:
        if not isinstance(item, dict):
            continue
        filled = item.get("filled")
        if not isinstance(filled, dict):
            continue
        qty = _decimal_from_value(filled.get("totalSz") or filled.get("sz") or filled.get("filledSz"))
        price = _decimal_from_value(filled.get("avgPx") or filled.get("avgPrice") or filled.get("px"))
        return qty, price
    return None, None


def _order_side(*, target_side: PositionSide, reduce_only: bool) -> str:
    if target_side == PositionSide.SHORT:
        return "BUY" if reduce_only else "SELL"
    return "SELL" if reduce_only else "BUY"


def _order_quantity_for_transition(
    *,
    mark_price: Decimal,
    target_delta_abs: Decimal,
    reduce_only: bool,
    transition_plan: Any,
    aggregate_follower_qty: Decimal | None,
) -> Decimal:
    quantity = target_delta_abs / mark_price if mark_price > 0 else Decimal("0")
    if not reduce_only and transition_plan is not None:
        open_qty = Decimal(getattr(transition_plan, "open_qty", 0) or 0)
        if open_qty > ALLOCATION_TRANSITION_TOLERANCE:
            quantity = open_qty
    if reduce_only and transition_plan is not None and transition_plan.close_qty_limit > 0:
        quantity = transition_plan.close_qty_limit
    if reduce_only and aggregate_follower_qty is not None:
        quantity = min(quantity, aggregate_follower_qty)
    return quantity


def _close_instead_of_leaving_untradeable_residual(
    *,
    transition_plan: Any | None,
    current_allocation: LeaderPositionAllocationRecord | None,
    min_order_value: Decimal,
) -> tuple[Any | None, bool]:
    """Turn a proportional reduce into a full close when its target is untradeable.

    Hyperliquid applies its minimum notional to reduce-only orders too.  Leaving
    a sub-minimum target therefore creates a follower position that a later
    close or flip cannot remove.  The formula is still evaluated first and is
    retained in ``formula_inputs``; only the untradeable residual is skipped.
    """
    if transition_plan is None or current_allocation is None:
        return transition_plan, False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value != AllocationTransitionAction.REDUCE.value:
        return transition_plan, False

    target = abs(Decimal(getattr(transition_plan, "target_notional", 0) or 0))
    minimum = abs(Decimal(min_order_value or 0))
    allocation_qty = abs(Decimal(current_allocation.allocated_qty or 0))
    current_notional = abs(
        Decimal(
            getattr(transition_plan, "current_allocation_notional", None)
            or current_allocation.allocated_notional
            or 0
        )
    )
    if (
        minimum <= ALLOCATION_TRANSITION_TOLERANCE
        or target <= ALLOCATION_TRANSITION_TOLERANCE
        or target >= minimum
        or allocation_qty <= ALLOCATION_TRANSITION_TOLERANCE
        or current_notional <= ALLOCATION_TRANSITION_TOLERANCE
    ):
        return transition_plan, False

    formula_inputs = dict(getattr(transition_plan, "formula_inputs", None) or {})
    formula_inputs["formula_target_notional_before_minimum_residual_close"] = str(target)
    formula_inputs["minimum_tradeable_notional"] = str(minimum)
    formula_inputs["minimum_residual_early_close"] = True
    formula_inputs["execution_target_notional"] = "0"
    formula_inputs["execution_formula"] = (
        "when the proportional follower remainder is below the venue minimum, "
        "close the full follower allocation so no untradeable residual survives"
    )
    return (
        replace(
            transition_plan,
            target_notional=Decimal("0"),
            delta_notional=-current_notional,
            close_qty_limit=allocation_qty,
            reason=(
                "leader proportional reduce target is below the venue minimum; "
                "close follower allocation instead of creating an untradeable residual"
            ),
            formula_inputs=formula_inputs,
        ),
        True,
    )


def _reduce_quantity_guard_blockers(
    *,
    reduce_only: bool,
    transition_plan: Any | None,
    current_allocation: LeaderPositionAllocationRecord | None,
    rounded_size: Decimal,
    aggregate_follower_qty: Decimal | None,
) -> list[str]:
    if not reduce_only or transition_plan is None:
        return []
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value not in {
        AllocationTransitionAction.REDUCE.value,
        AllocationTransitionAction.CLOSE.value,
        AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
    }:
        return []
    size = abs(Decimal(rounded_size or 0))
    if size <= ALLOCATION_TRANSITION_TOLERANCE:
        return []

    blockers: list[str] = []
    close_qty_limit = abs(Decimal(getattr(transition_plan, "close_qty_limit", 0) or 0))
    if close_qty_limit <= ALLOCATION_TRANSITION_TOLERANCE:
        blockers.append("REDUCE_QTY_GUARD: reduce/close has no positive close_qty_limit")
    elif size > close_qty_limit + ALLOCATION_TRANSITION_TOLERANCE:
        blockers.append("REDUCE_QTY_GUARD: rounded order size exceeds planned close_qty_limit")

    if current_allocation is None:
        blockers.append("REDUCE_QTY_GUARD: reduce/close requires current allocation")
    else:
        allocation_qty = abs(Decimal(current_allocation.allocated_qty or 0))
        if size > allocation_qty + ALLOCATION_TRANSITION_TOLERANCE:
            blockers.append("REDUCE_QTY_GUARD: rounded order size exceeds allocation qty")

    if aggregate_follower_qty is not None:
        follower_qty = abs(Decimal(aggregate_follower_qty or 0))
        if size > follower_qty + ALLOCATION_TRANSITION_TOLERANCE:
            blockers.append("REDUCE_QTY_GUARD: rounded order size exceeds follower actual position qty")
    return blockers


def _allocation_plan_warnings(transition_plan: Any | None) -> list[str]:
    if transition_plan is None:
        return []
    formula_inputs = getattr(transition_plan, "formula_inputs", None) or {}
    warnings = formula_inputs.get("warnings") or []
    return [str(item) for item in warnings if str(item)]


def _allocation_plan_warning_message(warning: str) -> str:
    if warning == LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK:
        return (
            "last_leader_position_size missing; legacy allocation reduce used leader notional ratio fallback"
        )
    if warning == MAX_POSITION_NOTIONAL_CAP_APPLIED:
        return "max per-coin follower position notional cap applied"
    return warning


def _is_deferred_reduce_block(
    *,
    reduce_only: bool,
    transition_plan: Any | None,
    validator_result: ValidatedOrderParams,
) -> bool:
    if not reduce_only or transition_plan is None:
        return False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value not in {
        AllocationTransitionAction.REDUCE.value,
        AllocationTransitionAction.CLOSE.value,
        AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
    }:
        return False
    errors = {str(item).upper() for item in (validator_result.errors or [])}
    return (
        str(validator_result.block_reason or "").upper() == "BLOCKED_TOO_SMALL"
        or "BLOCKED_TOO_SMALL" in errors
        or "BELOW_MIN_ORDER_VALUE" in errors
    )


def _override_final_close_min_order_validator(
    *,
    reduce_only: bool,
    transition_plan: Any | None,
    validator_result: ValidatedOrderParams,
) -> tuple[ValidatedOrderParams, bool]:
    return validator_result, False


def _is_pending_open_block(
    *,
    reduce_only: bool,
    transition_plan: Any | None,
    validator_result: ValidatedOrderParams,
    blockers: list[str],
) -> bool:
    if reduce_only or transition_plan is None:
        return False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value not in {
        AllocationTransitionAction.OPEN.value,
        AllocationTransitionAction.INCREASE.value,
        AllocationTransitionAction.FLIP_OPEN_SECOND.value,
    }:
        return False
    errors = {str(item).upper() for item in (validator_result.errors or [])}
    too_small = (
        str(validator_result.block_reason or "").upper() == "BLOCKED_TOO_SMALL"
        or "BLOCKED_TOO_SMALL" in errors
        or "BELOW_MIN_ORDER_VALUE" in errors
    )
    if not too_small:
        return False
    return all(
        "minimum order value" in str(blocker).lower()
        or "rounded order size is zero" in str(blocker).lower()
        for blocker in blockers
    )


def _pending_open_should_remain_pending(allocation: LeaderPositionAllocationRecord | None) -> bool:
    return not _allocation_active(allocation)


def _allocation_state_target_notional(
    *,
    target_notional: Decimal | None,
    transition_plan: Any | None,
    pending_open_reason: str | None,
    reduce_only: bool,
) -> Decimal | None:
    if pending_open_reason != PENDING_OPEN_REASON or reduce_only or transition_plan is None:
        return target_notional
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value not in {
        AllocationTransitionAction.OPEN.value,
        AllocationTransitionAction.INCREASE.value,
        AllocationTransitionAction.FLIP_OPEN_SECOND.value,
    }:
        return target_notional
    return Decimal(getattr(transition_plan, "current_allocation_notional", 0) or 0)


def _allow_target_notional_price_drift_for_transition(transition_plan: Any | None) -> bool:
    if transition_plan is None or transition_plan.action != AllocationTransitionAction.INCREASE:
        return False
    formula_inputs = getattr(transition_plan, "formula_inputs", None) or {}
    return str(formula_inputs.get("increase_delta_source") or "") in {
        "leader_position_size",
        "leader_fill_start_position_size",
    }


def _direction_guard_preserves_allocation(
    fill_direction_guard_reason: str | None,
    allocation: LeaderPositionAllocationRecord | None,
) -> bool:
    return allocation is not None and str(fill_direction_guard_reason or "").startswith("FILL_DIRECTION_GUARD:")


def _blocked_order_preserves_allocation_state(
    *,
    allocation: LeaderPositionAllocationRecord | None,
    blockers: list[str],
    pending_open_activation_reason: str | None,
    pending_open: bool,
    deferred_reduce: bool,
    direction_guard_preserve_allocation: bool,
) -> bool:
    if allocation is None or not blockers:
        return False
    if (
        pending_open_activation_reason
        or pending_open
        or deferred_reduce
        or direction_guard_preserve_allocation
        or _allocation_needs_manual_review(allocation)
    ):
        return False
    return True


def _preserve_allocation_state_after_direction_guard(
    allocation: LeaderPositionAllocationRecord,
    *,
    leader_account_value: Decimal | None,
    leader_position_notional: Decimal | None,
    leader_position_size: Decimal | None,
    copy_multiplier: Decimal,
    source_fill_id: str,
    now: datetime,
) -> None:
    allocation.target_notional = Decimal(allocation.allocated_notional or 0)
    allocation.last_leader_account_value = leader_account_value
    allocation.last_leader_position_notional = leader_position_notional
    allocation.last_leader_position_size = leader_position_size
    allocation.copy_multiplier = copy_multiplier
    allocation.last_source_fill_id = source_fill_id
    allocation.last_reconcile_at = now
    if _allocation_needs_manual_review(allocation):
        allocation.status = "NEEDS_MANUAL_REVIEW"
        return
    if _decimal_from_value(allocation.pending_reduce_qty) and _decimal_from_value(allocation.pending_reduce_qty) > 0:
        allocation.status = "REDUCING"
    elif str(allocation.status or "").upper() == "BLOCKED":
        allocation.status = "OPEN"


def _preserve_allocation_state_after_position_cap_rejection(
    allocation: LeaderPositionAllocationRecord,
    *,
    leader_account_value: Decimal | None,
    leader_position_notional: Decimal | None,
    leader_position_size: Decimal | None,
    copy_multiplier: Decimal,
    source_fill_id: str,
    now: datetime,
) -> None:
    """Advance the leader checkpoint without pretending the rejected add filled.

    The follower allocation remains exactly where it was, while the leader's
    post-fill position becomes the baseline for the next proportional reduce.
    This prevents a cap rejection from being retried implicitly or from making
    a later reduce use stale leader state.
    """

    allocation.target_notional = Decimal(allocation.allocated_notional or 0)
    allocation.last_leader_account_value = leader_account_value
    allocation.last_leader_position_notional = leader_position_notional
    allocation.last_leader_position_size = leader_position_size
    allocation.copy_multiplier = copy_multiplier
    allocation.last_source_fill_id = source_fill_id
    allocation.last_reconcile_at = now
    if _allocation_needs_manual_review(allocation):
        allocation.status = "NEEDS_MANUAL_REVIEW"
        return
    if _decimal_from_value(allocation.pending_reduce_qty) and _decimal_from_value(allocation.pending_reduce_qty) > 0:
        allocation.status = "REDUCING"
    elif str(allocation.status or "").upper() == "BLOCKED":
        allocation.status = "OPEN"


def _preserve_allocation_state_after_blocked_order(
    allocation: LeaderPositionAllocationRecord,
    *,
    now: datetime,
) -> None:
    allocation.target_notional = Decimal(allocation.allocated_notional or 0)
    allocation.last_reconcile_at = now
    if _allocation_needs_manual_review(allocation):
        allocation.status = "NEEDS_MANUAL_REVIEW"
        return
    if _decimal_from_value(allocation.pending_reduce_qty) and _decimal_from_value(allocation.pending_reduce_qty) > 0:
        allocation.status = "REDUCING"
    elif str(allocation.status or "").upper() == "BLOCKED":
        allocation.status = "OPEN"


def _should_fast_forward_below_min_active_increase(
    *,
    allocation: LeaderPositionAllocationRecord | None,
    transition_plan: Any | None,
    reduce_only: bool,
    pending_open: bool,
    pending_open_activation_reason: str | None,
    deferred_reduce: bool,
) -> bool:
    if not pending_open or pending_open_activation_reason or deferred_reduce or reduce_only:
        return False
    if not _allocation_active(allocation):
        return False
    if _decimal_from_value(getattr(allocation, "pending_reduce_qty", None)) not in {None, Decimal("0")}:
        return False
    if _decimal_from_value(getattr(allocation, "pending_reduce_notional", None)) not in {None, Decimal("0")}:
        return False
    if transition_plan is None or transition_plan.action != AllocationTransitionAction.INCREASE:
        return False
    return transition_plan.delta_notional > Decimal("0")


def _should_fast_forward_below_min_pending_open_lifecycle(
    *,
    allocation: LeaderPositionAllocationRecord | None,
    transition_plan: Any | None,
    reduce_only: bool,
    pending_open: bool,
    pending_open_activation_reason: str | None,
    deferred_reduce: bool,
) -> bool:
    if not pending_open or pending_open_activation_reason or deferred_reduce or reduce_only:
        return False
    if allocation is None or str(allocation.status or "").upper() != PENDING_OPEN_STATUS:
        return False
    if _allocation_active(allocation):
        return False
    if transition_plan is None:
        return False
    return transition_plan.action in {
        AllocationTransitionAction.OPEN,
        AllocationTransitionAction.INCREASE,
        AllocationTransitionAction.FLIP_OPEN_SECOND,
    }


def _fast_forward_below_min_active_increase(
    allocation: LeaderPositionAllocationRecord,
    *,
    leader_account_value: Decimal | None,
    leader_position_notional: Decimal | None,
    leader_position_size: Decimal | None,
    copy_multiplier: Decimal,
    source_fill_id: str,
    now: datetime,
) -> None:
    allocation.target_notional = Decimal(allocation.allocated_notional or 0)
    allocation.last_leader_account_value = leader_account_value
    allocation.last_leader_position_notional = leader_position_notional
    allocation.last_leader_position_size = leader_position_size
    allocation.copy_multiplier = copy_multiplier
    allocation.last_source_fill_id = source_fill_id
    allocation.last_reconcile_at = now
    if str(allocation.status or "").upper() in {"BLOCKED", PENDING_OPEN_STATUS}:
        allocation.status = "OPEN"


def _fast_forward_below_min_pending_open_lifecycle(
    allocation: LeaderPositionAllocationRecord,
    *,
    leader_account_value: Decimal | None,
    leader_position_notional: Decimal | None,
    leader_position_size: Decimal | None,
    copy_multiplier: Decimal,
    source_fill_id: str,
    now: datetime,
) -> None:
    allocation.status = PENDING_OPEN_STATUS
    allocation.target_notional = Decimal("0")
    allocation.last_leader_account_value = leader_account_value
    allocation.last_leader_position_notional = leader_position_notional
    allocation.last_leader_position_size = leader_position_size
    allocation.copy_multiplier = copy_multiplier
    allocation.last_source_fill_id = source_fill_id
    allocation.last_reconcile_at = now
    allocation.pending_reduce_reason = allocation.pending_reduce_reason or PENDING_OPEN_REASON
    allocation.pending_reduce_since = allocation.pending_reduce_since or now
    allocation.pending_reduce_source_fill_id = source_fill_id


def _is_account_value_pending_open(
    *,
    allocation: LeaderPositionAllocationRecord | None,
    fill_implied_position: Any | None,
    transition_plan: Any | None,
    blockers: list[str],
) -> bool:
    if allocation is not None or transition_plan is None:
        return False
    if not _fill_is_new_open_from_flat(fill_implied_position):
        return False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value not in {
        AllocationTransitionAction.BLOCK.value,
        AllocationTransitionAction.OPEN.value,
        AllocationTransitionAction.INCREASE.value,
        AllocationTransitionAction.FLIP_OPEN_SECOND.value,
    }:
        return False
    text = " ; ".join(str(item).lower() for item in blockers + [getattr(transition_plan, "reason", "")])
    if "account value unavailable" not in text and "resolved account value unavailable" not in text:
        return False
    allowed_fragments = (
        "account value unavailable",
        "resolved account value unavailable",
        "target/delta notional is below hyperliquid minimum order value",
        "minimum order value",
    )
    return all(any(fragment in str(blocker).lower() for fragment in allowed_fragments) for blocker in blockers)


def _transition_requires_account_value(transition_plan: Any | None) -> bool:
    if transition_plan is None:
        return False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value in {AllocationTransitionAction.OPEN.value, AllocationTransitionAction.FLIP_OPEN_SECOND.value}:
        return True
    if action_value == AllocationTransitionAction.BLOCK.value:
        return "account value unavailable" in str(getattr(transition_plan, "reason", "")).lower()
    return False


def _mark_deferred_reduce(
    allocation: LeaderPositionAllocationRecord,
    *,
    order: ExecutionOrder,
    transition_plan: Any | None,
    quantity: Decimal,
    reason: str,
    now: datetime,
) -> None:
    if _allocation_needs_manual_review(allocation):
        allocation.status = "NEEDS_MANUAL_REVIEW"
        allocation.pending_reduce_reason = reason[:1000]
        return
    allocation_qty = abs(Decimal(allocation.allocated_qty or 0))
    pending_qty = max(Decimal("0"), min(Decimal(quantity or 0), allocation_qty))
    allocation.status = "REDUCING"
    allocation.pending_reduce_qty = _q(pending_qty)
    allocation.pending_reduce_notional = _pending_reduce_notional(
        allocation=allocation,
        transition_plan=transition_plan,
        pending_qty=pending_qty,
    )
    allocation.pending_reduce_reason = reason[:1000]
    allocation.pending_reduce_since = getattr(allocation, "pending_reduce_since", None) or now
    allocation.pending_reduce_source_fill_id = order.source_fill_id


def _mark_minimum_residual_release_pending(
    allocation: LeaderPositionAllocationRecord,
    *,
    order: ExecutionOrder,
    quantity: Decimal,
    now: datetime,
) -> None:
    """Persist that a full economic close will release market ownership."""
    allocation_qty = abs(Decimal(allocation.allocated_qty or 0))
    pending_qty = min(
        allocation_qty,
        max(Decimal("0"), abs(Decimal(quantity or 0))),
    )
    allocation.status = "REDUCING"
    allocation.target_notional = Decimal("0")
    allocation.pending_reduce_qty = _q(pending_qty)
    allocation.pending_reduce_notional = abs(
        Decimal(allocation.allocated_notional or 0)
    )
    allocation.pending_reduce_reason = (
        MINIMUM_RESIDUAL_ECONOMIC_FLAT_PENDING_REASON
    )
    allocation.pending_reduce_since = now
    allocation.pending_reduce_source_fill_id = order.source_fill_id
    allocation.last_reconcile_at = now


def _pending_reduce_notional(
    *,
    allocation: LeaderPositionAllocationRecord,
    transition_plan: Any | None,
    pending_qty: Decimal,
) -> Decimal:
    avg_entry = _decimal_from_value(getattr(allocation, "avg_entry_price", None))
    if avg_entry is not None and avg_entry > 0:
        return _q(pending_qty * avg_entry)
    if transition_plan is not None:
        return _q(abs(Decimal(getattr(transition_plan, "delta_notional", 0) or 0)))
    return Decimal("0.00000000")


def _clear_deferred_reduce(allocation: LeaderPositionAllocationRecord) -> None:
    clear_deferred_reduce_state(allocation)


def _clear_deferred_reduce_after_follower_flat(
    allocation: LeaderPositionAllocationRecord,
) -> None:
    """Clear pending state while retaining an economic-flat lifecycle boundary."""
    minimum_residual_release_confirmed = bool(
        str(getattr(allocation, "status", "") or "").upper() == "CLOSED"
        and abs(Decimal(getattr(allocation, "allocated_qty", 0) or 0))
        <= ALLOCATION_TRANSITION_TOLERANCE
        and abs(Decimal(getattr(allocation, "allocated_notional", 0) or 0))
        <= ALLOCATION_TRANSITION_TOLERANCE
        and str(getattr(allocation, "pending_reduce_reason", "") or "")
        == MINIMUM_RESIDUAL_ECONOMIC_FLAT_PENDING_REASON
    )
    clear_deferred_reduce_state(allocation)
    if minimum_residual_release_confirmed:
        allocation.pending_reduce_reason = (
            MINIMUM_RESIDUAL_ECONOMIC_FLAT_REASON
        )


def _apply_pending_reduce_offset_from_plan(
    allocation: LeaderPositionAllocationRecord,
    transition_plan: Any | None,
) -> bool:
    formula_inputs = getattr(transition_plan, "formula_inputs", None) or {}
    if "pending_reduce_offset_notional" not in formula_inputs:
        return False
    remaining_notional = _decimal_from_value(formula_inputs.get("pending_reduce_remaining_notional"))
    remaining_qty = _decimal_from_value(formula_inputs.get("pending_reduce_remaining_qty"))
    if remaining_notional is None or remaining_qty is None:
        return False
    if remaining_notional <= ALLOCATION_TRANSITION_TOLERANCE or remaining_qty <= ALLOCATION_TRANSITION_TOLERANCE:
        _clear_deferred_reduce(allocation)
        if not _allocation_needs_manual_review(allocation):
            allocation.status = "OPEN"
        return True
    allocation.pending_reduce_notional = _q(remaining_notional)
    allocation.pending_reduce_qty = _q(remaining_qty)
    allocation.status = "REDUCING"
    if not allocation.pending_reduce_reason:
        allocation.pending_reduce_reason = "pending reduce partially offset by later leader add"
    return True


def _account_value_result_like(payload: dict[str, Any]) -> Any:
    class Result:
        withdrawable_or_available = _decimal_from_payload(payload, "available_collateral_used_for_margin_check")

    return Result()


def _effective_leverage_plan(settings: Settings, leader_position: LatestAccountPosition | None) -> Any:
    raw = getattr(leader_position, "raw_payload_masked", None) or {}
    return build_hyperliquid_leverage_plan(
        default_leverage=settings.hyperliquid_default_leverage,
        coin_max_leverage=raw.get("maxLeverage"),
    )


def _market_policy_effective_leverage(
    market_meta: dict[str, Any] | None,
    *,
    canonical_coin_value: str,
    configured_default_leverage: int,
) -> int:
    """Resolve the no-I/O market policy from prewarmed exchange metadata."""

    meta = market_meta if isinstance(market_meta, dict) else {}
    market_max = _int_or_none(meta.get("maxLeverage"))
    if market_max is None:
        return int(configured_default_leverage or 50)
    margin_mode = (
        FALLBACK_MARGIN_MODE
        if market_requires_isolated_margin(meta)
        else DESIRED_MARGIN_MODE
    )
    policy_default = 1 if margin_mode == FALLBACK_MARGIN_MODE else market_max
    return (
        effective_leverage_for_margin_mode(
            market_max,
            desired_default_leverage=policy_default,
            margin_mode=margin_mode,
            canonical_coin_value=canonical_coin_value,
        )
        or policy_default
    )


def _resolved_market_effective_leverage(
    leverage_plan: Any,
    *,
    configured_default_leverage: int,
    required_margin_mode: str,
    canonical_coin_value: str,
) -> int:
    """Use the persisted/confirmed market target for sizing and margin checks.

    ``leverage_plan.effective_leverage`` is built from MarketRiskSetting when a
    market has already been configured.  The process-wide default is only a
    fallback for a genuinely new market.  Reapplying the global default here
    would understate required margin whenever a market is deliberately set
    below its exchange maximum (for example, SOL at cross 10x with a higher
    market maximum).
    """

    planned_leverage = (
        _int_or_none(getattr(leverage_plan, "effective_leverage", None))
        or int(configured_default_leverage or 10)
    )
    resolved = effective_leverage_for_margin_mode(
        getattr(leverage_plan, "max_leverage", None),
        desired_default_leverage=planned_leverage,
        margin_mode=required_margin_mode,
        canonical_coin_value=canonical_coin_value,
    )
    return resolved or planned_leverage


def _observed_margin_mode(leader_position: LatestAccountPosition | None) -> str | None:
    raw = getattr(leader_position, "raw_payload_masked", None) or {}
    leverage = raw.get("leverage") or {}
    if isinstance(leverage, dict):
        value = str(leverage.get("type") or "").upper()
        if value == "CROSSED":
            return "CROSS"
        return value or None
    return None


def _should_refresh_live_leader_position_for_reduce(implied: FillImpliedPosition) -> bool:
    confidence = str(implied.confidence or "").upper()
    if confidence not in {"HIGH", "MEDIUM"}:
        return False
    if (
        confidence == "HIGH"
        and implied.is_close
        and not implied.is_flip
        and implied.size_after <= ALLOCATION_TRANSITION_TOLERANCE
    ):
        return False
    return bool(implied.is_close or implied.is_flip)


def _flat_live_leader_position_snapshot(*, observed_at: datetime) -> LiveLeaderPositionSnapshot:
    return LiveLeaderPositionSnapshot(
        side=PositionSide.FLAT,
        size=Decimal("0"),
        signed_size=Decimal("0"),
        notional=Decimal("0"),
        entry_px=None,
        mark_px=None,
        raw_payload_masked={},
        last_update_at=observed_at,
    )


def _live_adjusted_fill_implied_position(
    *,
    fill: FillEvent,
    implied: FillImpliedPosition,
    live_position: LiveLeaderPositionSnapshot | None,
    observed_at: datetime,
    force_reduce: bool = False,
) -> FillImpliedPosition | None:
    if not force_reduce and not _should_refresh_live_leader_position_for_reduce(implied):
        return None
    before = implied.start_position
    if before is None or abs(before) <= ALLOCATION_TRANSITION_TOLERANCE:
        return None

    if live_position is None or live_position.side == PositionSide.FLAT:
        if implied.is_close and implied.size_after <= ALLOCATION_TRANSITION_TOLERANCE:
            return None
        return replace(
            implied,
            side_after=PositionSide.FLAT,
            size_after=Decimal("0"),
            signed_size_after=Decimal("0"),
            notional_after_estimate=Decimal("0"),
            is_open=False,
            is_increase=False,
            is_reduce=False,
            is_close=True,
            is_flip=False,
            confidence="HIGH",
            reason="live leader account state confirms flat post-position",
            entry_px=fill.price if fill.price > 0 else implied.entry_px,
        )

    live_signed = Decimal(live_position.signed_size)
    live_abs = abs(live_signed)
    if live_abs <= ALLOCATION_TRANSITION_TOLERANCE:
        return replace(
            implied,
            side_after=PositionSide.FLAT,
            size_after=Decimal("0"),
            signed_size_after=Decimal("0"),
            notional_after_estimate=Decimal("0"),
            is_open=False,
            is_increase=False,
            is_reduce=False,
            is_close=True,
            is_flip=False,
            confidence="HIGH",
            reason="live leader account state confirms flat post-position",
            entry_px=fill.price if fill.price > 0 else implied.entry_px,
        )

    same_direction = (before > 0) == (live_signed > 0)
    live_ahead_or_equal = live_abs <= implied.size_after + ALLOCATION_TRANSITION_TOLERANCE
    if same_direction and not live_ahead_or_equal:
        return None

    is_flip = not same_direction
    is_reduce = same_direction and live_abs < abs(before) - ALLOCATION_TRANSITION_TOLERANCE
    if not (is_reduce or is_flip):
        return None

    notional = live_position.notional
    if notional == 0 and fill.price > 0:
        notional = live_signed * fill.price
    return replace(
        implied,
        side_after=live_position.side,
        size_after=live_abs,
        signed_size_after=live_signed,
        notional_after_estimate=notional,
        is_open=False,
        is_increase=False,
        is_reduce=is_reduce,
        is_close=False,
        is_flip=is_flip,
        confidence="HIGH",
        reason="live leader account state confirms post-reduce position",
        entry_px=live_position.entry_px or implied.entry_px or (fill.price if fill.price > 0 else None),
    )


def derive_leader_post_position_from_fill(fill: FillEvent) -> FillImpliedPosition:
    start_position = _decimal_from_value((fill.raw or {}).get("startPosition"))
    if start_position is None or fill.price <= 0 or fill.size <= 0:
        return FillImpliedPosition(
            dex=fill.market.dex,
            canonical_coin=fill.market.canonical_coin,
            side_after=PositionSide.FLAT,
            size_after=Decimal("0"),
            signed_size_after=Decimal("0"),
            notional_after_estimate=Decimal("0"),
            fill_size=fill.size,
            start_position=start_position,
            direction=str((fill.raw or {}).get("dir") or ""),
            is_open=False,
            is_increase=False,
            is_reduce=False,
            is_close=False,
            is_flip=False,
            confidence="UNKNOWN",
            reason="FILL_POSITION_DERIVATION_UNKNOWN",
            entry_px=fill.price if fill.price > 0 else None,
        )
    side = str(fill.side or "").upper()
    if side == "B":
        signed_position = start_position + fill.size
    elif side == "A":
        signed_position = start_position - fill.size
    else:
        return FillImpliedPosition(
            dex=fill.market.dex,
            canonical_coin=fill.market.canonical_coin,
            side_after=PositionSide.FLAT,
            size_after=Decimal("0"),
            signed_size_after=Decimal("0"),
            notional_after_estimate=Decimal("0"),
            fill_size=fill.size,
            start_position=start_position,
            direction=str((fill.raw or {}).get("dir") or ""),
            is_open=False,
            is_increase=False,
            is_reduce=False,
            is_close=False,
            is_flip=False,
            confidence="UNKNOWN",
            reason="FILL_SIDE_UNKNOWN",
            entry_px=fill.price if fill.price > 0 else None,
        )
    direction = str((fill.raw or {}).get("dir") or "")
    before_abs = abs(start_position)
    after_abs = abs(signed_position)
    before_nonflat = before_abs > ALLOCATION_TRANSITION_TOLERANCE
    after_nonflat = after_abs > ALLOCATION_TRANSITION_TOLERANCE
    same_direction = before_nonflat and after_nonflat and (start_position > 0) == (signed_position > 0)
    is_flip = before_nonflat and after_nonflat and (start_position > 0) != (signed_position > 0)
    is_open = not before_nonflat and after_nonflat
    is_close = before_nonflat and not after_nonflat
    is_increase = same_direction and after_abs > before_abs
    is_reduce = same_direction and after_abs < before_abs
    if abs(signed_position) <= ALLOCATION_TRANSITION_TOLERANCE:
        return FillImpliedPosition(
            dex=fill.market.dex,
            canonical_coin=fill.market.canonical_coin,
            side_after=PositionSide.FLAT,
            size_after=Decimal("0"),
            signed_size_after=Decimal("0"),
            notional_after_estimate=Decimal("0"),
            fill_size=fill.size,
            start_position=start_position,
            direction=direction,
            is_open=is_open,
            is_increase=is_increase,
            is_reduce=is_reduce,
            is_close=is_close,
            is_flip=is_flip,
            confidence="HIGH",
            reason="fill startPosition + side/size implies flat post-position",
            entry_px=fill.price,
        )
    side_hint = PositionSide.LONG if signed_position > 0 else PositionSide.SHORT
    return FillImpliedPosition(
        dex=fill.market.dex,
        canonical_coin=fill.market.canonical_coin,
        side_after=side_hint,
        size_after=abs(signed_position),
        signed_size_after=signed_position,
        notional_after_estimate=signed_position * fill.price,
        fill_size=fill.size,
        start_position=start_position,
        direction=direction,
        is_open=is_open,
        is_increase=is_increase,
        is_reduce=is_reduce,
        is_close=is_close,
        is_flip=is_flip,
        confidence="HIGH",
        reason="fill startPosition + side/size implies post-position",
        entry_px=fill.price,
    )


def _fill_derived_leader_position(fill: FillEvent) -> FillImpliedPosition:
    return derive_leader_post_position_from_fill(fill)


def _should_use_fill_derived_position(
    leader_position: LatestAccountPosition | None,
    fill: FillEvent,
    derived: FillImpliedPosition,
) -> bool:
    return derived.confidence == "HIGH"


def _fill_implied_payload(implied: FillImpliedPosition | None) -> dict[str, Any] | None:
    if implied is None:
        return None
    return {
        "dex": implied.dex,
        "canonical_coin": implied.canonical_coin,
        "side_after": implied.side_after.value,
        "size_after": str(implied.size_after),
        "signed_size_after": str(implied.signed_size_after),
        "notional_after_estimate": str(implied.notional_after_estimate),
        "fill_size": str(implied.fill_size),
        "start_position": str(implied.start_position) if implied.start_position is not None else None,
        "direction": implied.direction,
        "is_open": implied.is_open,
        "is_increase": implied.is_increase,
        "is_reduce": implied.is_reduce,
        "is_close": implied.is_close,
        "is_flip": implied.is_flip,
        "confidence": implied.confidence,
        "reason": implied.reason,
    }


def _snapshot_position_is_stale_for_fill(leader_position: LatestAccountPosition | None, fill: FillEvent) -> bool:
    if leader_position is None:
        return True
    event_time = fill.hyperliquid_event_time
    last_update_at = getattr(leader_position, "last_update_at", None)
    if event_time is not None and last_update_at is not None:
        if last_update_at.tzinfo is None:
            last_update_at = last_update_at.replace(tzinfo=timezone.utc)
        if last_update_at < event_time:
            return True
    return False


def _decimal_from_payload(payload: dict[str, Any] | None, key: str) -> Decimal | None:
    if not payload:
        return None
    return _decimal_from_value(payload.get(key) or payload.get(_camel(key)))


def _configured_leader_account_value(leader: Any) -> Decimal | None:
    value = _decimal_from_value(getattr(leader, "fixed_account_value", None))
    return value if value is not None and value > 0 else None


def _account_value_payload_needs_refresh(payload: dict[str, Any] | None) -> bool:
    value = _decimal_from_payload(payload, "account_value_used_for_sizing")
    if value is None or value <= 0:
        return True
    blockers = [str(item).lower() for item in (payload or {}).get("blockers") or []]
    return any("account value unavailable" in blocker for blocker in blockers)


def _account_abstraction_payload_has_usable_value(
    payload: dict[str, Any] | None,
    *,
    dexes: list[str] | tuple[str, ...] | set[str],
) -> bool:
    normalized_dexes = [str(dex or "").lower() for dex in dexes]
    if not payload or not normalized_dexes:
        return False
    for dex in normalized_dexes:
        resolved = _resolved_account_value_payload(payload, dex)
        ready, _ = _account_value_payload_ready(resolved, role=FOLLOWER)
        if not ready:
            return False
    return True


def _shared_market_meta_setting_key(*, network: str, dex: str) -> str:
    return (
        f"hyperliquid_market_meta:{SHARED_MARKET_META_CACHE_VERSION}:"
        f"{str(network or 'mainnet').lower()}:{str(dex or 'default').lower()}"
    )


def _shared_perp_dex_directory_setting_key(*, network: str) -> str:
    return (
        f"hyperliquid_perp_dex_directory:{SHARED_PERP_DEX_DIRECTORY_CACHE_VERSION}:"
        f"{str(network or 'mainnet').lower()}"
    )


def _perp_dex_asset_offsets_from_payload(payload: Any) -> dict[str, int]:
    """Translate Hyperliquid's ordered perpDexs response into SDK offsets."""

    if not isinstance(payload, list) or not payload:
        return {}
    offsets: dict[str, int] = {"": 0}
    for index, item in enumerate(payload[1:]):
        if not isinstance(item, dict):
            continue
        dex = str(item.get("name") or "").strip().lower()
        if not dex or dex in offsets:
            continue
        # Matches the official SDK: builder DEX asset ranges begin at 110000
        # and advance by 10000 in the order returned by perpDexs.
        offsets[dex] = 110_000 + index * 10_000
    return offsets


def _parse_shared_perp_dex_directory_payload(
    payload: dict[str, Any] | None,
    *,
    expected_network: str,
) -> tuple[dict[str, int], datetime] | None:
    if not isinstance(payload, dict):
        return None
    if str(payload.get("version") or "") != SHARED_PERP_DEX_DIRECTORY_CACHE_VERSION:
        return None
    if str(payload.get("network") or "").lower() != str(expected_network or "").lower():
        return None
    refreshed_at = _datetime_or_none(payload.get("refreshed_at"))
    raw_offsets = payload.get("asset_offsets")
    if refreshed_at is None or not isinstance(raw_offsets, dict):
        return None
    offsets: dict[str, int] = {}
    for raw_dex, raw_offset in raw_offsets.items():
        dex = str(raw_dex or "").strip().lower()
        offset = _int_or_none(raw_offset)
        if offset is None or offset < 0 or dex in offsets:
            return None
        offsets[dex] = offset
    if offsets.get("") != 0:
        return None
    return offsets, refreshed_at


def _parse_shared_market_meta_payload(
    payload: dict[str, Any] | None,
    *,
    expected_dex: str,
) -> tuple[dict[str, Any], datetime, int] | None:
    if not isinstance(payload, dict):
        return None
    if str(payload.get("version") or "") != SHARED_MARKET_META_CACHE_VERSION:
        return None
    if str(payload.get("dex") or "").lower() != str(expected_dex or "").lower():
        return None
    meta = payload.get("meta")
    if not isinstance(meta, dict) or not isinstance(meta.get("universe"), list):
        return None
    refreshed_at = _datetime_or_none(payload.get("refreshed_at"))
    asset_offset = _int_or_none(payload.get("asset_offset"))
    if refreshed_at is None or asset_offset is None or asset_offset < 0:
        return None
    return dict(meta), refreshed_at, asset_offset


def _resolved_account_value_payload(
    payload: dict[str, Any] | None,
    dex: str,
) -> dict[str, Any] | None:
    resolved = resolved_value_payload(payload, dex)
    if resolved is None:
        return None
    snapshot_updated_at = (payload or {}).get("updated_at") or (payload or {}).get("updatedAt")
    if snapshot_updated_at is not None:
        resolved["snapshot_updated_at"] = snapshot_updated_at
    return resolved


def _account_value_payload_is_stale(
    payload: dict[str, Any] | None,
    *,
    max_age_seconds: float,
    now: datetime | None = None,
) -> bool:
    if not payload:
        return False
    raw_updated_at = payload.get("snapshot_updated_at")
    if raw_updated_at is None:
        # Older tests and pre-migration payloads do not carry freshness
        # metadata. Missing/zero values are handled by the separate readiness
        # guard; a newly persisted production snapshot always has this field.
        return False
    updated_at = _datetime_or_none(raw_updated_at)
    if updated_at is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    max_age = max(1.0, float(max_age_seconds or 0.0))
    return updated_at < current - timedelta(seconds=max_age)


def _account_value_payload_ready(payload: dict[str, Any] | None, *, role: str) -> tuple[bool, str | None]:
    value = _decimal_from_payload(payload, "account_value_used_for_sizing")
    if value is None or value <= 0:
        return False, "account value used for sizing unavailable"
    blockers = [str(item) for item in (payload or {}).get("blockers") or []]
    if blockers:
        return False, "; ".join(blockers)
    if str(role or "").upper() == LEADER:
        safety = _leader_account_value_safety_blockers(payload)
        if safety:
            return False, "; ".join(safety)
    return True, None


def _leader_account_value_safety_blockers(payload: dict[str, Any] | None) -> list[str]:
    if not payload:
        return []
    mode = str(payload.get("account_abstraction_mode") or payload.get("mode") or "").upper()
    source = str(payload.get("account_value_source") or payload.get("source") or "").upper()
    if mode != MODE_DEX_ABSTRACTION:
        return []
    if source == SOURCE_ACCOUNT_TOTAL:
        return []
    if source == SOURCE_CLEARINGHOUSE:
        return [
            "leader DEX_ABSTRACTION account value must use CURRENT_ACCOUNT_TOTAL; "
            "selected-dex clearinghouse accountValue is unsafe for ACCOUNT_RATIO sizing"
        ]
    return [
        "leader DEX_ABSTRACTION account value source is not CURRENT_ACCOUNT_TOTAL; "
        "blocking ACCOUNT_RATIO sizing"
    ]


def _risk_setting_result_from_row(row: MarketRiskSetting) -> RiskSettingResult:
    return RiskSettingResult(
        is_ok=True,
        status=row.status,
        account_address=row.account_address,
        dex=row.dex,
        canonical_coin=row.canonical_coin,
        desired_margin_mode=row.desired_margin_mode or DESIRED_MARGIN_MODE,
        desired_leverage=row.desired_leverage or 10,
        market_max_leverage=row.market_max_leverage,
        effective_leverage=row.effective_leverage,
        actual_margin_mode=row.actual_margin_mode or DESIRED_MARGIN_MODE,
        actual_leverage=row.actual_leverage or row.effective_leverage,
        asset_id=row.asset_id,
        cache_used=True,
        row_id=row.id,
        last_confirmed_at=row.last_confirmed_at,
    )


def _risk_setting_row_says_cross_margin_unsupported(row: MarketRiskSetting) -> bool:
    text = " ".join(
        str(item or "")
        for item in (
            row.error_message,
            json.dumps(row.raw_response_masked, default=str) if row.raw_response_masked is not None else "",
        )
    ).lower()
    return "cross margin is not allowed" in text or "cross margin not allowed" in text


def _decimal_from_value(value: Any) -> Decimal | None:
    if value is None or str(value) == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _camel(value: str) -> str:
    parts = value.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


def _latency_trace_payload(
    *,
    fill: FillEvent,
    dedupe_started_at: datetime,
    dedupe_done_at: datetime,
    debounce_started_at: datetime,
    debounce_released_at: datetime,
    lock_wait_started_at: datetime,
    lock_acquired_at: datetime,
    decision_started_at: datetime,
    account_cache_read_at: datetime,
    account_cache_read_done_at: datetime,
    price_cache_read_at: datetime,
    price_cache_read_done_at: datetime,
    allocation_read_at: datetime,
    allocation_read_done_at: datetime,
    sizing_done_at: datetime,
    validator_started_at: datetime | None,
    validator_done_at: datetime | None,
    checklist_done_at: datetime,
    decision_done_at: datetime,
    order_plan_created_at: datetime,
    order_submit_started_at: datetime | None,
) -> dict[str, Any]:
    leader_order_time = _timestamp_ms_to_datetime(_first_present(fill.raw.get("orderTime"), fill.raw.get("oidTime")))
    return {
        "timestamps": {
            "leader_order_time": _iso_or_none(leader_order_time),
            "leader_fill_time": _iso_or_none(fill.hyperliquid_event_time),
            "hyperliquid_event_time": _iso_or_none(fill.hyperliquid_event_time),
            "ws_received_at": _iso_or_none(fill.ws_received_at),
            "parse_started_at": _iso_or_none(fill.parse_started_at),
            "parse_done_at": _iso_or_none(fill.parse_done_at),
            "fill_queue_enqueued_at": _iso_or_none(fill.queue_enqueued_at),
            "fill_worker_started_at": _iso_or_none(fill.fill_worker_started_at),
            "dedupe_started_at": _iso_or_none(dedupe_started_at),
            "dedupe_done_at": _iso_or_none(dedupe_done_at),
            "debounce_started_at": _iso_or_none(debounce_started_at),
            "debounce_released_at": _iso_or_none(debounce_released_at),
            "lock_wait_started_at": _iso_or_none(lock_wait_started_at),
            "lock_acquired_at": _iso_or_none(lock_acquired_at),
            "decision_started_at": _iso_or_none(decision_started_at),
            "account_cache_read_at": _iso_or_none(account_cache_read_at),
            "account_cache_read_done_at": _iso_or_none(account_cache_read_done_at),
            "price_cache_read_at": _iso_or_none(price_cache_read_at),
            "price_cache_read_done_at": _iso_or_none(price_cache_read_done_at),
            "allocation_read_at": _iso_or_none(allocation_read_at),
            "allocation_read_done_at": _iso_or_none(allocation_read_done_at),
            "sizing_done_at": _iso_or_none(sizing_done_at),
            "validator_started_at": _iso_or_none(validator_started_at),
            "validator_done_at": _iso_or_none(validator_done_at),
            "checklist_done_at": _iso_or_none(checklist_done_at),
            "decision_done_at": _iso_or_none(decision_done_at),
            "order_plan_created_at": _iso_or_none(order_plan_created_at),
            "order_submit_started_at": _iso_or_none(order_submit_started_at),
            "order_submit_done_at": None,
            "order_ack_at": None,
            "order_finalized_at": None,
            "allocation_update_started_at": None,
            "allocation_update_done_at": None,
            "state_refresh_scheduled_at": None,
        },
        "metrics": {},
        "details": {
            "leader_fill_ingress_channel": fill.ingress_channel,
        },
        "missing_latency_fields": [],
    }


def _trace_set(order: ExecutionOrder, key: str, value: datetime | None) -> None:
    trace = _latency_trace_copy(getattr(order, "latency_trace", None))
    timestamps = trace["timestamps"]
    timestamps[key] = _iso_or_none(value)
    order.latency_trace = trace


def _trace_set_detail(order: ExecutionOrder, key: str, value: Any) -> None:
    trace = _latency_trace_copy(getattr(order, "latency_trace", None))
    details = trace["details"]
    details[key] = _json_safe(value)
    order.latency_trace = trace


def _trace_merge_submit_trace(order: ExecutionOrder, submit_trace: dict[str, Any]) -> None:
    if not submit_trace:
        return
    trace = _latency_trace_copy(getattr(order, "latency_trace", None))
    timestamps = trace["timestamps"]
    details = trace["details"]
    for key, value in submit_trace.items():
        if key.endswith("_at"):
            timestamps[key] = _json_safe(value)
        else:
            details[key] = _json_safe(value)
    order.latency_trace = trace


def _latency_trace_copy(value: Any) -> dict[str, Any]:
    base = dict(value) if isinstance(value, dict) else {}
    timestamps = dict(base.get("timestamps") or {})
    metrics = dict(base.get("metrics") or {})
    details = dict(base.get("details") or {})
    missing = list(base.get("missing_latency_fields") or [])
    base["timestamps"] = timestamps
    base["metrics"] = metrics
    base["details"] = details
    base["missing_latency_fields"] = missing
    return base


def _record_allocation_event(
    db: Any,
    *,
    allocation: LeaderPositionAllocationRecord,
    order: ExecutionOrder,
    action: str,
    before_notional: Decimal | None,
    after_notional: Decimal | None,
    before_qty: Decimal | None,
    after_qty: Decimal | None,
    metadata: dict[str, Any] | None = None,
) -> None:
    db.add(
        AllocationEvent(
            allocation_id=allocation.id,
            execution_order_id=order.id,
            leader_id=allocation.leader_id or order.leader_id,
            leader_address=allocation.leader_address,
            source_fill_id=order.source_fill_id,
            execution_venue=allocation.execution_venue,
            dex=allocation.dex,
            canonical_coin=allocation.canonical_coin,
            position_side=allocation.position_side,
            action=action,
            before_notional=before_notional,
            after_notional=after_notional,
            before_qty=before_qty,
            after_qty=after_qty,
            metadata_json=_json_safe(metadata or {}),
        )
    )


def _recent_submit_latency_summary(
    samples: list[dict[str, Any]],
    *,
    threshold_ms: int,
) -> dict[str, Any]:
    def values(key: str) -> list[int]:
        return sorted(
            parsed
            for sample in samples
            if (parsed := _int_or_none(sample.get(key))) is not None
        )

    def percentile(items: list[int], fraction: float) -> int | None:
        if not items:
            return None
        index = min(len(items) - 1, max(0, round((len(items) - 1) * fraction)))
        return int(items[index])

    marker_values = values("ws_to_submit_ms")
    actual_values = values("ws_to_actual_send_ms")
    misses = [
        sample
        for sample in samples
        if (
            (_int_or_none(sample.get("ws_to_actual_send_ms")) or -1) > threshold_ms
            or (_int_or_none(sample.get("ws_to_submit_ms")) or -1) > threshold_ms
        )
    ]
    return {
        "threshold_ms": int(threshold_ms),
        "sample_count": len(samples),
        "slo_miss_count": len(misses),
        "ws_to_submit_p50_ms": percentile(marker_values, 0.50),
        "ws_to_submit_p95_ms": percentile(marker_values, 0.95),
        "ws_to_submit_max_ms": max(marker_values) if marker_values else None,
        "ws_to_actual_send_p50_ms": percentile(actual_values, 0.50),
        "ws_to_actual_send_p95_ms": percentile(actual_values, 0.95),
        "ws_to_actual_send_max_ms": max(actual_values) if actual_values else None,
        "last_slo_miss": dict(misses[-1]) if misses else None,
    }


def _set_latency_fields(order: ExecutionOrder) -> None:
    trace = _latency_trace_copy(getattr(order, "latency_trace", None))
    timestamps = trace["timestamps"]
    metrics = trace["metrics"]
    order.event_to_ws_ms = _delta_ms(order.hyperliquid_event_time, order.ws_received_at)
    order.event_to_receive_ms = order.event_to_ws_ms
    order.leader_event_to_ws_ms = order.event_to_ws_ms
    order.ws_to_parse_ms = _delta_ms(order.ws_received_at, _trace_time(timestamps, "parse_started_at"))
    order.ws_to_dedupe_ms = _delta_ms(order.ws_received_at, order.dedupe_done_at)
    order.parse_to_dedupe_ms = _delta_ms(_trace_time(timestamps, "parse_done_at"), _trace_time(timestamps, "dedupe_started_at"))
    order.debounce_ms = _delta_ms(_trace_time(timestamps, "debounce_started_at") or order.dedupe_done_at, order.debounce_released_at)
    order.lock_wait_ms = _delta_ms(_trace_time(timestamps, "lock_wait_started_at"), _trace_time(timestamps, "lock_acquired_at"))
    order.decision_ms = _delta_ms(order.decision_started_at, order.decision_done_at)
    account_cache_read_ms = _delta_ms(_trace_time(timestamps, "account_cache_read_at"), _trace_time(timestamps, "account_cache_read_done_at"))
    price_cache_read_ms = _delta_ms(_trace_time(timestamps, "price_cache_read_at"), _trace_time(timestamps, "price_cache_read_done_at"))
    allocation_read_ms = _delta_ms(_trace_time(timestamps, "allocation_read_at"), _trace_time(timestamps, "allocation_read_done_at"))
    order.cache_read_ms = _sum_ms(
        account_cache_read_ms,
        price_cache_read_ms,
        allocation_read_ms,
    )
    order.sizing_ms = _delta_ms(_trace_time(timestamps, "allocation_read_done_at"), _trace_time(timestamps, "sizing_done_at"))
    metrics["dedupe_ms"] = _delta_ms(_trace_time(timestamps, "dedupe_started_at"), order.dedupe_done_at)
    metrics["parse_to_fill_queue_ms"] = _delta_ms(
        _trace_time(timestamps, "parse_done_at"),
        _trace_time(timestamps, "fill_queue_enqueued_at"),
    )
    metrics["fill_queue_wait_ms"] = _delta_ms(
        _trace_time(timestamps, "fill_queue_enqueued_at"),
        _trace_time(timestamps, "fill_worker_started_at"),
    )
    metrics["fill_worker_to_dedupe_ms"] = _delta_ms(
        _trace_time(timestamps, "fill_worker_started_at"),
        _trace_time(timestamps, "dedupe_started_at"),
    )
    metrics["account_cache_read_ms"] = account_cache_read_ms
    metrics["price_cache_read_ms"] = price_cache_read_ms
    metrics["allocation_read_ms"] = allocation_read_ms
    metrics["aggregate_allocation_read_ms"] = _delta_ms(
        _trace_time(timestamps, "aggregate_allocation_read_started_at"),
        _trace_time(timestamps, "aggregate_allocation_read_done_at"),
    )
    metrics["follower_position_read_ms"] = _delta_ms(
        _trace_time(timestamps, "follower_position_read_started_at"),
        _trace_time(timestamps, "follower_position_read_done_at"),
    )
    metrics["opposite_allocation_guard_ms"] = _delta_ms(
        _trace_time(timestamps, "opposite_allocation_guard_started_at"),
        _trace_time(timestamps, "opposite_allocation_guard_done_at"),
    )
    metrics["market_leverage_plan_ms"] = _delta_ms(
        _trace_time(timestamps, "market_leverage_plan_started_at"),
        _trace_time(timestamps, "market_leverage_plan_done_at"),
    )
    metrics["sizing_guard_ms"] = _delta_ms(
        _trace_time(timestamps, "sizing_guard_started_at"),
        _trace_time(timestamps, "sizing_guard_done_at"),
    )
    metrics["post_sizing_guard_chain_ms"] = _delta_ms(
        _trace_time(timestamps, "sizing_done_at"),
        _trace_time(timestamps, "validator_started_at"),
    )
    metrics["validator_ms"] = _delta_ms(
        _trace_time(timestamps, "validator_started_at"),
        _trace_time(timestamps, "validator_done_at"),
    )
    order_submit_done_at = getattr(order, "order_submit_done_at", None)
    metrics["pending_submit_db_write_ms"] = _delta_ms(
        _trace_time(timestamps, "pending_submit_write_started_at"),
        _trace_time(timestamps, "pending_submit_write_done_at"),
    )
    metrics["plan_commit_to_submit_worker_ms"] = _delta_ms(
        _trace_time(timestamps, "order_plan_commit_started_at"),
        _trace_time(timestamps, "submit_worker_started_at"),
    )
    metrics["order_plan_commit_ms"] = _delta_ms(
        _trace_time(timestamps, "order_plan_commit_started_at"),
        _trace_time(timestamps, "order_plan_committed_at"),
    )
    metrics["submit_scheduler_lag_ms"] = _delta_ms(
        _trace_time(timestamps, "order_plan_committed_at"),
        _trace_time(timestamps, "submit_worker_started_at"),
    )
    metrics["submit_worker_barrier_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_worker_started_at"),
        _trace_time(timestamps, "submit_session_started_at"),
    )
    metrics["submit_order_load_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_session_started_at"),
        _trace_time(timestamps, "submit_order_loaded_at"),
    )
    metrics["submit_db_connection_acquire_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_db_connection_acquire_started_at"),
        _trace_time(timestamps, "submit_db_connection_acquired_at"),
    )
    metrics["submit_order_query_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_order_query_started_at"),
        _trace_time(timestamps, "submit_order_query_done_at"),
    )
    metrics["submit_plan_guard_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_plan_guard_started_at"),
        _trace_time(timestamps, "submit_plan_guard_done_at"),
    )
    metrics["submit_claim_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_claim_started_at"),
        _trace_time(timestamps, "submit_claim_done_at"),
    )
    metrics["submit_marker_cas_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_marker_write_started_at"),
        _trace_time(timestamps, "submit_marker_cas_done_at"),
    )
    metrics["submit_marker_commit_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_marker_cas_done_at"),
        _trace_time(timestamps, "submit_marker_commit_done_at"),
    )
    metrics["submit_marker_db_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_marker_write_started_at"),
        _trace_time(timestamps, "submit_marker_commit_done_at"),
    )
    metrics["asset_id_hydrate_ms"] = _delta_ms(
        _trace_time(timestamps, "asset_id_hydrate_started_at"),
        _trace_time(timestamps, "asset_id_hydrate_done_at"),
    )
    metrics["submit_flow_ms"] = _delta_ms(
        _trace_time(timestamps, "submit_flow_started_at"),
        order.order_finalized_at or order.order_ack_at or order_submit_done_at,
    )
    metrics["risk_setting_ms"] = _delta_ms(
        _trace_time(timestamps, "risk_setting_started_at"),
        _trace_time(timestamps, "risk_setting_done_at"),
    )
    metrics["risk_setting_to_exchange_submit_ms"] = _delta_ms(
        _trace_time(timestamps, "risk_setting_done_at"),
        order.order_submit_started_at,
    )
    metrics["sdk_exchange_resolve_ms"] = _delta_ms(
        _trace_time(timestamps, "sdk_order_call_started_at"),
        _trace_time(timestamps, "sdk_exchange_ready_at"),
    )
    metrics["sdk_prepare_sign_ms"] = _delta_ms(
        _trace_time(timestamps, "sdk_prepare_started_at")
        or _trace_time(timestamps, "sdk_order_call_started_at"),
        _trace_time(timestamps, "sdk_prepare_done_at")
        or _trace_time(timestamps, "sdk_http_post_started_at"),
    )
    metrics["direct_http_submit_slot_wait_ms"] = _delta_ms(
        _trace_time(timestamps, "direct_http_submit_slot_wait_started_at"),
        _trace_time(timestamps, "direct_http_submit_slot_acquired_at"),
    )
    metrics["sdk_nonce_wait_ms"] = _delta_ms(
        _trace_time(timestamps, "sdk_nonce_wait_started_at"),
        _trace_time(timestamps, "sdk_nonce_allocated_at"),
    )
    metrics["http_exchange_response_ms"] = _delta_ms(
        _trace_time(timestamps, "sdk_http_post_started_at"),
        _trace_time(timestamps, "sdk_http_post_done_at"),
    )
    metrics["submit_to_http_response_ms"] = _delta_ms(
        order.order_submit_started_at,
        _trace_time(timestamps, "sdk_http_post_done_at"),
    )
    metrics["http_response_to_ack_record_ms"] = _delta_ms(
        _trace_time(timestamps, "sdk_http_post_done_at"),
        order.order_ack_at,
    )
    metrics["exchange_submit_call_ms"] = _delta_ms(
        _trace_time(timestamps, "exchange_submit_call_started_at") or order.order_submit_started_at,
        _trace_time(timestamps, "exchange_submit_call_done_at") or order_submit_done_at,
    )
    metrics["exchange_response_parse_ms"] = _delta_ms(
        _trace_time(timestamps, "exchange_submit_call_done_at") or order_submit_done_at,
        _trace_time(timestamps, "exchange_response_parsed_at"),
    )
    metrics["http_response_to_fill_confirmed_ms"] = _delta_ms(
        _trace_time(timestamps, "sdk_http_post_done_at"),
        _trace_time(timestamps, "fill_confirmed_at"),
    )
    metrics["fill_confirmed_to_db_update_ms"] = _delta_ms(
        _trace_time(timestamps, "fill_confirmed_at"),
        _trace_time(timestamps, "allocation_update_done_at"),
    )
    metrics["ack_to_final_ms"] = _delta_ms(order.order_ack_at, order.order_finalized_at)
    first_ws_send_at = _trace_time(timestamps, "ws_action_send_started_at")
    fallback_at = _trace_time(timestamps, "websocket_http_fallback_at")
    http_send_at = _trace_time(timestamps, "sdk_http_post_started_at")
    # An explicit WebSocket error proves that the action was not accepted and
    # permits a safe HTTP fallback. In that case the effective submit is the
    # later HTTP write, not the rejected WS attempt; using the first attempt
    # here previously understated two live orders by roughly 164ms.
    actual_send_at = http_send_at if fallback_at is not None else first_ws_send_at or http_send_at
    metrics["ws_to_first_transport_attempt_ms"] = _delta_ms(
        order.ws_received_at,
        first_ws_send_at or http_send_at,
    )
    metrics["websocket_rejection_to_http_send_ms"] = _delta_ms(
        fallback_at,
        http_send_at,
    )
    metrics["websocket_attempt_to_fallback_ms"] = _delta_ms(
        first_ws_send_at,
        fallback_at,
    )
    metrics["ws_to_actual_send_ms"] = _delta_ms(order.ws_received_at, actual_send_at)
    metrics["submit_marker_to_actual_send_ms"] = _delta_ms(
        order.order_submit_started_at,
        actual_send_at,
    )
    order.checklist_ms = _delta_ms(_trace_time(timestamps, "validator_done_at") or _trace_time(timestamps, "sizing_done_at"), _trace_time(timestamps, "checklist_done_at"))
    order.ws_to_submit_ms = _delta_ms(order.ws_received_at, order.order_submit_started_at)
    order.receive_to_submit_ms = order.ws_to_submit_ms
    order.submit_to_ack_ms = _delta_ms(order.order_submit_started_at, order.order_ack_at)
    order.event_to_ack_ms = _delta_ms(order.hyperliquid_event_time, order.order_ack_at)
    order.event_to_final_ms = _delta_ms(order.hyperliquid_event_time, order.order_finalized_at)
    order.allocation_update_ms = _delta_ms(
        _trace_time(timestamps, "allocation_update_started_at"),
        _trace_time(timestamps, "allocation_update_done_at"),
    )
    order.total_hot_path_ms = _delta_ms(
        order.ws_received_at,
        order.order_finalized_at or order.order_ack_at or order.decision_done_at,
    )
    for key in [
        "leader_event_to_ws_ms",
        "ws_to_parse_ms",
        "parse_to_dedupe_ms",
        "debounce_ms",
        "lock_wait_ms",
        "decision_ms",
        "cache_read_ms",
        "sizing_ms",
        "checklist_ms",
        "ws_to_submit_ms",
        "submit_to_ack_ms",
        "event_to_ack_ms",
        "event_to_final_ms",
        "allocation_update_ms",
        "total_hot_path_ms",
    ]:
        metrics[key] = getattr(order, key, None)
    missing = [
        key
        for key, value in timestamps.items()
        if value is None
        and key
        in {
            "leader_fill_time",
            "ws_received_at",
            "parse_started_at",
            "parse_done_at",
            "dedupe_started_at",
            "dedupe_done_at",
            "debounce_started_at",
            "debounce_released_at",
            "lock_wait_started_at",
            "lock_acquired_at",
            "decision_started_at",
            "account_cache_read_at",
            "account_cache_read_done_at",
            "price_cache_read_at",
            "price_cache_read_done_at",
            "allocation_read_at",
            "allocation_read_done_at",
            "sizing_done_at",
            "validator_started_at",
            "validator_done_at",
            "checklist_done_at",
            "decision_done_at",
            "order_plan_created_at",
            "order_submit_started_at",
            "order_submit_done_at",
            "order_ack_at",
            "order_finalized_at",
            "allocation_update_started_at",
            "allocation_update_done_at",
            "state_refresh_scheduled_at",
        }
    ]
    trace["missing_latency_fields"] = missing
    order.missing_latency_fields = missing
    order.latency_trace = trace


def _delta_ms(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return int((end - start).total_seconds() * 1000)


def _sum_ms(*values: int | None) -> int | None:
    present = [value for value in values if value is not None]
    return sum(present) if present else None


def _trace_time(timestamps: dict[str, Any], key: str) -> datetime | None:
    value = timestamps.get(key)
    if value is None or str(value) == "":
        return None
    if isinstance(value, datetime):
        return value
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _timestamp_ms_to_datetime(value: Any) -> datetime | None:
    parsed = _int_or_none(value)
    if parsed is None or parsed <= 0:
        return None
    return datetime.fromtimestamp(parsed / 1000, timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _opposite_side(side: PositionSide) -> PositionSide:
    return PositionSide.SHORT if side == PositionSide.LONG else PositionSide.LONG


def _opposite_allocation_exists_in_snapshot(
    *,
    allocation_qty_by_side: dict[PositionSide, Decimal],
    current_allocation: LeaderPositionAllocationRecord | None,
    current_leader_id: int | None,
    new_side: PositionSide,
) -> bool:
    """Apply the aggregate netting guard using the already-loaded snapshot.

    The aggregate snapshot contains all enabled leaders.  The SQL guard this
    replaces excludes the current leader, so subtract that leader's sole active
    opposite-side allocation before deciding.  The partial unique index keeps
    that active leader/market/side row singular.
    """
    opposite = _opposite_side(new_side)
    opposite_qty = abs(Decimal(allocation_qty_by_side.get(opposite, 0) or 0))
    if (
        current_allocation is not None
        and current_leader_id is not None
        and getattr(current_allocation, "leader_id", None) == current_leader_id
        and not _allocation_row_closed(current_allocation)
        and _position_side_or_none(getattr(current_allocation, "position_side", None)) == opposite
    ):
        opposite_qty = max(
            Decimal("0"),
            opposite_qty - abs(Decimal(getattr(current_allocation, "allocated_qty", 0) or 0)),
        )
    return opposite_qty > ALLOCATION_TRANSITION_TOLERANCE


def _state_updated_at(state: LatestAccountState | None) -> datetime | None:
    if state is None:
        return None
    return _datetime_or_none(getattr(state, "last_update_at", None) or getattr(state, "updated_at", None))


def _latest_datetime(*values: datetime | None) -> datetime | None:
    latest: datetime | None = None
    for value in values:
        parsed = _datetime_or_none(value)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest


def _datetime_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _actual_scope_qty(
    positions: dict[tuple[str, str, str], list[LatestAccountPosition]],
    dex: str,
    canonical_coin: str,
    side: PositionSide,
) -> Decimal:
    position = _preferred_actual_position(
        positions.get((str(dex or "").lower(), str(canonical_coin or "").upper(), side.value), [])
    )
    return abs(Decimal(getattr(position, "size", 0) or 0)) if position is not None else Decimal("0")


def _preferred_actual_position(positions: list[LatestAccountPosition] | tuple[LatestAccountPosition, ...]) -> LatestAccountPosition | None:
    if not positions:
        return None
    return max(positions, key=_actual_position_preference_key)


def _actual_position_preference_key(position: LatestAccountPosition) -> tuple[int, datetime, int]:
    source = str(getattr(position, "mark_px_source", "") or "").upper()
    source_rank = 0 if source in LOCAL_POSITION_PROJECTION_SOURCES else 1
    updated_at = _datetime_or_none(getattr(position, "last_update_at", None))
    if updated_at is None:
        updated_at = datetime.min.replace(tzinfo=timezone.utc)
    try:
        row_id = int(getattr(position, "id", 0) or 0)
    except Exception:
        row_id = 0
    return source_rank, updated_at, row_id


def _position_entry_price(position: LatestAccountPosition | None) -> Decimal | None:
    if position is None:
        return None
    return _decimal_from_value(getattr(position, "entry_px", None))


def _position_sync_mark_price(
    position: LatestAccountPosition | None,
    actual_qty: Decimal,
    actual_notional: Decimal,
) -> Decimal:
    if actual_qty > ALLOCATION_TRANSITION_TOLERANCE and actual_notional > 0:
        return actual_notional / actual_qty
    if position is not None:
        for attr in ("mark_px", "mid_px", "entry_px"):
            value = _decimal_from_value(getattr(position, attr, None))
            if value is not None and value > 0:
                return value
    return Decimal("0")


def _allocation_active(allocation: LeaderPositionAllocationRecord | None) -> bool:
    if allocation is None or str(allocation.status).upper() == "CLOSED":
        return False
    return Decimal(allocation.allocated_qty or 0) > ALLOCATION_TRANSITION_TOLERANCE or Decimal(allocation.allocated_notional or 0) > ALLOCATION_TRANSITION_TOLERANCE


def _allocation_lifecycle_active(allocation: LeaderPositionAllocationRecord | None) -> bool:
    if allocation is None:
        return False
    status = str(allocation.status or "").upper()
    if status == "CLOSED":
        return False
    if status == PENDING_OPEN_STATUS:
        return _valid_pending_open_lifecycle(allocation)
    return _allocation_active(allocation)


def _coin_config_allows_fill(
    *,
    config_allowed: bool,
    allocation: LeaderPositionAllocationRecord | None,
    fill_implied_position: Any | None,
    transition_plan: Any | None,
) -> bool:
    """Apply coin filters at lifecycle boundaries, not in the middle of a position.

    A newly blocked coin must not claim a released market or open a new/flip
    lifecycle. An allocation that was already active (including a valid
    below-minimum pending lifecycle) must still receive increases, reductions,
    and its final close so filtering cannot strand follower exposure.
    """
    if config_allowed:
        return True
    if _fill_is_new_open_from_flat(fill_implied_position):
        return False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value == AllocationTransitionAction.FLIP_OPEN_SECOND.value:
        return False
    return _allocation_lifecycle_active(allocation)


def _allocation_market_owner_active(
    allocation: LeaderPositionAllocationRecord | None,
    pending_intents: PendingIntentLedger | None = None,
) -> bool:
    if allocation is None or str(allocation.status or "").upper() == "CLOSED":
        return False
    if _allocation_lifecycle_active(allocation):
        return True
    if pending_intents is not None and pending_intents.has_pending_allocation(allocation):
        return True
    return False


def _market_owner_blocker(
    owner_allocation: LeaderPositionAllocationRecord | None,
    *,
    leader: LeaderConfig,
    current_allocation: LeaderPositionAllocationRecord | None,
) -> str | None:
    if owner_allocation is None:
        return None
    current_leader_id = int(getattr(leader, "id", 0) or 0)
    owner_leader_id = int(getattr(owner_allocation, "leader_id", 0) or 0)
    owner_address = normalize_leader_address(getattr(owner_allocation, "leader_address", "") or "")
    current_address = normalize_leader_address(getattr(leader, "leader_address", "") or "")
    owner_allocation_id = _int_or_none(getattr(owner_allocation, "id", None))
    current_allocation_id = _int_or_none(getattr(current_allocation, "id", None))
    if owner_allocation_id is not None and current_allocation_id is not None and owner_allocation_id == current_allocation_id:
        return None
    if owner_leader_id and current_leader_id and owner_leader_id == current_leader_id:
        return None
    if owner_address and current_address and owner_address == current_address:
        return None
    coin = getattr(owner_allocation, "canonical_coin", None) or getattr(owner_allocation, "hyperliquid_coin", "")
    return (
        "MARKET_OWNER_BLOCKED: "
        f"{coin} is already owned by leader {mask_address(owner_address)} "
        f"(allocation {getattr(owner_allocation, 'id', None)}); waiting until that allocation is CLOSED"
    )


def _allocation_has_flat_leader_close_intent(allocation: LeaderPositionAllocationRecord | None) -> bool:
    if allocation is None:
        return False
    target = abs(Decimal(getattr(allocation, "target_notional", 0) or 0))
    if target > ALLOCATION_TRANSITION_TOLERANCE:
        return False
    leader_size = getattr(allocation, "last_leader_position_size", None)
    leader_notional = getattr(allocation, "last_leader_position_notional", None)
    if leader_size is not None and abs(Decimal(leader_size or 0)) <= ALLOCATION_TRANSITION_TOLERANCE:
        return True
    if leader_notional is not None and abs(Decimal(leader_notional or 0)) <= ALLOCATION_TRANSITION_TOLERANCE:
        return True
    return False


def _allocation_last_leader_abs_size(allocation: LeaderPositionAllocationRecord | None) -> Decimal:
    if allocation is None:
        return Decimal("0")
    value = _decimal_from_value(getattr(allocation, "last_leader_position_size", None))
    return abs(value) if value is not None else Decimal("0")


def _allocation_last_leader_abs_notional(allocation: LeaderPositionAllocationRecord | None) -> Decimal:
    if allocation is None:
        return Decimal("0")
    value = _decimal_from_value(getattr(allocation, "last_leader_position_notional", None))
    return abs(value) if value is not None else Decimal("0")


def _allocation_leader_snapshot_nonflat(allocation: LeaderPositionAllocationRecord | None) -> bool:
    return (
        _allocation_last_leader_abs_size(allocation) > ALLOCATION_TRANSITION_TOLERANCE
        or _allocation_last_leader_abs_notional(allocation) > ALLOCATION_TRANSITION_TOLERANCE
    )


def _valid_pending_open_lifecycle(allocation: LeaderPositionAllocationRecord | None) -> bool:
    if allocation is None or str(allocation.status or "").upper() != PENDING_OPEN_STATUS:
        return False
    if _allocation_active(allocation):
        return True
    return str(allocation.pending_reduce_reason or "") == PENDING_OPEN_REASON


def _stale_zero_allocation_reason(allocation: LeaderPositionAllocationRecord | None) -> str | None:
    if allocation is None:
        return None
    status = str(allocation.status or "").upper()
    if status == "CLOSED" or _allocation_active(allocation):
        return None
    if _valid_pending_open_lifecycle(allocation):
        return None
    reason = str(allocation.pending_reduce_reason or "").strip()
    suffix = f" ({reason})" if reason else ""
    return f"STALE_ZERO_ALLOCATION: zero-fill {status or 'UNKNOWN'} allocation cannot activate a missed lifecycle{suffix}"


def _close_zero_allocation_lifecycle(
    allocation: LeaderPositionAllocationRecord,
    *,
    reason: str,
    now: datetime,
) -> None:
    allocation.status = "CLOSED"
    allocation.target_notional = Decimal("0")
    allocation.allocated_notional = Decimal("0")
    allocation.allocated_qty = Decimal("0")
    allocation.pending_reduce_qty = None
    allocation.pending_reduce_notional = None
    allocation.pending_reduce_reason = reason[:1000]
    allocation.pending_reduce_since = None
    allocation.pending_reduce_source_fill_id = None
    allocation.last_reconcile_at = now


def _open_like_action(action: str | None) -> bool:
    return str(action or "").upper() in {
        AllocationTransitionAction.OPEN.value,
        AllocationTransitionAction.INCREASE.value,
        AllocationTransitionAction.FLIP_OPEN_SECOND.value,
        "OPEN_OR_INCREASE",
    }


def _reduce_like_action(action: str | None) -> bool:
    return str(action or "").upper() in {
        AllocationTransitionAction.REDUCE.value,
        AllocationTransitionAction.CLOSE.value,
        AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
        "CLOSE_OR_REDUCE",
    }


def _reduce_only_rejected_position_absent(error_message: str | None) -> bool:
    return "reduce only order would increase position" in str(error_message or "").lower()


def _with_clamped_reduce_validator_payload(
    checklist: dict[str, Any],
    *,
    clamped_qty: Decimal,
    clamped_notional: Decimal,
    original_qty: Decimal,
    original_notional: Decimal,
    remaining_allocation_qty: Decimal,
) -> dict[str, Any]:
    updated = dict(checklist)
    validator = dict(updated.get("order_validator") or {})
    payload = dict(validator.get("payload_masked") or {})
    qty_text = str(_q(clamped_qty))
    notional_text = str(_q(clamped_notional))
    validator["raw_size"] = qty_text
    validator["rounded_size"] = qty_text
    validator["estimated_notional"] = notional_text
    validator["target_delta_notional"] = notional_text
    payload["sz"] = qty_text
    payload["quantity"] = qty_text
    validator["payload_masked"] = payload
    warnings = list(validator.get("warnings") or [])
    warnings.append("reduce submit qty clamped to remaining allocation before exchange submit")
    validator["warnings"] = warnings
    updated["order_validator"] = validator
    updated["submit_reduce_allocation_guard"] = {
        "ok": True,
        "clamped": True,
        "original_qty": original_qty,
        "clamped_qty": clamped_qty,
        "remaining_allocation_qty": remaining_allocation_qty,
        "original_notional": original_notional,
        "clamped_notional": clamped_notional,
    }
    return updated


def _with_expanded_final_close_validator_payload(
    checklist: dict[str, Any],
    *,
    expanded_qty: Decimal,
    expanded_notional: Decimal,
    original_qty: Decimal,
    original_notional: Decimal,
    remaining_allocation_qty: Decimal,
) -> dict[str, Any]:
    updated = dict(checklist)
    validator = dict(updated.get("order_validator") or {})
    payload = dict(validator.get("payload_masked") or {})
    qty_text = str(_q(expanded_qty))
    notional_text = str(_q(expanded_notional))
    validator["raw_size"] = qty_text
    validator["rounded_size"] = qty_text
    validator["estimated_notional"] = notional_text
    validator["target_delta_notional"] = notional_text
    payload["sz"] = qty_text
    payload["quantity"] = qty_text
    validator["payload_masked"] = payload
    warnings = list(validator.get("warnings") or [])
    warnings.append("final close quantity expanded to remaining allocation after prior intents resolved")
    validator["warnings"] = warnings
    updated["order_validator"] = validator
    updated["submit_reduce_allocation_guard"] = {
        "ok": True,
        "expanded": True,
        "original_qty": original_qty,
        "expanded_qty": expanded_qty,
        "remaining_allocation_qty": remaining_allocation_qty,
        "original_notional": original_notional,
        "expanded_notional": expanded_notional,
    }
    return updated


def _pending_open_activation_block_reason(
    allocation: LeaderPositionAllocationRecord | None,
    fill_implied_position: Any | None,
    transition_plan: Any | None,
) -> str | None:
    if allocation is None or str(allocation.status or "").upper() != PENDING_OPEN_STATUS:
        return None
    if not _valid_pending_open_lifecycle(allocation):
        return "PENDING_OPEN_INVALID_REASON: only below-min initial opens can activate later fills"
    # The durable allocation can still be PENDING_OPEN while earlier OPEN or
    # INCREASE orders are represented by the in-memory pending-intent overlay.
    # Once that overlay has positive quantity, a following reduce/close belongs
    # to the same active lifecycle.  Submit barriers keep it behind the earlier
    # increases and the final reduce guard rechecks the filled allocation before
    # any exchange call.
    if _allocation_active(allocation):
        return None
    if transition_plan is None:
        return None
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value in {AllocationTransitionAction.NOOP.value, AllocationTransitionAction.BLOCK.value}:
        return None
    if fill_implied_position is None:
        return "PENDING_OPEN_WAITING_FOR_OPEN_OR_INCREASE: fill direction unavailable"
    confidence = str(getattr(fill_implied_position, "confidence", "") or "").upper()
    if confidence not in {"HIGH", "MEDIUM"}:
        return "PENDING_OPEN_WAITING_FOR_OPEN_OR_INCREASE: fill direction unavailable"
    if bool(getattr(fill_implied_position, "is_open", False)) or bool(getattr(fill_implied_position, "is_increase", False)):
        return None
    return "PENDING_OPEN_WAITING_FOR_OPEN_OR_INCREASE: reduce/close fill cannot activate pending open"


def _fill_direction_action_block_reason(
    fill_implied_position: Any | None,
    transition_plan: Any | None,
    *,
    allow_missed_reduce_catchup: bool = False,
    allow_checkpoint_flip_open: bool = False,
) -> str | None:
    if transition_plan is None:
        return None
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    return _fill_direction_action_value_block_reason(
        fill_implied_position,
        action_value,
        allow_missed_reduce_catchup=allow_missed_reduce_catchup,
        allow_reduce_fill_close=_transition_plan_targets_flat_leader(transition_plan),
        allow_checkpoint_flip_open=allow_checkpoint_flip_open,
    )


def _transition_plan_targets_flat_leader(transition_plan: Any | None) -> bool:
    if transition_plan is None:
        return False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    target_notional = _decimal_from_value(getattr(transition_plan, "target_notional", None))
    if target_notional is None or abs(target_notional) > ALLOCATION_TRANSITION_TOLERANCE:
        return False
    if action_value in {
        AllocationTransitionAction.CLOSE.value,
        AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
    }:
        return True
    formula_inputs = getattr(transition_plan, "formula_inputs", None)
    if not isinstance(formula_inputs, dict):
        return False
    leader_side = str(formula_inputs.get("leader_side") or "").upper()
    leader_size = _decimal_from_value(formula_inputs.get("leader_position_size"))
    leader_notional = _decimal_from_value(formula_inputs.get("leader_position_notional"))
    if leader_side == PositionSide.FLAT.value:
        return True
    if leader_size is not None and abs(leader_size) <= ALLOCATION_TRANSITION_TOLERANCE:
        return True
    return leader_notional is not None and abs(leader_notional) <= ALLOCATION_TRANSITION_TOLERANCE


def _fill_is_reduce_or_close(fill_implied_position: Any | None) -> bool:
    if fill_implied_position is None:
        return False
    confidence = str(getattr(fill_implied_position, "confidence", "") or "").upper()
    if confidence not in {"HIGH", "MEDIUM"}:
        return False
    return bool(getattr(fill_implied_position, "is_reduce", False)) or bool(
        getattr(fill_implied_position, "is_close", False)
    )


def _fill_previous_position_size(fill_implied_position: Any | None) -> Decimal | None:
    if not _fill_is_reduce_or_close(fill_implied_position):
        return None
    start_position = _decimal_from_value(getattr(fill_implied_position, "start_position", None))
    if start_position is None or abs(start_position) <= ALLOCATION_TRANSITION_TOLERANCE:
        return None
    return abs(start_position)


def _missed_reduce_catchup_allows_direction_mismatch(
    *,
    fill_implied_position: Any | None,
    transition_plan: Any | None,
    planning_allocation: Any | None,
) -> bool:
    if fill_implied_position is None or transition_plan is None or planning_allocation is None:
        return False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value != AllocationTransitionAction.REDUCE.value:
        return False
    confidence = str(getattr(fill_implied_position, "confidence", "") or "").upper()
    if confidence not in {"HIGH", "MEDIUM"}:
        return False
    if not (
        bool(getattr(fill_implied_position, "is_open", False))
        or bool(getattr(fill_implied_position, "is_increase", False))
    ):
        return False
    start_position = _decimal_from_value(getattr(fill_implied_position, "start_position", None))
    checkpoint_size = _decimal_from_value(getattr(planning_allocation, "last_leader_position_size", None))
    current_allocation_qty = _decimal_from_value(getattr(planning_allocation, "allocated_qty", None)) or Decimal("0")
    if start_position is None or checkpoint_size is None:
        return False
    if abs(checkpoint_size) <= ALLOCATION_TRANSITION_TOLERANCE:
        return False
    if current_allocation_qty <= ALLOCATION_TRANSITION_TOLERANCE:
        return False
    if abs(start_position) >= abs(checkpoint_size) - ALLOCATION_TRANSITION_TOLERANCE:
        return False
    current_notional = _decimal_from_value(getattr(transition_plan, "current_allocation_notional", None))
    target_notional = _decimal_from_value(getattr(transition_plan, "target_notional", None))
    if current_notional is None or target_notional is None:
        return False
    return target_notional < current_notional - ALLOCATION_TRANSITION_TOLERANCE


def _fill_direction_action_value_block_reason(
    fill_implied_position: Any | None,
    action_value: str,
    *,
    allow_missed_reduce_catchup: bool = False,
    allow_reduce_fill_close: bool = False,
    allow_checkpoint_flip_open: bool = False,
) -> str | None:
    action_value = str(action_value or "").upper()
    if action_value in {AllocationTransitionAction.NOOP.value, AllocationTransitionAction.BLOCK.value}:
        return None
    if fill_implied_position is None:
        return "FILL_DIRECTION_GUARD: fill direction unavailable"
    confidence = str(getattr(fill_implied_position, "confidence", "") or "").upper()
    if confidence not in {"HIGH", "MEDIUM"}:
        return "FILL_DIRECTION_GUARD: fill direction unavailable"

    open_actions = {
        AllocationTransitionAction.OPEN.value,
        AllocationTransitionAction.INCREASE.value,
        AllocationTransitionAction.FLIP_OPEN_SECOND.value,
    }
    reduce_actions = {
        AllocationTransitionAction.REDUCE.value,
        AllocationTransitionAction.CLOSE.value,
        AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
    }
    is_open = bool(getattr(fill_implied_position, "is_open", False))
    is_increase = bool(getattr(fill_implied_position, "is_increase", False))
    is_reduce = bool(getattr(fill_implied_position, "is_reduce", False))
    is_close = bool(getattr(fill_implied_position, "is_close", False))
    is_flip = bool(getattr(fill_implied_position, "is_flip", False))

    if (is_reduce or is_close) and action_value in open_actions:
        return "FILL_DIRECTION_GUARD: leader reduce/close fill cannot create or increase follower position"
    if (
        is_reduce
        and action_value in {AllocationTransitionAction.CLOSE.value, AllocationTransitionAction.FLIP_CLOSE_FIRST.value}
        and not allow_reduce_fill_close
    ):
        return "FILL_DIRECTION_GUARD: leader partial reduce fill cannot close follower allocation"
    if is_close and action_value == AllocationTransitionAction.REDUCE.value:
        return "FILL_DIRECTION_GUARD: leader close fill must close follower allocation"
    if (is_open or is_increase) and action_value in reduce_actions and not allow_missed_reduce_catchup:
        return "FILL_DIRECTION_GUARD: leader open/increase fill cannot reduce or close follower position"
    if is_flip and action_value in open_actions and not allow_checkpoint_flip_open:
        return "FILL_DIRECTION_GUARD: leader flip fill cannot directly open follower second leg"
    return None


def _order_intent_blockers(
    order: ExecutionOrder,
    fill_implied_position: Any | None,
    *,
    reduce_only: bool,
    allow_missed_reduce_catchup: bool = False,
) -> list[str]:
    blockers: list[str] = []
    action = str(order.order_action or "").upper()
    side = str(order.side or "").upper()
    position_side = str(order.position_side or "").upper()
    order_reduce_only = bool(order.reduce_only)
    open_actions = {
        AllocationTransitionAction.OPEN.value,
        AllocationTransitionAction.INCREASE.value,
        AllocationTransitionAction.FLIP_OPEN_SECOND.value,
    }
    reduce_actions = {
        AllocationTransitionAction.REDUCE.value,
        AllocationTransitionAction.CLOSE.value,
        AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
    }
    if order_reduce_only != bool(reduce_only):
        blockers.append("INTERNAL_SUBMIT_GUARD: reduce_only mismatch")
    if action in open_actions and order_reduce_only:
        blockers.append("INTERNAL_SUBMIT_GUARD: open/increase cannot be reduce_only")
    if action in reduce_actions and not order_reduce_only:
        blockers.append("INTERNAL_SUBMIT_GUARD: reduce/close must be reduce_only")
    if action not in open_actions and action not in reduce_actions:
        blockers.append("INTERNAL_SUBMIT_GUARD: unsupported submit action")
    checklist = getattr(order, "pre_trade_checklist", None) or {}
    if (
        bool(checklist.get("market_ownership_acquisition_required"))
        and not _fill_is_new_open_from_flat(fill_implied_position)
        and not bool(checklist.get("market_ownership_checkpoint_flip_open"))
        and not bool(checklist.get("market_ownership_economic_dust_reopen"))
    ):
        blockers.append(
            "OWNERSHIP_ACQUISITION_GUARD: a released market can only be claimed "
            "by a genuine leader open from startPosition=0, a checkpoint-contiguous "
            "flip, or a same-side increase from sub-minimum dust into tradeable size"
        )
    if position_side not in {"LONG", "SHORT"}:
        blockers.append("INTERNAL_SUBMIT_GUARD: missing position_side")
    else:
        expected_side = _order_side(target_side=PositionSide(position_side), reduce_only=order_reduce_only)
        if side != expected_side:
            blockers.append(
                f"INTERNAL_SUBMIT_GUARD: side/action mismatch expected {expected_side}, got {side or 'NONE'}"
            )
    direction_reason = _fill_direction_action_value_block_reason(
        fill_implied_position,
        action,
        allow_missed_reduce_catchup=allow_missed_reduce_catchup,
        allow_checkpoint_flip_open=bool(
            checklist.get("market_ownership_checkpoint_flip_open")
        ),
    )
    if direction_reason:
        blockers.append(direction_reason)
    return blockers


def _pending_open_allocation_flat(
    allocation: LeaderPositionAllocationRecord | None,
    transition_plan: Any | None,
) -> bool:
    if allocation is None or transition_plan is None:
        return False
    if str(allocation.status or "").upper() != PENDING_OPEN_STATUS:
        return False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    return (
        action_value == AllocationTransitionAction.NOOP.value
        and Decimal(allocation.allocated_qty or 0) <= ALLOCATION_TRANSITION_TOLERANCE
        and Decimal(allocation.allocated_notional or 0) <= ALLOCATION_TRANSITION_TOLERANCE
        and Decimal(getattr(transition_plan, "target_notional", 0) or 0) <= ALLOCATION_TRANSITION_TOLERANCE
    )


def _allocation_mismatch_from_stale_follower_state(
    *,
    allocation_mismatch: bool,
    follower_state_at: datetime | None,
    allocation_latest_reconcile_at: datetime | None,
) -> bool:
    if not allocation_mismatch or follower_state_at is None or allocation_latest_reconcile_at is None:
        return False
    return allocation_latest_reconcile_at > follower_state_at


def _empty_position_side_qtys() -> dict[PositionSide, Decimal]:
    return {PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")}


def _allocation_row_closed(row: Any) -> bool:
    return str(getattr(row, "status", "") or "").upper() == "CLOSED"


def _allocation_checkpoint_at(row: Any) -> datetime | None:
    return (
        _datetime_or_none(getattr(row, "last_reconcile_at", None))
        or _datetime_or_none(getattr(row, "updated_at", None))
        or _datetime_or_none(getattr(row, "created_at", None))
    )


def _latest_allocation_reconcile_at(rows: list[Any]) -> datetime | None:
    return max(
        (checkpoint for row in rows if (checkpoint := _allocation_checkpoint_at(row)) is not None),
        default=None,
    )


def _position_side_or_none(value: Any) -> PositionSide | None:
    if isinstance(value, PositionSide):
        return value if value in {PositionSide.LONG, PositionSide.SHORT} else None
    try:
        side = PositionSide(str(value or "").upper())
    except ValueError:
        return None
    return side if side in {PositionSide.LONG, PositionSide.SHORT} else None


def _position_side_qtys_payload(qtys: dict[PositionSide, Decimal] | None) -> dict[str, str]:
    values = qtys or _empty_position_side_qtys()
    return {side.value: str(Decimal(values.get(side, 0))) for side in (PositionSide.LONG, PositionSide.SHORT)}


def _unmanaged_follower_position_from_stale_follower_state(
    *,
    unmanaged_follower_position: bool,
    follower_state_at: datetime | None,
    allocation_latest_reconcile_at: datetime | None,
) -> bool:
    if not unmanaged_follower_position or follower_state_at is None or allocation_latest_reconcile_at is None:
        return False
    return allocation_latest_reconcile_at > follower_state_at


def _unmanaged_follower_position_qtys(
    *,
    follower_qty_by_side: dict[PositionSide, Decimal] | None,
    allocation_qty_by_side: dict[PositionSide, Decimal] | None,
) -> dict[PositionSide, Decimal]:
    follower = follower_qty_by_side or _empty_position_side_qtys()
    allocation = allocation_qty_by_side or _empty_position_side_qtys()
    return {
        side: max(Decimal(follower.get(side, 0)) - Decimal(allocation.get(side, 0)), Decimal("0"))
        for side in (PositionSide.LONG, PositionSide.SHORT)
    }


def _manual_same_side_position_sync(
    *,
    allocation: LeaderPositionAllocationRecord | None,
    planning_allocation: Any | None,
    transition_plan: Any | None,
    aggregate_side: PositionSide,
    follower_qty_by_side: dict[PositionSide, Decimal] | None,
    allocation_qty_by_side: dict[PositionSide, Decimal] | None,
    follower_state_at: datetime | None,
    allocation_latest_reconcile_at: datetime | None,
    has_pending_allocation: bool,
    mark_price: Decimal,
    allow_flat_close: bool = True,
    allow_actual_qty_increase: bool = False,
    trusted_actual_qty_ceiling: Decimal | None = None,
) -> dict[str, Any]:
    if allocation is None or planning_allocation is None or transition_plan is None:
        return {"applied": False}
    if has_pending_allocation:
        return {"applied": False}
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value in {
        AllocationTransitionAction.OPEN.value,
        AllocationTransitionAction.FLIP_OPEN_SECOND.value,
        AllocationTransitionAction.BLOCK.value,
    }:
        return {"applied": False}
    allocation_side = _position_side_or_none(getattr(allocation, "position_side", None))
    if allocation_side is None or allocation_side != aggregate_side:
        return {"applied": False}
    if follower_state_at is None:
        return {"applied": False}
    if allocation_latest_reconcile_at is not None and follower_state_at < allocation_latest_reconcile_at:
        return {"applied": False}
    follower_qtys = follower_qty_by_side or _empty_position_side_qtys()
    allocation_qtys = allocation_qty_by_side or _empty_position_side_qtys()
    actual_qty = abs(Decimal(follower_qtys.get(aggregate_side, 0)))
    current_qty = abs(Decimal(getattr(allocation, "allocated_qty", 0) or 0))
    planned_qty = abs(Decimal(getattr(planning_allocation, "allocated_qty", 0) or 0))
    allocation_sum_qty = abs(Decimal(allocation_qtys.get(aggregate_side, 0)))
    if abs(allocation_sum_qty - planned_qty) > ALLOCATION_TRANSITION_TOLERANCE:
        return {"applied": False}
    if abs(actual_qty - current_qty) <= ALLOCATION_TRANSITION_TOLERANCE:
        return {"applied": False}
    if actual_qty > current_qty + ALLOCATION_TRANSITION_TOLERANCE:
        trusted_ceiling = _decimal_from_value(trusted_actual_qty_ceiling)
        if (
            not allow_actual_qty_increase
            or trusted_ceiling is None
            or actual_qty > abs(trusted_ceiling) + ALLOCATION_TRANSITION_TOLERANCE
        ):
            return {"applied": False, "manual_unmanaged_increase": True}
    opposite = _opposite_side(aggregate_side)
    if abs(Decimal(follower_qtys.get(opposite, 0))) > ALLOCATION_TRANSITION_TOLERANCE:
        return {"applied": False}
    if actual_qty <= ALLOCATION_TRANSITION_TOLERANCE:
        if not allow_flat_close:
            return {"applied": False, "flat_close_blocked": True}
        return {
            "applied": True,
            "closed": True,
            "actual_qty": Decimal("0"),
            "actual_notional": Decimal("0"),
        }
    if mark_price <= 0:
        return {"applied": False}
    return {
        "applied": True,
        "closed": False,
        "actual_qty": _q(actual_qty),
        "actual_notional": _q(actual_qty * mark_price),
    }


def _allocation_sync_in_post_fill_snapshot_lag_guard(
    *,
    allocation_reconcile_at: datetime | None,
    latest_fill_event_at: datetime | None,
    guard_seconds: float,
) -> bool:
    if guard_seconds <= 0:
        return False
    # A routine successful sync advances allocation.last_reconcile_at on every
    # poll. Using that heartbeat here delays every genuine manual decrease by
    # the full guard window. Only an actual bot fill can make a down-snapshot
    # temporarily stale, so the protection must be anchored to FILL_APPLIED.
    checkpoint = latest_fill_event_at
    if checkpoint is None:
        return False
    return datetime.now(timezone.utc) - checkpoint <= timedelta(seconds=guard_seconds)


def _unmanaged_follower_position_blocker(
    *,
    transition_plan: Any | None,
    unmanaged_qty_by_side: dict[PositionSide, Decimal],
    unmanaged_position_state_lag: bool,
    canonical_coin: str,
) -> str | None:
    if transition_plan is None or unmanaged_position_state_lag:
        return None
    # A same-side reduce/close remains allocation-bounded, so any excess
    # follower quantity is left untouched. This is also the safe path when a
    # clearinghouse snapshot races just behind an already-applied allocation
    # fill during a burst of reductions.
    if _unmanaged_follower_position_reduce_safe(
        transition_plan=transition_plan,
        unmanaged_qty_by_side=unmanaged_qty_by_side,
    ):
        return None
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value in {AllocationTransitionAction.NOOP.value, AllocationTransitionAction.BLOCK.value}:
        return None
    unmanaged = {
        side.value: qty
        for side, qty in unmanaged_qty_by_side.items()
        if qty > ALLOCATION_TRANSITION_TOLERANCE
    }
    if not unmanaged:
        return None
    details = ", ".join(f"{side} qty={qty}" for side, qty in sorted(unmanaged.items()))
    return (
        "UNMANAGED_FOLLOWER_POSITION: follower has unallocated/manual "
        f"{canonical_coin} position ({details}); all copy actions disabled until follower position is flat"
    )


def _economic_dust_reopen_follower_flat_blocker(
    *,
    economic_dust_reopen: bool,
    follower_qty_by_side: dict[PositionSide, Decimal] | None,
) -> str | None:
    """Require literal follower flatness before treating leader dust as flat."""
    if not economic_dust_reopen:
        return None
    if follower_qty_by_side is None:
        return (
            "ECONOMIC_DUST_REOPEN_REQUIRES_FOLLOWER_FLAT: follower position "
            "state is unavailable"
        )
    nonflat = {
        side.value: abs(Decimal(follower_qty_by_side.get(side, 0)))
        for side in (PositionSide.LONG, PositionSide.SHORT)
        if abs(Decimal(follower_qty_by_side.get(side, 0)))
        > ALLOCATION_TRANSITION_TOLERANCE
    }
    if not nonflat:
        return None
    details = ", ".join(
        f"{side} qty={qty}" for side, qty in sorted(nonflat.items())
    )
    return (
        "ECONOMIC_DUST_REOPEN_REQUIRES_FOLLOWER_FLAT: follower still has "
        f"position ({details}); dust increase cannot start a new lifecycle"
    )


def _expected_no_action_block(error_message: str | None) -> bool:
    """Classify intentional first-owner rejection separately from true failures."""
    message = str(error_message or "")
    return (
        message.startswith("MARKET_OWNER_BLOCKED:")
        or message.startswith(f"{MANUAL_MARKET_OWNER_BLOCKED}:")
        or message.startswith(MAX_POSITION_NOTIONAL_CAP_EXCEEDED)
    )


def _unmanaged_follower_position_reduce_safe(
    *,
    transition_plan: Any | None,
    unmanaged_qty_by_side: dict[PositionSide, Decimal],
) -> bool:
    if transition_plan is None:
        return False
    action = getattr(transition_plan, "action", None)
    action_value = action.value if hasattr(action, "value") else str(action or "").upper()
    if action_value not in {
        AllocationTransitionAction.REDUCE.value,
        AllocationTransitionAction.CLOSE.value,
        AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
    }:
        return False
    closing_side = getattr(transition_plan, "old_side", None)
    if closing_side is None:
        return False
    if isinstance(closing_side, str):
        closing_side = _position_side_or_none(closing_side)
    if closing_side is None:
        return False
    for side, qty in unmanaged_qty_by_side.items():
        if qty <= ALLOCATION_TRANSITION_TOLERANCE:
            continue
        if side != closing_side:
            return False
    return True


def _effective_aggregate_follower_qty_for_reduce_scope(
    *,
    reduce_only: bool,
    allocation_mismatch_state_lag: bool,
    aggregate_follower_qty: Decimal | None,
    allocation_sum_qty: Decimal | None,
) -> Decimal | None:
    if not reduce_only or not allocation_mismatch_state_lag or allocation_sum_qty is None:
        return aggregate_follower_qty
    if aggregate_follower_qty is None:
        return allocation_sum_qty
    return max(aggregate_follower_qty, allocation_sum_qty)


def _allocation_needs_manual_review(allocation: LeaderPositionAllocationRecord | None) -> bool:
    return allocation is not None and str(allocation.status or "").upper() == "NEEDS_MANUAL_REVIEW"


def _ignore_without_allocation_reason(
    allocation: LeaderPositionAllocationRecord | None,
    fill_implied_position: Any | None,
    *,
    allow_economic_dust_reopen: bool = False,
) -> str | None:
    if _allocation_lifecycle_active(allocation):
        return None
    if _fill_is_new_open_from_flat(fill_implied_position):
        return None
    if _fill_is_checkpoint_contiguous_flip_open(allocation, fill_implied_position):
        return None
    if allow_economic_dust_reopen:
        return None
    return "IGNORED_OLD_LIFECYCLE: no follower allocation exists; waiting for leader open from flat"


def _fill_is_checkpoint_contiguous_flip_open(
    allocation: LeaderPositionAllocationRecord | None,
    fill_implied_position: Any | None,
) -> bool:
    """Allow only the exact flip following a deliberately early follower close.

    A sub-minimum leader remainder may cause the follower allocation to close
    early.  If the leader's next causal fill crosses that exact checkpoint into
    the opposite side, the new leg is a real opening lifecycle.  Arbitrary old
    adds/reduces/closes/flips remain unable to claim a released market.
    """
    if allocation is None or fill_implied_position is None:
        return False
    if str(getattr(allocation, "status", "") or "").upper() != "CLOSED":
        return False
    allocation_qty = abs(Decimal(getattr(allocation, "allocated_qty", 0) or 0))
    if allocation_qty > ALLOCATION_TRANSITION_TOLERANCE:
        return False
    confidence = str(getattr(fill_implied_position, "confidence", "") or "").upper()
    if confidence not in {"HIGH", "MEDIUM"} or not bool(
        getattr(fill_implied_position, "is_flip", False)
    ):
        return False
    checkpoint = _decimal_from_value(getattr(allocation, "last_leader_position_size", None))
    start_position = _decimal_from_value(getattr(fill_implied_position, "start_position", None))
    side_after = getattr(fill_implied_position, "side_after", None)
    if (
        checkpoint is None
        or start_position is None
        or abs(checkpoint) <= ALLOCATION_TRANSITION_TOLERANCE
        or abs(start_position - checkpoint) > ALLOCATION_TRANSITION_TOLERANCE
        or side_after not in {PositionSide.LONG, PositionSide.SHORT}
    ):
        return False
    return (checkpoint > 0 and side_after == PositionSide.SHORT) or (
        checkpoint < 0 and side_after == PositionSide.LONG
    )


def _fill_is_new_open_from_flat(fill_implied_position: Any | None) -> bool:
    if fill_implied_position is None:
        return False
    confidence = str(getattr(fill_implied_position, "confidence", "") or "").upper()
    if confidence not in {"HIGH", "MEDIUM"}:
        return False
    start_position = getattr(fill_implied_position, "start_position", None)
    if start_position is None or abs(Decimal(start_position or 0)) > ALLOCATION_TRANSITION_TOLERANCE:
        return False
    return bool(getattr(fill_implied_position, "is_open", False))


def _fill_is_economic_dust_reopen(
    fill_implied_position: Any | None,
    *,
    reference_price: Decimal,
    min_order_value: Decimal,
) -> bool:
    """Recognize a same-side add after the leader is economically flat.

    The follower deliberately closes instead of retaining an untradeable
    residual below the venue minimum.  A later fill may therefore be an
    ``is_increase`` for the leader while it is a genuine ``OPEN`` for the flat
    follower.  Require the fill's authoritative startPosition to be below the
    minimum and the post-fill position to cross back to at least the minimum.
    """
    if fill_implied_position is None:
        return False
    confidence = str(
        getattr(fill_implied_position, "confidence", "") or ""
    ).upper()
    if confidence not in {"HIGH", "MEDIUM"}:
        return False
    if not bool(getattr(fill_implied_position, "is_increase", False)):
        return False
    if (
        bool(getattr(fill_implied_position, "is_open", False))
        or bool(getattr(fill_implied_position, "is_reduce", False))
        or bool(getattr(fill_implied_position, "is_close", False))
        or bool(getattr(fill_implied_position, "is_flip", False))
    ):
        return False
    price = abs(Decimal(reference_price or 0))
    minimum = abs(Decimal(min_order_value or 0))
    start_position = _decimal_from_value(
        getattr(fill_implied_position, "start_position", None)
    )
    signed_size_after = _decimal_from_value(
        getattr(fill_implied_position, "signed_size_after", None)
    )
    if (
        price <= ALLOCATION_TRANSITION_TOLERANCE
        or minimum <= ALLOCATION_TRANSITION_TOLERANCE
        or start_position is None
        or signed_size_after is None
        or abs(start_position) <= ALLOCATION_TRANSITION_TOLERANCE
        or abs(signed_size_after) <= abs(start_position)
        or (start_position > 0) != (signed_size_after > 0)
    ):
        return False
    start_notional = abs(start_position) * price
    post_notional = abs(signed_size_after) * price
    return (
        start_notional < minimum
        and post_notional >= minimum
    )


def _fill_is_minimum_residual_checkpoint_reopen(
    allocation: LeaderPositionAllocationRecord | None,
    fill_implied_position: Any | None,
) -> bool:
    """Match a same-side add to a persisted minimum-residual early close."""
    if allocation is None or fill_implied_position is None:
        return False
    if str(getattr(allocation, "status", "") or "").upper() != "CLOSED":
        return False
    if (
        str(getattr(allocation, "pending_reduce_reason", "") or "")
        != MINIMUM_RESIDUAL_ECONOMIC_FLAT_REASON
    ):
        return False
    if (
        abs(Decimal(getattr(allocation, "allocated_qty", 0) or 0))
        > ALLOCATION_TRANSITION_TOLERANCE
        or abs(Decimal(getattr(allocation, "allocated_notional", 0) or 0))
        > ALLOCATION_TRANSITION_TOLERANCE
    ):
        return False
    return _fill_matches_minimum_residual_checkpoint(
        allocation,
        fill_implied_position,
    )


def _fill_is_minimum_residual_pending_checkpoint_reopen(
    allocation: LeaderPositionAllocationRecord | None,
    fill_implied_position: Any | None,
) -> bool:
    """Recognize a dust reopen that arrived while its early close is pending."""
    if allocation is None or fill_implied_position is None:
        return False
    if str(getattr(allocation, "status", "") or "").upper() == "CLOSED":
        return False
    if (
        str(getattr(allocation, "pending_reduce_reason", "") or "")
        != MINIMUM_RESIDUAL_ECONOMIC_FLAT_PENDING_REASON
    ):
        return False
    if (
        abs(Decimal(getattr(allocation, "target_notional", 0) or 0))
        > ALLOCATION_TRANSITION_TOLERANCE
    ):
        return False
    return _fill_matches_minimum_residual_checkpoint(
        allocation,
        fill_implied_position,
    )


def _fill_matches_minimum_residual_checkpoint(
    allocation: LeaderPositionAllocationRecord,
    fill_implied_position: Any,
) -> bool:
    confidence = str(
        getattr(fill_implied_position, "confidence", "") or ""
    ).upper()
    if confidence not in {"HIGH", "MEDIUM"} or not bool(
        getattr(fill_implied_position, "is_increase", False)
    ):
        return False
    if (
        bool(getattr(fill_implied_position, "is_open", False))
        or bool(getattr(fill_implied_position, "is_reduce", False))
        or bool(getattr(fill_implied_position, "is_close", False))
        or bool(getattr(fill_implied_position, "is_flip", False))
    ):
        return False
    checkpoint_size = _decimal_from_value(
        getattr(allocation, "last_leader_position_size", None)
    )
    start_position = _decimal_from_value(
        getattr(fill_implied_position, "start_position", None)
    )
    signed_size_after = _decimal_from_value(
        getattr(fill_implied_position, "signed_size_after", None)
    )
    allocation_side = _position_side_or_none(
        getattr(allocation, "position_side", None)
    )
    if (
        checkpoint_size is None
        or start_position is None
        or signed_size_after is None
        or allocation_side is None
        or abs(checkpoint_size) <= ALLOCATION_TRANSITION_TOLERANCE
    ):
        return False
    signed_checkpoint = (
        abs(checkpoint_size)
        if allocation_side == PositionSide.LONG
        else -abs(checkpoint_size)
    )
    return bool(
        abs(start_position - signed_checkpoint)
        <= ALLOCATION_TRANSITION_TOLERANCE
        and (start_position > 0) == (signed_size_after > 0)
        and abs(signed_size_after)
        > abs(start_position) + ALLOCATION_TRANSITION_TOLERANCE
    )


def _age_ms(value: datetime | None) -> int:
    if value is None:
        return 10**12
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int((datetime.now(timezone.utc) - value).total_seconds() * 1000)


def _q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None and str(value) != "":
            return value
    return None


def _signed_envelope_hash(envelope: dict[str, Any]) -> str:
    canonical = json.dumps(
        _json_safe(envelope),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _event_tid(event: FillEvent) -> int | None:
    return _int_or_none((event.raw or {}).get("tid"))


def _fill_cursor_key(value: tuple[int, int | None]) -> tuple[int, int]:
    return int(value[0]), int(value[1]) if value[1] is not None else -1


def _causal_sort_fill_payloads(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort same-market fills by their startPosition chain, then by venue tid."""
    ordered = sorted(
        list(fills or []),
        key=lambda fill: (
            _int_or_none(fill.get("time")) or 0,
            str(fill.get("coin") or ""),
            _int_or_none(fill.get("tid")) or -1,
        ),
    )
    result: list[dict[str, Any]] = []
    index = 0
    while index < len(ordered):
        first = ordered[index]
        group_key = (
            _int_or_none(first.get("time")) or 0,
            str(first.get("coin") or ""),
        )
        end = index + 1
        while end < len(ordered):
            candidate = ordered[end]
            if (
                _int_or_none(candidate.get("time")) or 0,
                str(candidate.get("coin") or ""),
            ) != group_key:
                break
            end += 1
        result.extend(_causal_sort_same_time_market_group(ordered[index:end]))
        index = end
    return result


def _causal_sort_same_time_market_group(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(fills) < 2:
        return fills
    edges: list[tuple[dict[str, Any], Decimal, Decimal]] = []
    for fill in fills:
        start = _decimal_from_value(fill.get("startPosition"))
        size = _decimal_from_value(fill.get("sz"))
        side = str(fill.get("side") or "").upper()
        if start is None or size is None or size < 0 or side not in {"A", "B"}:
            return fills
        finish = start + size if side == "B" else start - size
        edges.append((fill, start, finish))
    finish_values = {finish for _fill, _start, finish in edges}
    roots = [edge for edge in edges if edge[1] not in finish_values]
    if len(roots) != 1:
        return fills
    chain: list[dict[str, Any]] = []
    remaining = list(edges)
    current = roots[0]
    while True:
        chain.append(current[0])
        remaining.remove(current)
        if not remaining:
            return chain
        next_edges = [edge for edge in remaining if edge[1] == current[2]]
        if len(next_edges) != 1:
            return fills
        current = next_edges[0]


def _writer_lease_key(follower_account: str) -> int:
    digest = hashlib.blake2b(
        f"copytrade-writer:{str(follower_account or '').lower()}".encode("utf-8"),
        digest_size=8,
    ).digest()
    value = int.from_bytes(digest, byteorder="big", signed=False)
    return value if value < 2**63 else value - 2**64


def _market_transaction_key(market: MarketKey, execution_scope: str = "") -> int:
    digest = hashlib.blake2b(
        f"copytrade-market:{str(execution_scope or '').lower()}:{market.dex.lower()}:{market.canonical_coin.upper()}".encode("utf-8"),
        digest_size=8,
    ).digest()
    value = int.from_bytes(digest, byteorder="big", signed=False)
    return value if value < 2**63 else value - 2**64


def _market_arrival_key(market: MarketKey, execution_scope: str = "") -> int:
    """Separate durable-arrival ordering from the longer planning transaction.

    The singleton writer plus the in-process ingress lock preserves enqueue
    order. This database namespace additionally preserves source_fills.id order
    across process handoff without making a newly received fill wait for an
    unrelated planner/submit transaction holding the market-state lock.
    """
    digest = hashlib.blake2b(
        f"copytrade-arrival:{str(execution_scope or '').lower()}:{market.dex.lower()}:{market.canonical_coin.upper()}".encode("utf-8"),
        digest_size=8,
    ).digest()
    value = int.from_bytes(digest, byteorder="big", signed=False)
    return value if value < 2**63 else value - 2**64


def _follower_market_guard_query(
    market: MarketKey,
    *,
    execution_scope: str = "",
) -> Any:
    return (
        select(FollowerMarketGuard)
        .where(FollowerMarketGuard.execution_account == str(execution_scope or "").lower())
        .where(FollowerMarketGuard.execution_venue == ExecutionVenue.HYPERLIQUID.value)
        .where(FollowerMarketGuard.dex == str(market.dex or "").lower())
        .where(func.upper(FollowerMarketGuard.canonical_coin) == market.canonical_coin.upper())
        .limit(1)
    )


def _planned_follower_market_position_version(order: ExecutionOrder) -> int | None:
    return _int_or_none(
        (order.pre_trade_checklist or {}).get("follower_market_position_version")
    )
