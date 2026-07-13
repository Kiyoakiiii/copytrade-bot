from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from types import SimpleNamespace
from typing import Any

import structlog
import websockets
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import Settings
from app.db.session import SessionLocal
from app.models import (
    AppSetting,
    AllocationEvent,
    ExecutionOrder,
    LatestAccountPosition,
    LatestAccountState,
    LeaderConfig,
    LeaderPositionAllocationRecord,
    LeaderPositionBaseline,
    MarketRiskSetting,
    RiskEvent,
    SourceFill,
)
from app.services.account_abstraction import (
    AccountAbstractionService,
    MODE_DEX_ABSTRACTION,
    SOURCE_ACCOUNT_TOTAL,
    SOURCE_CLEARINGHOUSE,
    account_abstraction_setting_key,
    available_collateral_sufficient,
    load_account_abstraction_state,
    resolved_value_payload,
    resolve_account_value_for_sizing,
    save_account_abstraction_state,
)
from app.services.account_state import FOLLOWER, LEADER, AccountStateService, save_account_state
from app.services.allocations import (
    ALLOCATION_TRANSITION_TOLERANCE,
    LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK,
    MAX_POSITION_NOTIONAL_CAP_APPLIED,
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
    ensure_hyperliquid_market_risk_settings,
)
from app.services.leader_config import active_leaders_statement, is_coin_allowed, normalize_leader_address
from app.services.order_policy import (
    AutoCopyOrderPolicyError,
    HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
    assert_hyperliquid_auto_copy_order,
)
from app.services.sizing_guard import SizingGuardError, assert_sizing_mode_account_ratio
from app.services.task_status import store_task_status
from app.services.target_position import PositionSide
from app.tasks.leader_state_poller import schedule_account_state_refresh_if_stale

log = structlog.get_logger(__name__)

PENDING_OPEN_STATUS = "PENDING_OPEN"
PENDING_OPEN_REASON = "BELOW_MIN_ORDER_VALUE_PENDING_OPEN"
ACCOUNT_VALUE_PENDING_OPEN_REASON = "ACCOUNT_VALUE_UNAVAILABLE_PENDING_OPEN"
_COALESCED_FILL_NOTIONAL_KEY = "_copytrade_coalesced_fill_notional"
_COALESCED_FILL_SIZE_KEY = "_copytrade_coalesced_fill_size"
_COALESCED_FILL_COUNT_KEY = "_copytrade_coalesced_fill_count"
_COALESCED_SOURCE_IDS_KEY = "_copytrade_coalesced_source_fill_ids"
_SNAPSHOT_RECOVERY_KEY = "_copytrade_snapshot_recovery"
_SNAPSHOT_RECOVERY_REASON_KEY = "_copytrade_snapshot_recovery_reason"
HYPERLIQUID_WS_APP_PING_SECONDS = 30.0
LEADER_FILL_PRICE_FALLBACK_SOURCE = "LEADER_FILL_PRICE_FALLBACK"
RECENT_CLOSED_ALLOCATION_STATE_LAG_WINDOW = timedelta(minutes=5)
FIXED_LEADER_ACCOUNT_VALUE_SOURCE = "LEADER_CONFIG_FIXED"
FIXED_LEADER_ACCOUNT_VALUE_MODE = "FIXED_REFERENCE"
LOCAL_POSITION_PROJECTION_SOURCES = {"LOCAL_FILL_PROJECTION", "ORDER_RECOVERY_PROJECTION"}
UNRESOLVED_SAME_MARKET_ORDER_BLOCKER = "unresolved UNKNOWN/PENDING auto order exists for this leader/market"
UNRESOLVED_SAME_MARKET_RETRY_ATTEMPTS = 3
UNRESOLVED_SAME_MARKET_RETRY_SLEEP_SECONDS = 0.05
TRANSIENT_UNRESOLVED_ORDER_STATUSES = {"PENDING_SUBMIT", "SUBMITTING"}


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

    @property
    def hyperliquid_event_time(self) -> datetime | None:
        if not self.time_ms:
            return None
        return datetime.fromtimestamp(self.time_ms / 1000, timezone.utc)


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

    def set_price(self, *, dex: str, coin: str, price: Decimal | str, source: str = "REST") -> None:
        parsed = parse_coin(coin, default_dex=dex)
        value = Decimal(str(price))
        if value <= 0:
            return
        self._prices[parsed.canonical_coin] = PriceEntry(
            price=value,
            updated_at=datetime.now(timezone.utc),
            source=source,
        )

    def update_mids(self, *, dex: str, mids: dict[str, Any], source: str, replace: bool = False) -> None:
        updated: set[str] = set()
        for raw_coin, value in mids.items():
            try:
                parsed = parse_coin(str(raw_coin), default_dex=dex)
                self.set_price(dex=dex, coin=parsed.canonical_coin, price=Decimal(str(value)), source=source)
                updated.add(parsed.canonical_coin)
            except Exception:
                continue
        if replace and updated:
            target_dex = str(dex or "").lower()
            for canonical in list(self._prices):
                if parse_coin(canonical).dex == target_dex and canonical not in updated:
                    self._prices.pop(canonical, None)

    def get(self, canonical: str) -> PriceEntry | None:
        return self._prices.get(parse_coin(canonical).canonical_coin)

    def fresh_mids_for_dex(self, dex: str, *, now: datetime | None = None) -> dict[str, Decimal]:
        now = now or datetime.now(timezone.utc)
        result: dict[str, Decimal] = {}
        target_dex = str(dex or "").lower()
        for canonical, entry in self._prices.items():
            parsed = parse_coin(canonical)
            if parsed.dex != target_dex:
                continue
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
            entries = [
                (coin, entry)
                for coin, entry in self._prices.items()
                if parse_coin(coin).dex == str(dex or "").lower()
            ]
            ages = [int((now - entry.updated_at).total_seconds() * 1000) for _, entry in entries]
            stale = [coin for coin, entry in entries if int((now - entry.updated_at).total_seconds() * 1000) > self.stale_ms]
            result[str(dex or "").lower()] = {
                "markets_count": len(entries),
                "fresh": bool(entries) and not stale,
                "stale_markets_count": len(stale),
                "last_price_update_age_ms": min(ages) if ages else None,
            }
        return result

    def snapshot(self, dexes: list[str]) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        allowed = {str(dex or "").lower() for dex in dexes}
        prices: dict[str, dict[str, Any]] = {}
        for canonical, entry in self._prices.items():
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

    def has_cloid(self, cloid: str | None) -> bool:
        return bool(cloid and str(cloid) in self._by_cloid)

    def has_pending_allocation(self, allocation: LeaderPositionAllocationRecord | None) -> bool:
        scope = self._scope_from_allocation(allocation)
        return bool(scope and self._by_scope.get(scope))

    def submit_barriers_before(self, order: ExecutionOrder | None) -> list[PendingIntent]:
        current = self.intent_for_order(order)
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

    def blocker_message(self) -> str:
        return (
            "MANUAL_FOLLOWER_POSITION_GUARD: follower has a manual/unallocated "
            f"{self.canonical_coin} position; copy actions are disabled until that follower position is flat"
        )


class FollowerManualPositionGuard:
    def __init__(self) -> None:
        self._by_market: dict[tuple[str, str], FollowerManualPositionGuardEntry] = {}

    def mark(self, market: MarketKey, *, reason: str, observed_at: datetime | None = None) -> None:
        key = self._key(market)
        self._by_market[key] = FollowerManualPositionGuardEntry(
            dex=key[0],
            canonical_coin=key[1],
            created_at=_datetime_or_none(observed_at) or datetime.now(timezone.utc),
            reason=reason[:500],
        )

    def active_entry(self, market: MarketKey) -> FollowerManualPositionGuardEntry | None:
        return self._by_market.get(self._key(market))

    def clear(self, market: MarketKey) -> None:
        self._by_market.pop(self._key(market), None)

    def entries(self) -> list[FollowerManualPositionGuardEntry]:
        return list(self._by_market.values())

    def reconcile(
        self,
        market: MarketKey,
        *,
        unmanaged_qty_by_side: dict[PositionSide, Decimal],
        follower_state_at: datetime | None,
    ) -> FollowerManualPositionGuardEntry | None:
        entry = self.active_entry(market)
        if entry is None:
            return None
        observed_state_at = _datetime_or_none(follower_state_at)
        if observed_state_at is None or observed_state_at < entry.created_at:
            return entry
        has_unmanaged = any(qty > ALLOCATION_TRANSITION_TOLERANCE for qty in unmanaged_qty_by_side.values())
        if has_unmanaged:
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
        execution_client: HyperliquidExecutionClient,
        price_cache: LowLatencyPriceCache,
        manual_position_guard: FollowerManualPositionGuard | None = None,
    ) -> None:
        self.settings = settings
        self.info_client = info_client
        self.execution_client = execution_client
        self.price_cache = price_cache
        self.manual_position_guard = manual_position_guard
        self.market_leverage_plan_cache: dict[tuple[str, str], Any] = {}
        self._asset_id_cache: dict[tuple[str, str], int] = {}
        self._market_meta_cache: dict[str, tuple[datetime, dict[str, Any]]] = {}
        self._risk_settings_ok_cache: dict[tuple[str, str, int | None, str], RiskSettingResult] = {}
        self._risk_settings_locks: dict[tuple[str, str, int | None], asyncio.Lock] = {}
        self._account_abstraction_cache: dict[str, tuple[datetime, dict[str, Any]]] = {}
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
        for row in rows:
            result = _risk_setting_result_from_row(row)
            self._risk_settings_ok_cache[
                (
                    str(row.dex or "").lower(),
                    str(row.canonical_coin or "").upper(),
                    row.effective_leverage,
                    row.desired_margin_mode or DESIRED_MARGIN_MODE,
                )
            ] = result
            if row.actual_margin_mode and row.actual_margin_mode != row.desired_margin_mode:
                self._risk_settings_ok_cache[
                    (
                        str(row.dex or "").lower(),
                        str(row.canonical_coin or "").upper(),
                        row.effective_leverage,
                        row.actual_margin_mode,
                    )
                ] = result
        return len(rows)

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
        inserted = await self._record_source_fill(db, fill, processed=True)
        dedupe_done_at = datetime.now(timezone.utc)
        if not inserted:
            return None
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

        if not is_coin_allowed(leader, fill.market.canonical_coin):
            blockers.append("coin not allowed for leader")
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
                blockers.append("could not resolve Hyperliquid asset id for fill market")

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
            blockers.append("price cache stale or missing for fill market")

        blocking_unresolved = await self._blocking_unresolved_same_market_orders(
            db,
            leader_address=leader.leader_address,
            market=fill.market,
        )
        if blocking_unresolved:
            blockers.append(UNRESOLVED_SAME_MARKET_ORDER_BLOCKER)

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
        market_owner_allocation = await self._load_market_owner_allocation(db, fill.market)
        market_owner_blocker = _market_owner_blocker(
            market_owner_allocation,
            leader=leader,
            current_allocation=planning_allocation,
        )
        if market_owner_blocker:
            blockers.append(market_owner_blocker)
        lifecycle_ignore_reason = _ignore_without_allocation_reason(planning_allocation, fill_implied_position)
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
        sizing_done_at = datetime.now(timezone.utc)

        if transition_plan is not None and transition_plan.action == AllocationTransitionAction.BLOCK:
            blockers.append(transition_plan.reason)
        if _transition_requires_account_value(transition_plan):
            blockers.extend(account_value_blockers)
        plan_warnings = _allocation_plan_warnings(transition_plan)
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
        allocation_mismatch = False
        allocation_mismatch_state_lag = False
        unmanaged_follower_position = False
        unmanaged_follower_position_state_lag = False
        unmanaged_follower_position_reduce_safe = False
        unmanaged_follower_position_qty_by_side: dict[str, str] = {}
        manual_follower_position_guard = False
        manual_follower_position_guard_reason: str | None = None
        manual_follower_position_guard_created_at: datetime | None = None
        if aggregate_side != PositionSide.FLAT:
            allocation_qty_by_side, allocation_latest_reconcile_at = await self._allocation_sum_qtys_with_latest_reconcile(
                db,
                fill.market,
            )
            allocation_qty_by_side = self.pending_intents.effective_qtys(
                dex=fill.market.dex,
                canonical_coin=fill.market.canonical_coin,
                base_qtys=allocation_qty_by_side,
            )
            allocation_sum_qty = allocation_qty_by_side.get(aggregate_side, Decimal("0"))
            if (
                self.manual_position_guard is not None
                and transition_plan is not None
                and transition_plan.action not in {AllocationTransitionAction.NOOP, AllocationTransitionAction.BLOCK}
            ):
                guard_entry = self.manual_position_guard.active_entry(fill.market)
                if guard_entry is not None and not _allocation_lifecycle_active(planning_allocation):
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
            if await self._opposite_aggregate_allocation_exists(db, leader, fill.market, transition_plan.new_side):
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
        leverage_plan = await self._load_market_leverage_plan(db, fill.market, leader_position)
        effective_leverage = leverage_plan.effective_leverage or self.settings.hyperliquid_default_leverage
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

        kill_switch_active = await self._kill_switch_active(db)
        if (
            kill_switch_active
            and not reduce_only
            and target_delta_abs > ALLOCATION_TRANSITION_TOLERANCE
            and transition_plan is not None
            and transition_plan.action not in {AllocationTransitionAction.NOOP, AllocationTransitionAction.BLOCK}
        ):
            blockers.append("kill switch active")

        sizing_guard_error: str | None = None
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
        validator_result, final_close_min_order_override = _override_final_close_min_order_validator(
            reduce_only=reduce_only,
            transition_plan=transition_plan,
            validator_result=validator_result,
        )
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
                "leader_enabled": leader.enabled and leader.deleted_at is None,
                "coin_allowed": is_coin_allowed(leader, fill.market.canonical_coin),
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
                "follower_account_state_fresh": None,
                "follower_account_state_hot_path": False,
                "effective_leverage": effective_leverage,
                "market_max_leverage": leverage_plan.max_leverage,
                "market_sz_decimals": leverage_plan.sz_decimals,
                "market_asset_id_from_meta": leverage_plan.asset_id,
                "effective_leverage_confirmed": leverage_plan.ok_for_open,
                "leader_margin_mode_observed": leader_margin_mode_observed,
                "follower_margin_mode_required": DESIRED_MARGIN_MODE,
                "follower_margin_mode_confirmed": False,
                "follower_isolated_required": DESIRED_MARGIN_MODE == "ISOLATED",
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
                "pending_open": pending_open,
                "pending_open_reason": pending_open_reason,
                "pending_open_activation_blocked": bool(pending_open_activation_reason),
                "pending_open_activation_reason": pending_open_activation_reason,
                "missed_reduce_catchup": missed_reduce_catchup,
                "fill_direction_guard_reason": fill_direction_guard_reason,
                "blocked_order_preserves_allocation_state": blocked_order_preserves_allocation,
                "final_close_min_order_override": final_close_min_order_override,
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
                "error_code": _validator_error_code(validator_result),
                "order_validator": validator_result.to_dict(),
                "blockers": blockers,
            },
        )
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
        _set_latency_fields(order)
        db.add(order)
        await db.flush()
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

        if current_allocation is not None and transition_plan is not None:
            _record_allocation_event(
                db,
                allocation=current_allocation,
                order=order,
                action="DIRECTION_GUARD_BLOCKED"
                if direction_guard_preserve_allocation
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
        await db.commit()
        if status == "PENDING_SUBMIT" and submit_order:
            self._release_pending_intent_if_resolved(order)
        return order

    async def submit_planned_order(self, db: Any, order: ExecutionOrder, fill: FillEvent) -> None:
        status = str(order.status or "").upper()
        if status == "PENDING_SUBMIT":
            claimed = await self._claim_order_for_submit(db, order)
            if not claimed:
                return
        elif status == "SUBMITTING":
            return
        else:
            self.pending_intents.release(order)
            return
        await self._submit_hyperliquid_order(db, order, fill, reduce_only=bool(order.reduce_only))
        await self._apply_allocation_fill(db, order)
        await db.commit()
        self._release_pending_intent_if_resolved(order)

    async def _claim_order_for_submit(self, db: Any, order: ExecutionOrder) -> bool:
        if str(order.status or "").upper() != "PENDING_SUBMIT":
            return False
        claim_time = datetime.now(timezone.utc)
        if order.id is None or not isinstance(db, AsyncSession):
            order.status = "SUBMITTING"
            order.updated_at = claim_time
            if hasattr(db, "commit"):
                await db.commit()
            return True
        result = await db.execute(
            update(ExecutionOrder)
            .where(ExecutionOrder.id == order.id)
            .where(ExecutionOrder.status == "PENDING_SUBMIT")
            .values(status="SUBMITTING", updated_at=claim_time)
            .returning(ExecutionOrder.id)
        )
        claimed_id = result.scalar_one_or_none()
        if claimed_id is None:
            return False
        order.status = "SUBMITTING"
        order.updated_at = claim_time
        await db.commit()
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
        desired_leverage = await self._market_default_leverage(db, fill.market)
        market_max_leverage = _int_or_none(checklist.get("market_max_leverage"))
        effective_leverage = (
            _int_or_none(checklist.get("effective_leverage"))
            or (min(desired_leverage, market_max_leverage) if market_max_leverage is not None else None)
            or desired_leverage
        )
        account_address = self.settings.hyperliquid_follower_account_address() or ""
        if reduce_only:
            return (
                RiskSettingResult(
                    is_ok=True,
                    status="SKIPPED_REDUCE_ONLY_FAST_PATH",
                    account_address=account_address,
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

        cache_keys = [
            (
                str(fill.market.dex or "").lower(),
                str(fill.market.canonical_coin or "").upper(),
                effective_leverage,
                DESIRED_MARGIN_MODE,
            ),
            (
                str(fill.market.dex or "").lower(),
                str(fill.market.canonical_coin or "").upper(),
                effective_leverage,
                FALLBACK_MARGIN_MODE,
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
                market_max_leverage=checklist.get("market_max_leverage"),
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
    ) -> None:
        already_submitted = (
            order.order_submit_started_at is not None
            or order.order_submit_done_at is not None
            or order.order_ack_at is not None
            or bool(order.order_id)
            or bool(order.venue_order_id)
            or bool(order.raw_response)
        )
        if already_submitted or (isinstance(db, AsyncSession) and str(order.status or "").upper() != "SUBMITTING"):
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
            _set_latency_fields(order)
            return
        if not order.cloid or not order.estimated_price or order.quantity <= 0:
            order.status = "BLOCKED"
            order.dry_run = True
            order.error_message = "invalid order payload"
            await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message)
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
            return
        if await self._guard_reduce_submit_against_live_allocation(db, order, fill):
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
            exchange_submit_started_at = datetime.now(timezone.utc)
            await self._persist_submit_started_marker(db, order, exchange_submit_started_at)
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
                            message=f"IOC resting/open cancel failed: {exc}",
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
        except Exception as exc:
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
                order.error_message = f"Hyperliquid order not submitted: {exc}"
                await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message)
            else:
                order.status = "UNKNOWN"
                order.error_message = f"Hyperliquid order status unknown: {exc}"
            order.dry_run = False
        if order.status in {"REJECTED", "CANCELED", "EXPIRED"} and not order.executed_qty:
            await self._close_allocation_after_absent_reduce_rejection(db, order, fill)
            await self._close_zero_allocation_after_unsubmitted_open(db, order, fill, reason=order.error_message or order.status)
        _set_latency_fields(order)

    async def _persist_submit_started_marker(
        self,
        db: Any,
        order: ExecutionOrder,
        started_at: datetime,
    ) -> None:
        if isinstance(db, AsyncSession) and order.id is not None:
            result = await db.execute(
                update(ExecutionOrder)
                .where(ExecutionOrder.id == order.id)
                .where(ExecutionOrder.status == "SUBMITTING")
                .where(ExecutionOrder.order_submit_started_at.is_(None))
                .values(
                    order_submit_started_at=started_at,
                    binance_order_submit_at=started_at,
                    request_payload_masked=order.request_payload_masked,
                    pre_trade_checklist=order.pre_trade_checklist,
                    updated_at=started_at,
                )
                .returning(ExecutionOrder.id)
            )
            if result.scalar_one_or_none() is None:
                raise AutoCopyOrderPolicyError("INTERNAL_SUBMIT_GUARD: submit-start marker was not claimable")
            await db.commit()
        order.order_submit_started_at = started_at
        order.binance_order_submit_at = started_at
        _trace_set(order, "order_submit_started_at", started_at)
        _trace_set(order, "exchange_submit_call_started_at", started_at)

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
            "reduce-only order rejected because follower position was already absent"
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
            and not _order_market_owner_guard_allows_manual_guard(order)
        ):
            blockers.append("INTERNAL_SUBMIT_GUARD: manual follower position guard active")
        if action in {
            AllocationTransitionAction.REDUCE.value,
            AllocationTransitionAction.CLOSE.value,
            AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
        }:
            if order.allocation_id is None:
                blockers.append("INTERNAL_SUBMIT_GUARD: reduce/close requires allocation_id")
            if not checklist.get("allocation_scope_guard"):
                blockers.append("INTERNAL_SUBMIT_GUARD: reduce/close missing allocation scope guard")
            if checklist.get("allocation_mismatch") and not checklist.get("allocation_mismatch_state_lag"):
                blockers.append("INTERNAL_SUBMIT_GUARD: allocation mismatch present")
            if (
                checklist.get("unmanaged_follower_position")
                and not checklist.get("unmanaged_follower_position_state_lag")
            ):
                blockers.append("INTERNAL_SUBMIT_GUARD: unmanaged follower position present")
        elif action in {
            AllocationTransitionAction.OPEN.value,
            AllocationTransitionAction.INCREASE.value,
            AllocationTransitionAction.FLIP_OPEN_SECOND.value,
        }:
            if order.allocation_id is None:
                blockers.append("INTERNAL_SUBMIT_GUARD: open/increase requires allocation_id")
            if checklist.get("allocation_mismatch"):
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
            metadata={"order_action": order.order_action},
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
        stmt = (
            insert(SourceFill)
            .values(
                source_fill_id=fill.source_fill_id,
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
                processed_at=datetime.now(timezone.utc) if processed else None,
            )
            .on_conflict_do_nothing(index_elements=[SourceFill.source_fill_id])
            .returning(SourceFill.id)
        )
        result = await db.execute(stmt)
        inserted_id = result.scalar_one_or_none()
        if inserted_id is not None:
            await db.flush()
            return True
        if not processed:
            await db.flush()
            return False
        now = datetime.now(timezone.utc)
        promote_stmt = (
            update(SourceFill)
            .where(SourceFill.source_fill_id == fill.source_fill_id)
            .where(SourceFill.processed_at.is_(None))
            .values(
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
                processed_at=now,
                updated_at=now,
            )
            .returning(SourceFill.id)
        )
        result = await db.execute(promote_stmt)
        await db.flush()
        return result.scalar_one_or_none() is not None

    async def warm_market_meta_cache(self, dexes: list[str] | tuple[str, ...] | set[str]) -> None:
        results = await asyncio.gather(
            *(self._get_market_meta(dex) for dex in dexes),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                log.warning("market_meta_warmup_failed", error=str(result)[:160])

    async def _get_market_meta(self, dex: str, *, force_refresh: bool = False) -> dict[str, Any]:
        dex_key = str(dex or "").lower()
        now = datetime.now(timezone.utc)
        ttl_seconds = max(int(getattr(self.settings, "hyperliquid_risk_settings_ttl_seconds", 300) or 300), 30)
        cached = self._market_meta_cache.get(dex_key)
        if cached is not None and not force_refresh:
            cached_at, meta = cached
            if cached_at >= now - timedelta(seconds=ttl_seconds):
                return meta
        try:
            meta = await self.info_client.meta(dex_key)
        except TypeError:
            meta = await self.info_client.meta()
        meta = dict(meta or {})
        self._market_meta_cache[dex_key] = (now, meta)
        self._prime_market_plan_cache_from_meta(dex_key, meta)
        return meta

    def _prime_market_plan_cache_from_meta(self, dex: str, meta: dict[str, Any]) -> None:
        for index, item in enumerate(meta.get("universe", []) or []):
            if not isinstance(item, dict):
                continue
            parsed = parse_coin(str(item.get("name", "")), default_dex=dex)
            if not parsed.coin:
                continue
            market_meta = {**item, "asset_id": index, "index": index}
            plan = build_hyperliquid_leverage_plan(
                default_leverage=self.settings.hyperliquid_default_leverage,
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
        except Exception:
            asset_id = None
        if asset_id is not None:
            self._asset_id_cache[(str(fill.market.dex or "").lower(), str(fill.market.canonical_coin or "").upper())] = asset_id
        return self._fill_with_asset_id(fill, asset_id)

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
        return FillEvent(
            source_fill_id=fill.source_fill_id,
            leader_address=fill.leader_address,
            market=market,
            side=fill.side,
            price=fill.price,
            size=fill.size,
            time_ms=fill.time_ms,
            raw=fill.raw,
            is_snapshot=fill.is_snapshot,
            ws_received_at=fill.ws_received_at,
            parse_started_at=fill.parse_started_at,
            parse_done_at=fill.parse_done_at,
        )

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
                error=str(exc)[:160],
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
    ) -> Any:
        raw = getattr(leader_position, "raw_payload_masked", None) or {}
        raw_max = raw.get("maxLeverage") if isinstance(raw, dict) else None
        raw_sz_decimals = raw.get("szDecimals") if isinstance(raw, dict) else None
        cache_key = (str(market.dex or "").lower(), str(market.canonical_coin or "").upper())
        default_leverage = await self._market_default_leverage(db, market)
        cached = self.market_leverage_plan_cache.get(cache_key)
        if (
            cached is not None
            and cached.max_leverage is not None
            and cached.sz_decimals is not None
            and cached.effective_leverage == min(default_leverage, cached.max_leverage)
        ):
            return cached
        plan = build_hyperliquid_leverage_plan(
            default_leverage=default_leverage,
            coin_max_leverage=raw_max,
            sz_decimals=raw_sz_decimals,
            asset_id=market.asset_id,
            market_meta={**raw, "asset_id": market.asset_id, "index": market.asset_id}
            if raw_sz_decimals is not None and market.asset_id is not None
            else None,
        )
        if plan.max_leverage is not None and plan.sz_decimals is not None:
            self.market_leverage_plan_cache[cache_key] = plan
            return plan
        try:
            meta = await self._get_market_meta(market.dex)
        except Exception:
            return plan
        target = parse_coin(market.canonical_coin, default_dex=market.dex)
        for index, item in enumerate(meta.get("universe", []) or []):
            parsed = parse_coin(str(item.get("name", "")), default_dex=market.dex)
            if parsed.canonical_coin == target.canonical_coin:
                market_meta = {**item, "asset_id": index, "index": index}
                meta_plan = build_hyperliquid_leverage_plan(
                    default_leverage=default_leverage,
                    coin_max_leverage=item.get("maxLeverage"),
                    sz_decimals=item.get("szDecimals"),
                    asset_id=index,
                    market_meta=market_meta,
                )
                self.market_leverage_plan_cache[cache_key] = meta_plan
                return meta_plan
        if plan.max_leverage is not None:
            self.market_leverage_plan_cache[cache_key] = plan
        return plan

    async def _market_default_leverage(self, db: Any, market: MarketKey) -> int:
        configured_default = int(self.settings.hyperliquid_default_leverage or 10)
        account = self.settings.hyperliquid_follower_account_address()
        if not account:
            return configured_default
        row = await db.scalar(
            select(MarketRiskSetting)
            .where(MarketRiskSetting.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(MarketRiskSetting.account_address == account.lower())
            .where(MarketRiskSetting.dex == str(market.dex or "").lower())
            .where(func.upper(MarketRiskSetting.canonical_coin) == str(market.canonical_coin or "").upper())
            .where(MarketRiskSetting.status == STATUS_CONFIRMED)
            .order_by(MarketRiskSetting.last_confirmed_at.desc().nulls_last(), MarketRiskSetting.updated_at.desc())
            .limit(1)
        )
        if row is None:
            return configured_default
        for value in (row.effective_leverage, row.actual_leverage, row.desired_leverage):
            parsed = _int_or_none(value)
            if parsed is not None and parsed > 0:
                return parsed
        return configured_default

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
                error=str(exc)[:160],
            )

    async def _refresh_account_abstraction(self, db: Any, role: str, address: str, *, extra_dex: str | None = None) -> None:
        dexes = [dex.dex_name for dex in HyperliquidDexRegistry(self.settings).enabled_dexes()]
        normalized_extra = str(extra_dex or "").lower()
        if normalized_extra not in dexes:
            dexes.append(normalized_extra)
        snapshot = await AccountAbstractionService(self.info_client, self.settings).fetch_snapshot(
            role=role,
            address=address,
            dexes=dexes,
        )
        resolved_by_dex = {
            dex: resolve_account_value_for_sizing(snapshot, dex, self.settings)
            for dex in dexes
        }
        payload = await save_account_abstraction_state(
            db,
            snapshot=snapshot,
            resolved_by_dex=resolved_by_dex,
        )
        self._cache_account_abstraction_payload(snapshot.role, snapshot.address, payload)

    def _account_abstraction_cache_ttl(self) -> timedelta:
        seconds = max(1, min(int(getattr(self.settings, "account_state_stale_seconds", 10) or 10), 5))
        return timedelta(seconds=seconds)

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
        cached_at, payload = cached
        current = now or datetime.now(timezone.utc)
        if cached_at < current - self._account_abstraction_cache_ttl():
            return None
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
            value = resolved_value_payload(payload, dex)
            if value is None:
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
        result = resolved_value_payload(payload, dex)
        if result is None:
            self._schedule_state_refresh_if_stale()
            self._schedule_account_abstraction_refresh(role=role, address=address, dex=dex)
            return None
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
        return None

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
    follower_order_updates_subscribed: bool = False
    follower_user_events_subscribed: bool = False
    follower_user_fills_subscribed: bool = False
    follower_clearinghouse_subscribed: bool = False
    leader_user_fills_subscribed_count: int = 0
    last_ws_event_at: datetime | None = None
    last_allocation_sync_at: datetime | None = None
    allocation_sync_count: int = 0
    allocation_sync_skipped_count: int = 0
    allocation_sync_last_error: str | None = None
    reconnect_count: int = 0
    last_error: str | None = None


class HyperliquidLowLatencyWatcher:
    def __init__(
        self,
        *,
        settings: Settings,
        info_client: HyperliquidInfoClient,
        execution_client: HyperliquidExecutionClient,
        db_session_factory: Any,
        price_cache: LowLatencyPriceCache | None = None,
    ) -> None:
        self.settings = settings
        self.info_client = info_client
        self.execution_client = execution_client
        self.db_session_factory = db_session_factory
        self.price_cache = price_cache or LowLatencyPriceCache(stale_ms=settings.price_cache_stale_ms)
        self.manual_position_guard = FollowerManualPositionGuard()
        self.engine = FillDrivenExecutionEngine(
            settings=settings,
            info_client=info_client,
            execution_client=execution_client,
            price_cache=self.price_cache,
            manual_position_guard=self.manual_position_guard,
        )
        self.state = LowLatencyRuntimeState()
        self._stopped = asyncio.Event()
        self._leader_lock = asyncio.Lock()
        self._subscribed: set[str] = set()
        self._leader_fill_subscription_event = asyncio.Event()
        self._leader_fill_backfill_start_ms: int | None = None
        self._leader_fill_startup_backfill_done = False
        self._fill_queue_guard = asyncio.Lock()
        self._fill_queues: dict[tuple[str, str, str], asyncio.Queue[tuple[FillEvent, LeaderConfig]]] = {}
        self._fill_workers: dict[tuple[str, str, str], asyncio.Task] = {}
        self._submit_queue_guard = asyncio.Lock()
        self._submit_queues: dict[str, asyncio.Queue[tuple[int, FillEvent]]] = {}
        self._submit_workers: dict[str, asyncio.Task] = {}
        self._submit_retry_counts: dict[int, int] = {}
        self._suppressed_source_fill_guard = asyncio.Lock()
        self._suppressed_source_fill_ids: dict[str, datetime] = {}
        self._background_tasks: set[asyncio.Task] = set()
        self._follower_state_refresh_pending_dexes: set[str] = set()
        self._follower_state_refresh_task: asyncio.Task | None = None

    async def stop(self) -> None:
        self._stopped.set()

    async def run(self) -> None:
        await self._store_starting_status()
        await self.refresh_leaders()
        await self._refresh_active_follower_positions_once(reason="STARTUP_ACTIVE_REFRESH")
        self._schedule_background_task(self._warm_latency_caches())
        tasks = [
            asyncio.create_task(self._leader_fill_ws_loop()),
            asyncio.create_task(self._ws_loop()),
            asyncio.create_task(self._leader_refresh_loop()),
            asyncio.create_task(self._price_poll_loop()),
            asyncio.create_task(self._active_follower_position_refresh_loop()),
            asyncio.create_task(self._allocation_sync_loop()),
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

    async def _warm_latency_caches(self) -> None:
        dexes = [dex.dex_name for dex in HyperliquidDexRegistry(self.settings).enabled_dexes()]
        try:
            warmed = self.execution_client.warm_exchanges(dexes)
            if warmed:
                log.info("hyperliquid_exchange_cache_warmed", dexes=warmed)
        except Exception as exc:
            self.state.last_error = f"exchange warmup: {str(exc)[:160]}"
        await self.engine.warm_market_meta_cache(dexes)
        try:
            async with self.db_session_factory() as db:
                warmed_accounts = await self.engine.warm_account_abstraction_cache(
                    db,
                    leader_addresses=list(self.state.active_leaders),
                )
                warmed_risk = await self.engine.warm_risk_settings_cache(db)
                await db.commit()
            if warmed_accounts:
                log.info("hyperliquid_account_abstraction_cache_warmed", accounts=warmed_accounts)
            if warmed_risk:
                log.info("hyperliquid_risk_settings_cache_warmed", markets=warmed_risk)
        except Exception as exc:
            self.state.last_error = f"latency cache warmup: {str(exc)[:160]}"

    async def refresh_leaders(self) -> None:
        async with self.db_session_factory() as db:
            leaders = (await db.execute(active_leaders_statement())).scalars().all()
        active = {
            normalize_leader_address(leader.leader_address): leader for leader in leaders
        }
        ws_leaders, poll_fallback = self._partition_ws_leaders(list(active))
        async with self._leader_lock:
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
            self._update_websocket_connected_state()
        if changed:
            self._leader_fill_subscription_event.set()

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
                self.state.last_error = str(exc)[:200]
            await asyncio.sleep(float(self.settings.low_latency_leader_refresh_seconds))

    async def _price_poll_loop(self) -> None:
        while not self._stopped.is_set():
            dexes = HyperliquidDexRegistry(self.settings).enabled_dexes()
            price_status = self.price_cache.status_by_dex([dex.dex_name for dex in dexes])
            had_error = False
            for dex in dexes:
                status = price_status.get(str(dex.dex_name or "").lower()) or {}
                if self.state.websocket_connected and status.get("fresh"):
                    continue
                try:
                    mids = await self.info_client.all_mids(dex.dex_name)
                    self.price_cache.update_mids(dex=dex.dex_name, mids=mids, source="REST_POLL_FALLBACK", replace=True)
                except Exception as exc:
                    had_error = True
                    self.state.last_error = f"price cache {dex.dex_name or 'default'}: {str(exc)[:160]}"
            if not had_error and str(self.state.last_error or "").startswith("price cache "):
                self.state.last_error = None
            await asyncio.sleep(float(self.settings.price_cache_poll_seconds))

    async def _allocation_sync_loop(self) -> None:
        while not self._stopped.is_set():
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
                self.state.allocation_sync_last_error = str(exc)[:200]
                self.state.last_error = f"allocation sync: {str(exc)[:160]}"
                log.warning("allocation_sync_failed", error=str(exc))
            await asyncio.sleep(float(self.settings.allocation_sync_poll_seconds))

    async def _active_follower_position_refresh_loop(self) -> None:
        interval = float(getattr(self.settings, "follower_active_position_refresh_seconds", 1.0) or 0)
        if interval <= 0:
            return
        while not self._stopped.is_set():
            started = asyncio.get_running_loop().time()
            try:
                dexes = await self._active_allocation_dexes()
                if dexes:
                    self._schedule_follower_state_refresh(dexes, reason="ACTIVE_ALLOCATION_REFRESH")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = f"active follower position refresh: {str(exc)[:160]}"
                log.warning("active_follower_position_refresh_failed", error=str(exc))
            elapsed = asyncio.get_running_loop().time() - started
            await asyncio.sleep(max(0.05, interval - elapsed))

    async def _refresh_active_follower_positions_once(self, *, reason: str) -> int:
        dexes = await self._active_allocation_dexes()
        if not dexes:
            return 0
        return await self._refresh_follower_positions_for_dexes(dexes, source=reason)

    async def _active_allocation_dexes(self) -> list[str]:
        async with self.db_session_factory() as db:
            rows = (
                await db.execute(
                    select(LeaderPositionAllocationRecord.dex)
                    .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
                    .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                    .where(LeaderPositionAllocationRecord.status != "CLOSED")
                    .where(LeaderPositionAllocationRecord.allocated_qty > Decimal("0"))
                    .where(LeaderConfig.enabled.is_(True))
                    .where(LeaderConfig.deleted_at.is_(None))
                    .distinct()
                )
            ).scalars().all()
        return sorted({str(row or "").lower() for row in rows})

    async def _refresh_follower_positions_for_dexes(self, dexes: list[str], *, source: str) -> int:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return 0
        service = AccountStateService(self.info_client)
        refreshed = 0
        async with self.db_session_factory() as db:
            for dex in dexes:
                dex_name = str(dex or "").lower()
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
                except Exception as exc:
                    self.state.last_error = f"follower position refresh {dex_name or 'default'}: {str(exc)[:160]}"
                    log.warning(
                        "follower_position_refresh_failed",
                        dex=dex_name,
                        reason=source,
                        error=str(exc),
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
                        if _allocation_leader_snapshot_nonflat(allocation):
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
            if self.engine.pending_intents.has_pending_allocation(allocation):
                skipped += 1
                continue
            allocation_reconcile_at = _datetime_or_none(getattr(allocation, "last_reconcile_at", None))
            if allocation_reconcile_at is not None and state_at < allocation_reconcile_at:
                skipped += 1
                continue
            current_allocation_qty = abs(Decimal(allocation.allocated_qty or 0))
            latest_fill_event: Any | None = None
            if abs(actual_qty - current_allocation_qty) > ALLOCATION_TRANSITION_TOLERANCE:
                latest_fill_event = await self._latest_allocation_fill_applied_event(db, allocation)
                if (
                    actual_qty < current_allocation_qty - ALLOCATION_TRANSITION_TOLERANCE
                    and _allocation_sync_in_post_fill_snapshot_lag_guard(
                        allocation_reconcile_at=allocation_reconcile_at,
                        latest_fill_event_at=_datetime_or_none(getattr(latest_fill_event, "created_at", None)),
                        guard_seconds=float(self.settings.allocation_post_fill_snapshot_lag_guard_seconds),
                    )
                ):
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
            parsed_coin = parse_coin(canonical_coin, default_dex=dex)
            market = MarketKey(
                dex=dex,
                coin=parsed_coin.coin,
                canonical_coin=canonical_coin,
                raw_coin=canonical_coin,
                asset_id=None,
                venue_symbol=canonical_coin,
            )
            sync = _manual_same_side_position_sync(
                allocation=allocation,
                planning_allocation=allocation,
                transition_plan=SimpleNamespace(action=AllocationTransitionAction.INCREASE),
                aggregate_side=side,
                follower_qty_by_side={
                    PositionSide.LONG: _actual_scope_qty(actual_positions_by_scope, dex, canonical_coin, PositionSide.LONG),
                    PositionSide.SHORT: _actual_scope_qty(actual_positions_by_scope, dex, canonical_coin, PositionSide.SHORT),
                },
                allocation_qty_by_side={side: abs(Decimal(allocation.allocated_qty or 0))},
                follower_state_at=state_at,
                allocation_latest_reconcile_at=allocation_reconcile_at,
                has_pending_allocation=False,
                mark_price=mark_price,
                allow_actual_qty_increase=self.manual_position_guard.active_entry(market) is None,
                trusted_actual_qty_ceiling=_decimal_from_value(getattr(latest_fill_event, "after_qty", None)),
            )
            if not sync.get("applied"):
                if abs(actual_qty - current_allocation_qty) <= ALLOCATION_TRANSITION_TOLERANCE:
                    allocation.last_reconcile_at = state_at
                    if hasattr(allocation, "updated_at"):
                        allocation.updated_at = datetime.now(timezone.utc)
                    synced += 1
                continue
            before_qty = Decimal(allocation.allocated_qty or 0)
            before_notional = Decimal(allocation.allocated_notional or 0)
            if sync.get("closed"):
                allocation.allocated_qty = Decimal("0")
                allocation.allocated_notional = Decimal("0")
                allocation.target_notional = Decimal("0")
                allocation.status = "CLOSED"
                if _allocation_leader_snapshot_nonflat(allocation):
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
            if hasattr(allocation, "updated_at"):
                allocation.updated_at = allocation.last_reconcile_at
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
            if hasattr(allocation, "updated_at"):
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
        if hasattr(allocation, "updated_at"):
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
            _clear_deferred_reduce(allocation)
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
        if hasattr(allocation, "updated_at"):
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
                async with websockets.connect(self.settings.hyperliquid_ws_url, ping_interval=20) as ws:
                    self.state.market_ws_connected = True
                    self.state.low_latency_primary = True
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
                        await self._handle_ws_message(raw, ws_received_at=ws_received_at)
            except Exception as exc:
                self.state.market_ws_connected = False
                self._update_websocket_connected_state()
                self.state.low_latency_primary = False
                self.state.follower_order_updates_subscribed = False
                self.state.follower_user_events_subscribed = False
                self.state.follower_user_fills_subscribed = False
                self.state.follower_clearinghouse_subscribed = False
                self.state.reconnect_count += 1
                self.state.last_error = str(exc)[:200]
                log.warning("low_latency_ws_reconnect", error=str(exc))
                await asyncio.sleep(3)

    async def _leader_fill_ws_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                async with websockets.connect(self.settings.hyperliquid_ws_url, ping_interval=20) as ws:
                    self.state.leader_fills_ws_connected = True
                    self._update_websocket_connected_state()
                    self.state.last_error = None
                    self._subscribed.clear()
                    self._leader_fill_subscription_event.clear()
                    await self._subscribe_active_leaders(ws)
                    self._schedule_startup_leader_fill_backfill()
                    next_app_ping_at = asyncio.get_running_loop().time() + HYPERLIQUID_WS_APP_PING_SECONDS
                    if self._leader_fill_backfill_start_ms is not None:
                        start_time_ms = self._leader_fill_backfill_start_ms
                        self._leader_fill_backfill_start_ms = None
                        self._schedule_background_task(self._backfill_leader_fills_since(start_time_ms))
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
                        await self._handle_ws_message(raw, ws_received_at=ws_received_at)
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
                self.state.last_error = str(exc)[:200]
                log.warning("low_latency_leader_fill_ws_reconnect", error=str(exc))
                await asyncio.sleep(3)

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

    async def _backfill_leader_fills_since(self, start_time_ms: int) -> None:
        async with self._leader_lock:
            leaders = sorted(self.state.ws_leaders)
        if not leaders:
            return
        for address in leaders:
            try:
                fills = await self.info_client.user_fills_by_time(
                    address,
                    start_time_ms,
                    aggregate_by_time=False,
                )
            except Exception as exc:
                self.state.last_error = f"leader fill backfill {mask_address(address)}: {str(exc)[:160]}"
                log.warning(
                    "low_latency_leader_fill_backfill_failed",
                    leader_address=mask_address(address),
                    start_time_ms=start_time_ms,
                    error=str(exc),
                )
                continue
            fills = [
                fill
                for fill in list(fills or [])
                if (_int_or_none(fill.get("time")) or 0) >= int(start_time_ms)
            ]
            fills = sorted(
                fills,
                key=lambda item: (
                    _int_or_none(item.get("time")) or 0,
                    str(item.get("oid") or ""),
                    str(item.get("tid") or ""),
                ),
            )
            if not fills:
                continue
            await self._handle_ws_message(
                json.dumps({"channel": "userFills", "data": {"user": address, "fills": fills}}),
                ws_received_at=datetime.now(timezone.utc),
            )

    async def _subscribe_follower(self, ws: Any) -> None:
        follower = self.settings.hyperliquid_follower_account_address()
        if not follower:
            return
        subscriptions = {
            "orderUpdates": "follower_order_updates_subscribed",
            "userEvents": "follower_user_events_subscribed",
            "userFills": "follower_user_fills_subscribed",
            "clearinghouseState": "follower_clearinghouse_subscribed",
        }
        for sub_type, attr in subscriptions.items():
            await self._subscribe(ws, {"type": sub_type, "user": follower})
            setattr(self.state, attr, True)

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

    async def _handle_ws_message(self, raw_message: str | bytes, *, ws_received_at: datetime | None = None) -> None:
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
            if channel in {"userFills", "userEvents"}:
                await self._handle_follower_user_fills(channel, data, ws_received_at=ws_received_at)
                return
            return
        if channel != "userFills":
            return
        fills = _fills_from_message(channel, data)
        if not fills:
            return
        leader_address = user_address
        async with self._leader_lock:
            leader = self.state.active_leaders.get(leader_address)
            ws_leader = leader_address in self.state.ws_leaders
        if leader is None or not ws_leader:
            return
        is_snapshot = bool(data.get("isSnapshot"))
        events: list[FillEvent] = []
        for fill in fills:
            try:
                event = build_fill_event(
                    leader_address,
                    fill,
                    is_snapshot=is_snapshot,
                    ws_received_at=ws_received_at,
                )
            except Exception as exc:
                await self._record_parse_error(leader_address, fill, str(exc))
                continue
            self.state.last_event_time_by_dex[event.market.dex] = event.ws_received_at.isoformat()
            events.append(event)
        if not is_snapshot:
            events = await self._filter_suppressed_events(events)
            if not events:
                return
        await self._enqueue_fill_events(events, leader)

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
        async with self.db_session_factory() as db:
            for fill in fills:
                try:
                    market = parse_fill_to_market_key(fill)
                except Exception:
                    continue
                affected_dexes.add(market.dex)
                if await self._follower_fill_matches_auto_copy_order(db, fill):
                    continue
                self.manual_position_guard.mark(
                    market,
                    reason="follower fill was not linked to an AUTO_COPY order",
                    observed_at=ws_received_at,
                )
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
            .where(or_(*filters))
            .limit(1)
        )
        return row is not None

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
        reason = "FOLLOWER_CLEARINGHOUSE_WS" if payload is not None else "FOLLOWER_CLEARINGHOUSE_WS_EMPTY"
        self._schedule_follower_state_refresh([dex], reason=reason)

    def _schedule_follower_state_refresh(self, dexes: list[str] | set[str], *, reason: str) -> None:
        normalized = {str(dex or "").lower() for dex in dexes}
        if not normalized:
            normalized = {dex.dex_name for dex in HyperliquidDexRegistry(self.settings).enabled_dexes()}
        self._follower_state_refresh_pending_dexes.update(normalized)
        if self._follower_state_refresh_task is not None and not self._follower_state_refresh_task.done():
            return
        task = asyncio.create_task(self._follower_state_refresh_worker(reason=reason))
        self._follower_state_refresh_task = task
        self._background_tasks.add(task)
        task.add_done_callback(self._background_task_done)
        task.add_done_callback(self._follower_state_refresh_done)

    def _follower_state_refresh_done(self, task: asyncio.Task) -> None:
        if self._follower_state_refresh_task is task:
            self._follower_state_refresh_task = None

    async def _follower_state_refresh_worker(self, *, reason: str) -> None:
        while self._follower_state_refresh_pending_dexes:
            dexes = sorted(self._follower_state_refresh_pending_dexes)
            self._follower_state_refresh_pending_dexes.clear()
            await self._refresh_follower_positions_for_dexes(dexes, source=reason)

    async def _reconcile_manual_position_guards(self, db: Any, *, dexes: set[str] | None = None) -> int:
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
            before = self.manual_position_guard.active_entry(market)
            self.manual_position_guard.reconcile(
                market,
                unmanaged_qty_by_side=_unmanaged_follower_position_qtys(
                    follower_qty_by_side=follower_qty_by_side,
                    allocation_qty_by_side=allocation_qty_by_side,
                ),
                follower_state_at=follower_state_at,
            )
            if before is not None and self.manual_position_guard.active_entry(market) is None:
                cleared += 1
        return cleared

    def _fill_queue_key(self, event: FillEvent, leader: LeaderConfig) -> tuple[str, str, str]:
        return (
            ExecutionVenue.HYPERLIQUID.value,
            str(event.market.dex or "").lower(),
            event.market.canonical_coin.upper(),
        )

    async def _enqueue_fill_event(self, event: FillEvent, leader: LeaderConfig) -> None:
        await self._enqueue_fill_events([event], leader)

    async def _enqueue_fill_events(self, events: list[FillEvent], leader: LeaderConfig) -> None:
        if not events:
            return
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
                    queue.put_nowait((event, leader))
                worker = self._fill_workers.get(key)
                if worker is None or worker.done():
                    worker = asyncio.create_task(self._fill_worker(key, queue))
                    self._fill_workers[key] = worker

    async def _fill_worker(
        self,
        key: tuple[str, str, str],
        queue: asyncio.Queue[tuple[FillEvent, LeaderConfig]],
    ) -> None:
        while not self._stopped.is_set():
            first_event, first_leader = await queue.get()
            batch: list[tuple[FillEvent, LeaderConfig]] = [(first_event, first_leader)]
            while True:
                try:
                    batch.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            events = [event for event, _leader in batch]
            leader_by_source_fill_id = {event.source_fill_id: leader for event, leader in batch}
            skipped_events: list[FillEvent] = []
            try:
                selected_events = events
                if not first_event.is_snapshot:
                    selected_events, skipped_events, has_order_keys = _aggregate_same_order_fills(events)
                    if not has_order_keys:
                        selected_events = events
                        skipped_events = []
                    elif skipped_events:
                        await self._remember_suppressed_events(skipped_events)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = str(exc)[:200]
                log.exception(
                    "low_latency_fill_worker_batch_prepare_failed",
                    queue_key=":".join(key),
                    dex=key[1],
                    canonical_coin=key[2],
                    source_fill_id=first_event.source_fill_id,
                    error=str(exc),
                )
            else:
                selected_ok = True
                for event in selected_events:
                    leader = leader_by_source_fill_id.get(event.source_fill_id, first_leader)
                    try:
                        async with self.db_session_factory() as db:
                            if isinstance(self.engine, FillDrivenExecutionEngine):
                                order = await self.engine.handle_fill(db, event, leader, submit_order=False)
                            else:
                                order = await self.engine.handle_fill(db, event, leader)
                        if order is not None and order.status == "PENDING_SUBMIT" and order.id is not None:
                            await self._enqueue_submit_order(order, event)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        selected_ok = False
                        self.state.last_error = str(exc)[:200]
                        log.exception(
                            "low_latency_fill_worker_failed",
                            queue_key=":".join(key),
                            leader_id=getattr(leader, "id", None),
                            dex=key[1],
                            canonical_coin=key[2],
                            source_fill_id=event.source_fill_id,
                            error=str(exc),
                        )
                if skipped_events and selected_ok:
                    await self._record_skipped_source_fills(skipped_events)
            finally:
                for _event, _leader in batch:
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
            return f"{base}:serial"
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
        async with self._submit_queue_guard:
            queue = self._submit_queues.get(key)
            if queue is None:
                queue = asyncio.Queue()
                self._submit_queues[key] = queue
            worker = self._submit_workers.get(key)
            if worker is None or worker.done():
                worker = asyncio.create_task(self._submit_worker(key, queue))
                self._submit_workers[key] = worker
        queue.put_nowait((order_id, event))

    async def _submit_worker(
        self,
        key: str,
        queue: asyncio.Queue[tuple[int, FillEvent]],
    ) -> None:
        while not self._stopped.is_set():
            try:
                order_id, event = await asyncio.wait_for(queue.get(), timeout=10.0)
            except asyncio.TimeoutError:
                if queue.empty():
                    async with self._submit_queue_guard:
                        if self._submit_queues.get(key) is queue:
                            self._submit_queues.pop(key, None)
                        if self._submit_workers.get(key) is asyncio.current_task():
                            self._submit_workers.pop(key, None)
                    return
                continue
            try:
                await self._process_submit_queue_item(order_id, event)
                self._submit_retry_counts.pop(int(order_id), None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                retry_safe = False
                if _is_transient_submit_exception(exc):
                    retry_safe = await self._prepare_submit_retry_if_safe(order_id)
                retry_count = self._submit_retry_counts.get(int(order_id), 0)
                if retry_safe and retry_count < 3:
                    self._submit_retry_counts[int(order_id)] = retry_count + 1
                    log.warning(
                        "low_latency_submit_worker_retrying",
                        dex=key,
                        order_id=order_id,
                        source_fill_id=event.source_fill_id,
                        retry_count=retry_count + 1,
                        error=str(exc)[:300],
                    )
                    queue.put_nowait((order_id, event))
                else:
                    self._submit_retry_counts.pop(int(order_id), None)
                    self.state.last_error = str(exc)[:200]
                    log.exception(
                        "low_latency_submit_worker_failed",
                        dex=key,
                        order_id=order_id,
                        source_fill_id=event.source_fill_id,
                        error=str(exc),
                    )
            finally:
                queue.task_done()

    async def _process_submit_queue_item(self, order_id: int, event: FillEvent) -> None:
        async with self.db_session_factory() as db:
            order = await db.get(ExecutionOrder, order_id)
            if order is None:
                return
            status = str(order.status or "").upper()
            if status == "SUBMITTING":
                return
            if status != "PENDING_SUBMIT":
                self.engine.pending_intents.release(order)
                return
            if not self.engine.pending_intents.has_active_order(order):
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
            await self._wait_for_submit_barrier(order)
            await self.engine.submit_planned_order(db, order, event)

    async def _prepare_submit_retry_if_safe(self, order_id: int) -> bool:
        try:
            async with self.db_session_factory() as db:
                order = await db.get(ExecutionOrder, order_id)
                if order is None:
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
            self.state.last_error = str(exc)[:200]
            log.warning(
                "low_latency_submit_worker_retry_prepare_failed",
                order_id=order_id,
                error=str(exc),
            )
            return False

    async def _wait_for_submit_barrier(self, order: ExecutionOrder) -> None:
        wait_started_at: datetime | None = None
        logged = False
        while True:
            barriers = self.engine.pending_intents.submit_barriers_before(order)
            if not barriers:
                if wait_started_at is not None:
                    _trace_set(order, "submit_barrier_wait_done_at", datetime.now(timezone.utc))
                    _trace_set_detail(order, "submit_barrier_waited", True)
                return
            if wait_started_at is None:
                wait_started_at = datetime.now(timezone.utc)
                _trace_set(order, "submit_barrier_wait_started_at", wait_started_at)
            waited_ms = _delta_ms(wait_started_at, datetime.now(timezone.utc)) or 0
            if not logged and waited_ms >= 100:
                logged = True
                log.warning(
                    "low_latency_submit_barrier_waiting",
                    order_id=order.id,
                    source_fill_id=order.source_fill_id,
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
            await asyncio.sleep(0.005)

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
            ]

    async def _remember_suppressed_events(self, events: list[FillEvent]) -> None:
        if not events:
            return
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=60)
        async with self._suppressed_source_fill_guard:
            self._prune_suppressed_source_fill_ids(now)
            for event in events:
                self._suppressed_source_fill_ids[event.source_fill_id] = expires_at

    async def _forget_suppressed_events(self, events: list[FillEvent]) -> None:
        if not events:
            return
        async with self._suppressed_source_fill_guard:
            for event in events:
                self._suppressed_source_fill_ids.pop(event.source_fill_id, None)

    def _prune_suppressed_source_fill_ids(self, now: datetime) -> None:
        expired = [
            source_fill_id
            for source_fill_id, expires_at in self._suppressed_source_fill_ids.items()
            if expires_at <= now
        ]
        for source_fill_id in expired:
            self._suppressed_source_fill_ids.pop(source_fill_id, None)

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
            self.state.last_error = str(exc)[:200]
            log.warning("low_latency_background_task_failed", error=str(exc))

    async def _record_skipped_source_fills(self, events: list[FillEvent]) -> None:
        if not events:
            return
        recorded = False
        try:
            async with self.db_session_factory() as db:
                for event in events:
                    await self.engine._record_source_fill(db, event, processed=not event.is_snapshot)
                await db.commit()
            recorded = True
        except Exception as exc:
            self.state.last_error = str(exc)[:200]
            log.warning(
                "low_latency_skipped_source_fill_record_failed",
                error=str(exc),
                source_fill_ids=[event.source_fill_id for event in events[:10]],
                count=len(events),
            )
        if recorded:
            await self._forget_suppressed_events(events)

    async def _record_parse_error(self, leader_address: str, fill: dict[str, Any], message: str) -> None:
        async with self.db_session_factory() as db:
            db.add(
                RiskEvent(
                    severity="warning",
                    event_type="FILL_PARSE_FAILED",
                    symbol=str(fill.get("coin") or ""),
                    leader_address=leader_address,
                    message=message,
                    metadata_json={"fill": {k: str(v) for k, v in fill.items() if k in {"coin", "hash", "tid", "oid", "time"}}},
                )
            )
            await db.commit()

    async def _status_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.store_status()
            except Exception as exc:
                self.state.last_error = str(exc)[:200]
            await asyncio.sleep(1)

    async def _account_value_readiness(
        self,
        db: Any,
        *,
        dexes: list[str],
        active_leaders: list[str],
    ) -> dict[str, Any]:
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
                resolved = resolved_value_payload(payload, dex)
                ok, reason = _account_value_payload_ready(resolved, role=role)
                details[scope][label] = {
                    "ready": ok,
                    "source": (resolved or {}).get("account_value_source") or (resolved or {}).get("source"),
                    "mode": (resolved or {}).get("account_abstraction_mode") or (resolved or {}).get("mode"),
                    "account_value": (resolved or {}).get("account_value_used_for_sizing")
                    or (resolved or {}).get("accountValueUsedForSizing"),
                    "reason": reason,
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
            allocation_dexes = {
                str(row or "").lower()
                for row in (
                    await db.execute(
                        select(LeaderPositionAllocationRecord.dex)
                        .join(LeaderConfig, LeaderConfig.id == LeaderPositionAllocationRecord.leader_id)
                        .where(LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value)
                        .where(LeaderPositionAllocationRecord.status != "CLOSED")
                        .where(LeaderConfig.enabled.is_(True))
                        .where(LeaderConfig.deleted_at.is_(None))
                        .distinct()
                    )
                ).scalars().all()
            }
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
            ready = (
                self.state.websocket_connected
                and bool(active)
                and self.state.follower_order_updates_subscribed
                and (self.settings.allow_poll_fallback_live or not self.state.poll_fallback_leaders)
                and price_fresh
                and bool(account_value_status.get("ready"))
                and bool(follower_position_freshness.get("ready"))
            )
            self.state.low_latency_ready = ready
            payload = {
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
                "manual_position_guards": self.manual_position_guard.snapshot(),
                "follower_order_updates_subscribed": self.state.follower_order_updates_subscribed,
                "follower_user_events_subscribed": self.state.follower_user_events_subscribed,
                "follower_user_fills_subscribed": self.state.follower_user_fills_subscribed,
                "follower_clearinghouse_subscribed": self.state.follower_clearinghouse_subscribed,
                "leader_user_fills_subscribed_count": self.state.leader_user_fills_subscribed_count,
                "dex_price_cache_status": price_status,
                "price_cache_required_dexes": required_price_dexes,
                "price_cache_all_dexes_fresh": all(item["fresh"] for item in price_status.values()) if price_status else False,
                "price_cache": self.price_cache.snapshot(dexes),
                "default_dex_price_cache_fresh": price_status.get("", {}).get("fresh", False),
                "xyz_price_cache_fresh": price_status.get("xyz", {}).get("fresh", False),
                "last_ws_event_at": self.state.last_ws_event_at.isoformat() if self.state.last_ws_event_at else None,
                "last_ws_event_age_ms": last_age,
                "reconnect_count": self.state.reconnect_count,
                "last_error": self.state.last_error,
                "updated_at": now.isoformat(),
            }
            stmt = (
                insert(AppSetting)
                .values(key="watcher_status", value=payload, updated_at=now)
                .on_conflict_do_update(
                    index_elements=[AppSetting.key],
                    set_={"value": payload, "updated_at": now},
                )
            )
            await db.execute(stmt)
            await store_task_status(
                db,
                task_name="low_latency_watcher",
                last_error=self.state.last_error,
                metadata={
                    "websocket_connected": self.state.websocket_connected,
                    "ready_for_low_latency_live": ready,
                    "active_leaders": active,
                },
            )
            await store_task_status(
                db,
                task_name="price_cache_updater",
                last_error=self.state.last_error if not price_fresh else None,
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
                    .values(key="watcher_status", value=payload, updated_at=now)
                    .on_conflict_do_update(
                        index_elements=[AppSetting.key],
                        set_={"value": payload, "updated_at": now},
                    )
                )
                await db.execute(stmt)
                await db.commit()
        except Exception as exc:
            log.warning("starting_status_store_failed", error=str(exc)[:160])

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
        for key in allocation_keys:
            updated_at = positions_by_key.get(key)
            if updated_at is None:
                continue
            max_age_ms = max(max_age_ms, int((now - updated_at).total_seconds() * 1000))
        ready = not missing and max_age_ms <= threshold_ms
        return {
            "ready": ready,
            "active_allocation_count": len(allocation_keys),
            "max_age_ms": max_age_ms,
            "threshold_ms": threshold_ms,
            "missing": missing,
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
    groups: dict[tuple[str, str, str, str, str, str], list[int]] = {}
    passthrough: dict[int, FillEvent] = {}
    for idx, event in enumerate(events):
        key = _same_order_key(event)
        if key is None:
            passthrough[idx] = event
            continue
        groups.setdefault(key, []).append(idx)

    if not groups:
        return events, [], False

    selected_by_index: dict[int, FillEvent] = dict(passthrough)
    skipped: list[FillEvent] = []
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
    return selected, skipped, True


def _same_order_key(event: FillEvent) -> tuple[str, str, str, str, str, str] | None:
    raw = event.raw or {}
    oid = raw.get("oid")
    order_hash = raw.get("hash")
    if oid is not None:
        return (
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
    total_size = sum(abs(event.size) for event in segment)
    total_notional = sum(abs(event.price * event.size) for event in segment)
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
    raw[_COALESCED_SOURCE_IDS_KEY] = [event.source_fill_id for event in segment]

    return representative_idx, replace(
        representative,
        price=vwap,
        size=total_size,
        raw=raw,
    )


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
    raw[_COALESCED_SOURCE_IDS_KEY] = [item.source_fill_id for item in segment]
    return replace(event, raw=raw)


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
    )


def unresolved_same_market_order_query(*, leader_address: str, dex: str, canonical_coin: str):
    return (
        select(ExecutionOrder)
        .where(ExecutionOrder.source_type == "AUTO_COPY")
        .where(ExecutionOrder.status.in_(RECOVERY_ORDER_STATUSES))
        .where(ExecutionOrder.leader_address == leader_address.lower())
        .where(ExecutionOrder.dex == str(dex or "").lower())
        .where(func.upper(ExecutionOrder.canonical_coin) == str(canonical_coin).upper())
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
    text = f"{type(exc).__name__}: {exc}".lower()
    transient_fragments = (
        "deadlock detected",
        "deadlockdetectederror",
        "could not serialize access",
        "serializationfailure",
        "lock timeout",
        "pendingrollbackerror",
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
    if not _is_final_close_min_order_only_block(
        reduce_only=reduce_only,
        transition_plan=transition_plan,
        validator_result=validator_result,
    ):
        return validator_result, False
    warnings = list(validator_result.warnings or [])
    warnings.append(
        "final close below Hyperliquid minimum order value; attempting reduce-only close so copied allocation can flatten"
    )
    return (
        replace(
            validator_result,
            errors=[],
            warnings=warnings,
            block_reason=None,
        ),
        True,
    )


def _is_final_close_min_order_only_block(
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
        AllocationTransitionAction.CLOSE.value,
        AllocationTransitionAction.FLIP_CLOSE_FIRST.value,
    }:
        return False
    errors = {str(item).upper() for item in (validator_result.errors or [])}
    if errors != {"BELOW_MIN_ORDER_VALUE"}:
        return False
    return validator_result.rounded_size > 0 and validator_result.rounded_price > 0


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
        "details": {},
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
    metrics["account_cache_read_ms"] = account_cache_read_ms
    metrics["price_cache_read_ms"] = price_cache_read_ms
    metrics["allocation_read_ms"] = allocation_read_ms
    metrics["validator_ms"] = _delta_ms(
        _trace_time(timestamps, "validator_started_at"),
        _trace_time(timestamps, "validator_done_at"),
    )
    order_submit_done_at = getattr(order, "order_submit_done_at", None)
    metrics["pending_submit_db_write_ms"] = _delta_ms(
        _trace_time(timestamps, "pending_submit_write_started_at"),
        _trace_time(timestamps, "pending_submit_write_done_at"),
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
        _trace_time(timestamps, "sdk_order_call_started_at"),
        _trace_time(timestamps, "sdk_http_post_started_at"),
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


def _order_market_owner_guard_allows_manual_guard(order: ExecutionOrder) -> bool:
    checklist = order.pre_trade_checklist if isinstance(order.pre_trade_checklist, dict) else {}
    guard = checklist.get("market_owner_guard")
    if not isinstance(guard, dict) or guard.get("blocked"):
        return False
    owner_allocation_id = _int_or_none(guard.get("owner_allocation_id"))
    order_allocation_id = _int_or_none(getattr(order, "allocation_id", None))
    return owner_allocation_id is not None and order_allocation_id is not None and owner_allocation_id == order_allocation_id


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
    )


def _transition_plan_targets_flat_leader(transition_plan: Any | None) -> bool:
    if transition_plan is None:
        return False
    target_notional = _decimal_from_value(getattr(transition_plan, "target_notional", None))
    if target_notional is None or abs(target_notional) > ALLOCATION_TRANSITION_TOLERANCE:
        return False
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
    if is_flip and action_value in open_actions:
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
    checkpoint = _latest_datetime(allocation_reconcile_at, latest_fill_event_at)
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
) -> str | None:
    if _allocation_lifecycle_active(allocation):
        return None
    if _fill_is_new_open_from_flat(fill_implied_position):
        return None
    return "IGNORED_OLD_LIFECYCLE: no follower allocation exists; waiting for leader open from flat"


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
