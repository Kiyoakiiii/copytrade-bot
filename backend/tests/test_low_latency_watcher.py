import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from app.api.preflight import _low_latency_gate_blockers
from app.core.config import Settings
from app.models import (
    AllocationEvent,
    ExecutionOrder,
    LatestAccountPosition,
    LatestAccountState,
    LeaderPositionAllocationRecord,
    LeaderPositionBaseline,
    MarketRiskSetting,
    RiskEvent,
)
from app.services.calculator import calculate_target_notional_by_account_ratio
from app.services.leader_config import is_coin_allowed
from app.services.low_latency_watcher import (
    FillDrivenExecutionEngine,
    FillEvent,
    FollowerManualPositionGuard,
    HyperliquidLowLatencyWatcher,
    LEADER_FILL_PRICE_FALLBACK_SOURCE,
    LowLatencyPriceCache,
    MarketKey,
    PENDING_OPEN_REASON,
    PENDING_OPEN_STATUS,
    PendingIntentLedger,
    PriceEntry,
    LiveLeaderPositionSnapshot,
    _coalesce_queued_lifecycle_fills,
    _coalesce_same_batch_fills,
    _configured_leader_account_value,
    _clear_deferred_reduce,
    _direction_guard_preserves_allocation,
    _fill_direction_action_block_reason,
    _fill_notional_for_sizing,
    _fill_derived_leader_position,
    _hyperliquid_fill_qty_price,
    _hyperliquid_status,
    _account_value_payload_needs_refresh,
    _account_value_payload_ready,
    _allow_target_notional_price_drift_for_transition,
    _allocation_has_flat_leader_close_intent,
    _allocation_market_owner_active,
    _allocation_mismatch_from_stale_follower_state,
    _allocation_state_target_notional,
    _allocation_sync_in_post_fill_snapshot_lag_guard,
    _apply_pending_reduce_offset_from_plan,
    _effective_aggregate_follower_qty_for_reduce_scope,
    _execution_price_entry_for_fill,
    _ignore_without_allocation_reason,
    _is_account_value_pending_open,
    _is_deferred_reduce_block,
    _is_pending_open_block,
    _leader_account_value_safety_blockers,
    _latest_allocation_reconcile_at,
    _live_adjusted_fill_implied_position,
    _mark_deferred_reduce,
    _manual_same_side_position_sync,
    _market_owner_blocker,
    _missed_reduce_catchup_allows_direction_mismatch,
    _order_quantity_for_transition,
    _order_market_owner_guard_allows_manual_guard,
    _order_intent_blockers,
    _order_side,
    _override_final_close_min_order_validator,
    _pending_open_activation_block_reason,
    _pending_open_allocation_flat,
    _pending_open_should_remain_pending,
    _price_status_ready_for_low_latency_live,
    _preserve_allocation_state_after_direction_guard,
    _reduce_quantity_guard_blockers,
    _risk_setting_result_from_row,
    _required_price_status_dexes,
    _should_use_fill_derived_position,
    _set_latency_fields,
    _should_refresh_live_leader_position_for_reduce,
    _should_fast_forward_below_min_active_increase,
    _should_fast_forward_below_min_pending_open_lifecycle,
    _fast_forward_below_min_active_increase,
    _fast_forward_below_min_pending_open_lifecycle,
    _close_zero_allocation_lifecycle,
    _snapshot_event_after_allocation_checkpoint,
    _snapshot_recovery_allocation_side,
    _snapshot_recovery_fill,
    _snapshot_recovery_should_use_allocation_checkpoint,
    _stale_zero_allocation_reason,
    _trace_set,
    _unmanaged_follower_position_blocker,
    _unmanaged_follower_position_from_stale_follower_state,
    _unmanaged_follower_position_qtys,
    _unmanaged_follower_position_reduce_safe,
    derive_leader_post_position_from_fill,
    build_fill_event,
    parse_fill_to_market_key,
    unresolved_same_market_order_query,
)
from app.services.hyperliquid_execution import validate_hyperliquid_order_params
from app.services.hyperliquid_risk_settings import RiskSettingResult
from app.services.order_policy import HYPERLIQUID_AUTO_COPY_ORDER_TYPE
from app.services.allocations import AllocationTransitionAction, assert_allocation_scope, plan_leader_allocation_transition
from app.services.target_position import PositionSide
from app.services.account_abstraction import account_abstraction_setting_key


def settings(**overrides) -> Settings:
    return Settings(_env_file=None, hyperliquid_account_address="0x" + "5" * 40, **overrides)


def leader(address: str = "0x" + "1" * 40, **overrides):
    item = SimpleNamespace(
        id=1,
        leader_address=address.lower(),
        enabled=True,
        deleted_at=None,
        allowed_symbols=None,
        blocked_symbols=[],
        copy_multiplier=Decimal("0.1"),
        fixed_account_value=Decimal("100000"),
        preferred_venue="HYPERLIQUID",
    )
    for key, value in overrides.items():
        setattr(item, key, value)
    return item


def test_configured_leader_account_value_is_positive_and_per_leader() -> None:
    assert _configured_leader_account_value(leader(fixed_account_value=Decimal("50000"))) == Decimal("50000")
    assert _configured_leader_account_value(leader(fixed_account_value=Decimal("75000"))) == Decimal("75000")
    assert _configured_leader_account_value(leader(fixed_account_value=Decimal("0"))) is None
    assert _configured_leader_account_value(SimpleNamespace()) is None


def fill_event(
    *,
    coin: str = "BTC",
    dex: str = "",
    asset_id: int | None = 0,
    snapshot: bool = False,
    side: str = "B",
    start_position: str = "0",
    direction: str = "Open Long",
    price: str = "100",
) -> FillEvent:
    canonical = f"{dex}:{coin}" if dex else coin
    return FillEvent(
        source_fill_id="fill-1",
        leader_address=("0x" + "1" * 40).lower(),
        market=MarketKey(
            dex=dex,
            coin=coin,
            canonical_coin=canonical,
            raw_coin=canonical,
            asset_id=asset_id,
            venue_symbol=canonical,
        ),
        side=side,
        price=Decimal(price),
        size=Decimal("1"),
        time_ms=1_700_000_000_000,
        raw={
            "coin": canonical,
            "px": price,
            "sz": "1",
            "side": side,
            "time": 1_700_000_000_000,
            "startPosition": start_position,
            "dir": direction,
        },
        is_snapshot=snapshot,
        ws_received_at=datetime.fromtimestamp(1_700_000_000_100 / 1000, timezone.utc),
    )


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return self.rows

    def scalar_one_or_none(self):
        return self.rows[0] if self.rows else None


class SequenceExecuteSession:
    def __init__(self, rows_sequence):
        self.rows_sequence = [list(rows) for rows in rows_sequence]
        self.statements = []
        self.flushes = 0

    async def execute(self, stmt):
        self.statements.append(stmt)
        rows = self.rows_sequence.pop(0) if self.rows_sequence else []
        return FakeResult(rows)

    async def flush(self):
        self.flushes += 1


class FakeSession:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.added = []
        self.statements = []
        self.commits = 0
        self.flushes = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def execute(self, stmt):
        self.statements.append(stmt)
        return FakeResult(self.rows)

    async def scalar(self, stmt):
        self.statements.append(stmt)
        return None

    async def flush(self):
        self.flushes += 1
        return None

    async def commit(self):
        self.commits += 1
        return None

    async def get(self, model, key):
        return None

    def add(self, item):
        self.added.append(item)


class AllocationSession(FakeSession):
    def __init__(self, allocation):
        super().__init__()
        self.allocation = allocation

    async def get(self, model, key):
        if model is LeaderPositionAllocationRecord and key == self.allocation.id:
            return self.allocation
        return None


class RaisingExecuteSession:
    async def execute(self, stmt):
        raise AssertionError("DB execute should not be called on account-value cache hit")

    async def flush(self):
        return None

    async def commit(self):
        self.commits += 1

    async def get(self, model, key):
        return None

    def add(self, item):
        self.added.append(item)


class SequenceScalarSession(FakeSession):
    def __init__(self, values):
        super().__init__()
        self.values = list(values)

    async def scalar(self, stmt):
        self.statements.append(stmt)
        if self.values:
            return self.values.pop(0)
        return None


class AllocationGetSession(FakeSession):
    def __init__(self, allocation):
        super().__init__()
        self.allocation = allocation

    async def get(self, model, key):
        if model is LeaderPositionAllocationRecord and key == self.allocation.id:
            return self.allocation
        return None


def compiled_sql(stmt) -> str:
    return str(stmt.compile(compile_kwargs={"literal_binds": True})).lower()


class FakeSessionFactory:
    def __init__(self, rows=None):
        self.session = FakeSession(rows)

    def __call__(self):
        return self.session


class RecordingEngine:
    def __init__(self):
        self.calls = []
        self.recorded_source_fills = []

    async def handle_fill(self, db, event, leader_config):
        self.calls.append((event, leader_config))

    async def _record_source_fill(self, db, fill, *, processed):
        self.recorded_source_fills.append((fill.source_fill_id, processed))
        return True


async def handle_ws_and_drain(watcher, message):
    await watcher._handle_ws_message(json.dumps(message))
    await watcher._drain_fill_queues()
    await watcher._drain_background_tasks()


class FakeWs:
    def __init__(self):
        self.sent = []

    async def send(self, payload):
        self.sent.append(json.loads(payload))


class NoopInfoClient:
    async def meta(self, dex=""):
        return {"universe": [{"name": "BTC"}, {"name": "HYUNDAI"}]}


class PriceRestProbeInfoClient(NoopInfoClient):
    def __init__(self):
        self.all_mids_calls = 0

    async def all_mids(self, dex=""):
        self.all_mids_calls += 1
        raise AssertionError("hot path price lookup must not call REST all_mids")


class BackfillInfoClient(NoopInfoClient):
    def __init__(self, fills):
        self.fills = fills
        self.calls = []

    async def user_fills_by_time(self, user, start_time_ms, aggregate_by_time=False):
        self.calls.append((user, start_time_ms, aggregate_by_time))
        return self.fills


class PrecisionInfoClient(NoopInfoClient):
    def __init__(self):
        self.calls = 0

    async def meta(self, dex=""):
        self.calls += 1
        return {"universe": [{"name": "xyz:USAR", "maxLeverage": 10, "szDecimals": 2}]}


class TimeoutExecutionClient:
    def __init__(self):
        self.calls = 0
        self.leverage_updates = []

    async def meta(self, dex=""):
        return {"universe": [{"name": "BTC", "maxLeverage": 50}, {"name": "HYUNDAI", "maxLeverage": 5}]}

    async def account_state(self, address=None, dex=""):
        return {"withdrawable": "1000", "marginSummary": {"accountValue": "1000"}}

    async def update_leverage(self, *, coin, leverage, is_cross):
        self.leverage_updates.append({"coin": coin, "leverage": leverage, "is_cross": is_cross})
        return {"status": "ok"}

    async def place_market_order(self, **kwargs):
        self.calls += 1
        raise TimeoutError("request timed out after submit")


class LocalSdkPayloadErrorExecutionClient(TimeoutExecutionClient):
    async def place_market_order(self, **kwargs):
        self.calls += 1
        raise AttributeError("'str' object has no attribute 'to_raw'")


class RestingExecutionClient:
    def __init__(self):
        self.orders = []
        self.cancels = []
        self.leverage_updates = []

    async def meta(self, dex=""):
        return {"universe": [{"name": "BTC", "maxLeverage": 50}, {"name": "HYUNDAI", "maxLeverage": 5}]}

    async def account_state(self, address=None, dex=""):
        return {"withdrawable": "1000", "marginSummary": {"accountValue": "1000"}}

    async def update_leverage(self, *, coin, leverage, is_cross):
        self.leverage_updates.append({"coin": coin, "leverage": leverage, "is_cross": is_cross})
        return {"status": "ok"}

    async def place_market_order(self, **kwargs):
        self.orders.append(kwargs)
        return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 123}}]}}}

    async def cancel_by_cloid(self, **kwargs):
        self.cancels.append(kwargs)


class FilledExecutionClient(RestingExecutionClient):
    async def place_market_order(self, **kwargs):
        self.orders.append(kwargs)
        return {
            "status": "ok",
            "response": {
                "data": {
                    "statuses": [
                        {
                            "filled": {
                                "totalSz": str(kwargs["sz"]),
                                "avgPx": str(kwargs["limit_px"]),
                                "oid": 123,
                            }
                        }
                    ]
                }
            },
        }


class RejectedReduceOnlyExecutionClient(RestingExecutionClient):
    async def place_market_order(self, **kwargs):
        self.orders.append(kwargs)
        return {
            "status": "ok",
            "response": {
                "data": {
                    "statuses": [
                        {"error": "Reduce only order would increase position. asset=231"},
                    ],
                },
            },
        }


def valid_order_validator_payload(
    *,
    raw_size: str = "1",
    raw_price: str = "100",
    min_order_value: str = "10",
    action: str = "OPEN",
    side: str = "BUY",
    is_buy: bool = True,
    reduce_only: bool = False,
) -> dict:
    result = validate_hyperliquid_order_params(
        dex="",
        canonical_coin="BTC",
        asset_id=0,
        action=action,
        side=side,
        target_delta_notional=Decimal("100"),
        raw_size=Decimal(raw_size),
        raw_price=Decimal(raw_price),
        market_meta={"name": "BTC", "asset_id": 0, "index": 0, "szDecimals": 5, "maxLeverage": 50},
        order_policy={
            "cloid": "0x" + "1" * 32,
            "is_buy": is_buy,
            "reduce_only": reduce_only,
            "tif": "Ioc",
            "order_type": {"limit": {"tif": "Ioc"}},
            "slippage_bps": 0,
            "price_fresh": True,
            "min_order_value": min_order_value,
            "effective_leverage": 10,
        },
    )
    return {"order_validator": result.to_dict()}


def test_execution_price_entry_keeps_fresh_cache_reference() -> None:
    cached = PriceEntry(
        price=Decimal("101"),
        updated_at=datetime.now(timezone.utc),
        source="REST",
    )

    entry, fresh, fallback, source = _execution_price_entry_for_fill(
        cached_entry=cached,
        cache_fresh=True,
        fill=fill_event(price="100"),
    )

    assert entry is cached
    assert fresh is True
    assert fallback is False
    assert source == "REST"


def test_execution_price_entry_uses_fill_price_when_cache_stale() -> None:
    cached = PriceEntry(
        price=Decimal("101"),
        updated_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        source="REST",
    )

    entry, fresh, fallback, source = _execution_price_entry_for_fill(
        cached_entry=cached,
        cache_fresh=False,
        fill=fill_event(price="68.021"),
    )

    assert entry is not None
    assert entry.price == Decimal("68.021")
    assert entry.source == LEADER_FILL_PRICE_FALLBACK_SOURCE
    assert fresh is True
    assert fallback is True
    assert source == LEADER_FILL_PRICE_FALLBACK_SOURCE


def test_execution_price_entry_still_blocks_without_any_valid_price() -> None:
    entry, fresh, fallback, source = _execution_price_entry_for_fill(
        cached_entry=None,
        cache_fresh=False,
        fill=fill_event(price="0"),
    )

    assert entry is None
    assert fresh is False
    assert fallback is False
    assert source == "missing"


def test_fill_price_fallback_keeps_all_lifecycle_actions_price_fresh() -> None:
    stale_cached = PriceEntry(
        price=Decimal("101"),
        updated_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        source="REST",
    )
    entry, fresh, fallback, source = _execution_price_entry_for_fill(
        cached_entry=stale_cached,
        cache_fresh=False,
        fill=fill_event(price="68.021"),
    )

    assert entry is not None
    assert fresh is True
    assert fallback is True
    assert source == LEADER_FILL_PRICE_FALLBACK_SOURCE

    lifecycle_actions = [
        (AllocationTransitionAction.OPEN, False, "BUY"),
        (AllocationTransitionAction.INCREASE, False, "BUY"),
        (AllocationTransitionAction.FLIP_OPEN_SECOND, False, "BUY"),
        (AllocationTransitionAction.REDUCE, True, "SELL"),
        (AllocationTransitionAction.CLOSE, True, "SELL"),
        (AllocationTransitionAction.FLIP_CLOSE_FIRST, True, "SELL"),
    ]
    for action, reduce_only, side in lifecycle_actions:
        result = validate_hyperliquid_order_params(
            dex="",
            canonical_coin="HYPE",
            asset_id=100,
            action=action.value,
            side=side,
            target_delta_notional=Decimal("100"),
            raw_size=Decimal("1.23"),
            raw_price=entry.price,
            market_meta={"name": "HYPE", "asset_id": 100, "index": 100, "szDecimals": 2, "maxLeverage": 10},
            order_policy={
                "cloid": "0x" + "2" * 32,
                "is_buy": side == "BUY",
                "reduce_only": reduce_only,
                "tif": "Ioc",
                "order_type": {"limit": {"tif": "Ioc"}},
                "aggressive_market": True,
                "price_fresh": fresh,
                "min_order_value": Decimal("10"),
                "effective_leverage": 10,
            },
        )

        assert result.ok, (action.value, result.errors)
        assert "BLOCKED_PRICE_STALE" not in result.errors


def test_hot_path_price_lookup_does_not_rest_refresh_stale_cache() -> None:
    client = PriceRestProbeInfoClient()
    price_cache = LowLatencyPriceCache(stale_ms=2_000)
    price_cache.set_price(dex="", coin="BTC", price="101", source="REST_POLL_FALLBACK")
    price_cache._prices["BTC"] = PriceEntry(
        price=Decimal("101"),
        updated_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        source="REST_POLL_FALLBACK",
    )
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=client,
        execution_client=TimeoutExecutionClient(),
        price_cache=price_cache,
    )

    entry = asyncio.run(engine._load_hot_path_price(fill_event().market))

    assert entry is not None
    assert entry.price == Decimal("101")
    assert client.all_mids_calls == 0


def test_hot_path_price_lookup_does_not_rest_refresh_missing_cache() -> None:
    client = PriceRestProbeInfoClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=client,
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )

    entry = asyncio.run(engine._load_hot_path_price(fill_event().market))

    assert entry is None
    assert client.all_mids_calls == 0


def test_parse_fill_default_dex_btc() -> None:
    market = parse_fill_to_market_key({"coin": "BTC", "asset": 0})

    assert market.dex == ""
    assert market.coin == "BTC"
    assert market.canonical_coin == "BTC"
    assert market.asset_id == 0


def test_parse_fill_xyz_canonical_coin() -> None:
    market = parse_fill_to_market_key({"coin": "xyz:HYUNDAI", "asset": 7})

    assert market.dex == "xyz"
    assert market.coin == "HYUNDAI"
    assert market.canonical_coin == "xyz:HYUNDAI"
    assert market.venue_symbol == "xyz:HYUNDAI"


def test_parse_fill_uses_dex_hint_and_asset_id() -> None:
    market = parse_fill_to_market_key({"coin": "HYUNDAI", "dex": "xyz", "assetId": "4"})

    assert market.dex == "xyz"
    assert market.canonical_coin == "xyz:HYUNDAI"
    assert market.asset_id == 4


def test_build_fill_event_keeps_snapshot_flag() -> None:
    event = build_fill_event("0x" + "1" * 40, {"coin": "xyz:HYUNDAI", "px": "10", "sz": "2", "time": 1}, is_snapshot=True)

    assert event.is_snapshot is True
    assert event.market.canonical_coin == "xyz:HYUNDAI"
    assert event.price == Decimal("10")


def test_fill_derived_leader_position_uses_start_position_and_fill_side() -> None:
    open_long = build_fill_event(
        "0x" + "1" * 40,
        {"coin": "xyz:URNM", "px": "65.9", "sz": "200", "side": "B", "startPosition": "0", "time": 1},
        is_snapshot=False,
    )
    add_short = build_fill_event(
        "0x" + "1" * 40,
        {"coin": "xyz:BIRD", "px": "6.377", "sz": "10", "side": "A", "startPosition": "-39.2", "time": 1},
        is_snapshot=False,
    )

    long_position = _fill_derived_leader_position(open_long)
    short_position = _fill_derived_leader_position(add_short)

    assert long_position is not None
    assert long_position.side == PositionSide.LONG
    assert long_position.size == Decimal("200")
    assert long_position.notional == Decimal("13180.0")
    assert short_position is not None
    assert short_position.side == PositionSide.SHORT
    assert short_position.size == Decimal("49.2")
    assert short_position.notional == Decimal("-313.7484")


def test_derive_leader_post_position_classifies_reduce_close_and_flip() -> None:
    reduce_long = build_fill_event(
        "0x" + "1" * 40,
        {"coin": "xyz:USAR", "px": "25", "sz": "20", "side": "A", "startPosition": "100", "dir": "Close Long", "time": 1},
        is_snapshot=False,
    )
    close_short = build_fill_event(
        "0x" + "1" * 40,
        {"coin": "xyz:BIRD", "px": "6", "sz": "10", "side": "B", "startPosition": "-10", "dir": "Close Short", "time": 1},
        is_snapshot=False,
    )
    flip_to_long = build_fill_event(
        "0x" + "1" * 40,
        {"coin": "xyz:BIRD", "px": "6", "sz": "15", "side": "B", "startPosition": "-10", "dir": "Close Short", "time": 1},
        is_snapshot=False,
    )

    reduced = derive_leader_post_position_from_fill(reduce_long)
    closed = derive_leader_post_position_from_fill(close_short)
    flipped = derive_leader_post_position_from_fill(flip_to_long)

    assert reduced.is_reduce is True
    assert reduced.side_after == PositionSide.LONG
    assert reduced.size_after == Decimal("80")
    assert closed.is_close is True
    assert closed.side_after == PositionSide.FLAT
    assert flipped.is_flip is True
    assert flipped.side_after == PositionSide.LONG
    assert flipped.size_after == Decimal("5")


def test_fill_derived_position_wins_when_account_snapshot_lags_fill() -> None:
    event = build_fill_event(
        "0x" + "1" * 40,
        {"coin": "xyz:URNM", "px": "65.9", "sz": "200", "side": "B", "startPosition": "0", "time": 1_700_000_000_000},
        is_snapshot=False,
    )
    derived = _fill_derived_leader_position(event)
    missing_snapshot = None
    pre_fill_snapshot = LatestAccountPosition(
        account_state_id=1,
        role="LEADER",
        address=("0x" + "1" * 40).lower(),
        coin="URNM",
        dex="xyz",
        canonical_coin="xyz:URNM",
        side="LONG",
        size=Decimal("0"),
        notional=Decimal("0"),
        active=True,
        last_update_at=datetime.fromtimestamp((event.time_ms - 1_000) / 1000, timezone.utc),
    )

    assert _should_use_fill_derived_position(missing_snapshot, event, derived) is True
    assert _should_use_fill_derived_position(pre_fill_snapshot, event, derived) is True


def test_snapshot_updated_after_fill_cannot_block_fill_derived_position() -> None:
    event = build_fill_event(
        "0x" + "1" * 40,
        {"coin": "xyz:URNM", "px": "65.9", "sz": "200", "side": "B", "startPosition": "0", "time": 1_700_000_000_000},
        is_snapshot=False,
    )
    derived = derive_leader_post_position_from_fill(event)
    conflicting_snapshot = LatestAccountPosition(
        account_state_id=1,
        role="LEADER",
        address=("0x" + "1" * 40).lower(),
        coin="URNM",
        dex="xyz",
        canonical_coin="xyz:URNM",
        side="FLAT",
        size=Decimal("0"),
        notional=Decimal("0"),
        active=True,
        last_update_at=datetime.fromtimestamp((event.time_ms + 1_000) / 1000, timezone.utc),
    )

    assert _should_use_fill_derived_position(conflicting_snapshot, event, derived) is True


def test_snapshot_updated_after_fill_is_ignored_when_fill_derivation_is_high_confidence() -> None:
    event = build_fill_event(
        "0x" + "1" * 40,
        {"coin": "xyz:URNM", "px": "65.9", "sz": "200", "side": "B", "startPosition": "0", "time": 1_700_000_000_000},
        is_snapshot=False,
    )
    derived = derive_leader_post_position_from_fill(event)
    matching_snapshot = LatestAccountPosition(
        account_state_id=1,
        role="LEADER",
        address=("0x" + "1" * 40).lower(),
        coin="URNM",
        dex="xyz",
        canonical_coin="xyz:URNM",
        side="LONG",
        size=Decimal("200"),
        notional=Decimal("13180"),
        active=True,
        last_update_at=datetime.fromtimestamp((event.time_ms + 1_000) / 1000, timezone.utc),
    )

    assert _should_use_fill_derived_position(matching_snapshot, event, derived) is True


def test_fill_derivation_unknown_is_not_high_confidence() -> None:
    event = build_fill_event(
        "0x" + "1" * 40,
        {"coin": "xyz:URNM", "px": "65.9", "sz": "200", "side": "B", "time": 1_700_000_000_000},
        is_snapshot=False,
    )

    derived = derive_leader_post_position_from_fill(event)

    assert derived.confidence == "UNKNOWN"
    assert derived.reason == "FILL_POSITION_DERIVATION_UNKNOWN"


def allocation_record(
    *,
    qty: str = "0.42",
    notional: str = "11.07666",
    status: str = "OPEN",
    pending_reason: str | None = None,
) -> LeaderPositionAllocationRecord:
    return LeaderPositionAllocationRecord(
        leader_id=1,
        leader_address=("0x" + "1" * 40).lower(),
        hyperliquid_coin="USAR",
        dex="xyz",
        canonical_coin="xyz:USAR",
        execution_venue="HYPERLIQUID",
        venue_symbol="xyz:USAR",
        position_side="LONG",
        target_notional=Decimal(notional),
        allocated_notional=Decimal(notional),
        allocated_qty=Decimal(qty),
        avg_entry_price=Decimal("26.373"),
        copy_multiplier=Decimal("0.1"),
        status=status,
        pending_reduce_reason=(
            pending_reason
            if pending_reason is not None
            else PENDING_OPEN_REASON
            if status == PENDING_OPEN_STATUS
            else None
        ),
    )


def test_lifecycle_gate_without_allocation_only_allows_open_from_flat() -> None:
    reduce_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_reduce=True, start_position=Decimal("200"))
    increase_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=True, start_position=Decimal("200"))
    open_fill = SimpleNamespace(confidence="HIGH", is_open=True, is_reduce=False, start_position=Decimal("0"))
    nonflat_open_fill = SimpleNamespace(confidence="HIGH", is_open=True, is_reduce=False, start_position=Decimal("200"))
    unknown_fill = SimpleNamespace(confidence="UNKNOWN", is_open=False, start_position=None)

    assert _ignore_without_allocation_reason(None, reduce_fill) is not None
    assert _ignore_without_allocation_reason(None, increase_fill) is not None
    assert _ignore_without_allocation_reason(None, unknown_fill) is not None
    assert _ignore_without_allocation_reason(None, nonflat_open_fill) is not None
    assert _ignore_without_allocation_reason(None, None) is not None
    assert _ignore_without_allocation_reason(None, open_fill) is None
    assert _ignore_without_allocation_reason(allocation_record(), reduce_fill) is None


def test_released_market_owner_only_allows_new_open_not_other_leader_old_reduces() -> None:
    closed_owner = allocation_record(qty="0", notional="0", status="CLOSED")
    closed_owner.leader_id = 1
    closed_owner.leader_address = ("0x" + "1" * 40).lower()
    closed_owner.canonical_coin = "xyz:USAR"
    closed_owner.dex = "xyz"

    assert not _allocation_market_owner_active(closed_owner)
    assert _market_owner_blocker(None, leader=leader(address="0x" + "2" * 40, id=2), current_allocation=None) is None

    old_lifecycle_reduce_fill = SimpleNamespace(
        confidence="HIGH",
        is_open=False,
        is_increase=False,
        is_reduce=True,
        is_close=False,
        is_flip=False,
        start_position=Decimal("200"),
    )
    old_lifecycle_close_fill = SimpleNamespace(
        confidence="HIGH",
        is_open=False,
        is_increase=False,
        is_reduce=False,
        is_close=True,
        is_flip=False,
        start_position=Decimal("125"),
    )

    assert "IGNORED_OLD_LIFECYCLE" in (_ignore_without_allocation_reason(None, old_lifecycle_reduce_fill) or "")
    assert "IGNORED_OLD_LIFECYCLE" in (_ignore_without_allocation_reason(None, old_lifecycle_close_fill) or "")

    reduce_plan = plan_leader_allocation_transition(
        leader_id=2,
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:USAR",
        leader_side=PositionSide.LONG,
        leader_position_notional=Decimal("1000"),
        leader_position_size=Decimal("100"),
        leader_account_value_used=Decimal("100000"),
        follower_account_value_used=Decimal("20000"),
        copy_multiplier=Decimal("1"),
        current_allocation=None,
        leader_previous_position_size=Decimal("200"),
        leader_fill_is_reduce_or_close=True,
    )
    close_plan = plan_leader_allocation_transition(
        leader_id=2,
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:USAR",
        leader_side=PositionSide.FLAT,
        leader_position_notional=Decimal("0"),
        leader_position_size=Decimal("0"),
        leader_account_value_used=Decimal("100000"),
        follower_account_value_used=Decimal("20000"),
        copy_multiplier=Decimal("1"),
        current_allocation=None,
        leader_previous_position_size=Decimal("125"),
        leader_fill_is_reduce_or_close=True,
    )

    assert reduce_plan.action == AllocationTransitionAction.NOOP
    assert reduce_plan.delta_notional == Decimal("0")
    assert "cannot open or increase" in reduce_plan.reason
    assert close_plan.action == AllocationTransitionAction.NOOP
    assert close_plan.delta_notional == Decimal("0")


def test_pending_intent_overlay_preserves_pending_reduce_without_losing_residual() -> None:
    ledger = PendingIntentLedger()
    allocation = allocation_record(qty="0.272", notional="54.4")
    allocation.id = 10
    pending_reduce = submit_barrier_order(order_id=640, action="REDUCE", reduce_only=True)
    pending_reduce.quantity = Decimal("0.117")
    pending_reduce.notional = Decimal("23.4")

    ledger.reserve(pending_reduce, allocation)

    assert ledger.overlay_allocation(allocation).allocated_qty == Decimal("0.15500000")


def test_final_close_submit_expands_to_remaining_allocation_after_prior_intent_resolves() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = allocation_record(qty="0.272", notional="54.4")
    allocation.id = 10
    order = submit_barrier_order(order_id=641, action="CLOSE", reduce_only=True)
    order.quantity = Decimal("0.155")
    order.notional = Decimal("31")
    order.delta_notional = Decimal("-31")
    order.estimated_price = Decimal("200")
    order.pre_trade_checklist = {
        "order_validator": {
            "warnings": [],
            "payload_masked": {"sz": "0.155", "quantity": "0.155"},
        }
    }
    db = AllocationGetSession(allocation)

    blocked = asyncio.run(
        engine._guard_reduce_submit_against_live_allocation(
            db,
            order,
            fill_event(coin="USAR", dex="xyz", side="A", start_position="1", direction="Close Long"),
        )
    )

    assert blocked is False
    assert order.quantity == Decimal("0.272")
    assert order.pre_trade_checklist["submit_reduce_allocation_guard"]["expanded"] is True
    assert any(
        getattr(item, "event_type", None) == "FINAL_CLOSE_SUBMIT_QTY_EXPANDED_TO_ALLOCATION"
        for item in db.added
    )


def test_final_close_submit_does_not_expand_over_prior_unresolved_reduce() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10
    prior = submit_barrier_order(order_id=643, action="REDUCE", reduce_only=True)
    prior.quantity = Decimal("0.4")
    prior.notional = Decimal("40")
    close = submit_barrier_order(order_id=644, action="CLOSE", reduce_only=True)
    close.quantity = Decimal("0.6")
    close.notional = Decimal("60")
    close.delta_notional = Decimal("-60")
    close.estimated_price = Decimal("100")
    close.pre_trade_checklist = {
        "order_validator": {
            "warnings": [],
            "payload_masked": {"sz": "0.6", "quantity": "0.6"},
        }
    }
    engine.pending_intents.reserve(prior, allocation)
    engine.pending_intents.reserve(close, allocation)
    db = AllocationGetSession(allocation)

    blocked = asyncio.run(
        engine._guard_reduce_submit_against_live_allocation(
            db,
            close,
            fill_event(coin="USAR", dex="xyz", side="A", start_position="1", direction="Close Long"),
        )
    )

    assert blocked is False
    assert close.quantity == Decimal("0.6")
    assert close.pre_trade_checklist["submit_parallel_reduce_guard"]["prior_unresolved_reduce_qty"] == "0.40000000"
    assert close.pre_trade_checklist["submit_parallel_reduce_guard"]["safe_remaining_allocation_qty"] == "0.60000000"
    assert not any(
        getattr(item, "event_type", None) == "FINAL_CLOSE_SUBMIT_QTY_EXPANDED_TO_ALLOCATION"
        for item in db.added
    )


def test_reduce_submit_clamps_to_remaining_after_prior_unresolved_reduce() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10
    prior = submit_barrier_order(order_id=645, action="REDUCE", reduce_only=True)
    prior.quantity = Decimal("0.7")
    prior.notional = Decimal("70")
    current = submit_barrier_order(order_id=646, action="REDUCE", reduce_only=True)
    current.quantity = Decimal("0.5")
    current.notional = Decimal("50")
    current.delta_notional = Decimal("-50")
    current.estimated_price = Decimal("100")
    engine.pending_intents.reserve(prior, allocation)
    engine.pending_intents.reserve(current, allocation)
    db = AllocationGetSession(allocation)

    blocked = asyncio.run(
        engine._guard_reduce_submit_against_live_allocation(
            db,
            current,
            fill_event(coin="USAR", dex="xyz", side="A", start_position="1", direction="Close Long"),
        )
    )

    assert blocked is False
    assert current.quantity == Decimal("0.30000000")
    assert current.notional == Decimal("30.00000000")
    assert any(
        getattr(item, "event_type", None) == "REDUCE_SUBMIT_QTY_CLAMPED_TO_ALLOCATION"
        for item in db.added
    )


def test_partial_reduce_submit_is_never_expanded() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = allocation_record(qty="0.272", notional="54.4")
    allocation.id = 10
    order = submit_barrier_order(order_id=642, action="REDUCE", reduce_only=True)
    order.quantity = Decimal("0.155")
    order.notional = Decimal("31")
    db = AllocationGetSession(allocation)

    blocked = asyncio.run(
        engine._guard_reduce_submit_against_live_allocation(
            db,
            order,
            fill_event(coin="USAR", dex="xyz", side="A", start_position="2", direction="Close Long"),
        )
    )

    assert blocked is False
    assert order.quantity == Decimal("0.155")
    assert db.added == []


def test_real_fill_lifecycle_without_allocation_only_copies_new_open_from_flat() -> None:
    leader_addr = "0x" + "1" * 40
    fills = {
        "open_from_flat": {"coin": "xyz:URNM", "px": "65.9", "sz": "200", "side": "B", "startPosition": "0", "dir": "Open Long", "time": 1},
        "add_existing_long": {"coin": "xyz:URNM", "px": "65.9", "sz": "10", "side": "B", "startPosition": "200", "dir": "Open Long", "time": 2},
        "reduce_existing_long": {"coin": "xyz:URNM", "px": "65.9", "sz": "10", "side": "A", "startPosition": "200", "dir": "Close Long", "time": 3},
        "close_existing_long": {"coin": "xyz:URNM", "px": "65.9", "sz": "200", "side": "A", "startPosition": "200", "dir": "Close Long", "time": 4},
        "flip_existing_short": {"coin": "xyz:BIRD", "px": "6", "sz": "15", "side": "B", "startPosition": "-10", "dir": "Close Short", "time": 5},
    }
    decisions = {
        name: _ignore_without_allocation_reason(
            None,
            derive_leader_post_position_from_fill(build_fill_event(leader_addr, fill, is_snapshot=False)),
        )
        for name, fill in fills.items()
    }

    assert decisions["open_from_flat"] is None
    assert decisions["add_existing_long"] is not None
    assert decisions["reduce_existing_long"] is not None
    assert decisions["close_existing_long"] is not None
    assert decisions["flip_existing_short"] is not None


def test_same_batch_close_fills_coalesce_to_final_close() -> None:
    leader_addr = "0x" + "1" * 40
    events = [
        build_fill_event(
            leader_addr,
            {"coin": "xyz:GME", "px": "25.7", "sz": "233.18", "side": "A", "startPosition": "497.99", "dir": "Close Long", "time": 1},
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {"coin": "xyz:GME", "px": "25.7", "sz": "233.18", "side": "A", "startPosition": "264.81", "dir": "Close Long", "time": 2},
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {"coin": "xyz:GME", "px": "25.7", "sz": "31.63", "side": "A", "startPosition": "31.63", "dir": "Close Long", "time": 3},
            is_snapshot=False,
        ),
    ]

    selected, skipped = _coalesce_same_batch_fills(events)

    assert selected == [events[-1]]
    assert skipped == events[:2]


def test_same_batch_open_from_flat_keeps_first_and_final_fill() -> None:
    leader_addr = "0x" + "1" * 40
    events = [
        build_fill_event(
            leader_addr,
            {"coin": "cash:CAR", "px": "183", "sz": "0.27", "side": "B", "startPosition": "0", "dir": "Open Long", "time": 1},
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {"coin": "cash:CAR", "px": "183", "sz": "0.27", "side": "B", "startPosition": "0.27", "dir": "Open Long", "time": 2},
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {"coin": "cash:CAR", "px": "183", "sz": "0.27", "side": "B", "startPosition": "0.54", "dir": "Open Long", "time": 3},
            is_snapshot=False,
        ),
    ]

    selected, skipped = _coalesce_same_batch_fills(events)

    assert [event.source_fill_id for event in selected] == [
        events[0].source_fill_id,
        events[-1].source_fill_id,
    ]
    assert skipped == [events[1]]
    assert _fill_notional_for_sizing(selected[0]) == Decimal("49.41")
    assert _fill_notional_for_sizing(selected[1]) == Decimal("98.82")


def test_same_order_fragments_aggregate_to_single_open_event() -> None:
    leader_addr = "0x" + "1" * 40
    base = {
        "coin": "ETH",
        "side": "A",
        "time": 1,
        "oid": 123,
        "hash": "0xabc",
        "dir": "Open Short",
    }
    events = [
        build_fill_event(leader_addr, {**base, "tid": 1, "px": "100", "sz": "10", "startPosition": "0"}, is_snapshot=False),
        build_fill_event(leader_addr, {**base, "tid": 2, "px": "101", "sz": "15", "startPosition": "-10"}, is_snapshot=False),
        build_fill_event(leader_addr, {**base, "tid": 3, "px": "102", "sz": "25", "startPosition": "-25"}, is_snapshot=False),
    ]

    selected, skipped = _coalesce_same_batch_fills(events)
    implied = derive_leader_post_position_from_fill(selected[0])

    assert len(selected) == 1
    assert skipped == events[1:]
    assert selected[0].source_fill_id == events[0].source_fill_id
    assert selected[0].size == Decimal("50")
    assert selected[0].price == Decimal("101.3")
    assert _fill_notional_for_sizing(selected[0]) == Decimal("5065")
    assert implied.is_open
    assert implied.side_after.value == "SHORT"
    assert implied.size_after == Decimal("50")


def test_same_order_duplicate_fragments_are_not_double_counted() -> None:
    leader_addr = "0x" + "1" * 40
    first = {
        "coin": "ETH",
        "side": "A",
        "time": 1,
        "oid": 123,
        "hash": "0xabc",
        "tid": 1,
        "px": "100",
        "sz": "10",
        "startPosition": "0",
        "dir": "Open Short",
    }
    second = {
        "coin": "ETH",
        "side": "A",
        "time": 1,
        "oid": 123,
        "hash": "0xabc",
        "tid": 2,
        "px": "101",
        "sz": "15",
        "startPosition": "-10",
        "dir": "Open Short",
    }
    events = [
        build_fill_event(leader_addr, first, is_snapshot=False),
        build_fill_event(leader_addr, first, is_snapshot=False),
        build_fill_event(leader_addr, second, is_snapshot=False),
    ]

    selected, skipped = _coalesce_same_batch_fills(events)

    assert len(selected) == 1
    assert skipped == events[1:]
    assert selected[0].size == Decimal("25")
    assert _fill_notional_for_sizing(selected[0]) == Decimal("2515")


def test_same_order_close_fragments_aggregate_to_single_close_event() -> None:
    leader_addr = "0x" + "1" * 40
    base = {
        "coin": "ETH",
        "side": "A",
        "time": 1,
        "oid": 123,
        "hash": "0xabc",
        "dir": "Close Long",
    }
    events = [
        build_fill_event(leader_addr, {**base, "tid": 1, "px": "100", "sz": "20", "startPosition": "50"}, is_snapshot=False),
        build_fill_event(leader_addr, {**base, "tid": 2, "px": "101", "sz": "30", "startPosition": "30"}, is_snapshot=False),
    ]

    selected, skipped = _coalesce_same_batch_fills(events)
    implied = derive_leader_post_position_from_fill(selected[0])

    assert len(selected) == 1
    assert skipped == [events[1]]
    assert selected[0].source_fill_id == events[0].source_fill_id
    assert selected[0].size == Decimal("50")
    assert _fill_notional_for_sizing(selected[0]) == Decimal("5030")
    assert implied.is_close
    assert implied.side_after.value == "FLAT"


def test_same_order_fragments_with_different_hashes_still_aggregate_by_oid() -> None:
    leader_addr = "0x" + "1" * 40
    events = [
        build_fill_event(
            leader_addr,
            {
                "coin": "ETH",
                "side": "B",
                "time": 1,
                "oid": 123,
                "hash": "0xaaa",
                "tid": 1,
                "px": "100",
                "sz": "20",
                "startPosition": "-50",
                "dir": "Close Short",
            },
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {
                "coin": "ETH",
                "side": "B",
                "time": 2,
                "oid": 123,
                "hash": "0xbbb",
                "tid": 2,
                "px": "101",
                "sz": "30",
                "startPosition": "-30",
                "dir": "Close Short",
            },
            is_snapshot=False,
        ),
    ]

    selected, skipped = _coalesce_same_batch_fills(events)
    implied = derive_leader_post_position_from_fill(selected[0])

    assert len(selected) == 1
    assert skipped == [events[1]]
    assert selected[0].size == Decimal("50")
    assert _fill_notional_for_sizing(selected[0]) == Decimal("5030")
    assert implied.is_close


def test_same_batch_close_then_reopen_keeps_lifecycle_boundary() -> None:
    leader_addr = "0x" + "1" * 40
    base = {"coin": "xyz:GME", "side": "B", "time": 1, "oid": 123, "hash": "0xabc"}
    close_short = build_fill_event(
        leader_addr,
        {**base, "tid": 1, "px": "28.8", "sz": "10", "startPosition": "-10", "dir": "Close Short"},
        is_snapshot=False,
    )
    open_long = build_fill_event(
        leader_addr,
        {**base, "tid": 2, "px": "28.8", "sz": "5", "startPosition": "0", "dir": "Open Long"},
        is_snapshot=False,
    )

    selected, skipped = _coalesce_same_batch_fills([close_short, open_long])

    assert selected == [close_short, open_long]
    assert skipped == []


def test_queued_contiguous_partial_reduces_coalesce_to_one_submit_event() -> None:
    leader_addr = "0x" + "1" * 40
    events = [
        build_fill_event(
            leader_addr,
            {"coin": "HYPE", "px": "43", "sz": "100", "side": "A", "startPosition": "1000", "dir": "Close Long", "time": 1},
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {"coin": "HYPE", "px": "44", "sz": "50", "side": "A", "startPosition": "900", "dir": "Close Long", "time": 2},
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {"coin": "HYPE", "px": "45", "sz": "150", "side": "A", "startPosition": "850", "dir": "Close Long", "time": 3},
            is_snapshot=False,
        ),
    ]

    selected, skipped = _coalesce_queued_lifecycle_fills(events)
    implied = derive_leader_post_position_from_fill(selected[0])

    assert len(selected) == 1
    assert skipped == events[1:]
    assert selected[0].source_fill_id == events[0].source_fill_id
    assert selected[0].size == Decimal("300")
    assert selected[0].price == Decimal("44.16666666666666666666666667")
    assert _fill_notional_for_sizing(selected[0]) == Decimal("13250")
    assert implied.is_reduce
    assert not implied.is_close
    assert implied.size_after == Decimal("700")


def test_queued_reduce_coalescing_does_not_cross_close_boundary() -> None:
    leader_addr = "0x" + "1" * 40
    reduce_fill = build_fill_event(
        leader_addr,
        {"coin": "HYPE", "px": "43", "sz": "100", "side": "A", "startPosition": "1000", "dir": "Close Long", "time": 1},
        is_snapshot=False,
    )
    close_fill = build_fill_event(
        leader_addr,
        {"coin": "HYPE", "px": "43", "sz": "900", "side": "A", "startPosition": "900", "dir": "Close Long", "time": 2},
        is_snapshot=False,
    )

    selected, skipped = _coalesce_queued_lifecycle_fills([reduce_fill, close_fill])

    assert selected == [reduce_fill, close_fill]
    assert skipped == []


def test_queued_reduce_coalescing_requires_contiguous_start_position() -> None:
    leader_addr = "0x" + "1" * 40
    first = build_fill_event(
        leader_addr,
        {"coin": "HYPE", "px": "43", "sz": "100", "side": "A", "startPosition": "1000", "dir": "Close Long", "time": 1},
        is_snapshot=False,
    )
    gap = build_fill_event(
        leader_addr,
        {"coin": "HYPE", "px": "43", "sz": "50", "side": "A", "startPosition": "850", "dir": "Close Long", "time": 2},
        is_snapshot=False,
    )

    selected, skipped = _coalesce_queued_lifecycle_fills([first, gap])

    assert selected == [first, gap]
    assert skipped == []


def test_queued_contiguous_increases_coalesce_to_one_submit_event() -> None:
    leader_addr = "0x" + "1" * 40
    events = [
        build_fill_event(
            leader_addr,
            {"coin": "HYPE", "px": "43", "sz": "50", "side": "B", "startPosition": "100", "dir": "Open Long", "time": 1},
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {"coin": "HYPE", "px": "44", "sz": "25", "side": "B", "startPosition": "150", "dir": "Open Long", "time": 2},
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {"coin": "HYPE", "px": "45", "sz": "25", "side": "B", "startPosition": "175", "dir": "Open Long", "time": 3},
            is_snapshot=False,
        ),
    ]

    selected, skipped = _coalesce_queued_lifecycle_fills(events)
    implied = derive_leader_post_position_from_fill(selected[0])

    assert len(selected) == 1
    assert skipped == events[1:]
    assert selected[0].source_fill_id == events[0].source_fill_id
    assert selected[0].size == Decimal("100")
    assert selected[0].price == Decimal("43.75")
    assert _fill_notional_for_sizing(selected[0]) == Decimal("4375")
    assert implied.is_increase
    assert implied.size_after == Decimal("200")


def test_queued_open_from_flat_and_increases_coalesce_to_one_open_event() -> None:
    leader_addr = "0x" + "1" * 40
    events = [
        build_fill_event(
            leader_addr,
            {"coin": "HYPE", "px": "10", "sz": "100", "side": "B", "startPosition": "0", "dir": "Open Long", "time": 1},
            is_snapshot=False,
        ),
        build_fill_event(
            leader_addr,
            {"coin": "HYPE", "px": "12", "sz": "50", "side": "B", "startPosition": "100", "dir": "Open Long", "time": 2},
            is_snapshot=False,
        ),
    ]

    selected, skipped = _coalesce_queued_lifecycle_fills(events)
    implied = derive_leader_post_position_from_fill(selected[0])

    assert len(selected) == 1
    assert skipped == [events[1]]
    assert selected[0].size == Decimal("150")
    assert selected[0].price == Decimal("10.66666666666666666666666667")
    assert _fill_notional_for_sizing(selected[0]) == Decimal("1600")
    assert implied.is_open
    assert implied.size_after == Decimal("150")


def test_queued_add_coalescing_does_not_cross_reduce_boundary() -> None:
    leader_addr = "0x" + "1" * 40
    increase = build_fill_event(
        leader_addr,
        {"coin": "HYPE", "px": "43", "sz": "50", "side": "B", "startPosition": "100", "dir": "Open Long", "time": 1},
        is_snapshot=False,
    )
    reduce_fill = build_fill_event(
        leader_addr,
        {"coin": "HYPE", "px": "43", "sz": "20", "side": "A", "startPosition": "150", "dir": "Close Long", "time": 2},
        is_snapshot=False,
    )

    selected, skipped = _coalesce_queued_lifecycle_fills([increase, reduce_fill])

    assert selected == [increase, reduce_fill]
    assert skipped == []


def test_queued_add_coalescing_requires_contiguous_start_position() -> None:
    leader_addr = "0x" + "1" * 40
    first = build_fill_event(
        leader_addr,
        {"coin": "HYPE", "px": "43", "sz": "50", "side": "B", "startPosition": "100", "dir": "Open Long", "time": 1},
        is_snapshot=False,
    )
    gap = build_fill_event(
        leader_addr,
        {"coin": "HYPE", "px": "43", "sz": "25", "side": "B", "startPosition": "175", "dir": "Open Long", "time": 2},
        is_snapshot=False,
    )

    selected, skipped = _coalesce_queued_lifecycle_fills([first, gap])

    assert selected == [first, gap]
    assert skipped == []


def test_partial_reduce_ignores_live_flat_override_on_hot_path() -> None:
    event = fill_event(side="B", start_position="-200", direction="Close Short")
    implied = derive_leader_post_position_from_fill(event)

    adjusted = _live_adjusted_fill_implied_position(
        fill=event,
        implied=implied,
        live_position=None,
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert implied.is_reduce
    assert adjusted is None


def test_partial_reduce_does_not_require_synchronous_live_refresh() -> None:
    event = fill_event(side="B", start_position="-200", direction="Close Short")
    implied = derive_leader_post_position_from_fill(event)

    assert implied.is_reduce
    assert not implied.is_close
    assert _should_refresh_live_leader_position_for_reduce(implied) is False


def test_fill_confirmed_exact_close_skips_synchronous_live_refresh_but_flip_keeps_it() -> None:
    close_event = fill_event(side="B", start_position="-1", direction="Close Short")
    close_implied = derive_leader_post_position_from_fill(close_event)
    flip_event = fill_event(side="B", start_position="-0.5", direction="Close Short")
    flip_implied = derive_leader_post_position_from_fill(flip_event)

    assert close_implied.is_close
    assert close_implied.confidence == "HIGH"
    assert close_implied.size_after == 0
    assert _should_refresh_live_leader_position_for_reduce(close_implied) is False
    assert flip_implied.is_flip
    assert _should_refresh_live_leader_position_for_reduce(flip_implied) is True


def test_stale_live_reduce_position_that_is_behind_fill_is_ignored() -> None:
    event = fill_event(side="B", start_position="-200", direction="Close Short")
    implied = derive_leader_post_position_from_fill(event)
    stale = LiveLeaderPositionSnapshot(
        side=PositionSide.SHORT,
        size=Decimal("210"),
        signed_size=Decimal("-210"),
        notional=Decimal("-21000"),
        entry_px=Decimal("100"),
        mark_px=Decimal("100"),
        raw_payload_masked={},
        last_update_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    adjusted = _live_adjusted_fill_implied_position(
        fill=event,
        implied=implied,
        live_position=stale,
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert adjusted is None


def test_pending_open_lifecycle_allows_later_add_after_initial_open_too_small() -> None:
    pending = allocation_record(qty="0", notional="0", status=PENDING_OPEN_STATUS)
    add_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=True, start_position=Decimal("-9.95"))

    assert _ignore_without_allocation_reason(pending, add_fill) is None


def test_non_minimum_order_pending_open_cannot_activate_later_add() -> None:
    pending = allocation_record(
        qty="0",
        notional="0",
        status=PENDING_OPEN_STATUS,
        pending_reason="CROSS_MARGIN_NOT_SUPPORTED",
    )
    add_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=True, start_position=Decimal("-500"))

    assert _ignore_without_allocation_reason(pending, add_fill) is not None
    assert "only below-min" in (
        _pending_open_activation_block_reason(pending, add_fill, SimpleNamespace(action=AllocationTransitionAction.OPEN))
        or ""
    )


def test_stale_zero_allocation_is_closed_before_lifecycle_reuse() -> None:
    allocation = allocation_record(qty="0", notional="0", status="OPEN")

    reason = _stale_zero_allocation_reason(allocation)
    assert reason is not None

    _close_zero_allocation_lifecycle(allocation, reason=reason, now=datetime(2026, 5, 22, tzinfo=timezone.utc))

    assert allocation.status == "CLOSED"
    assert allocation.target_notional == Decimal("0")
    assert allocation.allocated_qty == Decimal("0")
    assert "zero-fill OPEN allocation" in allocation.pending_reduce_reason


def test_pending_open_can_only_activate_on_open_or_increase_fill() -> None:
    pending = allocation_record(qty="0", notional="0", status=PENDING_OPEN_STATUS)
    open_plan = SimpleNamespace(action=AllocationTransitionAction.OPEN)
    add_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=True, is_reduce=False, is_close=False)
    reduce_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=False, is_reduce=True, is_close=False)
    close_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=False, is_reduce=False, is_close=True)

    assert _pending_open_activation_block_reason(pending, add_fill, open_plan) is None
    assert "reduce/close fill cannot activate pending open" in (
        _pending_open_activation_block_reason(pending, reduce_fill, open_plan) or ""
    )
    assert "reduce/close fill cannot activate pending open" in (
        _pending_open_activation_block_reason(pending, close_fill, open_plan) or ""
    )


def test_fill_direction_guard_blocks_reduce_or_close_fill_from_opening() -> None:
    reduce_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=False, is_reduce=True, is_close=False, is_flip=False)
    close_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=False, is_reduce=False, is_close=True, is_flip=False)
    open_plan = SimpleNamespace(action=AllocationTransitionAction.OPEN)
    increase_plan = SimpleNamespace(action=AllocationTransitionAction.INCREASE)
    reduce_plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE)
    close_plan = SimpleNamespace(action=AllocationTransitionAction.CLOSE)

    assert "reduce/close fill cannot create" in (
        _fill_direction_action_block_reason(reduce_fill, open_plan) or ""
    )
    assert "reduce/close fill cannot create" in (
        _fill_direction_action_block_reason(close_fill, increase_plan) or ""
    )
    assert _fill_direction_action_block_reason(reduce_fill, reduce_plan) is None
    assert _fill_direction_action_block_reason(close_fill, close_plan) is None


def test_fill_direction_guard_blocks_open_or_increase_fill_from_reducing() -> None:
    open_fill = SimpleNamespace(confidence="HIGH", is_open=True, is_increase=False, is_reduce=False, is_close=False, is_flip=False)
    increase_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=True, is_reduce=False, is_close=False, is_flip=False)
    reduce_plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE)
    close_plan = SimpleNamespace(action=AllocationTransitionAction.CLOSE)
    open_plan = SimpleNamespace(action=AllocationTransitionAction.OPEN)
    increase_plan = SimpleNamespace(action=AllocationTransitionAction.INCREASE)

    assert "open/increase fill cannot reduce" in (
        _fill_direction_action_block_reason(open_fill, reduce_plan) or ""
    )
    assert "open/increase fill cannot reduce" in (
        _fill_direction_action_block_reason(increase_fill, close_plan) or ""
    )
    assert _fill_direction_action_block_reason(open_fill, open_plan) is None
    assert _fill_direction_action_block_reason(increase_fill, increase_plan) is None


def test_fill_direction_guard_blocks_flip_fill_from_direct_second_leg_open() -> None:
    flip_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=False, is_reduce=False, is_close=False, is_flip=True)
    open_second = SimpleNamespace(action=AllocationTransitionAction.FLIP_OPEN_SECOND)
    close_first = SimpleNamespace(action=AllocationTransitionAction.FLIP_CLOSE_FIRST)

    assert "flip fill cannot directly open" in (
        _fill_direction_action_block_reason(flip_fill, open_second) or ""
    )
    assert _fill_direction_action_block_reason(flip_fill, close_first) is None


def test_missed_reduce_catchup_allows_open_fill_to_reduce_stale_allocation() -> None:
    fill = SimpleNamespace(
        confidence="HIGH",
        is_open=False,
        is_increase=True,
        is_reduce=False,
        is_close=False,
        is_flip=False,
        start_position=Decimal("1000"),
    )
    plan = SimpleNamespace(
        action=AllocationTransitionAction.REDUCE,
        current_allocation_notional=Decimal("800"),
        target_notional=Decimal("500"),
    )
    allocation = SimpleNamespace(
        last_leader_position_size=Decimal("2000"),
        allocated_qty=Decimal("11"),
    )

    allowed = _missed_reduce_catchup_allows_direction_mismatch(
        fill_implied_position=fill,
        transition_plan=plan,
        planning_allocation=allocation,
    )

    assert allowed is True
    assert _fill_direction_action_block_reason(
        fill,
        plan,
        allow_missed_reduce_catchup=allowed,
    ) is None


def test_missed_reduce_catchup_requires_checkpoint_gap() -> None:
    fill = SimpleNamespace(
        confidence="HIGH",
        is_open=False,
        is_increase=True,
        is_reduce=False,
        is_close=False,
        is_flip=False,
        start_position=Decimal("2000"),
    )
    plan = SimpleNamespace(
        action=AllocationTransitionAction.REDUCE,
        current_allocation_notional=Decimal("800"),
        target_notional=Decimal("500"),
    )
    allocation = SimpleNamespace(
        last_leader_position_size=Decimal("2000"),
        allocated_qty=Decimal("11"),
    )

    assert _missed_reduce_catchup_allows_direction_mismatch(
        fill_implied_position=fill,
        transition_plan=plan,
        planning_allocation=allocation,
    ) is False


def test_order_intent_guard_blocks_side_and_reduce_only_mismatch_without_io() -> None:
    open_fill = SimpleNamespace(confidence="HIGH", is_open=True, is_increase=False, is_reduce=False, is_close=False, is_flip=False)
    valid_open = SimpleNamespace(order_action="OPEN", side="BUY", position_side="LONG", reduce_only=False)
    bad_side = SimpleNamespace(order_action="OPEN", side="SELL", position_side="LONG", reduce_only=False)
    bad_reduce_flag = SimpleNamespace(order_action="REDUCE", side="SELL", position_side="LONG", reduce_only=False)

    assert _order_intent_blockers(valid_open, open_fill, reduce_only=False) == []
    assert any("side/action mismatch" in item for item in _order_intent_blockers(bad_side, open_fill, reduce_only=False))
    assert any("reduce/close must be reduce_only" in item for item in _order_intent_blockers(bad_reduce_flag, open_fill, reduce_only=False))


def test_order_intent_guard_blocks_close_fill_from_open_order_without_io() -> None:
    close_fill = SimpleNamespace(confidence="HIGH", is_open=False, is_increase=False, is_reduce=False, is_close=True, is_flip=False)
    order = SimpleNamespace(order_action="OPEN", side="SELL", position_side="SHORT", reduce_only=False)

    assert any("reduce/close fill cannot create" in item for item in _order_intent_blockers(order, close_fill, reduce_only=False))


def test_pending_open_lifecycle_closes_if_leader_flats_before_min_order() -> None:
    pending = allocation_record(qty="0", notional="0", status=PENDING_OPEN_STATUS)
    transition_plan = SimpleNamespace(action=AllocationTransitionAction.NOOP, target_notional=Decimal("0"))

    assert _pending_open_allocation_flat(pending, transition_plan)


def test_below_min_new_open_is_pending_open_only_without_other_blockers() -> None:
    validator_result = validate_hyperliquid_order_params(
        dex="xyz",
        canonical_coin="xyz:GME",
        asset_id=41,
        action="OPEN",
        side="SELL",
        target_delta_notional=Decimal("1.45"),
        raw_size=Decimal("0.049"),
        raw_price=Decimal("29.9"),
        market_meta={"name": "xyz:GME", "asset_id": 41, "index": 41, "szDecimals": 2, "maxLeverage": 10},
        order_policy={
            "cloid": "0x" + "2" * 32,
            "is_buy": False,
            "reduce_only": False,
            "tif": "Ioc",
            "order_type": {"limit": {"tif": "Ioc"}},
            "slippage_bps": 0,
            "price_fresh": True,
            "min_order_value": "10",
            "effective_leverage": 10,
        },
    )
    transition_plan = SimpleNamespace(action=AllocationTransitionAction.OPEN)

    assert _is_pending_open_block(
        reduce_only=False,
        transition_plan=transition_plan,
        validator_result=validator_result,
        blockers=["target/delta notional is below Hyperliquid minimum order value (target=1.45, estimated=1.16, min=10)"],
    )
    assert not _is_pending_open_block(
        reduce_only=False,
        transition_plan=transition_plan,
        validator_result=validator_result,
        blockers=[
            "kill switch active",
            "target/delta notional is below Hyperliquid minimum order value (target=1.45, estimated=1.16, min=10)",
        ],
    )


def test_below_min_open_state_target_does_not_accumulate_skipped_notional() -> None:
    transition_plan = SimpleNamespace(
        action=AllocationTransitionAction.INCREASE,
        current_allocation_notional=Decimal("100"),
    )

    assert _allocation_state_target_notional(
        target_notional=Decimal("105"),
        transition_plan=transition_plan,
        pending_open_reason="BELOW_MIN_ORDER_VALUE_PENDING_OPEN",
        reduce_only=False,
    ) == Decimal("100")


def test_below_min_add_on_active_allocation_does_not_mark_pending_open() -> None:
    active = allocation_record(qty="0.976", notional="633.33311", status=PENDING_OPEN_STATUS)
    unfilled = allocation_record(qty="0", notional="0", status=PENDING_OPEN_STATUS)

    assert not _pending_open_should_remain_pending(active)
    assert _pending_open_should_remain_pending(unfilled)


def test_below_min_active_increase_fast_forward_updates_checkpoint_only() -> None:
    allocation = allocation_record(qty="20", notional="2000", status="OPEN")
    transition_plan = SimpleNamespace(
        action=AllocationTransitionAction.INCREASE,
        delta_notional=Decimal("0.44"),
    )

    assert _should_fast_forward_below_min_active_increase(
        allocation=allocation,
        transition_plan=transition_plan,
        reduce_only=False,
        pending_open=True,
        pending_open_activation_reason=None,
        deferred_reduce=False,
    )

    now = datetime(2026, 7, 10, tzinfo=timezone.utc)
    _fast_forward_below_min_active_increase(
        allocation,
        leader_account_value=Decimal("50000"),
        leader_position_notional=Decimal("50110"),
        leader_position_size=Decimal("501.1"),
        copy_multiplier=Decimal("1"),
        source_fill_id="small-add-fill",
        now=now,
    )

    assert allocation.status == "OPEN"
    assert allocation.allocated_notional == Decimal("2000")
    assert allocation.target_notional == Decimal("2000")
    assert allocation.last_leader_position_size == Decimal("501.1")
    assert allocation.last_source_fill_id == "small-add-fill"
    assert allocation.last_reconcile_at == now


def test_below_min_active_increase_fast_forward_promotes_active_pending_open_to_open() -> None:
    allocation = allocation_record(qty="20", notional="2000", status=PENDING_OPEN_STATUS)

    _fast_forward_below_min_active_increase(
        allocation,
        leader_account_value=Decimal("50000"),
        leader_position_notional=Decimal("50110"),
        leader_position_size=Decimal("501.1"),
        copy_multiplier=Decimal("1"),
        source_fill_id="small-add-fill",
        now=datetime(2026, 7, 10, tzinfo=timezone.utc),
    )

    assert allocation.status == "OPEN"
    assert allocation.allocated_notional == Decimal("2000")
    assert allocation.target_notional == Decimal("2000")


def test_below_min_active_increase_fast_forward_does_not_bypass_pending_reduce() -> None:
    allocation = allocation_record(qty="20", notional="2000", status="OPEN")
    allocation.pending_reduce_qty = Decimal("0.1")
    transition_plan = SimpleNamespace(
        action=AllocationTransitionAction.INCREASE,
        delta_notional=Decimal("0.44"),
    )

    assert not _should_fast_forward_below_min_active_increase(
        allocation=allocation,
        transition_plan=transition_plan,
        reduce_only=False,
        pending_open=True,
        pending_open_activation_reason=None,
        deferred_reduce=False,
    )


def test_below_min_active_increase_fast_forward_only_allows_clean_active_increase() -> None:
    active = allocation_record(qty="20", notional="2000", status="OPEN")
    inactive = allocation_record(qty="0", notional="0", status=PENDING_OPEN_STATUS)
    increase_plan = SimpleNamespace(action=AllocationTransitionAction.INCREASE, delta_notional=Decimal("0.44"))
    reduce_plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE, delta_notional=Decimal("-0.44"))

    assert not _should_fast_forward_below_min_active_increase(
        allocation=inactive,
        transition_plan=increase_plan,
        reduce_only=False,
        pending_open=True,
        pending_open_activation_reason=None,
        deferred_reduce=False,
    )
    assert not _should_fast_forward_below_min_active_increase(
        allocation=active,
        transition_plan=reduce_plan,
        reduce_only=True,
        pending_open=False,
        pending_open_activation_reason=None,
        deferred_reduce=True,
    )
    assert not _should_fast_forward_below_min_active_increase(
        allocation=active,
        transition_plan=increase_plan,
        reduce_only=False,
        pending_open=True,
        pending_open_activation_reason="PENDING_OPEN_WAITING_FOR_OPEN_OR_INCREASE",
        deferred_reduce=False,
    )
    assert not _should_fast_forward_below_min_active_increase(
        allocation=active,
        transition_plan=increase_plan,
        reduce_only=False,
        pending_open=False,
        pending_open_activation_reason=None,
        deferred_reduce=False,
    )


def test_below_min_pending_open_lifecycle_fast_forward_updates_checkpoint_only() -> None:
    allocation = allocation_record(qty="0", notional="0", status=PENDING_OPEN_STATUS)
    transition_plan = SimpleNamespace(action=AllocationTransitionAction.OPEN, delta_notional=Decimal("6"))

    assert _should_fast_forward_below_min_pending_open_lifecycle(
        allocation=allocation,
        transition_plan=transition_plan,
        reduce_only=False,
        pending_open=True,
        pending_open_activation_reason=None,
        deferred_reduce=False,
    )

    now = datetime(2026, 7, 10, tzinfo=timezone.utc)
    _fast_forward_below_min_pending_open_lifecycle(
        allocation,
        leader_account_value=Decimal("50000"),
        leader_position_notional=Decimal("300"),
        leader_position_size=Decimal("3"),
        copy_multiplier=Decimal("1"),
        source_fill_id="pending-small-add",
        now=now,
    )

    assert allocation.status == PENDING_OPEN_STATUS
    assert allocation.allocated_notional == Decimal("0")
    assert allocation.allocated_qty == Decimal("0")
    assert allocation.target_notional == Decimal("0")
    assert allocation.last_leader_position_size == Decimal("3")
    assert allocation.pending_reduce_reason == PENDING_OPEN_REASON
    assert allocation.pending_reduce_source_fill_id == "pending-small-add"
    assert allocation.last_reconcile_at == now


def test_below_min_pending_open_lifecycle_fast_forward_does_not_create_first_lifecycle_or_reduce() -> None:
    inactive = allocation_record(qty="0", notional="0", status=PENDING_OPEN_STATUS)
    active = allocation_record(qty="1", notional="100", status=PENDING_OPEN_STATUS)
    open_plan = SimpleNamespace(action=AllocationTransitionAction.OPEN, delta_notional=Decimal("6"))
    reduce_plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE, delta_notional=Decimal("-6"))

    assert not _should_fast_forward_below_min_pending_open_lifecycle(
        allocation=None,
        transition_plan=open_plan,
        reduce_only=False,
        pending_open=True,
        pending_open_activation_reason=None,
        deferred_reduce=False,
    )
    assert not _should_fast_forward_below_min_pending_open_lifecycle(
        allocation=inactive,
        transition_plan=reduce_plan,
        reduce_only=True,
        pending_open=False,
        pending_open_activation_reason=None,
        deferred_reduce=True,
    )
    assert not _should_fast_forward_below_min_pending_open_lifecycle(
        allocation=inactive,
        transition_plan=open_plan,
        reduce_only=False,
        pending_open=True,
        pending_open_activation_reason="PENDING_OPEN_WAITING_FOR_OPEN_OR_INCREASE",
        deferred_reduce=False,
    )
    assert not _should_fast_forward_below_min_pending_open_lifecycle(
        allocation=active,
        transition_plan=open_plan,
        reduce_only=False,
        pending_open=True,
        pending_open_activation_reason=None,
        deferred_reduce=False,
    )


def test_non_pending_open_state_target_keeps_executable_target() -> None:
    transition_plan = SimpleNamespace(
        action=AllocationTransitionAction.INCREASE,
        current_allocation_notional=Decimal("100"),
    )

    assert _allocation_state_target_notional(
        target_notional=Decimal("111"),
        transition_plan=transition_plan,
        pending_open_reason=None,
        reduce_only=False,
    ) == Decimal("111")


def test_direction_guard_preserves_existing_allocation_state() -> None:
    allocation = allocation_record(qty="26.41", notional="1132.22311", status="BLOCKED")

    assert _direction_guard_preserves_allocation("FILL_DIRECTION_GUARD: leader reduce/close fill cannot create", allocation)

    _preserve_allocation_state_after_direction_guard(
        allocation,
        leader_account_value=Decimal("1472915.644043"),
        leader_position_notional=Decimal("658848.6"),
        leader_position_size=Decimal("15300"),
        copy_multiplier=Decimal("3"),
        source_fill_id="fill-reduce",
        now=datetime(2026, 5, 10, tzinfo=timezone.utc),
    )

    assert allocation.target_notional == Decimal("1132.22311")
    assert allocation.allocated_notional == Decimal("1132.22311")
    assert allocation.allocated_qty == Decimal("26.41")
    assert allocation.status == "OPEN"
    assert allocation.last_leader_position_size == Decimal("15300")
    assert allocation.last_source_fill_id == "fill-reduce"


def test_missing_account_value_new_open_creates_pending_lifecycle() -> None:
    open_fill = SimpleNamespace(confidence="HIGH", is_open=True, is_increase=False, start_position=Decimal("0"))
    transition_plan = SimpleNamespace(action=AllocationTransitionAction.BLOCK, reason="leader account value unavailable")

    assert _is_account_value_pending_open(
        allocation=None,
        fill_implied_position=open_fill,
        transition_plan=transition_plan,
        blockers=[
            "leader resolved account value unavailable",
            "follower resolved account value unavailable",
            "leader account value unavailable",
            "target/delta notional is below Hyperliquid minimum order value (target=0, estimated=0, min=10)",
        ],
    )


def test_missing_account_value_pending_lifecycle_rejects_hard_blockers() -> None:
    open_fill = SimpleNamespace(confidence="HIGH", is_open=True, is_increase=False, start_position=Decimal("0"))
    transition_plan = SimpleNamespace(action=AllocationTransitionAction.BLOCK, reason="leader account value unavailable")

    assert not _is_account_value_pending_open(
        allocation=None,
        fill_implied_position=open_fill,
        transition_plan=transition_plan,
        blockers=["coin not allowed for leader", "leader account value unavailable"],
    )


def test_account_value_payload_readiness_requires_positive_resolved_value() -> None:
    missing = None
    empty = {"account_value_used_for_sizing": None}
    usable = {
        "account_value_used_for_sizing": "598.20",
        "account_value_source": "SPOT_CLEARINGHOUSE_STATE",
        "account_abstraction_mode": "UNIFIED",
        "blockers": [],
    }

    assert _account_value_payload_needs_refresh(missing)
    assert _account_value_payload_needs_refresh(empty)
    assert not _account_value_payload_needs_refresh(usable)
    assert _account_value_payload_ready(usable, role="FOLLOWER") == (True, None)


def test_leader_dex_abstraction_payload_ready_requires_account_total_source() -> None:
    unsafe = {
        "account_value_used_for_sizing": "735.12",
        "account_value_source": "CLEARINGHOUSE_STATE",
        "account_abstraction_mode": "DEX_ABSTRACTION",
        "blockers": [],
    }
    safe = {
        "account_value_used_for_sizing": "34656.30",
        "account_value_source": "CURRENT_ACCOUNT_TOTAL",
        "account_abstraction_mode": "DEX_ABSTRACTION",
        "blockers": [],
    }

    assert _account_value_payload_ready(unsafe, role="LEADER")[0] is False
    assert _account_value_payload_ready(safe, role="LEADER") == (True, None)


def test_leader_dex_abstraction_clearinghouse_account_value_is_blocked() -> None:
    blockers = _leader_account_value_safety_blockers(
        {
            "account_abstraction_mode": "DEX_ABSTRACTION",
            "account_value_source": "CLEARINGHOUSE_STATE",
            "account_value_used_for_sizing": "735.120421",
        }
    )
    assert blockers
    assert "CURRENT_ACCOUNT_TOTAL" in blockers[0]


def test_leader_dex_abstraction_current_account_total_is_allowed() -> None:
    assert _leader_account_value_safety_blockers(
        {
            "account_abstraction_mode": "DEX_ABSTRACTION",
            "account_value_source": "CURRENT_ACCOUNT_TOTAL",
            "account_value_used_for_sizing": "33943.9856448971575",
        }
    ) == []


def test_risk_setting_result_from_row_can_warm_process_cache() -> None:
    row = SimpleNamespace(
        id=7,
        status="CONFIRMED",
        account_address="0x" + "5" * 40,
        dex="xyz",
        canonical_coin="xyz:GME",
        desired_margin_mode="CROSS",
        desired_leverage=10,
        market_max_leverage=10,
        effective_leverage=10,
        actual_margin_mode="CROSS",
        actual_leverage=10,
        asset_id=41,
        last_confirmed_at=datetime.now(timezone.utc),
    )
    result = _risk_setting_result_from_row(row)
    assert result.is_ok is True
    assert result.cache_used is True
    assert result.dex == "xyz"
    assert result.canonical_coin == "xyz:GME"
    assert result.effective_leverage == 10


def test_real_urnm_replay_sizing_uses_fill_implied_post_notional_not_snapshot_zero() -> None:
    event = build_fill_event(
        "0x" + "1" * 40,
        {
            "coin": "xyz:URNM",
            "px": "65.9",
            "sz": "200.0",
            "side": "B",
            "startPosition": "0.0",
            "dir": "Open Long",
            "time": 1777805629735,
        },
        is_snapshot=False,
    )
    derived = derive_leader_post_position_from_fill(event)

    target = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("19104.209018"),
        leader_position_notional=derived.notional_after_estimate,
        follower_account_value=Decimal("599.502228"),
        copy_multiplier=Decimal("0.1"),
    )

    assert derived.notional_after_estimate == Decimal("13180.00")
    assert target == Decimal("41.35967816")
    assert target >= Decimal("10")


def test_allowed_coins_null_allows_xyz() -> None:
    assert is_coin_allowed(leader(allowed_symbols=None), "xyz:HYUNDAI") is True


def test_allowed_coins_custom_matches_xyz_raw_and_canonical() -> None:
    assert is_coin_allowed(leader(allowed_symbols=["HYUNDAI"]), "xyz:HYUNDAI") is True
    assert is_coin_allowed(leader(allowed_symbols=["xyz:HYUNDAI"]), "xyz:HYUNDAI") is True


def test_price_cache_separates_default_and_xyz() -> None:
    cache = LowLatencyPriceCache(stale_ms=2_000)
    cache.set_price(dex="", coin="BTC", price="50000", source="test")
    cache.set_price(dex="xyz", coin="BTC", price="7", source="test")

    assert cache.get("BTC").price == Decimal("50000")
    assert cache.get("xyz:BTC").price == Decimal("7")


def test_price_cache_stale_detection_blocks_freshness() -> None:
    cache = LowLatencyPriceCache(stale_ms=1)
    cache.set_price(dex="", coin="BTC", price="50000", source="test")
    cache._prices["BTC"].updated_at = datetime.now(timezone.utc) - timedelta(milliseconds=5)

    assert cache.is_fresh("BTC") is False


def test_price_cache_replace_mids_prunes_removed_markets_for_same_dex() -> None:
    cache = LowLatencyPriceCache(stale_ms=2_000)
    cache.update_mids(dex="", mids={"ETH": "2200", "@311": "1"}, source="WEBSOCKET")
    cache._prices["@311"].updated_at = datetime.now(timezone.utc) - timedelta(milliseconds=5_000)

    assert cache.status_by_dex([""])[""]["fresh"] is False

    cache.update_mids(dex="", mids={"ETH": "2201"}, source="REST_POLL_FALLBACK", replace=True)

    assert cache.get("@311") is None
    assert cache.get("ETH").price == Decimal("2201")
    assert cache.status_by_dex([""])[""]["fresh"] is True


def test_price_cache_replace_mids_only_prunes_target_dex() -> None:
    cache = LowLatencyPriceCache(stale_ms=2_000)
    cache.update_mids(dex="", mids={"ETH": "2200", "@311": "1"}, source="WEBSOCKET")
    cache.update_mids(dex="xyz", mids={"GME": "30"}, source="WEBSOCKET")

    cache.update_mids(dex="", mids={"ETH": "2201"}, source="REST_POLL_FALLBACK", replace=True)

    assert cache.get("@311") is None
    assert cache.get("xyz:GME").price == Decimal("30")


def test_follower_position_qty_uses_active_latest_position() -> None:
    now = datetime.now(timezone.utc)
    state = SimpleNamespace(id=1, last_update_at=now, updated_at=now)
    position = SimpleNamespace(size=Decimal("-0.768"), mark_px_source="POSITION_MARK_PX", last_update_at=now, id=1)

    class PositionDb(FakeSession):
        async def scalar(self, stmt):
            self.statements.append(stmt)
            return state

        async def execute(self, stmt):
            self.statements.append(stmt)
            return FakeResult([position])

    db = PositionDb()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )

    qty, state_at = asyncio.run(
        engine._follower_position_qty_with_state_at(
            db,
            MarketKey(dex="", coin="BNB", canonical_coin="BNB", raw_coin="BNB", asset_id=7, venue_symbol="BNB"),
            PositionSide.SHORT,
        )
    )

    assert qty == Decimal("0.768")
    assert state_at == now
    sql = compiled_sql(db.statements[1])
    assert "latest_account_positions.active is true" in sql


def test_follower_position_qtys_prefer_account_snapshot_over_projection_duplicate() -> None:
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=("0x" + "5" * 40).lower(),
        dex="",
        last_update_at=datetime(2026, 7, 9, 8, 30, tzinfo=timezone.utc),
    )
    snapshot_position = LatestAccountPosition(
        id=20,
        account_state_id=1,
        role="FOLLOWER",
        address=("0x" + "5" * 40).lower(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("3"),
        notional=Decimal("300"),
        mark_px_source="POSITION_MARK_PX",
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 9, 8, 29, 59, tzinfo=timezone.utc),
    )
    projection_duplicate = LatestAccountPosition(
        id=21,
        account_state_id=1,
        role="FOLLOWER",
        address=("0x" + "5" * 40).lower(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("9"),
        notional=Decimal("900"),
        mark_px_source="LOCAL_FILL_PROJECTION",
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 9, 8, 30, 1, tzinfo=timezone.utc),
    )

    class DuplicatePositionDb(FakeSession):
        async def scalar(self, stmt):
            self.statements.append(stmt)
            return state

        async def execute(self, stmt):
            self.statements.append(stmt)
            return FakeResult([projection_duplicate, snapshot_position])

    db = DuplicatePositionDb()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )

    qtys, _state_at = asyncio.run(
        engine._follower_position_qtys_with_state_at(
            db,
            MarketKey(dex="", coin="HYPE", canonical_coin="HYPE", raw_coin="HYPE", asset_id=1, venue_symbol="HYPE"),
        )
    )

    assert qtys[PositionSide.LONG] == Decimal("3")
    assert qtys[PositionSide.SHORT] == Decimal("0")


def test_ws_all_mids_updates_price_cache_by_dex() -> None:
    cfg = settings()
    watcher = HyperliquidLowLatencyWatcher(
        settings=cfg,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )

    asyncio.run(watcher._handle_ws_message(json.dumps({"channel": "allMids", "data": {"dex": "xyz", "mids": {"HYUNDAI": "10"}}})))

    assert watcher.price_cache.get("xyz:HYUNDAI").price == Decimal("10")


def test_ws_all_mids_flat_payload_updates_default_dex() -> None:
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )

    asyncio.run(watcher._handle_ws_message(json.dumps({"channel": "allMids", "data": {"BTC": "50000"}})))

    assert watcher.price_cache.get("BTC").price == Decimal("50000")


def test_fill_queue_key_is_market_scoped_across_leaders() -> None:
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    event = fill_event(coin="BTC")

    assert watcher._fill_queue_key(event, leader(id=1)) == watcher._fill_queue_key(
        event,
        leader(address="0x" + "2" * 40, id=2),
    )
    assert watcher._fill_queue_key(event, leader(id=1)) == ("HYPERLIQUID", "", "BTC")


def test_market_owner_blocker_blocks_other_leader_without_current_allocation() -> None:
    owner = LeaderPositionAllocationRecord(
        id=7,
        leader_id=1,
        leader_address=("0x" + "1" * 40).lower(),
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    other_leader = leader(address="0x" + "2" * 40, id=2)

    assert _allocation_market_owner_active(owner)
    blocker = _market_owner_blocker(owner, leader=other_leader, current_allocation=None)
    assert blocker is not None
    assert "MARKET_OWNER_BLOCKED" in blocker
    assert _market_owner_blocker(owner, leader=leader(id=1), current_allocation=None) is None

    current_allocation = LeaderPositionAllocationRecord(
        id=8,
        leader_id=2,
        leader_address=("0x" + "2" * 40).lower(),
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="SHORT",
        target_notional=Decimal("50"),
        allocated_notional=Decimal("50"),
        allocated_qty=Decimal("0.5"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    assert _market_owner_blocker(owner, leader=other_leader, current_allocation=current_allocation) is not None
    assert _market_owner_blocker(owner, leader=leader(id=1), current_allocation=owner) is None


def test_market_owner_pending_open_blocks_other_leaders_until_closed() -> None:
    owner = LeaderPositionAllocationRecord(
        id=9,
        leader_id=1,
        leader_address=("0x" + "1" * 40).lower(),
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("0"),
        allocated_notional=Decimal("0"),
        allocated_qty=Decimal("0"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status=PENDING_OPEN_STATUS,
        pending_reduce_reason=PENDING_OPEN_REASON,
    )
    other_leader = leader(address="0x" + "2" * 40, id=2)

    assert _allocation_market_owner_active(owner)
    blocker = _market_owner_blocker(owner, leader=other_leader, current_allocation=None)
    assert blocker is not None
    assert "MARKET_OWNER_BLOCKED" in blocker

    owner.status = "CLOSED"
    assert not _allocation_market_owner_active(owner)


def test_stale_zero_allocation_does_not_hold_market_owner_slot() -> None:
    stale = LeaderPositionAllocationRecord(
        id=10,
        leader_id=1,
        leader_address=("0x" + "1" * 40).lower(),
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("0"),
        allocated_notional=Decimal("0"),
        allocated_qty=Decimal("0"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )

    assert _stale_zero_allocation_reason(stale) is not None
    assert not _allocation_market_owner_active(stale)


def test_ws_non_dict_payload_is_ignored_without_reconnect_error() -> None:
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )

    asyncio.run(watcher._handle_ws_message(json.dumps({"channel": "subscriptionResponse", "data": []})))

    assert watcher.state.last_error is None


def test_required_price_status_dexes_ignore_unrelated_enabled_dexes() -> None:
    assert _required_price_status_dexes(
        enabled_dexes=["", "xyz", "cash", "flx"],
        event_dexes={"xyz"},
        allocation_dexes={""},
    ) == ["", "xyz"]


def test_low_latency_ready_uses_price_stream_liveness_not_all_market_freshness() -> None:
    assert _price_status_ready_for_low_latency_live(
        {
            "": {
                "markets_count": 930,
                "fresh": False,
                "stale_markets_count": 930,
                "last_price_update_age_ms": 2_600,
            },
            "xyz": {
                "markets_count": 100,
                "fresh": False,
                "stale_markets_count": 100,
                "last_price_update_age_ms": 2_400,
            },
        },
        ["", "xyz"],
        stale_ms=2_000,
    ) is True


def test_low_latency_ready_blocks_when_required_price_stream_is_stale() -> None:
    assert _price_status_ready_for_low_latency_live(
        {
            "": {
                "markets_count": 930,
                "fresh": False,
                "stale_markets_count": 930,
                "last_price_update_age_ms": 6_001,
            },
        },
        [""],
        stale_ms=2_000,
    ) is False


def test_user_fill_event_immediately_calls_engine_without_poller() -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.state.active_leaders[address] = leader(address)
    watcher.state.ws_leaders.add(address)
    message = {"channel": "userFills", "data": {"user": address, "fills": [{"coin": "BTC", "px": "50000", "sz": "0.1", "time": 1}]}}

    asyncio.run(handle_ws_and_drain(watcher, message))

    assert len(engine.calls) == 1
    assert engine.calls[0][0].market.canonical_coin == "BTC"


class FollowerStateInfoClient(NoopInfoClient):
    async def clearinghouse_state(self, user, dex=""):
        return {
            "withdrawable": "1000",
            "marginSummary": {"accountValue": "1000"},
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "1",
                        "positionValue": "100",
                        "entryPx": "100",
                    }
                }
            ],
        }


def test_follower_user_fill_marks_manual_guard_without_calling_leader_engine() -> None:
    cfg = settings()
    follower = cfg.hyperliquid_follower_account_address()
    watcher = HyperliquidLowLatencyWatcher(
        settings=cfg,
        info_client=FollowerStateInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.engine.pending_intents = PendingIntentLedger()
    watcher.engine.manual_position_guard = watcher.manual_position_guard
    message = {
        "channel": "userFills",
        "data": {
            "user": follower,
            "fills": [{"coin": "BTC", "px": "100", "sz": "1", "time": 1, "oid": "manual-1"}],
        },
    }

    asyncio.run(handle_ws_and_drain(watcher, message))

    market = MarketKey(dex="", coin="BTC", canonical_coin="BTC", raw_coin="BTC", asset_id=None, venue_symbol="BTC")
    assert watcher.manual_position_guard.active_entry(market) is not None
    assert engine.calls == []


def test_follower_user_fill_snapshot_does_not_mark_manual_guard() -> None:
    cfg = settings()
    follower = cfg.hyperliquid_follower_account_address()
    watcher = HyperliquidLowLatencyWatcher(
        settings=cfg,
        info_client=FollowerStateInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.engine.pending_intents = PendingIntentLedger()
    watcher.engine.manual_position_guard = watcher.manual_position_guard
    message = {
        "channel": "userFills",
        "data": {
            "user": follower,
            "isSnapshot": True,
            "fills": [{"coin": "BTC", "px": "100", "sz": "1", "time": 1, "oid": "historical-1"}],
        },
    }

    asyncio.run(handle_ws_and_drain(watcher, message))

    market = MarketKey(dex="", coin="BTC", canonical_coin="BTC", raw_coin="BTC", asset_id=None, venue_symbol="BTC")
    assert watcher.manual_position_guard.active_entry(market) is None
    assert engine.calls == []


def test_follower_auto_copy_fill_does_not_mark_manual_guard() -> None:
    cfg = settings()
    follower = cfg.hyperliquid_follower_account_address()

    class AutoOrderMatchSession(FakeSession):
        async def scalar(self, stmt):
            self.statements.append(stmt)
            if "execution_orders" in compiled_sql(stmt):
                return 123
            return None

    class AutoOrderMatchFactory:
        def __init__(self):
            self.session = AutoOrderMatchSession()

        def __call__(self):
            return self.session

    watcher = HyperliquidLowLatencyWatcher(
        settings=cfg,
        info_client=FollowerStateInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=AutoOrderMatchFactory(),
    )
    message = {
        "channel": "userFills",
        "data": {
            "user": follower,
            "fills": [{"coin": "BTC", "px": "100", "sz": "1", "time": 1, "oid": "auto-1"}],
        },
    }

    asyncio.run(handle_ws_and_drain(watcher, message))

    market = MarketKey(dex="", coin="BTC", canonical_coin="BTC", raw_coin="BTC", asset_id=None, venue_symbol="BTC")
    assert watcher.manual_position_guard.active_entry(market) is None


def test_follower_clearinghouse_state_ws_queues_latest_position_refresh() -> None:
    class FollowerStateInfoClient(NoopInfoClient):
        async def clearinghouse_state(self, address, dex=""):
            return {
                "withdrawable": "1000",
                "marginSummary": {"accountValue": "1000"},
                "assetPositions": [
                    {
                        "position": {
                            "coin": "BTC",
                            "szi": "1",
                            "positionValue": "100",
                            "entryPx": "100",
                        }
                    }
                ],
            }

    cfg = settings()
    follower = cfg.hyperliquid_follower_account_address()
    factory = FakeSessionFactory()
    watcher = HyperliquidLowLatencyWatcher(
        settings=cfg,
        info_client=FollowerStateInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=factory,
    )
    message = {
        "channel": "clearinghouseState",
        "data": {
            "user": follower,
            "dex": "",
            "clearinghouseState": {
                "withdrawable": "1000",
                "marginSummary": {"accountValue": "1000"},
                "assetPositions": [
                    {
                        "position": {
                            "coin": "BTC",
                            "szi": "1",
                            "positionValue": "100",
                            "entryPx": "100",
                        }
                    }
                ],
            },
        },
    }

    asyncio.run(handle_ws_and_drain(watcher, message))

    assert any(isinstance(item, LatestAccountState) for item in factory.session.added)
    assert any(isinstance(item, LatestAccountPosition) for item in factory.session.added)


def test_leader_user_events_on_trade_handler_are_ignored() -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.state.active_leaders[address] = leader(address)
    watcher.state.ws_leaders.add(address)
    message = {"channel": "userEvents", "data": {"user": address, "fills": [{"coin": "BTC", "px": "10", "sz": "1", "time": 1}]}}

    asyncio.run(handle_ws_and_drain(watcher, message))

    assert engine.calls == []


def test_snapshot_fill_does_not_reconcile() -> None:
    class SnapshotEngine(FillDrivenExecutionEngine):
        def __init__(self):
            self.recorded = []

        async def _record_source_fill(self, db, fill, *, processed):
            self.recorded.append(processed)

        async def reconcile_leader_symbol_allocation(self, *args, **kwargs):
            raise AssertionError("snapshot must not reconcile")

    engine = SnapshotEngine()

    result = asyncio.run(engine.handle_fill(FakeSession(), fill_event(snapshot=True), leader()))

    assert result is None
    assert engine.recorded == [False]


def test_snapshot_recovery_never_opens_new_allocation() -> None:
    open_fill = fill_event(snapshot=True, side="B", start_position="0", direction="Open Long")
    implied = derive_leader_post_position_from_fill(open_fill)

    assert implied.is_open
    assert _snapshot_recovery_allocation_side(implied) is None


def test_snapshot_recovery_reduce_uses_existing_allocation_checkpoint() -> None:
    reduce_fill = fill_event(snapshot=True, side="A", start_position="1200", direction="Close Long")
    recovered = _snapshot_recovery_fill(reduce_fill, reason="test")
    implied = derive_leader_post_position_from_fill(recovered)
    allocation = SimpleNamespace(
        last_reconcile_at=datetime.fromtimestamp((reduce_fill.time_ms - 1) / 1000, timezone.utc),
        last_leader_position_size=Decimal("2000"),
    )

    assert _snapshot_recovery_allocation_side(implied) == PositionSide.LONG
    assert _snapshot_event_after_allocation_checkpoint(reduce_fill, allocation) is True
    assert _snapshot_recovery_should_use_allocation_checkpoint(
        fill=recovered,
        planning_allocation=allocation,
        leader_previous_position_size=Decimal("1200"),
        leader_position_size=Decimal("1000"),
    ) is True


def test_snapshot_recovery_rejects_fill_before_allocation_checkpoint() -> None:
    reduce_fill = fill_event(snapshot=True, side="A", start_position="1200", direction="Close Long")
    allocation = SimpleNamespace(
        last_reconcile_at=datetime.fromtimestamp((reduce_fill.time_ms + 1) / 1000, timezone.utc),
        last_leader_position_size=Decimal("2000"),
    )

    assert _snapshot_event_after_allocation_checkpoint(reduce_fill, allocation) is False


def test_duplicate_fill_does_not_reconcile_or_record_again() -> None:
    class DuplicateEngine(FillDrivenExecutionEngine):
        def __init__(self):
            pass

        async def _record_source_fill(self, db, fill, *, processed):
            return False

        async def reconcile_leader_symbol_allocation(self, *args, **kwargs):
            raise AssertionError("duplicate must not reconcile")

    result = asyncio.run(DuplicateEngine().handle_fill(FakeSession(), fill_event(), leader()))

    assert result is None


def test_source_fill_insert_conflict_does_not_reconcile() -> None:
    class RaceDuplicateEngine(FillDrivenExecutionEngine):
        def __init__(self):
            pass

        async def _source_fill_seen(self, db, source_fill_id):
            return False

        async def _record_source_fill(self, db, fill, *, processed):
            return False

        async def reconcile_leader_symbol_allocation(self, *args, **kwargs):
            raise AssertionError("insert conflict duplicate must not reconcile")

    result = asyncio.run(RaceDuplicateEngine().handle_fill(FakeSession(), fill_event(), leader()))

    assert result is None


def test_live_non_duplicate_fill_reconciles_once() -> None:
    class ReconcileEngine(FillDrivenExecutionEngine):
        def __init__(self):
            super().__init__(
                settings=settings(),
                info_client=NoopInfoClient(),
                execution_client=TimeoutExecutionClient(),
                price_cache=LowLatencyPriceCache(stale_ms=2_000),
            )
            self.recorded = []
            self.reconciled = []

        async def _record_source_fill(self, db, fill, *, processed):
            self.recorded.append((fill.source_fill_id, processed))
            return True

        async def reconcile_leader_symbol_allocation(self, db, **kwargs):
            self.reconciled.append(kwargs["fill"])
            return ExecutionOrder(source_fill_id=kwargs["fill"].source_fill_id, status="NOOP")

    engine = ReconcileEngine()
    fill = fill_event(snapshot=False)

    result = asyncio.run(engine.handle_fill(FakeSession(), fill, leader()))

    assert result is not None
    assert result.source_fill_id == fill.source_fill_id
    assert engine.recorded == [(fill.source_fill_id, True)]
    assert engine.reconciled == [fill]


def test_snapshot_recovery_fill_reconciles_once_as_live_fill() -> None:
    class ReconcileEngine(FillDrivenExecutionEngine):
        def __init__(self):
            super().__init__(
                settings=settings(),
                info_client=NoopInfoClient(),
                execution_client=TimeoutExecutionClient(),
                price_cache=LowLatencyPriceCache(stale_ms=2_000),
            )
            self.recorded = []
            self.reconciled = []

        async def _snapshot_recovery_reason(self, db, fill, leader_config):
            return "test recovery"

        async def _record_source_fill(self, db, fill, *, processed):
            self.recorded.append((fill.source_fill_id, processed, fill.is_snapshot))
            return True

        async def reconcile_leader_symbol_allocation(self, db, **kwargs):
            self.reconciled.append(kwargs["fill"])
            return ExecutionOrder(source_fill_id=kwargs["fill"].source_fill_id, status="NOOP")

    engine = ReconcileEngine()
    fill = fill_event(snapshot=True, side="A", start_position="10", direction="Close Long")

    result = asyncio.run(engine.handle_fill(FakeSession(), fill, leader()))

    assert result is not None
    assert engine.recorded == [(fill.source_fill_id, True, False)]
    assert len(engine.reconciled) == 1
    assert engine.reconciled[0].is_snapshot is False
    assert engine.reconciled[0].raw["_copytrade_snapshot_recovery"] is True


def test_source_fill_insert_conflict_promotes_unprocessed_snapshot() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    db = SequenceExecuteSession([[], [123]])

    inserted = asyncio.run(engine._record_source_fill(db, fill_event(snapshot=False), processed=True))

    assert inserted is True
    assert len(db.statements) == 2
    update_sql = str(db.statements[1]).lower()
    assert "update source_fills" in update_sql
    assert "processed_at is null" in update_sql


def test_source_fill_processed_duplicate_stays_deduped() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    db = SequenceExecuteSession([[], []])

    inserted = asyncio.run(engine._record_source_fill(db, fill_event(snapshot=False), processed=True))

    assert inserted is False
    assert len(db.statements) == 2


def test_source_fill_unprocessed_snapshot_insert_conflict_does_not_promote() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    db = SequenceExecuteSession([[]])

    inserted = asyncio.run(engine._record_source_fill(db, fill_event(snapshot=True), processed=False))

    assert inserted is False
    assert len(db.statements) == 1


def assert_runtime_coalesces_same_order_fills_to_one_submit_event(
    fills: list[dict],
    *,
    expected_size: str,
    expected_implied_flag: str,
) -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.state.active_leaders[address] = leader(address)
    watcher.state.ws_leaders.add(address)
    message = {"channel": "userFills", "data": {"user": address, "fills": fills}}

    asyncio.run(handle_ws_and_drain(watcher, message))

    expected_events = [build_fill_event(address, fill, is_snapshot=False) for fill in fills]
    assert [call[0].source_fill_id for call in engine.calls] == [expected_events[0].source_fill_id]
    assert [call[0].size for call in engine.calls] == [Decimal(expected_size)]
    implied = derive_leader_post_position_from_fill(engine.calls[0][0])
    assert getattr(implied, expected_implied_flag) is True
    assert engine.recorded_source_fills == [
        (event.source_fill_id, True) for event in expected_events[1:]
    ]


def test_runtime_coalesces_same_order_fills_to_one_submit_event() -> None:
    base = {
        "coin": "HYPE",
        "side": "B",
        "time": 1,
        "oid": 123,
        "hash": "0xabc",
        "dir": "Open Long",
    }
    fills = [
        {**base, "tid": 1, "px": "100", "sz": "1", "startPosition": "0"},
        {**base, "tid": 2, "px": "101", "sz": "2", "startPosition": "1"},
    ]
    assert_runtime_coalesces_same_order_fills_to_one_submit_event(
        fills,
        expected_size="3",
        expected_implied_flag="is_open",
    )


def test_runtime_coalesces_same_order_increase_fills_to_one_submit_event() -> None:
    base = {
        "coin": "HYPE",
        "side": "B",
        "time": 1,
        "oid": 123,
        "hash": "0xabc",
        "dir": "Open Long",
    }
    fills = [
        {**base, "tid": 1, "px": "100", "sz": "1", "startPosition": "10"},
        {**base, "tid": 2, "px": "101", "sz": "2", "startPosition": "11"},
    ]
    assert_runtime_coalesces_same_order_fills_to_one_submit_event(
        fills,
        expected_size="3",
        expected_implied_flag="is_increase",
    )


def test_runtime_coalesces_same_order_reduce_fills_to_one_submit_event() -> None:
    base = {
        "coin": "HYPE",
        "side": "A",
        "time": 1,
        "oid": 123,
        "hash": "0xabc",
        "dir": "Close Long",
    }
    fills = [
        {**base, "tid": 1, "px": "100", "sz": "1", "startPosition": "10"},
        {**base, "tid": 2, "px": "101", "sz": "2", "startPosition": "9"},
    ]
    assert_runtime_coalesces_same_order_fills_to_one_submit_event(
        fills,
        expected_size="3",
        expected_implied_flag="is_reduce",
    )


def test_runtime_coalesces_same_order_close_fills_to_one_submit_event() -> None:
    base = {
        "coin": "HYPE",
        "side": "A",
        "time": 1,
        "oid": 123,
        "hash": "0xabc",
        "dir": "Close Long",
    }
    fills = [
        {**base, "tid": 1, "px": "100", "sz": "1", "startPosition": "3"},
        {**base, "tid": 2, "px": "101", "sz": "2", "startPosition": "2"},
    ]
    assert_runtime_coalesces_same_order_fills_to_one_submit_event(
        fills,
        expected_size="3",
        expected_implied_flag="is_close",
    )


def test_runtime_coalesces_ten_close_fragments_from_same_leader_order() -> None:
    base = {
        "coin": "HYPE",
        "side": "A",
        "time": 1780461427866,
        "oid": 454420427859,
        "hash": "<REDACTED_32_BYTE_HEX_IDENTIFIER>",
        "dir": "Close Long",
    }
    fills = [
        {**base, "tid": 995616685055507, "px": "42.5", "sz": "0.4", "startPosition": "1000.0"},
        {**base, "tid": 438759399203612, "px": "42.5", "sz": "277.6", "startPosition": "999.6"},
        {**base, "tid": 1094101528762297, "px": "42.5", "sz": "109.56", "startPosition": "722.0"},
        {**base, "tid": 208944243158836, "px": "42.5", "sz": "113.81", "startPosition": "612.44"},
        {**base, "tid": 783458579733775, "px": "42.5", "sz": "9.49", "startPosition": "498.63"},
        {**base, "tid": 339171085841828, "px": "42.5", "sz": "277.6", "startPosition": "489.14"},
        {**base, "tid": 854900440411904, "px": "42.5", "sz": "30.64", "startPosition": "211.54"},
        {**base, "tid": 26529487701487, "px": "42.5", "sz": "30.24", "startPosition": "180.9"},
        {**base, "tid": 409814000212042, "px": "42.5", "sz": "77.84", "startPosition": "150.66"},
        {**base, "tid": 1093110571678954, "px": "42.5", "sz": "72.82", "startPosition": "72.82"},
    ]
    assert_runtime_coalesces_same_order_fills_to_one_submit_event(
        fills,
        expected_size="1000.0",
        expected_implied_flag="is_close",
    )


def test_runtime_does_not_coalesce_different_order_fills() -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.state.active_leaders[address] = leader(address)
    watcher.state.ws_leaders.add(address)
    fills = [
        {"coin": "HYPE", "side": "B", "time": 1, "oid": 123, "hash": "0xabc", "tid": 1, "px": "100", "sz": "1", "startPosition": "0", "dir": "Open Long"},
        {"coin": "HYPE", "side": "B", "time": 1, "oid": 456, "hash": "0xdef", "tid": 2, "px": "101", "sz": "2", "startPosition": "1", "dir": "Open Long"},
    ]
    message = {"channel": "userFills", "data": {"user": address, "fills": fills}}

    asyncio.run(handle_ws_and_drain(watcher, message))

    expected_events = [build_fill_event(address, fill, is_snapshot=False) for fill in fills]
    assert [call[0].source_fill_id for call in engine.calls] == [
        event.source_fill_id for event in expected_events
    ]
    assert [call[0].size for call in engine.calls] == [Decimal("1"), Decimal("2")]
    assert engine.recorded_source_fills == []


def test_snapshot_batch_is_not_coalesced_by_fill_worker() -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.state.active_leaders[address] = leader(address)
    watcher.state.ws_leaders.add(address)
    base = {
        "coin": "HYPE",
        "side": "A",
        "oid": 456,
        "hash": "0xdef",
        "dir": "Close Long",
    }
    fills = [
        {**base, "tid": 1, "time": 1, "px": "100", "sz": "1", "startPosition": "10"},
        {**base, "tid": 2, "time": 2, "px": "100", "sz": "2", "startPosition": "9"},
        {**base, "tid": 3, "time": 3, "px": "100", "sz": "3", "startPosition": "7"},
    ]
    message = {"channel": "userFills", "data": {"user": address, "isSnapshot": True, "fills": fills}}

    asyncio.run(handle_ws_and_drain(watcher, message))

    assert [call[0].source_fill_id for call in engine.calls] == [
        build_fill_event(address, fill, is_snapshot=True).source_fill_id for fill in fills
    ]
    assert engine.recorded_source_fills == []


def test_skipped_snapshot_fill_records_unprocessed() -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    event = build_fill_event(
        address,
        {"coin": "HYPE", "side": "A", "time": 1, "tid": 1, "px": "100", "sz": "1", "startPosition": "10", "dir": "Close Long"},
        is_snapshot=True,
    )

    asyncio.run(watcher._record_skipped_source_fills([event]))

    assert engine.recorded_source_fills == [(event.source_fill_id, False)]


def test_suppressed_skipped_fill_does_not_enter_execution_queue() -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.state.active_leaders[address] = leader(address)
    watcher.state.ws_leaders.add(address)
    fill = {"coin": "HYPE", "side": "B", "time": 1, "tid": 1, "px": "100", "sz": "1", "startPosition": "0", "dir": "Open Long"}
    event = build_fill_event(address, fill, is_snapshot=False)
    message = {"channel": "userFills", "data": {"user": address, "fills": [fill]}}

    async def scenario():
        await watcher._remember_suppressed_events([event])
        await watcher._handle_ws_message(json.dumps(message))
        await watcher._drain_fill_queues()

    asyncio.run(scenario())

    assert engine.calls == []


def test_refresh_leaders_assigns_poll_fallback_when_user_limit_exceeded() -> None:
    l1 = leader("0x" + "1" * 40)
    l2 = leader("0x" + "2" * 40)
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(hyperliquid_ws_leader_subscription_limit=1),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory([l1, l2]),
    )

    asyncio.run(watcher.refresh_leaders())

    assert len(watcher.state.ws_leaders) == 1
    assert len(watcher.state.poll_fallback_leaders) == 1


def test_dynamic_leader_add_is_seen_by_subscription_refresh() -> None:
    l1 = leader("0x" + "1" * 40)
    l2 = leader("0x" + "2" * 40)
    factory = FakeSessionFactory([l1])
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=factory,
    )
    asyncio.run(watcher.refresh_leaders())
    factory.session.rows = [l1, l2]

    asyncio.run(watcher.refresh_leaders())
    ws = FakeWs()
    asyncio.run(watcher._subscribe_active_leaders(ws))

    users = [item["subscription"]["user"] for item in ws.sent if item["subscription"]["type"] == "userFills"]
    assert l1.leader_address in users
    assert l2.leader_address in users


def test_active_leader_subscription_uses_user_fills_only_to_avoid_duplicate_fill_channels() -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    watcher.state.ws_leaders.add(address)
    ws = FakeWs()

    asyncio.run(watcher._subscribe_active_leaders(ws))

    subscriptions = [item["subscription"] for item in ws.sent]
    assert subscriptions == [{"type": "userFills", "user": address}]


def test_ws_app_ping_uses_hyperliquid_application_ping() -> None:
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    ws = FakeWs()

    asyncio.run(watcher._send_ws_ping(ws))

    assert ws.sent == [{"method": "ping"}]


def test_websocket_connected_requires_leader_fill_socket_when_leaders_are_on_ws() -> None:
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    watcher.state.ws_leaders.add(("0x" + "1" * 40).lower())

    watcher.state.market_ws_connected = True
    watcher.state.leader_fills_ws_connected = False
    watcher._update_websocket_connected_state()
    assert watcher.state.websocket_connected is False

    watcher.state.leader_fills_ws_connected = True
    watcher._update_websocket_connected_state()
    assert watcher.state.websocket_connected is True


def test_leader_fill_backfill_replays_disconnect_window_without_old_fills() -> None:
    address = ("0x" + "1" * 40).lower()
    fills = [
        {"coin": "HYPE", "side": "B", "time": 90, "tid": 1, "px": "100", "sz": "1"},
        {"coin": "HYPE", "side": "B", "time": 120, "tid": 2, "px": "102", "sz": "1"},
        {"coin": "HYPE", "side": "B", "time": 110, "tid": 3, "px": "101", "sz": "1"},
    ]
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=BackfillInfoClient(fills),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.state.active_leaders[address] = leader(address)
    watcher.state.ws_leaders.add(address)

    async def scenario():
        await watcher._backfill_leader_fills_since(100)
        await watcher._drain_fill_queues()

    asyncio.run(scenario())

    assert watcher.info_client.calls == [(address, 100, False)]
    assert [call[0].time_ms for call in engine.calls] == [110, 120]


def test_startup_leader_fill_backfill_schedules_recent_window_once() -> None:
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(leader_fill_startup_backfill_seconds=60),
        info_client=BackfillInfoClient([]),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    starts: list[int] = []
    scheduled = []

    def fake_backfill(start_time_ms):
        starts.append(start_time_ms)

        async def noop():
            return None

        return noop()

    def fake_schedule(coro):
        scheduled.append(coro)
        coro.close()

    watcher._backfill_leader_fills_since = fake_backfill
    watcher._schedule_background_task = fake_schedule

    assert watcher._schedule_startup_leader_fill_backfill(now_ms=100_000) is True
    assert watcher._schedule_startup_leader_fill_backfill(now_ms=100_000) is False
    assert starts == [40_000]
    assert len(scheduled) == 1


def test_dynamic_leader_disable_delete_discards_events() -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    engine = RecordingEngine()
    watcher.engine = engine
    watcher.state.ws_leaders.add(address)
    message = {"channel": "userFills", "data": {"user": address, "fills": [{"coin": "BTC", "px": "1", "sz": "1", "time": 1}]}}

    asyncio.run(watcher._handle_ws_message(json.dumps(message)))

    assert engine.calls == []


def test_reconnect_resubscribe_clears_sent_subscription_cache() -> None:
    address = ("0x" + "1" * 40).lower()
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    watcher.state.ws_leaders.add(address)
    ws = FakeWs()
    asyncio.run(watcher._subscribe_active_leaders(ws))
    watcher._subscribed.clear()
    asyncio.run(watcher._subscribe_active_leaders(ws))

    user_fill_subs = [item for item in ws.sent if item["subscription"]["type"] == "userFills"]
    assert len(user_fill_subs) == 2


def test_order_side_handles_reduce_only_short_and_long() -> None:
    assert _order_side(target_side=PositionSide.LONG, reduce_only=False) == "BUY"
    assert _order_side(target_side=PositionSide.LONG, reduce_only=True) == "SELL"
    assert _order_side(target_side=PositionSide.SHORT, reduce_only=False) == "SELL"
    assert _order_side(target_side=PositionSide.SHORT, reduce_only=True) == "BUY"


def test_reduce_only_quantity_uses_close_qty_limit_for_percentage_close() -> None:
    plan = SimpleNamespace(close_qty_limit=Decimal("0.40"))

    quantity = _order_quantity_for_transition(
        mark_price=Decimal("80"),
        target_delta_abs=Decimal("40"),
        reduce_only=True,
        transition_plan=plan,
        aggregate_follower_qty=None,
    )

    assert quantity == Decimal("0.40")


def test_reduce_only_quantity_does_not_drift_with_mark_price() -> None:
    plan = SimpleNamespace(close_qty_limit=Decimal("0.40"))

    low_mark = _order_quantity_for_transition(
        mark_price=Decimal("80"),
        target_delta_abs=Decimal("40"),
        reduce_only=True,
        transition_plan=plan,
        aggregate_follower_qty=None,
    )
    high_mark = _order_quantity_for_transition(
        mark_price=Decimal("120"),
        target_delta_abs=Decimal("40"),
        reduce_only=True,
        transition_plan=plan,
        aggregate_follower_qty=None,
    )

    assert low_mark == Decimal("0.40")
    assert high_mark == Decimal("0.40")


def test_reduce_only_quantity_clamps_to_actual_follower_position_qty() -> None:
    plan = SimpleNamespace(close_qty_limit=Decimal("0.40"))

    quantity = _order_quantity_for_transition(
        mark_price=Decimal("80"),
        target_delta_abs=Decimal("40"),
        reduce_only=True,
        transition_plan=plan,
        aggregate_follower_qty=Decimal("0.25"),
    )

    assert quantity == Decimal("0.25")


def test_open_quantity_uses_target_notional_without_slippage_shrink() -> None:
    quantity = _order_quantity_for_transition(
        mark_price=Decimal("100"),
        target_delta_abs=Decimal("25"),
        reduce_only=False,
        transition_plan=SimpleNamespace(close_qty_limit=Decimal("0")),
        aggregate_follower_qty=None,
    )

    assert quantity == Decimal("0.25")
    assert quantity * Decimal("100") == Decimal("25")


def test_increase_quantity_uses_planned_open_qty_not_notional_mark_conversion() -> None:
    quantity = _order_quantity_for_transition(
        mark_price=Decimal("100"),
        target_delta_abs=Decimal("5"),
        reduce_only=False,
        transition_plan=SimpleNamespace(open_qty=Decimal("0.10"), close_qty_limit=Decimal("0")),
        aggregate_follower_qty=None,
    )

    assert quantity == Decimal("0.10")


def test_open_increase_allocation_mismatch_can_be_stale_follower_state() -> None:
    follower_state_at = datetime(2026, 5, 5, 14, 53, 16, tzinfo=timezone.utc)
    allocation_updated_at = datetime(2026, 5, 5, 14, 53, 17, tzinfo=timezone.utc)

    assert _allocation_mismatch_from_stale_follower_state(
        allocation_mismatch=True,
        follower_state_at=follower_state_at,
        allocation_latest_reconcile_at=allocation_updated_at,
    )
    assert not _allocation_mismatch_from_stale_follower_state(
        allocation_mismatch=True,
        follower_state_at=allocation_updated_at,
        allocation_latest_reconcile_at=follower_state_at,
    )
    assert not _allocation_mismatch_from_stale_follower_state(
        allocation_mismatch=False,
        follower_state_at=follower_state_at,
        allocation_latest_reconcile_at=allocation_updated_at,
    )


def test_unmanaged_follower_position_blocks_new_opens() -> None:
    unmanaged = _unmanaged_follower_position_qtys(
        follower_qty_by_side={PositionSide.LONG: Decimal("1.20"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("0.20"), PositionSide.SHORT: Decimal("0")},
    )

    blocker = _unmanaged_follower_position_blocker(
        transition_plan=SimpleNamespace(action=AllocationTransitionAction.OPEN),
        unmanaged_qty_by_side=unmanaged,
        unmanaged_position_state_lag=False,
        canonical_coin="HYPE",
    )

    assert unmanaged[PositionSide.LONG] == Decimal("1.00")
    assert blocker is not None
    assert "UNMANAGED_FOLLOWER_POSITION" in blocker


def test_unmanaged_opposite_follower_position_blocks_new_opens() -> None:
    unmanaged = _unmanaged_follower_position_qtys(
        follower_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0.50")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
    )

    blocker = _unmanaged_follower_position_blocker(
        transition_plan=SimpleNamespace(action=AllocationTransitionAction.OPEN),
        unmanaged_qty_by_side=unmanaged,
        unmanaged_position_state_lag=False,
        canonical_coin="HYPE",
    )

    assert blocker is not None
    assert "SHORT qty=0.50" in blocker


def test_unmanaged_same_side_follower_position_blocks_reduces() -> None:
    unmanaged = _unmanaged_follower_position_qtys(
        follower_qty_by_side={PositionSide.LONG: Decimal("1.20"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("0.20"), PositionSide.SHORT: Decimal("0")},
    )

    plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE, old_side=PositionSide.LONG)
    blocker = _unmanaged_follower_position_blocker(
        transition_plan=plan,
        unmanaged_qty_by_side=unmanaged,
        unmanaged_position_state_lag=False,
        canonical_coin="HYPE",
    )

    assert _unmanaged_follower_position_reduce_safe(transition_plan=plan, unmanaged_qty_by_side=unmanaged)
    assert blocker is not None
    assert "UNMANAGED_FOLLOWER_POSITION" in blocker


def test_unmanaged_opposite_follower_position_blocks_reduces() -> None:
    unmanaged = _unmanaged_follower_position_qtys(
        follower_qty_by_side={PositionSide.LONG: Decimal("0.20"), PositionSide.SHORT: Decimal("0.50")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("0.20"), PositionSide.SHORT: Decimal("0")},
    )

    plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE, old_side=PositionSide.LONG)
    blocker = _unmanaged_follower_position_blocker(
        transition_plan=plan,
        unmanaged_qty_by_side=unmanaged,
        unmanaged_position_state_lag=False,
        canonical_coin="HYPE",
    )

    assert not _unmanaged_follower_position_reduce_safe(transition_plan=plan, unmanaged_qty_by_side=unmanaged)
    assert blocker is not None
    assert "SHORT qty=0.50" in blocker


def test_unmanaged_follower_position_flat_allows_copy_resume() -> None:
    unmanaged = _unmanaged_follower_position_qtys(
        follower_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
    )

    blocker = _unmanaged_follower_position_blocker(
        transition_plan=SimpleNamespace(action=AllocationTransitionAction.OPEN),
        unmanaged_qty_by_side=unmanaged,
        unmanaged_position_state_lag=False,
        canonical_coin="HYPE",
    )

    assert unmanaged[PositionSide.LONG] == Decimal("0")
    assert unmanaged[PositionSide.SHORT] == Decimal("0")
    assert blocker is None


def test_manual_same_side_position_sync_does_not_absorb_manual_increase() -> None:
    allocation = allocation_record(qty="1", notional="100")
    plan = SimpleNamespace(action=AllocationTransitionAction.INCREASE)

    sync = _manual_same_side_position_sync(
        allocation=allocation,
        planning_allocation=allocation,
        transition_plan=plan,
        aggregate_side=PositionSide.LONG,
        follower_qty_by_side={PositionSide.LONG: Decimal("1.5"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("1"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=datetime(2026, 6, 8, 0, 0, 2, tzinfo=timezone.utc),
        allocation_latest_reconcile_at=datetime(2026, 6, 8, 0, 0, 1, tzinfo=timezone.utc),
        has_pending_allocation=False,
        mark_price=Decimal("100"),
    )

    assert sync["applied"] is False
    assert sync["manual_unmanaged_increase"] is True


def test_manual_same_side_position_sync_can_repair_trusted_actual_increase() -> None:
    allocation = allocation_record(qty="1", notional="100")
    plan = SimpleNamespace(action=AllocationTransitionAction.INCREASE)

    sync = _manual_same_side_position_sync(
        allocation=allocation,
        planning_allocation=allocation,
        transition_plan=plan,
        aggregate_side=PositionSide.LONG,
        follower_qty_by_side={PositionSide.LONG: Decimal("1.5"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("1"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=datetime(2026, 6, 8, 0, 0, 2, tzinfo=timezone.utc),
        allocation_latest_reconcile_at=datetime(2026, 6, 8, 0, 0, 1, tzinfo=timezone.utc),
        has_pending_allocation=False,
        mark_price=Decimal("100"),
        allow_actual_qty_increase=True,
        trusted_actual_qty_ceiling=Decimal("1.5"),
    )

    assert sync["applied"] is True
    assert sync["closed"] is False
    assert sync["actual_qty"] == Decimal("1.50000000")
    assert sync["actual_notional"] == Decimal("150.00000000")


def test_flat_leader_close_intent_is_detected_for_close_only_sync() -> None:
    allocation = allocation_record(qty="0.336", notional="611.949408")
    allocation.target_notional = Decimal("0")
    allocation.last_leader_position_size = Decimal("0")
    allocation.last_leader_position_notional = Decimal("0")

    assert _allocation_has_flat_leader_close_intent(allocation)


class AllocationSyncDb(FakeSession):
    def __init__(self, allocation_rows, state_rows, position_rows, disabled_allocation_rows=None):
        super().__init__()
        self.rows_sequence = [
            list(disabled_allocation_rows or []),
            list(allocation_rows),
            list(state_rows),
            list(position_rows),
        ]

    async def execute(self, stmt):
        self.statements.append(stmt)
        rows = self.rows_sequence.pop(0) if self.rows_sequence else []
        return FakeResult(rows)


class TrustedAllocationSyncDb(AllocationSyncDb):
    def __init__(self, allocation_rows, state_rows, position_rows, latest_fill_event):
        super().__init__(allocation_rows, state_rows, position_rows)
        self.latest_fill_event = latest_fill_event

    async def scalar(self, stmt):
        self.statements.append(stmt)
        return self.latest_fill_event


def test_allocation_sync_closes_deleted_leader_allocation() -> None:
    app_settings = settings()
    allocation = allocation_record(qty="25.21", notional="1695.654312")
    allocation.id = 351
    allocation.leader_id = 4
    allocation.leader_address = ("0x" + "4" * 36 + "ddd1").lower()
    allocation.hyperliquid_coin = "HYPE"
    allocation.dex = ""
    allocation.canonical_coin = "HYPE"
    allocation.venue_symbol = "HYPE"
    allocation.position_side = "LONG"
    allocation.status = "OPEN"
    allocation.last_leader_position_size = Decimal("3000")
    allocation.last_leader_position_notional = Decimal("200000")
    allocation.last_reconcile_at = datetime(2026, 7, 12, 21, 45, 14, tzinfo=timezone.utc)
    deleted_at = datetime(2026, 7, 12, 21, 45, 46, tzinfo=timezone.utc)
    db = AllocationSyncDb(
        [],
        [],
        [],
        disabled_allocation_rows=[(allocation, False, deleted_at)],
    )
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (1, 0)
    assert allocation.status == "CLOSED"
    assert allocation.allocated_qty == Decimal("0")
    assert allocation.allocated_notional == Decimal("0")
    assert allocation.target_notional == Decimal("0")
    assert allocation.last_leader_position_size == Decimal("0")
    events = [item for item in db.added if isinstance(item, AllocationEvent)]
    risks = [item for item in db.added if isinstance(item, RiskEvent)]
    assert events[-1].action == "DELETED_LEADER_ALLOCATION_CLOSED"
    assert risks[-1].event_type == "DELETED_LEADER_ALLOCATION_CLOSED"


def test_allocation_sync_closes_flat_leader_close_intent_when_follower_actual_is_flat() -> None:
    app_settings = settings()
    allocation = allocation_record(qty="9262", notional="2199.72701")
    allocation.id = 284
    allocation.leader_id = 5
    allocation.leader_address = ("0x" + "2" * 40).lower()
    allocation.hyperliquid_coin = "JUP"
    allocation.dex = ""
    allocation.canonical_coin = "JUP"
    allocation.venue_symbol = "JUP"
    allocation.position_side = "SHORT"
    allocation.target_notional = Decimal("0")
    allocation.last_leader_position_size = Decimal("0")
    allocation.last_leader_position_notional = Decimal("0")
    allocation.status = "REDUCING"
    allocation.last_reconcile_at = datetime(2026, 7, 4, 7, 14, 26, tzinfo=timezone.utc)
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        account_label="follower",
        last_update_at=datetime(2026, 7, 4, 7, 14, 27, tzinfo=timezone.utc),
    )
    db = AllocationSyncDb([allocation], [state], [])
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (1, 0)
    assert allocation.status == "CLOSED"
    assert allocation.allocated_qty == Decimal("0")
    assert allocation.allocated_notional == Decimal("0")
    assert allocation.target_notional == Decimal("0")
    events = [item for item in db.added if isinstance(item, AllocationEvent)]
    assert events[-1].action == "ALLOCATION_AUTO_CLOSED_FLAT_LEADER_AND_FOLLOWER"


def test_allocation_sync_keeps_flat_leader_close_intent_reducing_when_follower_residual_exists() -> None:
    app_settings = settings()
    allocation = allocation_record(qty="100", notional="25")
    allocation.id = 285
    allocation.leader_id = 5
    allocation.leader_address = ("0x" + "2" * 40).lower()
    allocation.hyperliquid_coin = "JUP"
    allocation.dex = ""
    allocation.canonical_coin = "JUP"
    allocation.venue_symbol = "JUP"
    allocation.position_side = "SHORT"
    allocation.target_notional = Decimal("0")
    allocation.last_leader_position_size = Decimal("0")
    allocation.last_leader_position_notional = Decimal("0")
    allocation.status = "REDUCING"
    allocation.last_reconcile_at = datetime(2026, 7, 4, 7, 14, 26, tzinfo=timezone.utc)
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        account_label="follower",
        last_update_at=datetime(2026, 7, 4, 7, 14, 27, tzinfo=timezone.utc),
    )
    position = LatestAccountPosition(
        account_state_id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        coin="JUP",
        dex="",
        canonical_coin="JUP",
        side="SHORT",
        size=Decimal("-50"),
        notional=Decimal("-12.5"),
        mark_px=Decimal("0.25"),
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 4, 7, 14, 27, tzinfo=timezone.utc),
    )
    db = AllocationSyncDb([allocation], [state], [position])
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (1, 0)
    assert allocation.status == "REDUCING"
    assert allocation.target_notional == Decimal("0")
    assert allocation.allocated_qty == Decimal("50.00000000")
    assert allocation.allocated_notional == Decimal("12.50000000")
    events = [item for item in db.added if isinstance(item, AllocationEvent)]
    assert events[-1].action == "ALLOCATION_AUTO_SYNCED_FLAT_LEADER_CLOSE_INTENT"


def test_allocation_sync_waits_until_leader_flat_after_follower_manually_flattens_active_lifecycle() -> None:
    app_settings = settings()
    allocation = allocation_record(qty="5.065", notional="8687.9945")
    allocation.id = 307
    allocation.leader_id = 4
    allocation.leader_address = ("0x" + "3" * 40).lower()
    allocation.hyperliquid_coin = "SNDK"
    allocation.dex = "xyz"
    allocation.canonical_coin = "xyz:SNDK"
    allocation.venue_symbol = "xyz:SNDK"
    allocation.position_side = "LONG"
    allocation.target_notional = Decimal("8687.9945")
    allocation.last_leader_position_size = Decimal("15")
    allocation.last_leader_position_notional = Decimal("25740")
    allocation.last_leader_account_value = Decimal("2000000")
    allocation.status = "OPEN"
    allocation.last_reconcile_at = datetime(2026, 7, 6, 22, 55, 20, tzinfo=timezone.utc)
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="xyz",
        account_label="follower",
        last_update_at=datetime(2026, 7, 6, 22, 55, 25, tzinfo=timezone.utc),
    )
    db = AllocationSyncDb([allocation], [state], [])
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (1, 0)
    assert allocation.status == "CLOSED"
    baselines = [item for item in db.added if isinstance(item, LeaderPositionBaseline)]
    assert len(baselines) == 1
    assert baselines[0].baseline_status == "WAIT_UNTIL_FLAT"
    assert baselines[0].canonical_coin == "XYZ:SNDK"
    assert baselines[0].last_leader_size == Decimal("15")


def test_allocation_sync_closes_multiple_same_scope_allocations_when_follower_actual_is_flat() -> None:
    app_settings = settings()
    first = allocation_record(qty="1", notional="100")
    first.id = 401
    first.leader_id = 4
    first.leader_address = ("0x" + "4" * 40).lower()
    first.dex = "xyz"
    first.hyperliquid_coin = "SNDK"
    first.canonical_coin = "xyz:SNDK"
    first.venue_symbol = "xyz:SNDK"
    first.position_side = "LONG"
    first.last_leader_position_size = Decimal("0")
    first.last_leader_position_notional = Decimal("0")
    first.last_reconcile_at = datetime(2026, 7, 6, 22, 55, 20, tzinfo=timezone.utc)
    second = allocation_record(qty="2", notional="200")
    second.id = 402
    second.leader_id = 5
    second.leader_address = ("0x" + "5" * 40).lower()
    second.dex = "xyz"
    second.hyperliquid_coin = "SNDK"
    second.canonical_coin = "xyz:SNDK"
    second.venue_symbol = "xyz:SNDK"
    second.position_side = "LONG"
    second.last_leader_position_size = Decimal("10")
    second.last_leader_position_notional = Decimal("17000")
    second.last_reconcile_at = datetime(2026, 7, 6, 22, 55, 20, tzinfo=timezone.utc)
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="xyz",
        account_label="follower",
        last_update_at=datetime(2026, 7, 6, 22, 55, 25, tzinfo=timezone.utc),
    )
    db = AllocationSyncDb([first, second], [state], [])
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (2, 0)
    assert first.status == second.status == "CLOSED"
    assert first.allocated_qty == second.allocated_qty == Decimal("0")
    events = [item for item in db.added if isinstance(item, AllocationEvent)]
    assert [event.action for event in events[:2]] == [
        "ALLOCATION_AUTO_CLOSED_MULTIPLE_SCOPE_FOLLOWER_FLAT",
        "ALLOCATION_AUTO_CLOSED_MULTIPLE_SCOPE_FOLLOWER_FLAT",
    ]
    baselines = [item for item in db.added if isinstance(item, LeaderPositionBaseline)]
    assert len(baselines) == 1
    assert baselines[0].leader_id == 5
    assert baselines[0].baseline_status == "WAIT_UNTIL_FLAT"


def test_allocation_sync_prefers_account_snapshot_over_projection_duplicate() -> None:
    app_settings = settings()
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 403
    allocation.leader_id = 4
    allocation.leader_address = ("0x" + "4" * 40).lower()
    allocation.dex = ""
    allocation.hyperliquid_coin = "HYPE"
    allocation.canonical_coin = "HYPE"
    allocation.venue_symbol = "HYPE"
    allocation.position_side = "LONG"
    allocation.status = "OPEN"
    allocation.last_reconcile_at = datetime(2026, 7, 9, 8, 29, 58, tzinfo=timezone.utc)
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        account_label="follower",
        last_update_at=datetime(2026, 7, 9, 8, 30, 2, tzinfo=timezone.utc),
    )
    snapshot_position = LatestAccountPosition(
        id=20,
        account_state_id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("0.5"),
        notional=Decimal("50"),
        mark_px_source="POSITION_MARK_PX",
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 9, 8, 30, 0, tzinfo=timezone.utc),
    )
    projection_duplicate = LatestAccountPosition(
        id=21,
        account_state_id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("9"),
        notional=Decimal("900"),
        mark_px_source="LOCAL_FILL_PROJECTION",
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 9, 8, 30, 1, tzinfo=timezone.utc),
    )
    db = AllocationSyncDb([allocation], [state], [projection_duplicate, snapshot_position])
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (1, 0)
    assert allocation.allocated_qty == Decimal("0.50000000")
    assert allocation.allocated_notional == Decimal("50")


def test_allocation_sync_repairs_snapshot_lag_after_auto_copy_fill() -> None:
    app_settings = settings()
    allocation = allocation_record(qty="17", notional="1139.544")
    allocation.id = 404
    allocation.leader_id = 4
    allocation.leader_address = ("0x" + "4" * 40).lower()
    allocation.dex = ""
    allocation.hyperliquid_coin = "HYPE"
    allocation.canonical_coin = "HYPE"
    allocation.venue_symbol = "HYPE"
    allocation.position_side = "LONG"
    allocation.status = "OPEN"
    allocation.last_reconcile_at = datetime(2026, 7, 9, 17, 32, 22, tzinfo=timezone.utc)
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        account_label="follower",
        last_update_at=datetime(2026, 7, 9, 17, 32, 30, tzinfo=timezone.utc),
    )
    actual_position = LatestAccountPosition(
        id=22,
        account_state_id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("25.18"),
        notional=Decimal("1689.17512"),
        mark_px=Decimal("67.0765"),
        mark_px_source="POSITION_MARK_PX",
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 9, 17, 32, 30, tzinfo=timezone.utc),
    )
    db = TrustedAllocationSyncDb(
        [allocation],
        [state],
        [actual_position],
        SimpleNamespace(after_qty=Decimal("25.18"), created_at=datetime(2026, 7, 9, 17, 32, 14, tzinfo=timezone.utc)),
    )
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (1, 0)
    assert allocation.allocated_qty == Decimal("25.18000000")
    assert allocation.allocated_notional == Decimal("1689.175120000000")
    events = [item for item in db.added if isinstance(item, RiskEvent)]
    assert events[-1].event_type == "ALLOCATION_AUTO_SYNCED_TO_FOLLOWER_POSITION"


def test_allocation_sync_refreshes_reconcile_time_when_actual_qty_matches() -> None:
    app_settings = settings()
    allocation = allocation_record(qty="17", notional="1139.544")
    allocation.id = 409
    allocation.leader_id = 4
    allocation.leader_address = ("0x" + "4" * 40).lower()
    allocation.dex = ""
    allocation.hyperliquid_coin = "HYPE"
    allocation.canonical_coin = "HYPE"
    allocation.venue_symbol = "HYPE"
    allocation.position_side = "LONG"
    allocation.status = "OPEN"
    allocation.last_reconcile_at = datetime(2026, 7, 9, 17, 32, 22, tzinfo=timezone.utc)
    state_at = datetime(2026, 7, 9, 17, 32, 30, tzinfo=timezone.utc)
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        account_label="follower",
        last_update_at=state_at,
    )
    actual_position = LatestAccountPosition(
        id=26,
        account_state_id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("17"),
        notional=Decimal("1139.544"),
        mark_px=Decimal("67.032"),
        active=True,
        status="OPEN",
        last_update_at=state_at,
    )
    db = AllocationSyncDb([allocation], [state], [actual_position])
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (1, 0)
    assert allocation.allocated_qty == Decimal("17")
    assert allocation.last_reconcile_at == state_at
    assert [item for item in db.added if isinstance(item, (AllocationEvent, RiskEvent))] == []


def test_allocation_sync_does_not_absorb_manual_guarded_increase() -> None:
    app_settings = settings()
    allocation = allocation_record(qty="17", notional="1139.544")
    allocation.id = 405
    allocation.leader_id = 4
    allocation.leader_address = ("0x" + "4" * 40).lower()
    allocation.dex = ""
    allocation.hyperliquid_coin = "HYPE"
    allocation.canonical_coin = "HYPE"
    allocation.venue_symbol = "HYPE"
    allocation.position_side = "LONG"
    allocation.status = "OPEN"
    allocation.last_reconcile_at = datetime(2026, 7, 9, 17, 32, 22, tzinfo=timezone.utc)
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        account_label="follower",
        last_update_at=datetime(2026, 7, 9, 17, 32, 30, tzinfo=timezone.utc),
    )
    actual_position = LatestAccountPosition(
        id=23,
        account_state_id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("25.18"),
        notional=Decimal("1689.17512"),
        mark_px=Decimal("67.0765"),
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 9, 17, 32, 30, tzinfo=timezone.utc),
    )
    db = AllocationSyncDb([allocation], [state], [actual_position])
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )
    watcher.manual_position_guard.mark(
        MarketKey(dex="", coin="HYPE", canonical_coin="HYPE", raw_coin="HYPE", asset_id=None, venue_symbol="HYPE"),
        reason="test manual fill",
        observed_at=datetime(2026, 7, 9, 17, 32, 25, tzinfo=timezone.utc),
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (0, 0)
    assert allocation.allocated_qty == Decimal("17")


def test_allocation_sync_does_not_absorb_untrusted_increase_after_restart() -> None:
    app_settings = settings()
    allocation = allocation_record(qty="17", notional="1139.544")
    allocation.id = 406
    allocation.leader_id = 4
    allocation.leader_address = ("0x" + "4" * 40).lower()
    allocation.dex = ""
    allocation.hyperliquid_coin = "HYPE"
    allocation.canonical_coin = "HYPE"
    allocation.venue_symbol = "HYPE"
    allocation.position_side = "LONG"
    allocation.status = "OPEN"
    allocation.last_reconcile_at = datetime(2026, 7, 9, 17, 32, 22, tzinfo=timezone.utc)
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        account_label="follower",
        last_update_at=datetime(2026, 7, 9, 17, 32, 30, tzinfo=timezone.utc),
    )
    actual_position = LatestAccountPosition(
        id=24,
        account_state_id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("30"),
        notional=Decimal("2010"),
        mark_px=Decimal("67"),
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 9, 17, 32, 30, tzinfo=timezone.utc),
    )
    db = TrustedAllocationSyncDb(
        [allocation],
        [state],
        [actual_position],
        SimpleNamespace(after_qty=Decimal("25.18"), created_at=datetime(2026, 7, 9, 17, 32, 14, tzinfo=timezone.utc)),
    )
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (0, 0)
    assert allocation.allocated_qty == Decimal("17")


def test_allocation_sync_skips_recent_post_fill_stale_down_snapshot() -> None:
    app_settings = settings(allocation_post_fill_snapshot_lag_guard_seconds=30)
    now = datetime.now(timezone.utc)
    allocation = allocation_record(qty="25.18", notional="1690.63556")
    allocation.id = 407
    allocation.leader_id = 4
    allocation.leader_address = ("0x" + "4" * 40).lower()
    allocation.dex = ""
    allocation.hyperliquid_coin = "HYPE"
    allocation.canonical_coin = "HYPE"
    allocation.venue_symbol = "HYPE"
    allocation.position_side = "LONG"
    allocation.status = "OPEN"
    allocation.last_reconcile_at = now
    state = LatestAccountState(
        id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        account_label="follower",
        last_update_at=now,
    )
    stale_position = LatestAccountPosition(
        id=25,
        account_state_id=1,
        role="FOLLOWER",
        address=app_settings.hyperliquid_follower_account_address(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("17"),
        notional=Decimal("1139.544"),
        mark_px=Decimal("67.032"),
        active=True,
        status="OPEN",
        last_update_at=now,
    )
    db = TrustedAllocationSyncDb(
        [allocation],
        [state],
        [stale_position],
        SimpleNamespace(after_qty=Decimal("25.18"), created_at=now),
    )
    watcher = HyperliquidLowLatencyWatcher(
        settings=app_settings,
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: db,
    )

    synced, skipped = asyncio.run(watcher._sync_allocations_to_actual_follower_positions(db))

    assert (synced, skipped) == (0, 1)
    assert allocation.allocated_qty == Decimal("25.18")
    assert _allocation_sync_in_post_fill_snapshot_lag_guard(
        allocation_reconcile_at=allocation.last_reconcile_at,
        latest_fill_event_at=now,
        guard_seconds=30,
    )


def test_manual_sync_replanned_add_uses_actual_qty_as_percentage_base() -> None:
    allocation = allocation_record(qty="1.5", notional="150")
    allocation.last_leader_position_size = Decimal("100")

    item = plan_leader_allocation_transition(
        leader_id=1,
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:USAR",
        leader_side=PositionSide.LONG,
        leader_position_notional=Decimal("11000"),
        leader_position_size=Decimal("110"),
        leader_account_value_used=None,
        follower_account_value_used=None,
        copy_multiplier=Decimal("1"),
        current_allocation=allocation,
    )

    quantity = _order_quantity_for_transition(
        mark_price=Decimal("100"),
        target_delta_abs=abs(item.delta_notional),
        reduce_only=False,
        transition_plan=item,
        aggregate_follower_qty=Decimal("1.5"),
    )

    assert item.action == AllocationTransitionAction.INCREASE
    assert item.delta_notional == Decimal("15.00000000")
    assert quantity == Decimal("0.15000000")


def test_position_ratio_increase_validator_allows_mark_price_drift_above_accounting_delta() -> None:
    allocation = allocation_record(qty="0.992", notional="87.242432")
    allocation.hyperliquid_coin = "CL"
    allocation.canonical_coin = "xyz:CL"
    allocation.venue_symbol = "CL"
    allocation.position_side = "SHORT"
    allocation.avg_entry_price = Decimal("87.946")
    allocation.last_leader_position_size = Decimal("500")

    item = plan_leader_allocation_transition(
        leader_id=1,
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:CL",
        leader_side=PositionSide.SHORT,
        leader_position_notional=Decimal("-88916"),
        leader_position_size=Decimal("1000"),
        leader_account_value_used=None,
        follower_account_value_used=None,
        copy_multiplier=Decimal("1"),
        current_allocation=allocation,
        leader_previous_position_size=Decimal("500"),
    )
    quantity = _order_quantity_for_transition(
        mark_price=Decimal("88.794"),
        target_delta_abs=abs(item.delta_notional),
        reduce_only=False,
        transition_plan=item,
        aggregate_follower_qty=Decimal("0.992"),
    )
    allow_price_drift = _allow_target_notional_price_drift_for_transition(item)
    result = validate_hyperliquid_order_params(
        dex="xyz",
        canonical_coin="xyz:CL",
        asset_id=29,
        action="INCREASE",
        side="SELL",
        target_delta_notional=abs(item.delta_notional),
        raw_size=quantity,
        raw_price=Decimal("88.794"),
        market_meta={"name": "CL", "asset_id": 29, "szDecimals": 3, "maxLeverage": 4},
        order_policy={
            "cloid": "0x" + "b" * 32,
            "is_buy": False,
            "reduce_only": False,
            "tif": "Ioc",
            "order_type": {"limit": {"tif": "Ioc"}},
            "aggressive_market": True,
            "price_fresh": True,
            "min_order_value": 10,
            "effective_leverage": 4,
            "allow_target_notional_price_drift": allow_price_drift,
        },
    )

    assert item.action == AllocationTransitionAction.INCREASE
    assert item.formula_inputs["increase_delta_source"] == "leader_fill_start_position_size"
    assert quantity == Decimal("0.99200000")
    assert result.estimated_notional == Decimal("88.08364800")
    assert result.target_delta_notional == Decimal("87.24243200")
    assert result.estimated_notional > result.target_delta_notional
    assert result.ok
    assert "BLOCKED_TARGET_NOTIONAL_EXCEEDED" not in result.errors


def test_manual_same_side_position_sync_closes_allocation_when_actual_position_is_flat() -> None:
    allocation = allocation_record(qty="1", notional="100")
    plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE)

    sync = _manual_same_side_position_sync(
        allocation=allocation,
        planning_allocation=allocation,
        transition_plan=plan,
        aggregate_side=PositionSide.LONG,
        follower_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("1"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=datetime(2026, 6, 8, 0, 0, 2, tzinfo=timezone.utc),
        allocation_latest_reconcile_at=datetime(2026, 6, 8, 0, 0, 1, tzinfo=timezone.utc),
        has_pending_allocation=False,
        mark_price=Decimal("0"),
    )

    assert sync["applied"] is True
    assert sync["closed"] is True
    assert sync["actual_qty"] == Decimal("0")
    assert sync["actual_notional"] == Decimal("0")


def test_flat_stale_allocation_new_open_uses_fresh_account_ratio_sizing() -> None:
    stale_allocation = allocation_record(qty="0.386", notional="697.34760000")
    stale_allocation.canonical_coin = "xyz:SNDK"
    stale_allocation.venue_symbol = "xyz:SNDK"
    stale_allocation.hyperliquid_coin = "SNDK"
    stale_allocation.copy_multiplier = Decimal("1")
    stale_allocation.last_leader_position_size = Decimal("1.062")
    stale_allocation.last_leader_position_notional = Decimal("1822.392")
    stale_allocation.last_leader_account_value = Decimal("2000000")

    sync = _manual_same_side_position_sync(
        allocation=stale_allocation,
        planning_allocation=stale_allocation,
        transition_plan=SimpleNamespace(action=AllocationTransitionAction.INCREASE),
        aggregate_side=PositionSide.LONG,
        follower_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("0.386"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=datetime(2026, 7, 6, 22, 55, 25, tzinfo=timezone.utc),
        allocation_latest_reconcile_at=datetime(2026, 7, 6, 22, 55, 20, tzinfo=timezone.utc),
        has_pending_allocation=False,
        mark_price=Decimal("1716"),
    )
    assert sync["closed"] is True

    reused_lifecycle_plan = plan_leader_allocation_transition(
        leader_id=4,
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:SNDK",
        leader_side=PositionSide.LONG,
        leader_position_notional=Decimal("25740"),
        leader_position_size=Decimal("15"),
        leader_account_value_used=Decimal("2000000"),
        follower_account_value_used=Decimal("14330.47253499"),
        copy_multiplier=Decimal("1"),
        current_allocation=stale_allocation,
        leader_fill_notional=Decimal("23917.608"),
        leader_previous_position_size=Decimal("1.062"),
    )
    fresh_lifecycle_plan = plan_leader_allocation_transition(
        leader_id=4,
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:SNDK",
        leader_side=PositionSide.LONG,
        leader_position_notional=Decimal("25740"),
        leader_position_size=Decimal("15"),
        leader_account_value_used=Decimal("2000000"),
        follower_account_value_used=Decimal("14330.47253499"),
        copy_multiplier=Decimal("1"),
        current_allocation=None,
        leader_fill_notional=Decimal("23917.608"),
        leader_previous_position_size=Decimal("1.062"),
    )

    assert reused_lifecycle_plan.delta_notional > Decimal("9000")
    assert fresh_lifecycle_plan.action == AllocationTransitionAction.OPEN
    assert fresh_lifecycle_plan.target_notional == Decimal("184.43318153")
    assert fresh_lifecycle_plan.delta_notional == Decimal("184.43318153")


def test_manual_same_side_position_sync_can_block_flat_snapshot_close() -> None:
    allocation = allocation_record(qty="1", notional="100")
    plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE)

    sync = _manual_same_side_position_sync(
        allocation=allocation,
        planning_allocation=allocation,
        transition_plan=plan,
        aggregate_side=PositionSide.LONG,
        follower_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("1"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=datetime(2026, 6, 8, 0, 0, 2, tzinfo=timezone.utc),
        allocation_latest_reconcile_at=datetime(2026, 6, 8, 0, 0, 1, tzinfo=timezone.utc),
        has_pending_allocation=False,
        mark_price=Decimal("0"),
        allow_flat_close=False,
    )

    assert sync["applied"] is False
    assert sync["flat_close_blocked"] is True


def test_stale_allocation_close_replans_against_actual_follower_qty() -> None:
    allocation = allocation_record(qty="61.23", notional="5150")
    allocation.last_leader_position_size = Decimal("7000")
    initial_plan = plan_leader_allocation_transition(
        leader_id=1,
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:USAR",
        leader_side=PositionSide.FLAT,
        leader_position_notional=Decimal("0"),
        leader_position_size=Decimal("0"),
        leader_account_value_used=None,
        follower_account_value_used=None,
        copy_multiplier=Decimal("1"),
        current_allocation=allocation,
    )

    sync = _manual_same_side_position_sync(
        allocation=allocation,
        planning_allocation=allocation,
        transition_plan=initial_plan,
        aggregate_side=PositionSide.LONG,
        follower_qty_by_side={PositionSide.LONG: Decimal("23.97"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("61.23"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=datetime(2026, 6, 8, 0, 0, 2, tzinfo=timezone.utc),
        allocation_latest_reconcile_at=datetime(2026, 6, 8, 0, 0, 1, tzinfo=timezone.utc),
        has_pending_allocation=False,
        mark_price=Decimal("84.08057812"),
    )
    assert sync["applied"] is True
    assert sync["actual_qty"] == Decimal("23.97000000")

    allocation.allocated_qty = sync["actual_qty"]
    allocation.allocated_notional = sync["actual_notional"]
    allocation.target_notional = sync["actual_notional"]
    allocation.avg_entry_price = Decimal("84.08057812")
    replanned = plan_leader_allocation_transition(
        leader_id=1,
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:USAR",
        leader_side=PositionSide.FLAT,
        leader_position_notional=Decimal("0"),
        leader_position_size=Decimal("0"),
        leader_account_value_used=None,
        follower_account_value_used=None,
        copy_multiplier=Decimal("1"),
        current_allocation=allocation,
    )
    quantity = _order_quantity_for_transition(
        mark_price=Decimal("84.08057812"),
        target_delta_abs=abs(replanned.delta_notional),
        reduce_only=True,
        transition_plan=replanned,
        aggregate_follower_qty=Decimal("23.97"),
    )
    blockers = _reduce_quantity_guard_blockers(
        reduce_only=True,
        transition_plan=replanned,
        current_allocation=allocation,
        rounded_size=quantity,
        aggregate_follower_qty=Decimal("23.97"),
    )

    assert replanned.action == AllocationTransitionAction.CLOSE
    assert replanned.close_qty_limit == Decimal("23.97000000")
    assert quantity == Decimal("23.97000000")
    assert blockers == []


def test_manual_same_side_position_sync_rejects_multiple_allocations_or_opposite_position() -> None:
    allocation = allocation_record(qty="1", notional="100")
    plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE)
    fresh = datetime(2026, 6, 8, 0, 0, 2, tzinfo=timezone.utc)
    previous = datetime(2026, 6, 8, 0, 0, 1, tzinfo=timezone.utc)

    multi_allocation = _manual_same_side_position_sync(
        allocation=allocation,
        planning_allocation=allocation,
        transition_plan=plan,
        aggregate_side=PositionSide.LONG,
        follower_qty_by_side={PositionSide.LONG: Decimal("1.5"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("1.2"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=fresh,
        allocation_latest_reconcile_at=previous,
        has_pending_allocation=False,
        mark_price=Decimal("100"),
    )
    opposite_position = _manual_same_side_position_sync(
        allocation=allocation,
        planning_allocation=allocation,
        transition_plan=plan,
        aggregate_side=PositionSide.LONG,
        follower_qty_by_side={PositionSide.LONG: Decimal("1.5"), PositionSide.SHORT: Decimal("0.1")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("1"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=fresh,
        allocation_latest_reconcile_at=previous,
        has_pending_allocation=False,
        mark_price=Decimal("100"),
    )
    stale_follower_state = _manual_same_side_position_sync(
        allocation=allocation,
        planning_allocation=allocation,
        transition_plan=plan,
        aggregate_side=PositionSide.LONG,
        follower_qty_by_side={PositionSide.LONG: Decimal("1.5"), PositionSide.SHORT: Decimal("0")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("1"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=previous,
        allocation_latest_reconcile_at=fresh,
        has_pending_allocation=False,
        mark_price=Decimal("100"),
    )

    assert multi_allocation["applied"] is False
    assert opposite_position["applied"] is False
    assert stale_follower_state["applied"] is False


def test_resolved_account_values_use_hot_cache_without_db() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    follower_address = engine.settings.hyperliquid_follower_account_address()
    leader_address = "0x" + "1" * 40
    follower_key = account_abstraction_setting_key("FOLLOWER", follower_address)
    leader_key = account_abstraction_setting_key("LEADER", leader_address)
    engine.cache_account_abstraction_payloads(
        {
            follower_key: {
                "resolved_by_dex": {
                    "": {
                        "account_value_used_for_sizing": "100",
                        "account_value_source": "SPOT_CLEARINGHOUSE_STATE",
                        "account_abstraction_mode": "UNIFIED",
                    }
                }
            },
            leader_key: {
                "resolved_by_dex": {
                    "": {
                        "account_value_used_for_sizing": "1000",
                        "account_value_source": "CURRENT_ACCOUNT_TOTAL",
                        "account_abstraction_mode": "DEX_ABSTRACTION",
                    }
                }
            },
        }
    )

    follower_value, leader_value = asyncio.run(
        engine._resolved_account_values(
            RaisingExecuteSession(),
            leader_address=leader_address,
            dex="",
        )
    )

    assert follower_value["account_value_used_for_sizing"] == "100"
    assert leader_value["account_value_used_for_sizing"] == "1000"


def test_unmanaged_follower_position_can_be_stale_follower_state() -> None:
    follower_state_at = datetime(2026, 5, 5, 14, 53, 16, tzinfo=timezone.utc)
    allocation_updated_at = datetime(2026, 5, 5, 14, 53, 17, tzinfo=timezone.utc)

    assert _unmanaged_follower_position_from_stale_follower_state(
        unmanaged_follower_position=True,
        follower_state_at=follower_state_at,
        allocation_latest_reconcile_at=allocation_updated_at,
    )
    assert not _unmanaged_follower_position_from_stale_follower_state(
        unmanaged_follower_position=True,
        follower_state_at=allocation_updated_at,
        allocation_latest_reconcile_at=follower_state_at,
    )


def test_recent_closed_allocation_checkpoint_marks_unmanaged_snapshot_stale() -> None:
    follower_state_at = datetime(2026, 7, 1, 22, 28, 10, tzinfo=timezone.utc)
    closed_reconcile_at = datetime(2026, 7, 1, 22, 28, 13, tzinfo=timezone.utc)
    rows = [
        SimpleNamespace(
            status="CLOSED",
            last_reconcile_at=closed_reconcile_at,
            updated_at=datetime(2026, 7, 1, 22, 28, 12, tzinfo=timezone.utc),
            created_at=datetime(2026, 7, 1, 22, 27, 59, tzinfo=timezone.utc),
        )
    ]

    allocation_latest_reconcile_at = _latest_allocation_reconcile_at(rows)
    unmanaged = _unmanaged_follower_position_qtys(
        follower_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("210273")},
        allocation_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
    )
    is_state_lag = _unmanaged_follower_position_from_stale_follower_state(
        unmanaged_follower_position=True,
        follower_state_at=follower_state_at,
        allocation_latest_reconcile_at=allocation_latest_reconcile_at,
    )

    blocker = _unmanaged_follower_position_blocker(
        transition_plan=SimpleNamespace(action=AllocationTransitionAction.OPEN),
        unmanaged_qty_by_side=unmanaged,
        unmanaged_position_state_lag=is_state_lag,
        canonical_coin="HEMI",
    )

    assert allocation_latest_reconcile_at == closed_reconcile_at
    assert is_state_lag is True
    assert blocker is None


def test_reduce_scope_uses_allocation_sum_when_follower_snapshot_lags() -> None:
    effective_qty = _effective_aggregate_follower_qty_for_reduce_scope(
        reduce_only=True,
        allocation_mismatch_state_lag=True,
        aggregate_follower_qty=Decimal("0.10"),
        allocation_sum_qty=Decimal("1.00"),
    )

    quantity = _order_quantity_for_transition(
        mark_price=Decimal("100"),
        target_delta_abs=Decimal("40"),
        reduce_only=True,
        transition_plan=SimpleNamespace(close_qty_limit=Decimal("0.40")),
        aggregate_follower_qty=effective_qty,
    )

    assert effective_qty == Decimal("1.00")
    assert quantity == Decimal("0.40")


def test_reduce_pre_scope_does_not_block_unclamped_close_limit_above_actual_follower_qty() -> None:
    allocation = allocation_record(qty="61.23", notional="5148.896483")
    plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE, close_qty_limit=Decimal("32.656"))
    quantity = _order_quantity_for_transition(
        mark_price=Decimal("61.8745"),
        target_delta_abs=Decimal("2015.41145756"),
        reduce_only=True,
        transition_plan=plan,
        aggregate_follower_qty=Decimal("23.97"),
    )

    assert_allocation_scope(
        {
            "action": AllocationTransitionAction.REDUCE.value,
            "leader_id": 1,
            "execution_venue": "HYPERLIQUID",
            "dex": "xyz",
            "canonical_coin": "xyz:USAR",
            "old_side": "LONG",
            "close_qty_limit": plan.close_qty_limit,
        },
        allocation,
        aggregate_follower_qty=None,
        allocation_sum_qty=Decimal("61.23"),
    )
    blockers = _reduce_quantity_guard_blockers(
        reduce_only=True,
        transition_plan=plan,
        current_allocation=allocation,
        rounded_size=quantity,
        aggregate_follower_qty=Decimal("23.97"),
    )

    assert quantity == Decimal("23.97")
    assert blockers == []


def test_reduce_quantity_guard_blocks_size_above_close_qty_limit() -> None:
    blockers = _reduce_quantity_guard_blockers(
        reduce_only=True,
        transition_plan=SimpleNamespace(
            action=AllocationTransitionAction.REDUCE,
            close_qty_limit=Decimal("0.40"),
        ),
        current_allocation=allocation_record(qty="1"),
        rounded_size=Decimal("0.41"),
        aggregate_follower_qty=Decimal("1"),
    )

    assert "REDUCE_QTY_GUARD: rounded order size exceeds planned close_qty_limit" in blockers


def test_reduce_quantity_guard_blocks_size_above_allocation_qty() -> None:
    blockers = _reduce_quantity_guard_blockers(
        reduce_only=True,
        transition_plan=SimpleNamespace(
            action=AllocationTransitionAction.CLOSE,
            close_qty_limit=Decimal("0.50"),
        ),
        current_allocation=allocation_record(qty="0.30"),
        rounded_size=Decimal("0.40"),
        aggregate_follower_qty=Decimal("1"),
    )

    assert "REDUCE_QTY_GUARD: rounded order size exceeds allocation qty" in blockers


def test_reduce_quantity_guard_blocks_size_above_follower_actual_qty() -> None:
    blockers = _reduce_quantity_guard_blockers(
        reduce_only=True,
        transition_plan=SimpleNamespace(
            action=AllocationTransitionAction.REDUCE,
            close_qty_limit=Decimal("0.40"),
        ),
        current_allocation=allocation_record(qty="1"),
        rounded_size=Decimal("0.30"),
        aggregate_follower_qty=Decimal("0.20"),
    )

    assert "REDUCE_QTY_GUARD: rounded order size exceeds follower actual position qty" in blockers


def test_reduce_quantity_guard_allows_size_at_all_limits() -> None:
    blockers = _reduce_quantity_guard_blockers(
        reduce_only=True,
        transition_plan=SimpleNamespace(
            action=AllocationTransitionAction.REDUCE,
            close_qty_limit=Decimal("0.40"),
        ),
        current_allocation=allocation_record(qty="0.40"),
        rounded_size=Decimal("0.40"),
        aggregate_follower_qty=Decimal("0.40"),
    )

    assert blockers == []


def test_pre_submit_guard_allows_stale_allocation_mismatch_for_reduce() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.40"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=True,
        pre_trade_checklist={
            "allocation_scope_guard": True,
            "allocation_mismatch": True,
            "allocation_mismatch_state_lag": True,
        },
    )

    blockers = engine._pre_submit_internal_blockers(
        order=order,
        fill=fill_event(side="A", start_position="2", direction="Close Long"),
        reduce_only=True,
    )

    assert blockers == []


def test_pre_submit_guard_blocks_real_allocation_mismatch_for_reduce() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.40"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=True,
        pre_trade_checklist={
            "allocation_scope_guard": True,
            "allocation_mismatch": True,
            "allocation_mismatch_state_lag": False,
        },
    )

    blockers = engine._pre_submit_internal_blockers(
        order=order,
        fill=fill_event(side="A", start_position="2", direction="Close Long"),
        reduce_only=True,
    )

    assert "INTERNAL_SUBMIT_GUARD: allocation mismatch present" in blockers


def test_pre_submit_guard_blocks_stale_allocation_mismatch_for_open() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="INCREASE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.40"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=False,
        pre_trade_checklist={
            "allocation_mismatch": True,
            "allocation_mismatch_state_lag": True,
        },
    )

    blockers = engine._pre_submit_internal_blockers(
        order=order,
        fill=fill_event(side="B", start_position="2", direction="Open Long"),
        reduce_only=False,
    )

    assert "INTERNAL_SUBMIT_GUARD: allocation mismatch present" in blockers


def test_pre_submit_guard_blocks_unmanaged_follower_position_for_open() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="INCREASE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.40"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=False,
        pre_trade_checklist={
            "unmanaged_follower_position": True,
            "unmanaged_follower_position_state_lag": False,
        },
    )

    blockers = engine._pre_submit_internal_blockers(
        order=order,
        fill=fill_event(side="B", start_position="2", direction="Open Long"),
        reduce_only=False,
    )

    assert "INTERNAL_SUBMIT_GUARD: unmanaged follower position present" in blockers


def test_pre_submit_guard_blocks_unmanaged_follower_position_for_reduce() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.20"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=True,
        pre_trade_checklist={
            "allocation_scope_guard": True,
            "unmanaged_follower_position": True,
            "unmanaged_follower_position_state_lag": False,
        },
    )

    blockers = engine._pre_submit_internal_blockers(
        order=order,
        fill=fill_event(side="A", start_position="2", direction="Close Long"),
        reduce_only=True,
    )

    assert "INTERNAL_SUBMIT_GUARD: unmanaged follower position present" in blockers


def test_pre_submit_guard_blocks_same_side_unmanaged_position_for_reduce_safe_order() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="SELL",
        position_side="LONG",
        order_action="CLOSE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.20"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=True,
        pre_trade_checklist={
            "allocation_scope_guard": True,
            "unmanaged_follower_position": True,
            "unmanaged_follower_position_state_lag": False,
            "unmanaged_follower_position_reduce_safe": True,
        },
    )

    blockers = engine._pre_submit_internal_blockers(
        order=order,
        fill=fill_event(side="A", start_position="2", direction="Close Long"),
        reduce_only=True,
    )

    assert "INTERNAL_SUBMIT_GUARD: unmanaged follower position present" in blockers


def test_pre_submit_guard_blocks_runtime_manual_position_guard() -> None:
    manual_guard = FollowerManualPositionGuard()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
        manual_position_guard=manual_guard,
    )
    fill = fill_event()
    manual_guard.mark(fill.market, reason="manual follower fill")
    order = ExecutionOrder(
        id=645,
        leader_address="0x" + "1" * 40,
        source_fill_id="fill-1",
        source_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="INCREASE",
        order_type="MARKET",
        quantity=Decimal("1"),
        notional=Decimal("100"),
        status="PENDING_SUBMIT",
        pre_trade_checklist={},
    )

    blockers = engine._pre_submit_internal_blockers(order=order, fill=fill, reduce_only=False)

    assert "INTERNAL_SUBMIT_GUARD: manual follower position guard active" in blockers


def test_pre_submit_guard_allows_manual_guard_for_existing_market_owner_allocation() -> None:
    manual_guard = FollowerManualPositionGuard()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
        manual_position_guard=manual_guard,
    )
    fill = fill_event()
    manual_guard.mark(fill.market, reason="manual follower fill after auto owner")
    order = ExecutionOrder(
        id=646,
        allocation_id=77,
        leader_address="0x" + "1" * 40,
        source_fill_id="fill-1",
        source_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="INCREASE",
        order_type="MARKET",
        quantity=Decimal("1"),
        notional=Decimal("100"),
        status="PENDING_SUBMIT",
        pre_trade_checklist={
            "market_owner_guard": {
                "blocked": False,
                "owner_allocation_id": 77,
            },
        },
    )

    blockers = engine._pre_submit_internal_blockers(order=order, fill=fill, reduce_only=False)

    assert _order_market_owner_guard_allows_manual_guard(order)
    assert "INTERNAL_SUBMIT_GUARD: manual follower position guard active" not in blockers


def test_manual_position_guard_clears_only_after_fresh_unmanaged_flat_state() -> None:
    guard = FollowerManualPositionGuard()
    market = MarketKey(dex="", coin="BTC", canonical_coin="BTC", raw_coin="BTC", asset_id=None, venue_symbol="BTC")
    created_at = datetime(2026, 7, 9, 1, 0, 0, tzinfo=timezone.utc)
    guard.mark(market, reason="manual follower fill", observed_at=created_at)

    stale = guard.reconcile(
        market,
        unmanaged_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=created_at - timedelta(milliseconds=1),
    )
    fresh = guard.reconcile(
        market,
        unmanaged_qty_by_side={PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
        follower_state_at=created_at + timedelta(milliseconds=1),
    )

    assert stale is not None
    assert fresh is None
    assert guard.active_entry(market) is None


def test_watcher_reconcile_manual_position_guards_clears_after_manual_flat() -> None:
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=FakeSessionFactory(),
    )
    market = MarketKey(dex="", coin="BTC", canonical_coin="BTC", raw_coin="BTC", asset_id=None, venue_symbol="BTC")
    created_at = datetime(2026, 7, 9, 1, 0, 0, tzinfo=timezone.utc)
    watcher.manual_position_guard.mark(market, reason="manual follower fill", observed_at=created_at)

    async def follower_qtys(db, market):
        return (
            {PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
            created_at + timedelta(seconds=1),
        )

    async def allocation_qtys(db, market):
        return (
            {PositionSide.LONG: Decimal("0"), PositionSide.SHORT: Decimal("0")},
            created_at + timedelta(seconds=1),
        )

    watcher.engine._follower_position_qtys_with_state_at = follower_qtys
    watcher.engine._allocation_sum_qtys_with_latest_reconcile = allocation_qtys

    cleared = asyncio.run(watcher._reconcile_manual_position_guards(FakeSession()))

    assert cleared == 1
    assert watcher.manual_position_guard.active_entry(market) is None


def test_min_order_reduce_block_marks_deferred_reduce_without_losing_state() -> None:
    validator = validate_hyperliquid_order_params(
        dex="",
        canonical_coin="BTC",
        asset_id=0,
        action="REDUCE",
        side="SELL",
        target_delta_notional=Decimal("5"),
        raw_size=Decimal("0.05"),
        raw_price=Decimal("100"),
        market_meta={"name": "BTC", "asset_id": 0, "index": 0, "szDecimals": 5, "maxLeverage": 50},
        order_policy={
            "cloid": "0x" + "1" * 32,
            "is_buy": False,
            "reduce_only": True,
            "tif": "Ioc",
            "order_type": {"limit": {"tif": "Ioc"}},
            "slippage_bps": 0,
            "price_fresh": True,
            "min_order_value": Decimal("10"),
            "effective_leverage": 10,
        },
    )
    transition_plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE, delta_notional=Decimal("-5"))
    allocation = LeaderPositionAllocationRecord(
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        binance_symbol=None,
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("95"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("0.1"),
        status="OPEN",
    )
    order = ExecutionOrder(source_fill_id="fill-1")
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    assert _is_deferred_reduce_block(
        reduce_only=True,
        transition_plan=transition_plan,
        validator_result=validator,
    )
    _mark_deferred_reduce(
        allocation,
        order=order,
        transition_plan=transition_plan,
        quantity=Decimal("0.05"),
        reason="BELOW_MIN_ORDER_VALUE",
        now=now,
    )

    assert allocation.status == "REDUCING"
    assert allocation.allocated_qty == Decimal("1")
    assert allocation.pending_reduce_qty == Decimal("0.05000000")
    assert allocation.pending_reduce_notional == Decimal("5.00000000")
    assert allocation.pending_reduce_reason == "BELOW_MIN_ORDER_VALUE"
    assert allocation.pending_reduce_since == now
    assert allocation.pending_reduce_source_fill_id == "fill-1"

    _clear_deferred_reduce(allocation)
    assert allocation.pending_reduce_qty is None
    assert allocation.pending_reduce_reason is None


def test_pending_reduce_offset_updates_remaining_deferred_reduce() -> None:
    allocation = LeaderPositionAllocationRecord(
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        binance_symbol=None,
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("0.1"),
        status="REDUCING",
        pending_reduce_qty=Decimal("0.05"),
        pending_reduce_notional=Decimal("5"),
        pending_reduce_reason="BELOW_MIN_ORDER_VALUE",
    )
    transition_plan = SimpleNamespace(
        formula_inputs={
            "pending_reduce_offset_notional": "3",
            "pending_reduce_remaining_notional": "2",
            "pending_reduce_remaining_qty": "0.02",
        }
    )

    assert _apply_pending_reduce_offset_from_plan(allocation, transition_plan) is True
    assert allocation.status == "REDUCING"
    assert allocation.pending_reduce_notional == Decimal("2.00000000")
    assert allocation.pending_reduce_qty == Decimal("0.02000000")
    assert allocation.pending_reduce_reason == "BELOW_MIN_ORDER_VALUE"


def test_pending_reduce_offset_clears_deferred_reduce_when_fully_offset() -> None:
    allocation = LeaderPositionAllocationRecord(
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        binance_symbol=None,
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("0.1"),
        status="REDUCING",
        pending_reduce_qty=Decimal("0.05"),
        pending_reduce_notional=Decimal("5"),
        pending_reduce_reason="BELOW_MIN_ORDER_VALUE",
    )
    transition_plan = SimpleNamespace(
        formula_inputs={
            "pending_reduce_offset_notional": "5",
            "pending_reduce_remaining_notional": "0",
            "pending_reduce_remaining_qty": "0",
        }
    )

    assert _apply_pending_reduce_offset_from_plan(allocation, transition_plan) is True
    assert allocation.status == "OPEN"
    assert allocation.pending_reduce_notional is None
    assert allocation.pending_reduce_qty is None
    assert allocation.pending_reduce_reason is None


def test_final_close_below_min_order_value_is_locally_allowed() -> None:
    validator = validate_hyperliquid_order_params(
        dex="",
        canonical_coin="BTC",
        asset_id=0,
        action="CLOSE",
        side="SELL",
        target_delta_notional=Decimal("5"),
        raw_size=Decimal("0.05"),
        raw_price=Decimal("100"),
        market_meta={"name": "BTC", "asset_id": 0, "index": 0, "szDecimals": 5, "maxLeverage": 50},
        order_policy={
            "cloid": "0x" + "1" * 32,
            "is_buy": False,
            "reduce_only": True,
            "tif": "Ioc",
            "order_type": {"limit": {"tif": "Ioc"}},
            "slippage_bps": 0,
            "price_fresh": True,
            "min_order_value": Decimal("10"),
            "effective_leverage": 10,
        },
    )
    transition_plan = SimpleNamespace(action=AllocationTransitionAction.CLOSE)

    overridden, used = _override_final_close_min_order_validator(
        reduce_only=True,
        transition_plan=transition_plan,
        validator_result=validator,
    )

    assert validator.ok is False
    assert validator.errors == ["BELOW_MIN_ORDER_VALUE"]
    assert used is True
    assert overridden.ok is True
    assert overridden.passes_min_order_value is False
    assert any("attempting reduce-only close" in item for item in overridden.warnings)


def test_partial_reduce_below_min_order_value_is_not_close_override() -> None:
    validator = validate_hyperliquid_order_params(
        dex="",
        canonical_coin="BTC",
        asset_id=0,
        action="REDUCE",
        side="SELL",
        target_delta_notional=Decimal("5"),
        raw_size=Decimal("0.05"),
        raw_price=Decimal("100"),
        market_meta={"name": "BTC", "asset_id": 0, "index": 0, "szDecimals": 5, "maxLeverage": 50},
        order_policy={
            "cloid": "0x" + "1" * 32,
            "is_buy": False,
            "reduce_only": True,
            "tif": "Ioc",
            "order_type": {"limit": {"tif": "Ioc"}},
            "slippage_bps": 0,
            "price_fresh": True,
            "min_order_value": Decimal("10"),
            "effective_leverage": 10,
        },
    )
    transition_plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE)

    overridden, used = _override_final_close_min_order_validator(
        reduce_only=True,
        transition_plan=transition_plan,
        validator_result=validator,
    )

    assert used is False
    assert overridden is validator
    assert _is_deferred_reduce_block(
        reduce_only=True,
        transition_plan=transition_plan,
        validator_result=validator,
    )


def test_manual_review_allocation_cannot_be_marked_deferred_reduce() -> None:
    transition_plan = SimpleNamespace(action=AllocationTransitionAction.REDUCE, delta_notional=Decimal("-5"))
    allocation = LeaderPositionAllocationRecord(
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        binance_symbol=None,
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("95"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("0.1"),
        status="NEEDS_MANUAL_REVIEW",
    )
    order = ExecutionOrder(source_fill_id="fill-1")

    _mark_deferred_reduce(
        allocation,
        order=order,
        transition_plan=transition_plan,
        quantity=Decimal("0.05"),
        reason="BELOW_MIN_ORDER_VALUE",
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert allocation.status == "NEEDS_MANUAL_REVIEW"
    assert allocation.pending_reduce_qty is None
    assert allocation.pending_reduce_notional is None
    assert allocation.pending_reduce_reason == "BELOW_MIN_ORDER_VALUE"
    assert allocation.pending_reduce_source_fill_id is None


def test_latency_fields_include_ws_to_submit_and_event_to_ack() -> None:
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        status="FILLED",
        dry_run=False,
    )
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    order.hyperliquid_event_time = base
    order.ws_received_at = base + timedelta(milliseconds=50)
    order.dedupe_done_at = base + timedelta(milliseconds=60)
    order.debounce_released_at = base + timedelta(milliseconds=160)
    order.decision_started_at = base + timedelta(milliseconds=160)
    order.decision_done_at = base + timedelta(milliseconds=190)
    order.order_submit_started_at = base + timedelta(milliseconds=200)
    order.order_ack_at = base + timedelta(milliseconds=300)
    order.order_finalized_at = base + timedelta(milliseconds=350)

    _set_latency_fields(order)

    assert order.event_to_ws_ms == 50
    assert order.ws_to_submit_ms == 150
    assert order.event_to_ack_ms == 300
    assert order.event_to_final_ms == 350
    assert order.total_hot_path_ms == 300


def test_latency_trace_records_split_submit_metrics_and_copies_json() -> None:
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        status="FILLED",
        dry_run=False,
    )
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    order.hyperliquid_event_time = base
    order.ws_received_at = base + timedelta(milliseconds=10)
    order.order_submit_started_at = base + timedelta(milliseconds=200)
    order.order_submit_done_at = base + timedelta(milliseconds=290)
    order.order_ack_at = base + timedelta(milliseconds=290)
    order.order_finalized_at = base + timedelta(milliseconds=310)
    order.latency_trace = {"timestamps": {}, "metrics": {}, "missing_latency_fields": []}
    before = order.latency_trace

    _trace_set(order, "risk_setting_started_at", base + timedelta(milliseconds=100))
    _trace_set(order, "risk_setting_done_at", base + timedelta(milliseconds=160))
    _trace_set(order, "asset_id_hydrate_started_at", base + timedelta(milliseconds=170))
    _trace_set(order, "asset_id_hydrate_done_at", base + timedelta(milliseconds=195))
    _trace_set(order, "sdk_order_call_started_at", base + timedelta(milliseconds=205))
    _trace_set(order, "sdk_exchange_ready_at", base + timedelta(milliseconds=207))
    _trace_set(order, "sdk_http_post_started_at", base + timedelta(milliseconds=220))
    _trace_set(order, "sdk_http_post_done_at", base + timedelta(milliseconds=285))
    _trace_set(order, "exchange_submit_call_started_at", base + timedelta(milliseconds=200))
    _trace_set(order, "exchange_submit_call_done_at", base + timedelta(milliseconds=290))
    _trace_set(order, "exchange_response_parsed_at", base + timedelta(milliseconds=300))
    _trace_set(order, "fill_confirmed_at", base + timedelta(milliseconds=305))
    _trace_set(order, "allocation_update_done_at", base + timedelta(milliseconds=340))
    _set_latency_fields(order)

    metrics = order.latency_trace["metrics"]
    assert order.latency_trace is not before
    assert metrics["risk_setting_ms"] == 60
    assert metrics["asset_id_hydrate_ms"] == 25
    assert metrics["sdk_exchange_resolve_ms"] == 2
    assert metrics["sdk_prepare_sign_ms"] == 15
    assert metrics["http_exchange_response_ms"] == 65
    assert metrics["submit_to_http_response_ms"] == 85
    assert metrics["http_response_to_ack_record_ms"] == 5
    assert metrics["http_response_to_fill_confirmed_ms"] == 20
    assert metrics["fill_confirmed_to_db_update_ms"] == 35
    assert metrics["exchange_submit_call_ms"] == 90
    assert metrics["exchange_response_parse_ms"] == 10


def test_low_latency_required_blocks_when_watcher_not_running() -> None:
    blockers = _low_latency_gate_blockers(
        settings(),
        {
            "low_latency_watcher_running": False,
            "websocket_connected": False,
            "follower_order_updates_subscribed": False,
            "dex_price_cache_status": {},
            "ready_for_low_latency_live": False,
            "poll_fallback_count": 0,
        },
        {"leaders_not_subscribed": []},
    )

    assert any("watcher is not running" in item for item in blockers)
    assert any("WebSocket is not connected" in item for item in blockers)


def test_low_latency_gate_ok_when_running_subscribed_and_prices_fresh() -> None:
    blockers = _low_latency_gate_blockers(
        settings(),
        {
            "low_latency_watcher_running": True,
            "websocket_connected": True,
            "follower_order_updates_subscribed": True,
            "dex_price_cache_status": {"": {"fresh": True}, "xyz": {"fresh": True}},
            "ready_for_low_latency_live": True,
            "poll_fallback_count": 0,
        },
        {"leaders_not_subscribed": []},
    )

    assert blockers == []


def test_poll_fallback_leader_blocks_low_latency_live_by_default() -> None:
    blockers = _low_latency_gate_blockers(
        settings(),
        {
            "low_latency_watcher_running": True,
            "websocket_connected": True,
            "follower_order_updates_subscribed": True,
            "dex_price_cache_status": {"": {"fresh": True}},
            "ready_for_low_latency_live": False,
            "poll_fallback_count": 1,
        },
        {"leaders_not_subscribed": []},
    )

    assert any("poll fallback" in item for item in blockers)


def test_poll_fallback_can_be_explicitly_allowed() -> None:
    blockers = _low_latency_gate_blockers(
        settings(allow_poll_fallback_live=True),
        {
            "low_latency_watcher_running": True,
            "websocket_connected": True,
            "follower_order_updates_subscribed": True,
            "dex_price_cache_status": {"": {"fresh": True}},
            "ready_for_low_latency_live": True,
            "poll_fallback_count": 1,
        },
        {"leaders_not_subscribed": []},
    )

    assert blockers == []


def test_unresolved_order_query_is_scoped_to_leader_and_market() -> None:
    stmt = unresolved_same_market_order_query(
        leader_address="0x" + "A" * 40,
        dex="xyz",
        canonical_coin="xyz:HYUNDAI",
    )
    text = str(stmt)

    assert "execution_orders.leader_address" in text
    assert "execution_orders.dex" in text
    assert "execution_orders.canonical_coin" in text


def test_account_ratio_formula_used_for_fill_driven_sizing() -> None:
    target = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("10000"),
        leader_position_notional=Decimal("1000"),
        follower_account_value=Decimal("399.6"),
        copy_multiplier=Decimal("0.1"),
    )

    assert target == Decimal("3.99600000")


def test_hyperliquid_filled_response_parses_qty_price_and_status() -> None:
    response = {"status": "ok", "response": {"data": {"statuses": [{"filled": {"totalSz": "1.5", "avgPx": "10", "oid": 7}}]}}}

    assert _hyperliquid_status(response) == "FILLED"
    assert _hyperliquid_fill_qty_price(response) == (Decimal("1.5"), Decimal("10"))


def test_hyperliquid_error_response_is_rejected_not_submitted() -> None:
    response = {"status": "ok", "response": {"data": {"statuses": [{"error": "Order has invalid price."}]}}}

    assert _hyperliquid_status(response) == "REJECTED"


def test_market_precision_loaded_from_meta_when_position_has_only_max_leverage() -> None:
    info = PrecisionInfoClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=info,
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    market = MarketKey(
        dex="xyz",
        coin="USAR",
        canonical_coin="xyz:USAR",
        raw_coin="USAR",
        asset_id=36,
        venue_symbol="xyz:USAR",
    )
    position = LatestAccountPosition(raw_payload_masked={"maxLeverage": 10})

    async def run():
        first = await engine._load_market_leverage_plan(FakeSession(), market, position)
        second = await engine._load_market_leverage_plan(FakeSession(), market, position)
        return first, second

    first, second = asyncio.run(run())

    assert first.sz_decimals == 2
    assert second.sz_decimals == 2
    assert info.calls == 1


def test_market_leverage_plan_uses_confirmed_per_market_override() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    market = MarketKey(
        dex="xyz",
        coin="CL",
        canonical_coin="xyz:CL",
        raw_coin="xyz:CL",
        asset_id=29,
        venue_symbol="xyz:CL",
    )
    position = LatestAccountPosition(raw_payload_masked={"maxLeverage": 20, "szDecimals": 3})
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="xyz",
        canonical_coin="xyz:CL",
        desired_margin_mode="ISOLATED",
        desired_leverage=4,
        market_max_leverage=20,
        effective_leverage=4,
        actual_margin_mode="ISOLATED",
        actual_leverage=4,
        status="CONFIRMED",
    )

    plan = asyncio.run(engine._load_market_leverage_plan(SequenceScalarSession([row]), market, position))

    assert plan.effective_leverage == 4
    assert plan.max_leverage == 20
    assert plan.sz_decimals == 3


def test_missing_fill_asset_id_can_use_market_risk_settings_cache() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    market = MarketKey(
        dex="xyz",
        coin="USAR",
        canonical_coin="xyz:USAR",
        raw_coin="xyz:USAR",
        asset_id=None,
        venue_symbol="xyz:USAR",
    )

    class AssetDb(FakeSession):
        async def scalar(self, stmt):
            self.statements.append(stmt)
            return 36

    db = AssetDb()
    asset_id = asyncio.run(engine._load_cached_asset_id(db, market))
    hydrated = engine._fill_with_asset_id(fill_event(coin="USAR", dex="xyz", asset_id=None), asset_id)

    assert asset_id == 36
    assert hydrated.market.asset_id == 36
    assert "market_risk_settings" in str(db.statements[0])
    assert "market_risk_settings.asset_id IS NOT NULL" in str(db.statements[0])


def test_order_submit_timeout_marks_unknown_without_retry() -> None:
    client = TimeoutExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        pre_trade_checklist=valid_order_validator_payload(),
    )

    asyncio.run(engine._submit_hyperliquid_order(FakeSession(), order, fill_event(), reduce_only=False))

    assert client.leverage_updates == [{"coin": "BTC", "leverage": 10, "is_cross": True}]
    assert client.calls == 1
    assert order.status == "UNKNOWN"
    assert order.order_ack_at is not None


def test_local_sdk_payload_error_marks_failed_not_unknown() -> None:
    client = LocalSdkPayloadErrorExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        pre_trade_checklist=valid_order_validator_payload(),
    )

    asyncio.run(engine._submit_hyperliquid_order(FakeSession(), order, fill_event(), reduce_only=False))

    assert client.calls == 1
    assert order.status == "FAILED"
    assert "not submitted" in order.error_message
    assert order.order_finalized_at is not None


def test_ioc_resting_order_is_cancelled_and_risk_recorded() -> None:
    client = RestingExecutionClient()
    db = FakeSession()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        pre_trade_checklist=valid_order_validator_payload(),
    )

    asyncio.run(engine._submit_hyperliquid_order(db, order, fill_event(), reduce_only=False))

    assert client.leverage_updates == [{"coin": "BTC", "leverage": 10, "is_cross": True}]
    assert client.cancels == [{"coin": "BTC", "cloid": "0x" + "1" * 32}]
    assert isinstance(order.request_payload_masked["sz"], str)
    assert isinstance(order.request_payload_masked["limit_px"], str)
    json.dumps(order.request_payload_masked)
    assert order.status == "UNKNOWN"
    assert any(getattr(item, "event_type", "") == "IOC_RESTING_CANCEL_REQUESTED" for item in db.added)


def test_validator_block_prevents_exchange_submit_call() -> None:
    client = TimeoutExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.01"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        pre_trade_checklist=valid_order_validator_payload(raw_size="0.01", raw_price="100"),
    )

    asyncio.run(engine._submit_hyperliquid_order(FakeSession(), order, fill_event(), reduce_only=False))

    assert client.calls == 0
    assert client.leverage_updates == []
    assert order.status == "BLOCKED"
    assert order.dry_run is True
    assert order.order_submit_started_at is None
    assert "minimum order value" in order.error_message


def test_reduce_only_submit_skips_risk_setting_update() -> None:
    client = RestingExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = LeaderPositionAllocationRecord(
        id=1,
        leader_id=1,
        leader_address="0x" + "1" * 40,
        execution_venue="HYPERLIQUID",
        dex="",
        canonical_coin="BTC",
        position_side="LONG",
        allocated_qty=Decimal("1"),
        allocated_notional=Decimal("100"),
        avg_entry_price=Decimal("100"),
        status="OPEN",
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="SELL",
        position_side="LONG",
        order_action="CLOSE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=True,
        pre_trade_checklist={
            **valid_order_validator_payload(
                action="CLOSE",
                side="SELL",
                is_buy=False,
                reduce_only=True,
            ),
            "allocation_scope_guard": True,
        },
    )

    asyncio.run(
        engine._submit_hyperliquid_order(
            AllocationSession(allocation),
            order,
            fill_event(side="A", start_position="1", direction="Close Long"),
            reduce_only=True,
        )
    )

    assert client.leverage_updates == []
    assert client.orders[0]["reduce_only"] is True
    assert order.pre_trade_checklist["follower_risk_setting_source"] == "reduce_only_skip"


def test_reduce_only_submit_blocks_when_allocation_already_flat() -> None:
    client = FilledExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = LeaderPositionAllocationRecord(
        id=1,
        leader_id=1,
        leader_address="0x" + "1" * 40,
        execution_venue="HYPERLIQUID",
        dex="",
        canonical_coin="BTC",
        position_side="LONG",
        allocated_qty=Decimal("0"),
        allocated_notional=Decimal("0"),
        status="CLOSED",
    )
    order = ExecutionOrder(
        id=10,
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_fill_id="fill-flat",
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="SELL",
        position_side="LONG",
        order_action="CLOSE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.5"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=True,
        pre_trade_checklist={
            **valid_order_validator_payload(
                action="CLOSE",
                side="SELL",
                is_buy=False,
                reduce_only=True,
            ),
            "allocation_scope_guard": True,
        },
    )

    asyncio.run(
        engine._submit_hyperliquid_order(
            AllocationSession(allocation),
            order,
            fill_event(side="A", start_position="1", direction="Close Long"),
            reduce_only=True,
        )
    )

    assert client.orders == []
    assert order.status == "BLOCKED"
    assert "allocation already flat" in order.error_message


def test_reduce_only_rejected_when_position_absent_closes_allocation() -> None:
    client = RejectedReduceOnlyExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = LeaderPositionAllocationRecord(
        id=402,
        leader_id=5,
        leader_address="0x" + "9" * 40,
        execution_venue="HYPERLIQUID",
        dex="",
        canonical_coin="CASHCAT",
        hyperliquid_coin="CASHCAT",
        venue_symbol="CASHCAT",
        position_side="SHORT",
        allocated_qty=Decimal("2408"),
        allocated_notional=Decimal("242.822720"),
        target_notional=Decimal("242.822720"),
        avg_entry_price=Decimal("0.10084"),
        status="OPEN",
    )
    order = ExecutionOrder(
        id=5258,
        allocation_id=402,
        leader_id=5,
        leader_address=allocation.leader_address,
        source_fill_id="cashcat-close-fill",
        source_coin="CASHCAT",
        execution_venue="HYPERLIQUID",
        venue_symbol="CASHCAT",
        hyperliquid_coin="CASHCAT",
        canonical_coin="CASHCAT",
        side="BUY",
        position_side="SHORT",
        order_action="CLOSE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("2408"),
        estimated_price=Decimal("0.10084"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=True,
        pre_trade_checklist={
            **valid_order_validator_payload(
                raw_size="2408",
                raw_price="0.10084",
                action="CLOSE",
                side="BUY",
                is_buy=True,
                reduce_only=True,
            ),
            "allocation_scope_guard": True,
        },
    )

    db = AllocationSession(allocation)

    asyncio.run(
        engine._submit_hyperliquid_order(
            db,
            order,
            fill_event(coin="CASHCAT", asset_id=231, side="B", start_position="-1", direction="Close Short"),
            reduce_only=True,
        )
    )

    assert client.orders != []
    assert order.status == "REJECTED"
    assert "Reduce only order would increase position" in order.error_message
    assert allocation.status == "CLOSED"
    assert allocation.allocated_qty == Decimal("0")
    assert allocation.allocated_notional == Decimal("0")
    assert allocation.target_notional == Decimal("0")
    assert allocation.pending_reduce_qty is None
    assert allocation.last_reconcile_at is not None
    assert any(
        getattr(item, "action", "") == "ALLOCATION_CLOSED_AFTER_ABSENT_REDUCE_REJECTION"
        for item in db.added
    )
    assert any(
        getattr(item, "event_type", "") == "ALLOCATION_CLOSED_AFTER_ABSENT_REDUCE_REJECTION"
        for item in db.added
    )


def test_reduce_only_submit_clamps_qty_to_remaining_allocation() -> None:
    client = FilledExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = LeaderPositionAllocationRecord(
        id=1,
        leader_id=1,
        leader_address="0x" + "1" * 40,
        execution_venue="HYPERLIQUID",
        dex="",
        canonical_coin="BTC",
        position_side="LONG",
        allocated_qty=Decimal("0.46"),
        allocated_notional=Decimal("32.2"),
        avg_entry_price=Decimal("70"),
        status="OPEN",
    )
    order = ExecutionOrder(
        id=11,
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_fill_id="fill-clamp",
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1.3"),
        notional=Decimal("92.3"),
        delta_notional=Decimal("-92.3"),
        estimated_price=Decimal("71"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=True,
        pre_trade_checklist={
            **valid_order_validator_payload(
                raw_size="1.3",
                raw_price="71",
                action="REDUCE",
                side="SELL",
                is_buy=False,
                reduce_only=True,
            ),
            "allocation_scope_guard": True,
        },
    )

    asyncio.run(
        engine._submit_hyperliquid_order(
            AllocationSession(allocation),
            order,
            fill_event(side="A", start_position="2", direction="Close Long"),
            reduce_only=True,
        )
    )

    assert len(client.orders) == 1
    assert client.orders[0]["sz"] == Decimal("0.46000000")
    assert order.quantity == Decimal("0.46")
    assert order.status == "FILLED"
    assert order.executed_qty == Decimal("0.46000000")
    assert order.pre_trade_checklist["submit_reduce_allocation_guard"]["clamped"] is True


def test_process_risk_cache_avoids_repeated_submit_update() -> None:
    client = TimeoutExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )

    def make_order(cloid: str) -> ExecutionOrder:
        return ExecutionOrder(
            allocation_id=1,
            leader_address="0x" + "1" * 40,
            source_coin="BTC",
            execution_venue="HYPERLIQUID",
            side="BUY",
            position_side="LONG",
            order_action="OPEN",
            order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
            quantity=Decimal("1"),
            estimated_price=Decimal("100"),
            cloid=cloid,
            status="PENDING_SUBMIT",
            dry_run=False,
            pre_trade_checklist=valid_order_validator_payload(),
        )

    asyncio.run(engine._submit_hyperliquid_order(FakeSession(), make_order("0x" + "1" * 32), fill_event(), reduce_only=False))
    asyncio.run(engine._submit_hyperliquid_order(FakeSession(), make_order("0x" + "2" * 32), fill_event(), reduce_only=False))

    assert client.calls == 2
    assert client.leverage_updates == [{"coin": "BTC", "leverage": 10, "is_cross": True}]


def test_warmed_risk_cache_avoids_submit_path_leverage_update() -> None:
    client = TimeoutExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="",
        canonical_coin="BTC",
        desired_margin_mode="CROSS",
        desired_leverage=10,
        market_max_leverage=50,
        effective_leverage=10,
        actual_margin_mode="CROSS",
        actual_leverage=10,
        status="CONFIRMED",
    )

    asyncio.run(engine.warm_risk_settings_cache(FakeSession(rows=[row])))
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        pre_trade_checklist=valid_order_validator_payload(),
    )

    asyncio.run(engine._submit_hyperliquid_order(FakeSession(), order, fill_event(), reduce_only=False))

    assert client.calls == 1
    assert client.leverage_updates == []
    assert order.pre_trade_checklist["follower_risk_setting_source"] == "process_cache"


def submit_barrier_order(
    *,
    order_id: int,
    action: str,
    reduce_only: bool = False,
    source_fill_id: str | None = None,
) -> ExecutionOrder:
    side = "SELL" if reduce_only else "BUY"
    return ExecutionOrder(
        id=order_id,
        leader_id=1,
        allocation_id=10,
        leader_address=("0x" + "1" * 40).lower(),
        source_fill_id=source_fill_id or f"fill-barrier-{order_id}",
        source_coin="USAR",
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:USAR",
        side=side,
        position_side="LONG",
        order_action=action,
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.5"),
        notional=Decimal("50"),
        estimated_price=Decimal("100"),
        cloid="0x" + f"{order_id:032x}"[-32:],
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=reduce_only,
    )


def test_pending_intent_submit_barrier_allows_consecutive_increases() -> None:
    ledger = PendingIntentLedger()
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10
    first = submit_barrier_order(order_id=601, action="INCREASE")
    second = submit_barrier_order(order_id=602, action="INCREASE")

    ledger.reserve(first, allocation)
    ledger.reserve(second, allocation)

    assert ledger.submit_barriers_before(second) == []


def test_pending_intent_submit_barrier_orders_reduce_after_prior_intent() -> None:
    ledger = PendingIntentLedger()
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10
    increase = submit_barrier_order(order_id=611, action="INCREASE")
    reduce = submit_barrier_order(order_id=612, action="REDUCE", reduce_only=True)

    ledger.reserve(increase, allocation)
    ledger.reserve(reduce, allocation)

    assert [intent.order_id for intent in ledger.submit_barriers_before(reduce)] == [611]
    ledger.release(increase)
    assert ledger.submit_barriers_before(reduce) == []


def test_pending_intent_submit_barrier_allows_parallel_reduces() -> None:
    ledger = PendingIntentLedger()
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10
    first = submit_barrier_order(order_id=613, action="REDUCE", reduce_only=True)
    second = submit_barrier_order(order_id=614, action="REDUCE", reduce_only=True)

    ledger.reserve(first, allocation)
    ledger.reserve(second, allocation)

    assert ledger.submit_barriers_before(second) == []
    assert ledger.reduce_quantity_before(second) == first.quantity


def test_pending_intent_submit_barrier_blocks_open_after_prior_reduce() -> None:
    ledger = PendingIntentLedger()
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10
    reduce = submit_barrier_order(order_id=621, action="REDUCE", reduce_only=True)
    increase = submit_barrier_order(order_id=622, action="INCREASE")

    ledger.reserve(reduce, allocation)
    ledger.reserve(increase, allocation)

    assert [intent.order_id for intent in ledger.submit_barriers_before(increase)] == [621]


def test_recovery_finalized_pending_intent_is_released_before_next_market_plan() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10
    order = submit_barrier_order(order_id=623, action="REDUCE", reduce_only=True)
    engine.pending_intents.reserve(order, allocation)

    market = MarketKey(
        dex="xyz",
        coin="USAR",
        canonical_coin="xyz:USAR",
        raw_coin="xyz:USAR",
        asset_id=1,
        venue_symbol="xyz:USAR",
    )
    db = FakeSession(rows=[(623, "FILLED")])

    asyncio.run(engine._release_resolved_pending_intents_for_market(db, market))

    assert not engine.pending_intents.has_active_order(order)
    assert engine.pending_intents.overlay_allocation(allocation).allocated_qty == Decimal("1")


def test_submit_queue_key_parallelizes_increases_and_reduces() -> None:
    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        db_session_factory=lambda: FakeSession(),
    )
    event = fill_event(coin="USAR", dex="xyz")
    first_increase = submit_barrier_order(order_id=631, action="INCREASE")
    second_increase = submit_barrier_order(order_id=632, action="INCREASE")
    first_reduce = submit_barrier_order(order_id=633, action="REDUCE", reduce_only=True)
    second_reduce = submit_barrier_order(order_id=634, action="REDUCE", reduce_only=True)

    assert watcher._submit_queue_key(event, order=first_increase) != watcher._submit_queue_key(
        event,
        order=second_increase,
    )
    assert watcher._submit_queue_key(event, order=first_reduce) != watcher._submit_queue_key(
        event,
        order=second_reduce,
    )


def test_pending_intent_overlay_prevents_incremental_add_accumulation() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10
    allocation.avg_entry_price = Decimal("100")
    allocation.last_leader_position_size = Decimal("110")
    allocation.last_leader_position_notional = Decimal("11000")
    allocation.copy_multiplier = Decimal("1")
    order = ExecutionOrder(
        id=501,
        leader_id=1,
        allocation_id=10,
        leader_address=("0x" + "1" * 40).lower(),
        source_fill_id="fill-pending-1",
        source_coin="USAR",
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:USAR",
        side="BUY",
        position_side="LONG",
        order_action="INCREASE",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("0.5"),
        notional=Decimal("50"),
        estimated_price=Decimal("100"),
        cloid="0x" + "a" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=False,
        target_notional=Decimal("150"),
        leader_account_value=Decimal("100000"),
        leader_position_notional=Decimal("11000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )

    engine.pending_intents.reserve(order, allocation)
    overlay = engine.pending_intents.overlay_allocation(allocation)
    plan = plan_leader_allocation_transition(
        leader_id=1,
        execution_venue="HYPERLIQUID",
        dex="xyz",
        canonical_coin="xyz:USAR",
        leader_side=PositionSide.LONG,
        leader_position_notional=Decimal("12000"),
        leader_position_size=Decimal("120"),
        leader_account_value_used=Decimal("100000"),
        follower_account_value_used=Decimal("1000"),
        copy_multiplier=Decimal("1"),
        current_allocation=overlay,
        leader_fill_notional=Decimal("1000"),
        leader_previous_position_size=Decimal("110"),
    )

    assert overlay.allocated_qty == Decimal("1.50000000")
    assert overlay.allocated_notional == Decimal("150.00000000")
    assert plan.current_allocation_notional == Decimal("150.00000000")
    assert plan.delta_notional == Decimal("13.63636364")
    assert plan.target_notional == Decimal("163.63636364")


def test_submit_worker_blocks_pending_order_without_runtime_intent() -> None:
    client = TimeoutExecutionClient()
    order = ExecutionOrder(
        id=777,
        leader_id=1,
        allocation_id=10,
        leader_address=("0x" + "1" * 40).lower(),
        source_fill_id="fill-no-intent",
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        dex="",
        canonical_coin="BTC",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        estimated_price=Decimal("100"),
        cloid="0x" + "b" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=False,
        pre_trade_checklist=valid_order_validator_payload(),
    )

    class OrderSession(FakeSession):
        async def get(self, model, key):
            if model is ExecutionOrder and key == order.id:
                return order
            return None

    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        db_session_factory=lambda: OrderSession(),
    )

    async def run() -> None:
        await watcher._enqueue_submit_order(order.id, fill_event())
        await watcher._drain_submit_queues()
        await watcher._cancel_submit_workers()

    asyncio.run(run())

    assert client.calls == 0
    assert order.status == "BLOCKED"
    assert "pending intent missing" in order.error_message


def test_submit_worker_skips_already_claimed_order_without_releasing_intent() -> None:
    client = TimeoutExecutionClient()
    order = submit_barrier_order(order_id=778, action="INCREASE", source_fill_id="fill-claimed")
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10

    class OrderSession(FakeSession):
        async def get(self, model, key):
            if model is ExecutionOrder and key == order.id:
                return order
            return None

    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        db_session_factory=lambda: OrderSession(),
    )
    watcher.engine.pending_intents.reserve(order, allocation)
    order.status = "SUBMITTING"

    async def run() -> None:
        await watcher._enqueue_submit_order(order.id, fill_event(coin="USAR", dex="xyz"))
        await watcher._drain_submit_queues()
        await watcher._cancel_submit_workers()

    asyncio.run(run())

    assert client.calls == 0
    assert watcher.engine.pending_intents.has_active_order(order)


def test_submit_worker_duplicate_enqueue_submits_once_and_applies_once() -> None:
    client = FilledExecutionClient()
    order = submit_barrier_order(order_id=779, action="INCREASE", source_fill_id="fill-dup-submit")
    order.source_coin = "BTC"
    order.dex = ""
    order.canonical_coin = "BTC"
    order.hyperliquid_coin = "BTC"
    order.quantity = Decimal("1")
    order.notional = Decimal("100")
    order.estimated_price = Decimal("100")
    order.pre_trade_checklist = valid_order_validator_payload()
    allocation = LeaderPositionAllocationRecord(
        id=10,
        leader_id=1,
        leader_address=order.leader_address,
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("0.1"),
        status="OPEN",
    )
    applied_events = []

    class OrderSession(FakeSession):
        async def get(self, model, key):
            if model is ExecutionOrder and key == order.id:
                return order
            if model is LeaderPositionAllocationRecord and key == allocation.id:
                return allocation
            return None

        async def scalar(self, stmt):
            self.statements.append(stmt)
            text = str(stmt)
            if "allocation_events" in text and applied_events:
                return 1
            return None

        def add(self, item):
            super().add(item)
            if isinstance(item, AllocationEvent) and item.action == "FILL_APPLIED":
                applied_events.append(item)

    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        db_session_factory=lambda: OrderSession(),
    )
    watcher.engine.pending_intents.reserve(order, allocation)

    async def run() -> None:
        await watcher._enqueue_submit_order(order, fill_event())
        await watcher._enqueue_submit_order(order, fill_event())
        await watcher._drain_submit_queues()
        await watcher._cancel_submit_workers()

    asyncio.run(run())

    assert len(client.orders) == 1, (order.status, order.error_message, watcher.state.last_error)
    assert len(applied_events) == 1
    assert order.status == "FILLED"
    assert allocation.allocated_qty == Decimal("2.00000000")
    assert not watcher.engine.pending_intents.has_active_order(order)


def test_submit_retry_resets_pre_exchange_submit_transient_failure() -> None:
    client = TimeoutExecutionClient()
    order = submit_barrier_order(order_id=780, action="INCREASE", source_fill_id="fill-transient-submit")
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10

    class OrderSession(FakeSession):
        async def get(self, model, key):
            if model is ExecutionOrder and key == order.id:
                return order
            return None

    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        db_session_factory=lambda: OrderSession(),
    )
    watcher.engine.pending_intents.reserve(order, allocation)
    order.status = "SUBMITTING"

    async def run() -> bool:
        return await watcher._prepare_submit_retry_if_safe(order.id)

    assert asyncio.run(run()) is True
    assert order.status == "PENDING_SUBMIT"


def test_submit_retry_does_not_reset_after_exchange_submit_started() -> None:
    client = TimeoutExecutionClient()
    order = submit_barrier_order(order_id=781, action="INCREASE", source_fill_id="fill-transient-sent")
    allocation = allocation_record(qty="1", notional="100")
    allocation.id = 10

    class OrderSession(FakeSession):
        async def get(self, model, key):
            if model is ExecutionOrder and key == order.id:
                return order
            return None

    watcher = HyperliquidLowLatencyWatcher(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        db_session_factory=lambda: OrderSession(),
    )
    watcher.engine.pending_intents.reserve(order, allocation)
    order.status = "SUBMITTING"
    order.order_submit_started_at = datetime.now(timezone.utc)

    async def run() -> bool:
        return await watcher._prepare_submit_retry_if_safe(order.id)

    assert asyncio.run(run()) is False
    assert order.status == "SUBMITTING"


def test_submit_risk_settings_uses_confirmed_per_market_leverage_override() -> None:
    client = TimeoutExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="xyz",
        canonical_coin="xyz:CL",
        desired_margin_mode="ISOLATED",
        desired_leverage=4,
        market_max_leverage=20,
        effective_leverage=4,
        actual_margin_mode="ISOLATED",
        actual_leverage=4,
        status="CONFIRMED",
    )
    order = ExecutionOrder(
        leader_address="0x" + "1" * 40,
        source_coin="xyz:CL",
        execution_venue="HYPERLIQUID",
        side="SELL",
        position_side="SHORT",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
    )

    result, source = asyncio.run(
        engine._submit_risk_settings(
            SequenceScalarSession([row, row]),
            order,
            fill_event(coin="CL", dex="xyz", asset_id=29, side="A", direction="Open Short"),
            reduce_only=False,
        )
    )

    assert result.is_ok
    assert source == "exchange_update"
    assert result.effective_leverage == 4
    assert result.actual_margin_mode == "ISOLATED"
    assert client.leverage_updates == [{"coin": "xyz:CL", "leverage": 4, "is_cross": False}]


def test_known_cross_unsupported_market_no_longer_blocks_hot_path() -> None:
    client = TimeoutExecutionClient()
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    row = MarketRiskSetting(
        execution_venue="HYPERLIQUID",
        account_address=("0x" + "5" * 40).lower(),
        dex="",
        canonical_coin="BTC",
        desired_margin_mode="CROSS",
        desired_leverage=10,
        market_max_leverage=50,
        effective_leverage=10,
        status="FAILED",
        error_message="RISK_SETTING_CONFIRMATION_UNKNOWN: Cross margin is not allowed for this asset.",
        raw_response_masked={"status": "err", "response": "Cross margin is not allowed for this asset."},
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        pre_trade_checklist=valid_order_validator_payload(),
    )

    asyncio.run(engine._submit_hyperliquid_order(SequenceScalarSession([row]), order, fill_event(), reduce_only=False))

    assert client.calls == 1
    assert client.leverage_updates == [{"coin": "BTC", "leverage": 10, "is_cross": True}]
    assert order.status == "UNKNOWN"
    assert order.pre_trade_checklist["follower_risk_setting_source"] == "exchange_update"
    assert order.pre_trade_checklist["follower_margin_mode_confirmed"] is True


def test_risk_setting_block_closes_zero_open_allocation() -> None:
    class RiskBlockedEngine(FillDrivenExecutionEngine):
        async def _submit_risk_settings(self, db, order, fill, *, reduce_only):
            return (
                RiskSettingResult(
                    is_ok=False,
                    status="FAILED",
                    account_address=("0x" + "5" * 40).lower(),
                    dex=fill.market.dex,
                    canonical_coin=fill.market.canonical_coin,
                    desired_margin_mode="CROSS",
                    desired_leverage=10,
                    effective_leverage=10,
                    reason_code="CROSS_MARGIN_NOT_SUPPORTED",
                    reason="cross margin is not allowed",
                ),
                "test",
            )

    allocation = allocation_record(qty="0", notional="0", status="OPEN")
    allocation.id = 1
    client = TimeoutExecutionClient()
    engine = RiskBlockedEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=client,
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    order = ExecutionOrder(
        allocation_id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="OPEN",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        estimated_price=Decimal("100"),
        cloid="0x" + "1" * 32,
        status="PENDING_SUBMIT",
        dry_run=False,
        reduce_only=False,
        pre_trade_checklist=valid_order_validator_payload(),
    )

    asyncio.run(engine._submit_hyperliquid_order(AllocationGetSession(allocation), order, fill_event(), reduce_only=False))

    assert order.status == "BLOCKED"
    assert client.calls == 0
    assert allocation.status == "CLOSED"
    assert "zero-fill OPEN allocation" in allocation.pending_reduce_reason
    assert "CROSS_MARGIN_NOT_SUPPORTED" in allocation.pending_reduce_reason


def test_filled_order_updates_allocation_not_global_state() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = LeaderPositionAllocationRecord(
        id=10,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("0"),
        allocated_qty=Decimal("0"),
        copy_multiplier=Decimal("0.1"),
        status="OPEN",
    )

    class AllocationDb(FakeSession):
        async def get(self, model, key):
            return allocation

    order = ExecutionOrder(
        allocation_id=10,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        order_action="OPEN",
        order_type="MARKET",
        quantity=Decimal("1"),
        executed_qty=Decimal("1"),
        avg_fill_price=Decimal("100"),
        status="FILLED",
        dry_run=False,
    )

    asyncio.run(engine._apply_allocation_fill(AllocationDb(), order))

    assert allocation.allocated_qty == Decimal("1")
    assert allocation.allocated_notional == Decimal("100.00000000")
    assert allocation.status == "OPEN"
    assert order.latency_trace is not None


def test_allocation_fill_apply_is_idempotent_by_execution_order() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = LeaderPositionAllocationRecord(
        id=10,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("0.1"),
        status="OPEN",
    )

    class IdempotentAllocationDb(FakeSession):
        async def scalar(self, stmt):
            self.statements.append(stmt)
            if "allocation_events" in str(stmt):
                return 999
            return None

        async def get(self, model, key):
            if model is LeaderPositionAllocationRecord and key == allocation.id:
                return allocation
            return None

    order = ExecutionOrder(
        id=12345,
        allocation_id=10,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        order_action="OPEN",
        order_type="MARKET",
        quantity=Decimal("1"),
        executed_qty=Decimal("1"),
        avg_fill_price=Decimal("100"),
        status="FILLED",
        dry_run=False,
    )

    asyncio.run(engine._apply_allocation_fill(IdempotentAllocationDb(), order))

    assert allocation.allocated_qty == Decimal("1")
    assert allocation.allocated_notional == Decimal("100")


def test_allocation_fill_refreshes_stale_identity_before_reduce() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    stale_allocation = LeaderPositionAllocationRecord(
        id=10,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="INTC",
        dex="xyz",
        canonical_coin="xyz:INTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="INTC",
        position_side="LONG",
        target_notional=Decimal("354"),
        allocated_notional=Decimal("354"),
        allocated_qty=Decimal("3.19"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    fresh_allocation = LeaderPositionAllocationRecord(
        id=10,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="INTC",
        dex="xyz",
        canonical_coin="xyz:INTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="INTC",
        position_side="LONG",
        target_notional=Decimal("284"),
        allocated_notional=Decimal("284"),
        allocated_qty=Decimal("2.84"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )

    class RefreshingAllocationDb(FakeSession):
        def __init__(self):
            super().__init__()
            self.scalar_calls = 0

        async def scalar(self, stmt):
            self.statements.append(stmt)
            self.scalar_calls += 1
            if self.scalar_calls == 2:
                return fresh_allocation
            return None

        async def get(self, model, key):
            if model is LeaderPositionAllocationRecord and key == stale_allocation.id:
                return stale_allocation
            return None

    order = ExecutionOrder(
        id=12346,
        allocation_id=10,
        leader_address="0x" + "1" * 40,
        source_coin="INTC",
        canonical_coin="xyz:INTC",
        execution_venue="HYPERLIQUID",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        order_type="MARKET",
        quantity=Decimal("0.13"),
        executed_qty=Decimal("0.13"),
        avg_fill_price=Decimal("100"),
        status="FILLED",
        reduce_only=True,
        dry_run=False,
    )

    db = RefreshingAllocationDb()
    asyncio.run(engine._apply_allocation_fill(db, order))

    assert stale_allocation.allocated_qty == Decimal("3.19")
    assert fresh_allocation.allocated_qty == Decimal("2.71")
    assert fresh_allocation.allocated_notional == Decimal("271.00000000")
    assert db.flushes == 1


def test_filled_short_close_updates_allocation_without_follower_projection() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = LeaderPositionAllocationRecord(
        id=10,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="HEMI",
        dex="",
        canonical_coin="HEMI",
        execution_venue="HYPERLIQUID",
        venue_symbol="HEMI",
        position_side="SHORT",
        target_notional=Decimal("1100"),
        allocated_notional=Decimal("1100"),
        allocated_qty=Decimal("210273"),
        avg_entry_price=Decimal("0.005231"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    position = LatestAccountPosition(
        id=20,
        account_state_id=1,
        role="FOLLOWER",
        address=("0x" + "5" * 40).lower(),
        dex="",
        coin="HEMI",
        canonical_coin="HEMI",
        raw_coin="HEMI",
        side="SHORT",
        size=Decimal("-210273"),
        notional=Decimal("-1100"),
        entry_px=Decimal("0.005231"),
        mark_px=Decimal("0.00523"),
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 1, 22, 28, 10, tzinfo=timezone.utc),
    )

    class NoProjectionSession(FakeSession):
        async def scalar(self, stmt):
            self.statements.append(stmt)
            text = compiled_sql(stmt)
            if "allocation_events" in text:
                return None
            if "leader_position_allocations" in text:
                return allocation
            if "latest_account_states" in text:
                raise AssertionError("allocation fill must not read follower account state")
            if "latest_account_positions" in text:
                raise AssertionError("allocation fill must not read follower positions")
            return None

    order = ExecutionOrder(
        id=12346,
        allocation_id=10,
        leader_address="0x" + "1" * 40,
        source_coin="HEMI",
        dex="",
        canonical_coin="HEMI",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="SHORT",
        order_action="CLOSE",
        order_type="MARKET",
        quantity=Decimal("210273"),
        executed_qty=Decimal("210273"),
        avg_fill_price=Decimal("0.00523"),
        status="FILLED",
        dry_run=False,
    )

    db = NoProjectionSession()
    asyncio.run(engine._apply_allocation_fill(db, order))

    assert allocation.allocated_qty == Decimal("0")
    assert allocation.allocated_notional == Decimal("0E-8")
    assert allocation.status == "CLOSED"
    assert position.active is True
    assert position.status == "OPEN"
    assert position.size == Decimal("-210273")
    assert position.notional == Decimal("-1100")
    assert position.closed_at is None
    assert not (position.raw_payload_masked or {}).get("local_fill_projection")
    projection_sql = [
        compiled_sql(stmt)
        for stmt in db.statements
        if "latest_account_states" in compiled_sql(stmt) or "latest_account_positions" in compiled_sql(stmt)
    ]
    assert not projection_sql


def test_filled_increase_updates_allocation_without_follower_projection() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    allocation = LeaderPositionAllocationRecord(
        id=10,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        venue_symbol="BTC",
        position_side="LONG",
        target_notional=Decimal("340"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    position = LatestAccountPosition(
        id=20,
        account_state_id=1,
        role="FOLLOWER",
        address=("0x" + "5" * 40).lower(),
        dex="",
        coin="BTC",
        canonical_coin="BTC",
        raw_coin="BTC",
        side="LONG",
        size=Decimal("1"),
        notional=Decimal("100"),
        entry_px=Decimal("100"),
        mark_px=Decimal("100"),
        active=True,
        status="OPEN",
        last_update_at=datetime(2026, 7, 1, 22, 28, 10, tzinfo=timezone.utc),
    )

    class NoProjectionSession(FakeSession):
        async def scalar(self, stmt):
            self.statements.append(stmt)
            text = compiled_sql(stmt)
            if "allocation_events" in text:
                return None
            if "leader_position_allocations" in text:
                return allocation
            if "latest_account_states" in text:
                raise AssertionError("allocation fill must not read follower account state")
            if "latest_account_positions" in text:
                raise AssertionError("allocation fill must not read follower positions")
            return None

    order = ExecutionOrder(
        id=12347,
        allocation_id=10,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        dex="",
        canonical_coin="BTC",
        execution_venue="HYPERLIQUID",
        side="BUY",
        position_side="LONG",
        order_action="INCREASE",
        order_type="MARKET",
        quantity=Decimal("2"),
        executed_qty=Decimal("2"),
        avg_fill_price=Decimal("120"),
        status="FILLED",
        dry_run=False,
    )

    db = NoProjectionSession()
    asyncio.run(engine._apply_allocation_fill(db, order))

    assert allocation.allocated_qty == Decimal("3")
    assert allocation.allocated_notional == Decimal("340.00000000")
    assert allocation.avg_entry_price == Decimal("113.33333333")
    assert position.active is True
    assert position.status == "OPEN"
    assert position.size == Decimal("1")
    assert position.notional == Decimal("100")
    assert position.entry_px == Decimal("100")
    assert not (position.raw_payload_masked or {}).get("local_fill_projection")
    projection_sql = [
        compiled_sql(stmt)
        for stmt in db.statements
        if "latest_account_states" in compiled_sql(stmt) or "latest_account_positions" in compiled_sql(stmt)
    ]
    assert not projection_sql


def test_blocked_unallocated_open_marks_position_wait_until_flat() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    db = FakeSession()
    state = LatestAccountState(
        role="LEADER",
        address=("0x" + "1" * 40).lower(),
        dex="hyna",
        account_label="leader",
        account_value=Decimal("10000"),
    )
    position = LatestAccountPosition(
        account_state_id=1,
        coin="ZEC",
        dex="hyna",
        canonical_coin="hyna:ZEC",
        side="LONG",
        size=Decimal("50"),
        notional=Decimal("20000"),
        entry_px=Decimal("400"),
        mark_px=Decimal("401"),
        active=True,
    )

    asyncio.run(
        engine._mark_current_position_wait_until_flat(
            db,
            leader=leader(),
            leader_state=state,
            leader_position=position,
            market=MarketKey(
                dex="hyna",
                coin="ZEC",
                canonical_coin="hyna:ZEC",
                raw_coin="hyna:ZEC",
                asset_id=1,
                venue_symbol="hyna:ZEC",
            ),
            reason="Auto-copy open blocked before execution: test",
        )
    )

    baselines = [item for item in db.added if isinstance(item, LeaderPositionBaseline)]
    assert len(baselines) == 1
    assert baselines[0].baseline_status == "WAIT_UNTIL_FLAT"
    assert baselines[0].canonical_coin == "hyna:ZEC"
    assert baselines[0].notional_at_enable == Decimal("20000")


def test_low_latency_market_identity_queries_use_case_insensitive_canonical_coin_filters() -> None:
    engine = FillDrivenExecutionEngine(
        settings=settings(),
        info_client=NoopInfoClient(),
        execution_client=TimeoutExecutionClient(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    market = MarketKey(
        dex="xyz",
        coin="HYUNDAI",
        canonical_coin="XYZ:HYUNDAI",
        raw_coin="xyz:HYUNDAI",
        asset_id=1,
        venue_symbol="xyz:HYUNDAI",
    )

    db = FakeSession()
    asyncio.run(engine._load_cached_asset_id(db, market))
    assert "upper(market_risk_settings.canonical_coin)" in compiled_sql(db.statements[-1])

    db = FakeSession()
    asyncio.run(engine._load_allocation(db, leader(), market, PositionSide.SHORT))
    assert "upper(leader_position_allocations.canonical_coin)" in compiled_sql(db.statements[-1])

    inactive = LeaderPositionAllocationRecord(
        id=1,
        leader_id=1,
        leader_address=("0x" + "1" * 40).lower(),
        hyperliquid_coin="HYUNDAI",
        dex="xyz",
        canonical_coin="xyz:HYUNDAI",
        execution_venue="HYPERLIQUID",
        venue_symbol="xyz:HYUNDAI",
        position_side="SHORT",
        target_notional=Decimal("0"),
        allocated_notional=Decimal("0"),
        allocated_qty=Decimal("0"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    owner = LeaderPositionAllocationRecord(
        id=2,
        leader_id=2,
        leader_address=("0x" + "2" * 40).lower(),
        hyperliquid_coin="HYUNDAI",
        dex="xyz",
        canonical_coin="xyz:HYUNDAI",
        execution_venue="HYPERLIQUID",
        venue_symbol="xyz:HYUNDAI",
        position_side="SHORT",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    db = FakeSession([inactive, owner])
    assert asyncio.run(engine._load_market_owner_allocation(db, market)) is owner
    owner_sql = compiled_sql(db.statements[-1])
    assert "upper(leader_position_allocations.canonical_coin)" in owner_sql
    assert "leader_position_allocations.leader_address =" not in owner_sql
    assert "join leader_configs" in owner_sql
    assert "leader_configs.enabled is true" in owner_sql
    assert "leader_configs.deleted_at is null" in owner_sql

    pending_owner = LeaderPositionAllocationRecord(
        id=3,
        leader_id=3,
        leader_address=("0x" + "3" * 40).lower(),
        hyperliquid_coin="HYUNDAI",
        dex="xyz",
        canonical_coin="xyz:HYUNDAI",
        execution_venue="HYPERLIQUID",
        venue_symbol="xyz:HYUNDAI",
        position_side="SHORT",
        target_notional=Decimal("0"),
        allocated_notional=Decimal("0"),
        allocated_qty=Decimal("0"),
        copy_multiplier=Decimal("1"),
        status=PENDING_OPEN_STATUS,
        pending_reduce_reason=PENDING_OPEN_REASON,
    )
    db = FakeSession([inactive, pending_owner, owner])
    assert asyncio.run(engine._load_market_owner_allocation(db, market)) is pending_owner

    db = FakeSession()
    asyncio.run(engine._allocation_sum_qty(db, market, PositionSide.SHORT))
    allocation_sum_sql = compiled_sql(db.statements[-1])
    assert "upper(leader_position_allocations.canonical_coin)" in allocation_sum_sql
    assert "join leader_configs" in allocation_sum_sql
    assert "leader_configs.enabled is true" in allocation_sum_sql
    assert "leader_configs.deleted_at is null" in allocation_sum_sql

    follower_state = LatestAccountState(
        id=11,
        role="FOLLOWER",
        address=("0x" + "5" * 40).lower(),
        dex="xyz",
        account_label="follower",
        account_value=Decimal("10000"),
        last_update_at=datetime.now(timezone.utc),
    )
    db = SequenceScalarSession([follower_state, None])
    asyncio.run(engine._follower_position_qty(db, market, PositionSide.SHORT))
    assert "upper(latest_account_positions.canonical_coin)" in compiled_sql(db.statements[-1])

    db = FakeSession()
    asyncio.run(engine._opposite_aggregate_allocation_exists(db, leader(), market, PositionSide.SHORT))
    opposite_sql = compiled_sql(db.statements[-1])
    assert "upper(leader_position_allocations.canonical_coin)" in opposite_sql
    assert "join leader_configs" in opposite_sql
    assert "leader_configs.enabled is true" in opposite_sql
    assert "leader_configs.deleted_at is null" in opposite_sql

    recovery_stmt = unresolved_same_market_order_query(
        leader_address=("0x" + "1" * 40),
        dex="xyz",
        canonical_coin="XYZ:HYUNDAI",
    )
    assert "upper(execution_orders.canonical_coin)" in compiled_sql(recovery_stmt)
