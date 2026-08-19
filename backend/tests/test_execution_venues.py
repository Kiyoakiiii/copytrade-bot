import asyncio
import json
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.api.preflight import _terminal_flat_leader_allocation_residuals
from app.models import ExecutionOrder, LeaderPositionAllocationRecord
from app.services.allocations import (
    AllocationStatus,
    AggregatePosition,
    LeaderPositionAllocation,
    apply_close_to_allocation,
    validate_aggregate_allocations_vs_binance,
    validate_aggregate_allocations_vs_venue,
)
from app.services.execution_router import (
    ExecutionRouter,
    ExecutionVenue,
    FallbackVenue,
    VenueAvailability,
    VenuePreference,
    VenueRouteStatus,
)
from app.services.hyperliquid_execution import (
    HyperliquidExecutionClient,
    HyperliquidRiskSettingsService,
    WebSocketActionNotAccepted,
    WebSocketActionNotSent,
    WebSocketActionSubmissionUncertain,
    _HyperliquidWebSocketActionTransport,
    _SignedActionSlot,
    _resolve_sdk_asset_id,
    _websocket_url_for_exchange,
    build_hyperliquid_cloid,
    build_hyperliquid_ioc_order,
    recover_hyperliquid_unknown_order,
    validate_hyperliquid_order_params,
)
from app.services.order_recovery import (
    _apply_hyperliquid_allocation_delta,
    _apply_hyperliquid_order_response,
    _recovery_order_due,
    _is_unstarted_hyperliquid_outbox_order,
    _resubmit_unstarted_hyperliquid_order,
    _replay_persisted_hyperliquid_action,
    _signed_action_hash,
    _should_defer_unknown_oid_recovery,
    _should_resubmit_stale_unknown_oid_order,
    _should_resubmit_unstarted_hyperliquid_order,
)
from app.services.target_position import PositionSide
from app.services.venue_config import default_venue_policy, venue_live_allowed


def compiled_sql(stmt) -> str:
    return str(stmt.compile(compile_kwargs={"literal_binds": True})).lower()


def test_cancelled_signed_action_slot_wait_does_not_leak_submit_capacity() -> None:
    semaphore = threading.BoundedSemaphore(1)
    assert semaphore.acquire(blocking=False) is True

    async def scenario() -> None:
        slot = _SignedActionSlot(semaphore, None)
        task = asyncio.create_task(slot.__aenter__())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        semaphore.release()
        # Give any cancelled waiter a chance to run. The previous to_thread
        # implementation could acquire here after cancellation and leak it.
        await asyncio.sleep(0.02)
        assert semaphore.acquire(blocking=False) is True
        semaphore.release()

    asyncio.run(scenario())


def allocation(
    leader_id: int,
    venue: ExecutionVenue,
    side: PositionSide,
    qty: str,
    *,
    coin: str = "BTC",
    symbol: str | None = "BTCUSDT",
    account: str = "hl-main",
) -> LeaderPositionAllocation:
    return LeaderPositionAllocation(
        leader_id=leader_id,
        leader_address=f"0xleader{leader_id}",
        hyperliquid_coin=coin,
        binance_symbol=symbol,
        execution_venue=venue,
        venue_account=account,
        venue_symbol=coin if venue == ExecutionVenue.HYPERLIQUID else (symbol or coin),
        position_side=side,
        target_notional=Decimal("10000"),
        allocated_notional=Decimal("10000"),
        allocated_qty=Decimal(qty),
        avg_entry_price=Decimal("50000"),
        last_leader_account_value=Decimal("80000"),
        last_leader_position_notional=Decimal("10000"),
        copy_multiplier=Decimal("1"),
        status=AllocationStatus.OPEN,
    )


def test_default_preferred_venue_is_hyperliquid() -> None:
    assert default_venue_policy().preferred_venue == VenuePreference.HYPERLIQUID


def test_binance_mapping_missing_uses_hyperliquid_when_coin_tradable() -> None:
    result = ExecutionRouter().route_order(
        leader_id=1,
        hyperliquid_coin="PURR",
        preferred_venue=VenuePreference.HYPERLIQUID,
        fallback_venue=FallbackVenue.NONE,
        hyperliquid=VenueAvailability(available=True, reason="ok"),
        binance=VenueAvailability(available=False, reason="mapping missing"),
    )
    assert result.status == VenueRouteStatus.OK
    assert result.execution_venue == ExecutionVenue.HYPERLIQUID


def test_binance_mapping_present_but_preferred_hyperliquid_still_hyperliquid() -> None:
    result = ExecutionRouter().route_order(
        leader_id=1,
        hyperliquid_coin="BTC",
        preferred_venue=VenuePreference.HYPERLIQUID,
        fallback_venue=FallbackVenue.BINANCE,
        hyperliquid=VenueAvailability(available=True, reason="ok"),
        binance=VenueAvailability(available=True, venue_symbol="BTCUSDT", reason="ok"),
    )
    assert result.execution_venue == ExecutionVenue.HYPERLIQUID


def test_preferred_binance_with_valid_mapping_routes_binance() -> None:
    result = ExecutionRouter().route_order(
        leader_id=1,
        hyperliquid_coin="BTC",
        preferred_venue=VenuePreference.BINANCE,
        fallback_venue=FallbackVenue.HYPERLIQUID,
        hyperliquid=VenueAvailability(available=True, reason="ok"),
        binance=VenueAvailability(available=True, venue_symbol="BTCUSDT", reason="ok"),
    )
    assert result.execution_venue == ExecutionVenue.BINANCE
    assert result.venue_symbol == "BTCUSDT"


def test_auto_preference_prioritizes_hyperliquid() -> None:
    result = ExecutionRouter().route_order(
        leader_id=1,
        hyperliquid_coin="BTC",
        preferred_venue=VenuePreference.AUTO,
        fallback_venue=FallbackVenue.BINANCE,
        hyperliquid=VenueAvailability(available=True, venue_symbol="BTC", reason="ok"),
        binance=VenueAvailability(available=True, venue_symbol="BTCUSDT", reason="ok"),
    )
    assert result.execution_venue == ExecutionVenue.HYPERLIQUID


def test_hyperliquid_unavailable_binance_fallback_enabled_routes_binance() -> None:
    result = ExecutionRouter().route_order(
        leader_id=1,
        hyperliquid_coin="BTC",
        preferred_venue=VenuePreference.AUTO,
        fallback_venue=FallbackVenue.BINANCE,
        hyperliquid=VenueAvailability(available=False, reason="hl down"),
        binance=VenueAvailability(available=True, venue_symbol="BTCUSDT", reason="ok"),
    )
    assert result.execution_venue == ExecutionVenue.BINANCE
    assert result.reason == "BINANCE_FALLBACK"


def test_both_venues_unavailable_blocks() -> None:
    result = ExecutionRouter().route_order(
        leader_id=1,
        hyperliquid_coin="NOPE",
        preferred_venue=VenuePreference.AUTO,
        fallback_venue=FallbackVenue.BINANCE,
        hyperliquid=VenueAvailability(available=False, reason="missing meta"),
        binance=VenueAvailability(available=False, reason="mapping missing"),
    )
    assert result.status == VenueRouteStatus.BLOCKED


def test_binance_mapping_missing_does_not_block_hyperliquid_tradable_coin() -> None:
    result = ExecutionRouter().route_order(
        leader_id=1,
        hyperliquid_coin="HYPE",
        preferred_venue=VenuePreference.AUTO,
        fallback_venue=FallbackVenue.BINANCE,
        hyperliquid=VenueAvailability(available=True, venue_symbol="HYPE", reason="ok"),
        binance=VenueAvailability(available=False, reason="Binance mapping missing"),
    )
    assert result.execution_venue == ExecutionVenue.HYPERLIQUID
    assert result.reason == "HYPERLIQUID_PRIMARY"


def test_hyperliquid_and_binance_allocations_are_venue_isolated() -> None:
    hl = allocation(1, ExecutionVenue.HYPERLIQUID, PositionSide.LONG, "0.2")
    bn = allocation(1, ExecutionVenue.BINANCE, PositionSide.LONG, "0.2")
    assert hl.execution_venue != bn.execution_venue
    assert hl.venue_symbol == "BTC"
    assert bn.venue_symbol == "BTCUSDT"


def test_leader1_hyperliquid_close_preserves_leader2_hyperliquid_allocation() -> None:
    leader1 = allocation(1, ExecutionVenue.HYPERLIQUID, PositionSide.LONG, "0.2")
    leader2 = allocation(2, ExecutionVenue.HYPERLIQUID, PositionSide.LONG, "0.1")
    _, closed = apply_close_to_allocation(
        leader1,
        close_qty=Decimal("0.2"),
        binance_position_qty=Decimal("0.3"),
    )
    assert closed.status == AllocationStatus.CLOSED
    assert leader2.allocated_qty == Decimal("0.1")


def test_hyperliquid_close_does_not_affect_binance_allocation() -> None:
    hl = allocation(1, ExecutionVenue.HYPERLIQUID, PositionSide.LONG, "0.2")
    bn = allocation(1, ExecutionVenue.BINANCE, PositionSide.LONG, "0.3")
    _, closed = apply_close_to_allocation(
        hl,
        close_qty=Decimal("0.2"),
        binance_position_qty=Decimal("0.2"),
    )
    assert closed.status == AllocationStatus.CLOSED
    assert bn.allocated_qty == Decimal("0.3")


def test_hyperliquid_same_direction_allocations_aggregate_by_venue_account() -> None:
    result = validate_aggregate_allocations_vs_venue(
        [
            allocation(1, ExecutionVenue.HYPERLIQUID, PositionSide.LONG, "0.2"),
            allocation(2, ExecutionVenue.HYPERLIQUID, PositionSide.LONG, "0.1"),
            allocation(1, ExecutionVenue.BINANCE, PositionSide.LONG, "0.4"),
        ],
        venue=ExecutionVenue.HYPERLIQUID,
        venue_symbol="BTC",
        venue_account="hl-main",
        long_qty=Decimal("0.3"),
        short_qty=Decimal("0"),
        tolerance=Decimal("0.0001"),
    )
    assert result.ok is True


def test_hyperliquid_opposite_direction_same_account_blocks() -> None:
    result = ExecutionRouter().validate_hyperliquid_netting_constraint(
        [
            allocation(1, ExecutionVenue.HYPERLIQUID, PositionSide.LONG, "0.2"),
            allocation(2, ExecutionVenue.HYPERLIQUID, PositionSide.SHORT, "0.1"),
        ],
        venue_account="hl-main",
        coin="BTC",
    )
    assert result.status == VenueRouteStatus.BLOCKED
    assert "opposite" in result.reason.lower()


def test_binance_venue_still_allows_long_and_short_allocations() -> None:
    result = validate_aggregate_allocations_vs_binance(
        [
            allocation(1, ExecutionVenue.BINANCE, PositionSide.LONG, "0.2"),
            allocation(2, ExecutionVenue.BINANCE, PositionSide.SHORT, "0.1"),
        ],
        AggregatePosition(symbol="BTCUSDT", long_qty=Decimal("0.2"), short_qty=Decimal("0.1")),
        tolerance=Decimal("0.0001"),
    )
    assert result.ok is True


def test_hyperliquid_open_uses_ioc_market_equivalent_no_gtc() -> None:
    order = build_hyperliquid_ioc_order(
        coin="BTC",
        is_buy=True,
        quantity=Decimal("0.01"),
        reference_price=Decimal("50000"),
        slippage_bps=100,
        reduce_only=False,
        cloid="0x" + "1" * 32,
    )
    assert order["order_type"] == {"limit": {"tif": "Ioc"}}
    assert order["limit_px"] == Decimal("52500.00000000")
    assert order["reduce_only"] is False


def test_hyperliquid_close_uses_reduce_only_true() -> None:
    order = build_hyperliquid_ioc_order(
        coin="BTC",
        is_buy=False,
        quantity=Decimal("0.01"),
        reference_price=Decimal("50000"),
        slippage_bps=100,
        reduce_only=True,
        cloid="0x" + "2" * 32,
    )
    assert order["order_type"]["limit"]["tif"] == "Ioc"
    assert order["limit_px"] == Decimal("47500.00000000")
    assert order["reduce_only"] is True


def test_hyperliquid_reduce_only_false_for_open() -> None:
    order = build_hyperliquid_ioc_order(
        coin="ETH",
        is_buy=False,
        quantity=Decimal("1"),
        reference_price=Decimal("3000"),
        slippage_bps=50,
        reduce_only=False,
        cloid="0x" + "3" * 32,
    )
    assert order["reduce_only"] is False
    assert order["limit_px"] == Decimal("2850.00000000")


def test_hyperliquid_ioc_order_rounds_price_and_size_to_market_precision() -> None:
    order = build_hyperliquid_ioc_order(
        coin="USAR",
        dex="xyz",
        is_buy=True,
        quantity=Decimal("0.10442719"),
        reference_price=Decimal("26.4225"),
        slippage_bps=100,
        reduce_only=False,
        cloid="0x" + "4" * 32,
        sz_decimals=2,
    )

    assert order["sz"] == Decimal("0.10")
    assert order["limit_px"] == Decimal("27.744")


def validator(
    *,
    raw_size: str = "0.147",
    raw_price: str = "26.4225",
    target: str = "20",
    side: str = "BUY",
    dex: str = "xyz",
    canonical: str = "xyz:USAR",
    asset_id: int | None = 36,
    market_meta: dict | None = None,
    order_policy: dict | None = None,
):
    return validate_hyperliquid_order_params(
        dex=dex,
        canonical_coin=canonical,
        asset_id=asset_id,
        action="OPEN",
        side=side,
        target_delta_notional=Decimal(target),
        raw_size=Decimal(raw_size),
        raw_price=Decimal(raw_price),
        market_meta=market_meta if market_meta is not None else {"name": canonical, "asset_id": asset_id, "szDecimals": 2, "maxLeverage": 10},
        order_policy={
            "cloid": "0x" + "4" * 32,
            "tif": "Ioc",
            "order_type": {"limit": {"tif": "Ioc"}},
            "slippage_bps": 100,
            "min_order_value": 10,
            "price_fresh": True,
            "effective_leverage": 10,
            **(order_policy or {}),
        },
    )


def test_validator_rounds_size_down_only() -> None:
    result = validator(raw_size="0.147")

    assert result.rounded_size == Decimal("0.14")
    assert result.rounded_size < result.raw_size


def test_validator_blocks_rounded_size_zero() -> None:
    result = validator(raw_size="0.001")

    assert result.block_reason == "BLOCKED_TOO_SMALL"
    assert "BLOCKED_TOO_SMALL" in result.errors


def test_validator_blocks_below_min_and_does_not_resize_to_min() -> None:
    result = validator(raw_size="0.14", target="9.99")

    assert result.estimated_notional < Decimal("10")
    assert result.rounded_size == Decimal("0.14")
    assert "BELOW_MIN_ORDER_VALUE" in result.errors


def test_validator_blocks_reduce_only_below_observed_min_order() -> None:
    result = validator(raw_size="0.14", order_policy={"reduce_only": True})

    assert result.reduce_only is True
    assert result.block_reason == "BLOCKED_TOO_SMALL"
    assert "BELOW_MIN_ORDER_VALUE" in result.errors
    assert result.passes_min_order_value is False


def test_validator_passes_at_min_order_value() -> None:
    result = validator(raw_size="0.4", raw_price="25", target="10.00", order_policy={"slippage_bps": 0})

    assert result.estimated_notional == Decimal("10.00000000")
    assert result.ok


def test_validator_blocks_open_size_that_exceeds_target_at_reference_price() -> None:
    result = validator(raw_size="0.41", raw_price="25", target="10.00", order_policy={"slippage_bps": 0})

    assert result.block_reason == "BLOCKED_TARGET_NOTIONAL_EXCEEDED"
    assert "BLOCKED_TARGET_NOTIONAL_EXCEEDED" in result.errors


def test_validator_allows_position_ratio_increase_price_drift_above_accounting_delta() -> None:
    result = validator(
        raw_size="0.496",
        raw_price="89.955",
        target="43.621216",
        side="SELL",
        canonical="xyz:CL",
        asset_id=29,
        market_meta={"name": "CL", "asset_id": 29, "szDecimals": 3, "maxLeverage": 20},
        order_policy={
            "aggressive_market": True,
            "allow_target_notional_price_drift": True,
            "effective_leverage": 4,
        },
    )

    assert result.estimated_notional == Decimal("44.61768000")
    assert result.ok
    assert "BLOCKED_TARGET_NOTIONAL_EXCEEDED" not in result.errors


def test_validator_blocks_missing_meta_asset_and_stale_price() -> None:
    missing_meta = validator(market_meta={})
    meta_without_asset = validator(market_meta={"name": "xyz:USAR", "szDecimals": 2, "maxLeverage": 10})
    missing_asset = validator(asset_id=None)
    stale_price = validator(order_policy={"price_fresh": False})

    assert "BLOCKED_MARKET_META_MISSING" in missing_meta.errors
    assert "BLOCKED_ASSET_ID_MISSING" in meta_without_asset.errors
    assert "BLOCKED_ASSET_ID_MISSING" in missing_asset.errors
    assert "BLOCKED_PRICE_STALE" in stale_price.errors


def test_validator_rejects_non_ioc_policy() -> None:
    result = validator(order_policy={"tif": "Gtc", "order_type": {"limit": {"tif": "Gtc"}}})

    assert "BLOCKED_INVALID_IOC_POLICY" in result.errors


def test_validator_rejects_invalid_cloid_and_price() -> None:
    bad_cloid = validator(order_policy={"cloid": "0x1234"})
    bad_price = validator(raw_price="0")

    assert "BLOCKED_INVALID_CLOID" in bad_cloid.errors
    assert "BLOCKED_INVALID_PRICE" in bad_price.errors


def test_validator_slippage_policy_prices_remain_legal_for_non_copy_paths() -> None:
    buy = validator(side="BUY")
    sell = validator(side="SELL", order_policy={"is_buy": False})

    assert buy.rounded_price == Decimal("26.687")
    assert sell.rounded_price == Decimal("26.158")
    assert buy.tick_size == Decimal("0.001")
    assert sell.tick_size == Decimal("0.001")


def test_validator_aggressive_market_ignores_slippage_bps_for_copy_path() -> None:
    buy = validator(side="BUY", order_policy={"aggressive_market": True, "slippage_bps": 0})
    sell = validator(side="SELL", order_policy={"is_buy": False, "aggressive_market": True, "slippage_bps": 0})

    assert buy.estimated_notional == Decimal("3.69915000")
    assert sell.estimated_notional == Decimal("3.69915000")
    assert buy.rounded_price == Decimal("27.744")
    assert sell.rounded_price == Decimal("25.102")
    assert buy.raw_limit_price == Decimal("27.743625")
    assert sell.raw_limit_price == Decimal("25.101375")


def test_aggressive_ioc_price_bound_cannot_recreate_skhx_oracle_rejection() -> None:
    result = validator(
        side="BUY",
        raw_size="1.210",
        raw_price="1005.55",
        target="1217.23753012",
        canonical="xyz:SKHX",
        asset_id=110022,
        market_meta={
            "name": "SKHX",
            "asset_id": 110022,
            "szDecimals": 3,
            "maxLeverage": 10,
        },
        order_policy={
            "aggressive_market": True,
            "allow_target_notional_price_drift": True,
            "effective_leverage": 1,
        },
    )

    assert result.ok
    assert result.raw_limit_price == Decimal("1055.8275")
    assert result.raw_limit_price <= Decimal("1005.55") * Decimal("1.05")


def test_validator_supports_default_and_hip3_markets_when_meta_exists() -> None:
    default = validator(dex="", canonical="BTC", asset_id=0, raw_size="0.01", raw_price="50000", target="500", market_meta={"name": "BTC", "asset_id": 0, "szDecimals": 5, "maxLeverage": 50})
    hip3 = validator(dex="xyz", canonical="xyz:USAR", asset_id=36, raw_size="0.4")

    assert default.ok
    assert hip3.ok


def test_hyperliquid_risk_settings_sets_cross_10x() -> None:
    class FakeClient:
        async def meta(self):
            return {"universe": [{"name": "BTC", "maxLeverage": 50}]}

        async def update_leverage(self, *, coin: str, leverage: int, is_cross: bool):
            self.payload = (coin, leverage, is_cross)
            return {"status": "ok"}

    async def run():
        client = FakeClient()
        result = await HyperliquidRiskSettingsService(client, expected_leverage=10).ensure_symbol_risk_settings("BTC")
        return result, client.payload

    result, payload = asyncio.run(run())
    assert result.is_ok is True
    assert payload == ("BTC", 10, True)


def test_hyperliquid_execution_client_strips_dex_prefix_for_sdk_leverage_and_orders() -> None:
    import hyperliquid.exchange as exchange_module

    class FakeInfo:
        def name_to_asset(self, name):
            self.name = name
            return 4242

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok"}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            return FakeResponse()

        async def aclose(self):
            return None

    class FakeExchange:
        def __init__(self):
            self.info = FakeInfo()
            self.wallet = object()
            self.vault_address = None
            self.expires_after = None
            self.base_url = "https://api.hyperliquid.xyz"
            self.leverage_updates = []
            self.orders = []

        def update_leverage(self, leverage: int, name: str, is_cross: bool = True):
            self.leverage_updates.append({"name": name, "leverage": leverage, "is_cross": is_cross})
            return {"status": "ok"}

        def order(self, name, is_buy, sz, limit_px, order_type, reduce_only=False, cloid=None):
            self.orders.append(
                {
                    "name": name,
                    "is_buy": is_buy,
                    "sz": sz,
                    "limit_px": limit_px,
                    "order_type": order_type,
                    "reduce_only": reduce_only,
                    "cloid": cloid,
                }
            )
            return {"status": "ok"}

    class FakeClient(HyperliquidExecutionClient):
        def __init__(self):
            super().__init__(info_url="http://localhost/info", private_key="0x" + "2" * 64, account_address="0x" + "5" * 40)
            self.exchange = FakeExchange()
            self._client = FakeHttpClient()
            self.dexes = []

        def _sdk_exchange(self, dex: str = ""):
            self.dexes.append(dex)
            return self.exchange

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(exchange_module, "get_timestamp_ms", lambda: 1234567890)
    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client = FakeClient()
        leverage_response = await client.update_leverage(
            coin="hyna:ZEC",
            leverage=10,
            is_cross=False,
            asset_id=47,
        )
        top_up_response = await client.top_up_isolated_only_margin(
            coin="hyna:ZEC",
            leverage=1,
            asset_id=47,
        )
        add_margin_response = await client.add_isolated_margin(
            coin="hyna:ZEC",
            amount=Decimal("12.3456781"),
            asset_id=47,
        )
        remove_margin_response = await client.remove_isolated_margin(
            coin="hyna:ZEC",
            amount=Decimal("12.3456789"),
            asset_id=47,
        )
        payload = build_hyperliquid_ioc_order(
            coin="ZEC",
            dex="hyna",
            is_buy=True,
            quantity=Decimal("0.5"),
            reference_price=Decimal("400"),
            slippage_bps=100,
            reduce_only=False,
            cloid="0x" + "4" * 32,
        )
        order_response = await client.place_market_order(**payload)
        await client.close()
        return (
            client,
            leverage_response,
            top_up_response,
            add_margin_response,
            remove_margin_response,
            order_response,
        )

    try:
        (
            client,
            leverage_response,
            top_up_response,
            add_margin_response,
            remove_margin_response,
            order_response,
        ) = asyncio.run(run())
        assert leverage_response == {"status": "ok"}
        assert top_up_response == {"status": "ok"}
        assert add_margin_response == {"status": "ok"}
        assert remove_margin_response == {"status": "ok"}
        assert order_response == {"status": "ok"}
        assert client.dexes == ["hyna", "hyna", "hyna", "hyna", "hyna"]
        leverage_payload = client._client.posts[0][1]
        assert leverage_payload["action"] == {
            "type": "updateLeverage",
            "asset": 4242,
            "isCross": False,
            "leverage": 10,
        }
        top_up_payload = client._client.posts[1][1]
        assert top_up_payload["action"] == {
            "type": "topUpIsolatedOnlyMargin",
            "asset": 4242,
            "leverage": "1",
        }
        add_margin_payload = client._client.posts[2][1]
        assert add_margin_payload["action"] == {
            "type": "updateIsolatedMargin",
            "asset": 4242,
            "isBuy": True,
            "ntli": 12345679,
        }
        remove_margin_payload = client._client.posts[3][1]
        assert remove_margin_payload["action"] == {
            "type": "updateIsolatedMargin",
            "asset": 4242,
            "isBuy": True,
            "ntli": -12345678,
        }
        assert client.exchange.info.name == "hyna:ZEC"
        assert client.exchange.orders[0]["name"] == "hyna:ZEC"
        assert client.exchange.orders[0]["sz"] == 0.5
        assert str(client.exchange.orders[0]["cloid"]) == "0x" + "4" * 32
        assert hasattr(client.exchange.orders[0]["cloid"], "to_raw")
    finally:
        monkeypatch.undo()


def test_hyperliquid_execution_client_resolves_mixed_case_sdk_market_name() -> None:
    import hyperliquid.exchange as exchange_module

    class FakeInfo:
        coin_to_asset = {"kBONK": 85}

        def name_to_asset(self, name):
            return self.coin_to_asset[name]

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok"}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            return FakeResponse()

        async def aclose(self):
            return None

    class FakeExchange:
        def __init__(self):
            self.info = FakeInfo()
            self.wallet = object()
            self.vault_address = None
            self.expires_after = None
            self.base_url = "https://api.hyperliquid.xyz"
            self.leverage_updates = []
            self.orders = []

        def update_leverage(self, leverage: int, name: str, is_cross: bool = True):
            self.leverage_updates.append({"name": name, "leverage": leverage, "is_cross": is_cross})
            return {"status": "ok"}

        def order(self, name, is_buy, sz, limit_px, order_type, reduce_only=False, cloid=None):
            self.orders.append({"name": name, "sz": sz, "reduce_only": reduce_only})
            return {"status": "ok"}

    class FakeClient(HyperliquidExecutionClient):
        def __init__(self):
            super().__init__(
                info_url="http://localhost/info",
                private_key="0x" + "3" * 64,
                account_address="0x" + "5" * 40,
            )
            self.exchange = FakeExchange()
            self._client = FakeHttpClient()

        def _sdk_exchange(self, dex: str = ""):
            self._prime_sdk_coin_name_cache(self.exchange, dex)
            return self.exchange

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(exchange_module, "get_timestamp_ms", lambda: 1234567890)
    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client = FakeClient()
        leverage_response = await client.update_leverage(coin="KBONK", leverage=10, is_cross=True)
        order_response = await client.place_market_order(
            coin="KBONK",
            dex="",
            is_buy=False,
            sz=Decimal("10000"),
            limit_px=Decimal("0.004"),
            reduce_only=False,
            cloid="0x" + "5" * 32,
        )
        await client.close()
        return client, leverage_response, order_response

    try:
        client, leverage_response, order_response = asyncio.run(run())

        assert leverage_response == {"status": "ok"}
        assert order_response == {"status": "ok"}
        assert client._client.posts[0][1]["action"] == {
            "type": "updateLeverage",
            "asset": 85,
            "isCross": True,
            "leverage": 10,
        }
        assert client.exchange.orders == [{"name": "kBONK", "sz": 10000.0, "reduce_only": False}]
        assert client._sdk_coin_name_cache[("", "KBONK")] == "kBONK"
    finally:
        monkeypatch.undo()


def test_sdk_market_name_cache_keeps_default_and_hip3_scopes_separate() -> None:
    client = HyperliquidExecutionClient(info_url="http://localhost/info")
    exchange = SimpleNamespace(
        info=SimpleNamespace(
            coin_to_asset={
                "kBONK": 85,
                "xyz:kBONK": 100085,
            }
        )
    )

    client._prime_sdk_coin_name_cache(exchange, "")
    client._prime_sdk_coin_name_cache(exchange, "xyz")

    assert client._sdk_coin_name_cache[("", "KBONK")] == "kBONK"
    assert client._sdk_coin_name_cache[("xyz", "XYZ:KBONK")] == "xyz:kBONK"


def test_hyperliquid_execution_client_direct_http_uses_sdk_signed_payload(monkeypatch) -> None:
    import hyperliquid.exchange as exchange_module

    class FakeInfo:
        def name_to_asset(self, name):
            self.name = name
            return 42

    class FakeExchange:
        def __init__(self):
            self.info = FakeInfo()
            self.wallet = object()
            self.vault_address = None
            self.expires_after = None
            self.base_url = "https://api.hyperliquid.xyz"

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"filled": {}}]}}}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            return FakeResponse()

        async def aclose(self):
            return None

    class FakeClient(HyperliquidExecutionClient):
        def __init__(self):
            super().__init__(
                info_url="http://localhost/info",
                private_key="0x" + "4" * 64,
                account_address="0x" + "5" * 40,
                order_submit_transport="http",
            )
            self.exchange = FakeExchange()
            self._client = FakeHttpClient()

        def _sdk_exchange(self, dex: str = ""):
            return self.exchange

    monkeypatch.setattr(exchange_module, "get_timestamp_ms", lambda: 1234567890)
    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client = FakeClient()
        trace = {}
        result = await client.place_market_order(
            coin="hyna:ZEC",
            dex="hyna",
            is_buy=True,
            sz=Decimal("0.5"),
            limit_px=Decimal("400"),
            reduce_only=False,
            cloid="0x" + "4" * 32,
            _latency_trace=trace,
        )
        return client, trace, result

    client, trace, result = asyncio.run(run())
    assert result["status"] == "ok"
    assert client._client.posts[0][0] == "https://api.hyperliquid.xyz/exchange"
    payload = client._client.posts[0][1]
    assert payload["nonce"] == 1234567890
    assert payload["signature"] == {"r": "0x1", "s": "0x2", "v": 27}
    assert payload["vaultAddress"] is None
    assert payload["action"]["type"] == "order"
    assert payload["action"]["orders"][0]["a"] == 42
    assert payload["action"]["orders"][0]["b"] is True
    assert payload["action"]["orders"][0]["c"] == "0x" + "4" * 32
    assert client.exchange.info.name == "hyna:ZEC"
    assert trace["sdk_http_payload_built_at"]
    assert trace["sdk_http_post_started_at"]
    assert trace["sdk_http_post_done_at"]


def test_persistable_signed_envelope_is_not_sent_until_submit_and_replays_identically(monkeypatch) -> None:
    import hyperliquid.exchange as exchange_module

    class FakeInfo:
        def name_to_asset(self, _name):
            return 42

    class FakeExchange:
        info = FakeInfo()
        wallet = object()
        vault_address = None
        expires_after = None
        base_url = "https://api.hyperliquid.xyz"

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok", "response": {"data": {"statuses": [{"filled": {}}]}}}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            return FakeResponse()

        async def aclose(self):
            return None

    class FakeClient(HyperliquidExecutionClient):
        def __init__(self):
            super().__init__(
                info_url="http://localhost/info",
                private_key="0x" + "8" * 64,
                account_address="0x" + "5" * 40,
            )
            self.exchange = FakeExchange()
            self._client = FakeHttpClient()

        def _sdk_exchange(self, dex: str = ""):
            return self.exchange

    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client = FakeClient()
        trace = {}
        envelope = client.prepare_market_order_envelope(
            nonce=1_700_000_000_123,
            latency_trace=trace,
            coin="BTC",
            dex="",
            is_buy=True,
            sz=Decimal("0.01"),
            limit_px=Decimal("50000"),
            reduce_only=False,
            cloid="0x" + "8" * 32,
        )
        assert client._client.posts == []
        await client.submit_market_order_envelope(envelope)
        await client.submit_market_order_envelope(envelope)
        return client, envelope, trace

    client, envelope, trace = asyncio.run(run())
    assert len(client._client.posts) == 2
    assert client._client.posts[0][1] == envelope["payload"]
    assert client._client.posts[1][1] == envelope["payload"]
    assert client._client.posts[0][1]["nonce"] == 1_700_000_000_123
    assert trace["sdk_prepare_started_at"]
    assert trace["sdk_exchange_ready_at"]
    assert trace["sdk_prepare_done_at"]


def test_order_transport_warmup_is_read_only_and_uses_execution_origin() -> None:
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"BTC": "50000"}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            return FakeResponse()

    http_client = FakeHttpClient()
    client = SimpleNamespace(
        _client=http_client,
        _sdk_exchange=lambda _dex="": SimpleNamespace(base_url="https://api.hyperliquid.xyz"),
        _websocket_order_submit_enabled=lambda: False,
        _warm_http_order_origin=lambda base_url, payload: HyperliquidExecutionClient._warm_http_order_origin(
            SimpleNamespace(_client=http_client), base_url, payload
        ),
    )

    warmed = asyncio.run(HyperliquidExecutionClient.warm_order_transport(client, "xyz"))

    assert warmed is True
    assert http_client.posts == [
        ("https://api.hyperliquid.xyz/info", {"type": "allMids", "dex": "xyz"})
    ]


def test_websocket_exchange_url_uses_same_execution_origin() -> None:
    assert _websocket_url_for_exchange("https://api.hyperliquid.xyz") == (
        "wss://api.hyperliquid.xyz/ws"
    )
    assert _websocket_url_for_exchange("http://localhost:3001/") == "ws://localhost:3001/ws"


def test_signed_envelope_websocket_success_never_posts_http() -> None:
    class FakeTransport:
        def __init__(self):
            self.payloads = []

        async def post_action(self, payload, *, latency_trace=None):
            self.payloads.append(payload)
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": []}}}

        async def close(self):
            return None

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            raise AssertionError("successful websocket action must not use HTTP")

        async def aclose(self):
            return None

    async def run():
        client = HyperliquidExecutionClient(
            info_url="https://api.hyperliquid.xyz/info",
            private_key="0x" + "1" * 64,
            account_address="0x" + "2" * 40,
            order_submit_transport="websocket",
        )
        fake_http = FakeHttpClient()
        client._client = fake_http
        client._sdk_exchange = lambda _dex="": SimpleNamespace(base_url="https://api.hyperliquid.xyz")
        transport = FakeTransport()
        client._ws_action_transports[_websocket_url_for_exchange("https://api.hyperliquid.xyz")] = transport
        envelope = {"dex": "", "payload": {"action": {"type": "order"}, "nonce": 123}}
        result = await client.submit_market_order_envelope(envelope, latency_trace={})
        await client.close()
        return result, transport, fake_http

    result, transport, fake_http = asyncio.run(run())
    assert result["status"] == "ok"
    assert transport.payloads == [{"action": {"type": "order"}, "nonce": 123}]
    assert fake_http.posts == []


@pytest.mark.parametrize("failure", [WebSocketActionNotSent("connect failed"), WebSocketActionNotAccepted("429")])
def test_signed_envelope_websocket_falls_back_http_only_when_definitely_not_sent(failure) -> None:
    class FakeTransport:
        async def post_action(self, payload, *, latency_trace=None):
            raise failure

        async def close(self):
            return None

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": []}}}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            return FakeResponse()

        async def aclose(self):
            return None

    async def run():
        client = HyperliquidExecutionClient(
            info_url="https://api.hyperliquid.xyz/info",
            private_key="0x" + "3" * 64,
            account_address="0x" + "4" * 40,
            order_submit_transport="websocket",
        )
        fake_http = FakeHttpClient()
        client._client = fake_http
        client._sdk_exchange = lambda _dex="": SimpleNamespace(base_url="https://api.hyperliquid.xyz")
        client._ws_action_transports[_websocket_url_for_exchange("https://api.hyperliquid.xyz")] = FakeTransport()
        envelope = {"dex": "", "payload": {"action": {"type": "order"}, "nonce": 456}}
        trace = {}
        result = await client.submit_market_order_envelope(envelope, latency_trace=trace)
        await client.close()
        return result, fake_http, trace

    result, fake_http, trace = asyncio.run(run())
    assert result["status"] == "ok"
    assert fake_http.posts == [
        ("https://api.hyperliquid.xyz/exchange", {"action": {"type": "order"}, "nonce": 456})
    ]
    assert trace["websocket_http_fallback"] is True
    assert trace["effective_order_submit_transport"] == "http_fallback"
    assert trace["websocket_http_fallback_reason"] == type(failure).__name__


def test_explicit_websocket_rejection_opens_short_http_circuit_breaker() -> None:
    class RejectingTransport:
        def __init__(self):
            self.calls = 0

        async def post_action(self, payload, *, latency_trace=None):
            self.calls += 1
            raise WebSocketActionNotAccepted("429 Too Many Requests")

        async def close(self):
            return None

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": []}}}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            return FakeResponse()

        async def aclose(self):
            return None

    async def run():
        client = HyperliquidExecutionClient(
            info_url="https://api.hyperliquid.xyz/info",
            private_key="0x" + "3" * 64,
            account_address="0x" + "4" * 40,
            order_submit_transport="websocket",
        )
        fake_http = FakeHttpClient()
        client._client = fake_http
        client._sdk_exchange = lambda _dex="": SimpleNamespace(base_url="https://api.hyperliquid.xyz")
        transport = RejectingTransport()
        client._ws_action_transports[_websocket_url_for_exchange("https://api.hyperliquid.xyz")] = transport
        first_trace = {}
        second_trace = {}
        await client.submit_market_order_envelope(
            {"dex": "", "payload": {"action": {"type": "order"}, "nonce": 456}},
            latency_trace=first_trace,
        )
        await client.submit_market_order_envelope(
            {"dex": "", "payload": {"action": {"type": "order"}, "nonce": 457}},
            latency_trace=second_trace,
        )
        await client.close()
        return transport, fake_http, first_trace, second_trace

    transport, fake_http, first_trace, second_trace = asyncio.run(run())
    assert transport.calls == 1
    assert len(fake_http.posts) == 2
    assert first_trace["effective_order_submit_transport"] == "http_fallback"
    assert first_trace["websocket_http_fallback_message"] == "429 Too Many Requests"
    assert second_trace["effective_order_submit_transport"] == "http_circuit_breaker"
    assert second_trace["websocket_bypass_remaining_ms"] > 0


def test_signed_envelope_websocket_unknown_never_falls_back_or_resubmits() -> None:
    class FakeTransport:
        async def post_action(self, payload, *, latency_trace=None):
            raise WebSocketActionSubmissionUncertain("ack lost")

        async def close(self):
            return None

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            raise AssertionError("ambiguous websocket action must not fall back to HTTP")

        async def aclose(self):
            return None

    async def run():
        client = HyperliquidExecutionClient(
            info_url="https://api.hyperliquid.xyz/info",
            private_key="0x" + "5" * 64,
            account_address="0x" + "6" * 40,
            order_submit_transport="websocket",
        )
        fake_http = FakeHttpClient()
        client._client = fake_http
        client._sdk_exchange = lambda _dex="": SimpleNamespace(base_url="https://api.hyperliquid.xyz")
        client._ws_action_transports[_websocket_url_for_exchange("https://api.hyperliquid.xyz")] = FakeTransport()
        envelope = {"dex": "", "payload": {"action": {"type": "order"}, "nonce": 789}}
        with pytest.raises(WebSocketActionSubmissionUncertain, match="ack lost"):
            await client.submit_market_order_envelope(envelope, latency_trace={})
        await client.close()
        return fake_http

    fake_http = asyncio.run(run())
    assert fake_http.posts == []


def test_leverage_update_uses_warm_websocket_action_transport(monkeypatch) -> None:
    import hyperliquid.exchange as exchange_module

    class FakeInfo:
        def name_to_asset(self, name):
            assert name == "BTC"
            return 42

    exchange = SimpleNamespace(
        info=FakeInfo(),
        wallet=object(),
        vault_address=None,
        expires_after=None,
        base_url="https://api.hyperliquid.xyz",
    )

    class FakeTransport:
        def __init__(self):
            self.payloads = []

        async def post_action(self, payload, *, latency_trace=None):
            self.payloads.append(payload)
            return {"status": "ok", "response": {"type": "default"}}

        async def close(self):
            return None

    class NoHttp:
        async def post(self, url, json):
            raise AssertionError("successful leverage websocket action must not use HTTP")

        async def aclose(self):
            return None

    monkeypatch.setattr(exchange_module, "get_timestamp_ms", lambda: 1_700_000_000_000)
    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client = HyperliquidExecutionClient(
            info_url="https://api.hyperliquid.xyz/info",
            private_key="0x" + "7" * 64,
            account_address="0x" + "8" * 40,
            order_submit_transport="websocket",
        )
        client._client = NoHttp()
        client._sdk_exchange = lambda _dex="": exchange
        transport = FakeTransport()
        client._ws_action_transports[
            _websocket_url_for_exchange(exchange.base_url)
        ] = transport
        response = await client.update_leverage(
            coin="BTC",
            leverage=10,
            is_cross=True,
        )
        await client.close()
        return response, transport

    response, transport = asyncio.run(run())
    assert response["status"] == "ok"
    assert len(transport.payloads) == 1
    assert transport.payloads[0]["action"] == {
        "type": "updateLeverage",
        "asset": 42,
        "isCross": True,
        "leverage": 10,
    }


def test_ambiguous_leverage_websocket_action_never_blindly_falls_back_http(
    monkeypatch,
) -> None:
    import hyperliquid.exchange as exchange_module

    class FakeInfo:
        def name_to_asset(self, name):
            return 42

    exchange = SimpleNamespace(
        info=FakeInfo(),
        wallet=object(),
        vault_address=None,
        expires_after=None,
        base_url="https://api.hyperliquid.xyz",
    )

    class UncertainTransport:
        async def post_action(self, payload, *, latency_trace=None):
            raise WebSocketActionSubmissionUncertain("leverage acknowledgement lost")

        async def close(self):
            return None

    class NoHttp:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            raise AssertionError("ambiguous leverage action must fail closed")

        async def aclose(self):
            return None

    monkeypatch.setattr(exchange_module, "get_timestamp_ms", lambda: 1_700_000_000_100)
    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client = HyperliquidExecutionClient(
            info_url="https://api.hyperliquid.xyz/info",
            private_key="0x" + "c" * 64,
            account_address="0x" + "a" * 40,
            order_submit_transport="websocket",
        )
        no_http = NoHttp()
        client._client = no_http
        client._sdk_exchange = lambda _dex="": exchange
        client._ws_action_transports[
            _websocket_url_for_exchange(exchange.base_url)
        ] = UncertainTransport()
        with pytest.raises(
            WebSocketActionSubmissionUncertain,
            match="acknowledgement lost",
        ):
            await client.update_leverage(
                coin="BTC",
                leverage=10,
                is_cross=True,
            )
        await client.close()
        return no_http

    assert asyncio.run(run()).posts == []


def test_websocket_action_transport_matches_concurrent_responses_by_request_id(monkeypatch) -> None:
    import app.services.hyperliquid_execution as execution_module

    class FakeConnection:
        def __init__(self):
            self.sent = []
            self.incoming = asyncio.Queue()
            self.closed = False

        async def send(self, message):
            self.sent.append(json.loads(message))

        async def close(self):
            if not self.closed:
                self.closed = True
                await self.incoming.put(None)

        def __aiter__(self):
            return self

        async def __anext__(self):
            message = await self.incoming.get()
            if message is None:
                raise StopAsyncIteration
            return message

    async def run():
        connection = FakeConnection()

        async def fake_connect(*args, **kwargs):
            return connection

        monkeypatch.setattr(execution_module.websockets, "connect", fake_connect)
        transport = _HyperliquidWebSocketActionTransport(
            "wss://api.hyperliquid.xyz/ws",
            response_timeout=1,
        )
        first = asyncio.create_task(transport.post_action({"nonce": 1}))
        second = asyncio.create_task(transport.post_action({"nonce": 2}))
        for _ in range(100):
            if len(connection.sent) == 2:
                break
            await asyncio.sleep(0)
        assert len(connection.sent) == 2
        first_id = connection.sent[0]["id"]
        second_id = connection.sent[1]["id"]
        await connection.incoming.put(json.dumps({
            "channel": "post",
            "data": {"id": second_id, "response": {"type": "action", "payload": {"nonce": 2}}},
        }))
        await connection.incoming.put(json.dumps({
            "channel": "post",
            "data": {"id": first_id, "response": {"type": "action", "payload": {"nonce": 1}}},
        }))
        results = await asyncio.gather(first, second)
        await transport.close()
        return results

    assert asyncio.run(run()) == [{"nonce": 1}, {"nonce": 2}]


def test_warmed_websocket_action_transport_reconnects_before_next_order(monkeypatch) -> None:
    import app.services.hyperliquid_execution as execution_module

    class FakeConnection:
        def __init__(self):
            self.incoming = asyncio.Queue()
            self.closed = False

        async def send(self, message):
            return None

        async def close(self):
            if not self.closed:
                self.closed = True
                await self.incoming.put(None)

        def __aiter__(self):
            return self

        async def __anext__(self):
            message = await self.incoming.get()
            if message is None:
                raise StopAsyncIteration
            return message

    async def run():
        connections = [FakeConnection(), FakeConnection()]
        connect_count = 0

        async def fake_connect(*args, **kwargs):
            nonlocal connect_count
            connection = connections[min(connect_count, len(connections) - 1)]
            connect_count += 1
            return connection

        monkeypatch.setattr(execution_module.websockets, "connect", fake_connect)
        transport = _HyperliquidWebSocketActionTransport("wss://api.hyperliquid.xyz/ws")
        await transport.warm()
        assert connect_count == 1
        await connections[0].close()
        for _ in range(200):
            if connect_count >= 2 and transport._connection is connections[1]:
                break
            await asyncio.sleep(0)
        assert connect_count >= 2
        assert transport._connection is connections[1]
        await transport.close()

    asyncio.run(run())


def test_recovery_rejects_modified_persisted_signed_envelope() -> None:
    envelope = {"dex": "", "payload": {"action": {"type": "order"}, "nonce": 123}}
    order = ExecutionOrder(
        signed_action_envelope=envelope,
        signed_action_hash=_signed_action_hash(envelope),
        submit_nonce=123,
        submit_signer_scope="testnet:signer",
    )
    order.signed_action_envelope["payload"]["nonce"] = 124

    class FakeClient:
        signer_scope = "testnet:signer"

        async def submit_market_order_envelope(self, _envelope):
            raise AssertionError("tampered envelope must never reach the exchange")

    with pytest.raises(RuntimeError, match="integrity"):
        asyncio.run(_replay_persisted_hyperliquid_action(FakeClient(), order))


def test_hyperliquid_execution_client_direct_http_does_not_serialize_posts(monkeypatch) -> None:
    import hyperliquid.exchange as exchange_module

    class FakeInfo:
        def name_to_asset(self, name):
            return 42

    class FakeExchange:
        def __init__(self):
            self.info = FakeInfo()
            self.wallet = object()
            self.vault_address = None
            self.expires_after = None
            self.base_url = "https://api.hyperliquid.xyz"

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"filled": {}}]}}}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []
            self.in_flight = 0
            self.max_in_flight = 0
            self.both_started = asyncio.Event()
            self.release = asyncio.Event()

        async def post(self, url, json):
            self.posts.append((url, json))
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            if len(self.posts) >= 2:
                self.both_started.set()
            await self.release.wait()
            self.in_flight -= 1
            return FakeResponse()

        async def aclose(self):
            return None

    class FakeClient(HyperliquidExecutionClient):
        def __init__(self):
            super().__init__(
                info_url="http://localhost/info",
                private_key="0x" + "1" * 64,
                account_address="0x" + "5" * 40,
                order_submit_transport="http",
            )
            self.exchange = FakeExchange()
            self._client = FakeHttpClient()

        def _sdk_exchange(self, dex: str = ""):
            return self.exchange

    monkeypatch.setattr(exchange_module, "get_timestamp_ms", lambda: 1234567890)
    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client = FakeClient()
        task_one = asyncio.create_task(
            client.place_market_order(
                coin="hyna:ZEC",
                dex="hyna",
                is_buy=True,
                sz=Decimal("0.5"),
                limit_px=Decimal("400"),
                reduce_only=True,
                cloid="0x" + "4" * 32,
                _latency_trace={},
            )
        )
        task_two = asyncio.create_task(
            client.place_market_order(
                coin="hyna:ZEC",
                dex="hyna",
                is_buy=True,
                sz=Decimal("0.4"),
                limit_px=Decimal("400"),
                reduce_only=True,
                cloid="0x" + "5" * 32,
                _latency_trace={},
            )
        )
        await asyncio.wait_for(client._client.both_started.wait(), timeout=0.25)
        client._client.release.set()
        await asyncio.gather(task_one, task_two)
        return client

    client = asyncio.run(run())
    payloads = [payload for _, payload in client._client.posts]
    assert client._client.max_in_flight == 2
    assert [payload["nonce"] for payload in payloads] == [1234567890, 1234567891]
    assert {payload["action"]["orders"][0]["c"] for payload in payloads} == {
        "0x" + "4" * 32,
        "0x" + "5" * 32,
    }


def test_hyperliquid_execution_client_nonce_is_shared_across_clients(monkeypatch) -> None:
    import hyperliquid.exchange as exchange_module

    class FakeInfo:
        def name_to_asset(self, name):
            return 42

    class FakeExchange:
        def __init__(self):
            self.info = FakeInfo()
            self.wallet = object()
            self.vault_address = None
            self.expires_after = None
            self.base_url = "https://api.hyperliquid.xyz"

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"filled": {}}]}}}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            return FakeResponse()

        async def aclose(self):
            return None

    class FakeClient(HyperliquidExecutionClient):
        def __init__(self):
            super().__init__(
                info_url="http://localhost/info",
                private_key="0x" + "9" * 64,
                account_address="0x" + "5" * 40,
                order_submit_transport="http",
            )
            self.exchange = FakeExchange()
            self._client = FakeHttpClient()

        def _sdk_exchange(self, dex: str = ""):
            return self.exchange

    monkeypatch.setattr(exchange_module, "get_timestamp_ms", lambda: 3333333333)
    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client_one = FakeClient()
        client_two = FakeClient()
        await asyncio.gather(
            client_one.place_market_order(
                coin="BTC",
                dex="",
                is_buy=True,
                sz=Decimal("0.01"),
                limit_px=Decimal("50000"),
                reduce_only=False,
                cloid="0x" + "6" * 32,
            ),
            client_two.place_market_order(
                coin="BTC",
                dex="",
                is_buy=True,
                sz=Decimal("0.02"),
                limit_px=Decimal("50000"),
                reduce_only=False,
                cloid="0x" + "7" * 32,
            ),
        )
        return client_one, client_two

    client_one, client_two = asyncio.run(run())
    nonces = [
        client_one._client.posts[0][1]["nonce"],
        client_two._client.posts[0][1]["nonce"],
    ]
    assert sorted(nonces) == [3333333333, 3333333334]


def test_hyperliquid_durable_nonce_reservation_joins_shared_signer_counter() -> None:
    client_one = HyperliquidExecutionClient(
        info_url="https://api.hyperliquid.xyz/info",
        private_key="0x" + "1" * 64,
        account_address="0x" + "2" * 40,
        network="mainnet",
    )
    client_two = HyperliquidExecutionClient(
        info_url="https://api.hyperliquid.xyz/info",
        private_key="0x" + "1" * 64,
        account_address="0x" + "2" * 40,
        network="mainnet",
    )
    try:
        with client_one._nonce_state.lock:
            client_one._nonce_state.last_nonce = 5_000

        first = client_one.reserve_action_nonce_at_least(5_000)
        second = client_two.reserve_action_nonce_at_least(4_999)

        assert first == 5_001
        assert second == 5_002
        assert client_one._nonce_state is client_two._nonce_state
    finally:
        asyncio.run(client_one.close())
        asyncio.run(client_two.close())


def test_primed_sdk_market_metadata_builds_hip3_exchange_without_info_requests(monkeypatch) -> None:
    from hyperliquid.info import Info

    def unexpected_info_request(*_args, **_kwargs):
        raise AssertionError("primed SDK exchange must not pull market metadata")

    monkeypatch.setattr(Info, "post", unexpected_info_request)
    client = HyperliquidExecutionClient(
        info_url="https://api.hyperliquid.xyz/info",
        private_key="0x" + "1" * 64,
        account_address="0x" + "2" * 40,
        network="mainnet",
    )
    client.prime_sdk_market_metadata(
        dex="xyz",
        meta={
            "universe": [
                {"name": "xyz:ONE", "szDecimals": 2, "maxLeverage": 10},
                {"name": "xyz:TWO", "szDecimals": 3, "maxLeverage": 5},
            ]
        },
        asset_offset=750000,
    )
    try:
        exchange = client._sdk_exchange("xyz")
        assert exchange is not None
        assert _resolve_sdk_asset_id(exchange, "xyz:ONE") == 750000
        assert _resolve_sdk_asset_id(exchange, "xyz:TWO") == 750001
        assert exchange.info.asset_to_sz_decimals[750001] == 3
    finally:
        asyncio.run(client.close())


@pytest.mark.parametrize(
    ("dex", "asset_offset", "old_coin", "new_coin"),
    [
        ("", 0, "OLD", "NEW"),
        ("xyz", 750000, "xyz:OLD", "xyz:NEW"),
        ("flx", 880000, "flx:OLD", "flx:NEW"),
    ],
)
def test_refreshed_sdk_market_metadata_updates_existing_exchange_for_new_market(
    dex,
    asset_offset,
    old_coin,
    new_coin,
) -> None:
    class FakeInfo:
        def __init__(self) -> None:
            self.coin_to_asset = {old_coin: asset_offset}
            self.name_to_coin = {old_coin: old_coin}
            self.asset_to_sz_decimals = {asset_offset: 2}

        def name_to_asset(self, name):
            return self.coin_to_asset[name]

    class FakeExchange:
        def __init__(self) -> None:
            self.info = FakeInfo()

    client = HyperliquidExecutionClient(
        info_url="https://api.hyperliquid.xyz/info",
        private_key="0x" + "1" * 64,
        account_address="0x" + "2" * 40,
        network="mainnet",
    )
    exchange = FakeExchange()
    client._exchange_cache[dex] = exchange
    client._sdk_coin_name_cache[(dex, new_coin.upper())] = new_coin

    client.prime_sdk_market_metadata(
        dex=dex,
        meta={
            "universe": [
                {"name": old_coin, "szDecimals": 2, "maxLeverage": 10},
                {"name": new_coin, "szDecimals": 3, "maxLeverage": 5},
            ]
        },
        asset_offset=asset_offset,
    )

    assert _resolve_sdk_asset_id(exchange, new_coin) == asset_offset + 1
    assert exchange.info.name_to_coin[new_coin] == new_coin
    assert exchange.info.asset_to_sz_decimals[asset_offset + 1] == 3
    assert client._sdk_coin_name_cache[(dex, new_coin.upper())] == new_coin


def test_hyperliquid_execution_client_submit_slot_is_shared_across_clients(monkeypatch) -> None:
    import threading

    import hyperliquid.exchange as exchange_module

    class FakeInfo:
        def name_to_asset(self, name):
            return 42

    class FakeExchange:
        def __init__(self):
            self.info = FakeInfo()
            self.wallet = object()
            self.vault_address = None
            self.expires_after = None
            self.base_url = "https://api.hyperliquid.xyz"

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"filled": {}}]}}}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []
            self.first_started = asyncio.Event()
            self.release = asyncio.Event()

        async def post(self, url, json):
            self.posts.append((url, json))
            self.first_started.set()
            await self.release.wait()
            return FakeResponse()

        async def aclose(self):
            return None

    class FakeClient(HyperliquidExecutionClient):
        def __init__(self):
            super().__init__(
                info_url="http://localhost/info",
                private_key="0x" + "b" * 64,
                account_address="0x" + "5" * 40,
                order_submit_transport="http",
            )
            self.exchange = FakeExchange()
            self._client = FakeHttpClient()

        def _sdk_exchange(self, dex: str = ""):
            return self.exchange

    monkeypatch.setattr(exchange_module, "get_timestamp_ms", lambda: 5555555555)
    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client_one = FakeClient()
        client_one._nonce_state.submit_semaphore = threading.BoundedSemaphore(1)
        client_two = FakeClient()
        task_one = asyncio.create_task(
            client_one.place_market_order(
                coin="BTC",
                dex="",
                is_buy=True,
                sz=Decimal("0.01"),
                limit_px=Decimal("50000"),
                reduce_only=False,
                cloid="0x" + "a" * 32,
            )
        )
        await asyncio.wait_for(client_one._client.first_started.wait(), timeout=0.25)
        task_two = asyncio.create_task(
            client_two.place_market_order(
                coin="BTC",
                dex="",
                is_buy=True,
                sz=Decimal("0.02"),
                limit_px=Decimal("50000"),
                reduce_only=False,
                cloid="0x" + "b" * 32,
            )
        )
        await asyncio.sleep(0.05)
        assert client_two._client.posts == []
        client_one._client.release.set()
        await task_one
        await asyncio.wait_for(client_two._client.first_started.wait(), timeout=0.25)
        client_two._client.release.set()
        await task_two
        return client_one, client_two

    client_one, client_two = asyncio.run(run())
    assert len(client_one._client.posts) == 1
    assert len(client_two._client.posts) == 1
    assert sorted([client_one._client.posts[0][1]["nonce"], client_two._client.posts[0][1]["nonce"]]) == [
        5555555555,
        5555555556,
    ]


def test_hyperliquid_execution_client_cancel_by_cloid_uses_shared_nonce(monkeypatch) -> None:
    import hyperliquid.exchange as exchange_module
    from hyperliquid.utils.types import Cloid

    class FakeInfo:
        def name_to_asset(self, name):
            return 42

    class FakeExchange:
        def __init__(self):
            self.info = FakeInfo()
            self.wallet = object()
            self.vault_address = None
            self.expires_after = None
            self.base_url = "https://api.hyperliquid.xyz"

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"status": "ok"}

    class FakeHttpClient:
        def __init__(self):
            self.posts = []

        async def post(self, url, json):
            self.posts.append((url, json))
            return FakeResponse()

        async def aclose(self):
            return None

    class FakeClient(HyperliquidExecutionClient):
        def __init__(self):
            super().__init__(
                info_url="http://localhost/info",
                private_key="0x" + "a" * 64,
                account_address="0x" + "5" * 40,
                order_submit_transport="http",
            )
            self.exchange = FakeExchange()
            self._client = FakeHttpClient()

        def _sdk_exchange(self, dex: str = ""):
            return self.exchange

    monkeypatch.setattr(exchange_module, "get_timestamp_ms", lambda: 4444444444)
    monkeypatch.setattr(
        exchange_module,
        "sign_l1_action",
        lambda wallet, action, active_pool, nonce, expires_after, is_mainnet: {
            "r": "0x1",
            "s": "0x2",
            "v": 27,
        },
    )

    async def run():
        client = FakeClient()
        await client.place_market_order(
            coin="BTC",
            dex="",
            is_buy=True,
            sz=Decimal("0.01"),
            limit_px=Decimal("50000"),
            reduce_only=False,
            cloid="0x" + "8" * 32,
        )
        await client.cancel_by_cloid(coin="BTC", cloid="0x" + "8" * 32)
        return client

    client = asyncio.run(run())
    order_payload, cancel_payload = [payload for _, payload in client._client.posts]
    assert [order_payload["nonce"], cancel_payload["nonce"]] == [4444444444, 4444444445]
    assert cancel_payload["action"] == {
        "type": "cancelByCloid",
        "cancels": [{"asset": 42, "cloid": Cloid.from_str("0x" + "8" * 32).to_raw()}],
    }


def test_hyperliquid_execution_client_normalizes_empty_vault_address() -> None:
    client = HyperliquidExecutionClient(
        info_url="http://localhost/info",
        private_key="  0x" + "1" * 64 + "  ",
        account_address="",
        vault_address="",
    )

    assert client._private_key == "0x" + "1" * 64
    assert client._account_address is None
    assert client._vault_address is None
    asyncio.run(client.close())


def test_hyperliquid_does_not_copy_leader_leverage() -> None:
    service = HyperliquidRiskSettingsService(object(), expected_leverage=10)
    assert service.expected_leverage == 10


def test_hyperliquid_cloid_is_traceable_and_hex() -> None:
    cloid = build_hyperliquid_cloid(
        leader_address="0xabcdef1234567890",
        coin="HYPE",
        side="LONG",
        action="OPEN_OR_INCREASE",
        source_fill_id="feedfacecafebeef",
        timestamp_ms=1_714_000_000_000,
    )
    assert cloid.startswith("0x")
    assert len(cloid) == 34
    int(cloid[2:], 16)


def test_hyperliquid_timeout_unknown_not_retried_without_recovery() -> None:
    class TimeoutClient:
        def __init__(self):
            self.orders = []

        async def place_market_order(self, **kwargs):
            self.orders.append(kwargs)
            raise TimeoutError("unknown")

    async def run():
        client = TimeoutClient()
        with pytest.raises(TimeoutError):
            await client.place_market_order(coin="BTC", cloid="0x" + "4" * 32)
        return len(client.orders)

    assert asyncio.run(run()) == 1


def test_hyperliquid_unknown_recovery_queries_by_cloid() -> None:
    class QueryClient:
        async def get_order_by_cloid(self, *, coin: str, cloid: str):
            self.query = (coin, cloid)
            return {"status": "filled"}

    async def run():
        client = QueryClient()
        result = await recover_hyperliquid_unknown_order(client, coin="BTC", cloid="0x" + "5" * 32)
        return client.query, result

    query, result = asyncio.run(run())
    assert query == ("BTC", "0x" + "5" * 32)
    assert result["status"] == "filled"


def test_hyperliquid_recovery_unknown_oid_marks_failed() -> None:
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        side="BUY",
        status="UNKNOWN",
        dry_run=False,
    )

    _apply_hyperliquid_order_response(order, {"status": "unknownOid"})

    assert order.status == "FAILED"
    assert "not found" in order.error_message
    assert order.order_finalized_at is not None


def test_hyperliquid_unstarted_pending_submit_recovery_does_not_resubmit() -> None:
    cloid = "0x" + "6" * 32
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        hyperliquid_coin="HYPE",
        dex="",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        quantity=Decimal("0.2"),
        estimated_price=Decimal("68"),
        cloid=cloid,
        status="PENDING_SUBMIT",
        reduce_only=True,
        dry_run=False,
        pre_trade_checklist={
            "order_validator": {
                "payload_masked": {
                    "coin": "HYPE",
                    "dex": "",
                    "is_buy": False,
                    "sz": "0.2",
                    "limit_px": "67.9",
                    "reduce_only": True,
                    "cloid": cloid,
                }
            }
        },
    )

    assert _should_resubmit_unstarted_hyperliquid_order(order) is False
    assert _is_unstarted_hyperliquid_outbox_order(order) is True


def test_hyperliquid_started_order_is_not_treated_as_safe_outbox_replay() -> None:
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="BUY",
        cloid="0x" + "6" * 32,
        status="SUBMITTING",
        order_submit_started_at=datetime.now(timezone.utc),
    )

    assert _is_unstarted_hyperliquid_outbox_order(order) is False


def test_hyperliquid_recovery_resubmit_is_disabled() -> None:
    class Client:
        def __init__(self):
            self.payload = None

        async def place_market_order(self, **kwargs):
            self.payload = kwargs
            return {
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": {
                        "statuses": [
                            {
                                "filled": {
                                    "totalSz": "0.2",
                                    "avgPx": "68",
                                    "oid": 123,
                                    "cloid": kwargs["cloid"],
                                }
                            }
                        ]
                    },
                },
            }

    cloid = "0x" + "7" * 32
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        source_coin="HYPE",
        hyperliquid_coin="HYPE",
        quantity=Decimal("0.2"),
        estimated_price=Decimal("68"),
        cloid=cloid,
        status="PENDING_SUBMIT",
        reduce_only=True,
        pre_trade_checklist={
            "order_validator": {
                "payload_masked": {
                    "coin": "HYPE",
                    "is_buy": False,
                    "sz": "0.2",
                    "limit_px": "67.9",
                    "reduce_only": True,
                    "cloid": cloid,
                }
            }
        },
    )

    client = Client()

    with pytest.raises(RuntimeError, match="resubmit is disabled"):
        asyncio.run(_resubmit_unstarted_hyperliquid_order(client, order))

    assert client.payload is None
    assert order.order_submit_started_at is None
    assert order.order_ack_at is None


def test_hyperliquid_recovery_filled_response_updates_reduce_allocation() -> None:
    class Db:
        def __init__(self, allocation):
            self.allocation = allocation

        async def get(self, model, key):
            if model is LeaderPositionAllocationRecord and key == self.allocation.id:
                return self.allocation
            return None

    allocation = LeaderPositionAllocationRecord(
        id=1,
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="HYPE",
        dex="",
        canonical_coin="HYPE",
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        venue_symbol="HYPE",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    order = ExecutionOrder(
        allocation_id=1,
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        status="PENDING_SUBMIT",
        reduce_only=True,
        dry_run=False,
    )
    response = {
        "status": "ok",
        "response": {
            "type": "order",
            "data": {"statuses": [{"filled": {"totalSz": "0.25", "avgPx": "100", "oid": 456}}]},
        },
    }

    _apply_hyperliquid_order_response(order, response)
    asyncio.run(_apply_hyperliquid_allocation_delta(Db(allocation), order, Decimal("0")))

    assert order.status == "FILLED"
    assert order.executed_qty == Decimal("0.25")
    assert allocation.allocated_qty == Decimal("0.75")
    assert allocation.allocated_notional == Decimal("75.00000000")


def test_hyperliquid_recovery_does_not_project_follower_account_state() -> None:
    allocation = LeaderPositionAllocationRecord(
        id=1,
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="HYPE",
        dex="",
        canonical_coin="HYPE",
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        venue_symbol="HYPE",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )

    class Db:
        def __init__(self):
            self.statements = []

        async def get(self, model, key):
            if model is LeaderPositionAllocationRecord and key == allocation.id:
                return allocation
            return None

        async def scalar(self, stmt):
            self.statements.append(stmt)
            text = compiled_sql(stmt)
            if "latest_account_states" in text:
                raise AssertionError("order recovery must not read follower account state")
            if "latest_account_positions" in text:
                raise AssertionError("order recovery must not read follower positions")
            return None

        def add(self, item):
            return None

    order = ExecutionOrder(
        allocation_id=1,
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        status="FILLED",
        executed_qty=Decimal("0.25"),
        avg_fill_price=Decimal("100"),
        reduce_only=True,
        dry_run=False,
    )
    db = Db()

    asyncio.run(_apply_hyperliquid_allocation_delta(db, order, Decimal("0")))

    assert allocation.allocated_qty == Decimal("0.75")
    assert allocation.allocated_notional == Decimal("75.00000000")

    projection_sql = [
        compiled_sql(stmt)
        for stmt in db.statements
        if "latest_account_states" in compiled_sql(stmt) or "latest_account_positions" in compiled_sql(stmt)
    ]
    assert not projection_sql


def test_hyperliquid_recovery_refreshes_stale_allocation_before_delta() -> None:
    class Db:
        def __init__(self, stale_allocation, fresh_allocation):
            self.stale_allocation = stale_allocation
            self.fresh_allocation = fresh_allocation
            self.scalar_calls = 0
            self.events = []

        async def flush(self):
            return None

        async def scalar(self, stmt):
            self.scalar_calls += 1
            if self.scalar_calls == 1:
                return None
            if self.scalar_calls == 2:
                return self.fresh_allocation
            return None

        async def get(self, model, key):
            if model is LeaderPositionAllocationRecord and key == self.stale_allocation.id:
                return self.stale_allocation
            return None

        def add(self, item):
            self.events.append(item)

    stale_allocation = LeaderPositionAllocationRecord(
        id=1,
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="INTC",
        dex="xyz",
        canonical_coin="xyz:INTC",
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
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
        id=1,
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="INTC",
        dex="xyz",
        canonical_coin="xyz:INTC",
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        venue_symbol="INTC",
        position_side="LONG",
        target_notional=Decimal("284"),
        allocated_notional=Decimal("284"),
        allocated_qty=Decimal("2.84"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    order = ExecutionOrder(
        id=10,
        allocation_id=1,
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="INTC",
        canonical_coin="xyz:INTC",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        status="FILLED",
        executed_qty=Decimal("0.13"),
        avg_fill_price=Decimal("100"),
        reduce_only=True,
        dry_run=False,
    )

    asyncio.run(_apply_hyperliquid_allocation_delta(Db(stale_allocation, fresh_allocation), order, Decimal("0")))

    assert stale_allocation.allocated_qty == Decimal("3.19")
    assert fresh_allocation.allocated_qty == Decimal("2.71")
    assert fresh_allocation.allocated_notional == Decimal("271.00000000")


def test_hyperliquid_order_status_filled_uses_orig_size_for_recovery_delta() -> None:
    class Db:
        def __init__(self, allocation):
            self.allocation = allocation

        async def get(self, model, key):
            if model is LeaderPositionAllocationRecord and key == self.allocation.id:
                return self.allocation
            return None

    allocation = LeaderPositionAllocationRecord(
        id=1,
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="HYPE",
        dex="",
        canonical_coin="HYPE",
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        venue_symbol="HYPE",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status="OPEN",
    )
    order = ExecutionOrder(
        allocation_id=1,
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="SELL",
        position_side="LONG",
        order_action="REDUCE",
        quantity=Decimal("0.16"),
        estimated_price=Decimal("100"),
        status="PENDING_SUBMIT",
        reduce_only=True,
        dry_run=False,
    )
    response = {
        "status": "order",
        "order": {
            "status": "filled",
            "order": {
                "coin": "HYPE",
                "side": "A",
                "limitPx": "10",
                "sz": "0.0",
                "origSz": "0.16",
                "oid": 456,
            },
        },
    }

    _apply_hyperliquid_order_response(order, response)
    asyncio.run(_apply_hyperliquid_allocation_delta(Db(allocation), order, Decimal("0")))

    assert order.status == "FILLED"
    assert order.executed_qty == Decimal("0.16")
    assert order.avg_fill_price == Decimal("100")
    assert order.venue_order_id == "456"
    assert allocation.allocated_qty == Decimal("0.84")
    assert allocation.allocated_notional == Decimal("84.00000000")


def test_periodic_recovery_skips_fresh_pending_submit() -> None:
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="BUY",
        status="PENDING_SUBMIT",
        dry_run=False,
    )
    now = datetime.now(timezone.utc)
    order.created_at = now
    order.updated_at = now

    assert _recovery_order_due(order, now=now, min_pending_submit_age_seconds=10) is False


def test_periodic_recovery_includes_stale_pending_submit() -> None:
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="BUY",
        status="PENDING_SUBMIT",
        dry_run=False,
    )
    now = datetime.now(timezone.utc)
    order.created_at = now - timedelta(seconds=11)
    order.updated_at = order.created_at

    assert _recovery_order_due(order, now=now, min_pending_submit_age_seconds=10) is True


def test_periodic_recovery_includes_unknown_without_pending_delay() -> None:
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="BUY",
        status="UNKNOWN",
        dry_run=False,
    )
    now = datetime.now(timezone.utc)
    order.created_at = now
    order.updated_at = now

    assert _recovery_order_due(order, now=now, min_pending_submit_age_seconds=10) is True


def test_unknown_oid_recovery_defers_fresh_started_order() -> None:
    cloid = "0x" + "8" * 32
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="BUY",
        status="UNKNOWN",
        cloid=cloid,
        order_submit_started_at=datetime.now(timezone.utc) - timedelta(seconds=5),
        request_payload_masked={
            "coin": "HYPE",
            "dex": "",
            "is_buy": True,
            "sz": "0.1",
            "limit_px": "100",
            "reduce_only": False,
            "cloid": cloid,
        },
        dry_run=False,
    )
    now = datetime.now(timezone.utc)

    assert _should_defer_unknown_oid_recovery(order, now=now, unknown_oid_resubmit_age_seconds=30) is True
    assert _should_resubmit_stale_unknown_oid_order(order, now=now, unknown_oid_resubmit_age_seconds=30) is False


def test_unknown_oid_recovery_never_resubmits_stale_started_order() -> None:
    cloid = "0x" + "9" * 32
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="BUY",
        status="UNKNOWN",
        cloid=cloid,
        order_submit_started_at=datetime.now(timezone.utc) - timedelta(seconds=31),
        request_payload_masked={
            "coin": "HYPE",
            "dex": "",
            "is_buy": True,
            "sz": "0.1",
            "limit_px": "100",
            "reduce_only": False,
            "cloid": cloid,
        },
        dry_run=False,
    )
    now = datetime.now(timezone.utc)

    assert _should_defer_unknown_oid_recovery(order, now=now, unknown_oid_resubmit_age_seconds=30) is False
    assert _should_resubmit_stale_unknown_oid_order(order, now=now, unknown_oid_resubmit_age_seconds=30) is False


def test_unknown_oid_recovery_does_not_resubmit_without_payload() -> None:
    order = ExecutionOrder(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        leader_address="0x" + "1" * 40,
        source_coin="HYPE",
        side="BUY",
        status="UNKNOWN",
        cloid="0x" + "a" * 32,
        order_submit_started_at=datetime.now(timezone.utc) - timedelta(seconds=31),
        dry_run=False,
    )
    now = datetime.now(timezone.utc)

    assert _should_defer_unknown_oid_recovery(order, now=now, unknown_oid_resubmit_age_seconds=30) is False
    assert _should_resubmit_stale_unknown_oid_order(order, now=now, unknown_oid_resubmit_age_seconds=30) is False


def test_preflight_model_has_separate_hyperliquid_and_binance_ready_flags() -> None:
    result = ExecutionRouter().venue_readiness(
        hyperliquid_ready=True,
        binance_ready=False,
    )
    assert result["ready_for_live_hyperliquid"] is True
    assert result["ready_for_live_binance"] is False


def test_preflight_detects_shaz_style_residual_even_when_follower_matches_ledger() -> None:
    row = LeaderPositionAllocationRecord(
        id=494,
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="SHAZ",
        dex="xyz",
        canonical_coin="xyz:SHAZ",
        execution_venue="HYPERLIQUID",
        venue_symbol="xyz:SHAZ",
        position_side="LONG",
        target_notional=Decimal("0"),
        allocated_notional=Decimal("2.007516"),
        allocated_qty=Decimal("0.03"),
        avg_entry_price=Decimal("66.9172"),
        copy_multiplier=Decimal("1"),
        status="REDUCING",
        last_leader_position_size=Decimal("0"),
        last_leader_position_notional=Decimal("0"),
        pending_reduce_qty=Decimal("0.03"),
    )

    residuals = _terminal_flat_leader_allocation_residuals([row])

    assert residuals == [
        {
            "allocation_id": 494,
            "leader_id": 1,
            "symbol": "xyz:SHAZ",
            "dex": "xyz",
            "position_side": "LONG",
            "status": "REDUCING",
            "allocated_qty": "0.03",
            "allocated_notional": "2.007516",
            "pending_reduce_qty": "0.03",
            "message": "leader is flat but the follower allocation remains nonzero",
        }
    ]


def test_global_kill_switch_blocks_both_venues() -> None:
    assert venue_live_allowed(
        global_trading_enabled=True,
        venue_trading_enabled=True,
        kill_switch_active=True,
        venue_ready=True,
    ) is False


def test_hyperliquid_trading_disabled_is_dry_run_only() -> None:
    assert venue_live_allowed(
        global_trading_enabled=True,
        venue_trading_enabled=False,
        kill_switch_active=False,
        venue_ready=True,
    ) is False


def test_binance_trading_disabled_is_dry_run_only() -> None:
    assert venue_live_allowed(
        global_trading_enabled=True,
        venue_trading_enabled=False,
        kill_switch_active=False,
        venue_ready=True,
    ) is False


def test_global_trading_disabled_is_dry_run_for_all_venues() -> None:
    assert venue_live_allowed(
        global_trading_enabled=False,
        venue_trading_enabled=True,
        kill_switch_active=False,
        venue_ready=True,
    ) is False


def test_slippage_warning_does_not_block_route() -> None:
    result = ExecutionRouter().route_order(
        leader_id=1,
        hyperliquid_coin="BTC",
        preferred_venue=VenuePreference.HYPERLIQUID,
        fallback_venue=FallbackVenue.NONE,
        hyperliquid=VenueAvailability(available=True, reason="slippage warning"),
        binance=VenueAvailability(available=False, reason="disabled"),
        warnings=["slippage_bps_high"],
    )
    assert result.status == VenueRouteStatus.OK
    assert result.warnings == ["slippage_bps_high"]


def test_hyperliquid_meta_missing_coin_blocks() -> None:
    result = ExecutionRouter().route_order(
        leader_id=1,
        hyperliquid_coin="NOPE",
        preferred_venue=VenuePreference.HYPERLIQUID,
        fallback_venue=FallbackVenue.NONE,
        hyperliquid=VenueAvailability(available=False, reason="coin not in Hyperliquid universe"),
        binance=VenueAvailability(available=False, reason="mapping missing"),
    )
    assert result.status == VenueRouteStatus.BLOCKED


def test_manual_hyperliquid_order_source_manual_shape() -> None:
    order = HyperliquidExecutionClient.manual_order_record(
        coin="BTC",
        side="BUY",
        source_type="MANUAL",
    )
    assert order["execution_venue"] == "HYPERLIQUID"
    assert order["source_type"] == "MANUAL"


def test_manual_binance_order_source_manual_shape() -> None:
    result = ExecutionRouter().manual_order_source(ExecutionVenue.BINANCE)
    assert result == {"execution_venue": "BINANCE", "source_type": "MANUAL"}
