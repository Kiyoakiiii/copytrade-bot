import asyncio
import re
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.schemas.api import ManualOrderRequest
from app.services.allocations import (
    AllocationStatus,
    LeaderPositionAllocation,
    apply_execution_fill_to_allocation,
    can_open_new_allocation,
    validate_aggregate_allocations_vs_binance,
    AggregatePosition,
)
from app.services.auto_copy import (
    FillDrivenReconcileDispatcher,
    build_auto_copy_market_order,
    build_auto_copy_new_client_order_id,
    calculate_latency_fields,
    extract_market_fill,
    recover_unknown_order_by_client_id,
)
from app.services.calculator import OrderSide
from app.services.executor import CopyExecutor, CopyOrderIntent, InMemoryExecutionStore
from app.services.risk import RiskConfig
from app.services.risk_settings import RiskSettingsResult
from app.services.symbol_mapper import TradingRule
from app.services.target_position import PositionSide


def rule() -> TradingRule:
    return TradingRule(
        symbol="BTCUSDT",
        base_asset="BTC",
        status="TRADING",
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("5"),
        tick_size=Decimal("0.10"),
    )


def intent(
    *,
    side: OrderSide = OrderSide.BUY,
    position_side: str = "LONG",
    reduce_only: bool = False,
    source_fill_id: str = "fill-abcdef123456",
    use_market_order: bool = True,
) -> CopyOrderIntent:
    return CopyOrderIntent(
        leader_address="0xabcdef1234567890",
        source_fill_id=source_fill_id,
        source_coin="BTC",
        binance_symbol="BTCUSDT",
        side=side,
        notional=Decimal("100"),
        price=Decimal("50000"),
        reduce_only=reduce_only,
        position_side=position_side,
        event_time_ms=1_714_000_000_000,
        use_market_order=use_market_order,
        leader_account_value=Decimal("1000000"),
        leader_account_value_source="CLEARINGHOUSE_STATE",
        leader_position_notional=Decimal("100000"),
        follower_account_value=Decimal("1000"),
        follower_account_value_source="SPOT_CLEARINGHOUSE_STATE",
        leader_position_ratio=Decimal("0.1"),
        copy_multiplier=Decimal("1"),
        target_notional=Decimal("100"),
        delta_notional=Decimal("100"),
    )


def allocation(qty: str = "0.2", side: PositionSide = PositionSide.LONG) -> LeaderPositionAllocation:
    return LeaderPositionAllocation(
        leader_id=1,
        leader_address="0xabcdef1234567890",
        hyperliquid_coin="BTC",
        binance_symbol="BTCUSDT",
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


class FilledClient:
    def __init__(self, response=None) -> None:
        self.orders = []
        self.response = response or {
            "orderId": 123,
            "status": "FILLED",
            "clientOrderId": "x",
            "executedQty": "0.002",
            "avgPrice": "50010",
            "cumQuote": "100.02",
        }

    async def place_order(self, **kwargs):
        self.orders.append(kwargs)
        return {**self.response, "clientOrderId": kwargs.get("new_client_order_id", "x")}


class TimeoutClient:
    def __init__(self) -> None:
        self.orders = []

    async def place_order(self, **kwargs):
        self.orders.append(kwargs)
        raise TimeoutError("request timed out after submit")


class OkRiskSettings:
    async def ensure_symbol_risk_settings(self, symbol: str, *, reduce_only: bool):
        return RiskSettingsResult.ok(symbol=symbol, position_side="LONG,SHORT")


def live_config(**kwargs) -> RiskConfig:
    return RiskConfig(
        trading_enabled=True,
        frontend_live_confirmed=True,
        max_notional_per_trade=Decimal("1000"),
        **kwargs,
    )


def test_auto_open_long_is_market_only() -> None:
    order = build_auto_copy_market_order(
        symbol="BTCUSDT",
        allocation_side=PositionSide.LONG,
        action="OPEN_OR_INCREASE",
        quantity=Decimal("0.002"),
    )
    assert order["type"] == "MARKET"
    assert order["side"] == "BUY"
    assert order["positionSide"] == "LONG"
    assert "price" not in order
    assert "timeInForce" not in order


def test_auto_close_long_is_market_only() -> None:
    order = build_auto_copy_market_order(
        symbol="BTCUSDT",
        allocation_side=PositionSide.LONG,
        action="CLOSE_OR_REDUCE",
        quantity=Decimal("0.002"),
        is_close_intent=True,
        leader_allocation_qty=Decimal("0.01"),
        binance_position_qty=Decimal("0.02"),
    )
    assert order["type"] == "MARKET"
    assert order["side"] == "SELL"
    assert order["positionSide"] == "LONG"


def test_auto_open_short_and_close_short_are_market_only() -> None:
    open_order = build_auto_copy_market_order(
        symbol="BTCUSDT",
        allocation_side=PositionSide.SHORT,
        action="OPEN_OR_INCREASE",
        quantity=Decimal("0.002"),
    )
    close_order = build_auto_copy_market_order(
        symbol="BTCUSDT",
        allocation_side=PositionSide.SHORT,
        action="CLOSE_OR_REDUCE",
        quantity=Decimal("0.002"),
        is_close_intent=True,
        leader_allocation_qty=Decimal("0.01"),
        binance_position_qty=Decimal("0.02"),
    )
    assert open_order["type"] == close_order["type"] == "MARKET"
    assert open_order["side"] == "SELL"
    assert close_order["side"] == "BUY"
    assert open_order["positionSide"] == close_order["positionSide"] == "SHORT"


def test_auto_market_order_forbidden_fields_are_absent() -> None:
    order = build_auto_copy_market_order(
        symbol="BTCUSDT",
        allocation_side=PositionSide.LONG,
        action="OPEN_OR_INCREASE",
        quantity=Decimal("0.002"),
    )
    for key in ["price", "timeInForce", "reduceOnly", "closePosition"]:
        assert key not in order


def test_auto_order_rejects_position_side_both() -> None:
    with pytest.raises(ValueError):
        build_auto_copy_market_order(
            symbol="BTCUSDT",
            allocation_side=PositionSide.FLAT,
            action="OPEN_OR_INCREASE",
            quantity=Decimal("0.002"),
        )


def test_use_market_order_false_is_ignored_for_auto_copy() -> None:
    async def run():
        client = FilledClient()
        executor = CopyExecutor(
            store=InMemoryExecutionStore(),
            client=client,
            risk_settings=OkRiskSettings(),
        )
        result = await executor.execute(
            intent(use_market_order=False),
            rule=rule(),
            risk_config=live_config(),
        )
        return result, client.orders[0]

    result, order = asyncio.run(run())
    assert result.status == "FILLED"
    assert order["order_type"] == "MARKET"


def test_executor_auto_order_has_client_id_and_no_limit_params() -> None:
    async def run():
        client = FilledClient()
        executor = CopyExecutor(
            store=InMemoryExecutionStore(),
            client=client,
            risk_settings=OkRiskSettings(),
        )
        result = await executor.execute(intent(), rule=rule(), risk_config=live_config())
        return result, client.orders[0]

    result, order = asyncio.run(run())
    assert result.client_order_id
    assert len(result.client_order_id) <= 36
    assert re.match(r"^[.A-Z:/a-z0-9_-]{1,36}$", result.client_order_id)
    assert order["order_type"] == "MARKET"
    assert order["position_side"] == "LONG"
    assert order["reduce_only"] is False
    assert "price" not in order
    assert "time_in_force" not in order


def test_automatic_unknown_order_is_not_retried_by_source_fill() -> None:
    async def run():
        client = TimeoutClient()
        store = InMemoryExecutionStore()
        executor = CopyExecutor(
            store=store,
            client=client,
            risk_settings=OkRiskSettings(),
        )
        first = await executor.execute(intent(), rule=rule(), risk_config=live_config())
        second = await executor.execute(intent(), rule=rule(), risk_config=live_config())
        return first, second, client.orders, store.results

    first, second, orders, results = asyncio.run(run())
    assert first.status == "UNKNOWN"
    assert second.status == "DUPLICATE"
    assert len(orders) == 1
    assert results[0].status == "UNKNOWN"


def test_recover_unknown_order_queries_by_client_order_id() -> None:
    class QueryClient:
        def __init__(self):
            self.queries = []

        async def get_order(self, *, symbol: str, orig_client_order_id: str):
            self.queries.append((symbol, orig_client_order_id))
            return {"status": "FILLED", "executedQty": "0.002", "avgPrice": "50000"}

    async def run():
        client = QueryClient()
        result = await recover_unknown_order_by_client_id(
            client,
            symbol="BTCUSDT",
            client_order_id="ct_abcdef_BTCUSDT_L_O_123456_zzzzzz",
        )
        return client.queries, result

    queries, result = asyncio.run(run())
    assert queries == [("BTCUSDT", "ct_abcdef_BTCUSDT_L_O_123456_zzzzzz")]
    assert result["status"] == "FILLED"


def test_new_client_order_id_is_binance_safe_and_traceable() -> None:
    value = build_auto_copy_new_client_order_id(
        leader_address="0xabcdef1234567890",
        symbol="BTCUSDT",
        position_side="LONG",
        action="OPEN_OR_INCREASE",
        source_fill_id="fedcba9876543210",
        timestamp_ms=1_714_000_000_000,
    )
    assert value.startswith("ct_abcdef_BTCUSDT_L_O_fedcba_")
    assert len(value) <= 36
    assert re.match(r"^[.A-Z:/a-z0-9_-]{1,36}$", value)


def test_market_fill_extraction_prefers_actual_binance_values() -> None:
    fill = extract_market_fill(
        {
            "executedQty": "0.003",
            "avgPrice": "50100",
            "cumQuote": "150.30",
            "status": "FILLED",
        },
        estimated_price=Decimal("50000"),
    )
    assert fill.executed_qty == Decimal("0.003")
    assert fill.avg_fill_price == Decimal("50100")
    assert fill.cum_quote == Decimal("150.30")
    assert fill.slippage_bps == Decimal("20.00000000")


def test_allocation_updates_by_actual_executed_qty() -> None:
    updated = apply_execution_fill_to_allocation(
        allocation("0.2"),
        action="OPEN_OR_INCREASE",
        executed_qty=Decimal("0.05"),
        avg_fill_price=Decimal("51000"),
    )
    assert updated.allocated_qty == Decimal("0.25")
    assert updated.allocated_notional == Decimal("12550.00000000")


def test_partial_close_updates_only_actual_fill_qty() -> None:
    updated = apply_execution_fill_to_allocation(
        allocation("0.2"),
        action="CLOSE_OR_REDUCE",
        executed_qty=Decimal("0.05"),
        avg_fill_price=Decimal("50000"),
    )
    assert updated.allocated_qty == Decimal("0.15")
    assert updated.status == AllocationStatus.OPEN


def test_full_close_marks_allocation_closed() -> None:
    updated = apply_execution_fill_to_allocation(
        allocation("0.2"),
        action="CLOSE_OR_REDUCE",
        executed_qty=Decimal("0.2"),
        avg_fill_price=Decimal("50000"),
    )
    assert updated.allocated_qty == Decimal("0")
    assert updated.status == AllocationStatus.CLOSED


def test_external_manual_position_mismatch_blocks_new_opens_without_reopen() -> None:
    result = validate_aggregate_allocations_vs_binance(
        [allocation("0.2")],
        AggregatePosition(symbol="BTCUSDT", long_qty=Decimal("0.1"), short_qty=Decimal("0")),
        tolerance=Decimal("0.00000001"),
        source="MANUAL",
    )
    assert result.ok is False
    assert result.event_type == "ALLOCATION_MISMATCH"
    assert can_open_new_allocation(result) is False


def test_trading_disabled_and_kill_switch_do_not_submit_orders() -> None:
    async def run(config: RiskConfig):
        client = FilledClient()
        executor = CopyExecutor(store=InMemoryExecutionStore(), client=client)
        result = await executor.execute(intent(), rule=rule(), risk_config=config)
        return result, client.orders

    dry, dry_orders = asyncio.run(run(RiskConfig(trading_enabled=False)))
    killed, killed_orders = asyncio.run(run(live_config(kill_switch_active=True)))
    assert dry.status == "DRY_RUN"
    assert dry_orders == []
    assert killed.status == "REJECTED"
    assert killed_orders == []


def test_missing_hedge_or_isolated_or_10x_blocks_live_open() -> None:
    class BadRisk:
        async def ensure_symbol_risk_settings(self, symbol: str, *, reduce_only: bool):
            return RiskSettingsResult.blocked(
                symbol=symbol,
                reason="leverage is 20; expected 10",
                margin_type="ISOLATED",
                leverage=20,
            )

    async def run():
        client = FilledClient()
        executor = CopyExecutor(
            store=InMemoryExecutionStore(),
            client=client,
            risk_settings=BadRisk(),
        )
        result = await executor.execute(intent(), rule=rule(), risk_config=live_config())
        return result, client.orders

    result, orders = asyncio.run(run())
    assert result.status == "BLOCKED"
    assert orders == []


def test_manual_request_accepts_market_and_limit_source_manual() -> None:
    market = ManualOrderRequest(
        symbol="BTCUSDT",
        position_side="LONG",
        action="OPEN_OR_INCREASE",
        order_type="MARKET",
        quantity=Decimal("0.01"),
        confirmation="CONFIRM",
    )
    limit = ManualOrderRequest(
        symbol="BTCUSDT",
        position_side="LONG",
        action="OPEN_OR_INCREASE",
        order_type="LIMIT",
        quantity=Decimal("0.01"),
        price=Decimal("50000"),
        confirmation="CONFIRM",
    )
    assert market.order_type == "MARKET"
    assert limit.order_type == "LIMIT"


def test_fill_driven_dispatcher_calls_reconcile_immediately_per_key() -> None:
    calls = []

    async def reconcile(**kwargs):
        calls.append(kwargs)
        return "ok"

    async def run():
        dispatcher = FillDrivenReconcileDispatcher(reconcile)
        return await dispatcher.handle_fill(
            leader_id=1,
            symbol="BTCUSDT",
            position_side="LONG",
            source_fill_id="fill-1",
        )

    assert asyncio.run(run()) == "ok"
    assert calls == [
        {
            "leader_id": 1,
            "symbol": "BTCUSDT",
            "position_side": "LONG",
            "source_fill_id": "fill-1",
        }
    ]


def test_latency_fields_are_computed() -> None:
    event = datetime.fromtimestamp(1000, timezone.utc)
    received = datetime.fromtimestamp(1001, timezone.utc)
    submit = datetime.fromtimestamp(1003, timezone.utc)
    ack = datetime.fromtimestamp(1004, timezone.utc)
    finalized = datetime.fromtimestamp(1005, timezone.utc)
    fields = calculate_latency_fields(
        hyperliquid_event_time=event,
        event_received_at=received,
        binance_order_submit_at=submit,
        binance_order_ack_at=ack,
        order_finalized_at=finalized,
    )
    assert fields["event_to_receive_ms"] == 1000
    assert fields["receive_to_submit_ms"] == 2000
    assert fields["submit_to_ack_ms"] == 1000
    assert fields["event_to_ack_ms"] == 4000
    assert fields["event_to_final_ms"] == 5000
