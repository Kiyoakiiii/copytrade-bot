import asyncio
from decimal import Decimal

from app.services.calculator import OrderSide
from app.services.executor import CopyExecutor, CopyOrderIntent, InMemoryExecutionStore
from app.services.risk import RiskConfig
from app.services.symbol_mapper import TradingRule


class FakeBinanceClient:
    def __init__(self) -> None:
        self.orders = []

    async def place_order(self, **kwargs):
        self.orders.append(kwargs)
        return {"orderId": 123, "status": "FILLED"}


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


def intent(fill_id: str = "fill-1") -> CopyOrderIntent:
    return CopyOrderIntent(
        leader_address="0xleader",
        source_fill_id=fill_id,
        source_coin="BTC",
        binance_symbol="BTCUSDT",
        side=OrderSide.BUY,
        notional=Decimal("100"),
        price=Decimal("50000"),
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


def test_trading_disabled_records_dry_run_only() -> None:
    async def run():
        client = FakeBinanceClient()
        store = InMemoryExecutionStore()
        executor = CopyExecutor(store=store, client=client)
        result = await executor.execute(
            intent(), rule=rule(), risk_config=RiskConfig(trading_enabled=False)
        )
        return result, client.orders

    result, orders = asyncio.run(run())
    assert result.status == "DRY_RUN"
    assert orders == []


def test_live_requires_env_and_frontend_confirmation() -> None:
    async def run():
        client = FakeBinanceClient()
        executor = CopyExecutor(store=InMemoryExecutionStore(), client=client)
        result = await executor.execute(
            intent(),
            rule=rule(),
            risk_config=RiskConfig(
                trading_enabled=True,
                frontend_live_confirmed=True,
                max_notional_per_trade=Decimal("200"),
            ),
        )
        return result, client.orders

    result, orders = asyncio.run(run())
    assert result.status == "FILLED"
    assert result.dry_run is False
    assert orders[0]["quantity"] == "0.002"


def test_duplicate_fill_does_not_submit_twice() -> None:
    async def run():
        client = FakeBinanceClient()
        store = InMemoryExecutionStore()
        executor = CopyExecutor(store=store, client=client)
        live = RiskConfig(trading_enabled=True, frontend_live_confirmed=True)
        first = await executor.execute(intent(), rule=rule(), risk_config=live)
        second = await executor.execute(intent(), rule=rule(), risk_config=live)
        return first, second, client.orders

    first, second, orders = asyncio.run(run())
    assert first.status == "FILLED"
    assert second.status == "DUPLICATE"
    assert len(orders) == 1


def test_risk_limit_rejects_order() -> None:
    async def run():
        executor = CopyExecutor(store=InMemoryExecutionStore())
        return await executor.execute(
            intent(),
            rule=rule(),
            risk_config=RiskConfig(max_notional_per_trade=Decimal("50")),
        )

    result = asyncio.run(run())
    assert result.status == "REJECTED"
    assert "max_notional_per_trade" in result.reason


def test_dry_run_order_preserves_account_ratio_sizing_metadata() -> None:
    async def run():
        executor = CopyExecutor(store=InMemoryExecutionStore())
        return await executor.execute(
            CopyOrderIntent(
                leader_address="0xleader",
                source_fill_id="fill-sizing",
                source_coin="BTC",
                binance_symbol="BTCUSDT",
                side=OrderSide.BUY,
                notional=Decimal("10"),
                price=Decimal("1000"),
                leader_account_value=Decimal("1000000"),
                leader_position_notional=Decimal("100000"),
                follower_account_value=Decimal("1000"),
                leader_position_ratio=Decimal("0.1"),
                copy_multiplier=Decimal("0.1"),
                target_notional=Decimal("10"),
                delta_notional=Decimal("10"),
            ),
            rule=rule(),
            risk_config=RiskConfig(trading_enabled=False),
        )

    result = asyncio.run(run())
    assert result.status == "DRY_RUN"
    assert result.sizing_mode == "ACCOUNT_RATIO"
    assert result.leader_account_value == Decimal("1000000")
    assert result.leader_position_notional == Decimal("100000")
    assert result.follower_account_value == Decimal("1000")
    assert result.copy_multiplier == Decimal("0.1")
    assert result.target_notional == Decimal("10")
    assert result.delta_notional == Decimal("10")
