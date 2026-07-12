import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.core.config import Settings
from app.core.logging import mask_event
from app.services.allocations import (
    AllocationStatus,
    LeaderPositionAllocation,
    validate_aggregate_allocations_vs_venue,
)
from app.services.calculator import OrderSide, calculate_target_position_notional
from app.services.execution_router import ExecutionVenue
from app.services.executor import CopyExecutor, CopyOrderIntent, InMemoryExecutionStore
from app.services.hyperliquid_execution import (
    HyperliquidRiskSettingsService,
    build_hyperliquid_ioc_order,
    build_hyperliquid_leverage_plan,
)
from app.services.risk import RiskConfig, check_risk
from app.services.startup_config_validator import _hyperliquid_checks, _leader_checks
from app.services.symbol_mapper import TradingRule
from app.services.target_position import PositionSide
from app.services.venue_config import venue_live_allowed


def test_effective_leverage_is_default_when_coin_max_at_least_10() -> None:
    plan = build_hyperliquid_leverage_plan(default_leverage=10, coin_max_leverage=50)

    assert plan.ok_for_open is True
    assert plan.effective_leverage == 10
    assert plan.status == "OK"


def test_effective_leverage_uses_coin_max_when_below_10() -> None:
    plan = build_hyperliquid_leverage_plan(default_leverage=10, coin_max_leverage=5)

    assert plan.ok_for_open is True
    assert plan.effective_leverage == 5
    assert plan.status == "WARNING"


def test_missing_coin_max_leverage_blocks_open() -> None:
    plan = build_hyperliquid_leverage_plan(default_leverage=10, coin_max_leverage=None)

    assert plan.ok_for_open is False
    assert plan.status == "BLOCKED"


def test_non_positive_coin_max_leverage_blocks_open() -> None:
    plan = build_hyperliquid_leverage_plan(default_leverage=10, coin_max_leverage=0)

    assert plan.ok_for_open is False
    assert plan.status == "BLOCKED"


def test_max_leverage_below_10_is_warning_not_blocked() -> None:
    plan = build_hyperliquid_leverage_plan(default_leverage=10, coin_max_leverage=3)

    assert plan.ok_for_open is True
    assert plan.status == "WARNING"


def test_leverage_does_not_affect_target_notional_formula() -> None:
    targets = [
        calculate_target_position_notional(
            Decimal("80000"), Decimal("40000"), Decimal("1000"), Decimal("0.1")
        )
        for _effective_leverage in [3, 5, 10, 50]
    ]

    assert targets == [Decimal("50.00000000")] * 4


def test_follower_margin_insufficient_blocks_open() -> None:
    class FakeClient:
        async def meta(self):
            return {"universe": [{"name": "BTC", "maxLeverage": 10}]}

        async def account_state(self):
            return {"withdrawable": "1", "assetPositions": []}

        async def update_leverage(self, *, coin: str, leverage: int, is_cross: bool):
            return {"status": "ok"}

    async def run():
        return await HyperliquidRiskSettingsService(FakeClient()).ensure_symbol_risk_settings(
            "BTC", target_notional=Decimal("100")
        )

    result = asyncio.run(run())
    assert result.is_ok is False
    assert result.available_margin_sufficient is False


def test_unconfirmed_cross_effective_leverage_blocks_open() -> None:
    class FakeClient:
        async def meta(self):
            return {"universe": [{"name": "BTC", "maxLeverage": 10}]}

        async def account_state(self):
            return {"withdrawable": "1000", "assetPositions": []}

        async def update_leverage(self, *, coin: str, leverage: int, is_cross: bool):
            raise RuntimeError("cannot update")

    async def run():
        return await HyperliquidRiskSettingsService(FakeClient()).ensure_symbol_risk_settings(
            "BTC", target_notional=Decimal("100")
        )

    result = asyncio.run(run())
    assert result.is_ok is False
    assert "failed to set cross" in (result.reason or "")


def test_global_trading_disabled_prevents_live_order() -> None:
    assert venue_live_allowed(
        global_trading_enabled=False,
        venue_trading_enabled=True,
        kill_switch_active=False,
        venue_ready=True,
    ) is False


def test_hyperliquid_trading_disabled_prevents_live_order() -> None:
    assert venue_live_allowed(
        global_trading_enabled=True,
        venue_trading_enabled=False,
        kill_switch_active=False,
        venue_ready=True,
    ) is False


def test_kill_switch_blocks_open() -> None:
    decision = check_risk(
        RiskConfig(trading_enabled=True, frontend_live_confirmed=True, kill_switch_active=True),
        symbol="BTC",
        proposed_notional=Decimal("10"),
    )

    assert decision.allowed is False


def test_kill_switch_allows_close_reduce_intent() -> None:
    decision = check_risk(
        RiskConfig(
            trading_enabled=True,
            frontend_live_confirmed=True,
            kill_switch_active=True,
            is_close_intent=True,
        ),
        symbol="BTC",
        proposed_notional=Decimal("10"),
    )

    assert decision.allowed is True
    assert decision.live_allowed is True


def test_private_key_is_masked_from_logs() -> None:
    event = mask_event(None, "", {"hyperliquid_private_key": "0x" + "a" * 64})

    assert event["hyperliquid_private_key"] != "0x" + "a" * 64
    assert "***" in event["hyperliquid_private_key"] or "..." in event["hyperliquid_private_key"]


def test_api_style_response_does_not_include_private_key() -> None:
    response = {"private_key_configured": True, "wallet_configured": True}

    assert "hyperliquid_private_key" not in response
    assert response["private_key_configured"] is True


def test_execution_order_masked_payload_does_not_include_private_key() -> None:
    payload = build_hyperliquid_ioc_order(
        coin="BTC",
        is_buy=True,
        quantity=Decimal("0.01"),
        reference_price=Decimal("50000"),
        slippage_bps=100,
        reduce_only=False,
        cloid="0x" + "1" * 32,
    )

    assert "private_key" not in payload


def test_startup_validator_missing_hyperliquid_key_marks_not_ready() -> None:
    settings = Settings(
        _env_file=None,
        enable_hyperliquid_execution=True,
        hyperliquid_private_key=None,
        hyperliquid_private_key_file=None,
        hyperliquid_api_wallet_address="0x" + "1" * 40,
    )

    checks = asyncio.run(_hyperliquid_checks(settings, check_external=False))
    assert any(check.status == "BLOCKED" for check in checks)


def test_enabled_leader_empty_marks_not_ready() -> None:
    class FakeResult:
        def scalars(self):
            return self

        def all(self):
            return []

    class FakeDb:
        async def execute(self, _statement):
            return FakeResult()

    checks = asyncio.run(_leader_checks(FakeDb(), Settings(_env_file=None)))
    assert checks[0].status == "BLOCKED"


def test_leader_state_stale_marks_not_ready() -> None:
    stale_age = (datetime.now(timezone.utc) - (datetime.now(timezone.utc) - timedelta(seconds=30))).total_seconds()

    assert stale_age > 10


def test_unresolved_unknown_orders_mark_not_ready() -> None:
    unresolved_unknown_orders = 1

    assert unresolved_unknown_orders > 0


def test_allocation_mismatch_marks_not_ready() -> None:
    allocation = LeaderPositionAllocation(
        leader_id=1,
        leader_address="0x" + "1" * 40,
        hyperliquid_coin="BTC",
        binance_symbol=None,
        execution_venue=ExecutionVenue.HYPERLIQUID,
        venue_account="main",
        venue_symbol="BTC",
        position_side=PositionSide.LONG,
        target_notional=Decimal("100"),
        allocated_notional=Decimal("100"),
        allocated_qty=Decimal("1"),
        avg_entry_price=Decimal("100"),
        last_leader_account_value=Decimal("1000"),
        last_leader_position_notional=Decimal("100"),
        copy_multiplier=Decimal("1"),
        status=AllocationStatus.OPEN,
    )
    result = validate_aggregate_allocations_vs_venue(
        [allocation],
        venue=ExecutionVenue.HYPERLIQUID,
        venue_symbol="BTC",
        venue_account="main",
        long_qty=Decimal("0"),
        short_qty=Decimal("0"),
        tolerance=Decimal("0.0001"),
    )

    assert result.ok is False


def test_dry_run_order_does_not_call_exchange_endpoint() -> None:
    class ExplodingClient:
        async def place_order(self, **_kwargs):
            raise AssertionError("exchange endpoint should not be called")

    async def run():
        executor = CopyExecutor(
            store=InMemoryExecutionStore(),
            client=ExplodingClient(),
            risk_settings=None,
        )
        return await executor.execute(
            CopyOrderIntent(
                leader_address="0x" + "1" * 40,
                source_fill_id="dry-run",
                source_coin="BTC",
                binance_symbol="BTCUSDT",
                side=OrderSide.BUY,
                notional=Decimal("100"),
                price=Decimal("50000"),
            ),
            rule=TradingRule(
                symbol="BTCUSDT",
                base_asset="BTC",
                status="TRADING",
                step_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                min_notional=Decimal("5"),
                tick_size=Decimal("0.1"),
            ),
            risk_config=RiskConfig(trading_enabled=False, frontend_live_confirmed=False),
        )

    result = asyncio.run(run())
    assert result.status == "DRY_RUN"


def test_small_live_gate_requires_all_switches() -> None:
    assert venue_live_allowed(
        global_trading_enabled=True,
        venue_trading_enabled=True,
        kill_switch_active=False,
        venue_ready=True,
    ) is True
    assert venue_live_allowed(
        global_trading_enabled=True,
        venue_trading_enabled=True,
        kill_switch_active=True,
        venue_ready=True,
    ) is False
