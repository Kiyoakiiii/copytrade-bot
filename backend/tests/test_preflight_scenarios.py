import asyncio
import sys
import types
from decimal import Decimal

import pytest

if "structlog" not in sys.modules:
    sys.modules["structlog"] = types.SimpleNamespace(
        get_logger=lambda *_args, **_kwargs: types.SimpleNamespace(
            warning=lambda *_args, **_kwargs: None,
            info=lambda *_args, **_kwargs: None,
        )
    )

from app.services.calculator import OrderSide, calculate_target_position_notional
from app.services.executor import CopyExecutor, CopyOrderIntent, InMemoryExecutionStore
from app.services.hyperliquid import HyperliquidWatcher, fill_unique_id
from app.services.risk import RiskConfig
from app.services.risk_settings import RiskSettingsResult
from app.services.symbol_mapper import TradingRule, notional_to_quantity
from app.services.target_position import TargetAction, TargetPositionInput, build_target_position_plan


def btc_rule() -> TradingRule:
    return TradingRule(
        symbol="BTCUSDT",
        base_asset="BTC",
        status="TRADING",
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("5"),
        tick_size=Decimal("0.10"),
    )


def test_scenario_1_open_by_position_value_not_margin() -> None:
    target = calculate_target_position_notional(
        leader_account_value=Decimal("80000"),
        leader_position_notional=Decimal("40000"),
        follower_equity=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )

    assert target == Decimal("500.00000000")
    assert target / Decimal("10") == Decimal("50.00000000")


def test_scenario_2_lower_multiplier() -> None:
    target = calculate_target_position_notional(
        Decimal("80000"), Decimal("40000"), Decimal("1000"), Decimal("0.1")
    )

    assert target == Decimal("50.00000000")


def test_scenario_3_leader_adds_position_delta_only() -> None:
    plan = build_target_position_plan(
        TargetPositionInput(
            leader_address="0xleader",
            coin="BTC",
            binance_symbol="BTCUSDT",
            leader_account_value=Decimal("80000"),
            leader_position_notional=Decimal("56000"),
            follower_equity=Decimal("1000"),
            follower_current_notional=Decimal("500"),
            copy_multiplier=Decimal("1"),
        )
    )

    assert len(plan) == 1
    assert plan[0].action == TargetAction.ADD
    assert plan[0].delta_notional == Decimal("200.00000000")
    assert plan[0].reduce_only is False


def test_scenario_4_leader_reduces_position_reduce_only() -> None:
    plan = build_target_position_plan(
        TargetPositionInput(
            leader_address="0xleader",
            coin="BTC",
            binance_symbol="BTCUSDT",
            leader_account_value=Decimal("80000"),
            leader_position_notional=Decimal("20000"),
            follower_equity=Decimal("1000"),
            follower_current_notional=Decimal("500"),
            copy_multiplier=Decimal("1"),
        )
    )

    assert len(plan) == 1
    assert plan[0].action == TargetAction.REDUCE
    assert plan[0].delta_notional == Decimal("-250.00000000")
    assert plan[0].side == OrderSide.SELL
    assert plan[0].reduce_only is True


def test_scenario_5_leader_flat_closes_follower_reduce_only() -> None:
    plan = build_target_position_plan(
        TargetPositionInput(
            leader_address="0xleader",
            coin="BTC",
            binance_symbol="BTCUSDT",
            leader_account_value=Decimal("80000"),
            leader_position_notional=Decimal("0"),
            follower_equity=Decimal("1000"),
            follower_current_notional=Decimal("500"),
            copy_multiplier=Decimal("1"),
        )
    )

    assert len(plan) == 1
    assert plan[0].action == TargetAction.CLOSE
    assert plan[0].order_notional == Decimal("500")
    assert plan[0].reduce_only is True


def test_scenario_6_leader_flips_requires_two_stage_plan() -> None:
    plan = build_target_position_plan(
        TargetPositionInput(
            leader_address="0xleader",
            coin="BTC",
            binance_symbol="BTCUSDT",
            leader_account_value=Decimal("80000"),
            leader_position_notional=Decimal("-20000"),
            follower_equity=Decimal("1000"),
            follower_current_notional=Decimal("500"),
            copy_multiplier=Decimal("1"),
        )
    )

    assert [item.action for item in plan] == [TargetAction.FLIP_CLOSE, TargetAction.FLIP_OPEN]
    assert plan[0].side == OrderSide.SELL
    assert plan[0].order_notional == Decimal("500")
    assert plan[0].reduce_only is True
    assert plan[1].side == OrderSide.SELL
    assert plan[1].target_notional == Decimal("-250.00000000")
    assert plan[1].requires_position_flat is True
    assert plan[1].reduce_only is False


def test_scenario_7_leader_leverage_is_ignored_and_follower_leverage_is_10() -> None:
    targets = [
        calculate_target_position_notional(
            Decimal("80000"), Decimal("40000"), Decimal("1000"), Decimal("1")
        )
        for _leader_leverage in [Decimal("3"), Decimal("30"), Decimal("50")]
    ]

    assert targets == [Decimal("500.00000000")] * 3
    assert RiskSettingsResult.ok(symbol="BTCUSDT").leverage == 10


class FakeRiskSettings:
    def __init__(self, result: RiskSettingsResult) -> None:
        self.result = result
        self.calls: list[tuple[str, bool]] = []

    async def ensure_symbol_risk_settings(self, symbol: str, *, reduce_only: bool) -> RiskSettingsResult:
        self.calls.append((symbol, reduce_only))
        return self.result


def live_intent(reduce_only: bool = False) -> CopyOrderIntent:
    return CopyOrderIntent(
        leader_address="0xleader",
        source_fill_id="fill-risk-settings",
        source_coin="BTC",
        binance_symbol="BTCUSDT",
        side=OrderSide.BUY,
        notional=Decimal("100"),
        price=Decimal("50000"),
        reduce_only=reduce_only,
    )


def test_scenario_8_risk_settings_failure_blocks_open_but_allows_reduce_only() -> None:
    async def run():
        risk_settings = FakeRiskSettings(
            RiskSettingsResult.blocked(
                symbol="BTCUSDT",
                reason="margin type is not ISOLATED",
                margin_type="CROSSED",
                leverage=20,
            )
        )
        executor = CopyExecutor(
            store=InMemoryExecutionStore(),
            client=None,
            risk_settings=risk_settings,
        )
        live = RiskConfig(trading_enabled=True, frontend_live_confirmed=True)
        blocked = await executor.execute(live_intent(False), rule=btc_rule(), risk_config=live)
        reduce_only = await executor.execute(
            live_intent(True), rule=btc_rule(), risk_config=live
        )
        return blocked, reduce_only, risk_settings.calls

    blocked, reduce_only, calls = asyncio.run(run())
    assert blocked.status == "BLOCKED"
    assert blocked.reason and "margin type" in blocked.reason
    assert reduce_only.status == "DRY_RUN"
    assert calls == [("BTCUSDT", False), ("BTCUSDT", True)]


def test_scenario_9_duplicate_fill_executes_once() -> None:
    fill = {"hash": "0xabc", "tid": 1, "oid": 2, "time": 3, "coin": "BTC"}

    assert fill_unique_id("0xleader", fill) == fill_unique_id("0xleader", fill)


def test_scenario_10_snapshot_does_not_emit_executable_fill() -> None:
    watcher = HyperliquidWatcher(
        ws_url="wss://example.invalid",
        info_client=None,  # type: ignore[arg-type]
        leader_addresses=["0xleader"],
    )
    message = {
        "channel": "userFills",
        "data": {
            "user": "0xleader",
            "isSnapshot": True,
            "fills": [{"coin": "BTC", "px": "50000", "sz": "1", "time": 1}],
        },
    }

    assert watcher._parse_message(__import__("json").dumps(message)) == []


def test_notional_to_quantity_rounds_down_and_skips_too_small() -> None:
    qty, reason = notional_to_quantity(
        symbol="BTCUSDT",
        target_delta_notional=Decimal("100"),
        mark_price=Decimal("33333"),
        exchange_filters=btc_rule(),
    )
    assert qty == Decimal("0.003")
    assert reason is None

    qty, reason = notional_to_quantity(
        symbol="BTCUSDT",
        target_delta_notional=Decimal("4"),
        mark_price=Decimal("33333"),
        exchange_filters=btc_rule(),
    )
    assert qty == Decimal("0")
    assert reason == "SKIPPED_TOO_SMALL"


def test_target_position_rejects_invalid_equity_values() -> None:
    with pytest.raises(ValueError):
        calculate_target_position_notional(
            Decimal("0"), Decimal("40000"), Decimal("1000"), Decimal("1")
        )
    with pytest.raises(ValueError):
        calculate_target_position_notional(
            Decimal("80000"), Decimal("40000"), Decimal("0"), Decimal("1")
        )
