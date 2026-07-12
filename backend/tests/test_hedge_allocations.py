import asyncio
from decimal import Decimal

import pytest

from app.services.allocations import (
    AllocationStatus,
    AggregatePosition,
    LeaderPositionAllocation,
    apply_close_to_allocation,
    apply_open_to_allocation,
    calculate_leader_target_allocation,
    can_open_new_allocation,
    reconcile_leader_allocation_plan,
    validate_aggregate_allocations_vs_binance,
)
from app.services.hedge_orders import HedgeAction, build_hedge_mode_order
from app.services.risk_settings import BinanceRiskSettingsService
from app.services.target_position import PositionSide


def allocation(
    leader_id: int,
    side: PositionSide,
    notional: str,
    qty: str,
    status: AllocationStatus = AllocationStatus.OPEN,
) -> LeaderPositionAllocation:
    return LeaderPositionAllocation(
        leader_id=leader_id,
        leader_address=f"0xleader{leader_id}",
        hyperliquid_coin="BTC",
        binance_symbol="BTCUSDT",
        position_side=side,
        target_notional=Decimal(notional),
        allocated_notional=Decimal(notional),
        allocated_qty=Decimal(qty),
        avg_entry_price=Decimal("50000"),
        last_leader_account_value=Decimal("80000"),
        last_leader_position_notional=Decimal(notional if side == PositionSide.LONG else f"-{notional}"),
        copy_multiplier=Decimal("1"),
        status=status,
    )


def test_hedge_order_open_long_has_position_side_and_no_reduce_only() -> None:
    order = build_hedge_mode_order(
        symbol="BTCUSDT",
        allocation_side=PositionSide.LONG,
        action=HedgeAction.OPEN_OR_INCREASE,
        quantity=Decimal("0.2"),
        is_close_intent=False,
    )

    assert order.side == "BUY"
    assert order.position_side == "LONG"
    assert "reduceOnly" not in order.params
    assert "closePosition" not in order.params


def test_hedge_order_close_short_has_position_side_and_no_reduce_only() -> None:
    order = build_hedge_mode_order(
        symbol="BTCUSDT",
        allocation_side=PositionSide.SHORT,
        action=HedgeAction.CLOSE_OR_REDUCE,
        quantity=Decimal("0.1"),
        is_close_intent=True,
        leader_allocation_qty=Decimal("0.2"),
        binance_position_qty=Decimal("0.3"),
    )

    assert order.side == "BUY"
    assert order.position_side == "SHORT"
    assert "reduceOnly" not in order.params


def test_hedge_order_rejects_position_side_both() -> None:
    with pytest.raises(ValueError):
        build_hedge_mode_order(
            symbol="BTCUSDT",
            allocation_side=PositionSide.FLAT,
            action=HedgeAction.OPEN_OR_INCREASE,
            quantity=Decimal("0.1"),
            is_close_intent=False,
        )


def test_hedge_close_cannot_exceed_leader_allocation_or_binance_position() -> None:
    with pytest.raises(ValueError):
        build_hedge_mode_order(
            symbol="BTCUSDT",
            allocation_side=PositionSide.LONG,
            action=HedgeAction.CLOSE_OR_REDUCE,
            quantity=Decimal("0.3"),
            is_close_intent=True,
            leader_allocation_qty=Decimal("0.2"),
            binance_position_qty=Decimal("0.5"),
        )
    with pytest.raises(ValueError):
        build_hedge_mode_order(
            symbol="BTCUSDT",
            allocation_side=PositionSide.LONG,
            action=HedgeAction.CLOSE_OR_REDUCE,
            quantity=Decimal("0.3"),
            is_close_intent=True,
            leader_allocation_qty=Decimal("0.5"),
            binance_position_qty=Decimal("0.2"),
        )


def test_multi_leader_same_symbol_same_direction_leader1_close_preserves_leader2() -> None:
    leader1 = allocation(1, PositionSide.LONG, "10000", "0.2")
    leader2 = allocation(2, PositionSide.LONG, "5000", "0.1")

    order, closed = apply_close_to_allocation(
        leader1,
        close_qty=Decimal("0.2"),
        binance_position_qty=Decimal("0.3"),
    )

    assert order.side == "SELL"
    assert order.position_side == "LONG"
    assert order.quantity == Decimal("0.2")
    assert closed.status == AllocationStatus.CLOSED
    assert closed.allocated_qty == Decimal("0")
    assert leader2.allocated_qty == Decimal("0.1")
    assert leader2.allocated_notional == Decimal("5000")


def test_multi_leader_same_symbol_same_direction_leader2_reduce_half_only() -> None:
    leader1 = allocation(1, PositionSide.LONG, "0", "0", AllocationStatus.CLOSED)
    leader2 = allocation(2, PositionSide.LONG, "5000", "0.1")

    order, reduced = reconcile_leader_allocation_plan(
        current=leader2,
        target_notional_abs=Decimal("2500"),
        target_side=PositionSide.LONG,
        mark_price=Decimal("50000"),
        binance_position_qty=Decimal("0.1"),
    )[0]

    assert order.side == "SELL"
    assert order.position_side == "LONG"
    assert order.quantity == Decimal("0.05")
    assert reduced.allocated_notional == Decimal("2500")
    assert reduced.allocated_qty == Decimal("0.05")
    assert leader1.allocated_qty == Decimal("0")


def test_leader_flip_to_short_does_not_touch_other_leader_long() -> None:
    leader1 = allocation(1, PositionSide.LONG, "10000", "0.2")
    leader2 = allocation(2, PositionSide.LONG, "5000", "0.1")

    steps = reconcile_leader_allocation_plan(
        current=leader1,
        target_notional_abs=Decimal("2500"),
        target_side=PositionSide.SHORT,
        mark_price=Decimal("50000"),
        binance_position_qty=Decimal("0.2"),
    )

    assert steps[0][0].side == "SELL"
    assert steps[0][0].position_side == "LONG"
    assert steps[0][1].status == AllocationStatus.CLOSED
    assert steps[1][0].side == "SELL"
    assert steps[1][0].position_side == "SHORT"
    assert steps[1][1].allocated_qty == Decimal("0.05")
    assert leader2.position_side == PositionSide.LONG
    assert leader2.allocated_qty == Decimal("0.1")


def test_opposite_leaders_can_exist_as_long_and_short_allocations() -> None:
    short_alloc = apply_open_to_allocation(
        allocation(1, PositionSide.SHORT, "0", "0", AllocationStatus.CLOSED),
        target_notional_abs=Decimal("2500"),
        side=PositionSide.SHORT,
        mark_price=Decimal("50000"),
    )[1]
    long_alloc = allocation(2, PositionSide.LONG, "5000", "0.1")

    assert short_alloc.position_side == PositionSide.SHORT
    assert short_alloc.allocated_qty == Decimal("0.05")
    assert long_alloc.position_side == PositionSide.LONG
    assert long_alloc.allocated_qty == Decimal("0.1")


def test_aggregate_allocation_matches_binance_long_and_short() -> None:
    result = validate_aggregate_allocations_vs_binance(
        [
            allocation(1, PositionSide.LONG, "10000", "0.2"),
            allocation(2, PositionSide.LONG, "5000", "0.1"),
            allocation(3, PositionSide.SHORT, "2500", "0.05"),
        ],
        AggregatePosition(symbol="BTCUSDT", long_qty=Decimal("0.3"), short_qty=Decimal("0.05")),
        tolerance=Decimal("0.0001"),
    )

    assert result.ok is True


def test_aggregate_allocation_mismatch_blocks_new_opens() -> None:
    result = validate_aggregate_allocations_vs_binance(
        [allocation(1, PositionSide.LONG, "10000", "0.2")],
        AggregatePosition(symbol="BTCUSDT", long_qty=Decimal("0.1"), short_qty=Decimal("0")),
        tolerance=Decimal("0.0001"),
    )

    assert result.ok is False
    assert can_open_new_allocation(result) is False


def test_manual_order_mismatch_warning_model() -> None:
    result = validate_aggregate_allocations_vs_binance(
        [allocation(1, PositionSide.LONG, "10000", "0.2")],
        AggregatePosition(symbol="BTCUSDT", long_qty=Decimal("0.25"), short_qty=Decimal("0")),
        tolerance=Decimal("0.0001"),
        source="MANUAL",
    )

    assert result.ok is False
    assert result.source == "MANUAL"
    assert result.event_type == "ALLOCATION_MISMATCH"


class FakeRiskClient:
    def __init__(
        self,
        *,
        hedge: bool,
        open_orders=None,
        positions=None,
        margin_type="ISOLATED",
        leverage=10,
    ) -> None:
        self.hedge = hedge
        self.open_orders = open_orders or []
        self.positions = positions or []
        self.margin_type = margin_type
        self.leverage = leverage
        self.changed_position_mode = False

    async def position_mode_dual_side(self) -> bool:
        return self.hedge

    async def open_orders_all(self):
        return self.open_orders

    async def position_risk_all(self):
        return self.positions

    async def change_position_mode(self, dual_side_position: bool):
        self.changed_position_mode = dual_side_position
        self.hedge = dual_side_position
        return {"code": 200, "msg": "success"}

    async def change_margin_type(self, symbol: str, margin_type: str):
        return {"code": 200}

    async def change_leverage(self, symbol: str, leverage: int):
        self.leverage = leverage
        return {"leverage": leverage}

    async def position_risk(self, symbol: str):
        return [
            {
                "symbol": symbol,
                "positionSide": "LONG",
                "marginType": self.margin_type,
                "leverage": str(self.leverage),
                "notional": "0",
                "positionAmt": "0",
            },
            {
                "symbol": symbol,
                "positionSide": "SHORT",
                "marginType": self.margin_type,
                "leverage": str(self.leverage),
                "notional": "0",
                "positionAmt": "0",
            },
        ]


def test_position_mode_not_hedge_switches_when_no_orders_or_positions() -> None:
    async def run():
        client = FakeRiskClient(hedge=False)
        result = await BinanceRiskSettingsService(client).ensure_account_position_mode_hedge()
        return result, client.changed_position_mode

    result, changed = asyncio.run(run())
    assert result.is_ok is True
    assert result.position_mode == "HEDGE"
    assert changed is True


def test_position_mode_not_hedge_blocks_when_positions_exist() -> None:
    async def run():
        client = FakeRiskClient(
            hedge=False,
            positions=[{"symbol": "BTCUSDT", "positionAmt": "0.1"}],
        )
        return await BinanceRiskSettingsService(client).ensure_account_position_mode_hedge()

    result = asyncio.run(run())
    assert result.is_ok is False
    assert "open positions" in (result.reason or "")


def test_symbol_risk_still_checks_isolated_and_10x_in_hedge_mode() -> None:
    async def run():
        client = FakeRiskClient(hedge=True, margin_type="CROSSED", leverage=20)
        return await BinanceRiskSettingsService(client).ensure_symbol_risk_settings("BTCUSDT")

    result = asyncio.run(run())
    assert result.is_ok is False
    assert "margin type" in (result.reason or "") or "leverage" in (result.reason or "")


def test_calculate_leader_target_allocation_is_leader_specific() -> None:
    target = calculate_leader_target_allocation(
        leader_id=1,
        leader_address="0xleader1",
        symbol="BTCUSDT",
        coin="BTC",
        leader_account_value=Decimal("80000"),
        leader_position_notional=Decimal("-20000"),
        follower_equity=Decimal("1000"),
        copy_multiplier=Decimal("1"),
        mark_price=Decimal("50000"),
    )

    assert target.leader_id == 1
    assert target.position_side == PositionSide.SHORT
    assert target.target_notional_abs == Decimal("250.00000000")
    assert target.target_qty == Decimal("0.00500000")


def test_close_all_is_not_used_for_leader_close() -> None:
    order = build_hedge_mode_order(
        symbol="BTCUSDT",
        allocation_side=PositionSide.LONG,
        action=HedgeAction.CLOSE_OR_REDUCE,
        quantity=Decimal("0.2"),
        is_close_intent=True,
        leader_allocation_qty=Decimal("0.2"),
        binance_position_qty=Decimal("0.3"),
    )

    assert "closePosition" not in order.params
