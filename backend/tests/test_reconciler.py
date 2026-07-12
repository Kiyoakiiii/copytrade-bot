from decimal import Decimal

from app.services.calculator import OrderSide
from app.services.reconciler import ReconcileInput, compute_reconcile_instruction


def test_reconciler_generates_delta_outside_tolerance() -> None:
    instruction = compute_reconcile_instruction(
        ReconcileInput(
            leader_address="0xleader",
            coin="ETH",
            binance_symbol="ETHUSDT",
            leader_position_notional=Decimal("40000"),
            leader_account_value=Decimal("80000"),
            follower_equity=Decimal("1000"),
            follower_current_notional=Decimal("300"),
            copy_multiplier=Decimal("1"),
        )
    )

    assert instruction is not None
    assert instruction.target_notional == Decimal("500.00000000")
    assert instruction.delta_notional == Decimal("200.00000000")
    assert instruction.side == OrderSide.BUY
    assert instruction.reduce_only is False


def test_reconciler_skips_small_diff() -> None:
    instruction = compute_reconcile_instruction(
        ReconcileInput(
            leader_address="0xleader",
            coin="ETH",
            binance_symbol="ETHUSDT",
            leader_position_notional=Decimal("40000"),
            leader_account_value=Decimal("80000"),
            follower_equity=Decimal("1000"),
            follower_current_notional=Decimal("498"),
            copy_multiplier=Decimal("1"),
            tolerance_bps=50,
            min_reconcile_notional=Decimal("1"),
        )
    )

    assert instruction is None

