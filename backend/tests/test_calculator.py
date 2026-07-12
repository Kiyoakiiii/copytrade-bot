from decimal import Decimal

import pytest

from app.services.calculator import (
    CopyAction,
    OrderSide,
    UnsupportedSizingMode,
    calculate_copy_notional,
    calculate_leader_position_ratio,
    calculate_target_notional_by_account_ratio,
    position_delta_orders,
    scale_position_orders,
)


def test_ratio_formula_uses_account_value_and_multiplier() -> None:
    result = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("80000"),
        leader_position_notional=Decimal("40000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("0.1"),
    )

    assert result == Decimal("50.00000000")


def test_account_ratio_original_requirement_example() -> None:
    assert calculate_leader_position_ratio(
        leader_account_value=Decimal("80000"),
        leader_position_notional=Decimal("40000"),
    ) == Decimal("0.50000000")
    assert calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("80000"),
        leader_position_notional=Decimal("40000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    ) == Decimal("500.00000000")


def test_account_ratio_multiplier_point_one() -> None:
    assert calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("80000"),
        leader_position_notional=Decimal("40000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("0.1"),
    ) == Decimal("50.00000000")


def test_big_leader_small_follower_does_not_copy_raw_notional() -> None:
    target = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000000"),
        leader_position_notional=Decimal("100000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )
    assert target == Decimal("100.00000000")
    assert target != Decimal("100000")


def test_big_leader_small_follower_multiplier_point_one() -> None:
    target = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000000"),
        leader_position_notional=Decimal("100000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("0.1"),
    )
    assert target == Decimal("10.00000000")
    assert target != Decimal("10000")


def test_leader_half_position_multiplier_point_one() -> None:
    assert calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000000"),
        leader_position_notional=Decimal("500000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("0.1"),
    ) == Decimal("50.00000000")


def test_leader_short_keeps_abs_target_and_short_side_is_external() -> None:
    target = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000000"),
        leader_position_notional=Decimal("-100000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )
    assert target == Decimal("100.00000000")


def test_leverage_does_not_affect_account_ratio_formula() -> None:
    targets = {
        leverage: calculate_target_notional_by_account_ratio(
            leader_account_value=Decimal("1000000"),
            leader_position_notional=Decimal("100000"),
            follower_account_value=Decimal("1000"),
            copy_multiplier=Decimal("1"),
        )
        for leverage in (3, 10, 20)
    }
    assert set(targets.values()) == {Decimal("100.00000000")}


def test_follower_account_value_changes_target() -> None:
    first = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000000"),
        leader_position_notional=Decimal("100000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )
    second = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000000"),
        leader_position_notional=Decimal("100000"),
        follower_account_value=Decimal("2000"),
        copy_multiplier=Decimal("1"),
    )
    assert first == Decimal("100.00000000")
    assert second == Decimal("200.00000000")


def test_max_notional_not_required_for_sizing_formula() -> None:
    target = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000000"),
        leader_position_notional=Decimal("100000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )
    assert target == Decimal("100.00000000")


def test_allocation_delta_is_target_minus_current() -> None:
    assert Decimal("100") - Decimal("40") == Decimal("60")
    assert Decimal("100") - Decimal("150") == Decimal("-50")


def test_fill_delta_scaling_mode_is_disabled() -> None:
    with pytest.raises(UnsupportedSizingMode, match="ACCOUNT_RATIO"):
        scale_position_orders([], Decimal("1"), Decimal("1"), Decimal("1"))


def test_legacy_leader_notional_delta_wrapper_is_forbidden() -> None:
    with pytest.raises(UnsupportedSizingMode, match="ACCOUNT_RATIO"):
        calculate_copy_notional(
            leader_notional_delta=Decimal("100000"),
            leader_account_value=Decimal("1000000"),
            follower_equity=Decimal("1000"),
            copy_multiplier=Decimal("0.1"),
        )


def test_position_delta_open_add_reduce_close_flip() -> None:
    assert position_delta_orders(0, 2, 100)[0].action == CopyAction.OPEN
    assert position_delta_orders(1, 3, 100)[0].notional == Decimal("200")
    reduce_order = position_delta_orders(3, 1, 100)[0]
    assert reduce_order.action == CopyAction.REDUCE
    assert reduce_order.side == OrderSide.SELL
    assert reduce_order.reduce_only is True

    close_order = position_delta_orders(-2, 0, 100)[0]
    assert close_order.action == CopyAction.CLOSE
    assert close_order.side == OrderSide.BUY

    flip_orders = position_delta_orders(1, -2, 100)
    assert [o.reduce_only for o in flip_orders] == [True, False]
    assert [o.side for o in flip_orders] == [OrderSide.SELL, OrderSide.SELL]
