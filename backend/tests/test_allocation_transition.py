from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.services.allocations import (
    AllocationScopeError,
    AllocationTransitionAction,
    DUST_LIFECYCLE_ACCOUNT_RATIO_RESET,
    LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK,
    MAX_POSITION_NOTIONAL_CAP_EXCEEDED,
    assert_allocation_scope,
    plan_leader_allocation_transition,
)
from app.services.execution_router import ExecutionRouter, ExecutionVenue, VenueRouteStatus
from app.services.leader_config import is_coin_allowed
from app.services.target_position import PositionSide


def allocation(
    *,
    leader_id: int = 1,
    venue: str = "HYPERLIQUID",
    dex: str = "",
    coin: str = "BTC",
    side: str = "LONG",
    notional: str = "100",
    qty: str = "1",
    status: str = "OPEN",
    last_leader_position_size: str | None = None,
    last_leader_position_notional: str | None = None,
    pending_reduce_qty: str | None = None,
    pending_reduce_notional: str | None = None,
):
    return SimpleNamespace(
        id=leader_id * 10,
        leader_id=leader_id,
        leader_address=f"0x{leader_id:040d}"[-42:],
        execution_venue=venue,
        venue_account="main",
        dex=dex,
        canonical_coin=coin,
        hyperliquid_coin=coin.split(":")[-1],
        position_side=side,
        allocated_notional=Decimal(notional),
        allocated_qty=Decimal(qty),
        avg_entry_price=Decimal("100"),
        last_leader_position_size=Decimal(last_leader_position_size) if last_leader_position_size is not None else None,
        last_leader_position_notional=Decimal(last_leader_position_notional) if last_leader_position_notional is not None else None,
        pending_reduce_qty=Decimal(pending_reduce_qty) if pending_reduce_qty is not None else None,
        pending_reduce_notional=Decimal(pending_reduce_notional) if pending_reduce_notional is not None else None,
        status=status,
    )


def plan(**overrides):
    payload = {
        "leader_id": 1,
        "execution_venue": "HYPERLIQUID",
        "dex": "",
        "canonical_coin": "BTC",
        "leader_side": PositionSide.LONG,
        "leader_position_notional": Decimal("10000"),
        "leader_account_value_used": Decimal("100000"),
        "follower_account_value_used": Decimal("1000"),
        "copy_multiplier": Decimal("1"),
        "current_allocation": None,
    }
    payload.update(overrides)
    return plan_leader_allocation_transition(**payload)


def test_flat_to_long_opens() -> None:
    item = plan()
    assert item.action == AllocationTransitionAction.OPEN
    assert item.target_notional == Decimal("100.00000000")
    assert item.reduce_only is False


def test_open_above_max_position_notional_is_rejected_in_full() -> None:
    item = plan(max_position_notional=Decimal("25"))

    assert item.action == AllocationTransitionAction.BLOCK
    assert item.target_notional == Decimal("0E-8")
    assert item.delta_notional == Decimal("0E-8")
    assert item.formula_inputs["target_notional_before_cap"] == "100.00000000"
    assert item.formula_inputs["max_position_notional_cap"] == "25.00000000"
    assert item.formula_inputs["position_cap_exceeded"] is True
    assert item.reason.startswith(MAX_POSITION_NOTIONAL_CAP_EXCEEDED)


def test_increase_above_max_position_notional_is_rejected_in_full() -> None:
    current = allocation(
        notional="25",
        qty="0.25",
        side="LONG",
        last_leader_position_size="100",
    )

    item = plan(
        current_allocation=current,
        leader_position_notional=Decimal("50000"),
        leader_position_size=Decimal("110"),
        max_position_notional=Decimal("25"),
    )

    assert item.action == AllocationTransitionAction.BLOCK
    assert item.target_notional == Decimal("25.00000000")
    assert item.delta_notional == Decimal("0E-8")
    assert item.formula_inputs["attempted_target_notional"] == "27.50000000"
    assert item.reason.startswith(MAX_POSITION_NOTIONAL_CAP_EXCEEDED)


def test_lowering_max_position_notional_does_not_force_an_unprompted_reduce() -> None:
    current = allocation(notional="80", qty="0.8", side="LONG", last_leader_position_size="100")

    item = plan(
        current_allocation=current,
        leader_position_notional=Decimal("10000"),
        leader_position_size=Decimal("100"),
        max_position_notional=Decimal("50"),
    )

    assert item.action == AllocationTransitionAction.NOOP
    assert item.target_notional == Decimal("80.00000000")
    assert item.delta_notional == Decimal("0E-8")
    assert item.close_qty_limit == Decimal("0E-8")


def test_same_side_reduce_still_follows_percentage_under_max_position_cap() -> None:
    current = allocation(notional="50", qty="0.5", side="LONG", last_leader_position_size="100")

    item = plan(
        current_allocation=current,
        leader_position_size=Decimal("60"),
        leader_position_notional=Decimal("6000"),
        max_position_notional=Decimal("50"),
    )

    assert item.action == AllocationTransitionAction.REDUCE
    assert item.target_notional == Decimal("30.00000000")
    assert item.close_qty_limit == Decimal("0.20000000")
    assert item.formula_inputs["reduce_ratio_source"] == "leader_position_size"


def test_cap_allows_reincrease_after_leader_reduces_below_cap() -> None:
    current = allocation(notional="16", qty="0.16", side="LONG", last_leader_position_size="80")

    item = plan(
        current_allocation=current,
        leader_position_size=Decimal("100"),
        leader_position_notional=Decimal("50000"),
        max_position_notional=Decimal("20"),
    )

    assert item.action == AllocationTransitionAction.INCREASE
    assert item.target_notional == Decimal("20.00000000")
    assert item.delta_notional == Decimal("4.00000000")
    assert item.reduce_only is False


def test_position_cap_allows_open_exactly_at_limit() -> None:
    item = plan(max_position_notional=Decimal("100"))

    assert item.action == AllocationTransitionAction.OPEN
    assert item.target_notional == Decimal("100.00000000")


def test_each_later_increase_is_rejected_again_when_projected_position_still_exceeds_cap() -> None:
    current = allocation(
        notional="19",
        qty="0.19",
        side="LONG",
        last_leader_position_size="110",
    )

    item = plan(
        current_allocation=current,
        leader_position_notional=Decimal("12000"),
        leader_position_size=Decimal("120"),
        leader_previous_position_size=Decimal("110"),
        max_position_notional=Decimal("20"),
    )

    assert item.action == AllocationTransitionAction.BLOCK
    assert item.target_notional == Decimal("19.00000000")
    assert item.delta_notional == Decimal("0E-8")
    assert Decimal(item.formula_inputs["attempted_target_notional"]) > Decimal("20")


def test_reduce_from_above_cap_follows_leader_percentage_and_is_not_forced_to_cap() -> None:
    current = allocation(
        notional="30",
        qty="0.30",
        side="LONG",
        last_leader_position_size="100",
    )

    item = plan(
        current_allocation=current,
        leader_position_notional=Decimal("9000"),
        leader_position_size=Decimal("90"),
        leader_previous_position_size=Decimal("100"),
        leader_fill_is_reduce_or_close=True,
        max_position_notional=Decimal("20"),
    )

    assert item.action == AllocationTransitionAction.REDUCE
    assert item.target_notional == Decimal("27.00000000")
    assert item.delta_notional == Decimal("-3.00000000")
    assert item.close_qty_limit == Decimal("0.03000000")


def test_close_from_above_cap_is_always_allowed() -> None:
    current = allocation(notional="30", qty="0.30", side="LONG")

    item = plan(
        current_allocation=current,
        leader_side=PositionSide.FLAT,
        leader_position_notional=Decimal("0"),
        leader_position_size=Decimal("0"),
        leader_fill_is_reduce_or_close=True,
        max_position_notional=Decimal("20"),
    )

    assert item.action == AllocationTransitionAction.CLOSE
    assert item.target_notional == Decimal("0E-8")
    assert item.delta_notional == Decimal("-30.00000000")
    assert item.reduce_only is True


def test_long_increase_reduce_close_and_noop() -> None:
    current = allocation(notional="100", qty="1", side="LONG", last_leader_position_size="100")
    assert plan(
        current_allocation=current,
        leader_position_notional=Decimal("15000"),
        leader_position_size=Decimal("150"),
    ).action == AllocationTransitionAction.INCREASE
    reduce = plan(current_allocation=current, leader_position_notional=Decimal("5000"), leader_position_size=Decimal("50"))
    assert reduce.action == AllocationTransitionAction.REDUCE
    assert reduce.reduce_only is True
    assert reduce.close_qty_limit == Decimal("0.50000000")
    assert plan(current_allocation=current, leader_position_notional=Decimal("0"), leader_side=PositionSide.FLAT).action == AllocationTransitionAction.CLOSE
    assert plan(
        current_allocation=current,
        leader_position_notional=Decimal("10000"),
        leader_position_size=Decimal("100"),
    ).action == AllocationTransitionAction.NOOP


def test_same_side_increase_uses_leader_position_size_ratio_not_account_value_drift() -> None:
    current = allocation(notional="100", qty="1", side="LONG", last_leader_position_size="100")
    item = plan(
        current_allocation=current,
        leader_position_size=Decimal("115"),
        leader_position_notional=Decimal("11500"),
        leader_fill_notional=Decimal("500"),
        leader_account_value_used=Decimal("1000000"),
        follower_account_value_used=Decimal("1000"),
    )

    assert item.action == AllocationTransitionAction.INCREASE
    assert item.target_notional == Decimal("115.00000000")
    assert item.delta_notional == Decimal("15.00000000")
    assert item.formula_inputs["increase_delta_source"] == "leader_position_size"
    assert item.formula_inputs["leader_position_increase_ratio"].startswith("0.15")
    assert item.open_qty == Decimal("0.15000000")


def test_same_side_increase_open_qty_uses_current_allocation_qty_not_notional_drift() -> None:
    current = allocation(notional="50", qty="1", side="LONG", last_leader_position_size="100")
    item = plan(
        current_allocation=current,
        leader_position_size=Decimal("110"),
        leader_position_notional=Decimal("11000"),
    )

    assert item.action == AllocationTransitionAction.INCREASE
    assert item.delta_notional == Decimal("5.00000000")
    assert item.open_qty == Decimal("0.10000000")


def test_same_side_dust_lifecycle_large_open_uses_fresh_account_ratio() -> None:
    current = allocation(
        notional="0.002808",
        qty="12",
        side="SHORT",
        last_leader_position_size="37",
        last_leader_position_notional="-0.008510",
    )

    item = plan(
        current_allocation=current,
        leader_side=PositionSide.SHORT,
        leader_position_notional=Decimal("-2489.917275350656309444200381"),
        leader_position_size=Decimal("10775899"),
        leader_previous_position_size=Decimal("37"),
        leader_fill_notional=Decimal("2489.908726"),
        leader_account_value_used=Decimal("60000"),
        follower_account_value_used=Decimal("18582.001983"),
        copy_multiplier=Decimal("1"),
    )

    assert item.action == AllocationTransitionAction.INCREASE
    assert item.target_notional == Decimal("771.12746247")
    assert item.delta_notional == Decimal("771.12465447")
    assert item.open_qty == Decimal("0E-8")
    assert item.reason == "dust residual lifecycle reset; use fresh account-ratio sizing"
    assert DUST_LIFECYCLE_ACCOUNT_RATIO_RESET in item.formula_inputs["warnings"]
    assert "leader_position_increase_ratio" not in item.formula_inputs


def test_same_side_increase_does_not_require_account_value_after_lifecycle_opened() -> None:
    current = allocation(notional="100", qty="1", side="LONG", last_leader_position_size="100")
    item = plan(
        current_allocation=current,
        leader_position_size=Decimal("125"),
        leader_position_notional=Decimal("12500"),
        leader_account_value_used=None,
        follower_account_value_used=None,
    )

    assert item.action == AllocationTransitionAction.INCREASE
    assert item.target_notional == Decimal("125.00000000")
    assert item.delta_notional == Decimal("25.00000000")


def test_same_side_increase_legacy_notional_ratio_fallback_requires_warning() -> None:
    current = allocation(notional="100", qty="1", side="LONG", last_leader_position_notional="10000")
    item = plan(
        current_allocation=current,
        leader_position_size=None,
        leader_position_notional=Decimal("12000"),
        leader_account_value_used=None,
        follower_account_value_used=None,
    )

    assert item.action == AllocationTransitionAction.INCREASE
    assert item.target_notional == Decimal("120.00000000")
    assert item.delta_notional == Decimal("20.00000000")
    assert item.formula_inputs["increase_delta_source"] == "leader_position_notional"
    assert LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK in item.formula_inputs["warnings"]


def test_topped_up_allocation_continues_same_lifecycle() -> None:
    current = allocation(
        notional="916.5058",
        qty="0.4",
        side="SHORT",
        last_leader_position_size="200",
        last_leader_position_notional="-457937.42277",
    )

    add = plan(
        current_allocation=current,
        leader_side=PositionSide.SHORT,
        leader_position_size=Decimal("210"),
        leader_position_notional=Decimal("-480834.2939085"),
        leader_fill_notional=Decimal("22896.8711385"),
        leader_account_value_used=Decimal("1410137.244085"),
        follower_account_value_used=Decimal("806.585996"),
        copy_multiplier=Decimal("3.5"),
    )
    reduce = plan(
        current_allocation=current,
        leader_side=PositionSide.SHORT,
        leader_position_size=Decimal("100"),
        leader_position_notional=Decimal("-228968.711385"),
    )
    close = plan(
        current_allocation=current,
        leader_side=PositionSide.FLAT,
        leader_position_size=Decimal("0"),
        leader_position_notional=Decimal("0"),
    )

    assert add.action == AllocationTransitionAction.INCREASE
    assert add.target_notional == Decimal("962.33109000")
    assert add.delta_notional == Decimal("45.82529000")
    assert add.formula_inputs["increase_delta_source"] == "leader_position_size"
    assert reduce.action == AllocationTransitionAction.REDUCE
    assert reduce.target_notional == Decimal("458.25290000")
    assert reduce.close_qty_limit == Decimal("0.20000000")
    assert reduce.formula_inputs["reduce_ratio_source"] == "leader_position_size"
    assert close.action == AllocationTransitionAction.CLOSE
    assert close.close_qty_limit == Decimal("0.40000000")


def test_below_min_same_side_add_is_not_accumulated_into_later_add() -> None:
    after_skipped_small_add = allocation(
        notional="100",
        qty="1",
        side="LONG",
        status="PENDING_OPEN",
        last_leader_position_size="105",
    )
    next_add = plan(
        current_allocation=after_skipped_small_add,
        leader_position_size=Decimal("120"),
        leader_position_notional=Decimal("12000"),
        leader_fill_notional=Decimal("1100"),
    )

    assert next_add.action == AllocationTransitionAction.INCREASE
    assert next_add.target_notional == Decimal("114.28571429")
    assert next_add.delta_notional == Decimal("14.28571429")
    assert "not accumulated" in next_add.formula_inputs["increase_formula"]


def test_same_side_reduce_follows_leader_size_percentage_not_account_ratio_drift() -> None:
    current = allocation(notional="100", qty="1", side="LONG", last_leader_position_size="100")
    reduce = plan(
        current_allocation=current,
        leader_position_size=Decimal("60"),
        leader_position_notional=Decimal("6000"),
        leader_account_value_used=Decimal("30000"),
    )

    assert reduce.action == AllocationTransitionAction.REDUCE
    assert reduce.target_notional == Decimal("60.00000000")
    assert reduce.delta_notional == Decimal("-40.00000000")
    assert reduce.close_qty_limit == Decimal("0.40000000")
    assert reduce.formula_inputs["reduce_ratio_source"] == "leader_position_size"
    assert reduce.formula_inputs["follower_reduce_ratio"].startswith("0.4")


def test_reduce_fill_uses_fill_start_position_not_account_ratio_catchup() -> None:
    current = allocation(notional="100", qty="1", side="LONG", last_leader_position_size="150")
    reduce = plan(
        current_allocation=current,
        leader_position_notional=Decimal("150000"),
        leader_position_size=Decimal("150"),
        leader_account_value_used=Decimal("100000"),
        leader_previous_position_size=Decimal("200"),
        leader_fill_is_reduce_or_close=True,
    )

    assert reduce.action == AllocationTransitionAction.REDUCE
    assert reduce.target_notional == Decimal("75.00000000")
    assert reduce.close_qty_limit == Decimal("0.25000000")
    assert reduce.formula_inputs["reduce_ratio_source"] == "leader_fill_start_position_size"


def test_reduce_fill_never_creates_account_ratio_catchup_increase() -> None:
    current = allocation(notional="100", qty="1", side="LONG", last_leader_position_size="150")
    item = plan(
        current_allocation=current,
        leader_position_notional=Decimal("150000"),
        leader_position_size=Decimal("150"),
        leader_account_value_used=Decimal("100000"),
        leader_fill_is_reduce_or_close=True,
    )

    assert item.action == AllocationTransitionAction.NOOP
    assert item.target_notional == Decimal("100.00000000")
    assert item.delta_notional == Decimal("0E-8")
    assert "cannot create account-ratio catch-up increase" in item.reason


def test_same_side_reduce_falls_back_to_leader_notional_percentage_for_old_allocations() -> None:
    current = allocation(notional="100", qty="1", side="LONG", last_leader_position_notional="10000")
    reduce = plan(
        current_allocation=current,
        leader_position_notional=Decimal("6000"),
        leader_account_value_used=Decimal("30000"),
    )

    assert reduce.action == AllocationTransitionAction.REDUCE
    assert reduce.target_notional == Decimal("60.00000000")
    assert reduce.close_qty_limit == Decimal("0.40000000")
    assert reduce.formula_inputs["reduce_ratio_source"] == "leader_position_notional"
    assert LEGACY_SIZE_MISSING_NOTIONAL_RATIO_FALLBACK in reduce.formula_inputs["warnings"]


def test_deferred_reduce_qty_is_carried_into_next_same_side_reduce() -> None:
    current = allocation(
        notional="100",
        qty="1",
        side="LONG",
        last_leader_position_size="95",
        pending_reduce_qty="0.05",
        pending_reduce_notional="5",
    )
    reduce = plan(
        current_allocation=current,
        leader_position_size=Decimal("90"),
        leader_position_notional=Decimal("9000"),
    )

    assert reduce.action == AllocationTransitionAction.REDUCE
    assert reduce.target_notional == Decimal("90.00000000")
    assert reduce.close_qty_limit == Decimal("0.10000000")
    assert reduce.formula_inputs["pending_reduce_qty_before_plan"] == "0.05"


def test_pending_reduce_offsets_later_same_side_add_before_increasing() -> None:
    current = allocation(
        notional="100",
        qty="1",
        side="LONG",
        last_leader_position_size="100",
        pending_reduce_qty="0.05",
        pending_reduce_notional="5",
    )

    item = plan(
        current_allocation=current,
        leader_position_size=Decimal("103"),
        leader_position_notional=Decimal("10300"),
        leader_fill_notional=Decimal("300"),
    )

    assert item.action == AllocationTransitionAction.NOOP
    assert item.target_notional == Decimal("100.00000000")
    assert item.delta_notional == Decimal("0E-8")
    assert item.reason == "leader add offsets pending reduce; no follower increase yet"
    assert item.formula_inputs["pending_reduce_offset_notional"] == "3.00000000"
    assert item.formula_inputs["pending_reduce_remaining_notional"] == "2.00000000"
    assert item.formula_inputs["pending_reduce_remaining_qty"] == "0.02000000"


def test_pending_reduce_is_subtracted_from_later_same_side_add() -> None:
    current = allocation(
        notional="100",
        qty="1",
        side="LONG",
        last_leader_position_size="100",
        pending_reduce_qty="0.05",
        pending_reduce_notional="5",
    )

    item = plan(
        current_allocation=current,
        leader_position_size=Decimal("112"),
        leader_position_notional=Decimal("11200"),
        leader_fill_notional=Decimal("1200"),
    )

    assert item.action == AllocationTransitionAction.INCREASE
    assert item.target_notional == Decimal("107.00000000")
    assert item.delta_notional == Decimal("7.00000000")
    assert item.formula_inputs["fill_delta_target_notional"] == "7.00000000"
    assert item.formula_inputs["pending_reduce_offset_notional"] == "5.00000000"
    assert item.formula_inputs["pending_reduce_remaining_notional"] == "0E-8"
    assert item.formula_inputs["pending_reduce_remaining_qty"] == "0E-8"


def test_many_below_min_active_adds_advance_checkpoint_without_accumulating_before_large_add() -> None:
    price = Decimal("100")
    leader_size = Decimal("500")
    current = allocation(
        notional="2000",
        qty="20",
        side="LONG",
        last_leader_position_size=str(leader_size),
        last_leader_position_notional=str(leader_size * price),
    )

    for _ in range(300):
        previous_size = leader_size
        leader_size += Decimal("0.11")
        item = plan(
            current_allocation=current,
            leader_position_size=leader_size,
            leader_position_notional=leader_size * price,
            leader_previous_position_size=previous_size,
            leader_fill_notional=Decimal("11"),
        )

        assert item.action == AllocationTransitionAction.INCREASE
        assert item.delta_notional < Decimal("10")
        current.last_leader_position_size = leader_size
        current.last_leader_position_notional = leader_size * price

    previous_size = leader_size
    leader_size += Decimal("50")
    item = plan(
        current_allocation=current,
        leader_position_size=leader_size,
        leader_position_notional=leader_size * price,
        leader_previous_position_size=previous_size,
        leader_fill_notional=Decimal("5000"),
    )

    assert current.allocated_notional == Decimal("2000")
    assert item.action == AllocationTransitionAction.INCREASE
    assert item.delta_notional == Decimal("187.61726079")
    assert item.target_notional == Decimal("2187.61726079")
    assert item.formula_inputs["increase_delta_source"] == "leader_fill_start_position_size"


def test_leader_full_close_closes_entire_leader_allocation_qty() -> None:
    current = allocation(notional="100", qty="1.25", side="LONG", last_leader_position_size="100")
    close = plan(
        current_allocation=current,
        leader_position_notional=Decimal("0"),
        leader_position_size=Decimal("0"),
        leader_side=PositionSide.FLAT,
    )

    assert close.action == AllocationTransitionAction.CLOSE
    assert close.target_notional == Decimal("0E-8")
    assert close.close_qty_limit == Decimal("1.25000000")


def test_long_to_short_closes_first_then_opens_second() -> None:
    current = allocation(notional="100", qty="1", side="LONG")
    first = plan(current_allocation=current, leader_position_notional=Decimal("-8000"), leader_side=PositionSide.SHORT)
    assert first.action == AllocationTransitionAction.FLIP_CLOSE_FIRST
    assert first.old_side == PositionSide.LONG
    assert first.new_side == PositionSide.SHORT
    assert first.close_qty_limit == Decimal("1.00000000")
    closed = allocation(notional="0", qty="0", side="LONG", status="CLOSED")
    second = plan(current_allocation=closed, leader_position_notional=Decimal("-8000"), leader_side=PositionSide.SHORT)
    assert second.action == AllocationTransitionAction.FLIP_OPEN_SECOND


def test_full_copied_lifecycle_open_add_reduce_close_and_reopen() -> None:
    opened = plan(
        current_allocation=None,
        leader_position_notional=Decimal("10000"),
        leader_position_size=Decimal("100"),
        leader_account_value_used=Decimal("100000"),
        follower_account_value_used=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )
    assert opened.action == AllocationTransitionAction.OPEN
    assert opened.target_notional == Decimal("100.00000000")

    current = allocation(notional="100", qty="1", side="LONG", last_leader_position_size="100")
    increased = plan(
        current_allocation=current,
        leader_position_notional=Decimal("15000"),
        leader_position_size=Decimal("150"),
        leader_account_value_used=Decimal("100000"),
        follower_account_value_used=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )
    assert increased.action == AllocationTransitionAction.INCREASE
    assert increased.delta_notional == Decimal("50.00000000")

    current = allocation(notional="150", qty="1.5", side="LONG", last_leader_position_size="150")
    reduced = plan(
        current_allocation=current,
        leader_position_notional=Decimal("9000"),
        leader_position_size=Decimal("90"),
        leader_account_value_used=Decimal("100000"),
        follower_account_value_used=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )
    assert reduced.action == AllocationTransitionAction.REDUCE
    assert reduced.close_qty_limit == Decimal("0.60000000")
    assert reduced.formula_inputs["reduce_ratio_source"] == "leader_position_size"

    current = allocation(notional="90", qty="0.9", side="LONG", last_leader_position_size="90")
    closed = plan(
        current_allocation=current,
        leader_position_notional=Decimal("0"),
        leader_position_size=Decimal("0"),
        leader_side=PositionSide.FLAT,
    )
    assert closed.action == AllocationTransitionAction.CLOSE
    assert closed.close_qty_limit == Decimal("0.90000000")

    reopened = plan(
        current_allocation=None,
        leader_position_notional=Decimal("-8000"),
        leader_position_size=Decimal("80"),
        leader_side=PositionSide.SHORT,
    )
    assert reopened.action == AllocationTransitionAction.OPEN
    assert reopened.new_side == PositionSide.SHORT


def test_pending_open_allocation_opens_when_lifecycle_target_becomes_large_enough() -> None:
    pending = allocation(notional="0", qty="0", side="SHORT", status="PENDING_OPEN", last_leader_position_size="9.95")
    item = plan(
        current_allocation=pending,
        leader_side=PositionSide.SHORT,
        leader_position_notional=Decimal("-4200"),
        leader_position_size=Decimal("140.27"),
        leader_account_value_used=Decimal("100000"),
        follower_account_value_used=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )

    assert item.action == AllocationTransitionAction.OPEN
    assert item.reduce_only is False
    assert item.new_side == PositionSide.SHORT


def test_pending_open_can_wait_through_many_below_min_adds_then_open_when_target_is_large_enough() -> None:
    price = Decimal("100")
    leader_size = Decimal("33")
    pending = allocation(
        notional="0",
        qty="0",
        side="LONG",
        status="PENDING_OPEN",
        last_leader_position_size=str(leader_size),
        last_leader_position_notional=str(leader_size * price),
    )

    below_min = plan(
        current_allocation=pending,
        leader_position_size=Decimal("80"),
        leader_position_notional=Decimal("8000"),
        leader_account_value_used=Decimal("1000000"),
        follower_account_value_used=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )
    assert below_min.action == AllocationTransitionAction.OPEN
    assert below_min.target_notional == Decimal("8.00000000")

    pending.last_leader_position_size = Decimal("80")
    pending.last_leader_position_notional = Decimal("8000")
    executable = plan(
        current_allocation=pending,
        leader_position_size=Decimal("233"),
        leader_position_notional=Decimal("23300"),
        leader_account_value_used=Decimal("1000000"),
        follower_account_value_used=Decimal("1000"),
        copy_multiplier=Decimal("1"),
    )

    assert executable.action == AllocationTransitionAction.OPEN
    assert executable.target_notional == Decimal("23.30000000")
    assert executable.delta_notional == Decimal("23.30000000")


def test_short_increase_reduce_close_and_short_to_long() -> None:
    current = allocation(notional="100", qty="1", side="SHORT", last_leader_position_size="100")
    assert plan(
        current_allocation=current,
        leader_side=PositionSide.SHORT,
        leader_position_notional=Decimal("-15000"),
        leader_position_size=Decimal("150"),
    ).action == AllocationTransitionAction.INCREASE
    assert plan(
        current_allocation=current,
        leader_side=PositionSide.SHORT,
        leader_position_notional=Decimal("-5000"),
        leader_position_size=Decimal("50"),
    ).action == AllocationTransitionAction.REDUCE
    assert plan(current_allocation=current, leader_side=PositionSide.FLAT, leader_position_notional=Decimal("0")).action == AllocationTransitionAction.CLOSE
    assert plan(current_allocation=current, leader_side=PositionSide.LONG, leader_position_notional=Decimal("8000")).action == AllocationTransitionAction.FLIP_CLOSE_FIRST


def test_scope_guard_prevents_cross_leader_venue_dex_coin_side_close() -> None:
    leader1 = allocation(leader_id=1, dex="xyz", coin="xyz:HYUNDAI", side="LONG")
    assert_allocation_scope(
        {
            "action": "CLOSE",
            "leader_id": 1,
            "execution_venue": "HYPERLIQUID",
            "dex": "xyz",
            "canonical_coin": "xyz:HYUNDAI",
            "old_side": "LONG",
            "quantity": Decimal("1"),
        },
        leader1,
        aggregate_follower_qty=Decimal("2"),
        allocation_sum_qty=Decimal("1"),
    )
    with pytest.raises(AllocationScopeError, match="leader_id"):
        assert_allocation_scope({"action": "CLOSE", "leader_id": 2, "execution_venue": "HYPERLIQUID", "dex": "xyz", "canonical_coin": "xyz:HYUNDAI", "old_side": "LONG"}, leader1)
    with pytest.raises(AllocationScopeError, match="dex"):
        assert_allocation_scope({"action": "CLOSE", "leader_id": 1, "execution_venue": "HYPERLIQUID", "dex": "", "canonical_coin": "xyz:HYUNDAI", "old_side": "LONG"}, leader1)
    with pytest.raises(AllocationScopeError, match="canonical_coin"):
        assert_allocation_scope({"action": "CLOSE", "leader_id": 1, "execution_venue": "HYPERLIQUID", "dex": "xyz", "canonical_coin": "xyz:URNM", "old_side": "LONG"}, leader1)


def test_multi_leader_same_side_close_and_reduce_are_isolated() -> None:
    leader1 = allocation(leader_id=1, notional="100", qty="1", last_leader_position_size="100")
    leader2 = allocation(leader_id=2, notional="100", qty="1", last_leader_position_size="200")
    close_1 = plan(current_allocation=leader1, leader_position_notional=Decimal("0"), leader_side=PositionSide.FLAT)
    assert close_1.action == AllocationTransitionAction.CLOSE
    assert close_1.close_qty_limit == Decimal("1.00000000")
    assert leader2.allocated_notional == Decimal("100")
    reduce_2 = plan(
        leader_id=2,
        current_allocation=leader2,
        leader_position_notional=Decimal("5000"),
        leader_position_size=Decimal("100"),
    )
    assert reduce_2.action == AllocationTransitionAction.REDUCE
    assert reduce_2.close_qty_limit == Decimal("0.50000000")
    assert leader1.allocated_notional == Decimal("100")
    reopen_1 = plan(current_allocation=None, leader_position_notional=Decimal("8000"))
    assert reopen_1.action == AllocationTransitionAction.OPEN
    assert reopen_1.target_notional == Decimal("80.00000000")


def test_scope_guard_caps_reduce_quantity_to_allocation_and_actual_follower_qty() -> None:
    current = allocation(notional="100", qty="1", side="LONG")
    with pytest.raises(AllocationScopeError, match="leader allocation qty"):
        assert_allocation_scope(
            {
                "action": "REDUCE",
                "leader_id": 1,
                "execution_venue": "HYPERLIQUID",
                "dex": "",
                "canonical_coin": "BTC",
                "old_side": "LONG",
                "quantity": Decimal("1.1"),
            },
            current,
            aggregate_follower_qty=Decimal("2"),
            allocation_sum_qty=Decimal("1"),
        )
    with pytest.raises(AllocationScopeError, match="aggregate follower position qty"):
        assert_allocation_scope(
            {
                "action": "REDUCE",
                "leader_id": 1,
                "execution_venue": "HYPERLIQUID",
                "dex": "",
                "canonical_coin": "BTC",
                "old_side": "LONG",
                "quantity": Decimal("0.8"),
            },
            current,
            aggregate_follower_qty=Decimal("0.7"),
            allocation_sum_qty=Decimal("1"),
        )


def test_hyperliquid_netting_blocks_opposite_aggregate_but_binance_hedge_can_plan() -> None:
    router = ExecutionRouter()
    result = router.validate_hyperliquid_netting_constraint(
        [
            allocation(leader_id=1, side="LONG", venue="HYPERLIQUID"),
            allocation(leader_id=2, side="SHORT", venue="HYPERLIQUID"),
        ],
        venue_account="main",
        coin="BTC",
    )
    assert result.status == VenueRouteStatus.BLOCKED
    binance_plan = plan(execution_venue="BINANCE", current_allocation=None, leader_side=PositionSide.SHORT, leader_position_notional=Decimal("-8000"))
    assert binance_plan.action == AllocationTransitionAction.OPEN


def test_venue_and_default_vs_xyz_are_isolated_by_scope() -> None:
    hyperliquid = allocation(leader_id=1, venue="HYPERLIQUID", dex="xyz", coin="xyz:BTC")
    with pytest.raises(AllocationScopeError, match="execution_venue"):
        assert_allocation_scope({"action": "CLOSE", "leader_id": 1, "execution_venue": "BINANCE", "dex": "xyz", "canonical_coin": "xyz:BTC", "old_side": "LONG"}, hyperliquid)
    default = allocation(leader_id=1, dex="", coin="BTC")
    with pytest.raises(AllocationScopeError, match="dex"):
        assert_allocation_scope({"action": "CLOSE", "leader_id": 1, "execution_venue": "HYPERLIQUID", "dex": "xyz", "canonical_coin": "BTC", "old_side": "LONG"}, default)


def test_disabled_or_deleted_leader_not_coin_allowed_for_new_open() -> None:
    disabled = SimpleNamespace(enabled=False, deleted_at=None, allowed_symbols=None, blocked_symbols=[])
    deleted = SimpleNamespace(enabled=True, deleted_at=object(), allowed_symbols=None, blocked_symbols=[])
    assert is_coin_allowed(disabled, "BTC") is False
    assert is_coin_allowed(deleted, "BTC") is False
