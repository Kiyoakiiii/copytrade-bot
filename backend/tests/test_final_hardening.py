import inspect
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.api.preflight import _account_ratio_policy_check, _latest_dry_run_order_check
from app.core.config import Settings
from app.models import AllocationEvent, ExecutionOrder
from app.services.calculator import (
    SIZING_MODE_ACCOUNT_RATIO,
    UnsupportedSizingMode,
    calculate_copy_notional,
    calculate_target_notional_by_account_ratio,
)
from app.services.execution_router import ExecutionVenue
from app.services.hyperliquid_execution import build_hyperliquid_ioc_order
from app.services.low_latency_watcher import (
    FillDrivenExecutionEngine,
    HyperliquidLowLatencyWatcher,
    _set_latency_fields,
    unresolved_same_market_order_query,
)
from app.services.order_policy import (
    AUTO_COPY_ORDER_POLICY,
    BINANCE_AUTO_COPY_ORDER_TYPE,
    HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
    AutoCopyOrderPolicyError,
    assert_binance_auto_copy_order,
    assert_hyperliquid_auto_copy_order,
)
from app.services.sizing_guard import SizingGuardError, assert_sizing_mode_account_ratio, prohibit_fill_size_multiplier


def valid_order_plan(**overrides):
    payload = {
        "sizing_mode": SIZING_MODE_ACCOUNT_RATIO,
        "leader_account_value": Decimal("1000000"),
        "leader_account_value_source": "CLEARINGHOUSE_STATE",
        "leader_position_notional": Decimal("100000"),
        "follower_account_value": Decimal("1000"),
        "follower_account_value_source": "SPOT_CLEARINGHOUSE_STATE",
        "leader_position_ratio": Decimal("0.1"),
        "copy_multiplier": Decimal("0.1"),
        "target_notional": Decimal("10.00000000"),
        "delta_notional": Decimal("10.00000000"),
    }
    payload.update(overrides)
    return payload


def test_account_ratio_required_examples_are_locked() -> None:
    assert calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000000"),
        leader_position_notional=Decimal("100000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("0.1"),
    ) == Decimal("10.00000000")
    assert calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("80000"),
        leader_position_notional=Decimal("40000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("0.1"),
    ) == Decimal("50.00000000")


def test_leverage_and_max_notional_do_not_change_account_ratio_target() -> None:
    targets = [
        calculate_target_notional_by_account_ratio(
            leader_account_value=Decimal("1000000"),
            leader_position_notional=Decimal("100000"),
            follower_account_value=Decimal("1000"),
            copy_multiplier=Decimal("0.1"),
        )
        for _leverage in (1, 10, 50)
    ]
    assert set(targets) == {Decimal("10.00000000")}
    assert calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000000"),
        leader_position_notional=Decimal("100000"),
        follower_account_value=Decimal("1000"),
        copy_multiplier=Decimal("0.1"),
    ) == Decimal("10.00000000")


def test_unified_account_value_regression_example_is_locked() -> None:
    assert calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1"),
        leader_position_notional=Decimal("0.28530645"),
        follower_account_value=Decimal("399.6"),
        copy_multiplier=Decimal("0.1"),
    ) == Decimal("11.40084574")


def test_copy_multiplier_zero_blocks_account_ratio() -> None:
    with pytest.raises(ValueError, match="copy_multiplier"):
        calculate_target_notional_by_account_ratio(
            leader_account_value=Decimal("100000"),
            leader_position_notional=Decimal("10000"),
            follower_account_value=Decimal("1000"),
            copy_multiplier=Decimal("0"),
        )


def test_xyz_hip3_uses_same_account_ratio_guard() -> None:
    assert_sizing_mode_account_ratio(valid_order_plan())


def test_account_ratio_guard_allows_explicit_max_position_cap() -> None:
    assert_sizing_mode_account_ratio(
        valid_order_plan(
            max_position_notional_cap=Decimal("6"),
            target_notional_before_cap=Decimal("10.00000000"),
            target_notional=Decimal("6.00000000"),
            delta_notional=Decimal("6.00000000"),
        )
    )


def test_account_ratio_guard_allows_position_ratio_incremental_increase_without_account_values() -> None:
    assert_sizing_mode_account_ratio(
        valid_order_plan(
            leader_account_value=None,
            leader_account_value_source=None,
            leader_position_notional=Decimal("10000"),
            follower_account_value=None,
            follower_account_value_source=None,
            leader_position_ratio=None,
            copy_multiplier=Decimal("1"),
            current_allocation_notional=Decimal("50"),
            previous_leader_position_size=Decimal("100"),
            leader_position_size=Decimal("110"),
            increase_delta_source="leader_position_size",
            fill_delta_target_notional=Decimal("5.00000000"),
            target_notional_before_cap=Decimal("55.00000000"),
            target_notional=Decimal("55.00000000"),
            delta_notional=Decimal("5.00000000"),
        )
    )


def test_account_ratio_guard_allows_pending_reduce_offset_incremental_increase() -> None:
    assert_sizing_mode_account_ratio(
        valid_order_plan(
            leader_position_notional=Decimal("10000"),
            copy_multiplier=Decimal("1"),
            current_allocation_notional=Decimal("50"),
            previous_leader_position_size=Decimal("100"),
            leader_position_size=Decimal("110"),
            increase_delta_source="leader_position_size",
            pending_reduce_offset_notional=Decimal("0.75"),
            fill_delta_target_notional=Decimal("4.25000000"),
            target_notional_before_cap=Decimal("54.25000000"),
            target_notional=Decimal("54.25000000"),
            delta_notional=Decimal("4.25000000"),
        )
    )


def test_account_ratio_guard_rejects_bad_incremental_position_ratio_delta() -> None:
    with pytest.raises(SizingGuardError, match="position-ratio increase target mismatch"):
        assert_sizing_mode_account_ratio(
            valid_order_plan(
                leader_position_notional=Decimal("10000"),
                copy_multiplier=Decimal("1"),
                current_allocation_notional=Decimal("50"),
                previous_leader_position_size=Decimal("100"),
                leader_position_size=Decimal("110"),
                increase_delta_source="leader_position_size",
                fill_delta_target_notional=Decimal("4.00000000"),
                target_notional_before_cap=Decimal("54.00000000"),
                target_notional=Decimal("54.00000000"),
                delta_notional=Decimal("4.00000000"),
            )
        )


def test_account_ratio_guard_rejects_bad_pre_cap_target() -> None:
    with pytest.raises(SizingGuardError, match="pre-cap target mismatch"):
        assert_sizing_mode_account_ratio(
            valid_order_plan(
                max_position_notional_cap=Decimal("6"),
                target_notional_before_cap=Decimal("9"),
                target_notional=Decimal("6.00000000"),
                delta_notional=Decimal("6.00000000"),
            )
        )


def test_unified_follower_value_is_required_for_sizing_guard() -> None:
    with pytest.raises(SizingGuardError, match="follower_account_value"):
        assert_sizing_mode_account_ratio(valid_order_plan(follower_account_value=None))


def test_leader_value_unknown_blocks_sizing_guard() -> None:
    with pytest.raises(SizingGuardError, match="leader_account_value"):
        assert_sizing_mode_account_ratio(valid_order_plan(leader_account_value=None))


def test_leader_notional_delta_and_fill_size_multiplier_paths_are_forbidden() -> None:
    with pytest.raises(UnsupportedSizingMode):
        calculate_copy_notional(Decimal("1000"), Decimal("10000"), Decimal("1000"), Decimal("0.1"))
    with pytest.raises(SizingGuardError):
        prohibit_fill_size_multiplier(Decimal("1"), Decimal("0.1"))


def test_hyperliquid_auto_copy_policy_only_allows_ioc_market_equivalent() -> None:
    payload = build_hyperliquid_ioc_order(
        coin="BTC",
        is_buy=True,
        quantity=Decimal("0.01"),
        reference_price=Decimal("50000"),
        slippage_bps=100,
        reduce_only=False,
        cloid="0x" + "1" * 32,
    )
    assert_hyperliquid_auto_copy_order(order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE, payload=payload)
    assert AUTO_COPY_ORDER_POLICY == "FAST_MARKET_ONLY"


def test_hyperliquid_auto_copy_rejects_gtc_alo_and_post_only() -> None:
    payload = {"order_type": {"limit": {"tif": "Gtc"}}}
    with pytest.raises(AutoCopyOrderPolicyError):
        assert_hyperliquid_auto_copy_order(order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE, payload=payload)
    payload = {"order_type": {"limit": {"tif": "Alo"}}}
    with pytest.raises(AutoCopyOrderPolicyError):
        assert_hyperliquid_auto_copy_order(order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE, payload=payload)
    payload = {"order_type": {"limit": {"tif": "Ioc"}}, "post_only": True}
    with pytest.raises(AutoCopyOrderPolicyError):
        assert_hyperliquid_auto_copy_order(order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE, payload=payload)
    payload = {"order_type": {"limit": {"tif": "Ioc"}}, "postOnly": True}
    with pytest.raises(AutoCopyOrderPolicyError):
        assert_hyperliquid_auto_copy_order(order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE, payload=payload)
    with pytest.raises(AutoCopyOrderPolicyError):
        assert_hyperliquid_auto_copy_order(order_type="LIMIT", payload={"order_type": {"limit": {"tif": "Ioc"}}})


def test_binance_auto_copy_policy_only_allows_market_without_forbidden_fields() -> None:
    payload = {"type": "MARKET", "positionSide": "LONG", "quantity": "0.01"}
    assert_binance_auto_copy_order(order_type=BINANCE_AUTO_COPY_ORDER_TYPE, payload=payload)
    with pytest.raises(AutoCopyOrderPolicyError):
        assert_binance_auto_copy_order(order_type="LIMIT", payload={**payload, "type": "LIMIT", "price": "1"})
    for forbidden in ("price", "timeInForce", "reduceOnly", "closePosition"):
        with pytest.raises(AutoCopyOrderPolicyError):
            assert_binance_auto_copy_order(order_type=BINANCE_AUTO_COPY_ORDER_TYPE, payload={**payload, forbidden: "bad"})
    with pytest.raises(AutoCopyOrderPolicyError):
        assert_binance_auto_copy_order(order_type=BINANCE_AUTO_COPY_ORDER_TYPE, payload={**payload, "positionSide": "BOTH"})


def test_fast_market_policy_check_blocks_wrong_dry_run_order_type() -> None:
    order = ExecutionOrder(
        id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("1"),
        status="DRY_RUN",
        dry_run=True,
        **valid_order_plan(),
    )
    assert _latest_dry_run_order_check(order)["ok"] is False


def test_final_live_account_ratio_check_ok_for_valid_order() -> None:
    order = ExecutionOrder(
        id=1,
        leader_address="0x" + "1" * 40,
        source_coin="BTC",
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        side="BUY",
        order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        quantity=Decimal("1"),
        status="DRY_RUN",
        dry_run=True,
        **valid_order_plan(),
    )
    assert _account_ratio_policy_check(order)["ok"] is True


def test_same_leader_market_unknown_query_is_not_global() -> None:
    text = str(
        unresolved_same_market_order_query(
            leader_address="0x" + "1" * 40,
            dex="xyz",
            canonical_coin="xyz:BTC",
        )
    )
    assert "execution_orders.leader_address" in text
    assert "execution_orders.dex" in text
    assert "execution_orders.canonical_coin" in text


def test_low_latency_hot_path_has_no_snapshot_or_lock_gate() -> None:
    source = "\n".join(
        [
            inspect.getsource(FillDrivenExecutionEngine.handle_fill),
            inspect.getsource(FillDrivenExecutionEngine.reconcile_leader_symbol_allocation),
        ]
    )

    assert "LowLatencyLockManager" not in source
    assert "self.lock_manager" not in source
    assert "_load_or_refresh_follower_state" not in source
    assert "_load_or_refresh_leader_position" not in source
    assert 'blockers.append("POSITION_RECONCILE_MISMATCH' not in source
    assert "latest_account_positions" not in source.lower()
    assert "latest_account_states" not in source.lower()


def test_low_latency_run_does_not_block_ws_or_allocation_sync_on_warmup() -> None:
    source = inspect.getsource(HyperliquidLowLatencyWatcher.run)

    assert "await self._warm_latency_caches()" not in source
    assert "self._schedule_background_task(self._warm_latency_caches())" in source
    assert '"leader_fill_ws": self._leader_fill_ws_loop()' in source
    assert '"allocation_sync": self._allocation_sync_loop()' in source
    assert "self._run_critical_task(name, coro)" in source


def test_allocation_sync_poll_interval_stays_below_stale_allocation_limit() -> None:
    settings = Settings(_env_file=None)

    assert settings.allocation_sync_poll_seconds <= 1.0
    assert settings.allocation_sync_poll_seconds < 3.0
    assert settings.leader_fill_startup_backfill_seconds >= 30.0


def test_allocation_event_source_fill_id_is_not_unique() -> None:
    assert AllocationEvent.__table__.c.source_fill_id.unique is not True


def test_latency_fields_record_ws_to_submit_and_event_to_ack() -> None:
    from datetime import datetime, timedelta, timezone

    order = SimpleNamespace(
        hyperliquid_event_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ws_received_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=10),
        dedupe_done_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=20),
        debounce_released_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=20),
        decision_started_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=20),
        decision_done_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=30),
        order_submit_started_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=40),
        order_ack_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=80),
        order_finalized_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=90),
    )
    _set_latency_fields(order)
    assert order.ws_to_submit_ms == 30
    assert order.event_to_ack_ms == 80
