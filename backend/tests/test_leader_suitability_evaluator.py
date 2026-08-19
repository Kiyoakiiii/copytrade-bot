from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest


SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import leader_suitability_evaluator as evaluator  # noqa: E402


def test_programmatic_leader_requires_complete_prefixed_address() -> None:
    with pytest.raises(ValueError, match="invalid public address"):
        evaluator.LeaderSpec("test", "1" * 40)


def test_programmatic_leader_canonicalizes_address_case() -> None:
    leader = evaluator.LeaderSpec("test", "0x" + "A" * 40)

    assert leader.address == "0x" + "a" * 40


def _metrics(**overrides):
    values = {
        "history_days": Decimal("200"),
        "fill_start_ms": 1,
        "fill_end_ms": 200 * evaluator.DAY_MS,
        "fill_limit_saturated": False,
        "perp_fill_count": 1000,
        "logical_fill_count": 500,
        "complete_lifecycle_count": 200,
        "market_count": 10,
        "position_mismatch_rate_pct": Decimal("0"),
        "current_account_total": Decimal("100000"),
        "normalized_net": Decimal("10000"),
        "normalized_turnover": Decimal("1000000"),
        "profit_factor": Decimal("3"),
        "friction_bps": Decimal("5"),
        "friction_net": Decimal("9500"),
        "friction_profit_factor": Decimal("2.5"),
        "breakeven_friction_bps": Decimal("100"),
        "annual_simple_return_pct": Decimal("90"),
        "friction_annual_simple_return_pct": Decimal("80"),
        "profitable_month_pct": Decimal("80"),
        "month_count": 7,
        "top3_win_concentration_pct": Decimal("30"),
        "fast_profit_share_pct": Decimal("5"),
        "pressure_drawdown": Decimal("2000"),
        "pressure_tail_pct": Decimal("10"),
        "regular_pressure_tail_pct": Decimal("8"),
        "official_portfolio_drawdown_pct": Decimal("6"),
        "worst_lifecycle_trough_pct": Decimal("7"),
        "worst_lifecycle_hold_hours": Decimal("8"),
        "worst_lifecycle_underwater_add_rate_pct": Decimal("0"),
        "worst_closed_loss_pct": Decimal("4"),
        "p95_losing_hold_hours": Decimal("10"),
        "loss_over_72h_pct": Decimal("0"),
        "current_open_losing_count": 0,
        "max_current_open_losing_hold_hours": Decimal("0"),
        "current_open_worst_trough_pct": Decimal("0"),
        "current_open_underwater_add_rate_pct": Decimal("0"),
        "underwater_add_rate_pct": Decimal("20"),
        "peak_simultaneous_notional": Decimal("30000"),
        "recommended_balance_for_target_tail": Decimal("150000"),
        "recommended_balance_ratio_to_current": Decimal("1.5"),
        "safe_size_skip_count_pct": Decimal("10"),
        "safe_size_skip_notional_pct": Decimal("0.5"),
        "maker_notional_pct": Decimal("95"),
        "raw_fee_bps": Decimal("0"),
        "interarrival_le_1s_pct": Decimal("99"),
        "max_logical_events_per_second": 100,
        "p95_logical_events_per_active_day": Decimal("10000"),
        "liquidation_fragments": 0,
        "liquidation_event_count": 0,
        "liquidation_cluster_count": 0,
        "latest_liquidation_ms": None,
        "liquidation_market_count": 0,
    }
    values.update(overrides)
    return values


def _analysis(label: str = "test", **metric_overrides):
    address = "0x" + "1" * 40
    return evaluator.LeaderAnalysis(
        spec=evaluator.LeaderSpec(label, address),
        summary={},
        fills=[],
        logical_events=[],
        lifecycles=[],
        metrics=_metrics(**metric_overrides),
    )


def test_reports_never_render_complete_address() -> None:
    analysis = _analysis()
    evaluator.score_analysis(analysis, cutoff_ms=300 * evaluator.DAY_MS)
    report = evaluator.render_report(
        [analysis],
        cutoff_ms=300 * evaluator.DAY_MS,
        friction_bps=Decimal("5"),
        target_tail_pct=Decimal("7"),
    )
    assert analysis.spec.address not in report
    assert "test" in report


def test_self_liquidations_are_observation_only() -> None:
    none = _analysis("none")
    repeated = _analysis(
        "repeated",
        liquidation_cluster_count=20,
        liquidation_event_count=30,
        liquidation_market_count=10,
        latest_liquidation_ms=250 * evaluator.DAY_MS,
    )
    evaluator.score_analysis(none, cutoff_ms=300 * evaluator.DAY_MS)
    evaluator.score_analysis(repeated, cutoff_ms=300 * evaluator.DAY_MS)
    assert repeated.score == none.score
    assert repeated.verdict == none.verdict
    assert repeated.hard_failures == none.hard_failures
    assert repeated.warnings == none.warnings


def test_severe_weeklong_lifecycle_drawdown_is_a_hard_gate() -> None:
    analysis = _analysis(
        worst_lifecycle_trough_pct=Decimal("35"),
        worst_lifecycle_hold_hours=Decimal("168"),
    )
    evaluator.score_analysis(analysis, cutoff_ms=300 * evaluator.DAY_MS)
    assert analysis.verdict == "REJECT"
    assert any("扛浮亏" in reason for reason in analysis.hard_failures or [])


def test_still_open_underwater_averaging_is_a_hard_gate() -> None:
    analysis = _analysis(
        current_open_losing_count=1,
        max_current_open_losing_hold_hours=Decimal("336"),
        current_open_worst_trough_pct=Decimal("15"),
        current_open_underwater_add_rate_pct=Decimal("50"),
    )
    evaluator.score_analysis(analysis, cutoff_ms=300 * evaluator.DAY_MS)
    assert analysis.verdict == "REJECT"
    assert any("未结束亏损仓位" in reason for reason in analysis.hard_failures or [])


def test_systematic_long_underwater_averaging_is_a_hard_gate() -> None:
    analysis = _analysis(
        p95_losing_hold_hours=Decimal("720"),
        loss_over_72h_pct=Decimal("50"),
        underwater_add_rate_pct=Decimal("50"),
    )

    evaluator.score_analysis(analysis, cutoff_ms=300 * evaluator.DAY_MS)

    assert analysis.verdict == "REJECT"
    assert any("系统性死扛" in reason for reason in analysis.hard_failures or [])


def test_frequency_and_maker_share_are_not_direct_score_penalties() -> None:
    calm = _analysis("calm", maker_notional_pct=Decimal("0"), interarrival_le_1s_pct=ZERO)
    busy = _analysis(
        "busy",
        maker_notional_pct=Decimal("100"),
        interarrival_le_1s_pct=Decimal("100"),
        max_logical_events_per_second=1000,
        p95_logical_events_per_active_day=Decimal("50000"),
    )
    evaluator.score_analysis(calm, cutoff_ms=300 * evaluator.DAY_MS)
    evaluator.score_analysis(busy, cutoff_ms=300 * evaluator.DAY_MS)
    assert calm.score == busy.score
    assert calm.verdict == busy.verdict


def test_safe_sizing_untradeable_notional_is_a_hard_gate() -> None:
    analysis = _analysis(safe_size_skip_notional_pct=Decimal("10"))
    evaluator.score_analysis(analysis, cutoff_ms=300 * evaluator.DAY_MS)
    assert analysis.verdict == "REJECT"
    assert any("最小订单" in reason for reason in analysis.hard_failures or [])


def test_account_conflict_uses_open_inside_peer_interval() -> None:
    candidate = _analysis("candidate")
    candidate.lifecycles = [
        SimpleNamespace(
            complete_start=True,
            scale=Decimal("1"),
            start_ms=20,
            end_ms=30,
            coin="ETH",
        ),
        SimpleNamespace(
            complete_start=True,
            scale=Decimal("1"),
            start_ms=40,
            end_ms=50,
            coin="BTC",
        ),
    ]
    peer = _analysis("peer")
    peer.lifecycles = [
        SimpleNamespace(
            complete_start=True,
            scale=Decimal("1"),
            start_ms=10,
            end_ms=25,
            coin="ETH",
        )
    ]
    assert evaluator.interval_conflict_pct(candidate, [peer]) == Decimal("50")


def test_flow_neutral_path_uses_post_transfer_capital_for_later_pnl() -> None:
    class Normalizer:
        @staticmethod
        def value_at(timestamp: int) -> Decimal:
            return Decimal("100") if timestamp < 20 else Decimal("1000")

    path = evaluator.balance.PressurePath(
        regular={10: Decimal("0"), 30: Decimal("-100")},
        stress={},
    )

    metrics = evaluator.flow_neutral_path_metrics(path, Normalizer())

    assert metrics["final_return"] == Decimal("-0.1")
    assert metrics["regular_drawdown"] == Decimal("0.1")
    assert metrics["pressure_drawdown"] == Decimal("0.1")


def test_account_total_history_does_not_add_perpetual_component_twice() -> None:
    portfolio = {
        "allTime": {
            "accountValueHistory": [[10, "150"], [20, "180"]],
            "pnlHistory": [],
        },
        "perpAllTime": {
            "accountValueHistory": [[10, "40"], [20, "50"]],
            "pnlHistory": [],
        },
    }

    normalizer = evaluator.risk.CapitalNormalizer(
        address="0x" + "a" * 40,
        portfolio=portfolio,
        ledger=[],
        raw_perp_curve=[],
    )

    assert evaluator.risk.total_account_samples(portfolio) == [
        (10, Decimal("150")),
        (20, Decimal("180")),
    ]
    assert normalizer.current_total_account_value == Decimal("180")
    assert normalizer.current_perp_account_value == Decimal("50")
    assert normalizer.current_spot_account_value == Decimal("130")


def test_pressure_extreme_does_not_mutate_flow_neutral_regular_path() -> None:
    class Normalizer:
        @staticmethod
        def value_at(timestamp: int) -> Decimal:
            return Decimal("1000") if timestamp < 19 else Decimal("1100")

    path = evaluator.balance.PressurePath(
        regular={10: Decimal("100"), 20: Decimal("150")},
        stress={10: Decimal("-100")},
    )

    metrics = evaluator.flow_neutral_path_metrics(path, Normalizer())

    assert metrics["final_return"] == Decimal("0.15")
    assert metrics["regular_drawdown"] == Decimal("0")
    assert metrics["pressure_drawdown"] == Decimal("2") / Decimal("11")


def test_flow_neutral_drawdown_chain_links_period_returns() -> None:
    class Normalizer:
        @staticmethod
        def value_at(timestamp: int) -> Decimal:
            if timestamp < 20:
                return Decimal("100")
            if timestamp < 30:
                return Decimal("150")
            return Decimal("100")

    path = evaluator.balance.PressurePath(
        regular={10: Decimal("50"), 20: Decimal("0"), 30: Decimal("-50")},
        stress={},
    )

    metrics = evaluator.flow_neutral_path_metrics(path, Normalizer())

    assert metrics["final_return"] == Decimal("-0.5")
    assert metrics["regular_drawdown"] == Decimal("2") / Decimal("3")
    assert metrics["regular_drawdown"] <= Decimal("1")


ZERO = Decimal("0")
