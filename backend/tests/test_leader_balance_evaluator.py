from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import leader_balance_evaluator as evaluator  # noqa: E402


def test_programmatic_leader_requires_complete_prefixed_address() -> None:
    with pytest.raises(ValueError, match="invalid public address"):
        evaluator.LeaderInput("test", "a" * 40)


def test_programmatic_leader_canonicalizes_address_case() -> None:
    leader = evaluator.LeaderInput("test", "0x" + "A" * 40)

    assert leader.address == "0x" + "a" * 40


def test_parse_leader_uses_address_suffix_by_default() -> None:
    address = "0x" + "a" * 40
    leader = evaluator.parse_leader(address)

    assert leader.label == "aaaa"
    assert leader.address == address


def test_recommendation_rounds_up_below_target_tail() -> None:
    theoretical, recommendation, resulting_tail = evaluator.recommend_balance(
        Decimal("171262.1084"),
        Decimal("15.0455430026022854"),
        Decimal("7"),
        round_to=Decimal("10000"),
    )

    assert theoretical.quantize(Decimal("0.01")) == Decimal("368104.49")
    assert recommendation == Decimal("370000")
    assert resulting_tail < Decimal("7")


def test_projected_raw_loss_recommendation_does_not_use_current_account_total() -> None:
    theoretical, recommendation, resulting_tail = (
        evaluator.recommend_balance_for_projected_loss(
            Decimal("9105.64"),
            Decimal("7"),
            round_to=Decimal("10000"),
        )
    )

    assert theoretical.quantize(Decimal("0.01")) == Decimal("130080.57")
    assert recommendation == Decimal("140000")
    assert resulting_tail < Decimal("7")


def test_exposure_elasticity_distinguishes_fixed_and_proportional_sizing() -> None:
    fixed = [
        evaluator.DailyExposureObservation(
            day_start_ms=index,
            opening_equity=Decimal(100 + index * 20),
            peak_gross_notional=Decimal("500"),
        )
        for index in range(10)
    ]
    proportional = [
        evaluator.DailyExposureObservation(
            day_start_ms=index,
            opening_equity=Decimal(100 + index * 20),
            peak_gross_notional=Decimal(200 + index * 40),
        )
        for index in range(10)
    ]

    fixed_beta, fixed_upper = evaluator.estimate_exposure_elasticity(fixed)
    proportional_beta, proportional_upper = evaluator.estimate_exposure_elasticity(
        proportional
    )

    assert fixed_beta == Decimal("0")
    assert fixed_upper == Decimal("0")
    assert proportional_beta.quantize(Decimal("0.0001")) == Decimal("1.0000")
    assert proportional_upper.quantize(Decimal("0.0001")) == Decimal("1.0000")


def test_loss_per_peak_gross_uses_whole_drawdown_block_exposure() -> None:
    path = evaluator.PressurePath(
        regular={0: Decimal("0"), 10: Decimal("-10"), 20: Decimal("-20")},
        stress={20: Decimal("-30")},
    )
    exposure = {0: Decimal("100"), 10: Decimal("200"), 20: Decimal("50")}

    severity = evaluator.max_loss_per_peak_gross(path, exposure)

    assert severity == Decimal("0.15")


def test_gross_exposure_peak_reports_complete_position_evidence() -> None:
    long = evaluator.risk.Lifecycle(
        life_id="long-1",
        coin="AAA",
        side=1,
        start_ms=1,
        complete_start=True,
        initial_price=Decimal("1"),
        initial_position=Decimal("1"),
    )
    long.valuations = [
        {"time_ms": 1, "notional": Decimal("40"), "extreme": False},
        {"time_ms": 3, "notional": Decimal("60"), "extreme": False},
    ]
    short = evaluator.risk.Lifecycle(
        life_id="short-1",
        coin="BBB",
        side=-1,
        start_ms=2,
        complete_start=True,
        initial_price=Decimal("1"),
        initial_position=Decimal("1"),
    )
    short.valuations = [
        {"time_ms": 2, "notional": Decimal("50"), "extreme": False},
        {"time_ms": 4, "notional": Decimal("0"), "extreme": False},
    ]

    peak = evaluator.gross_exposure_peak([long, short], start_ms=0, end_ms=4)

    assert peak.time_ms == 3
    assert peak.gross_notional == Decimal("110")
    assert [(item.market, item.direction, item.gross_notional) for item in peak.positions] == [
        ("AAA", "LONG", Decimal("60")),
        ("BBB", "SHORT", Decimal("50")),
    ]


def test_gross_exposure_terminates_incomplete_non_censored_lifecycle() -> None:
    stale = evaluator.risk.Lifecycle(
        life_id="stale-long",
        coin="AAA",
        side=1,
        start_ms=1,
        complete_start=True,
        initial_price=Decimal("1"),
        initial_position=Decimal("1"),
        end_ms=2,
        complete_end=False,
        right_censored=False,
    )
    stale.valuations = [
        {"time_ms": 1, "notional": Decimal("40"), "extreme": False},
    ]
    replacement = evaluator.risk.Lifecycle(
        life_id="replacement-short",
        coin="AAA",
        side=-1,
        start_ms=2,
        complete_start=True,
        initial_price=Decimal("1"),
        initial_position=Decimal("1"),
        end_ms=3,
        right_censored=True,
    )
    replacement.valuations = [
        {"time_ms": 2, "notional": Decimal("25"), "extreme": False},
    ]

    path = evaluator.gross_exposure_path([stale, replacement])
    peak = evaluator.gross_exposure_peak([stale, replacement], start_ms=2, end_ms=3)

    assert path[2] == Decimal("25")
    assert peak.gross_notional == Decimal("25")
    assert [(item.market, item.direction) for item in peak.positions] == [("AAA", "SHORT")]


def test_joint_drawdown_does_not_add_non_overlapping_losses() -> None:
    paths = {
        "a": evaluator.PressurePath(
            regular={0: Decimal("0"), 10: Decimal("-100"), 20: Decimal("0")},
            stress={},
        ),
        "b": evaluator.PressurePath(
            regular={0: Decimal("0"), 10: Decimal("50"), 20: Decimal("-50")},
            stress={},
        ),
    }

    result = evaluator.portfolio_drawdown(
        paths,
        {"a": Decimal("1"), "b": Decimal("1")},
        start_ms=0,
    )

    assert result.amount == Decimal("50")
    assert result.trough_ms == 10


def test_pressure_observation_is_not_carried_forward() -> None:
    path = evaluator.PressurePath(
        regular={0: Decimal("0"), 10: Decimal("0"), 20: Decimal("25")},
        stress={10: Decimal("-75")},
    )

    result = evaluator.portfolio_drawdown(
        {"leader": path},
        {"leader": Decimal("1")},
        start_ms=0,
    )

    assert result.amount == Decimal("75")
    assert result.trough_ms == 10


def test_concurrent_no_offset_drawdown_does_not_use_profit_offsets() -> None:
    paths = {
        "a": evaluator.PressurePath(
            regular={0: Decimal("0"), 10: Decimal("-40")},
            stress={},
        ),
        "b": evaluator.PressurePath(
            regular={0: Decimal("0"), 10: Decimal("25")},
            stress={},
        ),
    }

    amount, timestamp, parts = evaluator.concurrent_no_offset_drawdown(
        paths,
        {"a": Decimal("1"), "b": Decimal("1")},
        start_ms=0,
    )

    assert amount == Decimal("40")
    assert timestamp == 10
    assert parts == {"a": Decimal("40"), "b": Decimal("0")}


def test_public_fill_pagination_is_not_truncated_after_twelve_pages() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.calls = 0

        def post(self, payload, cache_key):
            del payload, cache_key
            page_number = self.calls
            self.calls += 1
            page_size = evaluator.risk.FILL_PAGE_SIZE if page_number < 13 else 1
            first = page_number * evaluator.risk.FILL_PAGE_SIZE
            return [
                {
                    "hash": f"0x{first + index:064x}",
                    "tid": first + index,
                    "oid": first + index,
                    "time": first + index + 1,
                    "coin": "TEST",
                    "px": "1",
                    "sz": "1",
                    "startPosition": "0",
                    "side": "B",
                }
                for index in range(page_size)
            ]

    client = FakeClient()
    fills, saturated = evaluator.risk.fetch_fills(
        client,
        "test",
        "0x" + "0" * 40,
        99_999_999,
    )

    assert client.calls == 14
    assert len(fills) == 13 * evaluator.risk.FILL_PAGE_SIZE + 1
    assert saturated is True
