from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import leader_balance_evaluator as evaluator  # noqa: E402


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
