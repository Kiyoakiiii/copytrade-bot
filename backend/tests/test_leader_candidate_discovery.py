from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import leader_candidate_discovery as discovery  # noqa: E402


def candidate() -> discovery.Candidate:
    address = "0x" + "1" * 40
    return discovery.Candidate(
        address=address,
        label=discovery.private_label(address),
        tag="maker",
        views={
            "thirty_days": {
                "pnl": "5000",
                "perps_equity": "20000",
                "sharpe": "3",
                "drawdown": "5",
                "copy_score": "80",
                "total_trades": "1000",
                "history_span_days": "30",
            },
            "all": {
                "pnl": "20000",
                "perps_equity": "20000",
                "sharpe": "3",
                "drawdown": "10",
                "copy_score": "80",
                "total_trades": "1000",
                "history_span_days": "365",
            },
        },
    )


def test_public_report_does_not_contain_complete_address() -> None:
    item = candidate()
    discovery.preliminary_score(item)
    item.quick_score = Decimal("80")
    item.quick_metrics = {
        "breakeven_bps_before_funding": "12",
        "own_liquidation_clusters_in_samples": 0,
    }
    report = discovery.render_public_report([item], cutoff_ms=1)
    assert item.address not in report
    assert item.label in report


def test_quick_fill_metrics_subtracts_fees_and_friction() -> None:
    item = candidate()
    fills = [
        {
            "coin": "ETH",
            "dir": "Close Long",
            "sz": "1",
            "px": "1000",
            "closedPnl": "10",
            "fee": "1",
            "crossed": False,
            "time": 100,
        }
    ]
    metrics = discovery.quick_fill_metrics(
        fills,
        address=item.address,
        cutoff_ms=100,
        friction_bps=Decimal("5"),
    )
    assert Decimal(metrics["net_before_funding"]) == Decimal("9")
    assert Decimal(metrics["friction_net_before_funding"]) == Decimal("8.5")
    assert Decimal(metrics["breakeven_bps_before_funding"]) == Decimal("90")
    assert Decimal(metrics["maker_notional_pct"]) == Decimal("100")


def test_two_liquidation_windows_are_clustered_separately() -> None:
    item = candidate()
    fills = [
        {
            "time": 1,
            "liquidation": {"liquidatedUser": item.address},
        },
        {
            "time": 10 * 60 * 1000,
            "liquidation": {"liquidatedUser": item.address},
        },
        {
            "time": 60 * 60 * 1000,
            "liquidation": {"liquidatedUser": item.address},
        },
    ]
    assert discovery.own_liquidation_clusters(fills, item.address) == 2


def test_diversified_selection_preserves_non_core_style() -> None:
    items = []
    for index in range(8):
        item = candidate()
        item.address = "0x" + f"{index + 1:040x}"
        item.label = discovery.private_label(item.address)
        item.tag = "dominant" if index < 7 else "rare"
        item.preliminary_score = Decimal(100 - index)
        items.append(item)
    selected = discovery.diversified_selection(items, 4)
    assert any(item.tag == "rare" for item in selected)


def test_suffix_labels_are_disambiguated_on_collision() -> None:
    first = candidate()
    second = candidate()
    first.address = "0x" + "1" * 36 + "beef"
    second.address = "0x" + "2" * 36 + "beef"
    discovery.assign_unique_labels([first, second])
    assert first.label.startswith("beef-")
    assert second.label.startswith("beef-")
    assert first.label != second.label


def test_quick_screen_liquidations_are_observation_only() -> None:
    cutoff_ms = 200 * discovery.DAY_MS

    class Client:
        def __init__(self, liquidations: bool) -> None:
            self.liquidations = liquidations

        def post(self, payload, cache_key):
            rows = []
            for index in range(20):
                row = {
                    "hash": str(index),
                    "tid": index,
                    "coin": "ETH",
                    "dir": "Close Long",
                    "sz": "1",
                    "px": "1000",
                    "closedPnl": "10",
                    "fee": "1",
                    "time": cutoff_ms - (index + 1) * 60 * 60 * 1000,
                }
                if self.liquidations and index in {0, 19}:
                    row["liquidation"] = {"liquidatedUser": item_with.address}
                rows.append(row)
            return rows

    item_without = candidate()
    item_with = candidate()
    item_without.preliminary_score = item_with.preliminary_score = Decimal("80")
    for item in (item_without, item_with):
        item.views["all"]["history_span_days"] = "365"

    discovery.quick_screen_candidate(
        item_without,
        client=Client(False),
        cutoff_ms=cutoff_ms,
        friction_bps=Decimal("5"),
    )
    discovery.quick_screen_candidate(
        item_with,
        client=Client(True),
        cutoff_ms=cutoff_ms,
        friction_bps=Decimal("5"),
    )

    assert item_with.quick_metrics["own_liquidation_clusters_in_samples"] == 2
    assert item_with.quick_score == item_without.quick_score
    assert item_with.quick_rejections == item_without.quick_rejections
