from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.models import ExecutionOrder, LeaderConfig, SourceFill
from app.services.leader_performance import (
    _actual_follower_fills_by_order,
    _build_leader_payload,
    _copyability_metrics,
    _follower_contribution,
    _leader_behavior,
    _logical_leader_events,
    _portfolio_metrics_since_join,
    performance_refresh_delay_seconds,
)


NOW = datetime(2026, 7, 21, 12, 0, tzinfo=timezone.utc)


def test_performance_refresh_is_due_when_cache_is_missing_or_invalid() -> None:
    assert performance_refresh_delay_seconds(None, now=NOW, interval_seconds=86400) == 0
    assert (
        performance_refresh_delay_seconds(
            {"generated_at": "invalid"},
            now=NOW,
            interval_seconds=86400,
        )
        == 0
    )


def test_fresh_performance_cache_survives_worker_restart_without_api_refresh() -> None:
    generated_at = NOW - timedelta(hours=3)

    delay = performance_refresh_delay_seconds(
        {"generated_at": generated_at.isoformat()},
        now=NOW,
        interval_seconds=86400,
    )

    assert delay == 21 * 60 * 60


def test_stale_performance_cache_is_due_for_daily_refresh() -> None:
    delay = performance_refresh_delay_seconds(
        {"generated_at": (NOW - timedelta(days=2)).isoformat()},
        now=NOW,
        interval_seconds=86400,
    )

    assert delay == 0


def _source_fill(
    source_id: str,
    *,
    time_ms: int,
    side: str,
    size: str,
    price: str,
    direction: str,
    oid: int,
    start_position: str,
    closed_pnl: str = "0",
    fee: str = "0",
) -> SourceFill:
    return SourceFill(
        source_fill_id=source_id,
        leader_address="<REDACTED_EVM_ADDRESS>",
        coin="TEST",
        canonical_coin="TEST",
        raw_coin="TEST",
        dex="",
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        source_time_ms=time_ms,
        is_snapshot=False,
        raw_fill={
            "side": side,
            "dir": direction,
            "oid": oid,
            "startPosition": start_position,
            "closedPnl": closed_pnl,
            "fee": fee,
        },
    )


def test_fragmented_leader_fills_are_one_lifecycle() -> None:
    fills = [
        _source_fill(
            "open-1",
            time_ms=1_000,
            side="B",
            size="1",
            price="100",
            direction="Open Long",
            oid=1,
            start_position="0",
            fee="0.10",
        ),
        _source_fill(
            "open-2",
            time_ms=1_000,
            side="B",
            size="1",
            price="101",
            direction="Open Long",
            oid=1,
            start_position="1",
            fee="0.10",
        ),
        _source_fill(
            "close-1",
            time_ms=3_601_000,
            side="A",
            size="2",
            price="110",
            direction="Close Long",
            oid=2,
            start_position="2",
            closed_pnl="19",
            fee="0.20",
        ),
    ]

    logical = _logical_leader_events(fills)
    behavior = _leader_behavior(logical, [])

    assert len(logical) == 2
    assert logical[0]["start_position"] == Decimal("0")
    assert logical[0]["size"] == Decimal("2")
    assert behavior["complete_lifecycles"] == 1
    assert behavior["winning_lifecycles"] == 1
    assert behavior["median_hold_hours"] == 1.0


def test_portfolio_window_starts_at_leader_join_and_tracks_drawdown() -> None:
    joined = NOW - timedelta(days=2)
    before = int((joined - timedelta(hours=1)).timestamp() * 1000)
    at_join = int(joined.timestamp() * 1000)
    after = int((joined + timedelta(hours=1)).timestamp() * 1000)
    end = int(NOW.timestamp() * 1000)
    portfolio = [
        (
            "perpAllTime",
            {"pnlHistory": [[before, "90"], [at_join, "100"], [after, "160"], [end, "120"]]},
        ),
        (
            "allTime",
            {"accountValueHistory": [[before, "1000"], [at_join, "1000"], [end, "1020"]]},
        ),
    ]

    metrics = _portfolio_metrics_since_join(portfolio, joined)

    assert metrics["portfolio_pnl_since_join"] == "20"
    assert metrics["portfolio_return_pct"] == "2.00"
    assert metrics["max_drawdown"] == "40"
    assert metrics["current_drawdown"] == "40"
    assert metrics["curve"][0]["pnl"] == "0"


def test_exchange_fills_are_contribution_source_of_truth_even_when_db_status_disagrees() -> None:
    joined = NOW - timedelta(days=1)
    missing = ExecutionOrder(
        id=1,
        leader_id=7,
        leader_address="0xleader",
        source_fill_id="source-1",
        source_type="AUTO_COPY",
        execution_venue="HYPERLIQUID",
        source_coin="TEST",
        side="BUY",
        quantity=Decimal("1"),
        status="FILLED",
        cloid="0xaaa",
        leader_entry_px=Decimal("100"),
        created_at=joined,
    )
    actual = ExecutionOrder(
        id=2,
        leader_id=7,
        leader_address="0xleader",
        source_fill_id="source-2",
        source_type="AUTO_COPY",
        execution_venue="HYPERLIQUID",
        source_coin="TEST",
        side="SELL",
        quantity=Decimal("1"),
        status="REDUCEONLYREJECTED",
        cloid="0xbbb",
        leader_entry_px=Decimal("110"),
        created_at=joined,
        event_to_final_ms=800,
    )
    follower_fills = [
        {
            "cloid": "0xbbb",
            "oid": 22,
            "time": int(NOW.timestamp() * 1000),
            "px": "109",
            "sz": "1",
            "closedPnl": "9",
            "fee": "1",
            "hash": "0xfill",
            "tid": 2,
            "coin": "TEST",
        }
    ]

    actual_map = _actual_follower_fills_by_order([missing, actual], follower_fills)
    follower = _follower_contribution([missing, actual], actual_map)
    copyability = _copyability_metrics(
        [missing, actual],
        actual_map,
        follower,
        {"minimum_10u_exempt_events": 0, "fcfs_blocked_events": 0, "manual_review_events": 0},
    )

    assert follower["realized_net_ex_funding"] == "8"
    assert follower["matched_exchange_orders"] == 1
    assert copyability["exchange_match_coverage_pct"] == 0.0
    assert copyability["database_status_disagreement_orders"] == 1


def test_performance_payload_exposes_suffix_and_full_public_address() -> None:
    full_address = "<REDACTED_EVM_ADDRESS>"
    joined = NOW - timedelta(days=30)
    leader = LeaderConfig(
        id=9,
        leader_address=full_address,
        enabled=True,
        created_at=joined - timedelta(days=60),
        performance_started_at=joined,
    )
    joined_ms = int(joined.timestamp() * 1000)
    now_ms = int(NOW.timestamp() * 1000)
    payload = _build_leader_payload(
        leader=leader,
        now=NOW,
        source_fills=[],
        outcomes_by_source={},
        orders=[],
        actual_fills_by_order={},
        allocation_events=[],
        leader_positions=[],
        follower_open={"unrealized_pnl": Decimal("0"), "notional": Decimal("0"), "manual_sync": False},
        portfolio=[
            (
                "perpAllTime",
                {"pnlHistory": [[joined_ms, "0"], [now_ms, "100"]]},
            ),
            (
                "allTime",
                {"accountValueHistory": [[joined_ms, "1000"], [now_ms, "1100"]]},
            ),
        ],
        funding=[],
    )
    serialized = json.dumps(payload).lower()

    assert payload["label"] == "Leader · ABCD"
    assert payload["joined_at"] == joined.isoformat()
    assert payload["observed_days"] == 30.0
    assert payload["address_suffix"] == "ABCD"
    assert payload["leader_address"] == full_address
    assert full_address.lower() in serialized
    assert "curve" not in payload["leader_account"]
    assert payload["history"]["leader_pnl"][-1]["pnl"] == "100"
