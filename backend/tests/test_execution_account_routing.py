from __future__ import annotations

from app.api.preflight import _aggregate_watcher_statuses
from app.core.config import Settings
from app.services.leader_config import active_leaders_statement
from app.services.low_latency_watcher import (
    MarketKey,
    _follower_market_guard_query,
    _market_arrival_key,
    _market_transaction_key,
    unresolved_same_market_order_query,
)


SUBACCOUNT = "<REDACTED_EVM_ADDRESS>"


def _market() -> MarketKey:
    return MarketKey(
        dex="",
        coin="BTC",
        canonical_coin="BTC",
        raw_coin="BTC",
        asset_id=0,
        venue_symbol="BTC",
    )


def test_explicit_route_targets_and_scopes_the_subaccount() -> None:
    settings = Settings(
        hyperliquid_account_address="0x" + "1" * 40,
        hyperliquid_subaccount_address=SUBACCOUNT.upper(),
        low_latency_leader_route_mode="explicit",
    )

    assert settings.hyperliquid_follower_account_address() == SUBACCOUNT
    assert settings.hyperliquid_execution_vault_address() == SUBACCOUNT
    assert settings.low_latency_execution_scope() == SUBACCOUNT
    assert settings.low_latency_watcher_status_key() == f"watcher_status:{SUBACCOUNT}"


def test_default_route_keeps_historical_empty_database_scope() -> None:
    settings = Settings(
        hyperliquid_account_address="0x" + "1" * 40,
        low_latency_leader_route_mode="DEFAULT",
    )

    assert settings.low_latency_execution_scope() == ""
    assert settings.low_latency_watcher_status_key() == "watcher_status"


def test_execution_subaccount_allowlist_is_normalized_and_deduplicated() -> None:
    second = "0x" + "2" * 40
    settings = Settings(
        _env_file=None,
        hyperliquid_execution_subaccount_addresses=f" {SUBACCOUNT.upper()}, {second}, {SUBACCOUNT} ",
        hyperliquid_subaccount_address=second.upper(),
    )

    assert settings.hyperliquid_execution_subaccount_address_list() == [SUBACCOUNT, second]


def test_market_locks_are_isolated_between_main_and_subaccount() -> None:
    market = _market()

    assert _market_transaction_key(market, "") != _market_transaction_key(market, SUBACCOUNT)
    assert _market_arrival_key(market, "") != _market_arrival_key(market, SUBACCOUNT)


def test_durable_queries_include_execution_account_scope() -> None:
    order_sql = str(
        unresolved_same_market_order_query(
            leader_address="0x" + "2" * 40,
            dex="",
            canonical_coin="BTC",
            execution_scope=SUBACCOUNT,
        ).compile(compile_kwargs={"literal_binds": True})
    ).lower()
    guard_sql = str(
        _follower_market_guard_query(
            _market(),
            execution_scope=SUBACCOUNT,
        ).compile(compile_kwargs={"literal_binds": True})
    ).lower()

    assert "execution_orders.venue_account" in order_sql
    assert SUBACCOUNT in order_sql
    assert "follower_market_guards.execution_account" in guard_sql
    assert SUBACCOUNT in guard_sql


def test_leader_route_queries_are_disjoint() -> None:
    main_sql = str(
        active_leaders_statement(execution_scope="", explicit_route=False).compile(
            compile_kwargs={"literal_binds": True}
        )
    ).lower()
    sub_sql = str(
        active_leaders_statement(
            execution_scope=SUBACCOUNT,
            explicit_route=True,
        ).compile(compile_kwargs={"literal_binds": True})
    ).lower()

    assert "coalesce(leader_configs.hyperliquid_vault_address" in main_sql
    assert SUBACCOUNT not in main_sql
    assert SUBACCOUNT in sub_sql


def test_preflight_watcher_health_aggregates_main_and_subaccount() -> None:
    main_leader = "0x" + "1" * 40
    sub_leader = "0x" + "2" * 40
    common = {
        "low_latency_watcher_running": True,
        "low_latency_primary": True,
        "low_latency_ready": True,
        "websocket_connected": True,
        "ready_for_low_latency_live": True,
        "poll_fallback_leaders": [],
        "follower_order_updates_subscribed": True,
        "follower_user_events_subscribed": True,
        "follower_user_fills_subscribed": True,
        "dex_price_cache_status": {
            "": {"fresh": True, "age_ms": 20},
            "xyz": {"fresh": True, "age_ms": 30},
        },
    }

    result = _aggregate_watcher_statuses(
        [
            {
                **common,
                "active_leaders": [main_leader],
                "ws_leaders": [main_leader],
                "leader_user_fills_subscribed_count": 1,
            },
            {
                **common,
                "active_leaders": [sub_leader],
                "ws_leaders": [sub_leader],
                "leader_user_fills_subscribed_count": 1,
            },
        ],
        required_scopes_present=True,
    )

    assert result["ready_for_low_latency_live"] is True
    assert result["ws_leaders"] == [main_leader, sub_leader]
    assert result["leader_user_fills_subscribed_count"] == 2
    assert result["dex_price_cache_status"][""]["fresh"] is True


def test_preflight_watcher_health_fails_closed_for_missing_or_unready_subaccount() -> None:
    main = {
        "low_latency_watcher_running": True,
        "websocket_connected": True,
        "ready_for_low_latency_live": True,
    }
    sub = {
        "low_latency_watcher_running": True,
        "websocket_connected": False,
        "ready_for_low_latency_live": False,
    }

    missing = _aggregate_watcher_statuses(
        [main],
        required_scopes_present=False,
    )
    unready = _aggregate_watcher_statuses(
        [main, sub],
        required_scopes_present=True,
    )

    assert missing["ready_for_low_latency_live"] is False
    assert missing["websocket_connected"] is False
    assert unready["ready_for_low_latency_live"] is False
    assert unready["websocket_connected"] is False
