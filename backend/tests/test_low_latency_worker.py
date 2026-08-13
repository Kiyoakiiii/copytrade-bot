import pytest

from app.core.config import Settings
from app.tasks.low_latency_watcher import _settings_with_explicit_execution_route
from app.tasks.low_latency_worker import _restart_delay_seconds


def test_explicit_worker_recovers_single_durable_route_when_env_route_missing() -> None:
    route = "0x" + "2" * 40
    settings = Settings(
        _env_file=None,
        low_latency_leader_route_mode="EXPLICIT",
        hyperliquid_account_address="0x" + "1" * 40,
        hyperliquid_subaccount_address=None,
        hyperliquid_vault_address=None,
    )

    resolved = _settings_with_explicit_execution_route(settings, [route, route.upper()])

    assert resolved.hyperliquid_subaccount_address == route
    assert resolved.hyperliquid_follower_account_address() == route
    assert settings.hyperliquid_subaccount_address is None


def test_explicit_worker_rejects_ambiguous_durable_routes() -> None:
    settings = Settings(_env_file=None, low_latency_leader_route_mode="EXPLICIT")

    with pytest.raises(RuntimeError, match="exactly one"):
        _settings_with_explicit_execution_route(
            settings,
            ["0x" + "2" * 40, "0x" + "3" * 40],
        )


def test_explicit_worker_rejects_env_and_durable_route_conflict() -> None:
    settings = Settings(
        _env_file=None,
        low_latency_leader_route_mode="EXPLICIT",
        hyperliquid_subaccount_address="0x" + "2" * 40,
    )

    with pytest.raises(RuntimeError, match="conflicts"):
        _settings_with_explicit_execution_route(settings, ["0x" + "3" * 40])


def test_watcher_restart_backoff_prevents_permanent_error_storm() -> None:
    assert [_restart_delay_seconds(value) for value in range(1, 9)] == [
        1.0,
        2.0,
        4.0,
        8.0,
        16.0,
        30.0,
        30.0,
        30.0,
    ]
