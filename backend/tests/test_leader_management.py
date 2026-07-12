import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api.leaders import _leader_payload, _normalized_payload, _replacement_data, _validate_leader_address, _validate_patch
from app.core.config import Settings
from app.services.leader_config import (
    ALL_COINS,
    CUSTOM_LIST,
    allowed_coins_mode,
    canonical_coin,
    enable_leader,
    is_coin_allowed,
    normalize_allowed_symbols_for_storage,
    normalize_blocked_symbols_for_storage,
    soft_delete_leader,
    watcher_consistency,
)
from app.services.startup_config_validator import _leader_checks


def leader(**overrides):
    data = {
        "id": 1,
        "enabled": True,
        "deleted_at": None,
        "delete_reason": None,
        "leader_address": "0x" + "1" * 40,
        "copy_multiplier": Decimal("0.1"),
        "fixed_account_value": Decimal("1000"),
        "allowed_symbols": None,
        "blocked_symbols": [],
        "max_notional_per_trade": Decimal("10"),
        "max_total_notional": Decimal("100"),
        "max_leverage": 1,
        "slippage_bps": 20,
        "preferred_venue": "HYPERLIQUID",
        "fallback_venue": "NONE",
        "enabled_venues": ["HYPERLIQUID"],
        "hyperliquid_account_id": None,
        "hyperliquid_vault_address": None,
    }
    data.update(overrides)
    return SimpleNamespace(**data)


def test_coin_allowed_returns_false_when_leader_disabled() -> None:
    assert is_coin_allowed(leader(enabled=False), "BTC") is False


def test_coin_allowed_returns_false_when_leader_deleted() -> None:
    assert is_coin_allowed(leader(deleted_at=datetime.now(timezone.utc)), "BTC") is False


def test_coin_allowed_returns_false_for_blocked_coin() -> None:
    assert is_coin_allowed(leader(blocked_symbols=["btc"]), "BTC") is False


def test_coin_allowed_none_allowlist_means_all_coins() -> None:
    assert is_coin_allowed(leader(allowed_symbols=None), "HYPE") is True


def test_coin_allowed_empty_allowlist_means_all_coins() -> None:
    assert is_coin_allowed(leader(allowed_symbols=[]), "PURR") is True


def test_coin_allowed_custom_list_allows_member() -> None:
    assert is_coin_allowed(leader(allowed_symbols=["BTC", "ETH"]), "eth") is True


def test_coin_allowed_custom_list_rejects_missing_coin() -> None:
    assert is_coin_allowed(leader(allowed_symbols=["BTC", "ETH"]), "SOL") is False


def test_coin_allowed_normalizes_case_and_perp_suffix() -> None:
    assert is_coin_allowed(leader(allowed_symbols=["btc"]), "BTC-PERP") is True


def test_coin_allowed_normalizes_binance_style_symbol() -> None:
    assert canonical_coin("BTCUSDT") == "BTC"
    assert is_coin_allowed(leader(allowed_symbols=["BTC"]), "btcusdt") is True


def test_allowed_storage_empty_becomes_null_for_all_coins() -> None:
    assert normalize_allowed_symbols_for_storage([]) is None


def test_allowed_storage_normalizes_and_deduplicates_custom_list() -> None:
    assert normalize_allowed_symbols_for_storage([" btc ", "BTC-PERP", "eth"]) == ["BTC", "ETH"]


def test_blocked_storage_defaults_to_empty_list() -> None:
    assert normalize_blocked_symbols_for_storage(None) == []


def test_allowed_coins_mode_reports_all_coins_for_null_or_empty() -> None:
    assert allowed_coins_mode(leader(allowed_symbols=None)) == ALL_COINS
    assert allowed_coins_mode(leader(allowed_symbols=[])) == ALL_COINS


def test_allowed_coins_mode_reports_custom_list_only_when_nonempty() -> None:
    assert allowed_coins_mode(leader(allowed_symbols=["BTC"])) == CUSTOM_LIST


def test_soft_delete_preserves_object_and_marks_disabled() -> None:
    item = leader(enabled=True)
    soft_delete_leader(item, reason="operator request")
    assert item.enabled is False
    assert item.deleted_at is not None
    assert item.delete_reason == "operator request"


def test_enable_leader_clears_soft_delete_fields() -> None:
    item = leader(enabled=False, deleted_at=datetime.now(timezone.utc), delete_reason="old")
    enable_leader(item)
    assert item.enabled is True
    assert item.deleted_at is None
    assert item.delete_reason is None


def test_watcher_consistency_reports_missing_and_stale_subscriptions() -> None:
    leaders = [
        leader(leader_address="0x" + "1" * 40, enabled=True, deleted_at=None),
        leader(leader_address="0x" + "2" * 40, enabled=False, deleted_at=None),
        leader(leader_address="0x" + "3" * 40, enabled=True, deleted_at=datetime.now(timezone.utc)),
    ]
    result = watcher_consistency(leaders=leaders, watcher_active_addresses=["0x" + "2" * 40])
    assert result["db_enabled_leaders_count"] == 1
    assert result["watcher_active_leaders_count"] == 1
    assert result["leaders_not_subscribed"] == ["0x" + "1" * 40]
    assert result["subscribed_but_disabled_or_deleted"] == ["0x" + "2" * 40]


def test_normalized_payload_empty_allowlist_does_not_default_btc_eth_sol() -> None:
    data = _normalized_payload({"allowed_symbols": [], "blocked_symbols": []})
    assert data["allowed_symbols"] is None
    assert data["blocked_symbols"] == []


def test_clear_api_error_for_invalid_leader_address() -> None:
    with pytest.raises(HTTPException) as exc:
        _validate_leader_address("not-an-address")
    assert exc.value.status_code == 400
    assert "leader_address" in exc.value.detail


def test_clear_api_error_for_invalid_copy_multiplier() -> None:
    with pytest.raises(HTTPException) as exc:
        _validate_patch({"copy_multiplier": Decimal("0")})
    assert exc.value.status_code == 400
    assert "copy_multiplier" in exc.value.detail


@pytest.mark.parametrize("value", [None, Decimal("0"), Decimal("-1")])
def test_clear_api_error_for_invalid_fixed_account_value(value) -> None:
    with pytest.raises(HTTPException) as exc:
        _validate_patch({"fixed_account_value": value})
    assert exc.value.status_code == 400
    assert "fixed_account_value" in exc.value.detail


def test_leader_payload_does_not_expose_private_key_or_secret_value() -> None:
    payload = _leader_payload(
        leader(hyperliquid_vault_address="0x" + "9" * 40),
        latest_state=None,
        allocation_stats={"count": 0, "notional": Decimal("0")},
        watcher_active=set(),
    )
    assert "hyperliquid_private_key" not in payload
    assert "hyperliquid_vault_address" not in payload
    assert payload["hyperliquid_vault_address_configured"] is True
    assert payload["fixed_account_value"] == "1000"


def test_replacement_data_copies_existing_active_leader_settings() -> None:
    data = _replacement_data(
        leader(
            copy_multiplier=Decimal("0.23"),
            fixed_account_value=Decimal("50000"),
            allowed_symbols=["xyz:URNM"],
            blocked_symbols=["xyz:GME"],
            max_notional_per_trade=Decimal("12"),
            max_total_notional=Decimal("34"),
            preferred_venue="HYPERLIQUID",
            fallback_venue="NONE",
            enabled_venues=["HYPERLIQUID"],
        )
    )
    assert data["enabled"] is True
    assert data["copy_multiplier"] == Decimal("0.23")
    assert data["fixed_account_value"] == Decimal("50000")
    assert data["allowed_symbols"] == ["xyz:URNM"]
    assert data["blocked_symbols"] == ["xyz:GME"]
    assert data["max_notional_per_trade"] == Decimal("12")
    assert data["max_total_notional"] == Decimal("34")


def test_startup_leader_checks_allow_empty_allowed_symbols_as_all_coins() -> None:
    class FakeResult:
        def scalars(self):
            return self

        def all(self):
            return [leader(allowed_symbols=None)]

    class FakeDb:
        async def execute(self, _statement):
            return FakeResult()

    checks = asyncio.run(_leader_checks(FakeDb(), Settings(_env_file=None)))
    messages = [check.message for check in checks]
    assert "enabled leader has no allowed coins" not in messages
    assert not any("allowed coins" in message for message in messages)
