import json
from decimal import Decimal

from app.core.config import Settings
from app.services.account_state import LEADER, parse_account_state
from app.services.hyperliquid import HyperliquidWatcher
from app.services.hyperliquid_dex import HyperliquidDexRegistry, canonical_coin, parse_coin
from app.services.hyperliquid_execution import (
    build_hyperliquid_cloid,
    build_hyperliquid_ioc_order,
    resolve_asset_id_from_meta,
)
from app.services.leader_config import allowed_coin_match_status, allowed_coins_mode, is_coin_allowed
from app.services.market_coverage import build_hyperliquid_market_coverage


def settings(**overrides) -> Settings:
    return Settings(_env_file=None, **overrides)


def leader_config(**overrides):
    class Leader:
        enabled = True
        deleted_at = None
        allowed_symbols = None
        blocked_symbols = []

    item = Leader()
    for key, value in overrides.items():
        setattr(item, key, value)
    return item


def test_account_address_takes_precedence_over_api_wallet_for_state_query() -> None:
    cfg = settings(
        hyperliquid_account_address="0x" + "1" * 40,
        hyperliquid_api_wallet_address="0x" + "2" * 40,
        hyperliquid_private_key=None,
    )

    assert cfg.hyperliquid_follower_account_address() == "0x" + "1" * 40
    assert cfg.hyperliquid_follower_account_address() != cfg.hyperliquid_api_wallet_address


def test_enabled_dexes_include_default_and_xyz() -> None:
    cfg = settings(enabled_hyperliquid_dexes=",xyz", enable_xyz_dex=True)

    assert [dex.dex_name for dex in HyperliquidDexRegistry(cfg).enabled_dexes()] == ["", "xyz"]


def test_parse_hip3_coin_canonical_form() -> None:
    parsed = parse_coin("xyz:XYZ100")

    assert parsed.dex == "xyz"
    assert parsed.coin == "XYZ100"
    assert parsed.canonical_coin == "xyz:XYZ100"
    assert canonical_coin(dex="xyz", coin="XYZ100") == "xyz:XYZ100"


def test_custom_dex_coin_name_is_not_forced_through_crypto_suffix_rules() -> None:
    parsed = parse_coin("xyz:ABCUSDT")

    assert parsed.dex == "xyz"
    assert parsed.coin == "ABCUSDT"
    assert parsed.canonical_coin == "xyz:ABCUSDT"
    assert parse_coin("BTCUSDT").canonical_coin == "BTC"


def test_xyz_account_state_position_uses_dex_and_canonical_coin() -> None:
    state = parse_account_state(
        role=LEADER,
        address="0x" + "3" * 40,
        dex="xyz",
        clearinghouse_state={
            "marginSummary": {"accountValue": "1000"},
            "assetPositions": [{"position": {"coin": "XYZ100", "szi": "2", "positionValue": "50"}}],
        },
    )

    assert state.dex == "xyz"
    assert state.positions[0].coin == "XYZ100"
    assert state.positions[0].canonical_coin == "xyz:XYZ100"
    assert state.positions[0].notional == Decimal("50")


def test_allowed_coins_all_includes_xyz_and_custom_supports_canonical() -> None:
    assert allowed_coins_mode(leader_config(allowed_symbols=None)) == "ALL_COINS"
    assert is_coin_allowed(leader_config(allowed_symbols=None), "xyz:XYZ100") is True
    assert is_coin_allowed(leader_config(allowed_symbols=["xyz:XYZ100"]), "xyz:XYZ100") is True
    assert is_coin_allowed(leader_config(allowed_symbols=["XYZ100"]), "xyz:XYZ100") is True
    assert is_coin_allowed(leader_config(allowed_symbols=["BTC"]), "xyz:XYZ100") is False


def test_raw_allowlist_symbol_reports_ambiguity_across_enabled_dexes() -> None:
    single = allowed_coin_match_status(
        leader_config(allowed_symbols=["HYUNDAI"]),
        "xyz:HYUNDAI",
        known_canonical_coins=["xyz:HYUNDAI"],
    )
    ambiguous = allowed_coin_match_status(
        leader_config(allowed_symbols=["HYUNDAI"]),
        "xyz:HYUNDAI",
        known_canonical_coins=["xyz:HYUNDAI", "abc:HYUNDAI"],
    )
    blocked = allowed_coin_match_status(
        leader_config(allowed_symbols=None, blocked_symbols=["xyz:HYUNDAI"]),
        "xyz:HYUNDAI",
        known_canonical_coins=["xyz:HYUNDAI"],
    )

    assert single["allowed"] is True
    assert single["status"] == "RAW_SYMBOL_MATCH"
    assert ambiguous["allowed"] is False
    assert ambiguous["status"] == "AMBIGUOUS_RAW_SYMBOL"
    assert ambiguous["ambiguous_matches"] == ["abc:HYUNDAI", "xyz:HYUNDAI"]
    assert blocked["allowed"] is False
    assert blocked["status"] == "BLOCKED"


def test_market_coverage_counts_all_enabled_dex_markets_without_product_filter() -> None:
    cfg = settings(enabled_hyperliquid_dexes=",xyz,abc", enable_xyz_dex=True)
    coverage = build_hyperliquid_market_coverage(
        enabled_dexes=HyperliquidDexRegistry(cfg).enabled_dexes(),
        metas_by_dex={
            "": {"universe": [{"name": "BTC", "productType": "crypto"}]},
            "xyz": {"universe": [{"name": "HYUNDAI"}, {"name": "URNM", "productType": "etf"}]},
            "abc": {"universe": [{"name": "WEIRD-INDEX"}]},
        },
        mids_by_dex={"": {"BTC": "60000"}, "xyz": {"HYUNDAI": "300", "URNM": "70"}, "abc": {"FUTURE": "1"}},
    )

    assert coverage["enabled_dexes"] == ["", "xyz", "abc"]
    assert coverage["markets_loaded_count_by_dex"]["xyz"] == 2
    assert coverage["markets_loaded_count_by_dex"]["abc"] == 2
    assert coverage["all_coins_mode_includes_hip3_tradfi_unknown"] is True
    assert coverage["binance_mapping_required_for_hyperliquid"] is False
    assert coverage["product_type_unknown_hidden"] is False


def test_watcher_parses_hip3_user_fill_without_snapshot_execution() -> None:
    watcher = HyperliquidWatcher(
        ws_url="wss://example.invalid",
        info_client=None,  # type: ignore[arg-type]
        leader_addresses=["0xleader"],
    )
    message = {
        "channel": "userFills",
        "data": {
            "user": "0xleader",
            "fills": [{"coin": "xyz:XYZ100", "px": "10", "sz": "1", "time": 1}],
        },
    }

    fills = watcher._parse_message(json.dumps(message))
    assert len(fills) == 1
    assert fills[0].dex == "xyz"
    assert fills[0].coin == "XYZ100"
    assert fills[0].canonical_coin == "xyz:XYZ100"


def test_hyperliquid_ioc_order_supports_hip3_canonical_coin_and_reduce_only() -> None:
    order = build_hyperliquid_ioc_order(
        dex="xyz",
        coin="XYZ100",
        is_buy=False,
        quantity=Decimal("1"),
        reference_price=Decimal("10"),
        slippage_bps=100,
        reduce_only=True,
        cloid="0x" + "1" * 32,
    )

    assert order["coin"] == "xyz:XYZ100"
    assert order["order_type"] == {"limit": {"tif": "Ioc"}}
    assert order["reduce_only"] is True


def test_hyperliquid_cloid_hash_includes_dex() -> None:
    default = build_hyperliquid_cloid(
        leader_address="0xleader",
        coin="XYZ100",
        side="LONG",
        action="OPEN_OR_INCREASE",
        source_fill_id="fill",
        timestamp_ms=1,
    )
    xyz = build_hyperliquid_cloid(
        leader_address="0xleader",
        dex="xyz",
        coin="XYZ100",
        side="LONG",
        action="OPEN_OR_INCREASE",
        source_fill_id="fill",
        timestamp_ms=1,
    )

    assert default != xyz
    assert xyz.startswith("0x")
    assert len(xyz) == 34


def test_asset_id_resolves_from_dex_meta_universe() -> None:
    assert resolve_asset_id_from_meta({"universe": [{"name": "BTC"}, {"name": "XYZ100"}]}, coin="xyz:XYZ100", dex="xyz") == 1
    assert resolve_asset_id_from_meta({"universe": [{"name": "xyz:XYZ100"}]}, coin="XYZ100", dex="xyz") == 0
    assert resolve_asset_id_from_meta({"universe": [{"name": "BTC"}]}, coin="xyz:XYZ100", dex="xyz") is None
