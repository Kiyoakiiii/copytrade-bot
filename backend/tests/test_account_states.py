import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.api.account_states import (
    _compact_account_state_summary,
    _compact_leader_position_payload,
    _follower_config_debug,
    _sizing_payload,
)
from app.models import LatestAccountPosition, LatestAccountState, RiskEvent
from app.services import account_state as account_state_service
from app.services.account_state import (
    AccountPositionState,
    AccountState,
    FOLLOWER,
    LEADER,
    account_state_payload,
    error_account_state,
    mask_payload,
    parse_account_state,
    save_account_state,
)
from app.services.leader_config import allowed_coins_mode, is_coin_allowed
from app.services.live_readiness import small_live_start_checklist
from app.services.risk import RiskConfig, check_risk


def raw_state() -> dict:
    return {
        "marginSummary": {
            "accountValue": "1000",
            "totalNtlPos": "300",
            "totalRawUsd": "1005",
            "totalMarginUsed": "30",
        },
        "withdrawable": "970",
        "assetPositions": [
            {
                "position": {
                    "coin": "BTC",
                    "szi": "0.01",
                    "positionValue": "100",
                    "entryPx": "9000",
                    "markPx": "10000",
                    "unrealizedPnl": "10",
                    "leverage": {"type": "isolated", "value": 10},
                    "marginUsed": "10",
                    "liquidationPx": "7000",
                }
            },
            {"position": {"coin": "HYPE", "szi": "-2", "positionValue": "200"}},
            {"position": {"coin": "PURR", "szi": "0", "positionValue": "0"}},
        ],
    }


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


class SettingsStub:
    hyperliquid_execution_network = "mainnet"
    hyperliquid_account_address = "0x" + "5" * 40
    hyperliquid_api_wallet_address = None
    hyperliquid_vault_address = None
    hyperliquid_subaccount_address = None

    def hyperliquid_follower_address_ambiguous(self) -> bool:
        return False

    def hyperliquid_signer_address(self) -> str:
        return "0x" + "5" * 40

    def hyperliquid_follower_account_address(self) -> str:
        return "0x" + "5" * 40

    def hyperliquid_signer_type(self) -> str:
        return "MAIN_WALLET"


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return self.rows

    def scalar_one_or_none(self):
        return self.rows[0] if self.rows else None


class SaveAccountStateDb:
    def __init__(self, state_row, position_rows):
        self.rows_sequence = [[state_row], list(position_rows)]
        self.added = []
        self.flushes = 0

    async def execute(self, stmt):
        rows = self.rows_sequence.pop(0) if self.rows_sequence else []
        return FakeResult(rows)

    def add(self, item):
        self.added.append(item)

    async def flush(self):
        self.flushes += 1


def test_follower_account_state_parses_account_value() -> None:
    state = parse_account_state(role=FOLLOWER, address="0x" + "1" * 40, clearinghouse_state=raw_state())
    assert state.account_value == Decimal("1000")
    assert state.withdrawable == Decimal("970")
    assert state.total_margin_used == Decimal("30")


def test_confirmed_unified_account_does_not_show_per_dex_zero_as_issue() -> None:
    state = parse_account_state(
        role=FOLLOWER,
        address="0x" + "5" * 40,
        clearinghouse_state={"marginSummary": {"accountValue": "0"}, "withdrawable": "0"},
    )
    debug = _follower_config_debug(
        SettingsStub(),
        state,
        [],
        spot_debug={"usdc_total": "399.6"},
        account_abstraction={
            "account_abstraction_mode": "UNIFIED",
            "resolved_by_dex": {
                "": {
                    "account_value_used_for_sizing": "399.6",
                    "source": "SPOT_CLEARINGHOUSE_STATE",
                    "blockers": [],
                }
            },
        },
    )
    assert debug["likely_issue"] == "OK"


def test_leader_sizing_uses_fixed_config_value_not_dynamic_balance() -> None:
    payload = _sizing_payload(
        leader_state=SimpleNamespace(account_value=Decimal("900000")),
        position={"notional": "2000", "canonical_coin": "HYPE", "side": "LONG"},
        leader=SimpleNamespace(
            fixed_account_value=Decimal("50000"),
            copy_multiplier=Decimal("0.5"),
        ),
        follower_state=SimpleNamespace(account_value=Decimal("1000")),
        allocations=[],
        leader_resolved={
            "account_value_used_for_sizing": "900000",
            "account_value_source": "CURRENT_ACCOUNT_TOTAL",
            "blockers": ["dynamic leader balance must not affect fixed sizing"],
        },
        follower_resolved={
            "account_value_used_for_sizing": "1000",
            "account_value_source": "SPOT_CLEARINGHOUSE_STATE",
            "account_abstraction_mode": "UNIFIED",
            "blockers": [],
        },
    )

    assert payload["leader_account_value_used_for_sizing"] == "50000"
    assert payload["leader_account_value_source"] == "LEADER_CONFIG_FIXED"
    assert Decimal(payload["target_notional"]) == Decimal("20")
    assert payload["error"] is None


def test_follower_positions_parse_long_short_flat() -> None:
    state = parse_account_state(role=FOLLOWER, address="0x" + "1" * 40, clearinghouse_state=raw_state())
    assert [position.side for position in state.positions] == ["LONG", "SHORT", "FLAT"]


def test_position_parser_keeps_mid_price_and_open_time() -> None:
    raw = raw_state()
    raw["assetPositions"][0]["position"]["midPx"] = "10001"
    raw["assetPositions"][0]["position"]["openedAt"] = 1_700_000_000_000
    state = parse_account_state(role=FOLLOWER, address="0x" + "1" * 40, clearinghouse_state=raw)
    position = state.positions[0]
    assert position.mark_px == Decimal("10000")
    assert position.mid_px == Decimal("10001")
    assert position.mark_px_source == "POSITION_MARK_PX"
    assert position.position_opened_at == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
    assert position.open_time_source == "POSITION_PAYLOAD"


def test_position_parser_uses_asset_ctx_mark_price() -> None:
    state = parse_account_state(
        role=LEADER,
        address="0x" + "2" * 40,
        dex="xyz",
        clearinghouse_state={
            "marginSummary": {"accountValue": "1000"},
            "assetPositions": [
                {
                    "position": {"coin": "HYUNDAI", "szi": "2", "entryPx": "370"},
                    "assetCtx": {"markPx": "374.5", "midPx": "374.4"},
                }
            ],
        },
    )
    assert state.positions[0].canonical_coin == "xyz:HYUNDAI"
    assert state.positions[0].mark_px == Decimal("374.5")
    assert state.positions[0].mid_px == Decimal("374.4")
    assert state.positions[0].mark_px_source == "POSITION_MARK_PX"


def test_position_parser_uses_price_cache_mid_when_mark_missing() -> None:
    state = parse_account_state(
        role=LEADER,
        address="0x" + "2" * 40,
        dex="xyz",
        clearinghouse_state={
            "marginSummary": {"accountValue": "1000"},
            "assetPositions": [{"position": {"coin": "HYUNDAI", "szi": "2", "entryPx": "370"}}],
        },
        price_mids={"xyz:HYUNDAI": "374.6"},
    )
    assert state.positions[0].mark_px == Decimal("374.6")
    assert state.positions[0].mid_px == Decimal("374.6")
    assert state.positions[0].mark_px_source == "PRICE_CACHE_MID"


def test_leader_account_state_parses_account_value() -> None:
    state = parse_account_state(role=LEADER, address="0x" + "2" * 40, clearinghouse_state=raw_state())
    assert state.role == LEADER
    assert state.account_value == Decimal("1000")


def test_leader_positions_include_all_coins_without_default_limit() -> None:
    state = parse_account_state(role=LEADER, address="0x" + "2" * 40, clearinghouse_state=raw_state())
    assert {position.coin for position in state.positions} == {"BTC", "HYPE", "PURR"}


def test_allowed_coins_null_marks_any_coin_copyable() -> None:
    leader = leader_config(allowed_symbols=None)
    assert is_coin_allowed(leader, "HYPE") is True
    assert allowed_coins_mode(leader) == "ALL_COINS"


def test_loaded_follower_state_stale_does_not_block_small_live_checklist() -> None:
    checklist = small_live_start_checklist(
        trading_enabled=True,
        hyperliquid_trading_enabled=True,
        kill_switch=False,
        follower={"account_value": "1", "withdrawable": "1", "stale": True},
        leaders=[],
        hyperliquid_ready=True,
        unknown_orders_count=0,
        allocation_mismatch=False,
    )
    assert any(item["name"] == "follower state loaded" and item["status"] == "OK" for item in checklist["checks"])


def test_loaded_leader_state_stale_does_not_block_small_live_checklist() -> None:
    checklist = small_live_start_checklist(
        trading_enabled=True,
        hyperliquid_trading_enabled=True,
        kill_switch=False,
        follower={"account_value": "1", "withdrawable": "1", "stale": False},
        leaders=[{"account_value": "1000", "stale": True, "leader": {"enabled": True, "copy_multiplier": "0.1", "allowed_coins_mode": "ALL_COINS", "preferred_venue": "HYPERLIQUID", "max_notional_per_trade": "5", "max_total_notional": "10"}}],
        hyperliquid_ready=True,
        unknown_orders_count=0,
        allocation_mismatch=False,
    )
    assert any(item["name"] == "each enabled leader state loaded" and item["status"] == "OK" for item in checklist["checks"])
    assert checklist["ready"] is True


def test_follower_state_loaded_check_is_ok() -> None:
    checklist = small_live_start_checklist(
        trading_enabled=True,
        hyperliquid_trading_enabled=True,
        kill_switch=False,
        follower={"account_value": "1", "withdrawable": "1", "stale": False},
        leaders=[],
        hyperliquid_ready=True,
        unknown_orders_count=0,
        allocation_mismatch=False,
    )
    assert any(item["name"] == "follower state loaded" and item["status"] == "OK" for item in checklist["checks"])


def test_enabled_leader_without_state_blocks_checklist() -> None:
    checklist = small_live_start_checklist(
        trading_enabled=True,
        hyperliquid_trading_enabled=True,
        kill_switch=False,
        follower={"account_value": "1", "withdrawable": "1", "stale": False},
        leaders=[{"stale": True, "error_message": "account state unavailable", "leader": {"enabled": True, "copy_multiplier": "0.1", "allowed_coins_mode": "ALL_COINS", "preferred_venue": "HYPERLIQUID", "max_notional_per_trade": "5", "max_total_notional": "10"}}],
        hyperliquid_ready=True,
        unknown_orders_count=0,
        allocation_mismatch=False,
    )
    assert checklist["ready"] is False


def test_dashboard_style_payload_does_not_return_private_key() -> None:
    payload = account_state_payload(None, [], extra={"private_key_configured": True})
    assert "hyperliquid_private_key" not in payload


def test_account_state_payload_does_not_return_private_key() -> None:
    state = parse_account_state(
        role=FOLLOWER,
        address="0x" + "1" * 40,
        clearinghouse_state={"marginSummary": {"accountValue": "1"}, "private_key": "secret"},
    )
    assert "private_key" not in {key for key, value in state.raw_payload_masked.items() if value != "***"}


def test_raw_payload_is_masked() -> None:
    assert mask_payload({"api_secret": "abc", "nested": {"signature": "sig"}}) == {
        "api_secret": "***",
        "nested": {"signature": "***"},
    }


def test_state_refresh_failure_returns_error_message() -> None:
    state = error_account_state(
        role=FOLLOWER,
        address="0x" + "1" * 40,
        account_label="follower",
        error_message="network timeout",
    )
    assert state.error_message == "network timeout"
    assert state.positions == []


def test_missing_position_requires_second_snapshot_before_close() -> None:
    now = datetime(2026, 6, 10, 5, 39, 20, tzinfo=timezone.utc)
    row = LatestAccountPosition(
        role=FOLLOWER,
        address="0x" + "1" * 40,
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("12.06"),
        notional=Decimal("712.38"),
        active=True,
        status="OPEN",
    )

    closed = account_state_service._mark_or_close_missing_position(row, now=now)

    assert closed is False
    assert row.active is True
    assert row.status == "MISSING"
    assert row.closed_at is None

    later = now + timedelta(seconds=1)
    closed = account_state_service._mark_or_close_missing_position(row, now=later)

    assert closed is True
    assert row.active is False
    assert row.status == "CLOSED"
    assert row.closed_at == later


def test_position_monitoring_marks_are_throttled_but_size_changes_are_immediate() -> None:
    now = datetime(2026, 8, 13, 12, 0, 1, tzinfo=timezone.utc)
    row = LatestAccountPosition(
        role=FOLLOWER,
        address="0x" + "1" * 40,
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("2"),
        entry_px=Decimal("40"),
        mark_px=Decimal("41"),
        mark_px_source="POSITION_MARK_PX",
        leverage=Decimal("10"),
        active=True,
        status="OPEN",
        last_update_at=now,
    )
    state = AccountState(
        role=FOLLOWER,
        address=row.address,
        dex="",
        dex_display_name="Hyperliquid",
        account_label="Follower",
        account_value=Decimal("1000"),
        withdrawable=Decimal("900"),
        total_ntl_pos=Decimal("84"),
        total_raw_usd=None,
        total_margin_used=Decimal("8.4"),
        positions=[],
        raw_payload_masked={},
        source="FOLLOWER_CLEARINGHOUSE_WS",
        updated_at=now + timedelta(seconds=1),
        error_message=None,
    )
    mark_only = AccountPositionState(
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        raw_coin="HYPE",
        product_type="PERP",
        side="LONG",
        size=Decimal("2"),
        notional=Decimal("84"),
        entry_px=Decimal("40"),
        mark_px=Decimal("42"),
        mid_px=Decimal("42"),
        mark_px_source="POSITION_MARK_PX",
        position_opened_at=None,
        open_time_source=None,
        unrealized_pnl=Decimal("4"),
        leverage=Decimal("10"),
        margin_used=Decimal("8.4"),
        liquidation_px=Decimal("20"),
        raw_payload_masked={},
    )

    account_state_service._update_position_row(
        row,
        state=state,
        position=mark_only,
        position_opened_at=None,
        open_time_source=None,
    )
    assert row.mark_px == Decimal("41")
    assert row.last_update_at == now

    changed_size = replace(mark_only, size=Decimal("3"), notional=Decimal("126"))
    account_state_service._update_position_row(
        row,
        state=state,
        position=changed_size,
        position_opened_at=None,
        open_time_source=None,
    )
    assert row.size == Decimal("3")
    assert row.mark_px == Decimal("42")
    assert row.last_update_at == state.updated_at


def test_save_account_state_closes_duplicate_active_position_rows() -> None:
    now = datetime(2026, 7, 9, 8, 30, tzinfo=timezone.utc)
    state_row = LatestAccountState(
        id=1,
        role=FOLLOWER,
        address=("0x" + "1" * 40).lower(),
        dex="",
        last_update_at=now - timedelta(seconds=1),
    )
    actual_row = LatestAccountPosition(
        id=10,
        account_state_id=1,
        role=FOLLOWER,
        address=("0x" + "1" * 40).lower(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("2"),
        notional=Decimal("200"),
        mark_px_source="POSITION_MARK_PX",
        active=True,
        status="OPEN",
        last_update_at=now - timedelta(seconds=2),
    )
    projection_duplicate = LatestAccountPosition(
        id=11,
        account_state_id=1,
        role=FOLLOWER,
        address=("0x" + "1" * 40).lower(),
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        side="LONG",
        size=Decimal("2"),
        notional=Decimal("200"),
        mark_px_source="LOCAL_FILL_PROJECTION",
        active=True,
        status="OPEN",
        last_update_at=now,
    )
    snapshot = AccountState(
        role=FOLLOWER,
        address=("0x" + "1" * 40).lower(),
        dex="",
        dex_display_name="Hyperliquid",
        account_label="follower",
        account_value=Decimal("1000"),
        withdrawable=Decimal("900"),
        total_ntl_pos=Decimal("300"),
        total_raw_usd=Decimal("1000"),
        total_margin_used=Decimal("30"),
        positions=[
            AccountPositionState(
                coin="HYPE",
                dex="",
                canonical_coin="HYPE",
                raw_coin="HYPE",
                product_type="perp",
                side="LONG",
                size=Decimal("3"),
                notional=Decimal("300"),
                entry_px=Decimal("100"),
                mark_px=Decimal("100"),
                mid_px=Decimal("100"),
                mark_px_source="POSITION_MARK_PX",
                position_opened_at=None,
                open_time_source=None,
                unrealized_pnl=None,
                leverage=None,
                margin_used=None,
                liquidation_px=None,
                raw_payload_masked={},
            )
        ],
        raw_payload_masked={},
        source="test",
        updated_at=now,
    )
    db = SaveAccountStateDb(state_row, [actual_row, projection_duplicate])

    saved = asyncio.run(save_account_state(db, snapshot))

    assert saved is state_row
    assert actual_row.active is True
    assert actual_row.size == Decimal("3")
    assert actual_row.mark_px_source == "POSITION_MARK_PX"
    assert projection_duplicate.active is False
    assert projection_duplicate.status == "CLOSED"
    events = [item for item in db.added if isinstance(item, RiskEvent)]
    assert [event.event_type for event in events] == ["ACCOUNT_POSITION_DUPLICATE_ACTIVE_ROW_CLOSED"]


def test_save_account_state_ignores_out_of_order_snapshot() -> None:
    stored_at = datetime(2026, 7, 9, 8, 30, 2, tzinfo=timezone.utc)
    state_row = LatestAccountState(
        id=1,
        role=FOLLOWER,
        address=("0x" + "1" * 40).lower(),
        dex="xyz",
        account_value=Decimal("1000"),
        source="newer",
        last_update_at=stored_at,
    )
    active_position = LatestAccountPosition(
        id=10,
        account_state_id=1,
        role=FOLLOWER,
        address=("0x" + "1" * 40).lower(),
        dex="xyz",
        coin="SKHX",
        canonical_coin="xyz:SKHX",
        side="SHORT",
        size=Decimal("2.041"),
        active=True,
        status="OPEN",
        last_update_at=stored_at,
    )
    stale_snapshot = AccountState(
        role=FOLLOWER,
        address=("0x" + "1" * 40).lower(),
        dex="xyz",
        dex_display_name="XYZ",
        account_label="follower",
        account_value=Decimal("900"),
        withdrawable=Decimal("800"),
        total_ntl_pos=Decimal("0"),
        total_raw_usd=Decimal("900"),
        total_margin_used=Decimal("0"),
        positions=[],
        raw_payload_masked={},
        source="older",
        updated_at=stored_at - timedelta(seconds=1),
    )
    db = SaveAccountStateDb(state_row, [active_position])

    saved = asyncio.run(save_account_state(db, stale_snapshot))

    assert saved is state_row
    assert state_row.account_value == Decimal("1000")
    assert state_row.source == "newer"
    assert active_position.active is True
    assert len(db.rows_sequence) == 1
    events = [item for item in db.added if isinstance(item, RiskEvent)]
    assert [event.event_type for event in events] == ["ACCOUNT_STATE_OUT_OF_ORDER_SNAPSHOT_IGNORED"]


def test_pretrade_checklist_blocks_open_when_follower_state_stale() -> None:
    decision = check_risk(
        RiskConfig(follower_account_state_fresh=False),
        symbol="BTC",
        proposed_notional=Decimal("10"),
    )
    assert decision.allowed is False
    assert "follower account state is stale" in decision.reasons


def test_pretrade_checklist_blocks_open_when_leader_state_stale() -> None:
    decision = check_risk(
        RiskConfig(leader_account_state_fresh=False),
        symbol="BTC",
        proposed_notional=Decimal("10"),
    )
    assert decision.allowed is False
    assert "leader account state is stale" in decision.reasons


def test_close_reduce_intent_allows_stale_state_by_rule() -> None:
    decision = check_risk(
        RiskConfig(
            is_close_intent=True,
            follower_account_state_fresh=False,
            leader_account_state_fresh=False,
        ),
        symbol="BTC",
        proposed_notional=Decimal("10"),
    )
    assert decision.allowed is True


def test_small_live_checklist_warns_missing_max_notional_without_blocking() -> None:
    checklist = small_live_start_checklist(
        trading_enabled=True,
        hyperliquid_trading_enabled=True,
        kill_switch=False,
        follower={"account_value": "1", "withdrawable": "1", "stale": False},
        leaders=[{"stale": False, "leader": {"enabled": True, "copy_multiplier": "0.1", "allowed_coins_mode": "ALL_COINS", "preferred_venue": "HYPERLIQUID", "max_notional_per_trade": None, "max_total_notional": "10"}}],
        hyperliquid_ready=True,
        unknown_orders_count=0,
        allocation_mismatch=False,
    )
    assert any(
        item["name"] == "max_notional_per_trade optional cap" and item["status"] == "WARNING"
        for item in checklist["checks"]
    )
    assert checklist["ready"] is True


def test_copy_multiplier_point_one_has_no_warning() -> None:
    checklist = small_live_start_checklist(
        trading_enabled=True,
        hyperliquid_trading_enabled=True,
        kill_switch=False,
        follower={"account_value": "1", "withdrawable": "1", "stale": False},
        leaders=[{"stale": False, "leader": {"enabled": True, "copy_multiplier": "0.1", "allowed_coins_mode": "ALL_COINS", "preferred_venue": "HYPERLIQUID", "max_notional_per_trade": "5", "max_total_notional": "10"}}],
        hyperliquid_ready=True,
        unknown_orders_count=0,
        allocation_mismatch=False,
    )
    assert not any(item["status"] == "WARNING" for item in checklist["checks"])


def test_copy_multiplier_above_point_one_warns() -> None:
    checklist = small_live_start_checklist(
        trading_enabled=True,
        hyperliquid_trading_enabled=True,
        kill_switch=False,
        follower={"account_value": "1", "withdrawable": "1", "stale": False},
        leaders=[{"stale": False, "leader": {"enabled": True, "copy_multiplier": "0.2", "allowed_coins_mode": "ALL_COINS", "preferred_venue": "HYPERLIQUID", "max_notional_per_trade": "5", "max_total_notional": "10"}}],
        hyperliquid_ready=True,
        unknown_orders_count=0,
        allocation_mismatch=False,
    )
    assert any(item["status"] == "WARNING" for item in checklist["checks"])


def test_all_coins_display_is_not_btc_eth_sol_default() -> None:
    leader = leader_config(allowed_symbols=None)
    assert allowed_coins_mode(leader) == "ALL_COINS"
    assert leader.allowed_symbols is None


def test_account_state_payload_marks_old_data_stale() -> None:
    old = datetime.now(timezone.utc) - timedelta(seconds=11)
    state = type(
        "State",
        (),
        {
            "role": FOLLOWER,
            "address": "0x" + "1" * 40,
            "account_label": "follower",
            "account_value": Decimal("1"),
            "withdrawable": Decimal("1"),
            "total_ntl_pos": Decimal("0"),
            "total_raw_usd": Decimal("1"),
            "total_margin_used": Decimal("0"),
            "last_update_at": old,
            "source": "info_endpoint",
            "error_message": None,
        },
    )()
    payload = account_state_payload(state, [], stale_seconds=10)
    assert payload["stale"] is True


def test_account_state_payload_returns_zero_values_and_camelcase_aliases() -> None:
    now = datetime.now(timezone.utc)
    state = SimpleNamespace(
        role=FOLLOWER,
        address="0x" + "1" * 40,
        account_label="follower",
        account_value=Decimal("0"),
        withdrawable=Decimal("0"),
        total_ntl_pos=Decimal("0"),
        total_raw_usd=Decimal("0"),
        total_margin_used=Decimal("0"),
        last_update_at=now,
        source="info_endpoint",
        error_message=None,
    )
    position = SimpleNamespace(
        role=FOLLOWER,
        address="0x" + "1" * 40,
        dex="",
        coin="HYPE",
        canonical_coin="HYPE",
        raw_coin="HYPE",
        product_type="perp",
        side="FLAT",
        size=Decimal("0"),
        notional=Decimal("0"),
        entry_px=Decimal("0"),
        mark_px=Decimal("0"),
        mid_px=Decimal("0"),
        mark_px_source="TEST",
        first_seen_at=now,
        position_opened_at=None,
        open_time_source="FIRST_SEEN",
        last_update_at=now,
        active=True,
        status="OPEN",
        closed_at=None,
        unrealized_pnl=Decimal("0"),
        leverage=Decimal("10"),
        margin_used=Decimal("0"),
        liquidation_px=None,
        raw_payload_masked={"leverage": {"type": "isolated", "value": 10}},
    )
    payload = account_state_payload(state, [position], stale_seconds=10, now=now)
    assert payload["account_value"] == "0"
    assert payload["accountValue"] == "0"
    assert payload["withdrawable"] == "0"
    assert payload["totalNtlPos"] == "0"
    assert payload["totalRawUsd"] == "0"
    assert payload["totalMarginUsed"] == "0"
    assert payload["positions"][0]["entryPx"] == "0"
    assert payload["positions"][0]["markPx"] == "0"
    assert payload["positions"][0]["midPx"] == "0"
    assert payload["positions"][0]["marginUsed"] == "0"
    assert payload["positions"][0]["marginMode"] == "ISOLATED"


def test_compact_leader_overview_omits_rich_duplicate_payloads() -> None:
    now = datetime.now(timezone.utc)
    state = SimpleNamespace(
        role=LEADER,
        address="0x" + "2" * 40,
        dex="xyz",
        dex_display_name="XYZ",
        account_label="leader",
        account_value=Decimal("1000"),
        withdrawable=Decimal("900"),
        total_ntl_pos=Decimal("100"),
        total_raw_usd=Decimal("1000"),
        total_margin_used=Decimal("10"),
        last_update_at=now,
        source="test",
        error_message=None,
    )
    open_position = SimpleNamespace(active=True)
    closed_position = SimpleNamespace(active=False)

    payload = _compact_account_state_summary(
        state,
        [open_position, closed_position],
        stale_seconds=10,
        include_closed=False,
    )

    assert payload["position_count"] == 1
    assert payload["positions"] == [{}]
    assert "dexStates" not in payload
    assert "account_abstraction" not in payload
    assert "accountAbstraction" not in payload


def test_compact_leader_position_keeps_display_fields_without_raw_payload() -> None:
    payload = _compact_leader_position_payload(
        {
            "coin": "HYPE",
            "canonical_coin": "HYPE",
            "side": "LONG",
            "size": "1",
            "copyable": True,
            "baseline_status": "COPY_ALLOWED",
            "last_copy_order_display_status": "LAST_ORDER_FILLED",
            "raw_position_payload": {"large": "debug"},
            "rawPositionPayload": {"large": "debug"},
            "account_abstraction": {"large": "debug"},
        }
    )

    assert payload["canonical_coin"] == "HYPE"
    assert payload["copyable"] is True
    assert payload["last_copy_order_display_status"] == "LAST_ORDER_FILLED"
    assert "raw_position_payload" not in payload
    assert "rawPositionPayload" not in payload
    assert "account_abstraction" not in payload


def test_account_state_payload_hides_closed_positions_by_default() -> None:
    now = datetime.now(timezone.utc)
    state = SimpleNamespace(
        role=FOLLOWER,
        address="0x" + "1" * 40,
        dex="",
        dex_display_name="Hyperliquid",
        account_label="follower",
        account_value=Decimal("1"),
        withdrawable=Decimal("1"),
        total_ntl_pos=Decimal("0"),
        total_raw_usd=Decimal("1"),
        total_margin_used=Decimal("0"),
        last_update_at=now,
        source="info_endpoint",
        error_message=None,
    )
    closed = SimpleNamespace(
        role=FOLLOWER,
        address="0x" + "1" * 40,
        dex="xyz",
        coin="URNM",
        canonical_coin="xyz:URNM",
        raw_coin="xyz:URNM",
        product_type="perp",
        side="LONG",
        size=Decimal("1"),
        notional=Decimal("100"),
        entry_px=Decimal("100"),
        mark_px=Decimal("100"),
        mid_px=Decimal("100"),
        mark_px_source="TEST",
        first_seen_at=now,
        position_opened_at=None,
        open_time_source="FIRST_SEEN",
        last_update_at=now,
        active=False,
        status="CLOSED",
        closed_at=now,
        unrealized_pnl=Decimal("0"),
        leverage=None,
        margin_used=None,
        liquidation_px=None,
        raw_payload_masked={},
    )
    assert account_state_payload(state, [closed], now=now)["positions"] == []
    payload = account_state_payload(state, [closed], now=now, include_closed=True)
    assert payload["positions"][0]["status"] == "CLOSED"
    assert payload["positions"][0]["closedAt"] == now.isoformat()


def test_frontend_operational_pages_use_low_frequency_db_snapshots() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "frontend/src/app").exists()), None)
    if root is None:
        pytest.skip("frontend source is not included in this backend-only test image")
    for path in [
        root / "frontend/src/app/dashboard/page.tsx",
        root / "frontend/src/app/leaders/[id]/page.tsx",
        root / "frontend/src/app/leaders/page.tsx",
    ]:
        text = path.read_text()
        assert "useDashboardStream" not in text
        assert "useRealtimeFallbackPolling" not in text
        assert "document.visibilityState" in text
        assert "setInterval" in text
    leaders_page = (root / "frontend/src/app/leaders/page.tsx").read_text()
    assert "/account-states/leaders?compact=true" in leaders_page
    assert "LEADER_OVERVIEW_REFRESH_INTERVAL_MS = 60_000" in leaders_page
