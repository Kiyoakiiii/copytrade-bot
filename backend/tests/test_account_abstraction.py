from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from app.models import ExecutionOrder
from app.services.account_abstraction import (
    AccountAbstractionService,
    MODE_INFERRED_UNIFIED,
    MODE_UNIFIED,
    MODE_UNKNOWN,
    SOURCE_ACCOUNT_TOTAL,
    SOURCE_CLEARINGHOUSE,
    SOURCE_PORTFOLIO,
    SOURCE_SPOT,
    available_collateral_sufficient,
    build_account_abstraction_snapshot,
    required_initial_margin,
    resolve_account_value_for_sizing,
    spot_account_value_from_response,
)
from app.services.calculator import calculate_target_notional_by_account_ratio


def cfg(**overrides):
    values = {
        "account_value_mode": "auto",
        "require_confirmed_account_abstraction_for_live": True,
        "allow_unified_account_for_live": True,
        "allow_portfolio_margin_for_live": False,
        "unified_account_collateral_source": "spot_or_portfolio",
        "default_collateral_token": "USDC",
        "account_value_reference_dexes": ",xyz",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def clearing(account_value: str, withdrawable: str = "0") -> dict:
    return {
        "marginSummary": {
            "accountValue": account_value,
            "totalNtlPos": "0",
            "totalMarginUsed": "0",
        },
        "withdrawable": withdrawable,
        "assetPositions": [],
    }


def spot(total: str, hold: str = "0", coin: str = "USDC") -> dict:
    return {"balances": [{"coin": coin, "total": total, "hold": hold}]}


def snapshot(
    *,
    role="FOLLOWER",
    user_abstraction=None,
    portfolio_state=None,
    spot_state=None,
    clearing_by_dex=None,
    settings=None,
):
    return build_account_abstraction_snapshot(
        role=role,
        address="0x" + "5" * 40,
        user_abstraction=user_abstraction,
        portfolio_state=portfolio_state,
        spot_state=spot_state,
        clearinghouse_by_dex=clearing_by_dex if clearing_by_dex is not None else {"": clearing("0")},
        settings=settings or cfg(),
    )


def test_unified_zero_clearinghouse_uses_spot_usdc_for_follower_account_value() -> None:
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"))
    result = resolve_account_value_for_sizing(snap, "", cfg())
    assert result.account_value == Decimal("399.6")
    assert result.source == SOURCE_SPOT


def test_confirmed_unified_refresh_skips_irrelevant_portfolio_and_market_calls() -> None:
    class ProbeInfoClient:
        def __init__(self):
            self.post_types = []
            self.clearing_dexes = []

        async def post_info(self, payload):
            self.post_types.append(payload["type"])
            if payload["type"] == "userAbstraction":
                return "unifiedAccount"
            raise AssertionError(f"unexpected info request: {payload['type']}")

        async def spot_clearinghouse_state(self, address):
            return spot("5000")

        async def clearinghouse_state(self, address, dex=""):
            self.clearing_dexes.append(dex)
            return clearing("0")

    client = ProbeInfoClient()
    snap = asyncio.run(
        AccountAbstractionService(client, cfg()).fetch_snapshot(
            role="FOLLOWER",
            address="0x" + "5" * 40,
            dexes=["", "xyz", "hyna", "cash"],
        )
    )

    assert client.post_types == ["userAbstraction"]
    assert client.clearing_dexes == ["", "xyz"]
    result = resolve_account_value_for_sizing(snap, "hyna", cfg())
    assert result.account_value == Decimal("5000")
    assert result.source == SOURCE_SPOT


def test_confirmed_unified_fast_refresh_only_reads_spot_balance() -> None:
    class FastProbeInfoClient:
        def __init__(self):
            self.spot_calls = 0

        async def post_info(self, payload):
            raise AssertionError(f"unexpected info request: {payload['type']}")

        async def spot_clearinghouse_state(self, address):
            self.spot_calls += 1
            return spot("5001.25", hold="10")

        async def clearinghouse_state(self, address, dex=""):
            raise AssertionError("fast unified refresh must not read clearinghouse state")

    client = FastProbeInfoClient()
    snap = asyncio.run(
        AccountAbstractionService(client, cfg()).fetch_snapshot(
            role="FOLLOWER",
            address="0x" + "5" * 40,
            dexes=["", "xyz", "hyna"],
            confirmed_unified_fast=True,
        )
    )

    assert client.spot_calls == 1
    result = resolve_account_value_for_sizing(snap, "hyna", cfg())
    assert result.account_value == Decimal("5001.25")
    assert result.withdrawable_or_available == Decimal("4991.25")


def test_unified_zero_clearinghouse_does_not_block_live_when_confirmed() -> None:
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"))
    result = resolve_account_value_for_sizing(snap, "", cfg())
    assert result.blockers == []
    assert result.mode == MODE_UNIFIED


def test_standard_mode_uses_clearinghouse_account_value() -> None:
    snap = snapshot(user_abstraction={"mode": "standard"}, clearing_by_dex={"": clearing("1200", "1100")})
    result = resolve_account_value_for_sizing(snap, "", cfg())
    assert result.account_value == Decimal("1200")
    assert result.withdrawable_or_available == Decimal("1100")
    assert result.source == SOURCE_CLEARINGHOUSE


def test_dex_abstraction_leader_uses_current_account_total_for_sizing() -> None:
    snap = snapshot(
        role="LEADER",
        user_abstraction="dexAbstraction",
        spot_state=spot("10181.50"),
        clearing_by_dex={
            "": clearing("12700.471209", "12700.471209"),
            "xyz": clearing("11068.702689", "74.833333"),
            "cash": clearing("735.120421", "0"),
        },
    )
    result = resolve_account_value_for_sizing(snap, "cash", cfg())
    assert result.account_value == Decimal("33950.673898")
    assert result.source == SOURCE_ACCOUNT_TOTAL
    assert result.mode == "DEX_ABSTRACTION"


def test_spot_account_value_uses_spot_market_mids() -> None:
    spot_state = {
        "balances": [
            {"coin": "USDC", "token": 0, "total": "1.5", "hold": "0"},
            {"coin": "USDE", "token": 235, "total": "2", "hold": "0"},
        ]
    }
    spot_meta = [{"universe": [{"tokens": [235, 0], "name": "@150", "index": 150}]}]
    value = spot_account_value_from_response(spot_state, spot_meta_and_asset_ctxs=spot_meta, all_mids={"@150": "0.9993"})
    assert value == Decimal("3.4986")


def test_portfolio_mode_prefers_portfolio_state() -> None:
    settings = cfg(account_value_mode="portfolio", allow_portfolio_margin_for_live=True)
    snap = snapshot(
        portfolio_state={"accountValue": "500.25"},
        spot_state=spot("399.6"),
        settings=settings,
    )
    result = resolve_account_value_for_sizing(snap, "", settings)
    assert result.account_value == Decimal("500.25")
    assert result.source == SOURCE_PORTFOLIO


def test_unknown_mode_with_spot_blocks_live_but_exposes_dry_run_value() -> None:
    snap = snapshot(spot_state=spot("399.6"), clearing_by_dex={}, settings=cfg())
    result = resolve_account_value_for_sizing(snap, "", cfg())
    assert result.mode == MODE_UNKNOWN
    assert result.account_value == Decimal("399.6")
    assert result.blockers


def test_leader_unified_uses_spot_or_portfolio_not_zero_clearinghouse() -> None:
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("2500"), clearing_by_dex={"xyz": clearing("0")})
    result = resolve_account_value_for_sizing(snap, "xyz", cfg())
    assert result.account_value == Decimal("2500")
    assert result.source == SOURCE_SPOT


def test_account_ratio_uses_resolved_account_values() -> None:
    target = calculate_target_notional_by_account_ratio(
        leader_account_value=Decimal("1000"),
        leader_position_notional=Decimal("100"),
        follower_account_value=Decimal("399.6"),
        copy_multiplier=Decimal("0.1"),
    )
    assert target == Decimal("3.99600000")


def test_account_ratio_example_399_6_10_percent_multiplier_point_1() -> None:
    assert (
        Decimal("399.6")
        * abs(Decimal("100") / Decimal("1000"))
        * Decimal("0.1")
    ) == Decimal("3.996")


def test_required_initial_margin_is_delta_notional_over_effective_leverage() -> None:
    assert required_initial_margin(Decimal("39.96"), 10) == Decimal("3.99600000")


def test_available_collateral_insufficient_blocks_open() -> None:
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("3"))
    result = resolve_account_value_for_sizing(snap, "", cfg())
    ok, required = available_collateral_sufficient(
        result,
        target_delta_notional=Decimal("100"),
        effective_leverage=10,
    )
    assert ok is False
    assert required == Decimal("10.00000000")


def test_xyz_position_with_unified_usdc_can_compute_target() -> None:
    follower = resolve_account_value_for_sizing(
        snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"), clearing_by_dex={"xyz": clearing("0")}),
        "xyz",
        cfg(),
    )
    leader = resolve_account_value_for_sizing(
        snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("1000"), clearing_by_dex={"xyz": clearing("0")}),
        "xyz",
        cfg(),
    )
    target = calculate_target_notional_by_account_ratio(
        leader_account_value=leader.account_value,
        leader_position_notional=Decimal("100"),
        follower_account_value=follower.account_value,
        copy_multiplier=Decimal("0.1"),
    )
    assert target == Decimal("3.99600000")


def test_xyz_zero_clearinghouse_unified_spot_does_not_directly_block() -> None:
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"), clearing_by_dex={"xyz": clearing("0")})
    result = resolve_account_value_for_sizing(snap, "xyz", cfg())
    assert "clearinghouseState accountValue is zero" not in "; ".join(result.blockers)


def test_non_usdc_hip3_collateral_unknown_blocks_live() -> None:
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"), clearing_by_dex={"xyz": clearing("0")})
    result = resolve_account_value_for_sizing(snap, "xyz", cfg(), collateral_token="XYZCOL")
    assert any("collateral token XYZCOL" in blocker for blocker in result.blockers)


def test_preflight_payload_can_display_account_abstraction_mode() -> None:
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"))
    assert snap.as_dict()["account_abstraction_mode"] == MODE_UNIFIED


def test_api_payload_does_not_expose_private_key() -> None:
    snap = snapshot(user_abstraction={"mode": "unified", "private_key": "secret"}, spot_state=spot("399.6"))
    payload = snap.as_dict()
    assert "private_key" not in str(payload).lower()


def test_execution_order_has_account_value_source_columns() -> None:
    assert hasattr(ExecutionOrder, "leader_account_value_source")
    assert hasattr(ExecutionOrder, "follower_account_value_source")
    assert hasattr(ExecutionOrder, "leader_account_abstraction_mode")
    assert hasattr(ExecutionOrder, "follower_account_abstraction_mode")


def test_clearinghouse_zero_no_longer_means_no_balance_for_confirmed_unified() -> None:
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"), clearing_by_dex={"": clearing("0")})
    result = resolve_account_value_for_sizing(snap, "", cfg())
    assert result.account_value == Decimal("399.6")
    assert "no balance" not in "; ".join(result.warnings + result.blockers).lower()


def test_standard_only_blocks_unified_account_live() -> None:
    settings = cfg(account_value_mode="standard_only")
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"), settings=settings)
    result = resolve_account_value_for_sizing(snap, "", settings)
    assert any("standard_only" in blocker for blocker in result.blockers)


def test_allow_unified_false_blocks_unified_account_live() -> None:
    settings = cfg(allow_unified_account_for_live=False)
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"), settings=settings)
    result = resolve_account_value_for_sizing(snap, "", settings)
    assert "ALLOW_UNIFIED_ACCOUNT_FOR_LIVE=false" in result.blockers


def test_confirmed_unified_spot_positive_ready_is_not_blocked_by_balance_zero() -> None:
    snap = snapshot(user_abstraction={"mode": "unified"}, spot_state=spot("399.6"), clearing_by_dex={"": clearing("0")})
    result = resolve_account_value_for_sizing(snap, "", cfg())
    assert result.account_value == Decimal("399.6")
    assert result.blockers == []


def test_inferred_unified_is_marked_when_user_abstraction_unavailable() -> None:
    snap = snapshot(spot_state=spot("399.6"), clearing_by_dex={"": clearing("0"), "xyz": clearing("0")})
    assert snap.mode == MODE_INFERRED_UNIFIED
    assert snap.inference is True
