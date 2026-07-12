from __future__ import annotations

from decimal import Decimal
from typing import Any


def small_live_start_checklist(
    *,
    trading_enabled: bool,
    hyperliquid_trading_enabled: bool,
    kill_switch: bool,
    follower: dict[str, Any],
    leaders: list[dict[str, Any]],
    hyperliquid_ready: bool,
    unknown_orders_count: int,
    allocation_mismatch: bool,
    hyperliquid_symbols: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    enabled_leaders = [item for item in leaders if (item.get("leader") or item).get("enabled", False)]
    checks = [
        _check("TRADING_ENABLED=true", trading_enabled, "TRADING_ENABLED is false"),
        _check(
            "HYPERLIQUID_TRADING_ENABLED=true",
            hyperliquid_trading_enabled,
            "Hyperliquid live trading flag is false",
        ),
        _check("kill_switch=false", not kill_switch, "Kill switch is active"),
        _check("follower accountValue loaded", bool(follower.get("account_value")), "Follower accountValue missing"),
        _check("follower withdrawable loaded", bool(follower.get("withdrawable")), "Follower withdrawable missing"),
        _check("follower state loaded", _follower_state_loaded(follower), "Follower Hyperliquid account state unavailable"),
        _check("at least one enabled leader", bool(enabled_leaders), "No enabled leader"),
        _check(
            "each enabled leader state loaded",
            all(_leader_state_loaded(item) for item in enabled_leaders),
            "One or more enabled leader states are unavailable",
        ),
        _check(
            "copy_multiplier displayed",
            all(bool((item.get("leader") or item).get("copy_multiplier")) for item in enabled_leaders),
            "copy_multiplier missing",
        ),
        _check(
            "allowed coins mode displayed",
            all(bool((item.get("leader") or item).get("allowed_coins_mode")) for item in enabled_leaders),
            "allowed coins mode missing",
        ),
        _check(
            "preferred venue displayed",
            all(bool((item.get("leader") or item).get("preferred_venue")) for item in enabled_leaders),
            "preferred venue missing",
        ),
        _check("Hyperliquid venue ready", hyperliquid_ready, "Hyperliquid venue is not ready"),
        _check("no unknown orders", unknown_orders_count == 0, "Unknown auto orders require recovery"),
        _check("no allocation mismatch", not allocation_mismatch, "Allocation mismatch present"),
        _check(
            "margin mode / effective leverage ready",
            not any(str(item.get("status", "")).upper() == "BLOCKED" for item in hyperliquid_symbols or []),
            "One or more Hyperliquid coins have blocked leverage/readiness",
        ),
        _optional_cap_check(
            "max_notional_per_trade optional cap",
            all(_positive((item.get("leader") or item).get("max_notional_per_trade")) for item in enabled_leaders),
            "No max notional cap set.",
        ),
        _optional_cap_check(
            "max_total_notional optional cap",
            all(_positive((item.get("leader") or item).get("max_total_notional")) for item in enabled_leaders),
            "No max total notional cap set.",
        ),
    ]
    for item in enabled_leaders:
        leader = item.get("leader") or item
        multiplier = _decimal_or_none(leader.get("copy_multiplier"))
        if multiplier is not None and multiplier > Decimal("0.1"):
            checks.append(
                {
                    "name": f"copy_multiplier warning for {leader.get('leader_address') or leader.get('address')}",
                    "status": "WARNING",
                    "message": "当前不是极小倍数，请确认风险。",
                }
            )
    return {
        "ready": not any(item["status"] == "BLOCKED" for item in checks),
        "checks": checks,
    }


def _check(name: str, ok: bool, message: str) -> dict[str, str]:
    return {"name": name, "status": "OK" if ok else "BLOCKED", "message": "OK" if ok else message}


def _optional_cap_check(name: str, ok: bool, message: str) -> dict[str, str]:
    return {"name": name, "status": "OK" if ok else "WARNING", "message": "OK" if ok else message}


def _positive(value: Any) -> bool:
    parsed = _decimal_or_none(value)
    return parsed is not None and parsed > 0


def _leader_state_loaded(item: dict[str, Any]) -> bool:
    if item.get("error_message"):
        return False
    if item.get("stale") is False:
        return True
    return bool(
        item.get("account_value")
        or item.get("accountValue")
        or item.get("withdrawable")
        or item.get("positions")
        or item.get("dex_states")
        or item.get("dexStates")
    )


def _follower_state_loaded(item: dict[str, Any]) -> bool:
    return bool(item.get("account_value") and item.get("withdrawable") and not item.get("error_message"))


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or str(value) == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None
