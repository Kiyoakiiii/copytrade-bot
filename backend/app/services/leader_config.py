from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from app.models import LeaderConfig
from app.services.hyperliquid_dex import canonical_coin as dex_canonical_coin
from app.services.hyperliquid_dex import parse_coin

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
ALL_COINS = "ALL_COINS"
CUSTOM_LIST = "CUSTOM_LIST"


def canonical_coin(value: str) -> str:
    parsed = parse_coin(value)
    return parsed.canonical_coin


def normalize_coin_list(values: Iterable[str] | None) -> list[str]:
    if values is None:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        coin = canonical_coin(value)
        if not coin or coin in seen:
            continue
        seen.add(coin)
        normalized.append(coin)
    return normalized


def normalize_allowed_symbols_for_storage(values: Iterable[str] | None) -> list[str] | None:
    normalized = normalize_coin_list(values)
    return normalized or None


def normalize_blocked_symbols_for_storage(values: Iterable[str] | None) -> list[str]:
    return normalize_coin_list(values)


def normalize_leader_address(value: str) -> str:
    return str(value or "").strip().lower()


def is_leader_deleted(leader_config: Any) -> bool:
    return getattr(leader_config, "deleted_at", None) is not None


def allowed_coins_mode(leader_config: Any) -> str:
    return CUSTOM_LIST if normalize_coin_list(getattr(leader_config, "allowed_symbols", None)) else ALL_COINS


def is_coin_allowed(leader_config: Any, coin: str) -> bool:
    if not bool(getattr(leader_config, "enabled", False)) or is_leader_deleted(leader_config):
        return False

    normalized_coin = canonical_coin(coin)
    if not normalized_coin:
        return False

    blocked = set(normalize_coin_list(getattr(leader_config, "blocked_symbols", None)))
    parsed = parse_coin(normalized_coin)
    raw_coin = parsed.coin
    if normalized_coin in blocked or raw_coin in blocked or dex_canonical_coin(dex="", coin=raw_coin) in blocked:
        return False

    allowed = set(normalize_coin_list(getattr(leader_config, "allowed_symbols", None)))
    if not allowed:
        return True
    return normalized_coin in allowed or raw_coin in allowed or dex_canonical_coin(dex="", coin=raw_coin) in allowed


def allowed_coin_match_status(
    leader_config: Any,
    coin: str,
    *,
    known_canonical_coins: Iterable[str] | None = None,
) -> dict[str, Any]:
    normalized_coin = canonical_coin(coin)
    parsed = parse_coin(normalized_coin)
    raw_coin = parsed.coin
    blocked = set(normalize_coin_list(getattr(leader_config, "blocked_symbols", None)))
    if normalized_coin in blocked or raw_coin in blocked or dex_canonical_coin(dex="", coin=raw_coin) in blocked:
        return {
            "allowed": False,
            "status": "BLOCKED",
            "warning": None,
            "matched_coin": normalized_coin,
            "ambiguous_matches": [],
        }
    allowed = set(normalize_coin_list(getattr(leader_config, "allowed_symbols", None)))
    if not allowed:
        return {
            "allowed": is_coin_allowed(leader_config, normalized_coin),
            "status": ALL_COINS,
            "warning": None,
            "matched_coin": normalized_coin,
            "ambiguous_matches": [],
        }
    if normalized_coin in allowed:
        return {
            "allowed": is_coin_allowed(leader_config, normalized_coin),
            "status": "EXACT_CANONICAL",
            "warning": None,
            "matched_coin": normalized_coin,
            "ambiguous_matches": [],
        }
    raw_allowed = raw_coin in allowed or dex_canonical_coin(dex="", coin=raw_coin) in allowed
    if raw_allowed:
        matches = sorted(
            {
                canonical_coin(item)
                for item in (known_canonical_coins or [])
                if parse_coin(canonical_coin(item)).coin == raw_coin
            }
        )
        if len(matches) > 1:
            return {
                "allowed": False,
                "status": "AMBIGUOUS_RAW_SYMBOL",
                "warning": f"{raw_coin} matches multiple enabled dex markets; use canonical dex:coin",
                "matched_coin": raw_coin,
                "ambiguous_matches": matches,
            }
        return {
            "allowed": is_coin_allowed(leader_config, normalized_coin),
            "status": "RAW_SYMBOL_MATCH",
            "warning": None,
            "matched_coin": matches[0] if matches else raw_coin,
            "ambiguous_matches": matches,
        }
    return {
        "allowed": False,
        "status": "NOT_IN_ALLOWLIST",
        "warning": None,
        "matched_coin": normalized_coin,
        "ambiguous_matches": [],
    }


def active_leaders_statement():
    return (
        select(LeaderConfig)
        .where(LeaderConfig.enabled.is_(True))
        .where(LeaderConfig.deleted_at.is_(None))
        .order_by(LeaderConfig.created_at.desc())
    )


def soft_delete_leader(
    leader_config: Any,
    *,
    reason: str | None = None,
    now: datetime | None = None,
) -> None:
    leader_config.enabled = False
    leader_config.deleted_at = now or datetime.now(timezone.utc)
    leader_config.delete_reason = reason or "deleted from Leaders page"


def disable_leader(leader_config: Any) -> None:
    leader_config.enabled = False


def enable_leader(leader_config: Any) -> None:
    leader_config.enabled = True
    leader_config.deleted_at = None
    leader_config.delete_reason = None


def active_leader_addresses(leaders: Iterable[Any]) -> set[str]:
    return {
        normalize_leader_address(leader.leader_address)
        for leader in leaders
        if bool(getattr(leader, "enabled", False)) and not is_leader_deleted(leader)
    }


def watcher_consistency(
    *,
    leaders: Iterable[Any],
    watcher_active_addresses: Iterable[str],
) -> dict[str, Any]:
    db_enabled = active_leader_addresses(leaders)
    watcher_active = {normalize_leader_address(address) for address in watcher_active_addresses if address}
    return {
        "db_enabled_leaders_count": len(db_enabled),
        "watcher_active_leaders_count": len(watcher_active),
        "leaders_not_subscribed": sorted(db_enabled - watcher_active),
        "subscribed_but_disabled_or_deleted": sorted(watcher_active - db_enabled),
    }


def decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    if value.is_zero():
        return "0"
    return format(value.normalize(), "f")
