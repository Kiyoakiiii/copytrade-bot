from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DEFAULT_DEX = ""
DEFAULT_DEX_DISPLAY = "Hyperliquid"


@dataclass(frozen=True)
class PerpDex:
    dex_name: str
    display_name: str
    enabled: bool
    is_hip3: bool
    deployer_address: str | None = None
    collateral_asset: str | None = None

    @property
    def key(self) -> str:
        return self.dex_name


@dataclass(frozen=True)
class ParsedCoin:
    dex: str
    coin: str
    canonical_coin: str
    raw_coin: str


def normalize_dex(value: str | None) -> str:
    return str(value or "").strip().lower()


def dex_display_name(dex: str | None) -> str:
    name = normalize_dex(dex)
    if not name:
        return DEFAULT_DEX_DISPLAY
    if name == "xyz":
        return "XYZ"
    return name


def parse_coin(raw_coin: str | None, *, default_dex: str = DEFAULT_DEX) -> ParsedCoin:
    raw = str(raw_coin or "").strip()
    fallback_dex = normalize_dex(default_dex)
    if ":" in raw:
        maybe_dex, coin = raw.split(":", 1)
        dex = normalize_dex(maybe_dex)
        coin_u = _normalize_coin_name(coin, strip_plain_usdt=not dex)
    else:
        dex = fallback_dex
        coin_u = _normalize_coin_name(raw, strip_plain_usdt=not dex)
    canonical = canonical_coin(dex=dex, coin=coin_u)
    return ParsedCoin(dex=dex, coin=coin_u, canonical_coin=canonical, raw_coin=raw)


def canonical_coin(*, dex: str | None, coin: str | None) -> str:
    dex_n = normalize_dex(dex)
    coin_u = _normalize_coin_name(coin, strip_plain_usdt=not dex_n)
    if not coin_u:
        return ""
    return f"{dex_n}:{coin_u}" if dex_n else coin_u


def market_key(*, dex: str | None, coin: str | None) -> str:
    return f"HYPERLIQUID|{canonical_coin(dex=dex, coin=coin)}"


def mask_address(value: str | None) -> str | None:
    if not value:
        return None
    address = str(value)
    if len(address) <= 12:
        return "***"
    return f"{address[:6]}...{address[-4:]}"


def mask_debug_payload(value: Any) -> Any:
    if isinstance(value, dict):
        masked: dict[str, Any] = {}
        for key, item in value.items():
            key_l = str(key).lower()
            if any(secret in key_l for secret in ("private", "secret", "signature", "api_key", "api-key")):
                masked[key] = "***"
            elif "address" in key_l and isinstance(item, str):
                masked[key] = mask_address(item)
            else:
                masked[key] = mask_debug_payload(item)
        return masked
    if isinstance(value, list):
        return [mask_debug_payload(item) for item in value]
    return value


class HyperliquidDexRegistry:
    def __init__(self, settings: Any) -> None:
        self._settings = settings

    def enabled_dexes(self) -> list[PerpDex]:
        result: list[PerpDex] = []
        seen: set[str] = set()
        for dex_name in self._settings.enabled_hyperliquid_dex_list():
            if dex_name in seen:
                continue
            seen.add(dex_name)
            result.append(
                PerpDex(
                    dex_name=dex_name,
                    display_name=dex_display_name(dex_name),
                    enabled=True,
                    is_hip3=bool(dex_name),
                )
            )
        return result


def _normalize_coin_name(value: str | None, *, strip_plain_usdt: bool = True) -> str:
    symbol = str(value or "").strip().upper()
    for suffix in ("-PERP", " PERP"):
        if symbol.endswith(suffix):
            symbol = symbol[: -len(suffix)]
    if symbol.endswith("/USDT"):
        symbol = symbol[:-5]
    if strip_plain_usdt and symbol.endswith("USDT") and len(symbol) > 4:
        symbol = symbol[:-4]
    return symbol.strip()
