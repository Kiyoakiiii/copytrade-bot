from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Any


@dataclass(frozen=True)
class TradingRule:
    symbol: str
    base_asset: str
    status: str
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    tick_size: Decimal

    @property
    def is_trading(self) -> bool:
        return self.status.upper() == "TRADING"


@dataclass(frozen=True)
class SymbolOverride:
    coin: str
    binance_symbol: str
    enabled: bool = True


@dataclass(frozen=True)
class SymbolMappingResult:
    coin: str
    binance_symbol: str | None
    enabled: bool
    reason: str | None = None
    rule: TradingRule | None = None


def _d(value: str | int | float | Decimal) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def floor_to_step(value: Decimal | str | int | float, step: Decimal | str) -> Decimal:
    value_d = _d(value)
    step_d = _d(step)
    if value_d <= 0 or step_d <= 0:
        return Decimal("0")
    steps = (value_d / step_d).to_integral_value(rounding=ROUND_DOWN)
    return steps * step_d


def decimal_to_exchange_str(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f")


def build_rules_from_exchange_info(exchange_info: dict[str, Any]) -> dict[str, TradingRule]:
    rules: dict[str, TradingRule] = {}
    for item in exchange_info.get("symbols", []):
        if item.get("contractType") not in (None, "PERPETUAL"):
            continue
        filters = {f.get("filterType"): f for f in item.get("filters", [])}
        lot = filters.get("LOT_SIZE", {})
        price = filters.get("PRICE_FILTER", {})
        min_notional_filter = filters.get("MIN_NOTIONAL", {})
        symbol = item["symbol"].upper()
        rules[symbol] = TradingRule(
            symbol=symbol,
            base_asset=item.get("baseAsset", "").upper(),
            status=item.get("status", "UNKNOWN"),
            step_size=_d(lot.get("stepSize", "0")),
            min_qty=_d(lot.get("minQty", "0")),
            min_notional=_d(
                min_notional_filter.get(
                    "notional", min_notional_filter.get("minNotional", "0")
                )
            ),
            tick_size=_d(price.get("tickSize", "0")),
        )
    return rules


class SymbolMapper:
    def __init__(
        self,
        binance_rules: dict[str, TradingRule],
        overrides: list[SymbolOverride] | None = None,
    ) -> None:
        self._rules = {k.upper(): v for k, v in binance_rules.items()}
        self._overrides = {(o.coin.upper()): o for o in overrides or []}

    def map_coin(self, coin: str) -> SymbolMappingResult:
        coin_u = coin.upper()
        override = self._overrides.get(coin_u)
        if override and not override.enabled:
            return SymbolMappingResult(
                coin=coin_u,
                binance_symbol=override.binance_symbol.upper(),
                enabled=False,
                reason="manual mapping disabled",
            )

        symbol = override.binance_symbol.upper() if override else f"{coin_u}USDT"
        rule = self._rules.get(symbol)
        if not rule:
            return SymbolMappingResult(
                coin=coin_u,
                binance_symbol=symbol,
                enabled=False,
                reason="binance symbol not found",
            )
        if not rule.is_trading:
            return SymbolMappingResult(
                coin=coin_u,
                binance_symbol=symbol,
                enabled=False,
                reason="binance symbol is not TRADING",
                rule=rule,
            )
        return SymbolMappingResult(
            coin=coin_u, binance_symbol=symbol, enabled=True, rule=rule
        )


def quantity_from_notional(
    notional: Decimal | str | int | float,
    price: Decimal | str | int | float,
    rule: TradingRule,
) -> tuple[Decimal, str | None]:
    notional_d = _d(notional)
    price_d = _d(price)
    if notional_d <= 0:
        return Decimal("0"), "notional must be positive"
    if price_d <= 0:
        return Decimal("0"), "price must be positive"
    qty = floor_to_step(notional_d / price_d, rule.step_size)
    if qty <= 0:
        return qty, "quantity rounded to zero"
    if qty < rule.min_qty:
        return qty, "quantity below minQty"
    if qty * price_d < rule.min_notional:
        return qty, "notional below minNotional"
    return qty, None


def notional_to_quantity(
    *,
    symbol: str,
    target_delta_notional: Decimal,
    mark_price: Decimal,
    exchange_filters: TradingRule,
) -> tuple[Decimal, str | None]:
    if exchange_filters.symbol.upper() != symbol.upper():
        return Decimal("0"), "SYMBOL_RULE_MISMATCH"
    qty, error = quantity_from_notional(
        abs(target_delta_notional), mark_price, exchange_filters
    )
    if error:
        return qty, "SKIPPED_TOO_SMALL"
    return qty, None


def round_price(price: Decimal | str | int | float, rule: TradingRule) -> Decimal:
    return floor_to_step(price, rule.tick_size)
