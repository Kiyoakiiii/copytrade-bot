from decimal import Decimal

from app.services.symbol_mapper import (
    SymbolMapper,
    SymbolOverride,
    TradingRule,
    build_rules_from_exchange_info,
    quantity_from_notional,
)


def rule(symbol: str = "BTCUSDT") -> TradingRule:
    return TradingRule(
        symbol=symbol,
        base_asset=symbol.replace("USDT", ""),
        status="TRADING",
        step_size=Decimal("0.001"),
        min_qty=Decimal("0.001"),
        min_notional=Decimal("5"),
        tick_size=Decimal("0.10"),
    )


def test_default_mapping_and_override_disable() -> None:
    mapper = SymbolMapper({"BTCUSDT": rule()}, [SymbolOverride("ETH", "ETHUSDT", False)])

    assert mapper.map_coin("BTC").enabled is True
    disabled = mapper.map_coin("ETH")
    assert disabled.enabled is False
    assert disabled.reason == "manual mapping disabled"


def test_quantity_rounding_and_min_notional() -> None:
    qty, error = quantity_from_notional(100, 33333, rule())
    assert qty == Decimal("0.003")
    assert error is None

    qty, error = quantity_from_notional(4, 33333, rule())
    assert qty == Decimal("0")
    assert error == "quantity rounded to zero"


def test_build_rules_from_exchange_info() -> None:
    exchange_info = {
        "symbols": [
            {
                "symbol": "SOLUSDT",
                "baseAsset": "SOL",
                "contractType": "PERPETUAL",
                "status": "TRADING",
                "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": "0.1", "minQty": "0.1"},
                    {"filterType": "PRICE_FILTER", "tickSize": "0.001"},
                    {"filterType": "MIN_NOTIONAL", "notional": "5"},
                ],
            }
        ]
    }

    rules = build_rules_from_exchange_info(exchange_info)
    assert rules["SOLUSDT"].step_size == Decimal("0.1")

