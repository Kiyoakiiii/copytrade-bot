from __future__ import annotations

from typing import Any

from app.services.hyperliquid_dex import PerpDex, canonical_coin, dex_display_name


def build_hyperliquid_market_coverage(
    *,
    enabled_dexes: list[PerpDex],
    metas_by_dex: dict[str, dict[str, Any]],
    mids_by_dex: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Summarize every configured Hyperliquid perps DEX without symbol/product filtering."""
    rows: list[dict[str, Any]] = []
    total_loaded = 0
    total_unknown_product = 0
    for dex in enabled_dexes:
        meta = metas_by_dex.get(dex.dex_name, {}) or {}
        mids = mids_by_dex.get(dex.dex_name, {}) or {}
        universe = list(meta.get("universe", []) or [])
        meta_names = {
            str(item.get("name") or "").strip()
            for item in universe
            if str(item.get("name") or "").strip()
        }
        mid_names = {
            str(name).strip()
            for name in mids.keys()
            if str(name).strip() and str(name).lower() not in {"dex", "type"}
        }
        all_names = sorted(meta_names | mid_names)
        unknown_product = 0
        examples: list[dict[str, Any]] = []
        for name in all_names:
            meta_item = next((item for item in universe if str(item.get("name") or "").strip() == name), None)
            product_type = _product_type(meta_item)
            if product_type == "unknown":
                unknown_product += 1
            if len(examples) < 10:
                examples.append(
                    {
                        "raw_name": name,
                        "canonical_coin": canonical_coin(dex=dex.dex_name, coin=name),
                        "product_type": product_type,
                        "from_meta": meta_item is not None,
                        "from_mids": name in mid_names,
                    }
                )
        total_loaded += len(all_names)
        total_unknown_product += unknown_product
        rows.append(
            {
                "dex": dex.dex_name,
                "dex_name": dex.dex_name,
                "display_name": dex.display_name or dex_display_name(dex.dex_name),
                "is_hip3": dex.is_hip3,
                "markets_loaded_count": len(all_names),
                "meta_universe_count": len(universe),
                "mids_markets_count": len(mid_names),
                "unknown_product_markets_count": unknown_product,
                "asset_id_mapping_ready": bool(universe),
                "examples": examples,
                "status": "OK" if all_names else "MISSING",
                "message": "OK" if all_names else "meta/allMids returned no markets for this dex",
            }
        )
    return {
        "enabled_dexes": [dex.dex_name for dex in enabled_dexes],
        "enabled_dex_count": len(enabled_dexes),
        "markets_loaded_count": total_loaded,
        "markets_loaded_count_by_dex": {row["dex"]: row["markets_loaded_count"] for row in rows},
        "unknown_product_markets_count": total_unknown_product,
        "all_coins_mode_includes_enabled_dex_markets": True,
        "all_coins_mode_includes_hip3_tradfi_unknown": True,
        "binance_mapping_required_for_hyperliquid": False,
        "product_type_unknown_hidden": False,
        "no_static_coin_filter": True,
        "canonical_scope_keys": ["execution_venue", "dex", "canonical_coin"],
        "rows": rows,
    }


def _product_type(item: dict[str, Any] | None) -> str:
    if not item:
        return "unknown"
    value = item.get("productType") or item.get("product_type") or item.get("type") or item.get("kind")
    return str(value or "unknown").strip() or "unknown"
