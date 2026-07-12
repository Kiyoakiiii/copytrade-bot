from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from app.api.deps import AppSettings, CurrentUser, DbSession
from app.models import SymbolMapping, VenueMapping
from app.schemas.api import VenueMappingPatch
from app.services.binance_client import BinanceFuturesClient
from app.services.execution_router import ExecutionVenue
from app.services.hyperliquid_execution import HyperliquidExecutionClient
from app.services.hyperliquid_dex import HyperliquidDexRegistry

router = APIRouter(tags=["venues"])


@router.get("/venues")
async def venue_settings(_: CurrentUser, db: DbSession, settings: AppSettings):
    venue_rows = (await db.execute(select(VenueMapping))).scalars().all()
    hl_mapping_count = sum(1 for row in venue_rows if row.execution_venue == ExecutionVenue.HYPERLIQUID.value)
    binance_mapping_count = sum(1 for row in venue_rows if row.execution_venue == ExecutionVenue.BINANCE.value)
    hyperliquid_connected = await _probe_hyperliquid(settings)
    binance_connected = await _probe_binance(settings)
    return {
        "default_preferred_venue": settings.default_preferred_venue,
        "fallback_venue": "BINANCE" if settings.enable_binance_fallback else "NONE",
        "global_trading_enabled": settings.trading_enabled,
        "venues": [
            {
                "venue": ExecutionVenue.HYPERLIQUID.value,
                "enabled": settings.enable_hyperliquid_execution,
                "trading_enabled": settings.hyperliquid_trading_enabled,
                "network": settings.hyperliquid_execution_network,
                "api_connected": hyperliquid_connected["ok"],
                "status": hyperliquid_connected["status"],
                "wallet_configured": bool(settings.hyperliquid_follower_account_address()),
                "account_address_configured": bool(settings.hyperliquid_account_address),
                "api_wallet_address_configured": bool(settings.hyperliquid_api_wallet_address),
                "subaccount_address_configured": bool(settings.hyperliquid_subaccount_address),
                "account_address_ambiguous": settings.hyperliquid_follower_address_ambiguous(),
                "vault_address_configured": bool(settings.hyperliquid_vault_address),
                "private_key_configured": bool(settings.hyperliquid_private_key_value()),
                "default_leverage": settings.hyperliquid_default_leverage,
                "margin_mode": settings.hyperliquid_default_margin_mode.upper(),
                "mapping_count": hl_mapping_count,
                "dexes": [
                    {
                        "dex_name": dex.dex_name,
                        "display_name": dex.display_name,
                        "enabled": dex.enabled,
                        "is_hip3": dex.is_hip3,
                        "meta_status": hyperliquid_connected.get("dex_status", {}).get(dex.dex_name, "unknown"),
                        "tradable_markets_count": hyperliquid_connected.get("dex_universe_count", {}).get(dex.dex_name, 0),
                        "low_latency_status": "blocked_poll_fallback",
                    }
                    for dex in HyperliquidDexRegistry(settings).enabled_dexes()
                ],
            },
            {
                "venue": ExecutionVenue.BINANCE.value,
                "enabled": settings.enable_binance_execution,
                "trading_enabled": settings.binance_trading_enabled,
                "network": "testnet" if settings.binance_testnet else "mainnet",
                "api_connected": binance_connected["ok"],
                "status": binance_connected["status"],
                "wallet_configured": bool(settings.binance_api_key and settings.binance_api_secret),
                "vault_address_configured": False,
                "private_key_configured": bool(settings.binance_api_key and settings.binance_api_secret),
                "default_leverage": settings.binance_expected_leverage,
                "margin_mode": settings.binance_expected_margin_type.upper(),
                "mapping_count": binance_mapping_count,
            },
        ],
    }


@router.get("/venue-mappings")
async def list_venue_mappings(_: CurrentUser, db: DbSession, settings: AppSettings):
    venue_rows = (
        await db.execute(
            select(VenueMapping).order_by(
                VenueMapping.hyperliquid_coin,
                VenueMapping.execution_venue,
                VenueMapping.venue_symbol,
            )
        )
    ).scalars().all()
    legacy_rows = (
        await db.execute(select(SymbolMapping).order_by(SymbolMapping.hyperliquid_coin))
    ).scalars().all()

    grouped: dict[str, dict[str, Any]] = defaultdict(dict)
    for row in venue_rows:
        grouped[row.hyperliquid_coin.upper()][row.execution_venue] = row
    for row in legacy_rows:
        coin = row.hyperliquid_coin.upper()
        grouped.setdefault(coin, {})
        grouped[coin].setdefault(
            ExecutionVenue.BINANCE.value,
            _legacy_binance_mapping(row),
        )
        grouped[coin].setdefault(
            ExecutionVenue.HYPERLIQUID.value,
            _synthetic_hyperliquid_mapping(row),
        )

    items = []
    for coin in sorted(grouped):
        hyperliquid = grouped[coin].get(ExecutionVenue.HYPERLIQUID.value)
        binance = grouped[coin].get(ExecutionVenue.BINANCE.value)
        route = _effective_route(
            coin=coin,
            hyperliquid=hyperliquid,
            binance=binance,
            default_preferred_venue=settings.default_preferred_venue,
            binance_fallback_enabled=settings.enable_binance_fallback,
        )
        items.append(
            {
                "coin": coin,
                "hyperliquid": _mapping_payload(hyperliquid, default_symbol=coin),
                "binance": _mapping_payload(binance, default_symbol=None),
                **route,
            }
        )
    return items


@router.patch("/venue-mappings/{mapping_id}")
async def patch_venue_mapping(
    mapping_id: int,
    _: CurrentUser,
    payload: VenueMappingPatch,
    db: DbSession,
):
    mapping = await db.get(VenueMapping, mapping_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="venue mapping not found")
    for key, value in payload.model_dump(exclude_unset=True).items():
        setattr(mapping, key, value)
    await db.commit()
    await db.refresh(mapping)
    return mapping


async def _probe_hyperliquid(settings: AppSettings) -> dict[str, Any]:
    if not settings.enable_hyperliquid_execution:
        return {"ok": False, "status": "disabled"}
    client = HyperliquidExecutionClient(
        info_url=f"{settings.hyperliquid_execution_base_url()}/info",
        private_key=settings.hyperliquid_private_key_value(),
        account_address=settings.hyperliquid_account_address or settings.hyperliquid_signer_address(),
        vault_address=settings.hyperliquid_vault_address,
        network=settings.hyperliquid_execution_network,
        timeout=3.0,
    )
    try:
        dex_status: dict[str, str] = {}
        dex_universe_count: dict[str, int] = {}
        for dex in HyperliquidDexRegistry(settings).enabled_dexes():
            try:
                meta = await client.meta(dex.dex_name)
                dex_status[dex.dex_name] = "connected" if meta.get("universe") else "missing_meta"
                dex_universe_count[dex.dex_name] = len(meta.get("universe", []) or [])
            except Exception as exc:
                dex_status[dex.dex_name] = str(exc)[:120]
                dex_universe_count[dex.dex_name] = 0
        return {"ok": any(status == "connected" for status in dex_status.values()), "status": "connected", "dex_status": dex_status, "dex_universe_count": dex_universe_count}
    except Exception as exc:
        return {"ok": False, "status": str(exc)[:160]}
    finally:
        await client.close()


async def _probe_binance(settings: AppSettings) -> dict[str, Any]:
    if not settings.enable_binance_execution:
        return {"ok": False, "status": "disabled"}
    if not settings.binance_api_key or not settings.binance_api_secret:
        return {"ok": False, "status": "credentials_not_configured"}
    client = BinanceFuturesClient(settings, timeout=3.0)
    try:
        await client.position_mode_dual_side()
        return {"ok": True, "status": "connected"}
    except Exception as exc:
        return {"ok": False, "status": str(exc)[:160]}
    finally:
        await client.close()


def _legacy_binance_mapping(row: SymbolMapping) -> dict[str, Any]:
    return {
        "id": None,
        "hyperliquid_coin": row.hyperliquid_coin.upper(),
        "execution_venue": ExecutionVenue.BINANCE.value,
        "venue_symbol": row.binance_symbol.upper(),
        "enabled": row.enabled,
        "is_default": False,
        "mapping_status": "OK" if row.enabled else "DISABLED",
        "reason": row.last_validation_error,
    }


def _synthetic_hyperliquid_mapping(row: SymbolMapping) -> dict[str, Any]:
    return {
        "id": None,
        "hyperliquid_coin": row.hyperliquid_coin.upper(),
        "execution_venue": ExecutionVenue.HYPERLIQUID.value,
        "venue_symbol": row.hyperliquid_coin.upper(),
        "enabled": True,
        "is_default": True,
        "mapping_status": "UNKNOWN",
        "reason": "requires Hyperliquid meta validation",
    }


def _mapping_payload(row: Any, *, default_symbol: str | None) -> dict[str, Any]:
    if row is None:
        return {
            "id": None,
            "venue_symbol": default_symbol,
            "enabled": False,
            "tradable": False,
            "mapping_status": "MISSING",
            "reason": "mapping missing",
            "is_default": False,
        }
    status = str(getattr(row, "mapping_status", "UNKNOWN")).upper()
    enabled = bool(getattr(row, "enabled", False))
    return {
        "id": getattr(row, "id", None),
        "venue_symbol": getattr(row, "venue_symbol", default_symbol),
        "enabled": enabled,
        "tradable": enabled and status not in {"DISABLED", "BLOCKED", "MISSING"},
        "mapping_status": status,
        "reason": getattr(row, "reason", None),
        "is_default": bool(getattr(row, "is_default", False)),
    }


def _effective_route(
    *,
    coin: str,
    hyperliquid: Any,
    binance: Any,
    default_preferred_venue: str,
    binance_fallback_enabled: bool,
) -> dict[str, str]:
    hl = _mapping_payload(hyperliquid, default_symbol=coin)
    bn = _mapping_payload(binance, default_symbol=None)
    preferred = default_preferred_venue.upper()
    if preferred == ExecutionVenue.BINANCE.value and bn["tradable"]:
        return {"default_venue": preferred, "effective_venue": "BINANCE", "reason": "BINANCE_PRIMARY"}
    if hl["tradable"]:
        reason = "HYPERLIQUID_PRIMARY"
        if not bn["tradable"]:
            reason = "BINANCE_MAPPING_MISSING_USE_HYPERLIQUID"
        return {"default_venue": preferred, "effective_venue": "HYPERLIQUID", "reason": reason}
    if binance_fallback_enabled and bn["tradable"]:
        return {"default_venue": preferred, "effective_venue": "BINANCE", "reason": "BINANCE_FALLBACK"}
    return {"default_venue": preferred, "effective_venue": "BLOCKED", "reason": "BOTH_UNAVAILABLE_BLOCKED"}
