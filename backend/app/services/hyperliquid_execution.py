from __future__ import annotations

import asyncio
import hashlib
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Any

import httpx

from app.services.account_abstraction import (
    AccountAbstractionService,
    available_collateral_sufficient,
    resolve_account_value_for_sizing,
)
from app.services.hyperliquid_dex import canonical_coin, parse_coin

AGGRESSIVE_IOC_BUY_PRICE_MULTIPLIER = Decimal("1.9")
AGGRESSIVE_IOC_SELL_PRICE_MULTIPLIER = Decimal("0.1")
DIRECT_HTTP_MAX_IN_FLIGHT = 64


@dataclass
class _SignerNonceState:
    lock: threading.Lock
    submit_semaphore: threading.BoundedSemaphore
    last_nonce: int = 0


_SIGNER_NONCE_STATES: dict[str, _SignerNonceState] = {}
_SIGNER_NONCE_STATES_LOCK = threading.Lock()


class _SignedActionSlot:
    def __init__(self, semaphore: threading.BoundedSemaphore, latency_trace: dict[str, Any] | None) -> None:
        self._semaphore = semaphore
        self._latency_trace = latency_trace
        self._acquired = False

    async def __aenter__(self) -> None:
        _trace_timestamp(self._latency_trace, "direct_http_submit_slot_wait_started_at")
        self._acquired = self._semaphore.acquire(blocking=False)
        if not self._acquired:
            await asyncio.to_thread(self._semaphore.acquire)
            self._acquired = True
        _trace_timestamp(self._latency_trace, "direct_http_submit_slot_acquired_at")

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._acquired:
            self._semaphore.release()
            self._acquired = False


def _signer_nonce_state(*, private_key: str | None, network: str) -> _SignerNonceState:
    key_material = str(private_key or "").strip().lower()
    network_key = str(network or "").strip().lower()
    fingerprint = hashlib.blake2s(key_material.encode(), digest_size=16).hexdigest()
    key = f"{network_key}:{fingerprint}"
    with _SIGNER_NONCE_STATES_LOCK:
        state = _SIGNER_NONCE_STATES.get(key)
        if state is None:
            state = _SignerNonceState(
                lock=threading.Lock(),
                submit_semaphore=threading.BoundedSemaphore(DIRECT_HTTP_MAX_IN_FLIGHT),
            )
            _SIGNER_NONCE_STATES[key] = state
        return state


@dataclass(frozen=True)
class HyperliquidRiskSettingsResult:
    is_ok: bool
    coin: str
    leverage: int | None = None
    margin_mode: str | None = None
    reason: str | None = None
    max_leverage: int | None = None
    effective_leverage: int | None = None
    warning: str | None = None
    hyperliquid_coin_exists: bool = False
    coin_max_leverage_loaded: bool = False
    margin_mode_confirmed: bool = False
    isolated_confirmed: bool = False
    leverage_confirmed: bool = False
    available_margin_sufficient: bool | None = None

    def checklist(self) -> dict[str, Any]:
        return {
            "hyperliquid_coin_exists": self.hyperliquid_coin_exists,
            "coin_max_leverage_loaded": self.coin_max_leverage_loaded,
            "coin_max_leverage": self.max_leverage,
            "effective_leverage": self.effective_leverage,
            "margin_mode": self.margin_mode,
            "margin_mode_confirmed": self.margin_mode_confirmed,
            "isolated_confirmed": self.isolated_confirmed,
            "leverage_confirmed": self.leverage_confirmed,
            "available_margin_sufficient": self.available_margin_sufficient,
        }


@dataclass(frozen=True)
class HyperliquidLeveragePlan:
    ok_for_open: bool
    max_leverage: int | None
    effective_leverage: int | None
    status: str
    reason: str | None = None
    warning: str | None = None
    sz_decimals: int | None = None
    asset_id: int | None = None
    market_meta: dict[str, Any] | None = None


def build_hyperliquid_leverage_plan(
    *,
    default_leverage: int,
    coin_max_leverage: Any,
    sz_decimals: Any = None,
    asset_id: Any = None,
    market_meta: dict[str, Any] | None = None,
) -> HyperliquidLeveragePlan:
    parsed_sz_decimals = _int_or_none(sz_decimals)
    parsed_asset_id = _int_or_none(asset_id)
    try:
        max_leverage = int(coin_max_leverage)
    except (TypeError, ValueError):
        return HyperliquidLeveragePlan(
            ok_for_open=False,
            max_leverage=None,
            effective_leverage=None,
            status="BLOCKED",
            reason="coin max leverage missing or invalid",
            sz_decimals=parsed_sz_decimals,
            asset_id=parsed_asset_id,
            market_meta=market_meta,
        )
    if max_leverage <= 0:
        return HyperliquidLeveragePlan(
            ok_for_open=False,
            max_leverage=max_leverage,
            effective_leverage=None,
            status="BLOCKED",
            reason="coin max leverage must be positive",
            sz_decimals=parsed_sz_decimals,
            asset_id=parsed_asset_id,
            market_meta=market_meta,
        )
    effective = min(default_leverage, max_leverage)
    if max_leverage < default_leverage:
        return HyperliquidLeveragePlan(
            ok_for_open=True,
            max_leverage=max_leverage,
            effective_leverage=effective,
            status="WARNING",
            warning=(
                f"coin max leverage {max_leverage} below default {default_leverage}; "
                "using max leverage"
            ),
            sz_decimals=parsed_sz_decimals,
            asset_id=parsed_asset_id,
            market_meta=market_meta,
        )
    return HyperliquidLeveragePlan(
        ok_for_open=True,
        max_leverage=max_leverage,
        effective_leverage=effective,
        status="OK",
        sz_decimals=parsed_sz_decimals,
        asset_id=parsed_asset_id,
        market_meta=market_meta,
    )


@dataclass(frozen=True)
class ValidatedOrderParams:
    dex: str
    canonical_coin: str
    asset_id: int | None
    action: str
    side: str
    cloid: str | None
    cloid_sdk_type: str | None
    is_buy: bool
    reduce_only: bool
    aggressive_market: bool
    tif: str
    raw_size: Decimal
    rounded_size: Decimal
    raw_price: Decimal
    raw_limit_price: Decimal
    rounded_price: Decimal
    estimated_notional: Decimal
    target_delta_notional: Decimal
    min_order_value: Decimal
    passes_min_order_value: bool
    sz_decimals: int | None
    price_decimals: int | None
    tick_size: Decimal | None
    max_leverage: int | None
    effective_leverage: int | None
    errors: list[str]
    warnings: list[str]
    block_reason: str | None

    @property
    def ok(self) -> bool:
        return self.block_reason is None and not self.errors

    def order_payload(self) -> dict[str, Any]:
        return {
            "coin": self.canonical_coin,
            "dex": self.dex,
            "is_buy": self.is_buy,
            "sz": self.rounded_size,
            "limit_px": self.rounded_price,
            "order_type": {"limit": {"tif": "Ioc"}},
            "reduce_only": self.reduce_only,
            "cloid": self.cloid,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "validator_status": "OK" if self.ok else "BLOCKED",
            "dex": self.dex,
            "canonical_coin": self.canonical_coin,
            "asset_id": self.asset_id,
            "action": self.action,
            "side": self.side,
            "cloid": self.cloid,
            "cloid_sdk_type": self.cloid_sdk_type,
            "is_buy": self.is_buy,
            "reduce_only": self.reduce_only,
            "aggressive_market": self.aggressive_market,
            "tif": self.tif,
            "raw_size": str(self.raw_size),
            "rounded_size": str(self.rounded_size),
            "raw_price": str(self.raw_price),
            "raw_limit_price": str(self.raw_limit_price),
            "rounded_price": str(self.rounded_price),
            "estimated_notional": str(self.estimated_notional),
            "target_delta_notional": str(self.target_delta_notional),
            "min_order_value": str(self.min_order_value),
            "passes_min_order_value": self.passes_min_order_value,
            "sz_decimals": self.sz_decimals,
            "price_decimals": self.price_decimals,
            "tick_size": str(self.tick_size) if self.tick_size is not None else None,
            "max_leverage": self.max_leverage,
            "effective_leverage": self.effective_leverage,
            "errors": self.errors,
            "warnings": self.warnings,
            "block_reason": self.block_reason,
            "payload_masked": {
                **self.order_payload(),
                "quantity": str(self.rounded_size),
                "sz": str(self.rounded_size),
                "limit_px": str(self.rounded_price),
            },
        }


def validate_hyperliquid_order_params(
    dex: str,
    canonical_coin: str,
    asset_id: int | None,
    action: str,
    side: str,
    target_delta_notional: Decimal,
    raw_size: Decimal,
    raw_price: Decimal,
    market_meta: dict[str, Any] | None,
    order_policy: dict[str, Any],
) -> ValidatedOrderParams:
    dex_key = str(dex or "").lower()
    parsed = parse_coin(canonical_coin, default_dex=dex_key)
    canonical = parsed.canonical_coin
    errors: list[str] = []
    warnings: list[str] = []
    block_reason: str | None = None
    side_u = str(side or "").upper()
    is_buy = bool(order_policy.get("is_buy")) if "is_buy" in order_policy else side_u == "BUY"
    reduce_only = bool(order_policy.get("reduce_only", False))
    tif = str(order_policy.get("tif") or order_policy.get("time_in_force") or "Ioc")
    cloid = str(order_policy.get("cloid") or "") or None
    cloid_sdk_type: str | None = None

    if not market_meta:
        errors.append("BLOCKED_MARKET_META_MISSING")
        block_reason = block_reason or "BLOCKED_MARKET_META_MISSING"
    meta_asset_id = _int_or_none(
        _first_not_none(
            (market_meta or {}).get("asset_id"),
            (market_meta or {}).get("assetId"),
            (market_meta or {}).get("index"),
        )
    )
    if market_meta and meta_asset_id is None:
        errors.append("BLOCKED_ASSET_ID_MISSING")
        block_reason = block_reason or "BLOCKED_ASSET_ID_MISSING"
    if asset_id is None:
        errors.append("BLOCKED_ASSET_ID_MISSING")
        block_reason = block_reason or "BLOCKED_ASSET_ID_MISSING"
    elif meta_asset_id is not None and int(asset_id) != meta_asset_id:
        errors.append("BLOCKED_ASSET_ID_MISMATCH")
        block_reason = block_reason or "BLOCKED_ASSET_ID_MISMATCH"
    if order_policy.get("price_fresh") is False:
        errors.append("BLOCKED_PRICE_STALE")
        block_reason = block_reason or "BLOCKED_PRICE_STALE"

    if not cloid:
        errors.append("BLOCKED_INVALID_CLOID")
        block_reason = block_reason or "BLOCKED_INVALID_CLOID"
    else:
        if not _valid_cloid_string(cloid):
            errors.append("BLOCKED_INVALID_CLOID")
            block_reason = block_reason or "BLOCKED_INVALID_CLOID"
        sdk_cloid = _sdk_cloid(cloid)
        cloid_sdk_type = type(sdk_cloid).__name__ if sdk_cloid is not None else None
        if not hasattr(sdk_cloid, "to_raw"):
            errors.append("BLOCKED_INVALID_CLOID_TYPE")
            block_reason = block_reason or "BLOCKED_INVALID_CLOID_TYPE"

    if tif != "Ioc":
        errors.append("BLOCKED_INVALID_IOC_POLICY")
        block_reason = block_reason or "BLOCKED_INVALID_IOC_POLICY"
    order_type = order_policy.get("order_type") or {"limit": {"tif": tif}}
    if isinstance(order_type, dict):
        limit_policy = order_type.get("limit") or {}
        if str(limit_policy.get("tif") or tif) != "Ioc" or limit_policy.get("postOnly"):
            errors.append("BLOCKED_INVALID_IOC_POLICY")
            block_reason = block_reason or "BLOCKED_INVALID_IOC_POLICY"
        for forbidden in ("Gtc", "Alo"):
            if str(limit_policy.get("tif") or "").lower() == forbidden.lower():
                errors.append("BLOCKED_INVALID_IOC_POLICY")
                block_reason = block_reason or "BLOCKED_INVALID_IOC_POLICY"

    sz_decimals = _int_or_none((market_meta or {}).get("szDecimals"))
    if market_meta and sz_decimals is None:
        errors.append("BLOCKED_PRECISION_MISSING")
        block_reason = block_reason or "BLOCKED_PRECISION_MISSING"
    max_leverage = _int_or_none((market_meta or {}).get("maxLeverage"))
    effective_leverage = _int_or_none(order_policy.get("effective_leverage"))
    min_order_value = _decimal_or_none(
        _first_not_none(
            (market_meta or {}).get("minOrderValue"),
            (market_meta or {}).get("min_order_value"),
            order_policy.get("min_order_value"),
        )
    ) or Decimal("10")

    raw_size_d = Decimal(str(raw_size or "0"))
    raw_price_d = Decimal(str(raw_price or "0"))
    target_delta = abs(Decimal(str(target_delta_notional or "0")))
    rounded_size = _round_hyperliquid_size(raw_size_d, sz_decimals=sz_decimals)
    aggressive_market = bool(order_policy.get("aggressive_market"))
    if aggressive_market:
        raw_limit_price = aggressive_hyperliquid_ioc_limit_price(
            raw_price_d,
            is_buy=is_buy,
            sz_decimals=sz_decimals,
        )
    else:
        slip = Decimal(str(order_policy.get("slippage_bps", 0))) / Decimal("10000")
        raw_limit_price = raw_price_d * (Decimal("1") + slip) if is_buy else raw_price_d * (Decimal("1") - slip)
    rounded_price = round_hyperliquid_limit_price(raw_limit_price, is_buy=is_buy, sz_decimals=sz_decimals)
    price_decimals = hyperliquid_price_decimals(raw_limit_price, sz_decimals=sz_decimals) if sz_decimals is not None else None
    tick_size = Decimal("1").scaleb(-price_decimals) if price_decimals is not None else None
    reference_notional = (rounded_size * raw_price_d).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
    estimated_notional = reference_notional
    passes_min = reference_notional >= min_order_value

    if raw_price_d <= 0 or rounded_price <= 0:
        errors.append("BLOCKED_INVALID_PRICE")
        block_reason = block_reason or "BLOCKED_INVALID_PRICE"
    if rounded_size <= 0:
        errors.append("BLOCKED_TOO_SMALL")
        block_reason = block_reason or "BLOCKED_TOO_SMALL"
    if reference_notional < min_order_value:
        errors.append("BELOW_MIN_ORDER_VALUE")
        block_reason = block_reason or "BLOCKED_TOO_SMALL"
    allow_target_notional_price_drift = bool(order_policy.get("allow_target_notional_price_drift", False))
    if (
        not reduce_only
        and target_delta > 0
        and reference_notional > target_delta + Decimal("0.00000001")
        and not allow_target_notional_price_drift
    ):
        errors.append("BLOCKED_TARGET_NOTIONAL_EXCEEDED")
        block_reason = block_reason or "BLOCKED_TARGET_NOTIONAL_EXCEEDED"
    elif (
        not reduce_only
        and target_delta > 0
        and reference_notional > target_delta + Decimal("0.00000001")
        and allow_target_notional_price_drift
    ):
        warnings.append("target notional exceeded only because price moved after quantity-based proportional sizing")
    if reduce_only:
        warnings.append("reduce-only orders still obey observed Hyperliquid minimum order value; below-min closes are locally blocked")

    return ValidatedOrderParams(
        dex=dex_key,
        canonical_coin=canonical,
        asset_id=asset_id,
        action=str(action or ""),
        side=side_u,
        cloid=cloid,
        cloid_sdk_type=cloid_sdk_type,
        is_buy=is_buy,
        reduce_only=reduce_only,
        aggressive_market=aggressive_market,
        tif=tif,
        raw_size=raw_size_d,
        rounded_size=rounded_size,
        raw_price=raw_price_d,
        raw_limit_price=raw_limit_price,
        rounded_price=rounded_price,
        estimated_notional=estimated_notional,
        target_delta_notional=target_delta,
        min_order_value=min_order_value,
        passes_min_order_value=passes_min,
        sz_decimals=sz_decimals,
        price_decimals=price_decimals,
        tick_size=tick_size,
        max_leverage=max_leverage,
        effective_leverage=effective_leverage,
        errors=errors,
        warnings=warnings,
        block_reason=block_reason,
    )


def check_hyperliquid_available_margin(
    *,
    account_state: dict[str, Any] | None,
    target_notional: Decimal | None,
    effective_leverage: int | None,
) -> bool | None:
    if target_notional is None or effective_leverage is None:
        return None
    if target_notional <= 0:
        return True
    if not account_state:
        return False
    available = _decimal_or_none(account_state.get("withdrawable"))
    if available is None:
        margin_summary = account_state.get("marginSummary") or {}
        available = _decimal_or_none(margin_summary.get("accountValue"))
    if available is None:
        return False
    required_margin = (abs(target_notional) / Decimal(effective_leverage)).quantize(
        Decimal("0.00000001"), rounding=ROUND_HALF_UP
    )
    return available >= required_margin


def build_hyperliquid_ioc_order(
    *,
    coin: str,
    dex: str = "",
    is_buy: bool,
    quantity: Decimal,
    reference_price: Decimal,
    slippage_bps: int,
    reduce_only: bool,
    cloid: str,
    sz_decimals: int | None = None,
) -> dict[str, Any]:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if reference_price <= 0:
        raise ValueError("reference_price must be positive")
    order_quantity = _round_hyperliquid_size(quantity, sz_decimals=sz_decimals)
    if order_quantity <= 0:
        raise ValueError("quantity rounded to zero for Hyperliquid market precision")
    limit_px = aggressive_hyperliquid_ioc_limit_price(
        reference_price,
        is_buy=is_buy,
        sz_decimals=sz_decimals,
    )
    return {
        "coin": canonical_coin(dex=dex, coin=coin),
        "dex": str(dex or "").lower(),
        "is_buy": is_buy,
        "sz": order_quantity,
        "limit_px": round_hyperliquid_limit_price(limit_px, is_buy=is_buy, sz_decimals=sz_decimals),
        "order_type": {"limit": {"tif": "Ioc"}},
        "reduce_only": reduce_only,
        "cloid": cloid,
    }


def aggressive_hyperliquid_ioc_limit_price(
    reference_price: Decimal,
    *,
    is_buy: bool,
    sz_decimals: int | None = None,
) -> Decimal:
    if reference_price <= 0:
        return Decimal("0")
    if is_buy:
        return reference_price * AGGRESSIVE_IOC_BUY_PRICE_MULTIPLIER
    raw_price = reference_price * AGGRESSIVE_IOC_SELL_PRICE_MULTIPLIER
    return max(raw_price, _minimum_hyperliquid_price(sz_decimals=sz_decimals))


def _minimum_hyperliquid_price(*, sz_decimals: int | None = None) -> Decimal:
    if sz_decimals is None:
        return Decimal("0.00000001")
    places = max(0, 6 - int(sz_decimals))
    return Decimal("1").scaleb(-places)


def round_hyperliquid_limit_price(
    price: Decimal,
    *,
    is_buy: bool,
    sz_decimals: int | None = None,
) -> Decimal:
    if sz_decimals is None:
        return price.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
    places = hyperliquid_price_decimals(price, sz_decimals=sz_decimals)
    quantum = Decimal("1").scaleb(-places)
    return price.quantize(quantum, rounding=ROUND_UP if is_buy else ROUND_DOWN)


def hyperliquid_price_decimals(price: Decimal, *, sz_decimals: int | None = None) -> int:
    if sz_decimals is None:
        return 8
    decimals = max(0, 6 - int(sz_decimals))
    sig_fig_decimals = max(0, 5 - price.adjusted() - 1)
    return min(decimals, sig_fig_decimals)


def _round_hyperliquid_size(quantity: Decimal, *, sz_decimals: int | None = None) -> Decimal:
    if sz_decimals is None:
        return quantity
    quantum = Decimal("1").scaleb(-max(0, int(sz_decimals)))
    return quantity.quantize(quantum, rounding=ROUND_DOWN)


def build_hyperliquid_cloid(
    *,
    leader_address: str,
    coin: str,
    dex: str = "",
    side: str,
    action: str,
    source_fill_id: str,
    timestamp_ms: int,
) -> str:
    trace = "|".join(
        [
            leader_address.lower(),
            str(dex or "").lower(),
            canonical_coin(dex=dex, coin=coin),
            side.upper(),
            action,
            source_fill_id,
            str(timestamp_ms),
        ]
    )
    return "0x" + hashlib.blake2s(trace.encode(), digest_size=16).hexdigest()


async def recover_hyperliquid_unknown_order(
    client: Any, *, coin: str, cloid: str
) -> dict[str, Any]:
    return await client.get_order_by_cloid(coin=coin, cloid=cloid)


class HyperliquidRiskSettingsService:
    def __init__(self, client: Any, *, expected_leverage: int = 10, settings: Any | None = None) -> None:
        self._client = client
        self.expected_leverage = expected_leverage
        self._settings = settings
        self.expected_margin_mode = _expected_margin_mode(settings)

    async def ensure_symbol_risk_settings(
        self,
        coin: str,
        *,
        reduce_only: bool = False,
        target_notional: Decimal | None = None,
    ) -> HyperliquidRiskSettingsResult:
        parsed = parse_coin(coin)
        coin_u = parsed.coin
        venue_coin = parsed.canonical_coin
        try:
            try:
                meta = await self._client.meta(parsed.dex)
            except TypeError:
                meta = await self._client.meta()
        except Exception as exc:
            return HyperliquidRiskSettingsResult(False, venue_coin, reason=f"cannot query Hyperliquid meta: {exc}")
        item = next(
            (
                row
                for row in meta.get("universe", [])
                if parse_coin(str(row.get("name", "")), default_dex=parsed.dex).canonical_coin == parsed.canonical_coin
            ),
            None,
        )
        if item is None:
            return HyperliquidRiskSettingsResult(
                is_ok=reduce_only,
                coin=venue_coin,
                reason="coin not in Hyperliquid universe",
                hyperliquid_coin_exists=False,
            )
        plan = build_hyperliquid_leverage_plan(
            default_leverage=self.expected_leverage,
            coin_max_leverage=item.get("maxLeverage"),
        )
        if not plan.ok_for_open and not reduce_only:
            return HyperliquidRiskSettingsResult(
                is_ok=False,
                coin=venue_coin,
                reason=plan.reason,
                max_leverage=plan.max_leverage,
                effective_leverage=plan.effective_leverage,
                hyperliquid_coin_exists=True,
                coin_max_leverage_loaded=plan.max_leverage is not None,
            )
        if not plan.ok_for_open and reduce_only:
            return HyperliquidRiskSettingsResult(
                is_ok=True,
                coin=venue_coin,
                reason=plan.reason,
                max_leverage=plan.max_leverage,
                effective_leverage=plan.effective_leverage,
                hyperliquid_coin_exists=True,
                coin_max_leverage_loaded=plan.max_leverage is not None,
                available_margin_sufficient=True,
            )

        account_state = await _maybe_account_state(self._client, dex=parsed.dex)
        margin_ok = check_hyperliquid_available_margin(
            account_state=account_state,
            target_notional=target_notional,
            effective_leverage=plan.effective_leverage,
        )
        abstraction_margin_blocker: str | None = None
        if margin_ok is False and target_notional is not None and not reduce_only:
            abstraction_margin_ok, abstraction_margin_blocker = await self._account_abstraction_margin_ok(
                dex=parsed.dex,
                target_notional=target_notional,
                effective_leverage=plan.effective_leverage,
            )
            if abstraction_margin_ok is not None:
                margin_ok = abstraction_margin_ok
        if margin_ok is False and not reduce_only:
            return HyperliquidRiskSettingsResult(
                is_ok=False,
                coin=venue_coin,
                leverage=plan.effective_leverage,
                margin_mode=self.expected_margin_mode,
                reason=abstraction_margin_blocker or "insufficient available margin for target notional",
                max_leverage=plan.max_leverage,
                effective_leverage=plan.effective_leverage,
                warning=plan.warning,
                hyperliquid_coin_exists=True,
                coin_max_leverage_loaded=True,
                available_margin_sufficient=False,
            )

        update_ok = False
        try:
            await self._client.update_leverage(
                coin=venue_coin,
                leverage=plan.effective_leverage,
                is_cross=_margin_mode_is_cross(self.expected_margin_mode),
            )
            update_ok = True
        except Exception as exc:
            if not reduce_only:
                return HyperliquidRiskSettingsResult(
                    is_ok=False,
                    coin=venue_coin,
                    margin_mode=self.expected_margin_mode,
                    reason=f"failed to set {self.expected_margin_mode.lower()} {plan.effective_leverage}x: {exc}",
                    max_leverage=plan.max_leverage,
                    effective_leverage=plan.effective_leverage,
                    warning=plan.warning,
                    hyperliquid_coin_exists=True,
                    coin_max_leverage_loaded=True,
                    available_margin_sufficient=margin_ok,
                )
        margin_mode_confirmed, leverage_confirmed = _risk_confirmed_from_account_state(
            account_state,
            coin=venue_coin,
            effective_leverage=plan.effective_leverage,
            expected_margin_mode=self.expected_margin_mode,
        )
        margin_mode_confirmed = margin_mode_confirmed or update_ok
        leverage_confirmed = leverage_confirmed or update_ok
        if (not margin_mode_confirmed or not leverage_confirmed) and not reduce_only:
            return HyperliquidRiskSettingsResult(
                is_ok=False,
                coin=venue_coin,
                leverage=plan.effective_leverage,
                margin_mode=self.expected_margin_mode,
                reason=f"could not confirm {self.expected_margin_mode.lower()} effective leverage",
                max_leverage=plan.max_leverage,
                effective_leverage=plan.effective_leverage,
                warning=plan.warning,
                hyperliquid_coin_exists=True,
                coin_max_leverage_loaded=True,
                margin_mode_confirmed=margin_mode_confirmed,
                isolated_confirmed=self.expected_margin_mode == "ISOLATED" and margin_mode_confirmed,
                leverage_confirmed=leverage_confirmed,
                available_margin_sufficient=margin_ok,
            )
        return HyperliquidRiskSettingsResult(
            is_ok=True,
            coin=venue_coin,
            leverage=plan.effective_leverage,
            margin_mode=self.expected_margin_mode,
            max_leverage=plan.max_leverage,
            effective_leverage=plan.effective_leverage,
            warning=plan.warning,
            hyperliquid_coin_exists=True,
            coin_max_leverage_loaded=True,
            margin_mode_confirmed=margin_mode_confirmed,
            isolated_confirmed=self.expected_margin_mode == "ISOLATED" and margin_mode_confirmed,
            leverage_confirmed=leverage_confirmed,
            available_margin_sufficient=True if reduce_only and margin_ok is None else margin_ok,
        )

    async def _account_abstraction_margin_ok(
        self,
        *,
        dex: str,
        target_notional: Decimal,
        effective_leverage: int | None,
    ) -> tuple[bool | None, str | None]:
        if self._settings is None or effective_leverage is None:
            return None, None
        account = getattr(self._client, "_vault_address", None) or getattr(self._client, "_account_address", None)
        if not account:
            return None, None
        try:
            dexes = ["", dex] if dex else [""]
            snapshot = await AccountAbstractionService(self._client, self._settings).fetch_snapshot(
                role="FOLLOWER",
                address=account,
                dexes=sorted(set(dexes)),
            )
            result = resolve_account_value_for_sizing(snapshot, dex, self._settings)
            if result.blockers:
                return False, "; ".join(result.blockers)
            ok, _required = available_collateral_sufficient(
                result,
                target_delta_notional=target_notional,
                effective_leverage=effective_leverage,
            )
            return ok, None if ok else "insufficient account-abstraction available collateral for target notional"
        except Exception as exc:
            return None, f"could not resolve account abstraction margin: {exc}"


class HyperliquidExecutionClient:
    def __init__(
        self,
        *,
        info_url: str,
        private_key: str | None = None,
        account_address: str | None = None,
        vault_address: str | None = None,
        network: str = "testnet",
        timeout: float = 10.0,
        order_submit_transport: str = "sdk",
    ) -> None:
        self._info_url = info_url
        self._private_key = private_key.strip() if isinstance(private_key, str) else private_key
        self._account_address = _clean_address(account_address)
        self._vault_address = _clean_address(vault_address)
        self._network = network
        self._order_submit_transport = str(order_submit_transport or "sdk").strip().lower()
        self._client = httpx.AsyncClient(timeout=timeout)
        self._exchange_cache: dict[str, Any] = {}
        self._exchange_order_locks: dict[str, asyncio.Lock] = {}
        self._sdk_coin_name_cache: dict[tuple[str, str], str] = {}
        self._nonce_state = _signer_nonce_state(private_key=self._private_key, network=self._network)

    @property
    def is_configured(self) -> bool:
        return bool(self._private_key and (self._account_address or self._vault_address))

    async def close(self) -> None:
        await self._client.aclose()

    async def post_info(self, payload: dict[str, Any]) -> Any:
        response = await self._client.post(self._info_url, json=payload)
        response.raise_for_status()
        return response.json()

    async def meta(self, dex: str = "") -> dict[str, Any]:
        payload: dict[str, Any] = {"type": "meta"}
        if dex:
            payload["dex"] = dex
        return await self.post_info(payload)

    async def all_mids(self, dex: str = "") -> dict[str, Any]:
        payload: dict[str, Any] = {"type": "allMids"}
        if dex:
            payload["dex"] = dex
        data = await self.post_info(payload)
        return dict(data or {})

    async def meta_and_asset_ctxs(self, dex: str = "") -> Any:
        payload: dict[str, Any] = {"type": "metaAndAssetCtxs"}
        if dex:
            payload["dex"] = dex
        return await self.post_info(payload)

    async def account_state(self, address: str | None = None, dex: str = "") -> dict[str, Any]:
        user = address or self._vault_address or self._account_address
        if not user:
            raise RuntimeError("Hyperliquid account address is not configured")
        payload: dict[str, Any] = {"type": "clearinghouseState", "user": user}
        if dex:
            payload["dex"] = dex
        return await self.post_info(payload)

    async def update_leverage(
        self,
        *,
        coin: str,
        leverage: int,
        is_cross: bool,
        asset_id: int | None = None,
    ) -> dict[str, Any]:
        parsed = parse_coin(coin)
        exchange = self._sdk_exchange(parsed.dex)
        if exchange is None:
            raise RuntimeError("Hyperliquid official SDK is not installed/configured")
        sdk_coin = self._resolve_sdk_coin_name(exchange, parsed)
        resolved_asset_id = int(asset_id) if asset_id is not None else _resolve_sdk_asset_id(exchange, sdk_coin)
        return await self._update_leverage_by_asset_id(
            exchange,
            asset_id=resolved_asset_id,
            leverage=leverage,
            is_cross=is_cross,
        )

    async def _update_leverage_by_asset_id(
        self,
        exchange: Any,
        *,
        asset_id: int,
        leverage: int,
        is_cross: bool,
    ) -> dict[str, Any]:
        try:
            from hyperliquid.exchange import MAINNET_API_URL, get_timestamp_ms, sign_l1_action
        except Exception as exc:
            raise RuntimeError("Hyperliquid official SDK signing helpers are unavailable") from exc

        async with self._signed_action_slot(None):
            timestamp = await self._next_action_nonce(get_timestamp_ms, None)
            action = {
                "type": "updateLeverage",
                "asset": int(asset_id),
                "isCross": bool(is_cross),
                "leverage": int(leverage),
            }
            signature = sign_l1_action(
                exchange.wallet,
                action,
                exchange.vault_address,
                timestamp,
                exchange.expires_after,
                exchange.base_url == MAINNET_API_URL,
            )
            payload: dict[str, Any] = {
                "action": action,
                "nonce": timestamp,
                "signature": signature,
            }
            payload["vaultAddress"] = (
                exchange.vault_address if action["type"] not in {"usdClassTransfer", "sendAsset"} else None
            )
            if exchange.expires_after is not None:
                payload["expiresAfter"] = exchange.expires_after
            response = await self._client.post(f"{exchange.base_url}/exchange", json=payload)
            response.raise_for_status()
            return response.json()

    async def place_market_order(self, **kwargs: Any) -> dict[str, Any]:
        latency_trace = kwargs.pop("_latency_trace", None)
        _trace_timestamp(latency_trace, "sdk_order_call_started_at")
        parsed = parse_coin(str(kwargs["coin"]), default_dex=str(kwargs.get("dex", "")))
        dex_name = str(parsed.dex or "").lower()
        _trace_detail(latency_trace, "sdk_exchange_cache_hit", dex_name in self._exchange_cache)
        _trace_detail(latency_trace, "sdk_exchange_cache_size_before", len(self._exchange_cache))
        exchange = self._sdk_exchange(parsed.dex)
        _trace_timestamp(latency_trace, "sdk_exchange_ready_at")
        _trace_detail(latency_trace, "sdk_exchange_cache_size_after", len(self._exchange_cache))
        if exchange is None:
            raise RuntimeError("Hyperliquid official SDK is not installed/configured")
        sdk_coin = self._resolve_sdk_coin_name(exchange, parsed)
        _trace_detail(latency_trace, "sdk_coin_name", sdk_coin)
        session = getattr(exchange, "session", None)
        if session is not None:
            _trace_detail(latency_trace, "sdk_session_id", id(session))
        size = kwargs.get("sz", kwargs.get("quantity"))
        if size is None:
            raise ValueError("Hyperliquid order size is missing")
        try:
            if self._order_submit_transport in {"http", "async_http", "direct_http"}:
                return await self._place_market_order_direct_http(
                    exchange=exchange,
                    dex_name=dex_name,
                    parsed=parsed,
                    sdk_coin=sdk_coin,
                    kwargs=kwargs,
                    size=size,
                    latency_trace=latency_trace,
                )
            return await self._place_market_order_sdk_locked(
                exchange=exchange,
                dex_name=dex_name,
                sdk_coin=sdk_coin,
                kwargs=kwargs,
                size=size,
                latency_trace=latency_trace,
            )
        finally:
            _trace_timestamp(latency_trace, "sdk_order_call_done_at")

    async def _place_market_order_sdk_locked(
        self,
        *,
        exchange: Any,
        dex_name: str,
        sdk_coin: str,
        kwargs: dict[str, Any],
        size: Any,
        latency_trace: dict[str, Any] | None,
    ) -> dict[str, Any]:
        order_lock = self._exchange_order_locks.setdefault(dex_name, asyncio.Lock())
        async with order_lock:
            previous_trace = getattr(exchange, "_copytrade_latency_trace", None)
            if isinstance(latency_trace, dict):
                exchange._copytrade_latency_trace = latency_trace
            try:
                return await asyncio.to_thread(
                    exchange.order,
                    sdk_coin,
                    kwargs["is_buy"],
                    float(size),
                    float(kwargs["limit_px"]),
                    {"limit": {"tif": "Ioc"}},
                    reduce_only=bool(kwargs.get("reduce_only", False)),
                    cloid=_sdk_cloid(kwargs.get("cloid")),
                )
            finally:
                if isinstance(latency_trace, dict):
                    if previous_trace is None:
                        try:
                            delattr(exchange, "_copytrade_latency_trace")
                        except AttributeError:
                            pass
                    else:
                        exchange._copytrade_latency_trace = previous_trace

    async def _place_market_order_direct_http(
        self,
        *,
        exchange: Any,
        dex_name: str,
        parsed: Any,
        sdk_coin: str,
        kwargs: dict[str, Any],
        size: Any,
        latency_trace: dict[str, Any] | None,
    ) -> dict[str, Any]:
        try:
            from hyperliquid.exchange import (
                MAINNET_API_URL,
                get_timestamp_ms,
                order_request_to_order_wire,
                order_wires_to_order_action,
                sign_l1_action,
            )
        except Exception:
            _trace_detail(latency_trace, "direct_http_unavailable_fallback", True)
            return await self._place_market_order_sdk_locked(
                exchange=exchange,
                dex_name=dex_name,
                sdk_coin=sdk_coin,
                kwargs=kwargs,
                size=size,
                latency_trace=latency_trace,
            )

        order_request = {
            "coin": sdk_coin,
            "is_buy": kwargs["is_buy"],
            "sz": float(size),
            "limit_px": float(kwargs["limit_px"]),
            "order_type": {"limit": {"tif": "Ioc"}},
            "reduce_only": bool(kwargs.get("reduce_only", False)),
        }
        cloid = _sdk_cloid(kwargs.get("cloid"))
        if cloid:
            order_request["cloid"] = cloid
        order_wire = order_request_to_order_wire(
            order_request,
            _resolve_sdk_asset_id(exchange, order_request["coin"]),
        )
        async with self._signed_action_slot(latency_trace):
            timestamp = await self._next_action_nonce(get_timestamp_ms, latency_trace)
            order_action = order_wires_to_order_action([order_wire], None, "na")
            signature = sign_l1_action(
                exchange.wallet,
                order_action,
                exchange.vault_address,
                timestamp,
                exchange.expires_after,
                exchange.base_url == MAINNET_API_URL,
            )
            payload: dict[str, Any] = {
                "action": order_action,
                "nonce": timestamp,
                "signature": signature,
            }
            payload["vaultAddress"] = (
                exchange.vault_address if order_action["type"] not in {"usdClassTransfer", "sendAsset"} else None
            )
            if exchange.expires_after is not None:
                payload["expiresAfter"] = exchange.expires_after
            _trace_timestamp(latency_trace, "sdk_http_payload_built_at")
            try:
                _trace_timestamp(latency_trace, "sdk_http_post_started_at")
                response = await self._client.post(f"{exchange.base_url}/exchange", json=payload)
                _trace_timestamp(latency_trace, "sdk_http_post_done_at")
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                _trace_timestamp(latency_trace, "sdk_http_post_done_at")
                _trace_detail(latency_trace, "sdk_http_post_error", type(exc).__name__)
                raise

    async def _next_action_nonce(self, get_timestamp_ms: Any, latency_trace: dict[str, Any] | None) -> int:
        _trace_timestamp(latency_trace, "sdk_nonce_wait_started_at")
        with self._nonce_state.lock:
            raw_nonce = int(get_timestamp_ms())
            nonce = raw_nonce if raw_nonce > self._nonce_state.last_nonce else self._nonce_state.last_nonce + 1
            self._nonce_state.last_nonce = nonce
        _trace_timestamp(latency_trace, "sdk_nonce_allocated_at")
        _trace_detail(latency_trace, "sdk_nonce", nonce)
        return nonce

    def _signed_action_slot(self, latency_trace: dict[str, Any] | None) -> Any:
        return _SignedActionSlot(self._nonce_state.submit_semaphore, latency_trace)

    async def get_order_by_cloid(self, *, coin: str, cloid: str) -> dict[str, Any]:
        user = self._vault_address or self._account_address
        if not user:
            raise RuntimeError("Hyperliquid account address is not configured")
        return await self.post_info({"type": "orderStatus", "user": user, "oid": cloid})

    async def cancel_by_cloid(self, *, coin: str, cloid: str) -> dict[str, Any]:
        parsed = parse_coin(coin)
        exchange = self._sdk_exchange(parsed.dex)
        if exchange is None:
            raise RuntimeError("Hyperliquid official SDK is not installed/configured")
        try:
            from hyperliquid.exchange import MAINNET_API_URL, get_timestamp_ms, sign_l1_action
        except Exception as exc:
            raise RuntimeError("Hyperliquid official SDK signing helpers are unavailable") from exc

        sdk_coin = self._resolve_sdk_coin_name(exchange, parsed)
        sdk_cloid = _sdk_cloid(cloid)
        if not hasattr(sdk_cloid, "to_raw"):
            raise ValueError("invalid Hyperliquid cloid")
        async with self._signed_action_slot(None):
            timestamp = await self._next_action_nonce(get_timestamp_ms, None)
            action = {
                "type": "cancelByCloid",
                "cancels": [
                    {
                        "asset": _resolve_sdk_asset_id(exchange, sdk_coin),
                        "cloid": sdk_cloid.to_raw(),
                    }
                ],
            }
            signature = sign_l1_action(
                exchange.wallet,
                action,
                exchange.vault_address,
                timestamp,
                exchange.expires_after,
                exchange.base_url == MAINNET_API_URL,
            )
            payload: dict[str, Any] = {
                "action": action,
                "nonce": timestamp,
                "signature": signature,
            }
            payload["vaultAddress"] = (
                exchange.vault_address if action["type"] not in {"usdClassTransfer", "sendAsset"} else None
            )
            if exchange.expires_after is not None:
                payload["expiresAfter"] = exchange.expires_after
            response = await self._client.post(f"{exchange.base_url}/exchange", json=payload)
            response.raise_for_status()
            return response.json()

    def warm_exchanges(self, dexes: list[str] | tuple[str, ...] | set[str]) -> list[str]:
        warmed: list[str] = []
        for dex in dexes:
            dex_name = str(dex or "").lower()
            if self._sdk_exchange(dex_name) is not None:
                warmed.append(dex_name)
        return warmed

    @staticmethod
    def manual_order_record(*, coin: str, side: str, source_type: str, dex: str = "") -> dict[str, str]:
        parsed = parse_coin(coin, default_dex=dex)
        return {
            "execution_venue": "HYPERLIQUID",
            "dex": parsed.dex,
            "canonical_coin": parsed.canonical_coin,
            "venue_symbol": parsed.canonical_coin,
            "side": side.upper(),
            "source_type": source_type,
        }

    def _sdk_exchange(self, dex: str = "") -> Any | None:
        if not self._private_key:
            return None
        dex_name = str(dex or "").lower()
        if dex_name in self._exchange_cache:
            return self._exchange_cache[dex_name]
        try:
            import eth_account
            from hyperliquid.exchange import Exchange
            from hyperliquid.utils import constants
        except Exception:
            return None
        base_url = (
            constants.TESTNET_API_URL
            if self._network.lower() == "testnet"
            else constants.MAINNET_API_URL
        )
        wallet = eth_account.Account.from_key(self._private_key)
        exchange = Exchange(
            wallet,
            base_url,
            account_address=self._account_address,
            vault_address=self._vault_address,
            perp_dexs=[dex_name],
        )
        exchange._post_action = _post_action_without_null_fields(exchange)  # type: ignore[method-assign]
        self._exchange_cache[dex_name] = exchange
        self._prime_sdk_coin_name_cache(exchange, dex_name)
        return exchange

    def _resolve_sdk_coin_name(self, exchange: Any, parsed: Any) -> str:
        key = (str(parsed.dex or "").lower(), str(parsed.canonical_coin or "").upper())
        cached = self._sdk_coin_name_cache.get(key)
        if cached:
            return cached
        self._prime_sdk_coin_name_cache(exchange, key[0])
        cached = self._sdk_coin_name_cache.get(key)
        if cached:
            return cached
        candidate = _sdk_coin_name(parsed)
        self._sdk_coin_name_cache[key] = candidate
        return candidate

    def _prime_sdk_coin_name_cache(self, exchange: Any, dex: str) -> None:
        coin_to_asset = getattr(getattr(exchange, "info", None), "coin_to_asset", None)
        if not isinstance(coin_to_asset, dict):
            return
        dex_key = str(dex or "").lower()
        for exact_name in coin_to_asset:
            exact_text = str(exact_name)
            if dex_key and ":" not in exact_text:
                continue
            if not dex_key and ":" in exact_text:
                continue
            parsed = parse_coin(exact_text, default_dex=dex_key)
            if parsed.dex != dex_key:
                continue
            key = (dex_key, str(parsed.canonical_coin or "").upper())
            self._sdk_coin_name_cache.setdefault(key, exact_text)


def _sdk_coin_name(parsed: Any) -> str:
    return parsed.canonical_coin if parsed.dex else parsed.coin


def _resolve_sdk_asset_id(exchange: Any, sdk_coin: str) -> int:
    info = getattr(exchange, "info", None)
    name_to_asset = getattr(info, "name_to_asset", None)
    if callable(name_to_asset):
        return int(name_to_asset(sdk_coin))
    coin_to_asset = getattr(info, "coin_to_asset", None)
    if isinstance(coin_to_asset, dict) and sdk_coin in coin_to_asset:
        return int(coin_to_asset[sdk_coin])
    raise RuntimeError(f"cannot resolve Hyperliquid asset id for {sdk_coin}")


def _sdk_cloid(value: Any) -> Any:
    if not value:
        return None
    if hasattr(value, "to_raw"):
        return value
    try:
        from hyperliquid.utils.types import Cloid

        return Cloid.from_str(str(value))
    except Exception:
        return value


def _valid_cloid_string(value: str) -> bool:
    if not isinstance(value, str) or len(value) != 34 or not value.startswith("0x"):
        return False
    try:
        int(value[2:], 16)
    except ValueError:
        return False
    return True


def _clean_address(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _post_action_without_null_fields(exchange: Any) -> Any:
    def post_action(action: dict[str, Any], signature: Any, nonce: int) -> Any:
        latency_trace = getattr(exchange, "_copytrade_latency_trace", None)
        payload: dict[str, Any] = {
            "action": action,
            "nonce": nonce,
            "signature": signature,
        }
        payload["vaultAddress"] = (
            exchange.vault_address if action["type"] not in {"usdClassTransfer", "sendAsset"} else None
        )
        if exchange.expires_after is not None:
            payload["expiresAfter"] = exchange.expires_after
        _trace_timestamp(latency_trace, "sdk_http_payload_built_at")
        try:
            _trace_timestamp(latency_trace, "sdk_http_post_started_at")
            response = exchange.post("/exchange", payload)
            _trace_timestamp(latency_trace, "sdk_http_post_done_at")
            return response
        except Exception as exc:
            _trace_timestamp(latency_trace, "sdk_http_post_done_at")
            _trace_detail(latency_trace, "sdk_http_post_error", type(exc).__name__)
            raise

    return post_action


def _trace_timestamp(trace: Any, key: str) -> None:
    if isinstance(trace, dict):
        trace[key] = datetime.now(timezone.utc).isoformat()


def _trace_detail(trace: Any, key: str, value: Any) -> None:
    if isinstance(trace, dict):
        trace[key] = value


def resolve_asset_id_from_meta(meta: dict[str, Any], *, coin: str, dex: str = "") -> int | None:
    parsed = parse_coin(coin, default_dex=dex)
    for index, item in enumerate(meta.get("universe", []) or []):
        listed = parse_coin(str(item.get("name", "")), default_dex=parsed.dex)
        if listed.canonical_coin == parsed.canonical_coin:
            return index
    return None


async def _maybe_account_state(client: Any, *, dex: str = "") -> dict[str, Any] | None:
    if not hasattr(client, "account_state"):
        return None
    try:
        try:
            return await client.account_state(dex=dex)
        except TypeError:
            return await client.account_state()
    except Exception:
        return None


def _risk_confirmed_from_account_state(
    account_state: dict[str, Any] | None,
    *,
    coin: str,
    effective_leverage: int | None,
    expected_margin_mode: str,
) -> tuple[bool, bool]:
    if not account_state or effective_leverage is None:
        return False, False
    expected = parse_coin(coin)
    for row in account_state.get("assetPositions", []):
        position = row.get("position", row)
        parsed = parse_coin(str(position.get("coin", "")), default_dex=expected.dex)
        if parsed.canonical_coin != expected.canonical_coin and parsed.coin != expected.coin:
            continue
        leverage = position.get("leverage") or {}
        if isinstance(leverage, dict):
            margin_type = _normalize_margin_mode(leverage.get("type"))
            value = _int_or_none(leverage.get("value"))
            return margin_type == _normalize_margin_mode(expected_margin_mode), value == effective_leverage
    return False, False


def _expected_margin_mode(settings: Any | None) -> str:
    return _normalize_margin_mode(getattr(settings, "hyperliquid_default_margin_mode", "CROSS"))


def _margin_mode_is_cross(value: str | None) -> bool:
    return _normalize_margin_mode(value) == "CROSS"


def _normalize_margin_mode(value: Any) -> str:
    text = str(value or "CROSS").upper()
    if text in {"CROSS", "CROSSED"}:
        return "CROSS"
    if text == "ISOLATED":
        return "ISOLATED"
    return text


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or str(value) == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None and str(value) != "":
            return value
    return None
