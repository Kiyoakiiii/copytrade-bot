from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from sqlalchemy.dialects.postgresql import insert

from app.models import AppSetting

MODE_STANDARD = "STANDARD"
MODE_UNIFIED = "UNIFIED"
MODE_INFERRED_UNIFIED = "INFERRED_UNIFIED"
MODE_PORTFOLIO = "PORTFOLIO"
MODE_DEX_ABSTRACTION = "DEX_ABSTRACTION"
MODE_LEGACY = "LEGACY"
MODE_UNKNOWN = "UNKNOWN"

SOURCE_CLEARINGHOUSE = "CLEARINGHOUSE_STATE"
SOURCE_SPOT = "SPOT_CLEARINGHOUSE_STATE"
SOURCE_PORTFOLIO = "PORTFOLIO_STATE"
SOURCE_PORTFOLIO_HISTORY = "PORTFOLIO_HISTORY"
SOURCE_ACCOUNT_TOTAL = "CURRENT_ACCOUNT_TOTAL"
SOURCE_UNKNOWN = "UNKNOWN"

CONFIDENCE_HIGH = "HIGH"
CONFIDENCE_MEDIUM = "MEDIUM"
CONFIDENCE_LOW = "LOW"


@dataclass(frozen=True)
class SpotTokenBalance:
    token: str
    total: Decimal | None
    hold: Decimal | None
    available: Decimal | None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "token": self.token,
            "total": _decimal_str(self.total),
            "hold": _decimal_str(self.hold),
            "available": _decimal_str(self.available),
        }


@dataclass(frozen=True)
class ClearinghouseDigest:
    dex: str
    account_value: Decimal | None
    withdrawable: Decimal | None
    total_ntl_pos: Decimal | None
    total_margin_used: Decimal | None
    positions_count: int
    state_available: bool
    error_message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "dex": self.dex,
            "account_value": _decimal_str(self.account_value),
            "accountValue": _decimal_str(self.account_value),
            "withdrawable": _decimal_str(self.withdrawable),
            "total_ntl_pos": _decimal_str(self.total_ntl_pos),
            "totalNtlPos": _decimal_str(self.total_ntl_pos),
            "total_margin_used": _decimal_str(self.total_margin_used),
            "totalMarginUsed": _decimal_str(self.total_margin_used),
            "positions_count": self.positions_count,
            "positionsCount": self.positions_count,
            "state_available": self.state_available,
            "stateAvailable": self.state_available,
            "error_message": self.error_message,
            "errorMessage": self.error_message,
        }


@dataclass(frozen=True)
class AccountAbstractionSnapshot:
    address: str
    role: str
    mode: str
    inference: bool
    balance_source: str
    margin_source: str
    portfolio_state_available: bool
    spot_state_available: bool
    clearinghouse_state_available: bool
    spot_balances: dict[str, SpotTokenBalance] = field(default_factory=dict)
    clearinghouse_by_dex: dict[str, ClearinghouseDigest] = field(default_factory=dict)
    portfolio_account_value: Decimal | None = None
    portfolio_source: str | None = None
    spot_account_value: Decimal | None = None
    perp_account_value: Decimal | None = None
    account_total_value: Decimal | None = None
    account_total_source: str | None = None
    account_total_dexes: list[str] = field(default_factory=list)
    user_abstraction_available: bool = False
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    error_message: str | None = None
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "address": self.address,
            "account_abstraction_mode": self.mode,
            "accountAbstractionMode": self.mode,
            "mode": self.mode,
            "inference": self.inference,
            "balance_source": self.balance_source,
            "balanceSource": self.balance_source,
            "margin_source": self.margin_source,
            "marginSource": self.margin_source,
            "portfolio_state_available": self.portfolio_state_available,
            "portfolioStateAvailable": self.portfolio_state_available,
            "spot_state_available": self.spot_state_available,
            "spotStateAvailable": self.spot_state_available,
            "clearinghouse_state_available": self.clearinghouse_state_available,
            "clearinghouseStateAvailable": self.clearinghouse_state_available,
            "user_abstraction_available": self.user_abstraction_available,
            "userAbstractionAvailable": self.user_abstraction_available,
            "portfolio_account_value": _decimal_str(self.portfolio_account_value),
            "portfolioAccountValue": _decimal_str(self.portfolio_account_value),
            "portfolio_source": self.portfolio_source,
            "portfolioSource": self.portfolio_source,
            "spot_account_value": _decimal_str(self.spot_account_value),
            "spotAccountValue": _decimal_str(self.spot_account_value),
            "perp_account_value": _decimal_str(self.perp_account_value),
            "perpAccountValue": _decimal_str(self.perp_account_value),
            "account_total_value": _decimal_str(self.account_total_value),
            "accountTotalValue": _decimal_str(self.account_total_value),
            "account_total_source": self.account_total_source,
            "accountTotalSource": self.account_total_source,
            "account_total_dexes": self.account_total_dexes,
            "accountTotalDexes": self.account_total_dexes,
            "spot_balances": {token: balance.as_dict() for token, balance in self.spot_balances.items()},
            "spotBalances": {token: balance.as_dict() for token, balance in self.spot_balances.items()},
            "clearinghouse_by_dex": {
                dex: state.as_dict() for dex, state in self.clearinghouse_by_dex.items()
            },
            "clearinghouseByDex": {
                dex: state.as_dict() for dex, state in self.clearinghouse_by_dex.items()
            },
            "updated_at": self.updated_at.isoformat(),
            "updatedAt": self.updated_at.isoformat(),
            "error_message": self.error_message,
            "errorMessage": self.error_message,
            "warnings": self.warnings,
        }


@dataclass(frozen=True)
class AccountValueResult:
    account_value: Decimal | None
    withdrawable_or_available: Decimal | None
    source: str
    mode: str
    confidence: str
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)
    collateral_token: str = "USDC"
    inference: bool = False

    @property
    def live_blocked(self) -> bool:
        return bool(self.blockers)

    def as_dict(self) -> dict[str, Any]:
        return {
            "account_value": _decimal_str(self.account_value),
            "accountValue": _decimal_str(self.account_value),
            "account_value_used_for_sizing": _decimal_str(self.account_value),
            "accountValueUsedForSizing": _decimal_str(self.account_value),
            "withdrawable_or_available": _decimal_str(self.withdrawable_or_available),
            "withdrawableOrAvailable": _decimal_str(self.withdrawable_or_available),
            "available_collateral_used_for_margin_check": _decimal_str(self.withdrawable_or_available),
            "availableCollateralUsedForMarginCheck": _decimal_str(self.withdrawable_or_available),
            "source": self.source,
            "account_value_source": self.source,
            "accountValueSource": self.source,
            "mode": self.mode,
            "account_abstraction_mode": self.mode,
            "accountAbstractionMode": self.mode,
            "confidence": self.confidence,
            "warnings": self.warnings,
            "blockers": self.blockers,
            "collateral_token": self.collateral_token,
            "collateralToken": self.collateral_token,
            "inference": self.inference,
            "live_blocked": self.live_blocked,
            "liveBlocked": self.live_blocked,
        }


class AccountAbstractionService:
    def __init__(self, info_client: Any, settings: Any | None = None) -> None:
        self._info_client = info_client
        self._settings = settings

    async def fetch_snapshot(
        self,
        *,
        role: str,
        address: str,
        dexes: list[str],
        collateral_token: str | None = None,
        confirmed_unified_fast: bool = False,
    ) -> AccountAbstractionSnapshot:
        errors: list[str] = []
        if confirmed_unified_fast:
            # The full refresh periodically reconfirms userAbstraction. Between
            # those checks, a confirmed unified execution account only needs
            # one lightweight spot balance request for sizing freshness.
            spot_state = await self._safe_spot(address, errors)
            spot_meta_and_asset_ctxs = None
            all_mids = None
            if spot_state is not None and _spot_state_requires_market_prices(spot_state):
                spot_meta_and_asset_ctxs = await self._safe_post(
                    {"type": "spotMetaAndAssetCtxs"},
                    errors,
                    "spotMetaAndAssetCtxs",
                )
                all_mids = await self._safe_post(
                    {"type": "allMids"},
                    errors,
                    "allMids",
                )
            return build_account_abstraction_snapshot(
                role=role,
                address=address,
                user_abstraction="unifiedAccount",
                portfolio_state=None,
                spot_state=spot_state,
                clearinghouse_by_dex={},
                settings=self._settings,
                error_message="; ".join(errors) if errors else None,
                collateral_token=collateral_token
                or _settings_str(self._settings, "default_collateral_token", "USDC"),
                spot_account_value=spot_account_value_from_response(
                    spot_state,
                    spot_meta_and_asset_ctxs=spot_meta_and_asset_ctxs,
                    all_mids=all_mids,
                ),
            )
        user_abstraction = await self._safe_post(
            {"type": "userAbstraction", "user": address},
            errors,
            "userAbstraction",
        )
        confirmed_mode = _confirmed_mode_from_user_abstraction(user_abstraction)
        # A confirmed unified account is sized from its spot collateral.  Do
        # not repeatedly probe portfolio endpoints that are irrelevant (and
        # may return 422) on every background balance refresh.
        portfolio_state = None
        if confirmed_mode in {None, MODE_PORTFOLIO}:
            portfolio_state = await self._safe_post(
                {"type": "portfolioState", "user": address},
                errors,
                "portfolioState",
            )
            if portfolio_state is None:
                portfolio_state = await self._safe_post(
                    {"type": "portfolio", "user": address},
                    errors,
                    "portfolio",
                )
            if portfolio_state is None:
                portfolio_state = await self._safe_post(
                    {"type": "batchPortfolioStates", "users": [address]},
                    errors,
                    "batchPortfolioStates",
                )
        spot_state = await self._safe_spot(address, errors)
        spot_meta_and_asset_ctxs = None
        all_mids = None
        # USDC and the other supported stable collateral tokens have a local
        # 1 USD fallback.  Market metadata is only needed when a non-stable
        # spot balance actually contributes to the account value.
        if spot_state is not None and _spot_state_requires_market_prices(spot_state):
            spot_meta_and_asset_ctxs = await self._safe_post(
                {"type": "spotMetaAndAssetCtxs"},
                errors,
                "spotMetaAndAssetCtxs",
            )
            all_mids = await self._safe_post(
                {"type": "allMids"},
                errors,
                "allMids",
            )
        spot_account_value = spot_account_value_from_response(
            spot_state,
            spot_meta_and_asset_ctxs=spot_meta_and_asset_ctxs,
            all_mids=all_mids,
        )
        clearinghouse_by_dex: dict[str, dict[str, Any] | None] = {}
        clearinghouse_dexes = list(dexes)
        if confirmed_mode == MODE_UNIFIED:
            requested = {str(dex or "").lower() for dex in dexes}
            clearinghouse_dexes = [
                dex
                for dex in _account_value_reference_dexes(self._settings)
                if dex in requested
            ]
            if not clearinghouse_dexes:
                clearinghouse_dexes = [""]
        for dex in clearinghouse_dexes:
            clearinghouse_by_dex[str(dex or "").lower()] = await self._safe_clearinghouse(
                address,
                dex=str(dex or "").lower(),
                errors=errors,
            )
        return build_account_abstraction_snapshot(
            role=role,
            address=address,
            user_abstraction=user_abstraction,
            portfolio_state=portfolio_state,
            spot_state=spot_state,
            clearinghouse_by_dex=clearinghouse_by_dex,
            settings=self._settings,
            error_message="; ".join(errors) if errors else None,
            collateral_token=collateral_token or _settings_str(self._settings, "default_collateral_token", "USDC"),
            spot_account_value=spot_account_value,
        )

    async def _safe_post(
        self,
        payload: dict[str, Any],
        errors: list[str],
        label: str,
    ) -> Any | None:
        if not hasattr(self._info_client, "post_info"):
            errors.append(f"{label}: post_info unavailable")
            return None
        try:
            return await self._info_client.post_info(payload)
        except Exception as exc:
            errors.append(f"{label}: {str(exc)[:120]}")
            return None

    async def _safe_spot(self, address: str, errors: list[str]) -> dict[str, Any] | None:
        try:
            if hasattr(self._info_client, "spot_clearinghouse_state"):
                return await self._info_client.spot_clearinghouse_state(address)
            return await self._info_client.post_info({"type": "spotClearinghouseState", "user": address})
        except Exception as exc:
            errors.append(f"spotClearinghouseState: {str(exc)[:120]}")
            return None

    async def _safe_clearinghouse(
        self,
        address: str,
        *,
        dex: str,
        errors: list[str],
    ) -> dict[str, Any] | None:
        try:
            if hasattr(self._info_client, "clearinghouse_state"):
                return await self._info_client.clearinghouse_state(address, dex=dex)
            if hasattr(self._info_client, "account_state"):
                return await self._info_client.account_state(address, dex=dex)
            payload: dict[str, Any] = {"type": "clearinghouseState", "user": address}
            if dex:
                payload["dex"] = dex
            return await self._info_client.post_info(payload)
        except Exception as exc:
            label = f"clearinghouseState[{dex or 'default'}]"
            errors.append(f"{label}: {str(exc)[:120]}")
            return None


def build_account_abstraction_snapshot(
    *,
    role: str,
    address: str,
    user_abstraction: Any | None,
    portfolio_state: Any | None,
    spot_state: dict[str, Any] | None,
    clearinghouse_by_dex: dict[str, dict[str, Any] | None],
    settings: Any | None = None,
    error_message: str | None = None,
    collateral_token: str = "USDC",
    spot_account_value: Decimal | None = None,
    updated_at: datetime | None = None,
) -> AccountAbstractionSnapshot:
    updated_at = updated_at or datetime.now(timezone.utc)
    spot_balances = _spot_balances(spot_state)
    if collateral_token.upper() not in spot_balances and spot_state:
        balance = spot_token_balance(spot_state, collateral_token)
        if balance.total is not None:
            spot_balances[collateral_token.upper()] = balance
    clearinghouse = {
        str(dex or "").lower(): _clearinghouse_digest(str(dex or "").lower(), state)
        for dex, state in clearinghouse_by_dex.items()
    }
    if spot_account_value is None:
        spot_account_value = _stable_spot_account_value_from_balances(spot_balances)
    perp_account_value, account_total_dexes = _perp_account_value_for_reference_dexes(clearinghouse, settings)
    account_total_value = _sum_positive_values(perp_account_value, spot_account_value)
    account_total_source = SOURCE_ACCOUNT_TOTAL if account_total_value is not None else None
    portfolio_account_value, portfolio_source = portfolio_value_from_response(portfolio_state)
    confirmed_mode = _confirmed_mode_from_user_abstraction(user_abstraction)
    warnings: list[str] = []
    mode: str
    inference = False
    forced_mode = _settings_str(settings, "account_value_mode", "auto").lower()

    if forced_mode == "unified":
        mode = MODE_UNIFIED
        warnings.append("ACCOUNT_VALUE_MODE=unified forced by configuration")
    elif forced_mode == "portfolio":
        mode = MODE_PORTFOLIO
        warnings.append("ACCOUNT_VALUE_MODE=portfolio forced by configuration")
    elif confirmed_mode:
        mode = confirmed_mode
    else:
        mode = _infer_mode(
            spot_balances=spot_balances,
            clearinghouse_by_dex=clearinghouse,
            portfolio_account_value=portfolio_account_value,
            collateral_token=collateral_token,
        )
        inference = True
        warnings.append("userAbstraction unavailable; account abstraction mode inferred")

    spot_available = spot_state is not None
    clearing_available = any(item.state_available for item in clearinghouse.values())
    portfolio_available = portfolio_account_value is not None
    balance_source = _balance_source_for_mode(mode, spot_balances, portfolio_account_value, collateral_token)
    margin_source = SOURCE_CLEARINGHOUSE if clearing_available else SOURCE_UNKNOWN
    if mode == MODE_PORTFOLIO and portfolio_available:
        margin_source = portfolio_source or SOURCE_PORTFOLIO
    elif mode in {MODE_UNIFIED, MODE_INFERRED_UNIFIED} and balance_source == SOURCE_PORTFOLIO:
        margin_source = portfolio_source or SOURCE_PORTFOLIO

    return AccountAbstractionSnapshot(
        role=role.upper(),
        address=str(address).lower(),
        mode=mode,
        inference=inference,
        balance_source=balance_source,
        margin_source=margin_source,
        portfolio_state_available=portfolio_available,
        spot_state_available=spot_available,
        clearinghouse_state_available=clearing_available,
        spot_balances=spot_balances,
        clearinghouse_by_dex=clearinghouse,
        portfolio_account_value=portfolio_account_value,
        portfolio_source=portfolio_source,
        spot_account_value=spot_account_value,
        perp_account_value=perp_account_value,
        account_total_value=account_total_value,
        account_total_source=account_total_source,
        account_total_dexes=account_total_dexes,
        user_abstraction_available=user_abstraction is not None,
        updated_at=updated_at,
        error_message=error_message,
        warnings=warnings,
    )


def resolve_account_value_for_sizing(
    snapshot: AccountAbstractionSnapshot,
    dex: str,
    settings: Any | None = None,
    collateral_token: str | None = None,
) -> AccountValueResult:
    collateral_token = (collateral_token or _settings_str(settings, "default_collateral_token", "USDC")).upper()
    dex_key = str(dex or "").lower()
    mode = snapshot.mode
    forced = _settings_str(settings, "account_value_mode", "auto").lower()
    warnings = list(snapshot.warnings)
    blockers: list[str] = []
    clearinghouse = snapshot.clearinghouse_by_dex.get(dex_key)
    spot = snapshot.spot_balances.get(collateral_token)
    spot_total = spot.total if spot else None
    spot_available = spot.available if spot else None
    has_spot_collateral = bool(spot_total is not None and spot_total > 0)

    if collateral_token != "USDC" and not has_spot_collateral:
        blockers.append(
            f"collateral token {collateral_token} balance unavailable; cannot confirm HIP-3 collateral"
        )

    if forced == "standard_only" and mode in {MODE_UNIFIED, MODE_INFERRED_UNIFIED, MODE_PORTFOLIO, MODE_UNKNOWN}:
        blockers.append("ACCOUNT_VALUE_MODE=standard_only blocks unified/portfolio/unknown account abstraction")

    if forced == "unified":
        mode = MODE_UNIFIED
    elif forced == "portfolio":
        mode = MODE_PORTFOLIO

    if (
        snapshot.role.upper() == "LEADER"
        and forced == "auto"
        and mode in {MODE_STANDARD, MODE_DEX_ABSTRACTION, MODE_LEGACY}
        and snapshot.account_total_value is not None
        and snapshot.account_total_value > 0
    ):
        selected_withdrawable = clearinghouse.withdrawable if clearinghouse else None
        return AccountValueResult(
            account_value=snapshot.account_total_value,
            withdrawable_or_available=selected_withdrawable or snapshot.account_total_value,
            source=snapshot.account_total_source or SOURCE_ACCOUNT_TOTAL,
            mode=mode,
            confidence=CONFIDENCE_HIGH if not snapshot.inference else CONFIDENCE_MEDIUM,
            warnings=warnings
            + [
                "leader ACCOUNT_RATIO sizing uses current account total, not selected-dex clearinghouse accountValue"
            ],
            blockers=blockers,
            collateral_token=collateral_token,
            inference=snapshot.inference,
        )

    if mode in {MODE_STANDARD, MODE_DEX_ABSTRACTION, MODE_LEGACY} or forced == "standard_only":
        result = _resolve_from_clearinghouse(
            mode=mode,
            clearinghouse=clearinghouse,
            confidence=CONFIDENCE_HIGH if not snapshot.inference else CONFIDENCE_MEDIUM,
            warnings=warnings,
            blockers=blockers,
            collateral_token=collateral_token,
            inference=snapshot.inference,
        )
        if result.account_value is not None and result.account_value <= 0 and has_spot_collateral:
            result.blockers.append(
                "standard clearinghouse accountValue is zero while spot collateral exists; unified/portfolio mode is not confirmed"
            )
        return result

    if mode in {MODE_UNIFIED, MODE_INFERRED_UNIFIED}:
        if not _settings_bool(settings, "allow_unified_account_for_live", True):
            blockers.append("ALLOW_UNIFIED_ACCOUNT_FOR_LIVE=false")
        if mode == MODE_INFERRED_UNIFIED and _settings_bool(
            settings,
            "require_confirmed_account_abstraction_for_live",
            True,
        ):
            blockers.append("account abstraction was inferred, not confirmed by userAbstraction")
        source_pref = _settings_str(settings, "unified_account_collateral_source", "spot_or_portfolio").lower()
        if source_pref == "portfolio" and snapshot.portfolio_account_value is not None:
            return AccountValueResult(
                account_value=snapshot.portfolio_account_value,
                withdrawable_or_available=spot_available or snapshot.portfolio_account_value,
                source=snapshot.portfolio_source or SOURCE_PORTFOLIO,
                mode=mode,
                confidence=CONFIDENCE_HIGH if mode == MODE_UNIFIED and not snapshot.inference else CONFIDENCE_MEDIUM,
                warnings=warnings,
                blockers=blockers,
                collateral_token=collateral_token,
                inference=snapshot.inference,
            )
        if has_spot_collateral:
            return AccountValueResult(
                account_value=spot_total,
                withdrawable_or_available=spot_available,
                source=SOURCE_SPOT,
                mode=mode,
                confidence=CONFIDENCE_HIGH if mode == MODE_UNIFIED and not snapshot.inference else CONFIDENCE_MEDIUM,
                warnings=warnings,
                blockers=blockers,
                collateral_token=collateral_token,
                inference=snapshot.inference,
            )
        if snapshot.portfolio_account_value is not None and snapshot.portfolio_account_value > 0:
            return AccountValueResult(
                account_value=snapshot.portfolio_account_value,
                withdrawable_or_available=snapshot.portfolio_account_value,
                source=snapshot.portfolio_source or SOURCE_PORTFOLIO,
                mode=mode,
                confidence=CONFIDENCE_MEDIUM,
                warnings=warnings + ["using portfolio value because spot collateral balance is unavailable"],
                blockers=blockers,
                collateral_token=collateral_token,
                inference=snapshot.inference,
            )
        blockers.append(f"{collateral_token} unified collateral balance unavailable")
        return AccountValueResult(
            account_value=None,
            withdrawable_or_available=None,
            source=SOURCE_UNKNOWN,
            mode=mode,
            confidence=CONFIDENCE_LOW,
            warnings=warnings,
            blockers=blockers,
            collateral_token=collateral_token,
            inference=snapshot.inference,
        )

    if mode == MODE_PORTFOLIO:
        if not _settings_bool(settings, "allow_portfolio_margin_for_live", False):
            blockers.append("ALLOW_PORTFOLIO_MARGIN_FOR_LIVE=false")
        if snapshot.portfolio_account_value is not None and snapshot.portfolio_account_value > 0:
            return AccountValueResult(
                account_value=snapshot.portfolio_account_value,
                withdrawable_or_available=spot_available or snapshot.portfolio_account_value,
                source=snapshot.portfolio_source or SOURCE_PORTFOLIO,
                mode=mode,
                confidence=CONFIDENCE_HIGH if not snapshot.inference else CONFIDENCE_MEDIUM,
                warnings=warnings,
                blockers=blockers,
                collateral_token=collateral_token,
                inference=snapshot.inference,
            )
        if has_spot_collateral:
            return AccountValueResult(
                account_value=spot_total,
                withdrawable_or_available=spot_available,
                source=SOURCE_SPOT,
                mode=mode,
                confidence=CONFIDENCE_MEDIUM,
                warnings=warnings + ["portfolioState unavailable; using eligible spot collateral balance"],
                blockers=blockers,
                collateral_token=collateral_token,
                inference=snapshot.inference,
            )
        blockers.append("portfolio account value and available collateral unavailable")
        return AccountValueResult(
            account_value=None,
            withdrawable_or_available=None,
            source=SOURCE_UNKNOWN,
            mode=mode,
            confidence=CONFIDENCE_LOW,
            warnings=warnings,
            blockers=blockers,
            collateral_token=collateral_token,
            inference=snapshot.inference,
        )

    if has_spot_collateral:
        message = "Account abstraction unknown; spot USDC detected. Need unified/portfolio handling confirmation."
        warnings.append(message)
        if _settings_bool(settings, "require_confirmed_account_abstraction_for_live", True):
            blockers.append(message)
        return AccountValueResult(
            account_value=spot_total,
            withdrawable_or_available=spot_available,
            source=SOURCE_SPOT,
            mode=MODE_UNKNOWN,
            confidence=CONFIDENCE_LOW,
            warnings=warnings,
            blockers=blockers,
            collateral_token=collateral_token,
            inference=snapshot.inference,
        )

    if clearinghouse and clearinghouse.account_value is not None and clearinghouse.account_value > 0:
        return AccountValueResult(
            account_value=clearinghouse.account_value,
            withdrawable_or_available=clearinghouse.withdrawable,
            source=SOURCE_CLEARINGHOUSE,
            mode=MODE_UNKNOWN,
            confidence=CONFIDENCE_LOW,
            warnings=warnings + ["account abstraction unknown; falling back to clearinghouseState"],
            blockers=blockers,
            collateral_token=collateral_token,
            inference=snapshot.inference,
        )

    blockers.append("account value unavailable from clearinghouse, spot, or portfolio state")
    return AccountValueResult(
        account_value=None,
        withdrawable_or_available=None,
        source=SOURCE_UNKNOWN,
        mode=MODE_UNKNOWN,
        confidence=CONFIDENCE_LOW,
        warnings=warnings,
        blockers=blockers,
        collateral_token=collateral_token,
        inference=snapshot.inference,
    )


def required_initial_margin(target_delta_notional: Decimal, effective_leverage: int | Decimal) -> Decimal:
    leverage = Decimal(str(effective_leverage))
    if leverage <= 0:
        raise ValueError("effective_leverage must be positive")
    return (abs(target_delta_notional) / leverage).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


def available_collateral_sufficient(
    result: AccountValueResult,
    *,
    target_delta_notional: Decimal,
    effective_leverage: int | Decimal,
) -> tuple[bool, Decimal]:
    required = required_initial_margin(target_delta_notional, effective_leverage)
    available = result.withdrawable_or_available
    if available is None:
        return False, required
    return available >= required, required


def account_abstraction_setting_key(role: str, address: str) -> str:
    return f"account_abstraction:{role.upper()}:{str(address).lower()}"


async def save_account_abstraction_state(
    db: Any,
    *,
    snapshot: AccountAbstractionSnapshot,
    resolved_by_dex: dict[str, AccountValueResult],
) -> dict[str, Any]:
    payload = {
        **snapshot.as_dict(),
        "resolved_by_dex": {str(dex or "").lower(): result.as_dict() for dex, result in resolved_by_dex.items()},
        "resolvedByDex": {str(dex or "").lower(): result.as_dict() for dex, result in resolved_by_dex.items()},
    }
    now = datetime.now(timezone.utc)
    stmt = (
        insert(AppSetting)
        .values(key=account_abstraction_setting_key(snapshot.role, snapshot.address), value=payload, updated_at=now)
        .on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={"value": payload, "updated_at": now},
        )
    )
    await db.execute(stmt)
    return payload


async def load_account_abstraction_state(db: Any, *, role: str, address: str | None) -> dict[str, Any] | None:
    if not address:
        return None
    row = await db.get(AppSetting, account_abstraction_setting_key(role, address))
    if not row or not isinstance(row.value, dict):
        return None
    return dict(row.value)


def resolved_value_payload(payload: dict[str, Any] | None, dex: str = "") -> dict[str, Any] | None:
    if not payload:
        return None
    resolved = payload.get("resolved_by_dex") or payload.get("resolvedByDex") or {}
    value = resolved.get(str(dex or "").lower())
    return dict(value) if isinstance(value, dict) else None


def spot_token_balance(spot_state: dict[str, Any] | None, token: str = "USDC") -> SpotTokenBalance:
    token_u = token.upper()
    for row in (spot_state or {}).get("balances") or []:
        if str(row.get("coin") or row.get("token") or "").upper() != token_u:
            continue
        total = _decimal_or_none(row.get("total"))
        hold = _decimal_or_none(row.get("hold")) or Decimal("0")
        available = _decimal_or_none(row.get("available"))
        if available is None and total is not None:
            available = total - hold
        return SpotTokenBalance(token=token_u, total=total, hold=hold, available=available)
    return SpotTokenBalance(token=token_u, total=None, hold=None, available=None)


def spot_account_value_from_response(
    spot_state: dict[str, Any] | None,
    *,
    spot_meta_and_asset_ctxs: Any | None = None,
    all_mids: dict[str, Any] | None = None,
) -> Decimal | None:
    balances = (spot_state or {}).get("balances") or []
    if not balances:
        return None
    prices_by_token = _spot_mid_prices_by_token(spot_meta_and_asset_ctxs, all_mids or {})
    total = Decimal("0")
    found = False
    for row in balances:
        qty = _decimal_or_none(row.get("total"))
        if qty is None or qty == 0:
            continue
        coin = str(row.get("coin") or row.get("token") or "").upper()
        token = row.get("token")
        price = prices_by_token.get(str(token)) if token is not None else None
        if price is None:
            price = _spot_fallback_price(coin)
        if price is None:
            continue
        total += qty * price
        found = True
    return total if found else None


def portfolio_value_from_response(data: Any) -> tuple[Decimal | None, str | None]:
    if data is None:
        return None, None
    if isinstance(data, list):
        preferred = [item for item in data if str(item.get("time") or item.get("type") or "").lower() == "day"] if all(isinstance(item, dict) for item in data) else []
        for item in preferred + list(data):
            value, source = portfolio_value_from_response(item)
            if value is not None:
                return value, source
        return None, None
    if not isinstance(data, dict):
        return _decimal_or_none(data), SOURCE_PORTFOLIO
    for key in (
        "accountValue",
        "account_value",
        "totalAccountValue",
        "total_account_value",
        "portfolioValue",
        "portfolio_value",
    ):
        value = _decimal_or_none(data.get(key))
        if value is not None:
            return value, SOURCE_PORTFOLIO
    for key in ("marginSummary", "crossMarginSummary", "portfolio", "state"):
        nested = data.get(key)
        if isinstance(nested, (dict, list)):
            value, source = portfolio_value_from_response(nested)
            if value is not None:
                return value, source
    history = data.get("accountValueHistory")
    if isinstance(history, list) and history:
        last = history[-1]
        if isinstance(last, (list, tuple)) and len(last) >= 2:
            value = _decimal_or_none(last[1])
            if value is not None:
                return value, SOURCE_PORTFOLIO_HISTORY
        value = _decimal_or_none(last)
        if value is not None:
            return value, SOURCE_PORTFOLIO_HISTORY
    return None, None


def _spot_mid_prices_by_token(
    spot_meta_and_asset_ctxs: Any | None,
    all_mids: dict[str, Any],
) -> dict[str, Decimal]:
    prices: dict[str, Decimal] = {"0": Decimal("1")}
    if not isinstance(spot_meta_and_asset_ctxs, list) or not spot_meta_and_asset_ctxs:
        return prices
    meta = spot_meta_and_asset_ctxs[0]
    if not isinstance(meta, dict):
        return prices
    for market in meta.get("universe") or []:
        if not isinstance(market, dict):
            continue
        tokens = market.get("tokens") or []
        if len(tokens) < 2 or str(tokens[1]) != "0":
            continue
        token = str(tokens[0])
        keys = [str(market.get("name") or ""), f"@{market.get('index')}"]
        for key in keys:
            price = _decimal_or_none(all_mids.get(key))
            if price is not None and price > 0:
                prices[token] = price
                break
    return prices


def _spot_fallback_price(coin: str) -> Decimal | None:
    if coin in {"USDC", "USDT", "USDT0", "USDE", "USDH", "USDL", "USDXL", "USDHL"}:
        return Decimal("1")
    return None


def _spot_state_requires_market_prices(spot_state: dict[str, Any] | None) -> bool:
    for row in (spot_state or {}).get("balances") or []:
        quantity = _decimal_or_none(row.get("total"))
        if quantity is None or quantity == 0:
            continue
        coin = str(row.get("coin") or row.get("token") or "").upper()
        if _spot_fallback_price(coin) is None:
            return True
    return False


def _stable_spot_account_value_from_balances(
    spot_balances: dict[str, SpotTokenBalance],
) -> Decimal | None:
    total = Decimal("0")
    found = False
    for coin, balance in spot_balances.items():
        price = _spot_fallback_price(coin)
        if price is None or balance.total is None or balance.total == 0:
            continue
        total += balance.total * price
        found = True
    return total if found else None


def _perp_account_value_for_reference_dexes(
    clearinghouse_by_dex: dict[str, ClearinghouseDigest],
    settings: Any | None,
) -> tuple[Decimal | None, list[str]]:
    total = Decimal("0")
    used: list[str] = []
    for dex in _account_value_reference_dexes(settings):
        state = clearinghouse_by_dex.get(dex)
        if state is None or not state.state_available or state.account_value is None or state.account_value <= 0:
            continue
        total += state.account_value
        used.append(dex)
    return (total if used else None, used)


def _account_value_reference_dexes(settings: Any | None) -> list[str]:
    raw = _settings_str(settings, "account_value_reference_dexes", ",xyz")
    result: list[str] = []
    for part in raw.split(","):
        dex = str(part or "").strip().lower()
        if dex not in result:
            result.append(dex)
    if "" not in result:
        result.insert(0, "")
    return result


def _sum_positive_values(*values: Decimal | None) -> Decimal | None:
    total = Decimal("0")
    found = False
    for value in values:
        if value is None or value <= 0:
            continue
        total += value
        found = True
    return total if found else None


def _resolve_from_clearinghouse(
    *,
    mode: str,
    clearinghouse: ClearinghouseDigest | None,
    confidence: str,
    warnings: list[str],
    blockers: list[str],
    collateral_token: str,
    inference: bool,
) -> AccountValueResult:
    if clearinghouse is None or not clearinghouse.state_available:
        blockers.append("clearinghouseState unavailable for selected dex")
        return AccountValueResult(
            account_value=None,
            withdrawable_or_available=None,
            source=SOURCE_CLEARINGHOUSE,
            mode=mode,
            confidence=CONFIDENCE_LOW,
            warnings=warnings,
            blockers=blockers,
            collateral_token=collateral_token,
            inference=inference,
        )
    if clearinghouse.account_value is None or clearinghouse.account_value <= 0:
        blockers.append("clearinghouseState accountValue is zero for selected dex")
    return AccountValueResult(
        account_value=clearinghouse.account_value,
        withdrawable_or_available=clearinghouse.withdrawable,
        source=SOURCE_CLEARINGHOUSE,
        mode=mode,
        confidence=confidence,
        warnings=warnings,
        blockers=blockers,
        collateral_token=collateral_token,
        inference=inference,
    )


def _infer_mode(
    *,
    spot_balances: dict[str, SpotTokenBalance],
    clearinghouse_by_dex: dict[str, ClearinghouseDigest],
    portfolio_account_value: Decimal | None,
    collateral_token: str,
) -> str:
    token = collateral_token.upper()
    spot = spot_balances.get(token)
    spot_positive = bool(spot and spot.total is not None and spot.total > 0)
    clearing_values = [
        item.account_value
        for item in clearinghouse_by_dex.values()
        if item.state_available and item.account_value is not None
    ]
    any_clearing_positive = any(value > 0 for value in clearing_values)
    all_clearing_zero = bool(clearing_values) and all(value <= 0 for value in clearing_values)
    portfolio_positive = bool(portfolio_account_value is not None and portfolio_account_value > 0)
    default_value = clearinghouse_by_dex.get("", ClearinghouseDigest("", None, None, None, None, 0, False)).account_value

    if spot_positive and (all_clearing_zero or portfolio_positive):
        return MODE_INFERRED_UNIFIED
    if portfolio_positive and all_clearing_zero:
        return MODE_INFERRED_UNIFIED
    if any_clearing_positive:
        if default_value is not None and default_value > 0:
            return MODE_STANDARD
        return MODE_DEX_ABSTRACTION
    return MODE_UNKNOWN


def _confirmed_mode_from_user_abstraction(data: Any | None) -> str | None:
    if data is None:
        return None
    text = str(data).lower()
    if "portfolio" in text:
        return MODE_PORTFOLIO
    if "unified" in text:
        return MODE_UNIFIED
    if "dex" in text and "abstraction" in text:
        return MODE_DEX_ABSTRACTION
    if "legacy" in text:
        return MODE_LEGACY
    if "standard" in text:
        return MODE_STANDARD
    return None


def _balance_source_for_mode(
    mode: str,
    spot_balances: dict[str, SpotTokenBalance],
    portfolio_account_value: Decimal | None,
    collateral_token: str,
) -> str:
    token = collateral_token.upper()
    spot = spot_balances.get(token)
    spot_positive = bool(spot and spot.total is not None and spot.total > 0)
    if mode == MODE_PORTFOLIO and portfolio_account_value is not None:
        return SOURCE_PORTFOLIO
    if mode in {MODE_UNIFIED, MODE_INFERRED_UNIFIED, MODE_UNKNOWN} and spot_positive:
        return SOURCE_SPOT
    if mode in {MODE_UNIFIED, MODE_INFERRED_UNIFIED, MODE_PORTFOLIO} and portfolio_account_value is not None:
        return SOURCE_PORTFOLIO
    if mode in {MODE_STANDARD, MODE_DEX_ABSTRACTION, MODE_LEGACY}:
        return SOURCE_CLEARINGHOUSE
    return SOURCE_UNKNOWN


def _spot_balances(spot_state: dict[str, Any] | None) -> dict[str, SpotTokenBalance]:
    result: dict[str, SpotTokenBalance] = {}
    for row in (spot_state or {}).get("balances") or []:
        token = str(row.get("coin") or row.get("token") or "").upper()
        if not token:
            continue
        result[token] = spot_token_balance({"balances": [row]}, token)
    return result


def _clearinghouse_digest(dex: str, state: dict[str, Any] | None) -> ClearinghouseDigest:
    if not state:
        return ClearinghouseDigest(
            dex=dex,
            account_value=None,
            withdrawable=None,
            total_ntl_pos=None,
            total_margin_used=None,
            positions_count=0,
            state_available=False,
        )
    margin = state.get("marginSummary") or state.get("crossMarginSummary") or {}
    return ClearinghouseDigest(
        dex=dex,
        account_value=_decimal_or_none(margin.get("accountValue")),
        withdrawable=_decimal_or_none(state.get("withdrawable")),
        total_ntl_pos=_decimal_or_none(margin.get("totalNtlPos")),
        total_margin_used=_decimal_or_none(margin.get("totalMarginUsed")),
        positions_count=len(state.get("assetPositions") or []),
        state_available=bool(margin),
    )


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or str(value) == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _decimal_str(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _settings_str(settings: Any | None, name: str, default: str) -> str:
    value = getattr(settings, name, default) if settings is not None else default
    if value is None:
        return default
    return str(value)


def _settings_bool(settings: Any | None, name: str, default: bool) -> bool:
    value = getattr(settings, name, default) if settings is not None else default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}
