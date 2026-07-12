from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol


EXPECTED_MARGIN_TYPE = "ISOLATED"
EXPECTED_LEVERAGE = 10
ALREADY_ISOLATED_CODES = {"-4046"}


class RiskSettingsClient(Protocol):
    async def position_mode_dual_side(self) -> bool:
        ...

    async def change_position_mode(self, dual_side_position: bool) -> dict[str, Any]:
        ...

    async def open_orders_all(self) -> list[dict[str, Any]]:
        ...

    async def position_risk_all(self) -> list[dict[str, Any]]:
        ...

    async def change_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        ...

    async def change_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        ...

    async def position_risk(self, symbol: str) -> list[dict[str, Any]]:
        ...


@dataclass(frozen=True)
class PositionModeResult:
    is_ok: bool
    position_mode: str
    reason: str | None = None


@dataclass(frozen=True)
class RiskSettingsResult:
    symbol: str
    is_ok: bool
    margin_type: str | None
    leverage: int | None
    position_side: str = "HEDGE"
    position_mode: str = "HEDGE"
    current_notional: Decimal = Decimal("0")
    reason: str | None = None

    @classmethod
    def ok(
        cls,
        *,
        symbol: str,
        margin_type: str = EXPECTED_MARGIN_TYPE,
        leverage: int = EXPECTED_LEVERAGE,
        position_side: str = "HEDGE",
        position_mode: str = "HEDGE",
        current_notional: Decimal = Decimal("0"),
    ) -> "RiskSettingsResult":
        return cls(
            symbol=symbol,
            is_ok=True,
            margin_type=margin_type,
            leverage=leverage,
            position_side=position_side,
            position_mode=position_mode,
            current_notional=current_notional,
        )

    @classmethod
    def blocked(
        cls,
        *,
        symbol: str,
        reason: str,
        margin_type: str | None = None,
        leverage: int | None = None,
        position_side: str = "HEDGE",
        position_mode: str = "HEDGE",
        current_notional: Decimal = Decimal("0"),
    ) -> "RiskSettingsResult":
        return cls(
            symbol=symbol,
            is_ok=False,
            margin_type=margin_type,
            leverage=leverage,
            position_side=position_side,
            position_mode=position_mode,
            current_notional=current_notional,
            reason=reason,
        )


def parse_binance_error_code(message: str) -> str | None:
    for code in ALREADY_ISOLATED_CODES:
        if code in message:
            return code
    return None


class BinanceRiskSettingsService:
    def __init__(
        self,
        client: RiskSettingsClient,
        *,
        expected_margin_type: str = EXPECTED_MARGIN_TYPE,
        expected_leverage: int = EXPECTED_LEVERAGE,
        require_hedge_mode: bool = True,
        require_one_way: bool | None = None,
    ) -> None:
        self._client = client
        self._expected_margin_type = expected_margin_type.upper()
        self._expected_leverage = expected_leverage
        self._require_hedge_mode = require_hedge_mode
        self._require_one_way = bool(require_one_way) if require_one_way is not None else False

    async def ensure_account_position_mode_hedge(self) -> PositionModeResult:
        try:
            dual_side = await self._client.position_mode_dual_side()
        except Exception as exc:
            return PositionModeResult(
                is_ok=False,
                position_mode="UNKNOWN",
                reason=f"cannot query position mode: {exc}",
            )

        if dual_side:
            return PositionModeResult(is_ok=True, position_mode="HEDGE")

        try:
            open_orders = await self._client.open_orders_all()
            positions = await self._client.position_risk_all()
        except Exception as exc:
            return PositionModeResult(
                is_ok=False,
                position_mode="ONE_WAY",
                reason=f"cannot verify account is flat before switching to Hedge Mode: {exc}",
            )

        has_open_orders = bool(open_orders)
        has_open_positions = _has_open_positions(positions)
        if has_open_orders or has_open_positions:
            reasons: list[str] = []
            if has_open_orders:
                reasons.append("open orders")
            if has_open_positions:
                reasons.append("open positions")
            return PositionModeResult(
                is_ok=False,
                position_mode="ONE_WAY",
                reason=(
                    "Binance account is one-way mode and cannot switch to Hedge Mode while "
                    + " and ".join(reasons)
                    + " exist"
                ),
            )

        try:
            await self._client.change_position_mode(True)
            dual_side = await self._client.position_mode_dual_side()
        except Exception as exc:
            return PositionModeResult(
                is_ok=False,
                position_mode="ONE_WAY",
                reason=f"failed to switch account to Hedge Mode: {exc}",
            )

        if not dual_side:
            return PositionModeResult(
                is_ok=False,
                position_mode="ONE_WAY",
                reason="failed to confirm account Hedge Mode after switch",
            )
        return PositionModeResult(is_ok=True, position_mode="HEDGE")

    async def ensure_symbol_risk_settings(
        self, symbol: str, *, reduce_only: bool = False
    ) -> RiskSettingsResult:
        if self._require_one_way:
            return RiskSettingsResult.blocked(
                symbol=symbol,
                reason="one-way mode is no longer supported; expected Hedge Mode",
                position_mode="ONE_WAY",
            )

        if self._require_hedge_mode:
            mode = await self.ensure_account_position_mode_hedge()
            if not mode.is_ok:
                return RiskSettingsResult.blocked(
                    symbol=symbol,
                    reason=mode.reason or "Binance account is not in Hedge Mode",
                    position_side=mode.position_mode,
                    position_mode=mode.position_mode,
                )
        else:
            mode = PositionModeResult(is_ok=True, position_mode="UNKNOWN")

        try:
            dual_side = await self._client.position_mode_dual_side()
        except Exception as exc:
            return RiskSettingsResult.blocked(
                symbol=symbol,
                reason=f"cannot query position mode: {exc}",
                position_mode=mode.position_mode,
            )
        if self._require_hedge_mode and not dual_side:
            return RiskSettingsResult.blocked(
                symbol=symbol,
                reason="Binance account is one-way mode; expected Hedge Mode",
                position_side="ONE_WAY",
                position_mode="ONE_WAY",
            )

        try:
            await self._client.change_margin_type(symbol, self._expected_margin_type)
        except Exception as exc:
            if parse_binance_error_code(str(exc)) not in ALREADY_ISOLATED_CODES:
                if not reduce_only:
                    return RiskSettingsResult.blocked(
                        symbol=symbol,
                        reason=f"failed to set margin type {self._expected_margin_type}: {exc}",
                        position_mode="HEDGE",
                    )

        try:
            leverage_response = await self._client.change_leverage(
                symbol, self._expected_leverage
            )
        except Exception as exc:
            if not reduce_only:
                return RiskSettingsResult.blocked(
                    symbol=symbol,
                    reason=f"failed to set leverage {self._expected_leverage}: {exc}",
                    position_mode="HEDGE",
                )
            leverage_response = {}

        try:
            rows = await self._client.position_risk(symbol)
        except Exception as exc:
            return RiskSettingsResult.blocked(
                symbol=symbol,
                reason=f"cannot confirm position risk: {exc}",
                position_mode="HEDGE",
            )

        symbol_rows = [
            item for item in rows if str(item.get("symbol", "")).upper() == symbol.upper()
        ]
        if not symbol_rows:
            return RiskSettingsResult.blocked(
                symbol=symbol,
                reason="symbol not found in positionRisk",
                position_mode="HEDGE",
            )

        row = symbol_rows[0]
        margin_type = str(row.get("marginType", "")).upper()
        leverage_raw = row.get("leverage", leverage_response.get("leverage"))
        leverage = int(leverage_raw) if leverage_raw is not None else None
        sides = {str(item.get("positionSide", "UNKNOWN")).upper() for item in symbol_rows}
        position_side = ",".join(sorted(sides))
        current_notional = sum(
            (Decimal(str(item.get("notional", "0"))) for item in symbol_rows),
            Decimal("0"),
        )

        mismatched_margin = next(
            (
                item
                for item in symbol_rows
                if str(item.get("marginType", "")).upper() != self._expected_margin_type
            ),
            None,
        )
        if mismatched_margin is not None:
            margin_type = str(mismatched_margin.get("marginType", "")).upper()
            return RiskSettingsResult.blocked(
                symbol=symbol,
                reason=f"margin type is {margin_type}; expected {self._expected_margin_type}",
                margin_type=margin_type,
                leverage=leverage,
                position_side=position_side,
                position_mode="HEDGE",
                current_notional=current_notional,
            )
        mismatched_leverage = next(
            (
                item
                for item in symbol_rows
                if _safe_int(item.get("leverage", leverage_response.get("leverage")))
                != self._expected_leverage
            ),
            None,
        )
        if mismatched_leverage is not None:
            leverage = _safe_int(mismatched_leverage.get("leverage"))
            return RiskSettingsResult.blocked(
                symbol=symbol,
                reason=f"leverage is {leverage}; expected {self._expected_leverage}",
                margin_type=margin_type,
                leverage=leverage,
                position_side=position_side,
                position_mode="HEDGE",
                current_notional=current_notional,
            )
        if self._require_hedge_mode and not sides.intersection({"LONG", "SHORT"}):
            return RiskSettingsResult.blocked(
                symbol=symbol,
                reason=f"positionSide is {position_side}; expected LONG/SHORT Hedge Mode rows",
                margin_type=margin_type,
                leverage=leverage,
                position_side=position_side,
                position_mode="HEDGE",
                current_notional=current_notional,
            )
        return RiskSettingsResult.ok(
            symbol=symbol,
            margin_type=margin_type,
            leverage=leverage,
            position_side=position_side,
            position_mode="HEDGE" if dual_side else "ONE_WAY",
            current_notional=current_notional,
        )


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _has_open_positions(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        raw = row.get("positionAmt", row.get("notional", "0"))
        try:
            if Decimal(str(raw)) != 0:
                return True
        except Exception:
            return True
    return False
