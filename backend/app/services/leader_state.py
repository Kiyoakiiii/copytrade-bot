from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from app.services.target_position import PositionSide


class LeaderStateService:
    def __init__(self, info_client: Any) -> None:
        self._info_client = info_client

    async def fetch_state(self, leader_address: str) -> "LeaderState":
        raw = await self._info_client.clearinghouse_state(leader_address)
        return parse_leader_state(leader_address, raw, websocket_status="http_poll")


@dataclass(frozen=True)
class LeaderPosition:
    coin: str
    side: PositionSide
    size: Decimal
    notional: Decimal
    entry_price: Decimal | None
    mark_price: Decimal | None
    unrealized_pnl: Decimal | None
    leverage: Decimal | None


@dataclass(frozen=True)
class LeaderState:
    leader_address: str
    account_value: Decimal
    withdrawable: Decimal | None
    total_ntl_pos: Decimal | None
    total_margin_used: Decimal | None
    positions: list[LeaderPosition]
    websocket_status: str
    updated_at: datetime

    def is_stale(self, *, now: datetime | None = None, max_age_seconds: int = 10) -> bool:
        now = now or datetime.now(timezone.utc)
        return (now - self.updated_at).total_seconds() > max_age_seconds


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _decimal(value: Any, default: str = "0") -> Decimal:
    parsed = _decimal_or_none(value)
    return parsed if parsed is not None else Decimal(default)


def parse_leader_state(
    leader_address: str,
    clearinghouse_state: dict[str, Any],
    *,
    websocket_status: str = "connected",
    updated_at: datetime | None = None,
) -> LeaderState:
    updated_at = updated_at or datetime.now(timezone.utc)
    margin_summary = clearinghouse_state.get("marginSummary") or {}
    account_value = _decimal(margin_summary.get("accountValue"))
    positions: list[LeaderPosition] = []

    for item in clearinghouse_state.get("assetPositions") or []:
        position = item.get("position") or item
        coin = str(position.get("coin", "")).upper()
        size = _decimal(position.get("szi"))
        abs_notional = abs(
            _decimal(
                position.get(
                    "positionValue",
                    position.get("notional", Decimal("0")),
                )
            )
        )
        signed_notional = abs_notional if size >= 0 else -abs_notional
        side = PositionSide.FLAT
        if size > 0:
            side = PositionSide.LONG
        elif size < 0:
            side = PositionSide.SHORT

        mark_price = _decimal_or_none(position.get("markPx"))
        if mark_price is None and size != 0:
            mark_price = (abs_notional / abs(size)).quantize(Decimal("0.00000001"))

        leverage_value = position.get("leverage")
        if isinstance(leverage_value, dict):
            leverage_value = leverage_value.get("value")

        positions.append(
            LeaderPosition(
                coin=coin,
                side=side,
                size=size,
                notional=signed_notional,
                entry_price=_decimal_or_none(position.get("entryPx")),
                mark_price=mark_price,
                unrealized_pnl=_decimal_or_none(
                    position.get("unrealizedPnl", position.get("unrealizedPnlRaw"))
                ),
                leverage=_decimal_or_none(leverage_value),
            )
        )

    return LeaderState(
        leader_address=leader_address.lower(),
        account_value=account_value,
        withdrawable=_decimal_or_none(clearinghouse_state.get("withdrawable")),
        total_ntl_pos=_decimal_or_none(margin_summary.get("totalNtlPos")),
        total_margin_used=_decimal_or_none(margin_summary.get("totalMarginUsed")),
        positions=positions,
        websocket_status=websocket_status,
        updated_at=updated_at,
    )


def leader_state_to_json(state: LeaderState) -> dict[str, Any]:
    return {
        "leader_address": state.leader_address,
        "accountValue": str(state.account_value),
        "withdrawable": str(state.withdrawable) if state.withdrawable is not None else None,
        "totalNtlPos": str(state.total_ntl_pos) if state.total_ntl_pos is not None else None,
        "totalMarginUsed": str(state.total_margin_used)
        if state.total_margin_used is not None
        else None,
        "websocket_status": state.websocket_status,
        "updated_at": state.updated_at.isoformat(),
        "positions": [
            {
                "coin": position.coin,
                "side": position.side.value,
                "size": str(position.size),
                "notional": str(position.notional),
                "entry_price": str(position.entry_price)
                if position.entry_price is not None
                else None,
                "mark_price": str(position.mark_price)
                if position.mark_price is not None
                else None,
                "unrealized_pnl": str(position.unrealized_pnl)
                if position.unrealized_pnl is not None
                else None,
                "leverage": str(position.leverage) if position.leverage is not None else None,
            }
            for position in state.positions
        ],
    }
