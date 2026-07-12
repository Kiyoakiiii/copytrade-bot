from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Awaitable, Callable

from app.services.hedge_orders import HedgeAction, build_hedge_mode_order
from app.services.order_policy import (
    AUTO_COPY_ORDER_POLICY,
    BINANCE_AUTO_COPY_ORDER_TYPE,
    assert_auto_copy_order_policy,
    assert_binance_auto_copy_order,
)
from app.services.target_position import PositionSide

AUTO_COPY_ORDER_TYPE = BINANCE_AUTO_COPY_ORDER_TYPE
RECOVERY_ORDER_STATUSES = {"PENDING_SUBMIT", "SUBMITTING", "UNKNOWN", "SUBMITTED", "PARTIALLY_FILLED"}
CLIENT_ORDER_ID_RE = re.compile(r"^[.A-Z:/a-z0-9_-]{1,36}$")


@dataclass(frozen=True)
class MarketFill:
    executed_qty: Decimal
    avg_fill_price: Decimal | None
    cum_quote: Decimal | None
    slippage_bps: Decimal | None
    status: str


def build_auto_copy_market_order(
    *,
    symbol: str,
    allocation_side: PositionSide,
    action: str,
    quantity: Decimal,
    is_close_intent: bool = False,
    leader_allocation_qty: Decimal | None = None,
    binance_position_qty: Decimal | None = None,
) -> dict[str, Any]:
    hedge_order = build_hedge_mode_order(
        symbol=symbol,
        allocation_side=allocation_side,
        action=HedgeAction(action),
        quantity=quantity,
        is_close_intent=is_close_intent,
        leader_allocation_qty=leader_allocation_qty,
        binance_position_qty=binance_position_qty,
    )
    payload = {
        "symbol": hedge_order.symbol,
        "side": hedge_order.side,
        "positionSide": hedge_order.position_side,
        "type": AUTO_COPY_ORDER_TYPE,
        "quantity": hedge_order.quantity,
    }
    assert_auto_copy_order_policy(AUTO_COPY_ORDER_POLICY)
    assert_binance_auto_copy_order(order_type=AUTO_COPY_ORDER_TYPE, payload=payload)
    return payload


def build_auto_copy_new_client_order_id(
    *,
    leader_address: str,
    symbol: str,
    position_side: str,
    action: str,
    source_fill_id: str,
    timestamp_ms: int | None = None,
) -> str:
    timestamp_ms = timestamp_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
    leader_short = _sanitize(leader_address.lower().removeprefix("0x"))[:6].ljust(6, "0")
    symbol_short = _sanitize(symbol.upper())[:8]
    pos = "L" if position_side.upper() == "LONG" else "S"
    act = "O" if action == "OPEN_OR_INCREASE" else "C"
    fill_short = _sanitize(source_fill_id.lower())[:6].ljust(6, "0")
    ts = _base36(timestamp_ms % (36**6)).rjust(6, "0")[-6:]
    value = f"ct_{leader_short}_{symbol_short}_{pos}_{act}_{fill_short}_{ts}"
    if len(value) > 36 or CLIENT_ORDER_ID_RE.fullmatch(value) is None:
        raise ValueError("generated Binance client order id is invalid")
    return value


def extract_market_fill(response: dict[str, Any], *, estimated_price: Decimal) -> MarketFill:
    executed_qty = _decimal_from_response(response, "executedQty", "cumQty") or Decimal("0")
    avg_fill_price = _decimal_from_response(response, "avgPrice")
    cum_quote = _decimal_from_response(response, "cumQuote")
    if (avg_fill_price is None or avg_fill_price == 0) and cum_quote and executed_qty > 0:
        avg_fill_price = (cum_quote / executed_qty).quantize(
            Decimal("0.00000001"), rounding=ROUND_HALF_UP
        )
    slippage_bps: Decimal | None = None
    if avg_fill_price is not None and estimated_price > 0:
        slippage_bps = ((avg_fill_price - estimated_price) / estimated_price * Decimal("10000")).quantize(
            Decimal("0.00000001"), rounding=ROUND_HALF_UP
        )
    return MarketFill(
        executed_qty=executed_qty,
        avg_fill_price=avg_fill_price,
        cum_quote=cum_quote,
        slippage_bps=slippage_bps,
        status=str(response.get("status", "UNKNOWN")),
    )


async def recover_unknown_order_by_client_id(
    client: Any, *, symbol: str, client_order_id: str
) -> dict[str, Any]:
    return await client.get_order(symbol=symbol, orig_client_order_id=client_order_id)


def calculate_latency_fields(
    *,
    hyperliquid_event_time: datetime | None,
    event_received_at: datetime | None,
    binance_order_submit_at: datetime | None,
    binance_order_ack_at: datetime | None,
    order_finalized_at: datetime | None,
) -> dict[str, int | None]:
    return {
        "event_to_receive_ms": _delta_ms(hyperliquid_event_time, event_received_at),
        "receive_to_submit_ms": _delta_ms(event_received_at, binance_order_submit_at),
        "submit_to_ack_ms": _delta_ms(binance_order_submit_at, binance_order_ack_at),
        "event_to_ack_ms": _delta_ms(hyperliquid_event_time, binance_order_ack_at),
        "event_to_final_ms": _delta_ms(hyperliquid_event_time, order_finalized_at),
    }


class LeaderSymbolLockManager:
    def __init__(self) -> None:
        self._locks: dict[tuple[int, str, str, str, str, str], asyncio.Lock] = {}
        self._guard = asyncio.Lock()

    @asynccontextmanager
    async def lock(
        self,
        *,
        leader_id: int,
        symbol: str,
        position_side: str,
        execution_venue: str = "BINANCE",
        dex: str = "",
        canonical_coin: str | None = None,
    ):
        key = (
            leader_id,
            execution_venue.upper(),
            str(dex or "").lower(),
            (canonical_coin or symbol).upper(),
            symbol.upper(),
            position_side.upper(),
        )
        async with self._guard:
            lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            yield


class FillDrivenReconcileDispatcher:
    def __init__(
        self,
        reconcile: Callable[..., Awaitable[Any]],
        lock_manager: LeaderSymbolLockManager | None = None,
    ) -> None:
        self._reconcile = reconcile
        self._lock_manager = lock_manager or LeaderSymbolLockManager()

    async def handle_fill(
        self,
        *,
        leader_id: int,
        symbol: str,
        position_side: str,
        source_fill_id: str,
        execution_venue: str = "BINANCE",
        dex: str = "",
        canonical_coin: str | None = None,
    ) -> Any:
        async with self._lock_manager.lock(
            leader_id=leader_id,
            symbol=symbol,
            position_side=position_side,
            execution_venue=execution_venue,
            dex=dex,
            canonical_coin=canonical_coin,
        ):
            return await self._reconcile(
                leader_id=leader_id,
                symbol=symbol,
                position_side=position_side,
                source_fill_id=source_fill_id,
            )


def _delta_ms(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return int((end - start).total_seconds() * 1000)


def _sanitize(value: str) -> str:
    return re.sub(r"[^.A-Z:/a-z0-9_-]", "", value)


def _base36(value: int) -> str:
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if value == 0:
        return "0"
    result = ""
    while value:
        value, index = divmod(value, 36)
        result = alphabet[index] + result
    return result


def _decimal_from_response(response: dict[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        value = response.get(key)
        if value is not None and str(value) != "":
            return Decimal(str(value))
    return None
