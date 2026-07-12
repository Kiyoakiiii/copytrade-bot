from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import httpx
import structlog
import websockets

from app.services.hyperliquid_dex import parse_coin

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class HyperliquidFill:
    source_fill_id: str
    leader_address: str
    coin: str
    dex: str
    canonical_coin: str
    raw_coin: str
    side: str
    price: str
    size: str
    time_ms: int
    raw: dict[str, Any]
    is_snapshot: bool = False


def fill_unique_id(leader_address: str, fill: dict[str, Any]) -> str:
    parts = [
        leader_address.lower(),
        str(fill.get("hash", "")),
        str(fill.get("tid", "")),
        str(fill.get("oid", "")),
        str(fill.get("time", "")),
        str(fill.get("coin", "")),
    ]
    return sha256("|".join(parts).encode("utf-8")).hexdigest()


class HyperliquidInfoClient:
    def __init__(self, info_url: str, timeout: float = 10.0) -> None:
        self._client = httpx.AsyncClient(timeout=timeout)
        self._info_url = info_url

    async def close(self) -> None:
        await self._client.aclose()

    async def post_info(self, payload: dict[str, Any]) -> Any:
        response = await self._client.post(self._info_url, json=payload)
        response.raise_for_status()
        return response.json()

    async def user_fills(self, user: str, aggregate_by_time: bool = False) -> list[dict[str, Any]]:
        data = await self.post_info(
            {"type": "userFills", "user": user, "aggregateByTime": aggregate_by_time}
        )
        return list(data or [])

    async def user_fills_by_time(
        self,
        user: str,
        start_time_ms: int,
        end_time_ms: int | None = None,
        aggregate_by_time: bool = False,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "type": "userFillsByTime",
            "user": user,
            "startTime": start_time_ms,
            "aggregateByTime": aggregate_by_time,
        }
        if end_time_ms is not None:
            payload["endTime"] = end_time_ms
        data = await self.post_info(payload)
        return list(data or [])

    async def clearinghouse_state(self, user: str, dex: str = "") -> dict[str, Any]:
        payload: dict[str, Any] = {"type": "clearinghouseState", "user": user}
        if dex:
            payload["dex"] = dex
        return await self.post_info(payload)

    async def open_orders(self, user: str) -> list[dict[str, Any]]:
        data = await self.post_info({"type": "openOrders", "user": user})
        return list(data or [])

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

    async def spot_clearinghouse_state(self, user: str) -> dict[str, Any]:
        return await self.post_info({"type": "spotClearinghouseState", "user": user})

    async def user_abstraction(self, user: str) -> Any:
        return await self.post_info({"type": "userAbstraction", "user": user})

    async def portfolio_state(self, user: str) -> Any:
        return await self.post_info({"type": "portfolioState", "user": user})

    async def portfolio(self, user: str) -> Any:
        return await self.post_info({"type": "portfolio", "user": user})


class HyperliquidWatcher:
    """Streams public leader fills and ignores snapshot messages for execution."""

    def __init__(
        self,
        *,
        ws_url: str,
        info_client: HyperliquidInfoClient,
        leader_addresses: Iterable[str],
        reconnect_seconds: float = 3.0,
    ) -> None:
        self._ws_url = ws_url
        self._info_client = info_client
        self._leaders = [addr.lower() for addr in leader_addresses]
        self._leader_set = set(self._leaders)
        self._reconnect_seconds = reconnect_seconds
        self._stopped = asyncio.Event()

    async def stop(self) -> None:
        self._stopped.set()

    def set_leaders(self, leader_addresses: Iterable[str]) -> None:
        self._leaders = [addr.lower() for addr in leader_addresses]
        self._leader_set = set(self._leaders)

    async def bootstrap_recent_fills(self) -> dict[str, list[HyperliquidFill]]:
        result: dict[str, list[HyperliquidFill]] = {}
        for leader in self._leaders:
            fills = await self._info_client.user_fills(leader)
            result[leader] = [self._parse_fill(leader, fill, is_snapshot=True) for fill in fills]
        return result

    async def stream_fills(self) -> AsyncIterator[HyperliquidFill]:
        while not self._stopped.is_set():
            try:
                async with websockets.connect(self._ws_url, ping_interval=20) as ws:
                    for leader in self._leaders:
                        await self._subscribe(ws, {"type": "userFills", "user": leader})
                        await self._subscribe(ws, {"type": "userEvents", "user": leader})
                        await self._subscribe(ws, {"type": "orderUpdates", "user": leader})
                        await self._subscribe(
                            ws, {"type": "clearinghouseState", "user": leader}
                        )
                    async for raw_message in ws:
                        for fill in self._parse_message(raw_message):
                            yield fill
            except Exception as exc:  # pragma: no cover - network loop
                log.warning("hyperliquid_ws_reconnect", error=str(exc))
                await asyncio.sleep(self._reconnect_seconds)

    async def _subscribe(self, ws: Any, subscription: dict[str, Any]) -> None:
        await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))

    def _parse_message(self, raw_message: str | bytes) -> list[HyperliquidFill]:
        message = json.loads(raw_message)
        channel = message.get("channel")
        data = message.get("data") or {}
        fills: list[dict[str, Any]] = []
        leader = str(data.get("user", "")).lower()
        is_snapshot = bool(data.get("isSnapshot"))

        if channel == "userFills":
            fills = list(data.get("fills") or [])
        elif channel == "userEvents" and isinstance(data.get("fills"), list):
            fills = list(data.get("fills") or [])
        else:
            return []

        if is_snapshot:
            return []
        if leader not in self._leader_set:
            return []
        return [self._parse_fill(leader, fill, is_snapshot=False) for fill in fills]

    def _parse_fill(
        self, leader: str, fill: dict[str, Any], *, is_snapshot: bool
    ) -> HyperliquidFill:
        return HyperliquidFill(
            source_fill_id=fill_unique_id(leader, fill),
            leader_address=leader,
            coin=parse_coin(str(fill.get("coin", ""))).coin,
            dex=parse_coin(str(fill.get("coin", ""))).dex,
            canonical_coin=parse_coin(str(fill.get("coin", ""))).canonical_coin,
            raw_coin=str(fill.get("coin", "")),
            side=str(fill.get("side", "")),
            price=str(fill.get("px", "0")),
            size=str(fill.get("sz", "0")),
            time_ms=int(fill.get("time", 0)),
            raw=fill,
            is_snapshot=is_snapshot,
        )
