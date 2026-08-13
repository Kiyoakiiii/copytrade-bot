from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import httpx
import structlog
import websockets

from app.services.hyperliquid_dex import parse_coin

log = structlog.get_logger(__name__)


def _info_retry_delay_seconds(response: httpx.Response, attempt: int) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return min(max(float(retry_after), 0.05), 2.0)
        except (TypeError, ValueError):
            pass
    return min(0.1 * (2**attempt), 0.8)


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
    def __init__(
        self,
        info_url: str,
        timeout: float = 10.0,
        *,
        min_request_interval_seconds: float = 0.0,
    ) -> None:
        self._client = httpx.AsyncClient(timeout=timeout)
        self._info_url = info_url
        # Position refresh, leader backfill and metadata warmup share this
        # client. Bound their concurrency so one startup/reconnect burst cannot
        # turn a single 429 into several synchronized retry loops.
        self._request_slots = asyncio.Semaphore(2)
        self._rate_limit_guard = asyncio.Lock()
        self._retry_not_before = 0.0
        self._request_spacing_guard = asyncio.Lock()
        self._min_request_interval_seconds = max(
            0.0,
            float(min_request_interval_seconds),
        )
        self._last_request_started_at = 0.0

    async def close(self) -> None:
        await self._client.aclose()

    async def post_info(self, payload: dict[str, Any]) -> Any:
        async with self._request_slots:
            response: httpx.Response | None = None
            for attempt in range(4):
                await self._wait_for_rate_limit_backoff()
                await self._wait_for_request_spacing()
                response = await self._client.post(self._info_url, json=payload)
                if response.status_code != 429 and response.status_code < 500:
                    response.raise_for_status()
                    return response.json()
                delay = _info_retry_delay_seconds(response, attempt)
                if response.status_code == 429:
                    # Add jitter so the dedicated watcher and backend poller on
                    # the same host do not remain phase-locked after a shared-IP
                    # rate-limit response. The client-wide deadline also makes
                    # queued callers join one backoff rather than start a storm.
                    delay += random.uniform(0.0, min(0.25, delay * 0.25))
                    await self._defer_info_requests(delay)
                if attempt >= 3:
                    response.raise_for_status()
                if response.status_code != 429:
                    await asyncio.sleep(delay)
        raise RuntimeError("Hyperliquid info request retry loop exited unexpectedly")

    async def _wait_for_request_spacing(self) -> None:
        if self._min_request_interval_seconds <= 0:
            return
        async with self._request_spacing_guard:
            now = asyncio.get_running_loop().time()
            delay = (
                self._last_request_started_at
                + self._min_request_interval_seconds
                - now
            )
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_request_started_at = asyncio.get_running_loop().time()

    async def _wait_for_rate_limit_backoff(self) -> None:
        while True:
            async with self._rate_limit_guard:
                delay = self._retry_not_before - asyncio.get_running_loop().time()
            if delay <= 0:
                return
            await asyncio.sleep(delay)

    async def _defer_info_requests(self, delay_seconds: float) -> None:
        retry_at = asyncio.get_running_loop().time() + max(0.0, float(delay_seconds))
        async with self._rate_limit_guard:
            self._retry_not_before = max(self._retry_not_before, retry_at)

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

    async def sub_accounts(self, user: str) -> list[dict[str, Any]]:
        data = await self.post_info({"type": "subAccounts", "user": user})
        return list(data or [])


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
