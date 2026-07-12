from __future__ import annotations

import hmac
import time
from hashlib import sha256
from typing import Any
from urllib.parse import urlencode

import httpx
import structlog

from app.core.config import Settings
from app.core.logging import mask_value

log = structlog.get_logger(__name__)


class BinanceClientError(RuntimeError):
    pass


class BinanceFuturesClient:
    """Small USD-M Futures REST client.

    It only signs server-side requests. API keys never leave this process.
    """

    def __init__(self, settings: Settings, timeout: float = 10.0) -> None:
        api_key = settings.binance_api_key.get_secret_value() if settings.binance_api_key else ""
        api_secret = (
            settings.binance_api_secret.get_secret_value()
            if settings.binance_api_secret
            else ""
        )
        self._api_key = api_key
        self._api_secret = api_secret.encode("utf-8")
        self._base_url = settings.binance_base_url()
        self._hedge_mode = settings.binance_hedge_mode
        self._client = httpx.AsyncClient(base_url=self._base_url, timeout=timeout)

    async def close(self) -> None:
        await self._client.aclose()

    def _signed_params(self, params: dict[str, Any]) -> dict[str, Any]:
        if not self._api_key or not self._api_secret:
            raise BinanceClientError("Binance API credentials are not configured")
        payload = dict(params)
        payload["timestamp"] = int(time.time() * 1000)
        query = urlencode(payload, doseq=True)
        payload["signature"] = hmac.new(self._api_secret, query.encode(), sha256).hexdigest()
        return payload

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> dict[str, Any] | list[Any]:
        params = params or {}
        headers = {"X-MBX-APIKEY": self._api_key} if signed else {}
        safe_params = {k: ("***" if k == "signature" else v) for k, v in params.items()}
        try:
            response = await self._client.request(
                method,
                path,
                params=self._signed_params(params) if signed else params,
                headers=headers,
            )
        except httpx.TimeoutException as exc:
            raise BinanceClientError("Binance request timed out") from exc
        except httpx.TransportError as exc:
            raise BinanceClientError("Binance network error") from exc

        if response.status_code in {418, 429}:
            raise BinanceClientError("Binance rate limit reached")
        if response.status_code >= 400:
            log.warning(
                "binance_request_failed",
                path=path,
                status_code=response.status_code,
                params=safe_params,
                api_key=mask_value(self._api_key),
            )
            raise BinanceClientError(response.text[:500])
        return response.json()

    async def exchange_info(self) -> dict[str, Any]:
        return await self._request("GET", "/fapi/v1/exchangeInfo")  # type: ignore[return-value]

    async def account(self) -> dict[str, Any]:
        return await self._request("GET", "/fapi/v2/account", signed=True)  # type: ignore[return-value]

    async def account_equity(self) -> str:
        account = await self.account()
        return str(account.get("totalMarginBalance") or account.get("totalWalletBalance") or "0")

    async def positions(self) -> list[dict[str, Any]]:
        account = await self.account()
        return list(account.get("positions", []))

    async def position_mode_dual_side(self) -> bool:
        data = await self._request("GET", "/fapi/v1/positionSide/dual", signed=True)
        if isinstance(data, dict):
            return str(data.get("dualSidePosition", "false")).lower() == "true"
        raise BinanceClientError("unexpected position mode response")

    async def change_position_mode(self, dual_side_position: bool) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/fapi/v1/positionSide/dual",
            params={"dualSidePosition": "true" if dual_side_position else "false"},
            signed=True,
        )  # type: ignore[return-value]

    async def open_orders_all(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/fapi/v1/openOrders", signed=True)
        return list(data or [])  # type: ignore[arg-type]

    async def change_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/fapi/v1/marginType",
            params={"symbol": symbol, "marginType": margin_type.upper()},
            signed=True,
        )  # type: ignore[return-value]

    async def change_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/fapi/v1/leverage",
            params={"symbol": symbol, "leverage": leverage},
            signed=True,
        )  # type: ignore[return-value]

    async def position_risk(self, symbol: str) -> list[dict[str, Any]]:
        data = await self._request(
            "GET",
            "/fapi/v2/positionRisk",
            params={"symbol": symbol},
            signed=True,
        )
        return list(data or [])  # type: ignore[arg-type]

    async def position_risk_all(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/fapi/v2/positionRisk", signed=True)
        return list(data or [])  # type: ignore[arg-type]

    async def get_order(
        self,
        *,
        symbol: str,
        orig_client_order_id: str | None = None,
        order_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol}
        if orig_client_order_id:
            params["origClientOrderId"] = orig_client_order_id
        elif order_id:
            params["orderId"] = order_id
        else:
            raise BinanceClientError("order query requires origClientOrderId or orderId")
        return await self._request(
            "GET",
            "/fapi/v1/order",
            params=params,
            signed=True,
        )  # type: ignore[return-value]

    async def place_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: str,
        reduce_only: bool,
        position_side: str = "LONG",
        new_client_order_id: str | None = None,
        price: str | None = None,
        time_in_force: str | None = None,
    ) -> dict[str, Any]:
        if not self._hedge_mode:
            raise BinanceClientError("Hedge Mode is required for live Binance orders")
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": quantity,
            "newOrderRespType": "RESULT",
        }
        position_side = position_side.upper()
        if position_side not in {"LONG", "SHORT"}:
            raise BinanceClientError("Hedge Mode orders require positionSide LONG or SHORT")
        params["positionSide"] = position_side
        if new_client_order_id:
            params["newClientOrderId"] = new_client_order_id
        if order_type.upper() == "LIMIT":
            if not price or not time_in_force:
                raise BinanceClientError("LIMIT orders require price and timeInForce")
            params["price"] = price
            params["timeInForce"] = time_in_force
        return await self._request("POST", "/fapi/v1/order", params=params, signed=True)  # type: ignore[return-value]
