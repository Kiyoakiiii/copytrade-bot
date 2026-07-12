from __future__ import annotations

from dataclasses import dataclass
from typing import Any

AUTO_COPY_ORDER_POLICY = "FAST_MARKET_ONLY"
HYPERLIQUID_AUTO_COPY_ORDER_TYPE = "IOC_MARKET_EQUIVALENT"
BINANCE_AUTO_COPY_ORDER_TYPE = "MARKET"


class AutoCopyOrderPolicyError(ValueError):
    pass


@dataclass(frozen=True)
class AutoCopyOrderPolicyStatus:
    order_policy: str
    hyperliquid_auto_copy_order_type: str
    binance_auto_copy_order_type: str
    ok: bool = True


def auto_copy_order_policy_status() -> AutoCopyOrderPolicyStatus:
    return AutoCopyOrderPolicyStatus(
        order_policy=AUTO_COPY_ORDER_POLICY,
        hyperliquid_auto_copy_order_type=HYPERLIQUID_AUTO_COPY_ORDER_TYPE,
        binance_auto_copy_order_type=BINANCE_AUTO_COPY_ORDER_TYPE,
    )


def assert_auto_copy_order_policy(policy: str = AUTO_COPY_ORDER_POLICY) -> None:
    if policy != AUTO_COPY_ORDER_POLICY:
        raise AutoCopyOrderPolicyError("AUTO_COPY_ORDER_POLICY must be FAST_MARKET_ONLY")


def assert_hyperliquid_auto_copy_order(*, order_type: str, payload: dict[str, Any]) -> None:
    assert_auto_copy_order_policy()
    if order_type != HYPERLIQUID_AUTO_COPY_ORDER_TYPE:
        raise AutoCopyOrderPolicyError("Hyperliquid AUTO_COPY order_type must be IOC_MARKET_EQUIVALENT")
    order_spec = payload.get("order_type") or {}
    tif = (order_spec.get("limit") or {}).get("tif") if isinstance(order_spec, dict) else None
    if tif != "Ioc":
        raise AutoCopyOrderPolicyError("Hyperliquid AUTO_COPY must use tif=Ioc")
    if tif in {"Gtc", "Alo"}:
        raise AutoCopyOrderPolicyError("Hyperliquid AUTO_COPY must not use Gtc/Alo")
    if payload.get("post_only") or payload.get("postOnly"):
        raise AutoCopyOrderPolicyError("Hyperliquid AUTO_COPY must not be post-only")


def assert_binance_auto_copy_order(*, order_type: str, payload: dict[str, Any]) -> None:
    assert_auto_copy_order_policy()
    if order_type != BINANCE_AUTO_COPY_ORDER_TYPE:
        raise AutoCopyOrderPolicyError("Binance AUTO_COPY order_type must be MARKET")
    forbidden = {"price", "timeInForce", "reduceOnly", "closePosition"}
    present = sorted(key for key in forbidden if key in payload)
    if present:
        raise AutoCopyOrderPolicyError(f"Binance AUTO_COPY MARKET payload has forbidden fields: {', '.join(present)}")
    if payload.get("type") != BINANCE_AUTO_COPY_ORDER_TYPE:
        raise AutoCopyOrderPolicyError("Binance AUTO_COPY payload type must be MARKET")
    if payload.get("positionSide") not in {"LONG", "SHORT"}:
        raise AutoCopyOrderPolicyError("Binance AUTO_COPY payload must include positionSide LONG/SHORT")
