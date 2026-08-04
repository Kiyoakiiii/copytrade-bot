from __future__ import annotations

from datetime import timezone
from typing import Any

from app.core.logging import redact_text


HYPERLIQUID_NETWORK_UPGRADE_POST_ONLY_REJECTION = (
    "HYPERLIQUID_NETWORK_UPGRADE_POST_ONLY_REJECTION"
)
COPY_ORDER_INSUFFICIENT_COLLATERAL = "COPY_ORDER_INSUFFICIENT_COLLATERAL"

EXECUTION_ALERT_EVENT_TYPES = {
    HYPERLIQUID_NETWORK_UPGRADE_POST_ONLY_REJECTION,
    COPY_ORDER_INSUFFICIENT_COLLATERAL,
}


def is_hyperliquid_network_upgrade_post_only_error(error: Any) -> bool:
    """Match only the exchange's explicit, definitely-unfilled upgrade gate."""

    normalized = " ".join(str(error or "").casefold().split())
    return (
        "only post-only orders allowed immediately after" in normalized
        and "network upgrade" in normalized
    )


def format_hyperliquid_network_upgrade_alert(event: Any) -> str:
    metadata = event.metadata_json if isinstance(getattr(event, "metadata_json", None), dict) else {}
    action = _action_label(metadata.get("order_action"))
    side = str(metadata.get("position_side") or "--").upper()
    quantity = str(metadata.get("quantity") or "--")
    order_id = str(metadata.get("order_id") or "--")
    symbol = str(metadata.get("canonical_coin") or getattr(event, "symbol", None) or "--")
    leader = str(getattr(event, "leader_address", None) or "--")
    created_at = getattr(event, "created_at", None)
    if created_at is not None:
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        created_label = created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        created_label = "--"
    return redact_text(
        "\n".join(
            [
                "🚨 Hyperliquid 跟单未成交",
                "原因：网络升级后临时仅允许 Post-Only 订单",
                f"时间：{created_label}",
                f"币种：{symbol}",
                f"动作：{action}",
                f"仓位方向：{side}",
                f"数量：{quantity}",
                f"Leader：{leader}",
                f"本地订单 ID：{order_id}",
                "机器人不会自动重试或补单，请立即检查实盘并手动处理。",
            ]
        )
    )


def format_copy_order_insufficient_collateral_alert(event: Any) -> str:
    metadata = event.metadata_json if isinstance(getattr(event, "metadata_json", None), dict) else {}
    action = _action_label(metadata.get("order_action"))
    side = str(metadata.get("position_side") or "--").upper()
    quantity = str(metadata.get("quantity") or "--")
    order_id = str(metadata.get("order_id") or "--")
    symbol = str(metadata.get("canonical_coin") or getattr(event, "symbol", None) or "--")
    leader = _address_suffix(getattr(event, "leader_address", None))
    account = str(metadata.get("execution_account_suffix") or "MAIN")
    required_margin = str(metadata.get("required_initial_margin") or "--")
    available_collateral = str(metadata.get("available_collateral") or "--")
    target_notional = str(metadata.get("target_notional") or "--")
    delta_notional = str(metadata.get("delta_notional") or "--")
    created_at = getattr(event, "created_at", None)
    if created_at is not None:
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        created_label = created_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        created_label = "--"
    return redact_text(
        "\n".join(
            [
                "🚨 跟单因可用保证金不足未下单",
                f"时间：{created_label}",
                f"账户：{account}",
                f"币种：{symbol}",
                f"动作：{action}",
                f"仓位方向：{side}",
                f"计划数量：{quantity}",
                f"目标名义价值：{target_notional} USDC",
                f"本笔名义价值：{delta_notional} USDC",
                f"所需初始保证金：{required_margin} USDC",
                f"当时可用保证金：{available_collateral} USDC",
                f"Leader：{leader}",
                f"本地订单 ID：{order_id}",
                "机器人没有向交易所发送该笔订单，也不会自动补单，请立即检查资金和实盘仓位。",
            ]
        )
    )


def format_execution_alert(event: Any) -> str:
    if getattr(event, "event_type", None) == COPY_ORDER_INSUFFICIENT_COLLATERAL:
        return format_copy_order_insufficient_collateral_alert(event)
    return format_hyperliquid_network_upgrade_alert(event)


def _action_label(value: Any) -> str:
    action = str(value or "").upper()
    return {
        "OPEN": "开仓",
        "INCREASE": "加仓",
        "REDUCE": "减仓",
        "CLOSE": "平仓",
        "FLIP_CLOSE_FIRST": "反手前平仓",
    }.get(action, action or "--")


def _address_suffix(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return "--"
    return normalized[-4:]
