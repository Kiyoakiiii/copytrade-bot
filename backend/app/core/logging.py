from __future__ import annotations

import logging
from typing import Any

import structlog

SENSITIVE_KEYS = {
    "api_key",
    "api_secret",
    "authorization",
    "auth",
    "cookie",
    "set-cookie",
    "x-csrf-token",
    "signature",
    "binance_api_key",
    "binance_api_secret",
    "hyperliquid_private_key",
    "private_key",
    "api_wallet_key",
    "secret",
    "password",
}


def mask_value(value: Any) -> Any:
    if value is None:
        return None
    text = str(value)
    if len(text) <= 8:
        return "***"
    return f"{text[:4]}...{text[-4:]}"


def mask_event(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    for key in list(event_dict.keys()):
        event_dict[key] = mask_payload(key, event_dict[key])
    return event_dict


def mask_payload(key: str, value: Any) -> Any:
    key_l = key.lower()
    if key_l in SENSITIVE_KEYS or any(token in key_l for token in ("private_key", "api_secret", "authorization", "cookie")):
        return mask_value(value)
    if isinstance(value, dict):
        return {item_key: mask_payload(str(item_key), item_value) for item_key, item_value in value.items()}
    if isinstance(value, list):
        return [mask_payload(key, item) for item in value]
    return value


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            mask_event,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
