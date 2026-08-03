from __future__ import annotations

import logging
import re
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
    "telegram_bot_token",
    "telegram_control_bot_token",
    "secret",
    "password",
}

REDACTED = "[REDACTED]"
_HEX_SECRET_PATTERN = re.compile(r"(?i)(?<![0-9a-f])(?:0x)?[0-9a-f]{64}(?![0-9a-f])")
_TELEGRAM_BOT_TOKEN_PATTERN = re.compile(r"(?i)\d{6,12}:[A-Za-z0-9_-]{20,}(?![A-Za-z0-9_-])")


def redact_text(value: Any) -> str:
    redacted = _HEX_SECRET_PATTERN.sub(REDACTED, str(value))
    return _TELEGRAM_BOT_TOKEN_PATTERN.sub(REDACTED, redacted)


def mask_value(value: Any) -> Any:
    if value is None:
        return None
    return REDACTED


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
    if isinstance(value, str):
        return redact_text(value)
    if "api.telegram.org/bot" in str(value):
        return redact_text(value)
    return value


class SensitiveDataFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_text(record.msg)
        if isinstance(record.args, dict):
            record.args = {key: mask_payload(str(key), value) for key, value in record.args.items()}
        elif isinstance(record.args, tuple):
            record.args = tuple(
                redact_text(value)
                if isinstance(value, str) or "api.telegram.org/bot" in str(value)
                else value
                for value in record.args
            )
        return True


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))
    for handler in logging.getLogger().handlers:
        handler.addFilter(SensitiveDataFilter())
    # httpx logs full request URLs at INFO. Telegram embeds the bot token in
    # its URL path, so keep transport logs below that threshold.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
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
