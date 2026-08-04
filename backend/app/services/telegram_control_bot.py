from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog
from sqlalchemy import select

from app.core.config import Settings
from app.core.logging import redact_text
from app.db.session import SessionLocal
from app.models import AppSetting, AuditLog, RiskEvent
from app.services.execution_alerts import (
    EXECUTION_ALERT_EVENT_TYPES,
    format_execution_alert,
)
from app.services.runtime_control import acquire_copy_trading_control_lock
from app.services.task_status import store_task_status

log = structlog.get_logger(__name__)

TELEGRAM_CONTROL_TASK_NAME = "telegram_control_bot"
TELEGRAM_CONTROL_OFFSET_KEY = "telegram_control_bot_offset"
TELEGRAM_EXECUTION_ALERT_TASK_NAME = "telegram_execution_alerts"
TELEGRAM_EXECUTION_ALERT_STATE_KEY = "telegram_execution_alert_state"
TELEGRAM_EXECUTION_ALERT_POLL_SECONDS = 1.0
TELEGRAM_EXECUTION_ALERT_HEARTBEAT_SECONDS = 60.0


class TelegramControlError(RuntimeError):
    pass


class TelegramControlTransientError(TelegramControlError):
    """A temporary Telegram/API failure that can be retried from the durable offset."""

    def __init__(self, message: str, *, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class TelegramControlTransportError(TelegramControlTransientError):
    """A transient transport failure that is safe to retry from the durable offset."""


def telegram_control_config_error(settings: Settings) -> str | None:
    if not settings.telegram_control_enabled:
        return None
    token = settings.telegram_control_bot_token_value()
    if not token or ":" not in token or any(character.isspace() for character in token):
        return "Telegram control bot token is missing or malformed"
    try:
        allowed_user_ids = settings.telegram_control_allowed_user_id_set()
    except ValueError:
        return "Telegram allowed user IDs must be comma-separated integers"
    if not allowed_user_ids:
        return "Telegram control requires at least one allowed user ID"
    if any(user_id <= 0 for user_id in allowed_user_ids):
        return "Telegram allowed user IDs must be positive integers"
    if float(settings.telegram_control_poll_timeout_seconds) <= 0:
        return "Telegram control poll timeout must be positive"
    return None


def normalize_telegram_command(text: Any) -> str | None:
    if not isinstance(text, str):
        return None
    parts = text.strip().split()
    if not parts:
        return None
    command = parts[0].lower()
    if not command.startswith("/"):
        return None
    return command.split("@", 1)[0]


class TelegramControlBot:
    def __init__(
        self,
        settings: Settings,
        *,
        db_session_factory: Any = SessionLocal,
        http_client_factory: Any = httpx.AsyncClient,
    ) -> None:
        token = settings.telegram_control_bot_token_value()
        if token is None:
            raise TelegramControlError("Telegram control bot token is not configured")
        self.settings = settings
        self.allowed_user_ids = settings.telegram_control_allowed_user_id_set()
        self.poll_timeout_seconds = min(
            50.0,
            max(1.0, float(settings.telegram_control_poll_timeout_seconds)),
        )
        self.db_session_factory = db_session_factory
        self.http_client_factory = http_client_factory
        # Telegram's Bot API requires the token in the URL. Never log this URL.
        self._base_url = f"https://api.telegram.org/bot{token}/"

    async def run(self) -> None:
        timeout = httpx.Timeout(self.poll_timeout_seconds + 10.0, connect=10.0)
        offset = await self._load_offset()
        consecutive_transport_failures = 0
        while True:
            try:
                async with self.http_client_factory(base_url=self._base_url, timeout=timeout) as client:
                    # A reconnect must never discard pending commands. The
                    # durable offset below makes repeated delivery idempotent.
                    await self._api_call(client, "deleteWebhook", {"drop_pending_updates": False})
                    await self._store_running_status(offset=offset)
                    while True:
                        updates = await self._api_call(
                            client,
                            "getUpdates",
                            {
                                "offset": offset,
                                "timeout": int(self.poll_timeout_seconds),
                                "allowed_updates": ["message"],
                            },
                        )
                        consecutive_transport_failures = 0
                        if not isinstance(updates, list):
                            raise TelegramControlError("Telegram getUpdates returned an invalid result")
                        for update in sorted(updates, key=_telegram_update_id):
                            update_id = _telegram_update_id(update)
                            if update_id < 0:
                                continue
                            offset = max(offset, update_id + 1)
                            await self.handle_update(update, next_offset=offset, client=client)
                        await self._store_running_status(offset=offset)
            except asyncio.CancelledError:
                raise
            except TelegramControlTransientError as exc:
                consecutive_transport_failures += 1
                retry_delay = _telegram_retry_delay_seconds(
                    consecutive_transport_failures,
                    retry_after_seconds=exc.retry_after_seconds,
                )
                safe_error = redact_text(exc)
                await self._store_retrying_status(
                    offset=offset,
                    retry_delay_seconds=retry_delay,
                    error=safe_error,
                )
                log.warning(
                    "telegram_control_transport_retrying",
                    error=safe_error,
                    retry_delay_seconds=retry_delay,
                    consecutive_failures=consecutive_transport_failures,
                )
                await asyncio.sleep(retry_delay)

    async def handle_update(
        self,
        update: dict[str, Any],
        *,
        next_offset: int | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        update_id = _telegram_update_id(update)
        offset = max(0, next_offset if next_offset is not None else update_id + 1)
        message = update.get("message") if isinstance(update, dict) else None
        if not isinstance(message, dict):
            await self._persist_offset(offset)
            return
        sender = message.get("from")
        chat = message.get("chat")
        sender_id = _integer_field(sender, "id")
        chat_id = _integer_field(chat, "id")
        chat_type = str(chat.get("type") or "") if isinstance(chat, dict) else ""
        if (
            sender_id is None
            or chat_id is None
            or sender_id not in self.allowed_user_ids
            or chat_type != "private"
            or chat_id != sender_id
        ):
            # Consume unauthorized/group updates silently so they cannot be
            # replayed forever, but disclose nothing about the bot state.
            await self._persist_offset(offset)
            return

        command = normalize_telegram_command(message.get("text"))
        if command in {"/on", "/off"}:
            kill_switch = command == "/off"
            previous, current = await self._set_kill_switch_and_offset(
                kill_switch=kill_switch,
                offset=offset,
                update_id=update_id,
                telegram_user_id=sender_id,
                chat_id=chat_id,
            )
            already_set = bool(previous.get("kill_switch", True)) == kill_switch
            if kill_switch:
                reply = (
                    "✅ 跟单已保持关闭。新的开仓和加仓会被阻止；已有仓位的减仓和平仓仍会执行。"
                    if already_set
                    else "✅ 跟单已关闭。新的开仓和加仓会被阻止；已有仓位的减仓和平仓仍会执行。"
                )
            else:
                reply = "✅ 跟单已保持开启。" if already_set else "✅ 跟单已开启。"
            await self._try_send_message(client, chat_id=chat_id, text=reply)
            return

        await self._persist_offset(offset)
        await self._try_send_message(
            client,
            chat_id=chat_id,
            text="可用命令：/off 关闭新开仓和加仓；/on 恢复跟单。",
        )

    async def _api_call(
        self,
        client: httpx.AsyncClient,
        method: str,
        payload: dict[str, Any],
    ) -> Any:
        try:
            response = await client.post(method, json=payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise TelegramControlTransportError(
                f"Telegram API transport failed: {redact_text(exc)}"
            ) from exc
        try:
            body = response.json()
        except Exception as exc:
            if response.status_code >= 500:
                raise TelegramControlTransientError(
                    f"Telegram API returned non-JSON response (HTTP {response.status_code})"
                ) from exc
            raise TelegramControlError(
                f"Telegram API returned non-JSON response (HTTP {response.status_code})"
            ) from exc
        if response.status_code >= 400 or not isinstance(body, dict) or not bool(body.get("ok")):
            description = (
                redact_text(body.get("description", "request failed"))
                if isinstance(body, dict)
                else "request failed"
            )
            error_code = _telegram_error_code(body)
            if response.status_code == 429 or response.status_code >= 500 or error_code == 429 or error_code >= 500:
                raise TelegramControlTransientError(
                    f"Telegram API request failed (HTTP {response.status_code}): {description}",
                    retry_after_seconds=_telegram_retry_after_seconds(body),
                )
            raise TelegramControlError(
                f"Telegram API request failed (HTTP {response.status_code}): {description}"
            )
        return body.get("result")

    async def _try_send_message(
        self,
        client: httpx.AsyncClient | None,
        *,
        chat_id: int,
        text: str,
    ) -> None:
        if client is None:
            return
        try:
            await self._api_call(client, "sendMessage", {"chat_id": chat_id, "text": text})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The control state and update offset were committed together before
            # replying. A reply failure must never replay or undo the command.
            log.warning("telegram_control_reply_failed", error=redact_text(exc))

    async def _load_offset(self) -> int:
        async with self.db_session_factory() as db:
            row = await db.get(AppSetting, TELEGRAM_CONTROL_OFFSET_KEY)
            if row is None:
                return 0
            try:
                return max(0, int((row.value or {}).get("offset", 0)))
            except (TypeError, ValueError):
                return 0

    async def _persist_offset(self, offset: int) -> None:
        async with self.db_session_factory() as db:
            await _store_offset_row(db, offset)
            await db.commit()

    async def _set_kill_switch_and_offset(
        self,
        *,
        kill_switch: bool,
        offset: int,
        update_id: int,
        telegram_user_id: int,
        chat_id: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        async with self.db_session_factory() as db:
            await acquire_copy_trading_control_lock(db)
            risk = await db.get(AppSetting, "risk")
            previous = dict(risk.value or {}) if risk is not None else {"kill_switch": True}
            current = {**previous, "kill_switch": bool(kill_switch)}
            if risk is None:
                db.add(AppSetting(key="risk", value=current))
            else:
                risk.value = current
            await _store_offset_row(db, offset)
            db.add(
                AuditLog(
                    user_id=None,
                    action="telegram.copy_trading_off" if kill_switch else "telegram.copy_trading_on",
                    ip_address=None,
                    metadata_json={
                        "telegram_user_id": telegram_user_id,
                        "chat_id": chat_id,
                        "update_id": update_id,
                        "previous": {"kill_switch": bool(previous.get("kill_switch", True))},
                        "current": {"kill_switch": bool(current["kill_switch"])},
                    },
                )
            )
            await db.commit()
            return previous, current

    async def _store_running_status(self, *, offset: int) -> None:
        async with self.db_session_factory() as db:
            await store_task_status(
                db,
                task_name=TELEGRAM_CONTROL_TASK_NAME,
                status="running",
                metadata={
                    "allowed_user_count": len(self.allowed_user_ids),
                    "next_update_offset": offset,
                    "transport": "long_polling",
                },
            )
            await db.commit()

    async def _store_retrying_status(
        self,
        *,
        offset: int,
        retry_delay_seconds: float,
        error: str,
    ) -> None:
        async with self.db_session_factory() as db:
            await store_task_status(
                db,
                task_name=TELEGRAM_CONTROL_TASK_NAME,
                status="degraded_retrying",
                last_error=error[:500],
                metadata={
                    "allowed_user_count": len(self.allowed_user_ids),
                    "next_update_offset": offset,
                    "transport": "long_polling",
                    "retry_delay_seconds": retry_delay_seconds,
                },
            )
            await db.commit()


class TelegramExecutionAlertWorker(TelegramControlBot):
    """Deliver durable execution-risk events without blocking order submission."""

    async def run(self) -> None:
        consecutive_failures = 0
        await self._store_alert_status(status="running")
        last_heartbeat_at = asyncio.get_running_loop().time()
        while True:
            try:
                delivered = await self.dispatch_once()
                consecutive_failures = 0
                if not delivered:
                    now = asyncio.get_running_loop().time()
                    if now - last_heartbeat_at >= TELEGRAM_EXECUTION_ALERT_HEARTBEAT_SECONDS:
                        await self._store_alert_status(status="running")
                        last_heartbeat_at = now
                    await asyncio.sleep(TELEGRAM_EXECUTION_ALERT_POLL_SECONDS)
                else:
                    last_heartbeat_at = asyncio.get_running_loop().time()
            except asyncio.CancelledError:
                raise
            except TelegramControlError as exc:
                consecutive_failures += 1
                retry_delay = _telegram_retry_delay_seconds(
                    consecutive_failures,
                    retry_after_seconds=getattr(exc, "retry_after_seconds", None),
                )
                safe_error = redact_text(exc)
                await self._store_alert_status(
                    status="degraded_retrying",
                    last_error=safe_error,
                    metadata={"retry_delay_seconds": retry_delay},
                )
                log.warning(
                    "telegram_execution_alert_retrying",
                    error=safe_error,
                    retry_delay_seconds=retry_delay,
                    consecutive_failures=consecutive_failures,
                )
                await asyncio.sleep(retry_delay)

    async def dispatch_once(self) -> bool:
        event, delivered_chat_ids = await self._load_next_alert()
        if event is None:
            return False
        event_id = int(event.id)
        await self._ensure_pending_alert(event_id, delivered_chat_ids)
        timeout = httpx.Timeout(10.0, connect=5.0)
        text = format_execution_alert(event)
        async with self.http_client_factory(base_url=self._base_url, timeout=timeout) as client:
            for chat_id in sorted(self.allowed_user_ids):
                if chat_id in delivered_chat_ids:
                    continue
                await self._api_call(
                    client,
                    "sendMessage",
                    {"chat_id": chat_id, "text": text},
                )
                delivered_chat_ids.add(chat_id)
                await self._store_alert_recipient(event_id, delivered_chat_ids)
        await self._complete_alert(event_id)
        await self._store_alert_status(
            status="running",
            metadata={"last_delivered_risk_event_id": event_id},
        )
        log.warning(
            "telegram_execution_alert_delivered",
            risk_event_id=event_id,
            recipient_count=len(delivered_chat_ids),
            event_type=event.event_type,
        )
        return True

    async def _load_next_alert(self) -> tuple[RiskEvent | None, set[int]]:
        async with self.db_session_factory() as db:
            state_row = await db.get(AppSetting, TELEGRAM_EXECUTION_ALERT_STATE_KEY)
            state = dict(state_row.value or {}) if state_row is not None else {}
            last_event_id = _nonnegative_int(state.get("last_event_id"))
            pending_event_id = _nonnegative_int(state.get("pending_event_id"))
            delivered_chat_ids = _positive_int_set(state.get("delivered_chat_ids"))
            if pending_event_id > last_event_id:
                pending = await db.get(RiskEvent, pending_event_id)
                if pending is not None:
                    return pending, delivered_chat_ids
            result = await db.execute(
                select(RiskEvent)
                .where(RiskEvent.event_type.in_(EXECUTION_ALERT_EVENT_TYPES))
                .where(RiskEvent.id > last_event_id)
                .order_by(RiskEvent.id.asc())
                .limit(1)
            )
            return result.scalars().first(), set()

    async def _ensure_pending_alert(self, event_id: int, delivered_chat_ids: set[int]) -> None:
        async with self.db_session_factory() as db:
            row = await db.get(AppSetting, TELEGRAM_EXECUTION_ALERT_STATE_KEY)
            current = dict(row.value or {}) if row is not None else {}
            last_event_id = _nonnegative_int(current.get("last_event_id"))
            if last_event_id >= event_id:
                return
            payload = {
                "last_event_id": last_event_id,
                "pending_event_id": event_id,
                "delivered_chat_ids": sorted(delivered_chat_ids),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if row is None:
                db.add(AppSetting(key=TELEGRAM_EXECUTION_ALERT_STATE_KEY, value=payload))
            else:
                row.value = payload
            await db.commit()

    async def _store_alert_recipient(self, event_id: int, delivered_chat_ids: set[int]) -> None:
        async with self.db_session_factory() as db:
            row = await db.get(AppSetting, TELEGRAM_EXECUTION_ALERT_STATE_KEY)
            current = dict(row.value or {}) if row is not None else {}
            if _nonnegative_int(current.get("last_event_id")) >= event_id:
                return
            payload = {
                "last_event_id": _nonnegative_int(current.get("last_event_id")),
                "pending_event_id": event_id,
                "delivered_chat_ids": sorted(delivered_chat_ids),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if row is None:
                db.add(AppSetting(key=TELEGRAM_EXECUTION_ALERT_STATE_KEY, value=payload))
            else:
                row.value = payload
            await db.commit()

    async def _complete_alert(self, event_id: int) -> None:
        async with self.db_session_factory() as db:
            row = await db.get(AppSetting, TELEGRAM_EXECUTION_ALERT_STATE_KEY)
            current = dict(row.value or {}) if row is not None else {}
            payload = {
                "last_event_id": max(
                    event_id,
                    _nonnegative_int(current.get("last_event_id")),
                ),
                "pending_event_id": None,
                "delivered_chat_ids": [],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if row is None:
                db.add(AppSetting(key=TELEGRAM_EXECUTION_ALERT_STATE_KEY, value=payload))
            else:
                row.value = payload
            await db.commit()

    async def _store_alert_status(
        self,
        *,
        status: str,
        last_error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        async with self.db_session_factory() as db:
            await store_task_status(
                db,
                task_name=TELEGRAM_EXECUTION_ALERT_TASK_NAME,
                status=status,
                last_error=last_error[:500] if last_error else None,
                metadata={
                    "allowed_user_count": len(self.allowed_user_ids),
                    "poll_interval_seconds": TELEGRAM_EXECUTION_ALERT_POLL_SECONDS,
                    "heartbeat_interval_seconds": TELEGRAM_EXECUTION_ALERT_HEARTBEAT_SECONDS,
                    **(metadata or {}),
                },
            )
            await db.commit()


async def _store_offset_row(db: Any, offset: int) -> None:
    row = await db.get(AppSetting, TELEGRAM_CONTROL_OFFSET_KEY)
    payload = {
        "offset": max(0, int(offset)),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if row is None:
        db.add(AppSetting(key=TELEGRAM_CONTROL_OFFSET_KEY, value=payload))
    else:
        try:
            current_offset = int((row.value or {}).get("offset", 0) or 0)
        except (TypeError, ValueError):
            current_offset = 0
        row.value = {**payload, "offset": max(current_offset, payload["offset"])}


def _telegram_update_id(update: Any) -> int:
    if not isinstance(update, dict):
        return -1
    try:
        return int(update.get("update_id"))
    except (TypeError, ValueError):
        return -1


def _integer_field(payload: Any, key: str) -> int | None:
    if not isinstance(payload, dict):
        return None
    try:
        return int(payload.get(key))
    except (TypeError, ValueError):
        return None


def _nonnegative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _positive_int_set(values: Any) -> set[int]:
    if not isinstance(values, list):
        return set()
    result: set[int] = set()
    for value in values:
        parsed = _nonnegative_int(value)
        if parsed > 0:
            result.add(parsed)
    return result


def _telegram_retry_delay_seconds(
    consecutive_failures: int,
    *,
    retry_after_seconds: float | None = None,
) -> float:
    # Reconnect quickly after a one-off disconnect, then cap the retry rate so
    # an upstream outage cannot create a retry storm. Telegram's explicit
    # retry_after is authoritative when it asks us to wait longer.
    exponent = min(6, max(0, int(consecutive_failures) - 1))
    backoff = min(30.0, 0.5 * (2**exponent))
    requested = max(0.0, float(retry_after_seconds or 0.0))
    return min(300.0, max(backoff, requested))


def _telegram_error_code(body: Any) -> int:
    if not isinstance(body, dict):
        return 0
    try:
        return int(body.get("error_code") or 0)
    except (TypeError, ValueError):
        return 0


def _telegram_retry_after_seconds(body: Any) -> float | None:
    if not isinstance(body, dict):
        return None
    parameters = body.get("parameters")
    if not isinstance(parameters, dict):
        return None
    try:
        retry_after = float(parameters.get("retry_after"))
    except (TypeError, ValueError):
        return None
    return max(0.0, retry_after)


async def run_telegram_control_bot(settings: Settings) -> None:
    await TelegramControlBot(settings).run()


async def run_telegram_execution_alerts(settings: Settings) -> None:
    await TelegramExecutionAlertWorker(settings).run()
