from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from app.core.config import Settings
from app.models import AppSetting, AuditLog, RiskEvent
from app.services.execution_alerts import (
    COPY_ORDER_INSUFFICIENT_COLLATERAL,
    HYPERLIQUID_NETWORK_UPGRADE_POST_ONLY_REJECTION,
    LEADER_LIQUIDATION_DETECTED,
    format_copy_order_insufficient_collateral_alert,
    format_hyperliquid_network_upgrade_alert,
    format_leader_liquidation_alert,
    is_hyperliquid_network_upgrade_post_only_error,
)
from app.services.telegram_control_bot import (
    TELEGRAM_CONTROL_OFFSET_KEY,
    TelegramControlBot,
    TelegramExecutionAlertWorker,
    _telegram_retry_delay_seconds,
    normalize_telegram_command,
    telegram_control_config_error,
)


def telegram_settings(**overrides) -> Settings:
    values = {
        "telegram_control_enabled": True,
        "telegram_control_bot_token": "123456:" + "A" * 35,
        "telegram_control_allowed_user_ids": "12345",
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


class SharedDatabase:
    def __init__(self, *, kill_switch: bool = False) -> None:
        self.rows = {
            "risk": AppSetting(key="risk", value={"kill_switch": kill_switch}),
        }
        self.added: list[object] = []
        self.commits = 0

    def session_factory(self):
        return FakeSession(self)


class FakeSession:
    def __init__(self, shared: SharedDatabase) -> None:
        self.shared = shared

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def get(self, model, key):
        if model is AppSetting:
            return self.shared.rows.get(key)
        return None

    def add(self, row):
        self.shared.added.append(row)
        if isinstance(row, AppSetting):
            self.shared.rows[row.key] = row

    async def commit(self):
        self.shared.commits += 1


class RecordingTelegramControlBot(TelegramControlBot):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.replies: list[tuple[int, str]] = []

    async def _try_send_message(self, client, *, chat_id: int, text: str) -> None:
        self.replies.append((chat_id, text))


class FakeTelegramResponse:
    def __init__(self, result=None, *, status_code: int = 200, body: dict | None = None) -> None:
        self.status_code = status_code
        self.result = result
        self.body = body

    def json(self):
        if self.body is not None:
            return self.body
        return {"ok": True, "result": self.result}


class ScriptedTelegramClient:
    def __init__(self, script: list[object]) -> None:
        self.script = script

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def post(self, method: str, *, json: dict):
        if method == "deleteWebhook":
            return FakeTelegramResponse(True)
        result = self.script.pop(0)
        if isinstance(result, BaseException):
            raise result
        if isinstance(result, FakeTelegramResponse):
            return result
        return FakeTelegramResponse(result)


class ScriptedTelegramClientFactory:
    def __init__(self, script: list[object]) -> None:
        self.script = script
        self.connections = 0

    def __call__(self, **_):
        self.connections += 1
        return ScriptedTelegramClient(self.script)


class RecordingSendClient:
    def __init__(self, calls: list[tuple[str, dict]]) -> None:
        self.calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    async def post(self, method: str, *, json: dict):
        self.calls.append((method, json))
        return FakeTelegramResponse(True)


class RecordingSendClientFactory:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, **_):
        return RecordingSendClient(self.calls)


class RetryRecordingTelegramControlBot(TelegramControlBot):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.statuses: list[tuple[str, int]] = []
        self.retries: list[tuple[float, str]] = []

    async def _load_offset(self) -> int:
        return 77

    async def _store_running_status(self, *, offset: int) -> None:
        self.statuses.append(("running", offset))

    async def _store_retrying_status(
        self,
        *,
        offset: int,
        retry_delay_seconds: float,
        error: str,
    ) -> None:
        self.retries.append((retry_delay_seconds, error))
        self.statuses.append(("degraded_retrying", offset))


def telegram_update(
    command: str,
    *,
    update_id: int = 100,
    user_id: int = 12345,
    chat_id: int = 12345,
    chat_type: str = "private",
) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "from": {"id": user_id},
            "chat": {"id": chat_id, "type": chat_type},
            "text": command,
        },
    }


def test_command_normalization_supports_bot_username_suffix() -> None:
    assert normalize_telegram_command(" /OFF@CopyTradeControlBot ") == "/off"
    assert normalize_telegram_command("/on") == "/on"
    assert normalize_telegram_command("off") is None


def test_authorized_off_and_on_update_risk_offset_audit_and_reply() -> None:
    shared = SharedDatabase(kill_switch=False)
    bot = RecordingTelegramControlBot(
        telegram_settings(),
        db_session_factory=shared.session_factory,
    )

    asyncio.run(bot.handle_update(telegram_update("/off", update_id=100)))

    assert shared.rows["risk"].value == {"kill_switch": True}
    assert shared.rows[TELEGRAM_CONTROL_OFFSET_KEY].value["offset"] == 101
    audits = [row for row in shared.added if isinstance(row, AuditLog)]
    assert audits[-1].action == "telegram.copy_trading_off"
    assert audits[-1].metadata_json["telegram_user_id"] == 12345
    assert "关闭" in bot.replies[-1][1]

    asyncio.run(bot.handle_update(telegram_update("/on", update_id=101)))

    assert shared.rows["risk"].value == {"kill_switch": False}
    assert shared.rows[TELEGRAM_CONTROL_OFFSET_KEY].value["offset"] == 102
    audits = [row for row in shared.added if isinstance(row, AuditLog)]
    assert audits[-1].action == "telegram.copy_trading_on"
    assert "开启" in bot.replies[-1][1]


def test_unauthorized_and_group_updates_are_silent_but_not_replayed() -> None:
    shared = SharedDatabase(kill_switch=False)
    bot = RecordingTelegramControlBot(
        telegram_settings(),
        db_session_factory=shared.session_factory,
    )

    asyncio.run(bot.handle_update(telegram_update("/off", update_id=200, user_id=99999)))
    asyncio.run(bot.handle_update(telegram_update("/off", update_id=201, chat_type="group")))

    assert shared.rows["risk"].value == {"kill_switch": False}
    assert shared.rows[TELEGRAM_CONTROL_OFFSET_KEY].value["offset"] == 202
    assert not [row for row in shared.added if isinstance(row, AuditLog)]
    assert bot.replies == []


def test_config_requires_token_and_numeric_allowlist_when_enabled() -> None:
    assert telegram_control_config_error(
        telegram_settings(telegram_control_bot_token=None)
    ) == "Telegram control bot token is missing or malformed"
    assert telegram_control_config_error(
        telegram_settings(telegram_control_allowed_user_ids="not-a-number")
    ) == "Telegram allowed user IDs must be comma-separated integers"
    assert telegram_control_config_error(
        telegram_settings(telegram_control_allowed_user_ids="-123")
    ) == "Telegram allowed user IDs must be positive integers"
    assert telegram_control_config_error(
        telegram_settings(telegram_control_enabled=False, telegram_control_bot_token=None)
    ) is None


def test_settings_repr_does_not_expose_telegram_token() -> None:
    settings = telegram_settings()
    token = settings.telegram_control_bot_token_value()

    assert token is not None
    assert token not in repr(settings)


def test_network_upgrade_alert_is_narrow_and_contains_manual_action_details() -> None:
    assert is_hyperliquid_network_upgrade_post_only_error(
        "Only post-only orders allowed immediately after network upgrade"
    )
    assert is_hyperliquid_network_upgrade_post_only_error(
        "Only post-only orders allowed immediately after a network upgrade"
    )
    assert not is_hyperliquid_network_upgrade_post_only_error("Order has invalid price")

    event = RiskEvent(
        id=77,
        severity="critical",
        event_type=HYPERLIQUID_NETWORK_UPGRADE_POST_ONLY_REJECTION,
        symbol="BABY",
        leader_address="0x" + "1" * 40,
        message="manual action required",
        metadata_json={
            "order_id": 8827,
            "order_action": "CLOSE",
            "position_side": "SHORT",
            "quantity": "44307",
        },
        created_at=datetime(2026, 8, 1, 9, 7, 47, tzinfo=timezone.utc),
    )

    text = format_hyperliquid_network_upgrade_alert(event)

    assert "BABY" in text
    assert "平仓" in text
    assert "SHORT" in text
    assert "44307" in text
    assert "8827" in text
    assert "不会自动重试或补单" in text
    assert "private" not in text.casefold()
    assert "signature" not in text.casefold()


def test_insufficient_collateral_alert_contains_actionable_values_and_masks_addresses() -> None:
    event = RiskEvent(
        id=79,
        severity="critical",
        event_type=COPY_ORDER_INSUFFICIENT_COLLATERAL,
        symbol="xyz:DELL",
        leader_address="0x" + "2" * 40,
        message="insufficient available collateral for target delta",
        metadata_json={
            "order_id": 9134,
            "execution_account_suffix": "8b8e",
            "order_action": "INCREASE",
            "position_side": "SHORT",
            "quantity": "1.78",
            "target_notional": "878.87",
            "delta_notional": "845.67",
            "required_initial_margin": "845.67",
            "available_collateral": "20.09",
        },
        created_at=datetime(2026, 8, 4, 17, 1, 33, tzinfo=timezone.utc),
    )

    text = format_copy_order_insufficient_collateral_alert(event)

    assert "xyz:DELL" in text
    assert "加仓" in text
    assert "8b8e" in text
    assert "845.67" in text
    assert "20.09" in text
    assert "2222" in text
    assert event.leader_address not in text
    assert "不会自动补单" in text


def test_leader_liquidation_alert_is_actionable_and_masks_address() -> None:
    event = RiskEvent(
        id=80,
        severity="critical",
        event_type=LEADER_LIQUIDATION_DETECTED,
        symbol="CASHCAT",
        leader_address="0x" + "1" * 40,
        message="leader liquidation detected",
        metadata_json={
            "execution_account_suffix": "MAIN",
            "event_time_ms": 1_785_900_000_123,
            "leverage_type": "Cross",
            "account_value": "920.15",
            "liquidated_positions": [{"coin": "CASHCAT", "szi": "-252764"}],
            "detection_source": "userNonFundingLedgerUpdates",
        },
        created_at=datetime(2026, 8, 5, 5, 0, tzinfo=timezone.utc),
    )

    text = format_leader_liquidation_alert(event)

    assert "Leader 发生强平" in text
    assert "CASHCAT -252764" in text
    assert "强平成交不跟随" in text
    assert "立即停止这个账户中该币种的自动跟单" in text
    assert "实际仓位归零前" in text
    assert "仓位归零后自动释放" in text
    assert "1111" in text
    assert event.leader_address not in text


def test_execution_alert_dispatch_resumes_recipients_without_resending_completed_one() -> None:
    event = RiskEvent(
        id=78,
        severity="critical",
        event_type=HYPERLIQUID_NETWORK_UPGRADE_POST_ONLY_REJECTION,
        symbol="BABY",
        leader_address="0x" + "1" * 40,
        message="manual action required",
        metadata_json={
            "order_id": 8827,
            "order_action": "CLOSE",
            "position_side": "SHORT",
            "quantity": "44307",
        },
        created_at=datetime(2026, 8, 1, 9, 7, 47, tzinfo=timezone.utc),
    )
    factory = RecordingSendClientFactory()

    class RecordingWorker(TelegramExecutionAlertWorker):
        def __init__(self):
            super().__init__(
                telegram_settings(telegram_control_allowed_user_ids="12345,67890"),
                http_client_factory=factory,
            )
            self.progress: list[tuple[str, object]] = []

        async def _load_next_alert(self):
            return event, {12345}

        async def _ensure_pending_alert(self, event_id, delivered_chat_ids):
            self.progress.append(("pending", (event_id, set(delivered_chat_ids))))

        async def _store_alert_recipient(self, event_id, delivered_chat_ids):
            self.progress.append(("recipient", (event_id, set(delivered_chat_ids))))

        async def _complete_alert(self, event_id):
            self.progress.append(("complete", event_id))

        async def _store_alert_status(self, **kwargs):
            self.progress.append(("status", kwargs))

    worker = RecordingWorker()

    assert asyncio.run(worker.dispatch_once()) is True

    send_calls = [payload for method, payload in factory.calls if method == "sendMessage"]
    assert [payload["chat_id"] for payload in send_calls] == [67890]
    assert worker.progress[0] == ("pending", (78, {12345}))
    assert ("recipient", (78, {12345, 67890})) in worker.progress
    assert ("complete", 78) in worker.progress


def test_transport_retry_backoff_is_fast_then_capped() -> None:
    assert [_telegram_retry_delay_seconds(value) for value in (1, 2, 3, 7, 20)] == [
        0.5,
        1.0,
        2.0,
        30.0,
        30.0,
    ]
    assert _telegram_retry_delay_seconds(1, retry_after_seconds=5) == 5.0
    assert _telegram_retry_delay_seconds(20, retry_after_seconds=600) == 300.0


def test_transport_disconnect_reconnects_without_escaping_supervisor(monkeypatch) -> None:
    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("app.services.telegram_control_bot.asyncio.sleep", no_sleep)
    factory = ScriptedTelegramClientFactory(
        [
            RuntimeError("temporary disconnect"),
            [],
            asyncio.CancelledError(),
        ]
    )
    bot = RetryRecordingTelegramControlBot(
        telegram_settings(),
        http_client_factory=factory,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(bot.run())

    assert factory.connections == 2
    assert bot.retries == [(0.5, "Telegram API transport failed: temporary disconnect")]
    assert bot.statuses == [
        ("running", 77),
        ("degraded_retrying", 77),
        ("running", 77),
        ("running", 77),
    ]


def test_rate_limit_and_server_error_retry_inside_control_task(monkeypatch) -> None:
    observed_sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        observed_sleeps.append(delay)

    monkeypatch.setattr("app.services.telegram_control_bot.asyncio.sleep", record_sleep)
    factory = ScriptedTelegramClientFactory(
        [
            FakeTelegramResponse(
                status_code=429,
                body={
                    "ok": False,
                    "error_code": 429,
                    "description": "Too Many Requests",
                    "parameters": {"retry_after": 5},
                },
            ),
            FakeTelegramResponse(
                status_code=502,
                body={
                    "ok": False,
                    "error_code": 502,
                    "description": "Bad Gateway",
                },
            ),
            [],
            asyncio.CancelledError(),
        ]
    )
    bot = RetryRecordingTelegramControlBot(
        telegram_settings(),
        http_client_factory=factory,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(bot.run())

    assert factory.connections == 3
    assert observed_sleeps == [5.0, 1.0]
    assert [retry[0] for retry in bot.retries] == [5.0, 1.0]
    assert all(status[1] == 77 for status in bot.statuses)
