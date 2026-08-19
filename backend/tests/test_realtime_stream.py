import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from app.api.dashboard import _overview_position_payload, _position_with_live_price
from app.api.stream import _events_for_change, dashboard_data_version, sse_event
from app.core.config import Settings
from app.services.watcher_status import (
    watcher_active_leaders_by_scope,
    watcher_statuses_by_scope,
)
from app.tasks.leader_state_poller import monitoring_account_state_stale_seconds


def _event_data(raw: str) -> dict:
    data_line = next(line for line in raw.splitlines() if line.startswith("data: "))
    return json.loads(data_line.removeprefix("data: "))


def test_overview_position_projection_includes_exchange_risk_and_funding_fields() -> None:
    row = SimpleNamespace(
        account_state_id=1,
        role="FOLLOWER",
        address="0xpublic",
        dex="",
        coin="TEST",
        canonical_coin="TEST",
        side="LONG",
        size=Decimal("2"),
        notional=Decimal("100"),
        entry_px=Decimal("48"),
        mark_px=Decimal("50"),
        unrealized_pnl=Decimal("4"),
        leverage=Decimal("5"),
        margin_used=Decimal("20"),
        liquidation_px=Decimal("30"),
        active=True,
        status="OPEN",
        raw_payload_masked={
            "leverage": {"type": "cross", "value": 5},
            "returnOnEquity": "0.2",
            "cumFunding": {"sinceOpen": "-0.75"},
        },
    )
    payload = _overview_position_payload(row, account_state=None)
    assert payload["return_on_equity"] == "0.2"
    assert payload["funding_since_open"] == "0.75"
    assert payload["margin_used"] == "20"
    assert payload["liquidation_px"] == "30"
    assert payload["leader_address"] is None
    assert payload["attribution"] == "MANUAL"


def test_overview_position_projection_includes_leader_attribution() -> None:
    row = SimpleNamespace(
        account_state_id=1,
        role="FOLLOWER",
        address="0xpublic",
        dex="xyz",
        coin="TEST",
        canonical_coin="xyz:TEST",
        side="SHORT",
        size=Decimal("2"),
        notional=Decimal("100"),
        entry_px=Decimal("52"),
        mark_px=Decimal("50"),
        unrealized_pnl=Decimal("4"),
        leverage=Decimal("5"),
        margin_used=Decimal("20"),
        liquidation_px=Decimal("70"),
        active=True,
        status="OPEN",
        raw_payload_masked={},
    )
    payload = _overview_position_payload(
        row,
        account_state=None,
        leader_address="leader-e040",
        attribution="LEADER",
    )
    assert payload["leader_address"].endswith("e040")
    assert payload["attribution"] == "LEADER"


def test_sse_event_contains_required_envelope_fields() -> None:
    raw = sse_event(
        event_type="dashboard_snapshot",
        data_version="version-1",
        payload={"stale": True, "data_age_ms": 123, "payload": "ok"},
    )
    assert "event: dashboard_snapshot" in raw
    body = _event_data(raw)
    assert body["event_id"] == "version-1:dashboard_snapshot"
    assert body["event_type"] == "dashboard_snapshot"
    assert body["server_time"]
    assert body["data_version"] == "version-1"
    assert body["stale"] is True
    assert body["data_age_ms"] == 123
    assert body["stale_flags"]["payload"] is True


def test_sse_payload_redacts_private_fields() -> None:
    raw = sse_event(
        event_type="dashboard_snapshot",
        data_version="version-1",
        payload={
            "hyperliquid_private_key": "0xsecret",
            "nested": {"api_key": "abc", "value": "visible"},
        },
    )
    body = _event_data(raw)
    encoded = json.dumps(body)
    assert "0xsecret" not in encoded
    assert "abc" not in encoded
    assert body["payload"]["hyperliquid_private_key"] == "***"
    assert body["payload"]["nested"]["api_key"] == "***"
    assert body["payload"]["nested"]["value"] == "visible"


def test_dashboard_data_version_changes_when_component_changes() -> None:
    first = dashboard_data_version({"orders": "1", "accounts": "1"})
    second = dashboard_data_version({"orders": "2", "accounts": "1"})
    assert first != second


def test_dashboard_stream_initial_change_sends_only_one_snapshot() -> None:
    snapshot = {"follower": {"positions": []}, "leaders": [], "recent_orders": []}
    assert _events_for_change({"accounts", "orders"}, snapshot, initial=True) == [
        ("dashboard_snapshot", snapshot)
    ]


def test_dashboard_stream_followup_change_sends_only_changed_components() -> None:
    snapshot = {
        "follower": {"positions": []},
        "leaders": [],
        "recent_orders": [{"id": 1}],
        "latency": {"latest_event_to_ack_ms": 20},
    }
    events = _events_for_change({"orders"}, snapshot)
    assert [event_type for event_type, _payload in events] == [
        "orders_update",
        "latency_update",
    ]
    assert all(event_type != "dashboard_snapshot" for event_type, _payload in events)


def test_dashboard_watcher_subscriptions_remain_isolated_by_execution_account() -> None:
    subaccount = "0x" + "a" * 40
    main_leader = "0x" + "1" * 40
    sub_leader = "0x" + "2" * 40
    rows = [
        SimpleNamespace(
            key="watcher_status",
            value={"active_leaders": [main_leader]},
        ),
        SimpleNamespace(
            key=f"watcher_status:{subaccount.upper()}",
            value={"active_leaders": [sub_leader]},
        ),
    ]

    active = watcher_active_leaders_by_scope(watcher_statuses_by_scope(rows))

    assert active[""] == {main_leader}
    assert active[subaccount] == {sub_leader}
    assert sub_leader not in active[""]


def test_dashboard_monitoring_freshness_does_not_reuse_hot_path_threshold() -> None:
    settings = Settings(
        account_state_stale_seconds=2,
        account_state_poll_seconds=5,
    )

    assert monitoring_account_state_stale_seconds(settings) == 30


def test_live_price_cache_updates_dashboard_position_mark_and_notional() -> None:
    position = {
        "canonical_coin": "hyna:ZEC",
        "coin": "ZEC",
        "side": "LONG",
        "size": "2",
        "notional": "100",
        "mark_px": "50",
    }
    updated = _position_with_live_price(
        position,
        live_price_cache={
            "HYNA:ZEC": {"price": "55", "stale": False, "age_ms": 300},
        },
    )
    assert updated["mark_px"] == "55"
    assert updated["mid_px"] == "55"
    assert updated["notional"] == "110.00000000"
    assert updated["mark_price_estimated"] is True


def test_primary_frontend_pages_do_not_open_database_polling_streams() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "frontend/src/app").exists()), None)
    if root is None:
        return
    for path in [
        root / "frontend/src/app/dashboard/page.tsx",
        root / "frontend/src/app/leaders/[id]/page.tsx",
        root / "frontend/src/app/leaders/page.tsx",
    ]:
        text = path.read_text()
        assert "useDashboardStream" not in text
        assert "useRealtimeFallbackPolling" not in text
        assert "DB snapshot" in text


def test_command_center_uses_the_dedicated_compact_overview_projection() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "frontend/src/app/dashboard/page.tsx").exists()), None)
    if root is None:
        return
    text = (root / "frontend/src/app/dashboard/page.tsx").read_text()
    assert 'apiFetch<Dashboard>("/dashboard/overview")' in text
    assert 'apiFetch<Dashboard>("/dashboard/realtime")' not in text
    assert 'apiFetch<FollowerAccount[]>("/account-states/followers")' not in text


def test_removed_operational_pages_redirect_to_consolidated_views() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "frontend/src/app").exists()), None)
    if root is None:
        return
    preflight = (root / "frontend/src/app/preflight/page.tsx").read_text()
    orders = (root / "frontend/src/app/orders/page.tsx").read_text()
    assert 'redirect("/risk")' in preflight
    assert 'redirect("/dashboard")' in orders
    assert "apiFetch" not in preflight
    assert "apiFetch" not in orders


def test_dashboard_realtime_schedules_refresh_without_sync_external_wait() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "backend/app/api/dashboard.py").exists()), None)
    if root is None:
        return
    text = (root / "backend/app/api/dashboard.py").read_text()
    assert "account_state_cache_status" in text
    assert "schedule_account_state_refresh_if_stale" not in text
    assert "ensure_recent_account_states" not in text
    assert "HyperliquidInfoClient" not in text


def test_stream_endpoint_does_not_import_external_hyperliquid_clients() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "backend/app/api/stream.py").exists()), None)
    if root is None:
        return
    text = (root / "backend/app/api/stream.py").read_text()
    assert "HyperliquidInfoClient" not in text
    generator = text[text.index("async def _dashboard_event_generator"):text.index("async def dashboard_component_versions")]
    assert "dashboard_component_versions()" not in generator
    assert "_dashboard_snapshot(" not in generator
    assert '"db_snapshot_only"' in generator
    assert "HyperliquidExecutionClient" not in text
    assert "clearinghouse_state" not in text
    assert "all_mids" not in text


def test_stream_endpoint_does_not_hold_auth_db_session_for_sse_lifetime() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "backend/app/api/stream.py").exists()), None)
    if root is None:
        return
    text = (root / "backend/app/api/stream.py").read_text()
    assert "CurrentUser" not in text
    assert "await current_user(" in text
    assert "request.cookies.get(settings.session_cookie_name)" in text
    assert text.index("await current_user(") < text.index("StreamingResponse(")


def test_key_background_tasks_write_task_status() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "backend/app").exists()), None)
    if root is None:
        return
    leader_poller = (root / "backend/app/tasks/leader_state_poller.py").read_text()
    watcher = (root / "backend/app/services/low_latency_watcher.py").read_text()
    main = (root / "backend/app/main.py").read_text()
    assert 'task_name="leader_state_poller"' in leader_poller
    assert 'task_name="account_state_poller"' in leader_poller
    assert 'task_name="low_latency_watcher"' in watcher
    assert 'task_name="price_cache_updater"' in watcher
    assert 'task_name="order_recovery"' in main
    assert "failed_restarting" in main
