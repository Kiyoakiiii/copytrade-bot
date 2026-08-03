import json
from pathlib import Path

from app.api.dashboard import _position_with_live_price
from app.api.stream import dashboard_data_version, sse_event


def _event_data(raw: str) -> dict:
    data_line = next(line for line in raw.splitlines() if line.startswith("data: "))
    return json.loads(data_line.removeprefix("data: "))


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


def test_frontend_operational_pages_use_stream_with_polling_fallback() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "frontend/src/app").exists()), None)
    if root is None:
        return
    realtime_source = (root / "frontend/src/lib/realtime.ts").read_text()
    assert "new EventSource" in realtime_source
    assert "fallbackMs = options.fallbackMs ?? 1000" in realtime_source
    assert "reconcileMs = options.reconcileMs ?? 45000" in realtime_source
    assert "/stream/dashboard" in realtime_source
    for path in [
        root / "frontend/src/app/dashboard/page.tsx",
        root / "frontend/src/app/leaders/[id]/page.tsx",
        root / "frontend/src/app/leaders/page.tsx",
        root / "frontend/src/app/orders/page.tsx",
        root / "frontend/src/app/preflight/page.tsx",
    ]:
        text = path.read_text()
        assert "useDashboardStream" in text
        assert "useRealtimeFallbackPolling" in text
        assert "realtime mode" in text


def test_final_live_check_is_button_only_not_interval_driven() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "frontend/src/app").exists()), None)
    if root is None:
        return
    text = (root / "frontend/src/app/preflight/page.tsx").read_text()
    assert '"/preflight/final-live-check"' in text
    final_call = text.index('"/preflight/final-live-check"')
    interval_call = text.find("setInterval")
    assert interval_call == -1 or abs(interval_call - final_call) > 500


def test_dashboard_realtime_schedules_refresh_without_sync_external_wait() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "backend/app/api/dashboard.py").exists()), None)
    if root is None:
        return
    text = (root / "backend/app/api/dashboard.py").read_text()
    assert "schedule_account_state_refresh_if_stale" in text
    assert "ensure_recent_account_states" not in text
    assert "HyperliquidInfoClient" not in text


def test_stream_endpoint_does_not_import_external_hyperliquid_clients() -> None:
    candidates = [Path(__file__).resolve().parents[2], Path(__file__).resolve().parents[1]]
    root = next((item for item in candidates if (item / "backend/app/api/stream.py").exists()), None)
    if root is None:
        return
    text = (root / "backend/app/api/stream.py").read_text()
    assert "HyperliquidInfoClient" not in text
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
