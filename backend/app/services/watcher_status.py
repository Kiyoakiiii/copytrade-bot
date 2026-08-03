from __future__ import annotations

from typing import Any, Iterable

from app.services.leader_config import normalize_leader_address


MAIN_WATCHER_STATUS_KEY = "watcher_status"
SCOPED_WATCHER_STATUS_PREFIX = "watcher_status:"


def watcher_execution_scope(value: Any) -> str:
    """Return the durable execution-account scope used by watcher status rows."""

    return str(value or "").strip().lower()


def watcher_statuses_by_scope(rows: Iterable[Any]) -> dict[str, dict[str, Any]]:
    """Index public watcher payloads by main/subaccount execution scope."""

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(getattr(row, "key", "") or "")
        if key == MAIN_WATCHER_STATUS_KEY:
            scope = ""
        elif key.startswith(SCOPED_WATCHER_STATUS_PREFIX):
            scope = watcher_execution_scope(key.removeprefix(SCOPED_WATCHER_STATUS_PREFIX))
        else:
            continue
        value = getattr(row, "value", None)
        result[scope] = value if isinstance(value, dict) else {}
    return result


def watcher_active_leaders_by_scope(
    statuses: dict[str, dict[str, Any]],
) -> dict[str, set[str]]:
    """Return subscriptions without mixing leaders from separate accounts."""

    result: dict[str, set[str]] = {}
    for scope, status in statuses.items():
        raw_active = status.get("active_leaders") or []
        if not isinstance(raw_active, (list, tuple, set)):
            raw_active = []
        result[watcher_execution_scope(scope)] = {
            normalize_leader_address(address)
            for address in raw_active
        }
    return result
