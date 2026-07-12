from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.dialects.postgresql import insert

from app.models import AppSetting


async def store_task_status(
    db: Any,
    *,
    task_name: str,
    status: str = "running",
    last_error: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    payload = {
        "task_name": task_name,
        "status": status,
        "last_heartbeat_at": now.isoformat(),
        "last_error": last_error,
        "metadata": metadata or {},
    }
    stmt = (
        insert(AppSetting)
        .values(key=f"task_status:{task_name}", value=payload, updated_at=now)
        .on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={"value": payload, "updated_at": now},
        )
    )
    await db.execute(stmt)
