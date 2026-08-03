from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# A stable PostgreSQL advisory-lock namespace for copy-trading control changes.
# The final exchange-submit gate and every runtime control writer share it, which
# gives /off a deterministic ordering against orders approaching submission.
COPY_TRADING_CONTROL_LOCK_ID = 4_837_004_218_337_092_716


async def acquire_copy_trading_control_lock(db: Any) -> None:
    if not isinstance(db, AsyncSession):
        return
    await db.execute(
        text("SELECT pg_advisory_xact_lock(:lock_id)"),
        {"lock_id": COPY_TRADING_CONTROL_LOCK_ID},
    )
