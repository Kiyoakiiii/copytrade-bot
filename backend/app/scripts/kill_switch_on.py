from __future__ import annotations

import asyncio

from sqlalchemy.dialects.postgresql import insert

from app.db.session import SessionLocal
from app.models import AppSetting


async def main() -> None:
    async with SessionLocal() as db:
        row = await db.get(AppSetting, "risk")
        current = dict(row.value) if row else {}
        current["kill_switch"] = True
        stmt = (
            insert(AppSetting)
            .values(key="risk", value=current)
            .on_conflict_do_update(index_elements=[AppSetting.key], set_={"value": current})
        )
        await db.execute(stmt)
        await db.commit()
    print("kill_switch=true")


if __name__ == "__main__":
    asyncio.run(main())
