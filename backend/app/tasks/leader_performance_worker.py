from __future__ import annotations

import asyncio

import structlog

from app.core.config import get_settings
from app.core.logging import configure_logging
from app.services.leader_performance import run_leader_performance_refresher


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = structlog.get_logger(__name__)
    if not settings.leader_performance_refresh_enabled:
        log.warning("leader_performance_worker_disabled")
        return
    log.info(
        "leader_performance_worker_started",
        refresh_seconds=settings.leader_performance_refresh_seconds,
        cache_only_frontend=True,
    )
    await run_leader_performance_refresher(settings)


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
