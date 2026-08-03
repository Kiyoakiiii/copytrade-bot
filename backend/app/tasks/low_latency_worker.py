from __future__ import annotations

import asyncio

import structlog
import uvloop

from app.core.config import get_settings
from app.core.logging import configure_logging, redact_text
from app.tasks.low_latency_watcher import run_low_latency_watcher


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = structlog.get_logger(__name__)
    while True:
        try:
            await run_low_latency_watcher(settings)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception(
                "dedicated_low_latency_watcher_failed_restarting",
                error=redact_text(exc)[:500],
            )
            await asyncio.sleep(1.0)


def main() -> None:
    # The dedicated process has no ASGI server to install uvloop for it. Run the
    # trading event loop explicitly on uvloop to reduce scheduler jitter during
    # dense fill bursts.
    uvloop.run(run())


if __name__ == "__main__":
    main()
