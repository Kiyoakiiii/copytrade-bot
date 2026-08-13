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
    consecutive_failures = 0
    while True:
        started = asyncio.get_running_loop().time()
        try:
            await run_low_latency_watcher(settings)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            runtime_seconds = asyncio.get_running_loop().time() - started
            if runtime_seconds >= 60.0:
                consecutive_failures = 0
            consecutive_failures += 1
            restart_delay_seconds = _restart_delay_seconds(consecutive_failures)
            log.exception(
                "dedicated_low_latency_watcher_failed_restarting",
                error=redact_text(exc)[:500],
                consecutive_failures=consecutive_failures,
                restart_delay_seconds=restart_delay_seconds,
            )
            await asyncio.sleep(restart_delay_seconds)
        else:
            consecutive_failures = 0
            # A writer-lease loss intentionally returns instead of raising.
            # Avoid a tight process loop while the next run reacquires it.
            await asyncio.sleep(1.0)


def _restart_delay_seconds(consecutive_failures: int) -> float:
    return float(min(30, 2 ** max(0, int(consecutive_failures) - 1)))


def main() -> None:
    # The dedicated process has no ASGI server to install uvloop for it. Run the
    # trading event loop explicitly on uvloop to reduce scheduler jitter during
    # dense fill bursts.
    uvloop.run(run())


if __name__ == "__main__":
    main()
