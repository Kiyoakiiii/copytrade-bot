from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core.config import Settings, get_settings


def _database_connect_args(settings: Settings) -> dict:
    return {
        "server_settings": {
            # PostgreSQL already detects lock cycles, but a non-cyclic waiter
            # can otherwise remain blocked behind a stalled transaction for an
            # unbounded time.  Abort the transaction and let the durable
            # fill/outbox retry path recover instead of hanging a worker.
            "lock_timeout": (
                f"{max(100, int(float(settings.database_lock_timeout_seconds) * 1000))}ms"
            ),
        }
    }


settings = get_settings()
engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=max(1, int(settings.database_pool_size)),
    max_overflow=max(0, int(settings.database_max_overflow)),
    pool_timeout=max(0.1, float(settings.database_pool_timeout_seconds)),
    connect_args=_database_connect_args(settings),
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


def create_low_latency_submit_session_factory(
    settings: Settings,
) -> tuple[AsyncEngine, async_sessionmaker[AsyncSession]]:
    """Build the isolated, prewarmed database pool used only by submit workers.

    The ordinary engine deliberately pre-pings every checkout because it serves
    long-lived API and maintenance tasks.  A live order already has a durable
    pre-send retry/CAS state machine, so adding a network ping before every
    required submit query only creates another scheduler/IO latency point.  A
    separate pool also prevents maintenance traffic from occupying submit
    checkout slots during a fill burst.
    """

    submit_engine = create_async_engine(
        settings.database_url,
        pool_pre_ping=False,
        pool_use_lifo=True,
        pool_size=max(1, int(settings.low_latency_submit_database_pool_size)),
        max_overflow=max(
            0,
            int(settings.low_latency_submit_database_max_overflow),
        ),
        pool_timeout=max(0.1, float(settings.database_pool_timeout_seconds)),
        connect_args=_database_connect_args(settings),
    )
    submit_sessions = async_sessionmaker(
        submit_engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )
    return submit_engine, submit_sessions


async def prewarm_database_engine(engine: AsyncEngine, *, connection_count: int) -> int:
    """Open the base submit pool concurrently, then return all slots idle/warm."""

    count = max(1, int(connection_count))

    async def connect_one():
        # SQLAlchemy returns an awaitable AsyncConnection rather than a native
        # coroutine object.  Wrapping it keeps create_task compatible with both
        # the real engine and lightweight test engines.
        return await engine.connect()

    tasks = [asyncio.create_task(connect_one()) for _ in range(count)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    connections = [item for item in results if not isinstance(item, BaseException)]
    try:
        failure = next(
            (item for item in results if isinstance(item, BaseException)),
            None,
        )
        if failure is not None:
            raise failure
        return len(connections)
    finally:
        if connections:
            await asyncio.gather(*(connection.close() for connection in connections))


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session
