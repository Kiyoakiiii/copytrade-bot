import asyncio

import pytest

from app.core.config import Settings
from app.db.session import (
    create_low_latency_submit_session_factory,
    prewarm_database_engine,
)


def submit_db_settings(**overrides) -> Settings:
    values = {
        "database_url": "postgresql+asyncpg://postgres/testdb",
        "low_latency_submit_database_pool_size": 3,
        "low_latency_submit_database_max_overflow": 2,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_low_latency_submit_pool_is_dedicated_and_skips_checkout_pre_ping() -> None:
    engine, sessions = create_low_latency_submit_session_factory(submit_db_settings())

    assert sessions.kw["bind"] is engine
    assert engine.sync_engine.pool._pre_ping is False
    assert engine.sync_engine.pool.size() == 3
    assert engine.sync_engine.pool._max_overflow == 2

    asyncio.run(engine.dispose())


def test_submit_pool_prewarm_opens_all_connections_concurrently_and_returns_them() -> None:
    class FakeConnection:
        def __init__(self, owner):
            self.owner = owner

        async def close(self):
            self.owner.closed += 1

    class FakeEngine:
        def __init__(self):
            self.started = 0
            self.max_started_before_yield = 0
            self.closed = 0

        async def connect(self):
            self.started += 1
            self.max_started_before_yield = max(self.max_started_before_yield, self.started)
            await asyncio.sleep(0)
            return FakeConnection(self)

    engine = FakeEngine()

    warmed = asyncio.run(prewarm_database_engine(engine, connection_count=8))

    assert warmed == 8
    assert engine.started == 8
    assert engine.max_started_before_yield == 8
    assert engine.closed == 8


def test_submit_pool_prewarm_accepts_connect_awaitable_that_is_not_a_coroutine() -> None:
    class FakeConnection:
        def __init__(self, owner):
            self.owner = owner

        async def close(self):
            self.owner.closed += 1

    class ConnectAwaitable:
        def __init__(self, connection):
            self.connection = connection

        def __await__(self):
            async def resolve():
                await asyncio.sleep(0)
                return self.connection

            return resolve().__await__()

    class SqlAlchemyShapedEngine:
        def __init__(self):
            self.closed = 0

        def connect(self):
            return ConnectAwaitable(FakeConnection(self))

    engine = SqlAlchemyShapedEngine()

    warmed = asyncio.run(prewarm_database_engine(engine, connection_count=3))

    assert warmed == 3
    assert engine.closed == 3


def test_submit_pool_prewarm_closes_successful_connections_if_one_open_fails() -> None:
    class FakeConnection:
        def __init__(self, owner):
            self.owner = owner

        async def close(self):
            self.owner.closed += 1

    class PartiallyFailingEngine:
        def __init__(self):
            self.calls = 0
            self.closed = 0

        async def connect(self):
            self.calls += 1
            call = self.calls
            await asyncio.sleep(0)
            if call == 2:
                raise ConnectionError("prewarm failed")
            return FakeConnection(self)

    engine = PartiallyFailingEngine()

    with pytest.raises(ConnectionError, match="prewarm failed"):
        asyncio.run(prewarm_database_engine(engine, connection_count=4))

    assert engine.closed == 3
