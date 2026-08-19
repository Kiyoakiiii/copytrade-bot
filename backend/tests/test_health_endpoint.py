from __future__ import annotations

import asyncio

from app.main import app, healthz


def test_healthz_is_lightweight_and_not_in_openapi() -> None:
    assert asyncio.run(healthz()) == {"status": "ok"}

    route = next(route for route in app.routes if getattr(route, "path", None) == "/healthz")
    assert route.include_in_schema is False
