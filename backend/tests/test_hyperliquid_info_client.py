import asyncio

import httpx

from app.services.hyperliquid import HyperliquidInfoClient


class ConcurrentProbeClient:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0

    async def post(self, url, json):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        return httpx.Response(
            200,
            json={"ok": True},
            request=httpx.Request("POST", url),
        )

    async def aclose(self):
        return None


def test_info_client_bounds_concurrent_requests_during_bursts() -> None:
    async def scenario() -> int:
        client = HyperliquidInfoClient("https://example.test/info")
        await client._client.aclose()
        probe = ConcurrentProbeClient()
        client._client = probe
        results = await asyncio.gather(
            *(client.post_info({"type": "meta", "index": index}) for index in range(20))
        )
        await client.close()
        assert results == [{"ok": True}] * 20
        return probe.max_active

    assert asyncio.run(scenario()) == 2


def test_info_client_429_sets_one_shared_retry_deadline(monkeypatch) -> None:
    class RateLimitedProbeClient:
        def __init__(self) -> None:
            self.calls = 0

        async def post(self, url, json):
            self.calls += 1
            status = 429 if self.calls == 1 else 200
            return httpx.Response(
                status,
                json={"ok": status == 200},
                request=httpx.Request("POST", url),
            )

        async def aclose(self):
            return None

    async def scenario() -> tuple[dict, int, float]:
        client = HyperliquidInfoClient("https://example.test/info")
        await client._client.aclose()
        probe = RateLimitedProbeClient()
        client._client = probe
        monkeypatch.setattr("app.services.hyperliquid.random.uniform", lambda _a, _b: 0.0)
        result = await client.post_info({"type": "meta"})
        retry_deadline = client._retry_not_before
        await client.close()
        return result, probe.calls, retry_deadline

    result, calls, retry_deadline = asyncio.run(scenario())

    assert result == {"ok": True}
    assert calls == 2
    assert retry_deadline > 0
