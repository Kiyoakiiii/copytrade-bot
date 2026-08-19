from __future__ import annotations

import sys
from pathlib import Path

import pytest


SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import leader_loss_risk_report as research  # noqa: E402


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self) -> bytes:
        return b'{"ok":true}'


def test_public_info_network_budget_counts_every_cache_miss(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("COPYTRADE_PUBLIC_INFO_MIN_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("COPYTRADE_PUBLIC_INFO_MAX_REQUESTS", "1")
    monkeypatch.delenv("COPYTRADE_PUBLIC_INFO_RATE_STATE", raising=False)
    monkeypatch.setattr(research.urllib.request, "urlopen", lambda *_args, **_kwargs: _Response())
    client = research.PublicInfoClient(tmp_path)

    assert client.post({"type": "portfolio"}, "first") == {"ok": True}
    with pytest.raises(RuntimeError, match="request budget reached"):
        client.post({"type": "portfolio"}, "second")


def test_public_info_cache_hit_consumes_no_network_budget(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("COPYTRADE_PUBLIC_INFO_MIN_INTERVAL_SECONDS", "0")
    monkeypatch.setenv("COPYTRADE_PUBLIC_INFO_MAX_REQUESTS", "1")
    monkeypatch.delenv("COPYTRADE_PUBLIC_INFO_RATE_STATE", raising=False)
    monkeypatch.setattr(research.urllib.request, "urlopen", lambda *_args, **_kwargs: _Response())
    client = research.PublicInfoClient(tmp_path)

    assert client.post({"type": "portfolio"}, "same") == {"ok": True}
    assert client.post({"type": "portfolio"}, "same") == {"ok": True}
    assert client._network_requests == 1


def test_public_history_cache_namespace_does_not_expose_address(tmp_path) -> None:
    address = "0x" + "a" * 40
    path = research.public_history_cache_namespace(tmp_path, 123, address)

    assert address not in str(path)
    assert path.parent.name == "123"
