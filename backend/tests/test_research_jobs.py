from __future__ import annotations

from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from app.api.research import ResearchJobCreate, _public_job, _request_hash, _request_payload
from app.tasks.leader_research_worker import _without_addresses


def test_research_request_normalizes_public_address() -> None:
    payload = ResearchJobCreate(
        tool="suitability",
        address="0x" + "A" * 40,
    )

    assert payload.address == "0x" + "a" * 40
    assert _request_payload(payload)["target_tail_pct"] == "7.5"


def test_research_request_rejects_malformed_address() -> None:
    with pytest.raises(ValidationError):
        ResearchJobCreate(tool="balance", address="a" * 40)


def test_research_cache_fingerprint_includes_tool_and_parameters() -> None:
    base = ResearchJobCreate(tool="suitability", address="0x" + "1" * 40)
    balance = ResearchJobCreate(tool="balance", address="0x" + "1" * 40)
    higher_friction = ResearchJobCreate(
        tool="suitability",
        address="0x" + "1" * 40,
        friction_bps="8",
    )

    assert _request_hash(_request_payload(base)) != _request_hash(_request_payload(balance))
    assert _request_hash(_request_payload(base)) != _request_hash(
        _request_payload(higher_friction)
    )


def test_worker_removes_addresses_from_persisted_result() -> None:
    payload = {
        "leaders": [
            {
                "address": "0x" + "1" * 40,
                "leader_address": "0x" + "2" * 40,
                "score": "80",
                "nested": {"address": "0x" + "3" * 40, "value": "ok"},
            }
        ]
    }

    sanitized = _without_addresses(payload)

    assert sanitized == {
        "leaders": [{"score": "80", "nested": {"value": "ok"}}]
    }


def test_public_job_exposes_only_operator_fields() -> None:
    row = SimpleNamespace(
        key="leader_research_job:00000000-0000-0000-0000-000000000000",
        value={
            "id": "00000000-0000-0000-0000-000000000000",
            "status": "COMPLETED",
            "request": {
                "tool": "suitability",
                "address": "0x" + "1" * 40,
                "friction_bps": "5",
                "target_tail_pct": "7.5",
                "round_to": "10000",
                "follower_balance": "20000",
            },
            "request_hash": "must-not-be-returned",
            "result": {"leaders": []},
        },
        created_at=SimpleNamespace(isoformat=lambda: "created"),
    )

    result = _public_job(row)

    assert result["status"] == "COMPLETED"
    assert result["address"].endswith("1111")
    assert "request_hash" not in result
