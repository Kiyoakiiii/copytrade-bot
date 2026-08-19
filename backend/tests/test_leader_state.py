from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.services.leader_state import parse_leader_state
from app.services.target_position import PositionSide
from app.tasks import leader_state_poller
from app.tasks.leader_state_poller import _leader_requires_dynamic_account_abstraction


def test_parse_leader_state_positions_and_leverage_display_only() -> None:
    state = parse_leader_state(
        "0xLeader",
        {
            "marginSummary": {
                "accountValue": "80000",
                "totalNtlPos": "40000",
                "totalMarginUsed": "1000",
            },
            "withdrawable": "79000",
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "1",
                        "positionValue": "40000",
                        "entryPx": "39000",
                        "unrealizedPnl": "1000",
                        "leverage": {"type": "isolated", "value": 30},
                    }
                }
            ],
        },
        updated_at=datetime(2026, 4, 25, tzinfo=timezone.utc),
    )

    assert state.leader_address == "0xleader"
    assert state.account_value == Decimal("80000")
    assert state.withdrawable == Decimal("79000")
    assert state.positions[0].side == PositionSide.LONG
    assert state.positions[0].notional == Decimal("40000")
    assert state.positions[0].mark_price == Decimal("40000.00000000")
    assert state.positions[0].leverage == Decimal("30")


def test_fixed_leader_sizing_skips_redundant_dynamic_abstraction_poll() -> None:
    class FixedLeader:
        fixed_account_value = Decimal("50000")

    class MissingFixedLeader:
        fixed_account_value = None

    assert _leader_requires_dynamic_account_abstraction(FixedLeader()) is False
    assert _leader_requires_dynamic_account_abstraction(MissingFixedLeader()) is True


@pytest.mark.asyncio
async def test_monitoring_poller_never_waits_on_exchange_inside_db_transaction(
    monkeypatch,
) -> None:
    leader = SimpleNamespace(
        id=1,
        leader_address="0xleader",
        fixed_account_value=Decimal("50000"),
    )

    class Result:
        def scalars(self):
            return self

        def all(self):
            return [leader]

    class Session:
        transaction_open = False
        commits = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def execute(self, _statement):
            self.transaction_open = True
            return Result()

        async def get(self, _model, _key):
            self.transaction_open = True
            return SimpleNamespace(value={"ready": True})

        async def commit(self):
            self.transaction_open = False
            self.commits += 1

        async def rollback(self):
            self.transaction_open = False

    session = Session()
    monkeypatch.setattr(leader_state_poller, "SessionLocal", lambda: session)
    dexes = [
        SimpleNamespace(dex_name="", display_name="Default"),
        SimpleNamespace(dex_name="xyz", display_name="XYZ"),
    ]
    monkeypatch.setattr(
        leader_state_poller,
        "HyperliquidDexRegistry",
        lambda _settings: SimpleNamespace(enabled_dexes=lambda: dexes),
    )

    async def db_write(db, *_args, **_kwargs):
        db.transaction_open = True

    monkeypatch.setattr(leader_state_poller, "store_task_status", db_write)
    monkeypatch.setattr(leader_state_poller, "save_account_state", db_write)
    monkeypatch.setattr(leader_state_poller, "sync_waiting_baselines_from_state", db_write)
    monkeypatch.setattr(
        leader_state_poller,
        "parse_account_state",
        lambda **kwargs: SimpleNamespace(dex=kwargs["dex"]),
    )
    monkeypatch.setattr(
        leader_state_poller,
        "parse_leader_state",
        lambda address, *_args, **_kwargs: SimpleNamespace(
            leader_address=address,
            account_value=Decimal("1"),
            withdrawable=Decimal("1"),
            total_ntl_pos=Decimal("0"),
            total_margin_used=Decimal("0"),
            websocket_status="connected",
            updated_at=datetime.now(timezone.utc),
        ),
    )
    monkeypatch.setattr(
        leader_state_poller,
        "leader_state_to_json",
        lambda _state: {"positions": []},
    )

    calls: list[tuple[str, str]] = []

    class Client:
        async def all_mids(self, dex):
            assert session.transaction_open is False
            calls.append(("mids", dex))
            return {}

        async def clearinghouse_state(self, _address, *, dex):
            assert session.transaction_open is False
            calls.append(("state", dex))
            return {}

    settings = SimpleNamespace(
        low_latency_required_for_live=True,
        hyperliquid_follower_account_address=lambda: None,
    )
    await leader_state_poller.poll_once(Client(), settings=settings)

    assert calls == [
        ("mids", ""),
        ("mids", "xyz"),
        ("state", ""),
        ("state", "xyz"),
    ]
    assert session.transaction_open is False
    assert session.commits >= 5
