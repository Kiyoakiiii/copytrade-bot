import asyncio
from decimal import Decimal
from types import SimpleNamespace

from app.models import AppSetting
from app.services.follower_migration import (
    FOLLOWER_MIGRATION_BLOCKED,
    FOLLOWER_MIGRATION_READY,
    prepare_follower_runtime_identity,
    public_follower_migration_payload,
)


OLD_ADDRESS = "0x" + "1" * 40
NEW_ADDRESS = "0x" + "2" * 40


class MigrationSettings:
    hyperliquid_execution_network = "mainnet"
    hyperliquid_info_url = "https://example.invalid/info"
    hyperliquid_api_wallet_address = None
    hyperliquid_vault_address = None
    hyperliquid_subaccount_address = None

    def hyperliquid_follower_account_address(self):
        return NEW_ADDRESS

    def hyperliquid_signer_address(self):
        return NEW_ADDRESS

    def enabled_hyperliquid_dex_list(self):
        return [""]


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return list(self.rows)


class FakeDb:
    def __init__(self, *, identity=None, select_results=None):
        self.identity = identity
        self.risk = SimpleNamespace(value={"kill_switch": False})
        self.select_results = list(select_results or [])
        self.added = []
        self.commits = 0
        self.rollbacks = 0
        self.flushes = 0

    async def get(self, model, key):
        if model is AppSetting and key == "follower_runtime_identity":
            return SimpleNamespace(value=self.identity) if self.identity is not None else None
        if model is AppSetting and key == "risk":
            return self.risk
        return None

    async def execute(self, statement):
        if getattr(statement, "is_select", False):
            return FakeResult(self.select_results.pop(0))
        return FakeResult([])

    def add(self, value):
        self.added.append(value)

    async def flush(self):
        self.flushes += 1

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1


class FakeInfoClient:
    def __init__(self, *, old_size="0", open_orders=None):
        self.old_size = old_size
        self.orders = list(open_orders or [])
        self.clearinghouse_calls = 0
        self.open_order_calls = 0

    async def clearinghouse_state(self, address, dex=""):
        self.clearinghouse_calls += 1
        size = self.old_size if address.lower() == OLD_ADDRESS else "0"
        positions = (
            [{"position": {"coin": "HYPE", "szi": size}}]
            if Decimal(size) != 0
            else []
        )
        return {
            "marginSummary": {"accountValue": "1000"},
            "assetPositions": positions,
        }

    async def open_orders(self, address):
        self.open_order_calls += 1
        return list(self.orders)


def identity_payload():
    return {
        "schema_version": 1,
        "status": FOLLOWER_MIGRATION_READY,
        "active_account_address": OLD_ADDRESS,
        "configured_account_address": OLD_ADDRESS,
        "active_network": "mainnet",
        "configured_network": "mainnet",
        "blockers": [],
    }


def allocation():
    return SimpleNamespace(
        id=7,
        leader_id=4,
        leader_address="0x" + "4" * 40,
        execution_venue="HYPERLIQUID",
        dex="",
        canonical_coin="HYPE",
        position_side="LONG",
        target_notional=Decimal("100"),
        allocated_notional=Decimal("95"),
        allocated_qty=Decimal("10"),
        status="OPEN",
        pending_reduce_qty=Decimal("1"),
        pending_reduce_notional=Decimal("9"),
        pending_reduce_reason="test",
        pending_reduce_since=None,
        pending_reduce_source_fill_id="fill-1",
        last_reconcile_at=None,
    )


def test_initialization_records_current_identity_without_migrating() -> None:
    db = FakeDb()
    result = asyncio.run(
        prepare_follower_runtime_identity(db, settings=MigrationSettings())
    )

    assert result.ready is True
    assert result.changed is False
    assert result.payload["active_account_address"] == NEW_ADDRESS
    assert result.payload["last_action"] == "INITIALIZED_CURRENT_ACCOUNT"


def test_changed_follower_blocks_manual_migration_without_account_checks() -> None:
    client = FakeInfoClient(old_size="3.5", open_orders=[{"oid": 123}])
    db = FakeDb(identity=identity_payload(), select_results=[["must-not-be-consumed"]])
    result = asyncio.run(
        prepare_follower_runtime_identity(
            db,
            settings=MigrationSettings(),
            info_client=client,
        )
    )

    assert result.ready is False
    assert result.changed is True
    assert result.status == FOLLOWER_MIGRATION_BLOCKED
    assert any("automatic follower migration is disabled" in item for item in result.blockers)
    assert result.payload["active_account_address"] == OLD_ADDRESS
    assert result.payload["configured_account_address"] == NEW_ADDRESS
    assert result.payload["automatic_migration_disabled"] is True
    assert result.payload["kill_switch_forced_on"] is True
    assert result.payload["last_action"] == "MANUAL_FOLLOWER_MIGRATION_REQUIRED"
    assert "old_open_positions" not in result.payload
    assert client.clearinghouse_calls == 0
    assert client.open_order_calls == 0
    assert db.select_results == [["must-not-be-consumed"]]
    assert db.added == []


def test_changed_follower_does_not_archive_allocations_or_reset_baselines() -> None:
    old_allocation = allocation()
    db = FakeDb(identity=identity_payload(), select_results=[[old_allocation]])
    result = asyncio.run(
        prepare_follower_runtime_identity(
            db,
            settings=MigrationSettings(),
            info_client=FakeInfoClient(),
        )
    )

    assert result.ready is False
    assert result.changed is True
    assert result.status == FOLLOWER_MIGRATION_BLOCKED
    assert old_allocation.status == "OPEN"
    assert old_allocation.allocated_qty == Decimal("10")
    assert old_allocation.allocated_notional == Decimal("95")
    assert old_allocation.pending_reduce_qty == Decimal("1")
    assert db.added == []
    assert db.rollbacks == 0
    assert db.select_results == [[old_allocation]]


def test_public_payload_masks_follower_addresses() -> None:
    payload = public_follower_migration_payload(
        {
            "active_account_address": OLD_ADDRESS,
            "configured_account_address": NEW_ADDRESS,
            "previous_account_address": OLD_ADDRESS,
        }
    )

    assert payload["active_account_address"] == "0x1111...1111"
    assert payload["configured_account_address"] == "0x2222...2222"
