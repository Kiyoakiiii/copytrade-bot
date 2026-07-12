import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from app.models import AppSetting, LeaderPositionBaseline
from app.services.account_state import AccountPositionState, AccountState
from app.services.baseline import (
    BASELINE_CLEARED,
    BASELINE_COPY_ALLOWED,
    BASELINE_WAIT_UNTIL_FLAT,
    GATE_BASELINE_CLEARED,
    GATE_BLOCKED_UNKNOWN,
    GATE_COPY_ALLOWED,
    GATE_WAIT_UNTIL_FLAT,
    baseline_capture_setting_key,
    baseline_copy_gate,
    baseline_status_for_position,
    capture_leader_position_baselines_from_state,
    evaluate_baseline_gate,
    sync_waiting_baselines_from_state,
)
from app.services.low_latency_watcher import (
    FillDrivenExecutionEngine,
    FillEvent,
    LowLatencyPriceCache,
    MarketKey,
    build_fill_event,
    derive_leader_post_position_from_fill,
)
from app.services.target_position import PositionSide


def leader(**overrides):
    item = SimpleNamespace(
        id=1,
        leader_address=("0x" + "1" * 40).lower(),
        enabled=True,
        deleted_at=None,
        allowed_symbols=None,
        blocked_symbols=[],
        copy_multiplier=Decimal("0.1"),
    )
    for key, value in overrides.items():
        setattr(item, key, value)
    return item


def baseline(status=BASELINE_WAIT_UNTIL_FLAT, *, leader_id=1, dex="xyz", coin="xyz:HYUNDAI"):
    return SimpleNamespace(
        id=7,
        leader_id=leader_id,
        leader_address=("0x" + str(leader_id))[:42],
        execution_venue="HYPERLIQUID",
        dex=dex,
        canonical_coin=coin,
        baseline_status=status,
        side_at_enable="SHORT",
        size_at_enable=Decimal("100"),
        notional_at_enable=Decimal("1000"),
        account_value_at_enable=Decimal("10000"),
        flat_confirmed_at=None,
        copy_allowed_at=None,
        last_leader_size=None,
        last_leader_notional=None,
        last_leader_entry_px=None,
        last_leader_mark_px=None,
        last_checked_at=None,
        reason=None,
    )


def fill_implied(
    *,
    side_after=PositionSide.LONG,
    size_after="200",
    signed_size_after="200",
    notional_after_estimate="13180",
    start_position="0",
    confidence="HIGH",
    is_open=True,
    is_increase=False,
    is_reduce=False,
    is_close=False,
    direction="Open Long",
):
    return SimpleNamespace(
        dex="xyz",
        canonical_coin="xyz:URNM",
        side_after=side_after,
        size_after=Decimal(size_after),
        signed_size_after=Decimal(signed_size_after),
        notional_after_estimate=Decimal(notional_after_estimate),
        fill_size=Decimal("200"),
        start_position=Decimal(start_position) if start_position is not None else None,
        direction=direction,
        is_open=is_open,
        is_increase=is_increase,
        is_reduce=is_reduce,
        is_close=is_close,
        is_flip=False,
        confidence=confidence,
        reason="test fill implied",
    )


def position(*, coin="HYUNDAI", dex="xyz", side="SHORT", size="-100", notional="1000", entry_px=None, mark_px=None, product_type="perp"):
    return AccountPositionState(
        coin=coin,
        dex=dex,
        canonical_coin=f"{dex}:{coin}" if dex else coin,
        raw_coin=f"{dex}:{coin}" if dex else coin,
        product_type=product_type,
        side=side,
        size=Decimal(size),
        notional=Decimal(notional),
        entry_px=Decimal(entry_px) if entry_px is not None else None,
        mark_px=Decimal(mark_px) if mark_px is not None else None,
        mid_px=Decimal(mark_px) if mark_px is not None else None,
        mark_px_source="TEST" if mark_px is not None else None,
        position_opened_at=None,
        open_time_source="FIRST_SEEN",
        unrealized_pnl=None,
        leverage=None,
        margin_used=None,
        liquidation_px=None,
        raw_payload_masked={},
    )


def state(*positions, dex="xyz"):
    return AccountState(
        role="LEADER",
        address=("0x" + "1" * 40).lower(),
        dex=dex,
        dex_display_name=dex or "Hyperliquid",
        account_label=None,
        account_value=Decimal("10000"),
        withdrawable=None,
        total_ntl_pos=None,
        total_raw_usd=None,
        total_margin_used=None,
        positions=list(positions),
        raw_payload_masked={},
        source="test",
        updated_at=datetime.now(timezone.utc),
    )


def fill_event(*, coin: str = "HYUNDAI", dex: str = "xyz") -> FillEvent:
    canonical = f"{dex}:{coin}" if dex else coin
    return FillEvent(
        source_fill_id="fill-baseline",
        leader_address=("0x" + "1" * 40).lower(),
        market=MarketKey(
            dex=dex,
            coin=coin,
            canonical_coin=canonical,
            raw_coin=canonical,
            asset_id=1,
            venue_symbol=canonical,
        ),
        side="B",
        price=Decimal("10"),
        size=Decimal("1"),
        time_ms=1_700_000_000_000,
        raw={"coin": canonical, "px": "10", "sz": "1", "time": 1_700_000_000_000},
        is_snapshot=False,
        ws_received_at=datetime.fromtimestamp(1_700_000_000_050 / 1000, timezone.utc),
    )


class Result:
    def __init__(self, rows):
        self.rows = rows

    def scalars(self):
        return self

    def all(self):
        return self.rows


class BaselineDb:
    def __init__(self, *, capture_ready=True, scalar_result=None):
        self.baselines = []
        self.added = []
        self.settings = {
            baseline_capture_setting_key(1): AppSetting(
                key=baseline_capture_setting_key(1),
                value={"ready": capture_ready},
            )
        }
        self.scalar_result = scalar_result

    async def get(self, model, key):
        if model is AppSetting:
            return self.settings.get(key)
        return None

    async def scalar(self, stmt):
        return self.scalar_result

    async def execute(self, stmt):
        return Result(self.baselines)

    async def flush(self):
        return None

    def add(self, item):
        if isinstance(item, LeaderPositionBaseline):
            item.id = len(self.baselines) + 1
            self.baselines.append(item)
        self.added.append(item)


class InspectingBaselineDb(BaselineDb):
    def __init__(self, *, capture_ready=True, scalar_result=None):
        super().__init__(capture_ready=capture_ready, scalar_result=scalar_result)
        self.last_scalar_sql = ""

    async def scalar(self, stmt):
        self.last_scalar_sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        return self.scalar_result


def test_new_leader_existing_xyz_short_creates_wait_baseline() -> None:
    db = BaselineDb()

    asyncio.run(
        capture_leader_position_baselines_from_state(
            db,
            leader=leader(),
            state=state(position()),
            reason="leader add",
            force_reset=True,
        )
    )

    assert len(db.baselines) == 1
    assert db.baselines[0].canonical_coin == "xyz:HYUNDAI"
    assert db.baselines[0].baseline_status == BASELINE_WAIT_UNTIL_FLAT


def test_baseline_preserves_entry_and_unknown_product_positions() -> None:
    db = BaselineDb()

    asyncio.run(
        capture_leader_position_baselines_from_state(
            db,
            leader=leader(allowed_symbols=None),
            state=state(position(coin="WEIRDSTOCK", entry_px="12.34", mark_px="12.50", product_type="unknown")),
            reason="leader add",
        )
    )

    assert db.baselines[0].canonical_coin == "xyz:WEIRDSTOCK"
    assert db.baselines[0].entry_px_at_enable == Decimal("12.34")
    assert db.baselines[0].mark_px_at_enable == Decimal("12.50")
    assert db.baselines[0].last_leader_entry_px == Decimal("12.34")


def test_sync_waiting_baselines_clears_flat_position_without_creating_new_rows() -> None:
    db = BaselineDb()
    db.baselines.append(baseline(coin="xyz:URNM"))
    db.baselines.append(baseline(coin="xyz:HYUNDAI"))

    asyncio.run(
        sync_waiting_baselines_from_state(
            db,
            leader=leader(),
            state=state(position(coin="HYUNDAI", entry_px="379.8", mark_px="374.5")),
            now=datetime(2026, 5, 2, tzinfo=timezone.utc),
        )
    )

    urnm = db.baselines[0]
    hyundai = db.baselines[1]
    assert urnm.baseline_status == BASELINE_CLEARED
    assert urnm.flat_confirmed_at is not None
    assert urnm.last_leader_size == Decimal("0")
    assert "latest leader account state" in urnm.reason
    assert hyundai.baseline_status == BASELINE_WAIT_UNTIL_FLAT
    assert hyundai.last_leader_entry_px == Decimal("379.8")
    assert len(db.baselines) == 2


def test_allowed_all_coins_baselines_all_open_positions() -> None:
    db = BaselineDb()

    asyncio.run(
        capture_leader_position_baselines_from_state(
            db,
            leader=leader(allowed_symbols=None),
            state=state(position(coin="HYUNDAI"), position(coin="URNM", size="50", notional="500")),
            reason="leader add",
        )
    )

    assert {row.canonical_coin for row in db.baselines} == {"xyz:HYUNDAI", "xyz:URNM"}


def test_custom_allowlist_baselines_only_allowed_existing_positions() -> None:
    db = BaselineDb()

    asyncio.run(
        capture_leader_position_baselines_from_state(
            db,
            leader=leader(allowed_symbols=["xyz:HYUNDAI"]),
            state=state(position(coin="HYUNDAI"), position(coin="URNM", size="50", notional="500")),
            reason="allowlist add",
        )
    )

    assert [row.canonical_coin for row in db.baselines] == ["xyz:HYUNDAI"]


def test_wait_until_flat_reduce_add_close_and_flip_are_ignored() -> None:
    row = baseline()

    reduce = evaluate_baseline_gate(row, current_leader_side=PositionSide.SHORT, current_leader_size=Decimal("50"), current_leader_notional=Decimal("-500"))
    add = evaluate_baseline_gate(row, current_leader_side=PositionSide.SHORT, current_leader_size=Decimal("120"), current_leader_notional=Decimal("-1200"))
    flip_without_flat = evaluate_baseline_gate(row, current_leader_side=PositionSide.LONG, current_leader_size=Decimal("20"), current_leader_notional=Decimal("200"))

    assert reduce.status == add.status == flip_without_flat.status == GATE_WAIT_UNTIL_FLAT
    assert reduce.copy_allowed is add.copy_allowed is flip_without_flat.copy_allowed is False


def test_flat_event_clears_baseline_but_does_not_copy() -> None:
    row = baseline()

    result = evaluate_baseline_gate(row, current_leader_side=PositionSide.FLAT, current_leader_size=Decimal("0"), current_leader_notional=Decimal("0"))

    assert result.copy_allowed is False
    assert result.status == GATE_BASELINE_CLEARED
    assert row.baseline_status == BASELINE_CLEARED
    assert row.flat_confirmed_at is not None


def test_cleared_snapshot_position_without_open_fill_waits_until_flat() -> None:
    long_row = baseline(BASELINE_CLEARED)
    short_row = baseline(BASELINE_CLEARED)

    long_result = evaluate_baseline_gate(long_row, current_leader_side=PositionSide.LONG, current_leader_size=Decimal("50"), current_leader_notional=Decimal("500"))
    short_result = evaluate_baseline_gate(short_row, current_leader_side=PositionSide.SHORT, current_leader_size=Decimal("50"), current_leader_notional=Decimal("-500"))

    assert long_result.copy_allowed is False
    assert short_result.copy_allowed is False
    assert long_result.status == short_result.status == GATE_WAIT_UNTIL_FLAT
    assert long_row.baseline_status == short_row.baseline_status == BASELINE_WAIT_UNTIL_FLAT


def test_new_copy_allowed_lifecycle_then_open_increase_reduce_close_can_copy() -> None:
    row = baseline(BASELINE_COPY_ALLOWED)

    for side, size, notional in [
        (PositionSide.LONG, Decimal("10"), Decimal("100")),
        (PositionSide.LONG, Decimal("20"), Decimal("200")),
        (PositionSide.LONG, Decimal("5"), Decimal("50")),
        (PositionSide.FLAT, Decimal("0"), Decimal("0")),
    ]:
        result = evaluate_baseline_gate(row, current_leader_side=side, current_leader_size=size, current_leader_notional=notional)
        assert result.copy_allowed is True
        assert result.status == GATE_COPY_ALLOWED


def test_baseline_absent_after_capture_still_requires_open_from_flat_fill() -> None:
    db = BaselineDb(capture_ready=True, scalar_result=None)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:URNM",
            current_leader_side=PositionSide.LONG,
            current_leader_size=Decimal("1"),
            current_leader_notional=Decimal("100"),
        )
    )

    assert result.copy_allowed is False
    assert result.status == GATE_WAIT_UNTIL_FLAT
    assert result.reason == "No copied lifecycle exists; waiting for leader open from flat."


def test_baseline_absent_allows_open_from_flat_fill_only() -> None:
    db = BaselineDb(capture_ready=True, scalar_result=None)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:URNM",
            current_leader_side=PositionSide.FLAT,
            current_leader_size=Decimal("0"),
            current_leader_notional=Decimal("0"),
            current_position_source="fill_implied",
            fill_implied_position=fill_implied(),
            snapshot_position_optional=SimpleNamespace(side="FLAT", size=Decimal("0"), notional=Decimal("0")),
        )
    )

    assert result.copy_allowed is True
    assert result.status == GATE_COPY_ALLOWED
    assert result.reason == "New open from flat; starting copy lifecycle."


def test_source_fills_checkpoint_cannot_replace_position_baseline() -> None:
    db = BaselineDb(capture_ready=False, scalar_result=None)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:HYUNDAI",
            current_leader_side=PositionSide.LONG,
            current_leader_size=Decimal("1"),
            current_leader_notional=Decimal("100"),
        )
    )

    assert result.copy_allowed is False
    assert result.status == GATE_BLOCKED_UNKNOWN


def test_baseline_gate_lookup_is_case_insensitive_for_dex_prefixed_coin() -> None:
    db = InspectingBaselineDb(capture_ready=True, scalar_result=baseline(coin="xyz:HYUNDAI"))

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:HYUNDAI",
            current_leader_side=PositionSide.SHORT,
            current_leader_size=Decimal("14.031"),
            current_leader_notional=Decimal("-5322.13982"),
        )
    )

    assert "upper(" in db.last_scalar_sql.lower()
    assert result.copy_allowed is False
    assert result.status == GATE_WAIT_UNTIL_FLAT


def test_cleared_baseline_uses_fill_implied_open_long_even_when_snapshot_is_flat() -> None:
    row = baseline(BASELINE_CLEARED, coin="xyz:URNM")
    db = BaselineDb(capture_ready=True, scalar_result=row)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:URNM",
            current_leader_side=PositionSide.FLAT,
            current_leader_size=Decimal("0"),
            current_leader_notional=Decimal("0"),
            current_position_source="fill_implied",
            fill_implied_position=fill_implied(),
            snapshot_position_optional=SimpleNamespace(side="FLAT", size=Decimal("0"), notional=Decimal("0")),
        )
    )

    assert result.copy_allowed is True
    assert result.status == GATE_COPY_ALLOWED
    assert row.baseline_status == BASELINE_COPY_ALLOWED
    assert row.last_leader_size == Decimal("200")


def test_real_urnm_replay_does_not_become_ignored_baseline_position() -> None:
    row = baseline(BASELINE_CLEARED, coin="xyz:URNM")
    db = BaselineDb(capture_ready=True, scalar_result=row)
    fill = build_fill_event(
        "0x" + "1" * 40,
        {
            "coin": "xyz:URNM",
            "px": "65.9",
            "sz": "200.0",
            "side": "B",
            "time": 1777805629735,
            "startPosition": "0.0",
            "dir": "Open Long",
            "closedPnl": "0.0",
            "oid": 408315844823,
            "tid": 362146563475037,
            "hash": "<REDACTED_32_BYTE_HEX_IDENTIFIER>",
        },
        is_snapshot=False,
    )
    implied = derive_leader_post_position_from_fill(fill)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:URNM",
            current_leader_side=PositionSide.FLAT,
            current_leader_size=Decimal("0"),
            current_leader_notional=Decimal("0"),
            current_position_source="fill_implied",
            fill_implied_position=implied,
            snapshot_position_optional=SimpleNamespace(side="FLAT", size=Decimal("0"), notional=Decimal("0")),
        )
    )

    assert implied.is_open is True
    assert implied.start_position == Decimal("0.0")
    assert implied.size_after == Decimal("200.0")
    assert result.copy_allowed is True
    assert result.status == GATE_COPY_ALLOWED
    assert row.baseline_status == BASELINE_COPY_ALLOWED


def test_waiting_baseline_copies_real_open_from_flat_fill() -> None:
    row = baseline(BASELINE_WAIT_UNTIL_FLAT, coin="xyz:URNM")
    db = BaselineDb(capture_ready=True, scalar_result=row)
    fill = build_fill_event(
        "0x" + "1" * 40,
        {
            "coin": "xyz:URNM",
            "px": "65.9",
            "sz": "200.0",
            "side": "B",
            "time": 1777805629735,
            "startPosition": "0.0",
            "dir": "Open Long",
            "closedPnl": "0.0",
            "oid": 408315844823,
            "tid": 362146563475037,
            "hash": "<REDACTED_32_BYTE_HEX_IDENTIFIER>",
        },
        is_snapshot=False,
    )
    implied = derive_leader_post_position_from_fill(fill)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:URNM",
            current_leader_side=PositionSide.FLAT,
            current_leader_size=Decimal("0"),
            current_leader_notional=Decimal("0"),
            current_position_source="fill_implied",
            fill_implied_position=implied,
            snapshot_position_optional=SimpleNamespace(side="FLAT", size=Decimal("0"), notional=Decimal("0")),
        )
    )

    assert implied.is_open is True
    assert implied.start_position == Decimal("0.0")
    assert result.copy_allowed is True
    assert result.status == GATE_COPY_ALLOWED
    assert result.reason == "New open from flat; starting copy lifecycle."
    assert row.baseline_status == BASELINE_COPY_ALLOWED


def test_cleared_baseline_does_not_copy_reduce_of_missed_lifecycle() -> None:
    row = baseline(BASELINE_CLEARED, coin="xyz:URNM")
    db = BaselineDb(capture_ready=True, scalar_result=row)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:URNM",
            current_leader_side=PositionSide.LONG,
            current_leader_size=Decimal("199.61"),
            current_leader_notional=Decimal("13433.753"),
            current_position_source="fill_implied",
            fill_implied_position=fill_implied(
                size_after="199.61",
                signed_size_after="199.61",
                notional_after_estimate="13433.753",
                start_position="200",
                is_open=False,
                is_reduce=True,
                direction="Close Long",
            ),
            snapshot_position_optional=None,
        )
    )

    assert result.copy_allowed is False
    assert result.status == GATE_WAIT_UNTIL_FLAT
    assert row.baseline_status == BASELINE_WAIT_UNTIL_FLAT
    assert row.size_at_enable == Decimal("199.61")
    assert row.reason == "Position appeared without a new open-from-flat fill; waiting until flat before copying."


def test_cleared_baseline_uses_fill_implied_open_short_even_when_snapshot_is_missing() -> None:
    row = baseline(BASELINE_CLEARED, coin="xyz:BIRD")
    db = BaselineDb(capture_ready=True, scalar_result=row)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:BIRD",
            current_leader_side=None,
            current_leader_size=None,
            current_leader_notional=None,
            current_position_source="fill_implied",
            fill_implied_position=fill_implied(
                side_after=PositionSide.SHORT,
                size_after="49.2",
                signed_size_after="-49.2",
                notional_after_estimate="-313.7484",
            ),
            snapshot_position_optional=None,
        )
    )

    assert result.copy_allowed is True
    assert result.status == GATE_COPY_ALLOWED
    assert row.last_leader_size == Decimal("49.2")


def test_cleared_baseline_snapshot_position_without_open_fill_waits_until_flat() -> None:
    row = baseline(BASELINE_CLEARED, coin="xyz:URNM")
    db = BaselineDb(capture_ready=True, scalar_result=row)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:URNM",
            current_leader_side=PositionSide.LONG,
            current_leader_size=Decimal("200"),
            current_leader_notional=Decimal("13180"),
            current_position_source="account_state",
            fill_implied_position=None,
            snapshot_position_optional=SimpleNamespace(side="LONG", size=Decimal("200"), notional=Decimal("13180")),
        )
    )

    assert result.copy_allowed is False
    assert result.status == GATE_WAIT_UNTIL_FLAT
    assert result.reason == "Position appeared without a new open-from-flat fill; waiting until flat before copying."
    assert row.baseline_status == BASELINE_WAIT_UNTIL_FLAT


def test_fill_derivation_unknown_blocks_baseline_without_ignored_order_semantics() -> None:
    row = baseline(BASELINE_CLEARED, coin="xyz:URNM")
    db = BaselineDb(capture_ready=True, scalar_result=row)

    result = asyncio.run(
        baseline_copy_gate(
            db,
            leader_id=1,
            execution_venue="HYPERLIQUID",
            dex="xyz",
            canonical_coin="xyz:URNM",
            current_leader_side=PositionSide.FLAT,
            current_leader_size=Decimal("0"),
            current_leader_notional=Decimal("0"),
            current_position_source="fill_implied",
            fill_implied_position=fill_implied(confidence="UNKNOWN", size_after="0", signed_size_after="0", notional_after_estimate="0", start_position=None, is_open=False),
        )
    )

    assert result.copy_allowed is False
    assert result.status == GATE_BLOCKED_UNKNOWN
    assert result.reason == "FILL_POSITION_DERIVATION_UNKNOWN"
    assert row.baseline_status == BASELINE_CLEARED


def test_baseline_is_scoped_by_coin_dex_and_leader() -> None:
    hyundai = baseline(leader_id=1, dex="xyz", coin="xyz:HYUNDAI")
    urnm = baseline(leader_id=1, dex="xyz", coin="xyz:URNM")
    default = baseline(leader_id=1, dex="", coin="HYUNDAI")
    other_leader = baseline(leader_id=2, dex="xyz", coin="xyz:HYUNDAI")

    assert hyundai.canonical_coin != urnm.canonical_coin
    assert hyundai.dex != default.dex
    assert hyundai.leader_id != other_leader.leader_id


def test_position_payload_marks_waiting_as_ignored_existing_position() -> None:
    decision = baseline_status_for_position(baseline=LeaderPositionBaseline(id=1, baseline_status=BASELINE_WAIT_UNTIL_FLAT), copy_allowed_by_config=True)

    assert decision["copyable"] is False
    assert decision["copy_status"] == "IGNORED_EXISTING_POSITION"


def test_position_payload_without_baseline_waits_for_new_open() -> None:
    decision = baseline_status_for_position(baseline=None, copy_allowed_by_config=True)

    assert decision["copyable"] is False
    assert decision["copy_status"] == "WAITING_FOR_NEW_OPEN"


def test_ignored_old_lifecycle_writes_ignored_order_without_exchange_qty() -> None:
    class Db:
        def __init__(self):
            self.added = []
            self.flushed = 0
            self.commits = 0

        def add(self, item):
            self.added.append(item)

        async def flush(self):
            self.flushed += 1

        async def commit(self):
            self.commits += 1

    engine = FillDrivenExecutionEngine(
        settings=SimpleNamespace(),
        info_client=SimpleNamespace(),
        execution_client=SimpleNamespace(),
        price_cache=LowLatencyPriceCache(stale_ms=2_000),
    )
    event = fill_event(coin="HYUNDAI", dex="xyz")
    now = datetime.now(timezone.utc)
    order = asyncio.run(
        engine._record_lifecycle_ignored_order(
            Db(),
            fill=event,
            leader=leader(),
            reason="IGNORED_OLD_LIFECYCLE: no follower allocation exists; waiting for leader open from flat",
            target_side_hint=PositionSide.SHORT,
            leader_position_notional=Decimal("-1200"),
            leader_entry_px=Decimal("72.5"),
            leader_account_value=Decimal("10000"),
            follower_account_value=Decimal("399.6"),
            dedupe_started_at=now,
            dedupe_done_at=now,
            debounce_started_at=now,
            debounce_released_at=now,
            lock_wait_started_at=now,
            lock_acquired_at=now,
            ws_received_at=event.ws_received_at,
            decision_started_at=now,
            account_cache_read_at=now,
            account_cache_read_done_at=now,
            price_cache_read_at=now,
            price_cache_read_done_at=now,
        )
    )

    assert order.status == "IGNORED"
    assert order.order_action == "IGNORED_OLD_LIFECYCLE"
    assert order.quantity == Decimal("0")
    assert order.allocation_id is None
