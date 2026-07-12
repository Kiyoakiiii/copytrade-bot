from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.models import AppSetting, LeaderConfig, LeaderPositionBaseline, RiskEvent, SourceFill
from app.services.account_state import LEADER, AccountState, parse_account_state, save_account_state
from app.services.execution_router import ExecutionVenue
from app.services.hyperliquid import fill_unique_id
from app.services.hyperliquid_dex import HyperliquidDexRegistry, canonical_coin, parse_coin
from app.services.leader_config import is_coin_allowed, normalize_leader_address
from app.services.target_position import PositionSide

BASELINE_WAIT_UNTIL_FLAT = "WAIT_UNTIL_FLAT"
BASELINE_CLEARED = "CLEARED"
BASELINE_COPY_ALLOWED = "COPY_ALLOWED"
BASELINE_MANUAL_OVERRIDE = "MANUAL_OVERRIDE"
BASELINE_UNKNOWN = "BASELINE_UNKNOWN"

GATE_COPY_ALLOWED = "COPY_ALLOWED"
GATE_WAIT_UNTIL_FLAT = "WAIT_UNTIL_FLAT"
GATE_BASELINE_CLEARED = "BASELINE_CLEARED"
GATE_BLOCKED_UNKNOWN = "BLOCKED_UNKNOWN"

BASELINE_TOLERANCE = Decimal("0.00000001")


@dataclass(frozen=True)
class BaselineGateResult:
    copy_allowed: bool
    status: str
    reason: str
    baseline_id: int | None = None
    ignored_position_notional: Decimal | None = None


@dataclass(frozen=True)
class BaselineCaptureResult:
    ready: bool
    captured_count: int
    waiting_until_flat_count: int
    baseline_unknown_count: int
    error: str | None = None


def baseline_capture_setting_key(leader_id: int | None) -> str:
    return f"leader_position_baseline_capture:{leader_id}"


async def capture_leader_position_baselines(
    db: Any,
    *,
    leader: LeaderConfig,
    settings: Any,
    info_client: Any,
    reason: str,
    force_reset: bool = True,
) -> BaselineCaptureResult:
    now = datetime.now(timezone.utc)
    errors: list[str] = []
    captured = 0
    waiting = 0
    enabled_dexes = HyperliquidDexRegistry(settings).enabled_dexes()
    for dex in enabled_dexes:
        try:
            raw = await info_client.clearinghouse_state(leader.leader_address, dex=dex.dex_name)
            state = parse_account_state(
                role=LEADER,
                address=leader.leader_address,
                dex=dex.dex_name,
                clearinghouse_state=raw,
                account_label=f"Leader {leader.id} / {dex.display_name}",
                updated_at=now,
            )
            await save_account_state(db, state)
            result = await capture_leader_position_baselines_from_state(
                db,
                leader=leader,
                state=state,
                reason=reason,
                force_reset=force_reset,
                now=now,
            )
            captured += result.captured_count
            waiting += result.waiting_until_flat_count
        except Exception as exc:
            errors.append(f"{dex.dex_name or 'default'}: {str(exc)[:200]}")
    checkpoint_warning = await initialize_source_fill_checkpoint(db, leader=leader, info_client=info_client, now=now)
    ready = not errors
    error = "; ".join(errors) if errors else checkpoint_warning
    await _store_capture_status(
        db,
        leader=leader,
        ready=ready,
        captured_count=captured,
        waiting_until_flat_count=waiting,
        baseline_unknown_count=0 if ready else 1,
        reason=reason,
        error=error,
        now=now,
    )
    return BaselineCaptureResult(
        ready=ready,
        captured_count=captured,
        waiting_until_flat_count=waiting,
        baseline_unknown_count=0 if ready else 1,
        error=error,
    )


async def capture_leader_position_baselines_from_state(
    db: Any,
    *,
    leader: LeaderConfig,
    state: AccountState,
    reason: str,
    force_reset: bool = True,
    now: datetime | None = None,
) -> BaselineCaptureResult:
    now = now or datetime.now(timezone.utc)
    seen: set[str] = set()
    captured = 0
    waiting = 0
    for position in state.positions:
        if not _position_is_open(position):
            continue
        if not is_coin_allowed(leader, position.canonical_coin):
            continue
        seen.add(position.canonical_coin)
        baseline = await _load_baseline(
            db,
            leader_id=leader.id,
            execution_venue=ExecutionVenue.HYPERLIQUID.value,
            dex=position.dex,
            canonical_coin=position.canonical_coin,
        )
        if baseline is None:
            baseline = LeaderPositionBaseline(
                leader_id=leader.id,
                leader_address=normalize_leader_address(leader.leader_address),
                execution_venue=ExecutionVenue.HYPERLIQUID.value,
                dex=position.dex,
                canonical_coin=position.canonical_coin,
            )
            db.add(baseline)
            await db.flush()
        if force_reset or str(baseline.baseline_status).upper() not in {
            BASELINE_WAIT_UNTIL_FLAT,
            BASELINE_COPY_ALLOWED,
            BASELINE_MANUAL_OVERRIDE,
        }:
            baseline.side_at_enable = position.side
            baseline.size_at_enable = abs(Decimal(position.size or 0))
            baseline.notional_at_enable = abs(Decimal(position.notional or 0))
            baseline.entry_px_at_enable = position.entry_px
            baseline.mark_px_at_enable = position.mark_px
            baseline.account_value_at_enable = state.account_value
            baseline.baseline_status = BASELINE_WAIT_UNTIL_FLAT
            baseline.first_seen_at = now
            baseline.flat_confirmed_at = None
            baseline.copy_allowed_at = None
            baseline.reason = reason
        baseline.last_leader_size = abs(Decimal(position.size or 0))
        baseline.last_leader_notional = abs(Decimal(position.notional or 0))
        baseline.last_leader_entry_px = position.entry_px
        baseline.last_leader_mark_px = position.mark_px
        baseline.last_checked_at = now
        captured += 1
        if str(baseline.baseline_status).upper() == BASELINE_WAIT_UNTIL_FLAT:
            waiting += 1
    await _clear_waiting_baselines_not_seen(
        db,
        leader=leader,
        dex=state.dex,
        seen_canonical_coins=seen,
        now=now,
    )
    await db.flush()
    return BaselineCaptureResult(
        ready=True,
        captured_count=captured,
        waiting_until_flat_count=waiting,
        baseline_unknown_count=0,
    )


async def sync_waiting_baselines_from_state(
    db: Any,
    *,
    leader: LeaderConfig,
    state: AccountState,
    now: datetime | None = None,
) -> None:
    """Sync existing WAIT_UNTIL_FLAT baselines from the latest leader account state.

    This does not create new baseline rows. Once initial baseline capture is ready,
    new position lifecycles must be handled by the fill gate instead of being marked
    as pre-existing positions.
    """
    now = now or datetime.now(timezone.utc)
    open_positions = {
        position.canonical_coin.upper(): position
        for position in state.positions
        if _position_is_open(position)
    }
    rows = (
        await db.execute(
            select(LeaderPositionBaseline)
            .where(LeaderPositionBaseline.leader_id == leader.id)
            .where(LeaderPositionBaseline.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(LeaderPositionBaseline.dex == str(state.dex or "").lower())
            .where(LeaderPositionBaseline.baseline_status == BASELINE_WAIT_UNTIL_FLAT)
        )
    ).scalars().all()
    for row in rows:
        position = open_positions.get(str(row.canonical_coin or "").upper())
        if position is None:
            row.baseline_status = BASELINE_CLEARED
            row.flat_confirmed_at = row.flat_confirmed_at or now
            row.last_leader_size = Decimal("0")
            row.last_leader_notional = Decimal("0")
            row.last_leader_entry_px = None
            row.last_leader_mark_px = None
            row.last_checked_at = now
            row.reason = "Baseline cleared by latest leader account state; next new open will be copyable."
            continue
        row.last_leader_size = abs(Decimal(position.size or 0))
        row.last_leader_notional = abs(Decimal(position.notional or 0))
        row.last_leader_entry_px = position.entry_px
        row.last_leader_mark_px = position.mark_px
        row.last_checked_at = now
    await db.flush()


async def baseline_copy_gate(
    db: Any,
    *,
    leader_id: int,
    execution_venue: str,
    dex: str,
    canonical_coin: str,
    current_leader_side: PositionSide | str | None,
    current_leader_size: Decimal | None,
    current_leader_notional: Decimal | None,
    current_position_source: str | None = None,
    fill_implied_position: Any | None = None,
    snapshot_position_optional: Any | None = None,
    now: datetime | None = None,
) -> BaselineGateResult:
    now = now or datetime.now(timezone.utc)
    capture_status = await db.get(AppSetting, baseline_capture_setting_key(leader_id))
    if capture_status is None:
        return BaselineGateResult(
            copy_allowed=False,
            status=GATE_BLOCKED_UNKNOWN,
            reason="Baseline state unknown.",
        )
    if not bool((capture_status.value or {}).get("ready")):
        return BaselineGateResult(
            copy_allowed=False,
            status=GATE_BLOCKED_UNKNOWN,
            reason="Cannot capture leader baseline positions.",
        )
    resolved = _resolve_gate_position(
        current_leader_side=current_leader_side,
        current_leader_size=current_leader_size,
        current_leader_notional=current_leader_notional,
        current_position_source=current_position_source,
        fill_implied_position=fill_implied_position,
        snapshot_position_optional=snapshot_position_optional,
    )
    if resolved is None:
        return BaselineGateResult(
            copy_allowed=False,
            status=GATE_BLOCKED_UNKNOWN,
            reason="FILL_POSITION_DERIVATION_UNKNOWN",
        )
    baseline = await _load_baseline(
        db,
        leader_id=leader_id,
        execution_venue=execution_venue,
        dex=dex,
        canonical_coin=canonical_coin,
    )
    if _fill_is_new_open_from_flat(fill_implied_position):
        if baseline is not None:
            _mark_baseline_copy_allowed_for_new_open(baseline, resolved=resolved, now=now)
            await db.flush()
        return BaselineGateResult(
            copy_allowed=True,
            status=GATE_COPY_ALLOWED,
            reason="New open from flat; starting copy lifecycle.",
            baseline_id=getattr(baseline, "id", None),
            ignored_position_notional=Decimal("0"),
        )
    if baseline is None:
        return BaselineGateResult(
            copy_allowed=False,
            status=GATE_WAIT_UNTIL_FLAT,
            reason="No copied lifecycle exists; waiting for leader open from flat.",
        )
    result = evaluate_baseline_gate(
        baseline,
        current_leader_side=resolved["side"],
        current_leader_size=resolved["size"],
        current_leader_notional=resolved["notional"],
        now=now,
    )
    await db.flush()
    return result


def _mark_baseline_copy_allowed_for_new_open(
    baseline: Any,
    *,
    resolved: dict[str, Any],
    now: datetime,
) -> None:
    size = abs(Decimal(resolved.get("size") or 0))
    notional = abs(Decimal(resolved.get("notional") or 0))
    baseline.baseline_status = BASELINE_COPY_ALLOWED
    baseline.flat_confirmed_at = now
    baseline.copy_allowed_at = now
    baseline.last_leader_size = size
    baseline.last_leader_notional = notional
    baseline.last_checked_at = now
    baseline.reason = "New open from flat; starting copy lifecycle."


def _fill_is_new_open_from_flat(fill_implied_position: Any | None) -> bool:
    if fill_implied_position is None:
        return False
    confidence = str(getattr(fill_implied_position, "confidence", "") or "").upper()
    if confidence not in {"HIGH", "MEDIUM"}:
        return False
    start_position = getattr(fill_implied_position, "start_position", None)
    if start_position is None or abs(Decimal(start_position or 0)) > BASELINE_TOLERANCE:
        return False
    return bool(getattr(fill_implied_position, "is_open", False))


def _resolve_gate_position(
    *,
    current_leader_side: PositionSide | str | None,
    current_leader_size: Decimal | None,
    current_leader_notional: Decimal | None,
    current_position_source: str | None,
    fill_implied_position: Any | None,
    snapshot_position_optional: Any | None,
) -> dict[str, Any] | None:
    source = str(current_position_source or "").lower()
    if fill_implied_position is not None:
        confidence = str(getattr(fill_implied_position, "confidence", "") or "").upper()
        if confidence in {"HIGH", "MEDIUM"}:
            return {
                "side": getattr(fill_implied_position, "side_after", current_leader_side),
                "size": getattr(fill_implied_position, "size_after", current_leader_size),
                "notional": getattr(fill_implied_position, "notional_after_estimate", current_leader_notional),
            }
        if source.startswith("fill"):
            return None
    if source.startswith("fill"):
        return None
    if snapshot_position_optional is not None:
        return {
            "side": getattr(snapshot_position_optional, "side", current_leader_side),
            "size": abs(Decimal(getattr(snapshot_position_optional, "size", current_leader_size) or 0)),
            "notional": getattr(snapshot_position_optional, "notional", current_leader_notional),
        }
    return {
        "side": current_leader_side,
        "size": current_leader_size,
        "notional": current_leader_notional,
    }


def evaluate_baseline_gate(
    baseline: Any,
    *,
    current_leader_side: PositionSide | str | None,
    current_leader_size: Decimal | None,
    current_leader_notional: Decimal | None,
    now: datetime | None = None,
) -> BaselineGateResult:
    now = now or datetime.now(timezone.utc)
    status = str(getattr(baseline, "baseline_status", "") or "").upper()
    size = abs(Decimal(current_leader_size or 0))
    notional = abs(Decimal(current_leader_notional or 0))
    side = _side_value(current_leader_side)
    if hasattr(baseline, "last_leader_size"):
        baseline.last_leader_size = size
    if hasattr(baseline, "last_leader_notional"):
        baseline.last_leader_notional = notional
    if hasattr(baseline, "last_checked_at"):
        baseline.last_checked_at = now
    if status == BASELINE_WAIT_UNTIL_FLAT:
        if size > BASELINE_TOLERANCE and side != PositionSide.FLAT.value:
            return BaselineGateResult(
                copy_allowed=False,
                status=GATE_WAIT_UNTIL_FLAT,
                reason="Existing position from before tracking; waiting until flat.",
                baseline_id=getattr(baseline, "id", None),
                ignored_position_notional=notional,
            )
        baseline.baseline_status = BASELINE_CLEARED
        baseline.flat_confirmed_at = now
        baseline.reason = "Baseline cleared; next new open will be copyable."
        return BaselineGateResult(
            copy_allowed=False,
            status=GATE_BASELINE_CLEARED,
            reason="Baseline cleared; next new open will be copyable.",
            baseline_id=getattr(baseline, "id", None),
            ignored_position_notional=Decimal("0"),
        )
    if status == BASELINE_CLEARED:
        if size > BASELINE_TOLERANCE and side in {PositionSide.LONG.value, PositionSide.SHORT.value}:
            baseline.baseline_status = BASELINE_WAIT_UNTIL_FLAT
            baseline.copy_allowed_at = None
            baseline.flat_confirmed_at = None
            baseline.side_at_enable = side
            baseline.size_at_enable = size
            baseline.notional_at_enable = notional
            baseline.reason = "Position appeared without a new open-from-flat fill; waiting until flat before copying."
            return BaselineGateResult(
                copy_allowed=False,
                status=GATE_WAIT_UNTIL_FLAT,
                reason="Position appeared without a new open-from-flat fill; waiting until flat before copying.",
                baseline_id=getattr(baseline, "id", None),
                ignored_position_notional=notional,
            )
        return BaselineGateResult(
            copy_allowed=False,
            status=GATE_BASELINE_CLEARED,
            reason="Baseline cleared; next new open will be copyable.",
            baseline_id=getattr(baseline, "id", None),
            ignored_position_notional=Decimal("0"),
        )
    if status in {BASELINE_COPY_ALLOWED, BASELINE_MANUAL_OVERRIDE}:
        return BaselineGateResult(
            copy_allowed=True,
            status=GATE_COPY_ALLOWED,
            reason="COPY_ALLOWED lifecycle.",
            baseline_id=getattr(baseline, "id", None),
            ignored_position_notional=Decimal("0"),
        )
    return BaselineGateResult(
        copy_allowed=False,
        status=GATE_BLOCKED_UNKNOWN,
        reason="Baseline state unknown.",
        baseline_id=getattr(baseline, "id", None),
        ignored_position_notional=notional,
    )


async def initialize_source_fill_checkpoint(
    db: Any,
    *,
    leader: LeaderConfig,
    info_client: Any,
    now: datetime | None = None,
) -> str | None:
    now = now or datetime.now(timezone.utc)
    try:
        fills = await info_client.user_fills(leader.leader_address)
    except Exception as exc:
        return f"source_fills checkpoint unavailable: {str(exc)[:160]}"
    for fill in fills:
        parsed = _fill_market(fill)
        source_id = fill_unique_id(leader.leader_address, fill)
        stmt = (
            insert(SourceFill)
            .values(
                source_fill_id=source_id,
                leader_address=normalize_leader_address(leader.leader_address),
                coin=parsed["coin"],
                dex=parsed["dex"],
                canonical_coin=parsed["canonical_coin"],
                raw_coin=parsed["raw_coin"],
                asset_id=_int_or_none(fill.get("asset") or fill.get("assetId") or fill.get("a")),
                side=str(fill.get("side") or fill.get("dir") or ""),
                price=Decimal(str(fill.get("px") or fill.get("price") or "0")),
                size=Decimal(str(fill.get("sz") or fill.get("size") or "0")),
                source_time_ms=int(fill.get("time") or fill.get("timeMs") or 0),
                ws_received_at=now,
                raw_fill=fill,
                is_snapshot=True,
                processed_at=None,
            )
            .on_conflict_do_nothing(index_elements=[SourceFill.source_fill_id])
        )
        await db.execute(stmt)
    await db.flush()
    return None


async def baseline_readiness_summary(db: Any, leaders: list[LeaderConfig]) -> dict[str, Any]:
    enabled = [leader for leader in leaders if leader.enabled and leader.deleted_at is None]
    rows = (
        await db.execute(
            select(LeaderPositionBaseline).where(
                LeaderPositionBaseline.leader_id.in_([leader.id for leader in enabled] or [-1])
            )
        )
    ).scalars().all()
    capture_rows = {
        leader.id: await db.get(AppSetting, baseline_capture_setting_key(leader.id))
        for leader in enabled
    }
    waiting = [row for row in rows if str(row.baseline_status).upper() == BASELINE_WAIT_UNTIL_FLAT]
    unknown_leaders = [
        leader
        for leader in enabled
        if capture_rows.get(leader.id) is None
        or not bool(((capture_rows.get(leader.id).value if capture_rows.get(leader.id) else {}) or {}).get("ready"))
    ]
    unknown_rows = [row for row in rows if str(row.baseline_status).upper() == BASELINE_UNKNOWN]
    baseline_unknown_count = len(unknown_leaders) + len(unknown_rows)
    return {
        "baseline_tracking_enabled": True,
        "baseline_ready": baseline_unknown_count == 0,
        "baseline_captured_for_all_enabled_leaders": baseline_unknown_count == 0,
        "ignored_existing_positions_count": len(waiting),
        "waiting_until_flat_count": len(waiting),
        "baseline_unknown_count": baseline_unknown_count,
        "waiting_until_flat_positions": [_baseline_payload(row) for row in waiting],
        "unknown_leaders": [
            {
                "leader_id": leader.id,
                "leader_address": leader.leader_address,
                "reason": "Cannot capture leader baseline positions.",
            }
            for leader in unknown_leaders
        ],
        "rows": [_baseline_payload(row) for row in rows],
    }


def baseline_status_for_position(
    *,
    baseline: LeaderPositionBaseline | None,
    copy_allowed_by_config: bool,
) -> dict[str, Any]:
    if not copy_allowed_by_config:
        return {
            "copy_status": "BLOCKED_BY_CONFIG",
            "copyable": False,
            "baseline_status": None,
            "baseline_id": None,
            "copy_reason": "coin blocked by leader allowlist/blocklist or leader disabled",
        }
    if baseline is None:
        return {
            "copy_status": "WAITING_FOR_NEW_OPEN",
            "copyable": False,
            "baseline_status": None,
            "baseline_id": None,
            "copy_reason": "No copied lifecycle exists; waiting for leader open from flat.",
        }
    status = str(baseline.baseline_status).upper()
    if status == BASELINE_WAIT_UNTIL_FLAT:
        return {
            "copy_status": "IGNORED_EXISTING_POSITION",
            "copyable": False,
            "baseline_status": status,
            "baseline_id": baseline.id,
            "copy_reason": "Existing position from before tracking; waiting until flat.",
        }
    if status == BASELINE_CLEARED:
        return {
            "copy_status": "BASELINE_CLEARED_WAITING_NEXT_OPEN",
            "copyable": False,
            "baseline_status": status,
            "baseline_id": baseline.id,
            "copy_reason": "Baseline cleared; next new open will be copyable.",
        }
    if status in {BASELINE_COPY_ALLOWED, BASELINE_MANUAL_OVERRIDE}:
        return {
            "copy_status": BASELINE_COPY_ALLOWED,
            "copyable": True,
            "baseline_status": status,
            "baseline_id": baseline.id,
            "copy_reason": "COPY_ALLOWED lifecycle.",
        }
    return {
        "copy_status": GATE_BLOCKED_UNKNOWN,
        "copyable": False,
        "baseline_status": status,
        "baseline_id": baseline.id,
        "copy_reason": "Baseline state unknown.",
    }


async def baselines_by_scope_for_leaders(
    db: Any,
    leader_ids: list[int],
) -> dict[tuple[int, str, str, str], LeaderPositionBaseline]:
    rows = (
        await db.execute(
            select(LeaderPositionBaseline).where(
                LeaderPositionBaseline.leader_id.in_(leader_ids or [-1])
            )
        )
    ).scalars().all()
    return {
        (
            int(row.leader_id or 0),
            str(row.execution_venue or "").upper(),
            str(row.dex or "").lower(),
            str(row.canonical_coin or "").upper(),
        ): row
        for row in rows
    }


def baseline_scope_key(*, leader_id: int, execution_venue: str, dex: str, canonical_coin: str) -> tuple[int, str, str, str]:
    return (
        int(leader_id),
        str(execution_venue or "").upper(),
        str(dex or "").lower(),
        str(canonical_coin or "").upper(),
    )


async def _load_baseline(
    db: Any,
    *,
    leader_id: int,
    execution_venue: str,
    dex: str,
    canonical_coin: str,
) -> LeaderPositionBaseline | None:
    return await db.scalar(
        select(LeaderPositionBaseline)
        .where(LeaderPositionBaseline.leader_id == leader_id)
        .where(LeaderPositionBaseline.execution_venue == str(execution_venue).upper())
        .where(LeaderPositionBaseline.dex == str(dex or "").lower())
        .where(func.upper(LeaderPositionBaseline.canonical_coin) == str(canonical_coin).upper())
        .limit(1)
    )


async def _clear_waiting_baselines_not_seen(
    db: Any,
    *,
    leader: LeaderConfig,
    dex: str,
    seen_canonical_coins: set[str],
    now: datetime,
) -> None:
    rows = (
        await db.execute(
            select(LeaderPositionBaseline)
            .where(LeaderPositionBaseline.leader_id == leader.id)
            .where(LeaderPositionBaseline.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(LeaderPositionBaseline.dex == str(dex or "").lower())
            .where(LeaderPositionBaseline.baseline_status == BASELINE_WAIT_UNTIL_FLAT)
        )
    ).scalars().all()
    seen = {coin.upper() for coin in seen_canonical_coins}
    for row in rows:
        if str(row.canonical_coin or "").upper() in seen:
            continue
        row.baseline_status = BASELINE_CLEARED
        row.flat_confirmed_at = row.flat_confirmed_at or now
        row.last_leader_size = Decimal("0")
        row.last_leader_notional = Decimal("0")
        row.last_checked_at = now
        row.reason = "Baseline cleared during capture; next new open will be copyable."


async def _store_capture_status(
    db: Any,
    *,
    leader: LeaderConfig,
    ready: bool,
    captured_count: int,
    waiting_until_flat_count: int,
    baseline_unknown_count: int,
    reason: str,
    error: str | None,
    now: datetime,
) -> None:
    payload = {
        "ready": ready,
        "leader_id": leader.id,
        "leader_address": leader.leader_address,
        "captured_count": captured_count,
        "ignored_existing_positions_count": waiting_until_flat_count,
        "waiting_until_flat_count": waiting_until_flat_count,
        "baseline_unknown_count": baseline_unknown_count,
        "reason": reason,
        "error": error,
        "captured_at": now.isoformat(),
    }
    stmt = (
        insert(AppSetting)
        .values(key=baseline_capture_setting_key(leader.id), value=payload, updated_at=now)
        .on_conflict_do_update(
            index_elements=[AppSetting.key],
            set_={"value": payload, "updated_at": now},
        )
    )
    await db.execute(stmt)
    if not ready:
        db.add(
            RiskEvent(
                severity="warning",
                event_type="LEADER_BASELINE_CAPTURE_FAILED",
                symbol=None,
                leader_address=leader.leader_address,
                message="Cannot capture leader baseline positions.",
                metadata_json=payload,
            )
        )


def _baseline_payload(row: LeaderPositionBaseline) -> dict[str, Any]:
    return {
        "id": row.id,
        "leader_id": row.leader_id,
        "leader_address": row.leader_address,
        "execution_venue": row.execution_venue,
        "dex": row.dex,
        "canonical_coin": row.canonical_coin,
        "side_at_enable": row.side_at_enable,
        "size_at_enable": str(row.size_at_enable) if row.size_at_enable is not None else None,
        "notional_at_enable": str(row.notional_at_enable) if row.notional_at_enable is not None else None,
        "entry_px_at_enable": str(getattr(row, "entry_px_at_enable", None))
        if getattr(row, "entry_px_at_enable", None) is not None
        else None,
        "mark_px_at_enable": str(getattr(row, "mark_px_at_enable", None))
        if getattr(row, "mark_px_at_enable", None) is not None
        else None,
        "account_value_at_enable": str(row.account_value_at_enable) if row.account_value_at_enable is not None else None,
        "baseline_status": row.baseline_status,
        "copy_status": "IGNORED_EXISTING_POSITION"
        if row.baseline_status == BASELINE_WAIT_UNTIL_FLAT
        else "BASELINE_CLEARED_WAITING_NEXT_OPEN"
        if row.baseline_status == BASELINE_CLEARED
        else row.baseline_status,
        "first_seen_at": row.first_seen_at.isoformat() if row.first_seen_at else None,
        "flat_confirmed_at": row.flat_confirmed_at.isoformat() if row.flat_confirmed_at else None,
        "copy_allowed_at": row.copy_allowed_at.isoformat() if row.copy_allowed_at else None,
        "last_leader_size": str(row.last_leader_size) if row.last_leader_size is not None else None,
        "last_leader_notional": str(row.last_leader_notional) if row.last_leader_notional is not None else None,
        "last_leader_entry_px": str(getattr(row, "last_leader_entry_px", None))
        if getattr(row, "last_leader_entry_px", None) is not None
        else None,
        "last_leader_mark_px": str(getattr(row, "last_leader_mark_px", None))
        if getattr(row, "last_leader_mark_px", None) is not None
        else None,
        "last_checked_at": row.last_checked_at.isoformat() if row.last_checked_at else None,
        "reason": row.reason,
    }


def _position_is_open(position: Any) -> bool:
    return abs(Decimal(getattr(position, "size", 0) or 0)) > BASELINE_TOLERANCE and str(
        getattr(position, "side", "")
    ).upper() in {"LONG", "SHORT"}


def _side_value(value: PositionSide | str | None) -> str:
    if isinstance(value, PositionSide):
        return value.value
    raw = str(value or "").upper()
    if raw in {"LONG", "SHORT"}:
        return raw
    return PositionSide.FLAT.value


def _fill_market(fill: dict[str, Any]) -> dict[str, str]:
    raw_coin = str(fill.get("coin") or fill.get("name") or fill.get("symbol") or "")
    dex = str(fill.get("dex") or fill.get("perpDex") or fill.get("dexName") or "")
    parsed = parse_coin(raw_coin, default_dex=dex)
    return {
        "coin": parsed.coin,
        "dex": parsed.dex,
        "canonical_coin": parsed.canonical_coin or canonical_coin(dex=parsed.dex, coin=parsed.coin),
        "raw_coin": raw_coin,
    }


def _int_or_none(value: Any) -> int | None:
    if value is None or str(value) == "":
        return None
    try:
        return int(value)
    except Exception:
        return None
