from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select

from app.models import LatestAccountPosition, LatestAccountState, RiskEvent
from app.services.hyperliquid_dex import canonical_coin, dex_display_name, parse_coin

FOLLOWER = "FOLLOWER"
LEADER = "LEADER"
INFO_ENDPOINT = "info_endpoint"
MISSING_CONFIRMATION_STATUS = "MISSING"
MAX_ACCOUNT_STATE_SOURCE_LENGTH = 32


@dataclass(frozen=True)
class AccountPositionState:
    coin: str
    dex: str
    canonical_coin: str
    raw_coin: str
    product_type: str | None
    side: str
    size: Decimal
    notional: Decimal
    entry_px: Decimal | None
    mark_px: Decimal | None
    mid_px: Decimal | None
    mark_px_source: str | None
    position_opened_at: datetime | None
    open_time_source: str | None
    unrealized_pnl: Decimal | None
    leverage: Decimal | None
    margin_used: Decimal | None
    liquidation_px: Decimal | None
    raw_payload_masked: dict[str, Any]


@dataclass(frozen=True)
class AccountState:
    role: str
    address: str
    dex: str
    dex_display_name: str
    account_label: str | None
    account_value: Decimal | None
    withdrawable: Decimal | None
    total_ntl_pos: Decimal | None
    total_raw_usd: Decimal | None
    total_margin_used: Decimal | None
    positions: list[AccountPositionState]
    raw_payload_masked: dict[str, Any]
    source: str
    updated_at: datetime
    error_message: str | None = None


class AccountStateService:
    def __init__(self, info_client: Any) -> None:
        self._info_client = info_client

    async def fetch_state(
        self,
        *,
        role: str,
        address: str,
        dex: str = "",
        account_label: str | None = None,
        source: str = INFO_ENDPOINT,
        price_mids: dict[str, Any] | None = None,
    ) -> AccountState:
        raw = await self._info_client.clearinghouse_state(address, dex=dex)
        return parse_account_state(
            role=role,
            address=address,
            dex=dex,
            clearinghouse_state=raw,
            account_label=account_label,
            source=source,
            price_mids=price_mids,
        )


def parse_account_state(
    *,
    role: str,
    address: str,
    clearinghouse_state: dict[str, Any],
    dex: str = "",
    account_label: str | None = None,
    source: str = INFO_ENDPOINT,
    updated_at: datetime | None = None,
    error_message: str | None = None,
    price_mids: dict[str, Any] | None = None,
) -> AccountState:
    updated_at = updated_at or datetime.now(timezone.utc)
    margin_summary = clearinghouse_state.get("marginSummary") or {}
    positions = [
        _parse_position(item, dex=dex, price_mids=price_mids)
        for item in clearinghouse_state.get("assetPositions") or []
    ]
    return AccountState(
        role=role.upper(),
        address=str(address).lower(),
        dex=str(dex or "").strip().lower(),
        dex_display_name=dex_display_name(dex),
        account_label=account_label,
        account_value=_decimal_or_none(margin_summary.get("accountValue")),
        withdrawable=_decimal_or_none(clearinghouse_state.get("withdrawable")),
        total_ntl_pos=_decimal_or_none(margin_summary.get("totalNtlPos")),
        total_raw_usd=_decimal_or_none(margin_summary.get("totalRawUsd")),
        total_margin_used=_decimal_or_none(margin_summary.get("totalMarginUsed")),
        positions=positions,
        raw_payload_masked=mask_payload(clearinghouse_state),
        source=source,
        updated_at=updated_at,
        error_message=error_message,
    )


def error_account_state(
    *,
    role: str,
    address: str,
    dex: str = "",
    account_label: str | None = None,
    error_message: str = "",
    source: str = INFO_ENDPOINT,
    updated_at: datetime | None = None,
) -> AccountState:
    return AccountState(
        role=role.upper(),
        address=str(address).lower(),
        dex=str(dex or "").strip().lower(),
        dex_display_name=dex_display_name(dex),
        account_label=account_label,
        account_value=None,
        withdrawable=None,
        total_ntl_pos=None,
        total_raw_usd=None,
        total_margin_used=None,
        positions=[],
        raw_payload_masked={},
        source=source,
        updated_at=updated_at or datetime.now(timezone.utc),
        error_message=error_message[:500],
    )


async def save_account_state(db: Any, state: AccountState) -> LatestAccountState:
    row = (
        await db.execute(
            select(LatestAccountState)
            .where(LatestAccountState.role == state.role)
            .where(LatestAccountState.address == state.address)
            .where(LatestAccountState.dex == state.dex)
        )
    ).scalar_one_or_none()
    if row is None:
        row = LatestAccountState(role=state.role, address=state.address, dex=state.dex)
        db.add(row)
        await db.flush()

    row.dex = state.dex
    row.dex_display_name = state.dex_display_name
    row.account_label = state.account_label
    row.account_value = state.account_value
    row.withdrawable = state.withdrawable
    row.total_ntl_pos = state.total_ntl_pos
    row.total_raw_usd = state.total_raw_usd
    row.total_margin_used = state.total_margin_used
    row.raw_payload_masked = state.raw_payload_masked
    row.source = _account_state_source(state.source)
    row.last_update_at = state.updated_at
    row.error_message = state.error_message
    await db.flush()

    if state.error_message:
        return row

    existing_rows = (
        await db.execute(
            select(LatestAccountPosition).where(LatestAccountPosition.account_state_id == row.id)
        )
    ).scalars().all()
    active_by_coin: dict[str, LatestAccountPosition] = {}
    active_rows = sorted(
        (item for item in existing_rows if _position_row_active(item)),
        key=_position_row_preference_key,
        reverse=True,
    )
    for item in active_rows:
        key = _position_row_coin_key(item)
        if not key:
            continue
        if key not in active_by_coin:
            active_by_coin[key] = item
            continue
        _close_position_row(item, now=state.updated_at)
        _record_duplicate_active_position_event(db, state=state, position=item)
    seen: set[str] = set()
    for position in state.positions:
        if not _position_is_open(position):
            continue
        key = position.canonical_coin.upper()
        seen.add(key)
        position_opened_at = position.position_opened_at
        open_time_source = position.open_time_source
        target = active_by_coin.get(key)
        if target is not None and str(target.side or "").upper() != position.side:
            _close_position_row(target, now=state.updated_at)
            target = None
        if target is None:
            target = LatestAccountPosition(
                account_state_id=row.id,
                role=state.role,
                address=state.address,
                dex=position.dex,
                coin=position.coin,
                canonical_coin=position.canonical_coin,
                raw_coin=position.raw_coin,
                first_seen_at=state.updated_at,
            )
            db.add(target)
        _update_position_row(
            target,
            state=state,
            position=position,
            position_opened_at=position_opened_at,
            open_time_source=open_time_source,
        )
    for key, existing in active_by_coin.items():
        if key not in seen:
            closed = _mark_or_close_missing_position(existing, now=state.updated_at)
            _record_missing_position_event(db, state=state, position=existing, closed=closed)
    await db.flush()
    return row


async def load_account_state_with_positions(
    db: Any,
    *,
    role: str,
    address: str,
    dex: str | None = None,
    include_closed: bool = False,
) -> tuple[LatestAccountState | None, list[LatestAccountPosition]]:
    stmt = (
        select(LatestAccountState)
        .where(LatestAccountState.role == role.upper())
        .where(LatestAccountState.address == address.lower())
    )
    if dex is not None:
        stmt = stmt.where(LatestAccountState.dex == str(dex or "").strip().lower())
    else:
        stmt = stmt.order_by(LatestAccountState.dex)
    state = (await db.execute(stmt.limit(1))).scalar_one_or_none()
    if state is None:
        return None, []
    positions = (
        await db.execute(
            select(LatestAccountPosition)
            .where(LatestAccountPosition.account_state_id == state.id)
            .where(True if include_closed else LatestAccountPosition.active.is_(True))
            .order_by(LatestAccountPosition.coin)
        )
    ).scalars().all()
    return state, positions


def account_state_payload(
    state: LatestAccountState | None,
    positions: list[LatestAccountPosition],
    *,
    stale_seconds: int = 10,
    now: datetime | None = None,
    extra: dict[str, Any] | None = None,
    include_closed: bool = False,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    age_ms = _age_ms(state.last_update_at, now) if state else None
    stale = age_ms is None or age_ms > stale_seconds * 1000
    account_value = _decimal_str(state.account_value) if state else None
    withdrawable = _decimal_str(state.withdrawable) if state else None
    total_ntl_pos = _decimal_str(state.total_ntl_pos) if state else None
    total_raw_usd = _decimal_str(state.total_raw_usd) if state else None
    total_margin_used = _decimal_str(state.total_margin_used) if state else None
    updated_at = state.last_update_at.isoformat() if state and state.last_update_at else None
    source = state.source if state else None
    error_message = state.error_message if state else "account state unavailable"
    state_dex = getattr(state, "dex", "") if state else None
    state_dex_display = getattr(state, "dex_display_name", None) if state else None
    payload = {
        "role": state.role if state else None,
        "address": state.address if state else None,
        "dex": state_dex,
        "dex_display_name": state_dex_display or dex_display_name(state_dex),
        "dexDisplayName": state_dex_display or dex_display_name(state_dex),
        "account_label": state.account_label if state else None,
        "account_value": account_value,
        "accountValue": account_value,
        "withdrawable": withdrawable,
        "total_ntl_pos": total_ntl_pos,
        "totalNtlPos": total_ntl_pos,
        "total_raw_usd": total_raw_usd,
        "totalRawUsd": total_raw_usd,
        "total_margin_used": total_margin_used,
        "totalMarginUsed": total_margin_used,
        "positions": [
            position_payload(position, account_state=state, stale=stale, data_age_ms=age_ms)
            for position in positions
            if include_closed or _position_row_active(position)
        ],
        "updated_at": updated_at,
        "updatedAt": updated_at,
        "data_age_ms": age_ms,
        "dataAgeMs": age_ms,
        "source": source,
        "stale": stale,
        "error_message": error_message,
        "errorMessage": error_message,
    }
    if extra:
        payload.update(extra)
    return payload


def position_payload(
    position: LatestAccountPosition,
    *,
    account_state: LatestAccountState | None = None,
    stale: bool | None = None,
    data_age_ms: int | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    position_updated_at = _datetime_from_any(getattr(position, "last_update_at", None))
    account_updated_at = (
        _datetime_from_any(getattr(account_state, "last_update_at", None))
        if account_state is not None
        else None
    )
    used_position_age = False
    if data_age_ms is None:
        freshest_at = _latest_datetime(account_updated_at, position_updated_at)
        data_age_ms = _age_ms(freshest_at, now)
        used_position_age = freshest_at is not None and freshest_at == position_updated_at
    elif position_updated_at is not None and (
        account_updated_at is None or position_updated_at > account_updated_at
    ):
        data_age_ms = _age_ms(position_updated_at, now)
        used_position_age = True
    if stale is None or used_position_age:
        stale = data_age_ms is not None and data_age_ms > 10_000
    size = _decimal_str(position.size)
    notional = _decimal_str(position.notional)
    entry_px = _decimal_str(position.entry_px)
    mark_px = _decimal_str(position.mark_px)
    mid_px = _decimal_str(getattr(position, "mid_px", None) or position.mark_px)
    unrealized_pnl = _decimal_str(position.unrealized_pnl)
    leverage = _decimal_str(position.leverage)
    margin_used = _decimal_str(position.margin_used)
    liquidation_px = _decimal_str(position.liquidation_px)
    margin_mode = _margin_mode(position.raw_payload_masked)
    position_dex = getattr(position, "dex", "")
    position_coin = getattr(position, "coin", "")
    position_canonical = getattr(position, "canonical_coin", None) or canonical_coin(dex=position_dex, coin=position_coin)
    first_seen_at = getattr(position, "first_seen_at", None)
    position_opened_at = getattr(position, "position_opened_at", None)
    open_time_source = getattr(position, "open_time_source", None) or ("POSITION_PAYLOAD" if position_opened_at else "FIRST_SEEN")
    open_time = position_opened_at or first_seen_at
    updated_at = getattr(position, "last_update_at", None)
    active = _position_row_active(position)
    role = getattr(position, "role", None) or (getattr(account_state, "role", None) if account_state else None)
    address = getattr(position, "address", None) or (getattr(account_state, "address", None) if account_state else None)
    return {
        "role": role,
        "account_address": address,
        "accountAddress": address,
        "execution_venue": "HYPERLIQUID",
        "executionVenue": "HYPERLIQUID",
        "dex": position_dex,
        "dex_display_name": dex_display_name(position_dex),
        "dexDisplayName": dex_display_name(position_dex),
        "coin": position_coin,
        "display_symbol": position_canonical,
        "displaySymbol": position_canonical,
        "canonical_coin": position_canonical,
        "canonicalCoin": position_canonical,
        "raw_coin": getattr(position, "raw_coin", None),
        "rawCoin": getattr(position, "raw_coin", None),
        "product_type": getattr(position, "product_type", None) or "unknown",
        "productType": getattr(position, "product_type", None) or "unknown",
        "side": position.side,
        "size": size,
        "notional": notional,
        "entry_px": entry_px,
        "entryPx": entry_px,
        "mark_px": mark_px,
        "markPx": mark_px,
        "mid_px": mid_px,
        "midPx": mid_px,
        "mark_px_source": getattr(position, "mark_px_source", None) or "ACCOUNT_STATE",
        "markPxSource": getattr(position, "mark_px_source", None) or "ACCOUNT_STATE",
        "mark_price_stale": bool(stale),
        "markPriceStale": bool(stale),
        "open_time": open_time.isoformat() if open_time else None,
        "openTime": open_time.isoformat() if open_time else None,
        "position_opened_at": position_opened_at.isoformat() if position_opened_at else None,
        "positionOpenedAt": position_opened_at.isoformat() if position_opened_at else None,
        "first_seen_at": first_seen_at.isoformat() if first_seen_at else None,
        "firstSeenAt": first_seen_at.isoformat() if first_seen_at else None,
        "open_time_source": open_time_source,
        "openTimeSource": open_time_source,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "updatedAt": updated_at.isoformat() if updated_at else None,
        "data_age_ms": data_age_ms,
        "dataAgeMs": data_age_ms,
        "stale": bool(stale),
        "active": active,
        "status": getattr(position, "status", None) or ("OPEN" if active else "CLOSED"),
        "closed_at": getattr(position, "closed_at", None).isoformat() if getattr(position, "closed_at", None) else None,
        "closedAt": getattr(position, "closed_at", None).isoformat() if getattr(position, "closed_at", None) else None,
        "unrealized_pnl": unrealized_pnl,
        "unrealizedPnl": unrealized_pnl,
        "leverage": leverage,
        "margin_used": margin_used,
        "marginUsed": margin_used,
        "margin_mode": margin_mode,
        "marginMode": margin_mode,
        "liquidation_px": liquidation_px,
        "liquidationPx": liquidation_px,
        "raw_position_payload": position.raw_payload_masked,
        "rawPositionPayload": position.raw_payload_masked,
    }


def state_is_fresh(state: LatestAccountState | None, *, stale_seconds: int, now: datetime | None = None) -> bool:
    if state is None or state.error_message:
        return False
    now = now or datetime.now(timezone.utc)
    age_ms = _age_ms(state.last_update_at, now)
    return age_ms is not None and age_ms <= stale_seconds * 1000


def _parse_position(
    item: dict[str, Any],
    *,
    dex: str = "",
    price_mids: dict[str, Any] | None = None,
) -> AccountPositionState:
    position = item.get("position") or item
    parsed = parse_coin(str(position.get("coin", "")), default_dex=dex)
    coin = parsed.coin
    size = _decimal(position.get("szi", position.get("size")))
    abs_notional = abs(_decimal(position.get("positionValue", position.get("notional"))))
    notional = abs_notional if size >= 0 else -abs_notional
    side = "FLAT"
    if size > 0:
        side = "LONG"
    elif size < 0:
        side = "SHORT"
    asset_ctx = item.get("assetCtx") or item.get("asset_ctx") or item.get("ctx") or {}
    mark_px = _decimal_or_none(_first_value(position, asset_ctx, keys=("markPx", "mark_px")))
    mid_px = _decimal_or_none(_first_value(position, asset_ctx, keys=("midPx", "mid_px")))
    mark_px_source = "POSITION_MARK_PX" if mark_px is not None else None
    if mark_px is None and mid_px is not None:
        mark_px = mid_px
        mark_px_source = "POSITION_MID_PX"
    if mark_px is None:
        price_mid = _price_mid_from_cache(price_mids, parsed=parsed)
        if price_mid is not None:
            mark_px = price_mid
            mid_px = price_mid
            mark_px_source = "PRICE_CACHE_MID"
    if mark_px is None and size != 0 and abs_notional != 0:
        mark_px = (abs_notional / abs(size)).quantize(Decimal("0.00000001"))
        mark_px_source = "POSITION_VALUE_DIV_SIZE"
    if mid_px is None:
        mid_px = mark_px
    leverage_value = position.get("leverage")
    if isinstance(leverage_value, dict):
        leverage_value = leverage_value.get("value")
    position_opened_at = _position_opened_at(position) or _position_opened_at(item)
    return AccountPositionState(
        coin=coin,
        dex=parsed.dex,
        canonical_coin=parsed.canonical_coin,
        raw_coin=parsed.raw_coin,
        product_type=_product_type(position),
        side=side,
        size=size,
        notional=notional,
        entry_px=_decimal_or_none(position.get("entryPx")),
        mark_px=mark_px,
        mid_px=mid_px,
        mark_px_source=mark_px_source,
        position_opened_at=position_opened_at,
        open_time_source="POSITION_PAYLOAD" if position_opened_at else "FIRST_SEEN",
        unrealized_pnl=_decimal_or_none(position.get("unrealizedPnl", position.get("unrealizedPnlRaw"))),
        leverage=_decimal_or_none(leverage_value),
        margin_used=_decimal_or_none(position.get("marginUsed")),
        liquidation_px=_decimal_or_none(position.get("liquidationPx")),
        raw_payload_masked=mask_payload(position),
    )


def mask_payload(value: Any) -> Any:
    if isinstance(value, dict):
        masked: dict[str, Any] = {}
        for key, item in value.items():
            key_l = str(key).lower()
            if any(secret in key_l for secret in ("private", "secret", "signature", "api_key", "api-key")):
                masked[key] = "***"
            else:
                masked[key] = mask_payload(item)
        return masked
    if isinstance(value, list):
        return [mask_payload(item) for item in value]
    return value


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _position_is_open(position: AccountPositionState) -> bool:
    return position.side in {"LONG", "SHORT"} and position.size is not None and position.size != 0


def _position_row_active(position: Any) -> bool:
    active = getattr(position, "active", None)
    if active is not None:
        return bool(active)
    status = str(getattr(position, "status", "OPEN") or "OPEN").upper()
    if status == "CLOSED":
        return False
    size = getattr(position, "size", None)
    try:
        if size is not None and Decimal(str(size)) == 0:
            return False
    except Exception:
        pass
    return True


def _position_row_coin_key(position: Any) -> str:
    return str(getattr(position, "canonical_coin", None) or getattr(position, "coin", "") or "").upper()


def _position_row_preference_key(position: Any) -> tuple[int, datetime, int]:
    source = str(getattr(position, "mark_px_source", "") or "").upper()
    source_rank = 0 if source in {"LOCAL_FILL_PROJECTION", "ORDER_RECOVERY_PROJECTION"} else 1
    updated_at = getattr(position, "last_update_at", None)
    if not isinstance(updated_at, datetime):
        updated_at = datetime.min.replace(tzinfo=timezone.utc)
    elif updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    try:
        row_id = int(getattr(position, "id", 0) or 0)
    except Exception:
        row_id = 0
    return source_rank, updated_at, row_id


def _update_position_row(
    row: LatestAccountPosition,
    *,
    state: AccountState,
    position: AccountPositionState,
    position_opened_at: datetime | None,
    open_time_source: str | None,
) -> None:
    row.role = state.role
    row.address = state.address
    row.dex = position.dex
    row.coin = position.coin
    row.canonical_coin = position.canonical_coin
    row.raw_coin = position.raw_coin
    row.product_type = position.product_type
    row.side = position.side
    row.size = position.size
    row.notional = position.notional
    row.entry_px = position.entry_px
    row.mark_px = position.mark_px
    row.mid_px = position.mid_px
    row.mark_px_source = position.mark_px_source
    row.unrealized_pnl = position.unrealized_pnl
    row.leverage = position.leverage
    row.margin_used = position.margin_used
    row.liquidation_px = position.liquidation_px
    row.active = True
    row.status = "OPEN"
    row.closed_at = None
    if position_opened_at is not None:
        row.position_opened_at = position_opened_at
        row.open_time_source = open_time_source or "POSITION_PAYLOAD"
    else:
        row.open_time_source = row.open_time_source or open_time_source or "FIRST_SEEN"
    row.first_seen_at = row.first_seen_at or position_opened_at or state.updated_at
    row.raw_payload_masked = position.raw_payload_masked
    row.last_update_at = state.updated_at


def _close_position_row(row: LatestAccountPosition, *, now: datetime) -> None:
    row.active = False
    row.status = "CLOSED"
    row.closed_at = row.closed_at or now
    row.last_update_at = now


def _mark_or_close_missing_position(row: LatestAccountPosition, *, now: datetime) -> bool:
    if str(getattr(row, "status", "") or "").upper() == MISSING_CONFIRMATION_STATUS:
        _close_position_row(row, now=now)
        return True
    row.active = True
    row.status = MISSING_CONFIRMATION_STATUS
    row.closed_at = None
    row.last_update_at = now
    return False


def _record_missing_position_event(
    db: Any,
    *,
    state: AccountState,
    position: LatestAccountPosition,
    closed: bool,
) -> None:
    canonical = str(getattr(position, "canonical_coin", None) or getattr(position, "coin", "") or "").upper()
    db.add(
        RiskEvent(
            severity="warning" if not closed else "error",
            event_type=(
                "ACCOUNT_POSITION_CLOSED_AFTER_MISSING_CONFIRMATION"
                if closed
                else "ACCOUNT_POSITION_MISSING_FROM_SNAPSHOT"
            ),
            symbol=canonical or None,
            leader_address=None,
            message=(
                "latest account position closed after consecutive missing snapshots"
                if closed
                else "latest account position missing from account snapshot; keeping previous position pending confirmation"
            ),
            metadata_json={
                "role": state.role,
                "address": state.address,
                "dex": state.dex,
                "canonical_coin": canonical,
                "side": getattr(position, "side", None),
                "size": str(getattr(position, "size", "")),
                "notional": str(getattr(position, "notional", "")),
                "snapshot_positions_count": len(state.positions),
                "snapshot_account_value": str(state.account_value) if state.account_value is not None else None,
                "snapshot_total_ntl_pos": str(state.total_ntl_pos) if state.total_ntl_pos is not None else None,
                "snapshot_source": state.source,
                "snapshot_at": state.updated_at.isoformat(),
                "closed": closed,
            },
        )
    )


def _record_duplicate_active_position_event(
    db: Any,
    *,
    state: AccountState,
    position: LatestAccountPosition,
) -> None:
    canonical = _position_row_coin_key(position)
    db.add(
        RiskEvent(
            severity="warning",
            event_type="ACCOUNT_POSITION_DUPLICATE_ACTIVE_ROW_CLOSED",
            symbol=canonical or None,
            leader_address=None,
            message="closed duplicate active account position row while saving account state",
            metadata_json={
                "role": state.role,
                "address": state.address,
                "dex": state.dex,
                "position_id": getattr(position, "id", None),
                "canonical_coin": canonical,
                "side": getattr(position, "side", None),
                "mark_px_source": getattr(position, "mark_px_source", None),
            },
        )
    )


def _position_opened_at(position: dict[str, Any]) -> datetime | None:
    for key in (
        "openedAt",
        "opened_at",
        "openTime",
        "open_time",
        "positionOpenedAt",
        "position_opened_at",
        "createdAt",
        "created_at",
        "timestamp",
        "time",
    ):
        parsed = _datetime_from_any(position.get(key))
        if parsed is not None:
            return parsed
    return None


def _datetime_from_any(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float, Decimal)) or str(value).isdigit():
        number = Decimal(str(value))
        if number > Decimal("100000000000"):
            number = number / Decimal("1000")
        try:
            return datetime.fromtimestamp(float(number), timezone.utc)
        except Exception:
            return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _account_state_source(value: str | None) -> str:
    source = str(value or INFO_ENDPOINT)
    return source[:MAX_ACCOUNT_STATE_SOURCE_LENGTH]


def _latest_datetime(*values: datetime | None) -> datetime | None:
    latest: datetime | None = None
    for value in values:
        parsed = _datetime_from_any(value)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest


def _first_value(*payloads: Any, keys: tuple[str, ...]) -> Any:
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key in keys:
            value = payload.get(key)
            if value is not None and value != "":
                return value
    return None


def _price_mid_from_cache(price_mids: dict[str, Any] | None, *, parsed: Any) -> Decimal | None:
    if not price_mids:
        return None
    for key in (
        parsed.canonical_coin,
        parsed.raw_coin,
        parsed.coin,
        f"{parsed.dex}:{parsed.coin}" if parsed.dex else parsed.coin,
    ):
        value = price_mids.get(key)
        try:
            parsed_value = _decimal_or_none(value)
        except Exception:
            continue
        if parsed_value is not None and parsed_value > 0:
            return parsed_value
    return None


def _decimal(value: Any, default: str = "0") -> Decimal:
    parsed = _decimal_or_none(value)
    return parsed if parsed is not None else Decimal(default)


def _decimal_str(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _margin_mode(raw_payload: Any) -> str | None:
    if not isinstance(raw_payload, dict):
        return None
    leverage = raw_payload.get("leverage")
    if isinstance(leverage, dict):
        mode = leverage.get("type")
        return str(mode).upper() if mode else None
    return None


def _product_type(position: dict[str, Any]) -> str | None:
    for key in ("productType", "product_type", "type", "kind"):
        value = position.get(key)
        if value:
            return str(value)
    return "unknown"


def _age_ms(value: datetime | None, now: datetime) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return max(0, int((now - value).total_seconds() * 1000))
