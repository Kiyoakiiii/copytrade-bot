from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    LatestAccountPosition,
    LatestAccountState,
    LeaderPositionAllocationRecord,
    LeaderPositionBaseline,
    MarketRiskSetting,
    SignerNonceState,
)
from app.services.baseline import BASELINE_WAIT_UNTIL_FLAT
from app.services.execution_router import ExecutionVenue
from app.services.hyperliquid_dex import canonical_coin, dex_display_name, mask_address, parse_coin
from app.services.hyperliquid_execution import (
    FORCED_ISOLATED_LEVERAGE_MARKETS,
    ISOLATED_TARGET_LEVERAGE,
    build_hyperliquid_leverage_plan,
    isolated_target_leverage,
    market_leverage_override,
    resolve_asset_id_from_meta,
)
from app.services.leader_config import active_leaders_statement, is_coin_allowed, normalize_leader_address

DESIRED_MARGIN_MODE = "CROSS"
FALLBACK_MARGIN_MODE = "ISOLATED"
ISOLATED_MARGIN_REBALANCE_BUFFER_RATIO = Decimal("0.005")
ISOLATED_MARGIN_REBALANCE_MIN_BUFFER = Decimal("1")
ISOLATED_MARGIN_REBALANCE_MIN_RELEASE = Decimal("1")
STATUS_UNKNOWN = "UNKNOWN"
STATUS_SETTING = "SETTING"
STATUS_CONFIRMED = "CONFIRMED"
STATUS_FAILED = "FAILED"
STATUS_NEEDS_REFRESH = "NEEDS_REFRESH"

REASON_MAX_LEVERAGE_UNKNOWN = "MAX_LEVERAGE_UNKNOWN"
REASON_RISK_SETTING_UPDATE_FAILED = "RISK_SETTING_UPDATE_FAILED"
REASON_MARGIN_MODE_NOT_CONFIRMED = "MARGIN_MODE_NOT_CONFIRMED"
REASON_ISOLATED_NOT_CONFIRMED = REASON_MARGIN_MODE_NOT_CONFIRMED
REASON_LEVERAGE_NOT_CONFIRMED = "LEVERAGE_NOT_CONFIRMED"
REASON_CONFIRMATION_UNKNOWN = "RISK_SETTING_CONFIRMATION_UNKNOWN"
REASON_CROSS_MARGIN_NOT_SUPPORTED = "CROSS_MARGIN_NOT_SUPPORTED"

OPEN_ACTIONS = {"OPEN", "INCREASE", "OPEN_OR_INCREASE", "FLIP_OPEN_SECOND", "PREPARE_OPEN"}
REDUCE_ACTIONS = {"REDUCE", "CLOSE", "CLOSE_OR_REDUCE", "FLIP_CLOSE_FIRST"}


@dataclass(frozen=True)
class RiskSettingResult:
    is_ok: bool
    status: str
    account_address: str
    dex: str
    canonical_coin: str
    desired_margin_mode: str = DESIRED_MARGIN_MODE
    desired_leverage: int = 10
    market_max_leverage: int | None = None
    effective_leverage: int | None = None
    actual_margin_mode: str | None = None
    actual_leverage: int | None = None
    asset_id: int | None = None
    reason_code: str | None = None
    reason: str | None = None
    warning: str | None = None
    cache_used: bool = False
    row_id: int | None = None
    last_confirmed_at: datetime | None = None

    @property
    def blocks_open(self) -> bool:
        return not self.is_ok

    def payload(self) -> dict[str, Any]:
        return {
            "ok": self.is_ok,
            "status": self.status,
            "account_address": self.account_address,
            "dex": self.dex,
            "dex_display_name": dex_display_name(self.dex),
            "canonical_coin": self.canonical_coin,
            "asset_id": self.asset_id,
            "desired_margin_mode": self.desired_margin_mode,
            "desired_leverage": self.desired_leverage,
            "market_max_leverage": self.market_max_leverage,
            "effective_leverage": self.effective_leverage,
            "actual_margin_mode": self.actual_margin_mode,
            "actual_leverage": self.actual_leverage,
            "reason_code": self.reason_code,
            "reason": self.reason,
            "warning": self.warning,
            "cache_used": self.cache_used,
            "row_id": self.row_id,
            "last_confirmed_at": self.last_confirmed_at.isoformat() if self.last_confirmed_at else None,
        }


def action_increases_risk(action_type: str | None, *, reduce_only: bool = False) -> bool:
    if reduce_only:
        return False
    action = str(action_type or "OPEN").upper()
    if action in REDUCE_ACTIONS:
        return False
    return True


def effective_leverage_for_market(
    market_max_leverage: int | str | None,
    *,
    desired_default_leverage: int = 10,
) -> int | None:
    try:
        max_leverage = int(market_max_leverage)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if max_leverage <= 0:
        return None
    return min(int(desired_default_leverage), max_leverage)


def desired_leverage_for_margin_mode(
    desired_default_leverage: int,
    margin_mode: str | None,
    *,
    canonical_coin_value: str | None = None,
) -> int:
    desired = int(desired_default_leverage or 10)
    explicit_override = market_leverage_override(canonical_coin_value)
    if explicit_override is not None:
        return min(desired, explicit_override)
    if isolated_leverage_required(
        margin_mode=margin_mode,
        canonical_coin_value=canonical_coin_value,
    ):
        return min(desired, isolated_target_leverage(canonical_coin_value))
    return desired


def isolated_leverage_required(
    *,
    margin_mode: str | None,
    canonical_coin_value: str | None,
) -> bool:
    return (
        _normalize_margin_mode(margin_mode) == FALLBACK_MARGIN_MODE
        or str(canonical_coin_value or "").upper()
        in FORCED_ISOLATED_LEVERAGE_MARKETS
    )


def effective_leverage_for_margin_mode(
    market_max_leverage: int | str | None,
    *,
    desired_default_leverage: int = 10,
    margin_mode: str | None = None,
    canonical_coin_value: str | None = None,
) -> int | None:
    desired = desired_leverage_for_margin_mode(
        desired_default_leverage,
        margin_mode,
        canonical_coin_value=canonical_coin_value,
    )
    return effective_leverage_for_market(market_max_leverage, desired_default_leverage=desired)


def market_requires_isolated_margin(market_meta: dict[str, Any] | None) -> bool:
    if not isinstance(market_meta, dict):
        return False
    only_isolated = market_meta.get("onlyIsolated")
    if isinstance(only_isolated, bool) and only_isolated:
        return True
    if str(only_isolated or "").strip().lower() in {"1", "true", "yes"}:
        return True
    margin_mode = str(market_meta.get("marginMode") or "").strip().lower()
    return margin_mode in {"nocross", "no_cross", "isolated", "isolatedonly", "isolated_only"}


async def ensure_hyperliquid_market_risk_settings(
    *,
    db: Any,
    client: Any,
    settings: Any,
    account_address: str,
    dex: str,
    canonical_coin_value: str,
    asset_id: int | None = None,
    market_max_leverage: int | str | None = None,
    market_only_isolated: bool | None = None,
    desired_default_leverage: int = 10,
    action_type: str = "OPEN",
    reduce_only: bool = False,
    force_refresh: bool = False,
    allow_stale_confirmed_cache: bool = False,
) -> RiskSettingResult:
    dex_n = str(dex or "").lower()
    parsed = parse_coin(canonical_coin_value, default_dex=dex_n)
    canonical = canonical_coin(dex=parsed.dex, coin=parsed.coin)
    account = normalize_leader_address(account_address)
    risk_increasing = action_increases_risk(action_type, reduce_only=reduce_only)
    ttl_seconds = int(getattr(settings, "hyperliquid_risk_settings_ttl_seconds", 300) or 300)
    requested_default = int(desired_default_leverage or 10)
    row = await _load_or_create_row(
        db,
        account_address=account,
        dex=parsed.dex,
        canonical_coin_value=canonical,
    )
    now = datetime.now(timezone.utc)
    # The caller resolves the current market policy from authoritative exchange
    # metadata.  Do not let an old persisted target (for example, legacy
    # isolated policy leverage on a market that now supports cross margin)
    # override it.
    desired_default = requested_default
    if not row.desired_margin_mode:
        row.desired_margin_mode = DESIRED_MARGIN_MODE
    row.asset_id = asset_id if asset_id is not None else row.asset_id
    row.last_checked_at = now

    resolved_max = _int_or_none(market_max_leverage) or _int_or_none(row.market_max_leverage)
    resolved_only_isolated = market_only_isolated
    if resolved_max is None or row.asset_id is None or resolved_only_isolated is None:
        loaded_max, resolved_asset_id, loaded_only_isolated = await _load_market_meta(
            client,
            dex=parsed.dex,
            canonical_coin_value=canonical,
        )
        if row.asset_id is None and resolved_asset_id is not None:
            row.asset_id = resolved_asset_id
        if resolved_max is None:
            resolved_max = loaded_max or _int_or_none(market_max_leverage) or _int_or_none(row.market_max_leverage)
        if resolved_only_isolated is None:
            resolved_only_isolated = loaded_only_isolated
    if resolved_only_isolated:
        # Hyperliquid publishes this capability in market metadata. Respecting
        # it avoids a guaranteed-failing cross-margin action on the first open
        # and applies the global isolated-only safety rule immediately.
        row.desired_margin_mode = FALLBACK_MARGIN_MODE
    elif resolved_only_isolated is False:
        # Margin capability can change over time.  An explicit current
        # cross-capable result must retire a stale isolated fallback instead of
        # preserving it forever.
        row.desired_margin_mode = DESIRED_MARGIN_MODE
    row.desired_leverage = desired_leverage_for_margin_mode(
        desired_default,
        row.desired_margin_mode,
        canonical_coin_value=canonical,
    )
    row.market_max_leverage = resolved_max
    effective = effective_leverage_for_margin_mode(
        resolved_max,
        desired_default_leverage=desired_default,
        margin_mode=row.desired_margin_mode,
        canonical_coin_value=canonical,
    )
    row.effective_leverage = effective

    if effective is None:
        _mark_failed(row, REASON_MAX_LEVERAGE_UNKNOWN, "market max leverage missing or invalid", now=now)
        await db.flush()
        return _result_from_row(
            row,
            is_ok=not risk_increasing,
            reason_code=REASON_MAX_LEVERAGE_UNKNOWN,
            reason="market max leverage missing or invalid",
            warning=None if risk_increasing else "risk setting unknown; allowing reduce/close only",
        )

    if (
        not force_refresh
        and allow_stale_confirmed_cache
        and _confirmed_cache_valid(row, effective_leverage=effective)
    ):
        row.status = STATUS_CONFIRMED
        row.last_checked_at = now
        await db.flush()
        return _result_from_row(row, is_ok=True, cache_used=True)

    if not force_refresh and _confirmed_cache_fresh(row, effective_leverage=effective, ttl_seconds=ttl_seconds, now=now):
        row.status = STATUS_CONFIRMED
        row.last_checked_at = now
        await db.flush()
        return _result_from_row(row, is_ok=True, cache_used=True)

    row.status = STATUS_SETTING
    row.error_message = None
    await db.flush()

    fallback_warning: str | None = None
    primary_failure: Any | None = None
    for margin_mode in _margin_mode_attempts(row.desired_margin_mode):
        attempt_desired_leverage = desired_leverage_for_margin_mode(
            desired_default,
            margin_mode,
            canonical_coin_value=canonical,
        )
        attempt_effective = effective_leverage_for_margin_mode(
            resolved_max,
            desired_default_leverage=desired_default,
            margin_mode=margin_mode,
            canonical_coin_value=canonical,
        )
        if attempt_effective is None:
            _mark_failed(row, REASON_MAX_LEVERAGE_UNKNOWN, "market max leverage missing or invalid", now=now)
            await db.flush()
            return _result_from_row(
                row,
                is_ok=not risk_increasing,
                reason_code=REASON_MAX_LEVERAGE_UNKNOWN,
                reason="market max leverage missing or invalid",
                warning=None if risk_increasing else "risk setting unknown; allowing reduce/close only",
            )
        try:
            action_nonce = await _allocate_durable_signed_action_nonce(db, client)
            response = await _update_leverage(
                client,
                coin=canonical,
                leverage=attempt_effective,
                is_cross=_margin_mode_is_cross(margin_mode),
                asset_id=row.asset_id,
                nonce=action_nonce,
            )
        except Exception as exc:
            if margin_mode == DESIRED_MARGIN_MODE and _should_try_isolated_fallback(exc):
                primary_failure = str(exc)
                fallback_warning = _isolated_fallback_warning(exc)
                continue
            message = f"failed to set {margin_mode.lower()} {attempt_effective}x: {exc}"
            _mark_failed(row, REASON_RISK_SETTING_UPDATE_FAILED, message, now=now)
            await db.flush()
            return _result_from_row(
                row,
                is_ok=not risk_increasing,
                reason_code=REASON_RISK_SETTING_UPDATE_FAILED,
                reason=message,
                warning=None if risk_increasing else message,
            )

        if (
            not _update_response_confirmed(response)
            and _normalize_margin_mode(margin_mode) == FALLBACK_MARGIN_MODE
            and _isolated_margin_top_up_required(response)
            and hasattr(client, "top_up_isolated_only_margin")
        ):
            initial_response = response
            try:
                action_nonce = await _allocate_durable_signed_action_nonce(db, client)
                top_up_response = await _top_up_isolated_only_margin(
                    client,
                    coin=canonical,
                    leverage=attempt_effective,
                    asset_id=row.asset_id,
                    nonce=action_nonce,
                )
            except Exception as exc:
                message = f"failed to top up isolated-only margin to {attempt_effective}x: {exc}"
                _mark_failed(row, REASON_RISK_SETTING_UPDATE_FAILED, message, now=now)
                await db.flush()
                return _result_from_row(
                    row,
                    is_ok=not risk_increasing,
                    reason_code=REASON_RISK_SETTING_UPDATE_FAILED,
                    reason=message,
                    warning=None if risk_increasing else message,
                )
            if (
                not _update_response_confirmed(top_up_response)
                and _asset_not_strict_isolated(top_up_response)
                and hasattr(client, "add_isolated_margin")
            ):
                margin_addition = await _isolated_margin_addition_for_target_leverage(
                    client,
                    account_address=account,
                    dex=parsed.dex,
                    canonical_coin_value=canonical,
                    leverage=attempt_effective,
                )
                if margin_addition is not None and margin_addition > 0:
                    try:
                        action_nonce = await _allocate_durable_signed_action_nonce(db, client)
                        add_margin_response = await _add_isolated_margin(
                            client,
                            coin=canonical,
                            amount=margin_addition,
                            asset_id=row.asset_id,
                            nonce=action_nonce,
                        )
                        if _update_response_confirmed(add_margin_response):
                            retry_nonce = await _allocate_durable_signed_action_nonce(db, client)
                            retry_response = await _update_leverage(
                                client,
                                coin=canonical,
                                leverage=attempt_effective,
                                is_cross=False,
                                asset_id=row.asset_id,
                                nonce=retry_nonce,
                            )
                        else:
                            retry_response = add_margin_response
                        top_up_response = (
                            {
                                "status": "ok",
                                "response": retry_response,
                                "no_cross_margin_addition": add_margin_response,
                            }
                            if _update_response_confirmed(retry_response)
                            else retry_response
                        )
                    except Exception as exc:
                        message = f"failed to add isolated margin for {attempt_effective}x: {exc}"
                        _mark_failed(row, REASON_RISK_SETTING_UPDATE_FAILED, message, now=now)
                        await db.flush()
                        return _result_from_row(
                            row,
                            is_ok=not risk_increasing,
                            reason_code=REASON_RISK_SETTING_UPDATE_FAILED,
                            reason=message,
                            warning=None if risk_increasing else message,
                        )
            response = (
                {
                    "initial_update": initial_response,
                    "isolated_only_margin_top_up": top_up_response,
                    "status": "ok",
                }
                if _update_response_confirmed(top_up_response)
                else top_up_response
            )

        if not _update_response_confirmed(response):
            if margin_mode == DESIRED_MARGIN_MODE and _should_try_isolated_fallback(response):
                primary_failure = _mask_response(response)
                fallback_warning = _isolated_fallback_warning(response)
                continue
            message = "Hyperliquid leverage update did not return confirmed ok status"
            _mark_failed(row, REASON_CONFIRMATION_UNKNOWN, message, now=now, response=response)
            await db.flush()
            return _result_from_row(
                row,
                is_ok=not risk_increasing,
                reason_code=REASON_CONFIRMATION_UNKNOWN,
                reason=message,
                warning=None if risk_increasing else message,
            )

        active_position_confirmed = await _confirm_active_position_risk_setting(
            client,
            account_address=account,
            dex=parsed.dex,
            canonical_coin_value=canonical,
            margin_mode=margin_mode,
            leverage=attempt_effective,
        )
        if active_position_confirmed is False:
            message = (
                "exchange accepted leverage update but active position state did not confirm "
                f"{margin_mode.lower()} {attempt_effective}x"
            )
            _mark_failed(
                row,
                REASON_LEVERAGE_NOT_CONFIRMED,
                message,
                now=now,
                response=response,
            )
            await db.flush()
            return _result_from_row(
                row,
                is_ok=not risk_increasing,
                reason_code=REASON_LEVERAGE_NOT_CONFIRMED,
                reason=message,
                warning=None if risk_increasing else message,
            )

        row.status = STATUS_CONFIRMED
        row.desired_margin_mode = margin_mode
        row.desired_leverage = attempt_desired_leverage
        row.effective_leverage = attempt_effective
        row.actual_margin_mode = margin_mode
        row.actual_leverage = attempt_effective
        row.last_set_at = now
        row.last_confirmed_at = now
        row.error_message = None
        row.raw_response_masked = _mask_response(
            {
                "primary_failure": primary_failure,
                "fallback_margin_mode": margin_mode if margin_mode != DESIRED_MARGIN_MODE else None,
                "response": response,
            }
            if primary_failure is not None
            else response
        )
        await db.flush()
        return _result_from_row(row, is_ok=True, warning=fallback_warning)

    message = "cross margin is not allowed and isolated fallback was not attempted"
    _mark_failed(row, REASON_CROSS_MARGIN_NOT_SUPPORTED, message, now=now, response=primary_failure)
    await db.flush()
    return _result_from_row(
        row,
        is_ok=not risk_increasing,
        reason_code=REASON_CROSS_MARGIN_NOT_SUPPORTED,
        reason=message,
        warning=None if risk_increasing else message,
    )


async def build_market_risk_settings_coverage(
    *,
    db: Any,
    settings: Any,
    client: Any | None = None,
    ensure: bool = False,
    force_refresh: bool = False,
) -> dict[str, Any]:
    account = settings.hyperliquid_follower_account_address()
    desired_default = int(getattr(settings, "hyperliquid_default_leverage", 10) or 10)
    ttl_seconds = int(getattr(settings, "hyperliquid_risk_settings_ttl_seconds", 300) or 300)
    candidates = await current_risk_setting_candidates(db)
    existing_rows = await _risk_rows_for_account(db, account)
    candidates = _merge_existing_risk_rows_as_prepare_candidates(candidates, existing_rows)
    if client is not None:
        candidates = await _merge_common_market_prewarm_candidates(candidates, settings=settings, client=client)
    results: list[RiskSettingResult] = []
    if ensure and client is not None and account:
        for item in candidates:
            if not item["risk_setting_required"] and not item.get("prepare_risk_setting"):
                continue
            result = await ensure_hyperliquid_market_risk_settings(
                db=db,
                client=client,
                settings=settings,
                account_address=account,
                dex=item["dex"],
                canonical_coin_value=item["canonical_coin"],
                asset_id=item.get("asset_id"),
                market_max_leverage=item.get("market_max_leverage"),
                desired_default_leverage=desired_default,
                action_type="PREPARE_OPEN",
                force_refresh=force_refresh,
            )
            results.append(result)
    rows = await _risk_rows_for_account(db, account)
    row_map = {
        (str(row.dex or "").lower(), str(row.canonical_coin or "").upper()): row
        for row in rows
    }
    payload_rows: list[dict[str, Any]] = []
    for item in candidates:
        row = row_map.get((str(item["dex"] or "").lower(), str(item["canonical_coin"] or "").upper()))
        payload_rows.append(_coverage_row(item, row, desired_default=desired_default, ttl_seconds=ttl_seconds))
    known_candidate_keys = {
        (str(item["dex"] or "").lower(), str(item["canonical_coin"] or "").upper())
        for item in candidates
    }
    for row in rows:
        if (str(row.dex or "").lower(), str(row.canonical_coin or "").upper()) in known_candidate_keys:
            continue
        payload_rows.append(_coverage_row(None, row, desired_default=desired_default, ttl_seconds=ttl_seconds))

    required_rows = [row for row in payload_rows if row["risk_setting_required"]]
    confirmed = [row for row in required_rows if row["status"] == STATUS_CONFIRMED and not row["cache_stale"]]
    failed = [row for row in required_rows if row["status"] == STATUS_FAILED]
    unknown = [
        row
        for row in required_rows
        if row["status"] in {STATUS_UNKNOWN, STATUS_NEEDS_REFRESH} or row["cache_stale"]
    ]
    blockers = [
        f"{row['canonical_coin']}: {row.get('error') or row['status']}"
        for row in failed + unknown
    ]
    return {
        "risk_settings_enabled": True,
        "margin_mode_setup_enabled": True,
        "isolated_setup_enabled": True,
        "isolated_fallback_enabled": True,
        "leverage_setup_enabled": True,
        "desired_margin_mode": DESIRED_MARGIN_MODE,
        "target_default_leverage": desired_default,
        "effective_leverage_rule": (
            "cross=min(default, market_max_leverage); isolated=3x; CASHCAT=1x"
        ),
        "ttl_seconds": ttl_seconds,
        "markets_confirmed_count": len(confirmed),
        "markets_failed_count": len(failed),
        "markets_unknown_count": len(unknown),
        "failed_markets": failed,
        "unknown_markets": unknown,
        "blockers": blockers,
        "rows": payload_rows,
        "ensure_results": [result.payload() for result in results],
    }


async def prepare_current_hyperliquid_risk_settings(
    *,
    db: Any,
    settings: Any,
    client: Any,
    force_refresh: bool = False,
) -> dict[str, Any]:
    return await build_market_risk_settings_coverage(
        db=db,
        settings=settings,
        client=client,
        ensure=True,
        force_refresh=force_refresh,
    )


async def seed_market_risk_settings_for_account_migration(
    *,
    db: Any,
    previous_account_address: str,
    new_account_address: str,
    desired_default_leverage: int = 10,
) -> dict[str, Any]:
    previous = normalize_leader_address(previous_account_address or "")
    new = normalize_leader_address(new_account_address or "")
    if not previous or not new or previous == new:
        return {
            "source_count": 0,
            "seeded_count": 0,
            "updated_count": 0,
            "preserved_confirmed_count": 0,
            "template_keys": [],
        }

    source_rows = await _risk_rows_for_account(db, previous)
    target_rows = await _risk_rows_for_account(db, new)
    target_by_key = {
        _risk_setting_key(row): row
        for row in target_rows
    }
    now = datetime.now(timezone.utc)
    seeded_count = 0
    updated_count = 0
    preserved_confirmed_count = 0
    template_keys: list[dict[str, str]] = []

    for source in source_rows:
        key = _risk_setting_key(source)
        dex, canonical_upper = key
        parsed = parse_coin(source.canonical_coin, default_dex=source.dex)
        canonical = canonical_coin(dex=parsed.dex, coin=parsed.coin)
        template_keys.append({"dex": dex, "canonical_coin": canonical_upper})

        desired_margin_mode = _normalize_margin_mode(
            source.desired_margin_mode or source.actual_margin_mode or DESIRED_MARGIN_MODE
        )
        desired_leverage = desired_leverage_for_margin_mode(
            _int_or_none(source.desired_leverage) or int(desired_default_leverage or 10),
            desired_margin_mode,
            canonical_coin_value=canonical,
        )
        market_max = _int_or_none(source.market_max_leverage)
        effective = effective_leverage_for_margin_mode(
            market_max,
            desired_default_leverage=desired_leverage,
            margin_mode=desired_margin_mode,
            canonical_coin_value=canonical,
        ) or _int_or_none(source.effective_leverage)

        target = target_by_key.get(key)
        if target is None:
            target = MarketRiskSetting(
                execution_venue=ExecutionVenue.HYPERLIQUID.value,
                account_address=new,
                dex=dex,
                canonical_coin=canonical,
                asset_id=source.asset_id,
                desired_margin_mode=desired_margin_mode,
                desired_leverage=desired_leverage,
                market_max_leverage=market_max,
                effective_leverage=effective,
                actual_margin_mode=None,
                actual_leverage=None,
                status=STATUS_NEEDS_REFRESH,
                last_set_at=None,
                last_confirmed_at=None,
                last_checked_at=now,
                error_message="SEEDED_FROM_PREVIOUS_FOLLOWER; waiting for new account confirmation",
                raw_response_masked={
                    "source": "follower_account_migration_template",
                    "previous_follower": mask_address(previous),
                    "new_follower": mask_address(new),
                    "previous_row_id": source.id,
                    "previous_status": source.status,
                },
            )
            db.add(target)
            seeded_count += 1
            continue

        target.asset_id = target.asset_id if target.asset_id is not None else source.asset_id
        target.desired_margin_mode = desired_margin_mode
        target.desired_leverage = desired_leverage
        target.market_max_leverage = market_max if market_max is not None else target.market_max_leverage
        target.effective_leverage = effective if effective is not None else target.effective_leverage
        target.last_checked_at = now
        target.raw_response_masked = {
            "source": "follower_account_migration_template",
            "previous_follower": mask_address(previous),
            "new_follower": mask_address(new),
            "previous_row_id": source.id,
            "previous_status": source.status,
            "preserved_existing_row_id": target.id,
        }
        if target.status == STATUS_CONFIRMED and _confirmed_cache_valid(
            target,
            effective_leverage=target.effective_leverage or 0,
        ):
            preserved_confirmed_count += 1
        else:
            target.status = STATUS_NEEDS_REFRESH
            target.actual_margin_mode = None
            target.actual_leverage = None
            target.last_set_at = None
            target.last_confirmed_at = None
            target.error_message = "SEEDED_FROM_PREVIOUS_FOLLOWER; waiting for new account confirmation"
        updated_count += 1

    await db.flush()
    return {
        "source_count": len(source_rows),
        "seeded_count": seeded_count,
        "updated_count": updated_count,
        "preserved_confirmed_count": preserved_confirmed_count,
        "template_keys": template_keys,
    }


async def prepare_migrated_hyperliquid_risk_settings(
    *,
    db: Any,
    settings: Any,
    client: Any,
    previous_account_address: str,
    new_account_address: str,
    force_refresh: bool = True,
) -> dict[str, Any]:
    desired_default = int(getattr(settings, "hyperliquid_default_leverage", 10) or 10)
    seed_payload = await seed_market_risk_settings_for_account_migration(
        db=db,
        previous_account_address=previous_account_address,
        new_account_address=new_account_address,
        desired_default_leverage=desired_default,
    )
    confirm_payload = await confirm_migrated_hyperliquid_risk_settings(
        db=db,
        settings=settings,
        client=client,
        new_account_address=new_account_address,
        template_keys=seed_payload.get("template_keys", []),
        force_refresh=force_refresh,
    )
    return {**seed_payload, **confirm_payload}


async def confirm_migrated_hyperliquid_risk_settings(
    *,
    db: Any,
    settings: Any,
    client: Any,
    new_account_address: str,
    template_keys: list[dict[str, str]],
    force_refresh: bool = True,
) -> dict[str, Any]:
    desired_default = int(getattr(settings, "hyperliquid_default_leverage", 10) or 10)
    template_keys = {
        (str(item.get("dex") or "").lower(), str(item.get("canonical_coin") or "").upper())
        for item in template_keys
    }
    if not template_keys:
        return {
            "confirmed_count": 0,
            "failed_count": 0,
            "blockers": [],
            "results": [],
        }
    rows = await _risk_rows_for_account(db, new_account_address)
    results: list[RiskSettingResult] = []
    for row in rows:
        key = _risk_setting_key(row)
        if key not in template_keys:
            continue
        result = await ensure_hyperliquid_market_risk_settings(
            db=db,
            client=client,
            settings=settings,
            account_address=new_account_address,
            dex=row.dex,
            canonical_coin_value=row.canonical_coin,
            asset_id=row.asset_id,
            market_max_leverage=row.market_max_leverage,
            desired_default_leverage=row.desired_leverage or desired_default,
            action_type="PREPARE_OPEN",
            force_refresh=force_refresh,
        )
        results.append(result)

    blockers = [
        f"{result.canonical_coin}: {result.reason_code or result.reason or result.status}"
        for result in results
        if not result.is_ok
    ]
    return {
        "confirmed_count": len([result for result in results if result.is_ok]),
        "failed_count": len([result for result in results if not result.is_ok]),
        "blockers": blockers,
        "results": [result.payload() for result in results],
    }


async def current_risk_setting_candidates(db: Any) -> list[dict[str, Any]]:
    leaders = (await db.execute(active_leaders_statement())).scalars().all()
    leaders_by_address = {
        normalize_leader_address(leader.leader_address): leader
        for leader in leaders
    }
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    leader_states = (
        await db.execute(select(LatestAccountState).where(LatestAccountState.role == "LEADER"))
    ).scalars().all()
    state_ids = [state.id for state in leader_states if normalize_leader_address(state.address) in leaders_by_address]
    positions_by_state: dict[int, list[LatestAccountPosition]] = {}
    if state_ids:
        positions = (
            await db.execute(
                select(LatestAccountPosition)
                .where(LatestAccountPosition.account_state_id.in_(state_ids))
                .where(LatestAccountPosition.active.is_(True))
            )
        ).scalars().all()
        for position in positions:
            positions_by_state.setdefault(position.account_state_id, []).append(position)
    baselines = (
        await db.execute(
            select(LeaderPositionBaseline).where(
                LeaderPositionBaseline.leader_id.in_([leader.id for leader in leaders] or [0])
            )
        )
    ).scalars().all()
    baseline_by_scope = {
        (
            int(row.leader_id or 0),
            str(row.dex or "").lower(),
            str(row.canonical_coin or "").upper(),
        ): row
        for row in baselines
    }
    for state in leader_states:
        leader = leaders_by_address.get(normalize_leader_address(state.address))
        if leader is None:
            continue
        for position in positions_by_state.get(state.id, []):
            canonical = position.canonical_coin or canonical_coin(dex=position.dex, coin=position.coin)
            if not canonical or not is_coin_allowed(leader, canonical):
                continue
            baseline = baseline_by_scope.get((int(leader.id), str(position.dex or "").lower(), str(canonical or "").upper()))
            waiting = bool(baseline and str(baseline.baseline_status).upper() == BASELINE_WAIT_UNTIL_FLAT)
            key = (position.dex, canonical)
            rows[key] = {
                "dex": position.dex,
                "dex_display_name": dex_display_name(position.dex),
                "canonical_coin": canonical,
                "asset_id": _asset_id_from_position(position),
                "market_max_leverage": _max_leverage_from_position(position),
                "source": "leader_open_position",
                "baseline_status": baseline.baseline_status if baseline else None,
                "risk_setting_required": not waiting,
                "reason": "WAIT_UNTIL_FLAT existing position" if waiting else "copyable/current market",
            }
    allocations = (
        await db.execute(
            select(LeaderPositionAllocationRecord).where(
                LeaderPositionAllocationRecord.execution_venue == ExecutionVenue.HYPERLIQUID.value,
                LeaderPositionAllocationRecord.status != "CLOSED",
            )
        )
    ).scalars().all()
    for allocation in allocations:
        canonical = allocation.canonical_coin or canonical_coin(dex=allocation.dex, coin=allocation.hyperliquid_coin)
        key = (allocation.dex, canonical)
        rows[key] = {
            **rows.get(key, {}),
            "dex": allocation.dex,
            "dex_display_name": dex_display_name(allocation.dex),
            "canonical_coin": canonical,
            "asset_id": rows.get(key, {}).get("asset_id"),
            "market_max_leverage": rows.get(key, {}).get("market_max_leverage"),
            "source": "active_allocation",
            "baseline_status": rows.get(key, {}).get("baseline_status"),
            "risk_setting_required": True,
            "reason": "active allocation",
        }
    return sorted(rows.values(), key=lambda item: (item["dex"], item["canonical_coin"]))


def _merge_existing_risk_rows_as_prepare_candidates(
    candidates: list[dict[str, Any]],
    existing_rows: list[MarketRiskSetting],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {
        (str(item.get("dex") or "").lower(), str(item.get("canonical_coin") or "").upper()): dict(item)
        for item in candidates
    }
    for row in existing_rows:
        parsed = parse_coin(row.canonical_coin, default_dex=row.dex)
        canonical = canonical_coin(dex=parsed.dex, coin=parsed.coin)
        key = (parsed.dex, canonical.upper())
        item = merged.get(key)
        if item is None:
            item = {
                "dex": parsed.dex,
                "dex_display_name": dex_display_name(parsed.dex),
                "canonical_coin": canonical,
                "asset_id": row.asset_id,
                "market_max_leverage": row.market_max_leverage,
                "source": "stored_risk_setting",
                "baseline_status": None,
                "risk_setting_required": False,
                "reason": "known market risk setting prewarm",
            }
            merged[key] = item
        else:
            item.setdefault("asset_id", row.asset_id)
            item.setdefault("market_max_leverage", row.market_max_leverage)
        item["prepare_risk_setting"] = True
    return sorted(merged.values(), key=lambda item: (item["dex"], item["canonical_coin"]))


async def _merge_common_market_prewarm_candidates(
    candidates: list[dict[str, Any]],
    *,
    settings: Any,
    client: Any,
) -> list[dict[str, Any]]:
    configured = _common_coin_list(settings)
    if not configured:
        return candidates
    meta = await _safe_market_meta(client, dex="")
    if not meta:
        return candidates
    configured_set = set(configured)
    merged: dict[tuple[str, str], dict[str, Any]] = {
        (str(item.get("dex") or "").lower(), str(item.get("canonical_coin") or "").upper()): dict(item)
        for item in candidates
    }
    for index, item in enumerate(meta.get("universe", []) or []):
        parsed = parse_coin(str(item.get("name", "")), default_dex="")
        if parsed.dex or parsed.coin not in configured_set:
            continue
        canonical = canonical_coin(dex="", coin=parsed.coin)
        key = ("", canonical.upper())
        candidate = merged.get(key)
        if candidate is None:
            candidate = {
                "dex": "",
                "dex_display_name": dex_display_name(""),
                "canonical_coin": canonical,
                "asset_id": index,
                "market_max_leverage": _int_or_none(item.get("maxLeverage")),
                "source": "common_market_prewarm",
                "baseline_status": None,
                "risk_setting_required": False,
                "reason": "default Hyperliquid common market prewarm",
            }
            merged[key] = candidate
        else:
            candidate["asset_id"] = candidate.get("asset_id") if candidate.get("asset_id") is not None else index
            candidate["market_max_leverage"] = candidate.get("market_max_leverage") or _int_or_none(item.get("maxLeverage"))
        candidate["prepare_risk_setting"] = True
    return sorted(merged.values(), key=lambda item: (item["dex"], item["canonical_coin"]))


async def _safe_market_meta(client: Any, *, dex: str) -> dict[str, Any] | None:
    try:
        try:
            data = await client.meta(dex)
        except TypeError:
            data = await client.meta()
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _common_coin_list(settings: Any) -> list[str]:
    if hasattr(settings, "hyperliquid_prewarm_common_coin_list"):
        return list(settings.hyperliquid_prewarm_common_coin_list())
    raw = str(getattr(settings, "hyperliquid_prewarm_common_coins", "") or "")
    coins: list[str] = []
    for part in raw.split(","):
        coin = part.strip().upper()
        if coin and coin not in coins:
            coins.append(coin)
    return coins


async def _load_or_create_row(
    db: Any,
    *,
    account_address: str,
    dex: str,
    canonical_coin_value: str,
) -> MarketRiskSetting:
    row = await db.scalar(
        select(MarketRiskSetting)
        .where(MarketRiskSetting.execution_venue == ExecutionVenue.HYPERLIQUID.value)
        .where(MarketRiskSetting.account_address == account_address)
        .where(MarketRiskSetting.dex == dex)
        .where(func.upper(MarketRiskSetting.canonical_coin) == str(canonical_coin_value).upper())
        .limit(1)
    )
    if row is not None:
        return row
    if hasattr(db, "execute"):
        insert_stmt = (
            pg_insert(MarketRiskSetting)
            .values(
                execution_venue=ExecutionVenue.HYPERLIQUID.value,
                account_address=account_address,
                dex=dex,
                canonical_coin=canonical_coin_value,
                desired_margin_mode=DESIRED_MARGIN_MODE,
                status=STATUS_UNKNOWN,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    MarketRiskSetting.execution_venue,
                    MarketRiskSetting.account_address,
                    MarketRiskSetting.dex,
                    MarketRiskSetting.canonical_coin,
                ]
            )
        )
        await db.execute(insert_stmt)
        row = await db.scalar(
            select(MarketRiskSetting)
            .where(MarketRiskSetting.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(MarketRiskSetting.account_address == account_address)
            .where(MarketRiskSetting.dex == dex)
            .where(func.upper(MarketRiskSetting.canonical_coin) == str(canonical_coin_value).upper())
            .limit(1)
        )
        if row is not None:
            return row
    row = MarketRiskSetting(
        execution_venue=ExecutionVenue.HYPERLIQUID.value,
        account_address=account_address,
        dex=dex,
        canonical_coin=canonical_coin_value,
        desired_margin_mode=DESIRED_MARGIN_MODE,
        status=STATUS_UNKNOWN,
    )
    db.add(row)
    await db.flush()
    return row


async def _risk_rows_for_account(db: Any, account: str | None) -> list[MarketRiskSetting]:
    if not account:
        return []
    return (
        await db.execute(
            select(MarketRiskSetting)
            .where(MarketRiskSetting.execution_venue == ExecutionVenue.HYPERLIQUID.value)
            .where(MarketRiskSetting.account_address == normalize_leader_address(account))
            .order_by(MarketRiskSetting.dex, MarketRiskSetting.canonical_coin)
        )
    ).scalars().all()


def _risk_setting_key(row: MarketRiskSetting) -> tuple[str, str]:
    parsed = parse_coin(row.canonical_coin, default_dex=row.dex)
    canonical = canonical_coin(dex=parsed.dex, coin=parsed.coin)
    return str(parsed.dex or "").lower(), str(canonical or "").upper()


async def _load_market_meta(
    client: Any,
    *,
    dex: str,
    canonical_coin_value: str,
) -> tuple[int | None, int | None, bool | None]:
    try:
        meta = await client.meta(dex)
    except TypeError:
        meta = await client.meta()
    except Exception:
        return None, None, None
    parsed_target = parse_coin(canonical_coin_value, default_dex=dex)
    for item in meta.get("universe", []) or []:
        parsed = parse_coin(str(item.get("name", "")), default_dex=dex)
        if parsed.canonical_coin != parsed_target.canonical_coin:
            continue
        max_leverage = _int_or_none(item.get("maxLeverage"))
        asset_id = resolve_asset_id_from_meta(meta, coin=parsed.coin, dex=dex)
        return max_leverage, asset_id, market_requires_isolated_margin(item)
    return None, None, None


def _confirmed_cache_fresh(
    row: MarketRiskSetting,
    *,
    effective_leverage: int,
    ttl_seconds: int,
    now: datetime,
) -> bool:
    if row.status != STATUS_CONFIRMED:
        return False
    if not _confirmed_margin_mode_allowed(row.actual_margin_mode):
        return False
    if int(row.actual_leverage or 0) != effective_leverage:
        return False
    if row.last_confirmed_at is None:
        return False
    return row.last_confirmed_at >= now - timedelta(seconds=ttl_seconds)


def _confirmed_cache_valid(
    row: MarketRiskSetting,
    *,
    effective_leverage: int,
) -> bool:
    if row.status != STATUS_CONFIRMED:
        return False
    if not _confirmed_margin_mode_allowed(row.actual_margin_mode):
        return False
    if int(row.actual_leverage or 0) != effective_leverage:
        return False
    return row.last_confirmed_at is not None


def _margin_mode_attempts(preferred: str | None) -> list[str]:
    normalized = _normalize_margin_mode(preferred or DESIRED_MARGIN_MODE)
    if normalized == DESIRED_MARGIN_MODE:
        return [DESIRED_MARGIN_MODE, FALLBACK_MARGIN_MODE]
    return [normalized]


def _margin_mode_is_cross(value: str | None) -> bool:
    return _normalize_margin_mode(value) == "CROSS"


async def _update_leverage(
    client: Any,
    *,
    coin: str,
    leverage: int,
    is_cross: bool,
    asset_id: int | None,
    nonce: int | None = None,
) -> Any:
    kwargs = {"coin": coin, "leverage": leverage, "is_cross": is_cross}
    if asset_id is not None:
        kwargs["asset_id"] = asset_id
    if nonce is not None:
        kwargs["nonce"] = nonce
    return await _call_with_optional_action_kwargs(
        client.update_leverage,
        kwargs,
    )


async def _top_up_isolated_only_margin(
    client: Any,
    *,
    coin: str,
    leverage: int,
    asset_id: int | None,
    nonce: int | None = None,
) -> Any:
    kwargs = {"coin": coin, "leverage": leverage}
    if asset_id is not None:
        kwargs["asset_id"] = asset_id
    if nonce is not None:
        kwargs["nonce"] = nonce
    return await _call_with_optional_action_kwargs(
        client.top_up_isolated_only_margin,
        kwargs,
    )


async def _add_isolated_margin(
    client: Any,
    *,
    coin: str,
    amount: Decimal,
    asset_id: int | None,
    nonce: int | None = None,
) -> Any:
    kwargs = {"coin": coin, "amount": amount}
    if asset_id is not None:
        kwargs["asset_id"] = asset_id
    if nonce is not None:
        kwargs["nonce"] = nonce
    return await _call_with_optional_action_kwargs(
        client.add_isolated_margin,
        kwargs,
    )


async def _remove_isolated_margin(
    client: Any,
    *,
    coin: str,
    amount: Decimal,
    asset_id: int | None,
    nonce: int | None = None,
) -> Any:
    kwargs = {"coin": coin, "amount": amount}
    if asset_id is not None:
        kwargs["asset_id"] = asset_id
    if nonce is not None:
        kwargs["nonce"] = nonce
    return await _call_with_optional_action_kwargs(
        client.remove_isolated_margin,
        kwargs,
    )


async def _call_with_optional_action_kwargs(method: Any, kwargs: dict[str, Any]) -> Any:
    remaining = dict(kwargs)
    for _attempt in range(3):
        try:
            return await method(**remaining)
        except TypeError as exc:
            message = str(exc)
            removed = False
            for key in ("nonce", "asset_id"):
                if key in remaining and key in message:
                    remaining.pop(key, None)
                    removed = True
            if not removed:
                raise
    return await method(**remaining)


async def _allocate_durable_signed_action_nonce(db: Any, client: Any) -> int | None:
    """Join non-order signed actions to the same cross-process nonce ledger.

    A master API wallet can sign for both the master and its subaccounts, but
    Hyperliquid tracks the nonce by signer.  Without this database allocation,
    simultaneous first-market leverage setup in two watcher processes could
    reuse one millisecond nonce even though their positions are isolated.
    """
    signer_scope = str(getattr(client, "signer_scope", "") or "")
    if not signer_scope or not isinstance(db, AsyncSession):
        return None
    now = datetime.now(timezone.utc)
    nonce_floor = int(now.timestamp() * 1000)
    statement = pg_insert(SignerNonceState).values(
        signer_scope=signer_scope,
        last_nonce=nonce_floor,
        updated_at=now,
    )
    allocated = await db.scalar(
        statement.on_conflict_do_update(
            index_elements=[SignerNonceState.signer_scope],
            set_={
                "last_nonce": func.greatest(
                    SignerNonceState.last_nonce + 1,
                    nonce_floor,
                ),
                "updated_at": now,
            },
        ).returning(SignerNonceState.last_nonce)
    )
    if allocated is None:
        raise RuntimeError("database did not allocate a signed-action nonce")
    nonce = int(allocated)
    reserve = getattr(client, "reserve_action_nonce_at_least", None)
    if callable(reserve):
        nonce = int(reserve(nonce))
        await db.execute(
            update(SignerNonceState)
            .where(SignerNonceState.signer_scope == signer_scope)
            .values(
                last_nonce=func.greatest(SignerNonceState.last_nonce, nonce),
                updated_at=now,
            )
        )
    return nonce


async def _isolated_margin_addition_for_target_leverage(
    client: Any,
    *,
    account_address: str,
    dex: str,
    canonical_coin_value: str,
    leverage: int,
) -> Decimal | None:
    if not hasattr(client, "account_state") or leverage <= 0:
        return None
    try:
        try:
            state = await client.account_state(address=account_address, dex=dex)
        except TypeError:
            state = await client.account_state(account_address, dex=dex)
    except Exception:
        return None
    expected = parse_coin(canonical_coin_value, default_dex=dex)
    for item in (state or {}).get("assetPositions", []) or []:
        position = item.get("position", item)
        parsed = parse_coin(str(position.get("coin") or ""), default_dex=dex)
        if parsed.canonical_coin != expected.canonical_coin:
            continue
        size = _decimal_or_none(position.get("szi") or position.get("size"))
        position_value = _decimal_or_none(position.get("positionValue") or position.get("notional"))
        margin_used = _decimal_or_none(position.get("marginUsed"))
        if (
            size is None
            or abs(size) <= Decimal("0.00000001")
            or position_value is None
            or margin_used is None
        ):
            return None
        target_margin = abs(position_value) / Decimal(leverage)
        shortfall = max(Decimal("0"), target_margin - margin_used)
        # Protect against mark-price movement between the state read and the
        # signed margin action. Extra isolated margin only lowers liquidation risk.
        price_buffer = max(Decimal("1"), target_margin * Decimal("0.005"))
        return shortfall + price_buffer
    return None


def isolated_margin_excess_for_target_leverage(
    position: Any,
    *,
    target_leverage: int = ISOLATED_TARGET_LEVERAGE,
) -> Decimal:
    """Return safely removable isolated margin, retaining a price buffer."""
    if target_leverage <= 0 or position is None:
        return Decimal("0")

    def value(*names: str) -> Any:
        for name in names:
            if isinstance(position, dict):
                candidate = position.get(name)
            else:
                candidate = getattr(position, name, None)
            if candidate is not None and candidate != "":
                return candidate
        return None

    size = _decimal_or_none(value("szi", "size"))
    notional = _decimal_or_none(value("positionValue", "notional"))
    entry_px = _decimal_or_none(value("entryPx", "entry_px"))
    margin_used = _decimal_or_none(value("marginUsed", "margin_used"))
    if size is None or abs(size) <= Decimal("0.00000001") or margin_used is None:
        return Decimal("0")

    reference_candidates: list[Decimal] = []
    if notional is not None:
        reference_candidates.append(abs(notional))
    if entry_px is not None:
        reference_candidates.append(abs(size * entry_px))
    if not reference_candidates:
        return Decimal("0")
    reference_notional = max(reference_candidates)
    if reference_notional <= 0:
        return Decimal("0")

    target_margin = reference_notional / Decimal(target_leverage)
    safety_buffer = max(
        ISOLATED_MARGIN_REBALANCE_MIN_BUFFER,
        target_margin * ISOLATED_MARGIN_REBALANCE_BUFFER_RATIO,
    )
    removable = margin_used - target_margin - safety_buffer
    if removable < ISOLATED_MARGIN_REBALANCE_MIN_RELEASE:
        return Decimal("0")
    return removable.quantize(Decimal("0.000001"), rounding=ROUND_DOWN)


async def _load_active_position_for_margin_rebalance(
    client: Any,
    *,
    account_address: str,
    dex: str,
    canonical_coin_value: str,
) -> dict[str, Any] | None:
    if not hasattr(client, "account_state"):
        return None
    try:
        state = await client.account_state(address=account_address, dex=dex)
    except TypeError:
        state = await client.account_state(account_address, dex=dex)
    expected = parse_coin(canonical_coin_value, default_dex=dex)
    for item in (state or {}).get("assetPositions", []) or []:
        position = item.get("position", item)
        if not isinstance(position, dict):
            continue
        parsed = parse_coin(str(position.get("coin") or ""), default_dex=dex)
        if parsed.canonical_coin != expected.canonical_coin:
            continue
        size = _decimal_or_none(position.get("szi") or position.get("size"))
        if size is not None and abs(size) > Decimal("0.00000001"):
            return position
    return None


async def remove_excess_isolated_margin_for_target_leverage(
    *,
    db: Any,
    client: Any,
    account_address: str,
    dex: str,
    canonical_coin_value: str,
    asset_id: int | None = None,
    target_leverage: int = ISOLATED_TARGET_LEVERAGE,
) -> Decimal:
    """Release legacy excess margin after an isolated leverage migration."""
    if not hasattr(client, "remove_isolated_margin"):
        return Decimal("0")
    position = await _load_active_position_for_margin_rebalance(
        client,
        account_address=account_address,
        dex=dex,
        canonical_coin_value=canonical_coin_value,
    )
    removable = isolated_margin_excess_for_target_leverage(
        position,
        target_leverage=target_leverage,
    )
    if removable <= 0:
        return Decimal("0")
    action_nonce = await _allocate_durable_signed_action_nonce(db, client)
    response = await _remove_isolated_margin(
        client,
        coin=canonical_coin_value,
        amount=removable,
        asset_id=asset_id,
        nonce=action_nonce,
    )
    if not _update_response_confirmed(response):
        raise RuntimeError(
            "isolated margin removal did not return confirmed ok status: "
            f"{_mask_response(response)}"
        )
    return removable


async def _confirm_active_position_risk_setting(
    client: Any,
    *,
    account_address: str,
    dex: str,
    canonical_coin_value: str,
    margin_mode: str,
    leverage: int,
) -> bool | None:
    if not hasattr(client, "account_state"):
        return None
    expected = parse_coin(canonical_coin_value, default_dex=dex)
    observed_active = False
    for attempt in range(3):
        try:
            try:
                state = await client.account_state(address=account_address, dex=dex)
            except TypeError:
                try:
                    state = await client.account_state(account_address, dex=dex)
                except TypeError:
                    state = await client.account_state(dex=dex)
        except Exception:
            return None
        for item in (state or {}).get("assetPositions", []) or []:
            position = item.get("position", item)
            parsed = parse_coin(str(position.get("coin") or ""), default_dex=dex)
            if parsed.canonical_coin != expected.canonical_coin:
                continue
            size = _decimal_or_none(position.get("szi") or position.get("size"))
            if size is None or abs(size) <= Decimal("0.00000001"):
                continue
            observed_active = True
            payload = position.get("leverage") or {}
            observed_mode = _normalize_margin_mode(
                payload.get("type") if isinstance(payload, dict) else None
            )
            observed_leverage = _int_or_none(
                payload.get("value") if isinstance(payload, dict) else None
            )
            if observed_mode == _normalize_margin_mode(margin_mode) and observed_leverage == leverage:
                return True
        if observed_active and attempt < 2:
            await asyncio.sleep(0.05)
    return False if observed_active else None


def _normalize_margin_mode(value: str | None) -> str:
    normalized = str(value or DESIRED_MARGIN_MODE).upper()
    if normalized in {"CROSS", "CROSSED"}:
        return "CROSS"
    if normalized == "ISOLATED":
        return "ISOLATED"
    return DESIRED_MARGIN_MODE


def _confirmed_margin_mode_allowed(value: str | None) -> bool:
    return _normalize_margin_mode(value) in {DESIRED_MARGIN_MODE, FALLBACK_MARGIN_MODE}


def _cross_margin_not_allowed(value: Any) -> bool:
    text = str(value).lower()
    if isinstance(value, dict):
        text = str(_mask_response(value)).lower()
    return "cross margin is not allowed" in text or "cross margin not allowed" in text


def _invalid_leverage_value(value: Any) -> bool:
    text = str(value).lower()
    if isinstance(value, dict):
        text = str(_mask_response(value)).lower()
    return "invalid leverage value" in text


def _isolated_margin_top_up_required(value: Any) -> bool:
    text = str(_mask_response(value) if isinstance(value, dict) else value).lower()
    return (
        "isolated position does not have sufficient margin available to decrease leverage" in text
        or ("decrease leverage" in text and "add margin to the position" in text)
    )


def _asset_not_strict_isolated(value: Any) -> bool:
    text = str(_mask_response(value) if isinstance(value, dict) else value).lower()
    return "asset not strict isolated" in text


def _should_try_isolated_fallback(value: Any) -> bool:
    return _cross_margin_not_allowed(value) or _invalid_leverage_value(value)


def _isolated_fallback_warning(value: Any) -> str:
    if _cross_margin_not_allowed(value):
        return "cross margin unsupported; using isolated margin for this market"
    return "cross leverage rejected; using isolated margin for this market"


def _mark_failed(
    row: MarketRiskSetting,
    reason_code: str,
    message: str,
    *,
    now: datetime,
    response: Any | None = None,
) -> None:
    row.status = STATUS_FAILED
    row.actual_margin_mode = None
    row.actual_leverage = None
    row.last_checked_at = now
    row.error_message = f"{reason_code}: {message}"
    row.raw_response_masked = _mask_response(response) if response is not None else None


def _result_from_row(
    row: MarketRiskSetting,
    *,
    is_ok: bool,
    reason_code: str | None = None,
    reason: str | None = None,
    warning: str | None = None,
    cache_used: bool = False,
) -> RiskSettingResult:
    parsed = parse_coin(row.canonical_coin, default_dex=row.dex)
    return RiskSettingResult(
        is_ok=is_ok,
        status=row.status,
        account_address=row.account_address,
        dex=row.dex,
        canonical_coin=parsed.canonical_coin,
        asset_id=row.asset_id,
        desired_margin_mode=row.desired_margin_mode,
        desired_leverage=row.desired_leverage or 10,
        market_max_leverage=row.market_max_leverage,
        effective_leverage=row.effective_leverage,
        actual_margin_mode=row.actual_margin_mode,
        actual_leverage=row.actual_leverage,
        reason_code=reason_code or _reason_code_from_row(row),
        reason=reason or row.error_message,
        warning=warning,
        cache_used=cache_used,
        row_id=row.id,
        last_confirmed_at=row.last_confirmed_at,
    )


def _coverage_row(
    item: dict[str, Any] | None,
    row: MarketRiskSetting | None,
    *,
    desired_default: int,
    ttl_seconds: int,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    status = row.status if row else STATUS_UNKNOWN
    last_confirmed_at = row.last_confirmed_at if row else None
    dex = (item or {}).get("dex") if item else row.dex
    canonical = (item or {}).get("canonical_coin") if item else row.canonical_coin
    max_leverage = (item or {}).get("market_max_leverage") if item else row.market_max_leverage
    if max_leverage is None and row is not None:
        max_leverage = row.market_max_leverage
    desired_margin_mode = row.desired_margin_mode if row and row.desired_margin_mode else DESIRED_MARGIN_MODE
    raw_desired_leverage = _int_or_none(row.desired_leverage if row else None) or desired_default
    desired_leverage = desired_leverage_for_margin_mode(
        raw_desired_leverage,
        desired_margin_mode,
        canonical_coin_value=canonical,
    )
    policy_effective_leverage = effective_leverage_for_margin_mode(
        max_leverage,
        desired_default_leverage=desired_leverage,
        margin_mode=desired_margin_mode,
        canonical_coin_value=canonical,
    )
    cache_stale = bool(
        status == STATUS_CONFIRMED
        and (
            last_confirmed_at is None
            or last_confirmed_at < now - timedelta(seconds=ttl_seconds)
            or policy_effective_leverage is None
            or _int_or_none(row.actual_leverage if row else None) != policy_effective_leverage
        )
    )
    return {
        "dex": dex,
        "dex_display_name": dex_display_name(dex),
        "canonical_coin": canonical,
        "asset_id": (item or {}).get("asset_id") if item else row.asset_id,
        "desired_margin_mode": desired_margin_mode,
        "desired_leverage": desired_leverage,
        "market_max_leverage": max_leverage,
        "effective_leverage": policy_effective_leverage,
        "actual_margin_mode": row.actual_margin_mode if row else None,
        "actual_leverage": row.actual_leverage if row else None,
        "status": STATUS_NEEDS_REFRESH if cache_stale else status,
        "cache_stale": cache_stale,
        "last_confirmed_at": last_confirmed_at.isoformat() if last_confirmed_at else None,
        "last_checked_at": row.last_checked_at.isoformat() if row and row.last_checked_at else None,
        "error": row.error_message if row else None,
        "risk_setting_required": bool((item or {}).get("risk_setting_required", False)),
        "prepare_risk_setting": bool((item or {}).get("prepare_risk_setting", False)),
        "source": (item or {}).get("source", "stored_setting"),
        "baseline_status": (item or {}).get("baseline_status"),
        "reason": (item or {}).get("reason"),
    }


def _reason_code_from_row(row: MarketRiskSetting) -> str | None:
    if not row.error_message:
        return None
    return str(row.error_message).split(":", 1)[0]


def _update_response_confirmed(response: Any) -> bool:
    if response is None:
        return False
    if not isinstance(response, dict):
        return True
    status = str(response.get("status", "ok")).lower()
    return status == "ok"


def _mask_response(response: Any) -> Any:
    if isinstance(response, dict):
        return response
    return {"response": str(response)[:500]}


def _int_or_none(value: Any) -> int | None:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _decimal_or_none(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (TypeError, ValueError, ArithmeticError):
        return None


def _max_leverage_from_position(position: LatestAccountPosition) -> int | None:
    raw = position.raw_payload_masked or {}
    if isinstance(raw, dict):
        return _int_or_none(raw.get("maxLeverage"))
    return None


def _asset_id_from_position(position: LatestAccountPosition) -> int | None:
    raw = position.raw_payload_masked or {}
    if not isinstance(raw, dict):
        return None
    for key in ("asset", "assetId", "asset_id"):
        value = _int_or_none(raw.get(key))
        if value is not None:
            return value
    return None
