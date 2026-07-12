from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select

from app.api.deps import AppSettings, CurrentUser, DbSession
from app.api.risk import get_risk_setting
from app.models import ExecutionOrder, LeaderPositionAllocationRecord, RiskEvent
from app.schemas.api import ManualOrderRequest
from app.services.binance_client import BinanceFuturesClient
from app.services.execution_router import ExecutionVenue
from app.services.hedge_orders import HedgeAction, build_hedge_mode_order
from app.services.hyperliquid_execution import (
    HyperliquidExecutionClient,
    HyperliquidRiskSettingsService,
    build_hyperliquid_cloid,
)
from app.services.hyperliquid_dex import parse_coin
from app.services.risk_settings import BinanceRiskSettingsService
from app.services.target_position import PositionSide

router = APIRouter(tags=["manual-orders"])


@router.post("/manual-orders")
async def manual_order(
    _: CurrentUser,
    payload: ManualOrderRequest,
    db: DbSession,
    settings: AppSettings,
):
    if payload.confirmation != "CONFIRM":
        raise HTTPException(status_code=400, detail="second confirmation required")
    if payload.quantity is None and payload.notional is None:
        raise HTTPException(status_code=400, detail="quantity or notional required")
    try:
        execution_venue = ExecutionVenue(payload.execution_venue.upper())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="execution_venue must be HYPERLIQUID or BINANCE") from exc
    order_type = payload.order_type.upper()
    if order_type not in {"MARKET", "LIMIT"}:
        raise HTTPException(status_code=400, detail="order_type must be MARKET or LIMIT")
    if execution_venue == ExecutionVenue.HYPERLIQUID and order_type != "MARKET":
        raise HTTPException(status_code=400, detail="Hyperliquid manual orders use MARKET-equivalent IOC")
    if order_type == "LIMIT" and payload.price is None:
        raise HTTPException(status_code=400, detail="LIMIT manual orders require price")
    try:
        position_side = PositionSide(payload.position_side.upper())
        action = HedgeAction(payload.action.upper())
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="position_side must be LONG/SHORT and action must be OPEN_OR_INCREASE/CLOSE_OR_REDUCE",
        ) from exc
    if position_side == PositionSide.FLAT:
        raise HTTPException(status_code=400, detail="manual Hedge Mode order requires LONG or SHORT")

    if payload.reduce_only and action == HedgeAction.OPEN_OR_INCREASE:
        action = HedgeAction.CLOSE_OR_REDUCE
    close_intent = action == HedgeAction.CLOSE_OR_REDUCE
    validation_qty = payload.quantity or Decimal("1")
    if validation_qty <= 0:
        raise HTTPException(status_code=400, detail="quantity must be positive")
    symbol = payload.symbol.upper()
    parsed_hl_coin = parse_coin(payload.symbol)
    source_coin = symbol.removesuffix("USDT") if execution_venue == ExecutionVenue.BINANCE else parsed_hl_coin.coin
    dex = "" if execution_venue == ExecutionVenue.BINANCE else parsed_hl_coin.dex
    canonical = parsed_hl_coin.canonical_coin if execution_venue == ExecutionVenue.HYPERLIQUID else source_coin
    hedge_order = build_hedge_mode_order(
        symbol=symbol,
        allocation_side=position_side,
        action=action,
        quantity=validation_qty,
        is_close_intent=close_intent,
        leader_allocation_qty=validation_qty if close_intent else None,
        binance_position_qty=validation_qty if close_intent else None,
    )
    if payload.side and payload.side.upper() != hedge_order.side:
        raise HTTPException(
            status_code=400,
            detail=f"side is derived from position_side/action; expected {hedge_order.side}",
        )

    risk_error: str | None = None
    hyperliquid_risk_checklist: dict | None = None
    risk_setting = await get_risk_setting(db)
    kill_switch_off = not bool(risk_setting.get("kill_switch", False))
    venue_trading_enabled = (
        settings.binance_trading_enabled
        if execution_venue == ExecutionVenue.BINANCE
        else settings.hyperliquid_trading_enabled
    )
    venue_execution_enabled = (
        settings.enable_binance_execution
        if execution_venue == ExecutionVenue.BINANCE
        else settings.enable_hyperliquid_execution
    )
    if settings.trading_enabled and venue_trading_enabled:
        if not kill_switch_off and not close_intent:
            risk_error = "kill switch is active"
        elif not venue_execution_enabled:
            risk_error = f"{execution_venue.value} execution is disabled"
        elif execution_venue == ExecutionVenue.BINANCE and (
            not settings.binance_api_key or not settings.binance_api_secret
        ):
            risk_error = "Binance API credentials are not configured"
        elif execution_venue == ExecutionVenue.HYPERLIQUID and (
            not settings.hyperliquid_private_key_value() or not settings.hyperliquid_follower_account_address()
        ):
            risk_error = "Hyperliquid execution wallet/private key is not configured"
        elif execution_venue == ExecutionVenue.BINANCE:
            client = BinanceFuturesClient(settings)
            try:
                result = await BinanceRiskSettingsService(
                    client,
                    expected_margin_type=settings.binance_expected_margin_type,
                    expected_leverage=settings.binance_expected_leverage,
                ).ensure_symbol_risk_settings(
                    payload.symbol.upper(), reduce_only=close_intent
                )
                if not result.is_ok:
                    risk_error = result.reason
            finally:
                await client.close()
        else:
            client = HyperliquidExecutionClient(
                info_url=f"{settings.hyperliquid_execution_base_url()}/info",
                private_key=settings.hyperliquid_private_key_value(),
                account_address=settings.hyperliquid_account_address or settings.hyperliquid_signer_address(),
                vault_address=settings.hyperliquid_vault_address,
                network=settings.hyperliquid_execution_network,
            )
            try:
                result = await HyperliquidRiskSettingsService(
                    client,
                    expected_leverage=settings.hyperliquid_default_leverage,
                    settings=settings,
                ).ensure_symbol_risk_settings(
                    source_coin,
                    reduce_only=close_intent,
                    target_notional=payload.notional,
                )
                hyperliquid_risk_checklist = result.checklist()
                if not result.is_ok:
                    risk_error = result.reason
            finally:
                await client.close()

    dry_run = not (settings.trading_enabled and venue_trading_enabled and venue_execution_enabled)
    status = "DRY_RUN" if dry_run else "PENDING_EXECUTION"
    if risk_error:
        status = "BLOCKED"
        dry_run = True
        db.add(
            RiskEvent(
                severity="critical",
                event_type="manual_order_preflight_failed",
                symbol=symbol,
                leader_address="manual",
                message=risk_error,
                metadata_json={
                    "position_side": position_side.value,
                    "action": action.value,
                    "close_intent": close_intent,
                },
            )
        )

    allocation_query = select(LeaderPositionAllocationRecord).where(
        LeaderPositionAllocationRecord.execution_venue == execution_venue.value,
        LeaderPositionAllocationRecord.status.notin_(["CLOSED"]),
    )
    if execution_venue == ExecutionVenue.BINANCE:
        allocation_query = allocation_query.where(LeaderPositionAllocationRecord.binance_symbol == symbol)
    else:
        canonical_key = str(canonical or "").upper()
        source_coin_key = str(source_coin or "").upper()
        allocation_query = allocation_query.where(
            (func.upper(LeaderPositionAllocationRecord.canonical_coin) == canonical_key)
            | (func.upper(LeaderPositionAllocationRecord.hyperliquid_coin) == source_coin_key)
        )
    active_allocations = (await db.execute(allocation_query)).scalars().all()
    if active_allocations:
        db.add(
            RiskEvent(
                severity="warning",
                event_type="ALLOCATION_MISMATCH",
                symbol=symbol,
                leader_address="manual",
                message="manual order can desync leader allocation ledger; reconcile before opening new copied positions",
                metadata_json={
                    "source": "MANUAL",
                    "position_side": position_side.value,
                    "action": action.value,
                    "allocation_count": len(active_allocations),
                },
            )
        )

    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    cloid = (
        build_hyperliquid_cloid(
            leader_address="manual",
            coin=source_coin,
            dex=dex,
            side=position_side.value,
            action=action.value,
            source_fill_id=f"manual-{timestamp_ms}",
            timestamp_ms=timestamp_ms,
        )
        if execution_venue == ExecutionVenue.HYPERLIQUID
        else None
    )
    live_disabled_reason = (
        "TRADING_ENABLED=false"
        if not settings.trading_enabled
        else f"{execution_venue.value}_TRADING_ENABLED=false"
        if not venue_trading_enabled
        else f"ENABLE_{execution_venue.value}_EXECUTION=false"
        if not venue_execution_enabled
        else None
    )
    order = ExecutionOrder(
        leader_address="manual",
        source_type="MANUAL",
        source_fill_id=None,
        source_coin=source_coin,
        execution_venue=execution_venue.value,
        dex=dex,
        canonical_coin=canonical,
        raw_coin_from_fill=payload.symbol,
        venue_symbol=symbol if execution_venue == ExecutionVenue.BINANCE else canonical,
        hyperliquid_coin=source_coin,
        binance_symbol=symbol if execution_venue == ExecutionVenue.BINANCE else None,
        side=hedge_order.side,
        position_side=position_side.value,
        order_action=action.value,
        order_type=order_type,
        cloid=cloid,
        quantity=payload.quantity or Decimal("0"),
        price=payload.price,
        estimated_price=payload.price,
        notional=payload.notional,
        order_id=None,
        status=status,
        dry_run=dry_run,
        reduce_only=close_intent,
        is_close_intent=close_intent,
        error_message=risk_error if risk_error else live_disabled_reason,
        raw_response=None,
        sizing_mode="MANUAL",
        target_notional=payload.notional,
        delta_notional=payload.notional,
        pre_trade_checklist={
            "trading_enabled": settings.trading_enabled,
            "venue_trading_enabled": venue_trading_enabled,
            "execution_venue_enabled": venue_execution_enabled,
            "kill_switch_off": kill_switch_off,
            "margin_type_isolated": risk_error is None and settings.trading_enabled and venue_trading_enabled,
            "leverage_is_10": risk_error is None and settings.trading_enabled and venue_trading_enabled,
            "position_mode_hedge": execution_venue == ExecutionVenue.HYPERLIQUID
            or (risk_error is None and settings.trading_enabled and venue_trading_enabled),
            "reduce_only_correct": True,
            "binance_reduce_only_omitted": execution_venue == ExecutionVenue.BINANCE,
            "hyperliquid_reduce_only_set": execution_venue == ExecutionVenue.HYPERLIQUID and close_intent,
            **(hyperliquid_risk_checklist or {}),
        },
    )
    db.add(order)
    await db.commit()
    await db.refresh(order)
    return order
