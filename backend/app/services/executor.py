from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Protocol

from app.services.calculator import SIZING_MODE_ACCOUNT_RATIO, OrderSide
from app.services.auto_copy import (
    AUTO_COPY_ORDER_TYPE,
    build_auto_copy_new_client_order_id,
    calculate_latency_fields,
    extract_market_fill,
)
from app.services.risk import RiskConfig, check_risk
from app.services.risk_settings import RiskSettingsResult
from app.services.sizing_guard import SizingGuardError, assert_sizing_mode_account_ratio
from app.services.symbol_mapper import TradingRule, decimal_to_exchange_str, quantity_from_notional


@dataclass(frozen=True)
class CopyOrderIntent:
    leader_address: str
    source_fill_id: str
    source_coin: str
    binance_symbol: str
    side: OrderSide
    notional: Decimal
    price: Decimal
    reduce_only: bool = False
    position_side: str | None = None
    action: str | None = None
    leader_id: int | None = None
    allocation_id: int | None = None
    source_type: str = "AUTO_COPY"
    event_time_ms: int | None = None
    event_received_at: datetime | None = None
    use_market_order: bool = True
    sizing_mode: str = SIZING_MODE_ACCOUNT_RATIO
    leader_account_value: Decimal | None = None
    leader_account_value_source: str | None = None
    leader_account_abstraction_mode: str | None = None
    leader_position_notional: Decimal | None = None
    follower_account_value: Decimal | None = None
    follower_account_value_source: str | None = None
    follower_account_abstraction_mode: str | None = None
    leader_position_ratio: Decimal | None = None
    copy_multiplier: Decimal | None = None
    target_notional: Decimal | None = None
    delta_notional: Decimal | None = None
    dex: str = ""
    canonical_coin: str | None = None
    raw_coin_from_fill: str | None = None
    asset_id: int | None = None


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    source_fill_id: str
    binance_symbol: str
    side: str
    quantity: Decimal
    notional: Decimal
    dry_run: bool
    reason: str | None = None
    exchange_order_id: str | None = None
    raw_response: dict[str, Any] | None = None
    checklist: dict[str, bool] | None = None
    source_type: str = "AUTO_COPY"
    position_side: str | None = None
    order_action: str | None = None
    order_type: str = AUTO_COPY_ORDER_TYPE
    client_order_id: str | None = None
    executed_qty: Decimal | None = None
    avg_fill_price: Decimal | None = None
    estimated_price: Decimal | None = None
    slippage_bps: Decimal | None = None
    hyperliquid_event_time: datetime | None = None
    event_received_at: datetime | None = None
    decision_started_at: datetime | None = None
    binance_order_submit_at: datetime | None = None
    binance_order_ack_at: datetime | None = None
    order_finalized_at: datetime | None = None
    event_to_receive_ms: int | None = None
    receive_to_submit_ms: int | None = None
    submit_to_ack_ms: int | None = None
    event_to_ack_ms: int | None = None
    event_to_final_ms: int | None = None
    sizing_mode: str | None = None
    leader_account_value: Decimal | None = None
    leader_account_value_source: str | None = None
    leader_account_abstraction_mode: str | None = None
    leader_position_notional: Decimal | None = None
    follower_account_value: Decimal | None = None
    follower_account_value_source: str | None = None
    follower_account_abstraction_mode: str | None = None
    leader_position_ratio: Decimal | None = None
    copy_multiplier: Decimal | None = None
    target_notional: Decimal | None = None
    delta_notional: Decimal | None = None
    dex: str = ""
    canonical_coin: str | None = None
    raw_coin_from_fill: str | None = None
    asset_id: int | None = None


class BinanceOrderClient(Protocol):
    async def place_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: str,
        reduce_only: bool,
        position_side: str,
        new_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        ...


class RiskSettingsService(Protocol):
    async def ensure_symbol_risk_settings(
        self, symbol: str, *, reduce_only: bool
    ) -> RiskSettingsResult:
        ...


class ExecutionStore(Protocol):
    async def seen_source_fill(self, source_fill_id: str) -> bool:
        ...

    async def save_execution(self, result: ExecutionResult) -> None:
        ...

    async def save_risk_event(
        self, *, symbol: str, leader_address: str, message: str, metadata: dict[str, Any]
    ) -> None:
        ...


class InMemoryExecutionStore:
    def __init__(self) -> None:
        self.results: list[ExecutionResult] = []
        self.risk_events: list[dict[str, Any]] = []
        self._seen: set[str] = set()

    async def seen_source_fill(self, source_fill_id: str) -> bool:
        return source_fill_id in self._seen

    async def save_execution(self, result: ExecutionResult) -> None:
        if result.client_order_id:
            for index, existing in enumerate(self.results):
                if existing.client_order_id == result.client_order_id:
                    self.results[index] = result
                    break
            else:
                self.results.append(result)
        else:
            self.results.append(result)
        if result.status not in {"DUPLICATE", "REJECTED", "BLOCKED"}:
            self._seen.add(result.source_fill_id)

    async def save_risk_event(
        self, *, symbol: str, leader_address: str, message: str, metadata: dict[str, Any]
    ) -> None:
        self.risk_events.append(
            {
                "symbol": symbol,
                "leader_address": leader_address,
                "message": message,
                "metadata": metadata,
            }
        )


class CopyExecutor:
    def __init__(
        self,
        *,
        store: ExecutionStore,
        client: BinanceOrderClient | None = None,
        risk_settings: RiskSettingsService | None = None,
        position_side: str = "LONG",
    ) -> None:
        self._store = store
        self._client = client
        self._risk_settings = risk_settings
        self._position_side = position_side

    def _base_checklist(
        self,
        *,
        risk_config: RiskConfig,
        duplicate_check_passed: bool,
        quantity_valid: bool,
        min_notional_passed: bool,
        reduce_only_correct: bool,
        risk_settings_result: RiskSettingsResult | None = None,
    ) -> dict[str, bool]:
        return {
            "trading_enabled": risk_config.trading_enabled,
            "kill_switch_off": not risk_config.kill_switch_active,
            "leader_enabled": risk_config.leader_enabled,
            "symbol_enabled": risk_config.symbol_enabled,
            "symbol_mapping_valid": True,
            "follower_account_state_fresh": risk_config.follower_account_state_fresh,
            "leader_account_state_fresh": risk_config.leader_account_state_fresh,
            "leader_account_value_positive": (
                risk_config.leader_account_value is not None
                and risk_config.leader_account_value > 0
            ),
            "follower_account_value_positive": (
                risk_config.follower_account_value is not None
                and risk_config.follower_account_value > 0
            ),
            "follower_withdrawable_sufficient": risk_config.follower_withdrawable_sufficient,
            "leader_position_exists_or_close_reduce": risk_config.leader_position_exists
            or risk_config.is_close_intent,
            "allowed_coin_check": risk_config.allowed_coin,
            "venue_route_check": risk_config.venue_route_ok,
            "binance_position_fresh": risk_settings_result is not None
            and risk_settings_result.is_ok,
            "margin_type_isolated": risk_settings_result is not None
            and risk_settings_result.margin_type == "ISOLATED",
            "leverage_is_10": risk_settings_result is not None
            and risk_settings_result.leverage == 10,
            "position_mode_hedge": risk_settings_result is not None
            and risk_settings_result.position_mode == "HEDGE",
            "quantity_valid": quantity_valid,
            "min_notional_passed": min_notional_passed,
            "max_notional_passed": True,
            "reduce_only_correct": reduce_only_correct,
            "duplicate_check_passed": duplicate_check_passed,
            "rate_limit_ok": True,
        }

    async def execute(
        self,
        intent: CopyOrderIntent,
        *,
        rule: TradingRule,
        risk_config: RiskConfig,
    ) -> ExecutionResult:
        decision_started_at = datetime.now(timezone.utc)
        event_received_at = intent.event_received_at or decision_started_at
        hyperliquid_event_time = (
            datetime.fromtimestamp(intent.event_time_ms / 1000, timezone.utc)
            if intent.event_time_ms is not None
            else None
        )
        sizing_kwargs = self._sizing_kwargs(intent)
        if await self._store.seen_source_fill(intent.source_fill_id):
            result = ExecutionResult(
                status="DUPLICATE",
                source_fill_id=intent.source_fill_id,
                binance_symbol=intent.binance_symbol,
                side=intent.side.value,
                quantity=Decimal("0"),
                notional=intent.notional,
                dry_run=True,
                reason="source fill already processed",
                checklist=self._base_checklist(
                    risk_config=risk_config,
                    duplicate_check_passed=False,
                    quantity_valid=False,
                    min_notional_passed=False,
                    reduce_only_correct=True,
                ),
                source_type=intent.source_type,
                hyperliquid_event_time=hyperliquid_event_time,
                event_received_at=event_received_at,
                decision_started_at=decision_started_at,
                **sizing_kwargs,
            )
            await self._store.save_execution(result)
            return result

        qty, qty_error = quantity_from_notional(intent.notional, intent.price, rule)
        if qty_error:
            result = ExecutionResult(
                status="SKIPPED",
                source_fill_id=intent.source_fill_id,
                binance_symbol=intent.binance_symbol,
                side=intent.side.value,
                quantity=qty,
                notional=intent.notional,
                dry_run=True,
                reason=qty_error,
                checklist=self._base_checklist(
                    risk_config=risk_config,
                    duplicate_check_passed=True,
                    quantity_valid=False,
                    min_notional_passed=False,
                    reduce_only_correct=True,
                ),
                source_type=intent.source_type,
                hyperliquid_event_time=hyperliquid_event_time,
                event_received_at=event_received_at,
                decision_started_at=decision_started_at,
                **sizing_kwargs,
            )
            await self._store.save_execution(result)
            return result

        risk_settings_result: RiskSettingsResult | None = None
        if self._risk_settings is not None:
            risk_settings_result = await self._risk_settings.ensure_symbol_risk_settings(
                intent.binance_symbol, reduce_only=intent.reduce_only
            )
            position_mode_blocks_close = risk_settings_result.position_mode != "HEDGE"
            if not risk_settings_result.is_ok and (
                not intent.reduce_only or position_mode_blocks_close
            ):
                await self._store.save_risk_event(
                    symbol=intent.binance_symbol,
                    leader_address=intent.leader_address,
                    message=risk_settings_result.reason or "risk settings check failed",
                    metadata={"source_fill_id": intent.source_fill_id},
                )
                result = ExecutionResult(
                    status="BLOCKED",
                    source_fill_id=intent.source_fill_id,
                    binance_symbol=intent.binance_symbol,
                    side=intent.side.value,
                    quantity=qty,
                    notional=intent.notional,
                    dry_run=True,
                    reason=risk_settings_result.reason,
                    checklist=self._base_checklist(
                        risk_config=risk_config,
                        duplicate_check_passed=True,
                        quantity_valid=True,
                        min_notional_passed=True,
                        reduce_only_correct=True,
                        risk_settings_result=risk_settings_result,
                    ),
                    source_type=intent.source_type,
                    hyperliquid_event_time=hyperliquid_event_time,
                    event_received_at=event_received_at,
                    decision_started_at=decision_started_at,
                    **sizing_kwargs,
                )
                await self._store.save_execution(result)
                return result

        decision_risk_config = replace(risk_config, is_close_intent=intent.reduce_only)
        decision = check_risk(
            decision_risk_config,
            symbol=intent.binance_symbol,
            proposed_notional=intent.notional,
        )
        if not decision.allowed:
            await self._store.save_risk_event(
                symbol=intent.binance_symbol,
                leader_address=intent.leader_address,
                message="; ".join(decision.reasons),
                metadata={"source_fill_id": intent.source_fill_id},
            )
            result = ExecutionResult(
                status="REJECTED",
                source_fill_id=intent.source_fill_id,
                binance_symbol=intent.binance_symbol,
                side=intent.side.value,
                quantity=qty,
                notional=intent.notional,
                dry_run=True,
                reason="; ".join(decision.reasons),
                checklist=self._base_checklist(
                    risk_config=risk_config,
                    duplicate_check_passed=True,
                    quantity_valid=True,
                    min_notional_passed=True,
                    reduce_only_correct=True,
                    risk_settings_result=risk_settings_result,
                ),
                source_type=intent.source_type,
                hyperliquid_event_time=hyperliquid_event_time,
                event_received_at=event_received_at,
                decision_started_at=decision_started_at,
                **sizing_kwargs,
            )
            await self._store.save_execution(result)
            return result

        if not decision.live_allowed:
            result = ExecutionResult(
                status="DRY_RUN",
                source_fill_id=intent.source_fill_id,
                binance_symbol=intent.binance_symbol,
                side=intent.side.value,
                quantity=qty,
                notional=intent.notional,
                dry_run=True,
                reason="live trading disabled or frontend confirmation missing",
                checklist=self._base_checklist(
                    risk_config=risk_config,
                    duplicate_check_passed=True,
                    quantity_valid=True,
                    min_notional_passed=True,
                    reduce_only_correct=True,
                    risk_settings_result=risk_settings_result,
                ),
                source_type=intent.source_type,
                hyperliquid_event_time=hyperliquid_event_time,
                event_received_at=event_received_at,
                decision_started_at=decision_started_at,
                **sizing_kwargs,
            )
            await self._store.save_execution(result)
            return result

        if self._client is None:
            result = ExecutionResult(
                status="DRY_RUN",
                source_fill_id=intent.source_fill_id,
                binance_symbol=intent.binance_symbol,
                side=intent.side.value,
                quantity=qty,
                notional=intent.notional,
                dry_run=True,
                reason="live trading requested without Binance client",
                checklist=self._base_checklist(
                    risk_config=risk_config,
                    duplicate_check_passed=True,
                    quantity_valid=True,
                    min_notional_passed=True,
                    reduce_only_correct=True,
                    risk_settings_result=risk_settings_result,
                ),
                source_type=intent.source_type,
                hyperliquid_event_time=hyperliquid_event_time,
                event_received_at=event_received_at,
                decision_started_at=decision_started_at,
                **sizing_kwargs,
            )
            await self._store.save_execution(result)
            return result

        try:
            assert_sizing_mode_account_ratio(intent)
        except SizingGuardError as exc:
            await self._store.save_risk_event(
                symbol=intent.binance_symbol,
                leader_address=intent.leader_address,
                message=str(exc),
                metadata={"source_fill_id": intent.source_fill_id},
            )
            result = ExecutionResult(
                status="BLOCKED",
                source_fill_id=intent.source_fill_id,
                binance_symbol=intent.binance_symbol,
                side=intent.side.value,
                quantity=qty,
                notional=intent.notional,
                dry_run=True,
                reason=str(exc),
                checklist=self._base_checklist(
                    risk_config=risk_config,
                    duplicate_check_passed=True,
                    quantity_valid=True,
                    min_notional_passed=True,
                    reduce_only_correct=True,
                    risk_settings_result=risk_settings_result,
                ),
                source_type=intent.source_type,
                hyperliquid_event_time=hyperliquid_event_time,
                event_received_at=event_received_at,
                decision_started_at=decision_started_at,
                **sizing_kwargs,
            )
            await self._store.save_execution(result)
            return result

        position_side = self._resolve_position_side(intent)
        action = intent.action or ("CLOSE_OR_REDUCE" if intent.reduce_only else "OPEN_OR_INCREASE")
        client_order_id = build_auto_copy_new_client_order_id(
            leader_address=intent.leader_address,
            symbol=intent.binance_symbol,
            position_side=position_side,
            action=action,
            source_fill_id=intent.source_fill_id,
            timestamp_ms=int(decision_started_at.timestamp() * 1000),
        )
        pending = ExecutionResult(
            status="PENDING_SUBMIT",
            source_fill_id=intent.source_fill_id,
            binance_symbol=intent.binance_symbol,
            side=intent.side.value,
            quantity=qty,
            notional=intent.notional,
            dry_run=False,
            checklist=self._base_checklist(
                risk_config=risk_config,
                duplicate_check_passed=True,
                quantity_valid=True,
                min_notional_passed=True,
                reduce_only_correct=True,
                risk_settings_result=risk_settings_result,
            ),
            source_type=intent.source_type,
            position_side=position_side,
            order_action=action,
            order_type=AUTO_COPY_ORDER_TYPE,
            client_order_id=client_order_id,
            estimated_price=intent.price,
            hyperliquid_event_time=hyperliquid_event_time,
            event_received_at=event_received_at,
            decision_started_at=decision_started_at,
            **sizing_kwargs,
        )
        await self._store.save_execution(pending)

        submit_at = datetime.now(timezone.utc)
        try:
            response = await self._client.place_order(
                symbol=intent.binance_symbol,
                side=intent.side.value,
                order_type=AUTO_COPY_ORDER_TYPE,
                quantity=decimal_to_exchange_str(qty),
                reduce_only=False,
                position_side=position_side,
                new_client_order_id=client_order_id,
            )
            ack_at = datetime.now(timezone.utc)
        except Exception as exc:
            ack_at = datetime.now(timezone.utc)
            latencies = calculate_latency_fields(
                hyperliquid_event_time=hyperliquid_event_time,
                event_received_at=event_received_at,
                binance_order_submit_at=submit_at,
                binance_order_ack_at=ack_at,
                order_finalized_at=None,
            )
            result = ExecutionResult(
                status="UNKNOWN",
                source_fill_id=intent.source_fill_id,
                binance_symbol=intent.binance_symbol,
                side=intent.side.value,
                quantity=qty,
                notional=intent.notional,
                dry_run=False,
                reason=f"Binance order status unknown: {exc}",
                checklist=pending.checklist,
                source_type=intent.source_type,
                position_side=position_side,
                order_action=action,
                order_type=AUTO_COPY_ORDER_TYPE,
                client_order_id=client_order_id,
                estimated_price=intent.price,
                hyperliquid_event_time=hyperliquid_event_time,
                event_received_at=event_received_at,
                decision_started_at=decision_started_at,
                binance_order_submit_at=submit_at,
                binance_order_ack_at=ack_at,
                **sizing_kwargs,
                **latencies,
            )
            await self._store.save_execution(result)
            return result

        market_fill = extract_market_fill(response, estimated_price=intent.price)
        response_status = str(response.get("status", "SUBMITTED"))
        finalized_at = (
            datetime.now(timezone.utc)
            if response_status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}
            else None
        )
        latencies = calculate_latency_fields(
            hyperliquid_event_time=hyperliquid_event_time,
            event_received_at=event_received_at,
            binance_order_submit_at=submit_at,
            binance_order_ack_at=ack_at,
            order_finalized_at=finalized_at,
        )
        result = ExecutionResult(
            status=response_status,
            source_fill_id=intent.source_fill_id,
            binance_symbol=intent.binance_symbol,
            side=intent.side.value,
            quantity=qty,
            notional=intent.notional,
            dry_run=False,
            exchange_order_id=str(response.get("orderId")) if response.get("orderId") else None,
            raw_response=response,
            checklist=self._base_checklist(
                risk_config=risk_config,
                duplicate_check_passed=True,
                quantity_valid=True,
                min_notional_passed=True,
                reduce_only_correct=True,
                risk_settings_result=risk_settings_result,
            ),
            source_type=intent.source_type,
            position_side=position_side,
            order_action=action,
            order_type=AUTO_COPY_ORDER_TYPE,
            client_order_id=client_order_id,
            executed_qty=market_fill.executed_qty,
            avg_fill_price=market_fill.avg_fill_price,
            estimated_price=intent.price,
            slippage_bps=market_fill.slippage_bps,
            hyperliquid_event_time=hyperliquid_event_time,
            event_received_at=event_received_at,
            decision_started_at=decision_started_at,
            binance_order_submit_at=submit_at,
            binance_order_ack_at=ack_at,
            order_finalized_at=finalized_at,
            **sizing_kwargs,
            **latencies,
        )
        await self._store.save_execution(result)
        return result

    def _resolve_position_side(self, intent: CopyOrderIntent) -> str:
        if intent.position_side:
            position_side = intent.position_side.upper()
        elif intent.reduce_only:
            position_side = "LONG" if intent.side == OrderSide.SELL else "SHORT"
        else:
            position_side = "LONG" if intent.side == OrderSide.BUY else "SHORT"
        if position_side not in {"LONG", "SHORT"}:
            position_side = self._position_side.upper()
        if position_side not in {"LONG", "SHORT"}:
            raise ValueError("Hedge Mode orders require positionSide LONG or SHORT")
        return position_side

    def _sizing_kwargs(self, intent: CopyOrderIntent) -> dict[str, Any]:
        return {
            "sizing_mode": intent.sizing_mode,
            "leader_account_value": intent.leader_account_value,
            "leader_account_value_source": intent.leader_account_value_source,
            "leader_account_abstraction_mode": intent.leader_account_abstraction_mode,
            "leader_position_notional": intent.leader_position_notional,
            "follower_account_value": intent.follower_account_value,
            "follower_account_value_source": intent.follower_account_value_source,
            "follower_account_abstraction_mode": intent.follower_account_abstraction_mode,
            "leader_position_ratio": intent.leader_position_ratio,
            "copy_multiplier": intent.copy_multiplier,
            "target_notional": intent.target_notional,
            "delta_notional": intent.delta_notional,
            "dex": intent.dex,
            "canonical_coin": intent.canonical_coin,
            "raw_coin_from_fill": intent.raw_coin_from_fill,
            "asset_id": intent.asset_id,
        }
