from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal


def _d(value: Decimal | str | int | float | None, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(frozen=True)
class RiskConfig:
    trading_enabled: bool = False
    frontend_live_confirmed: bool = False
    leader_enabled: bool = True
    symbol_enabled: bool = True
    max_notional_per_trade: Decimal | None = None
    leader_max_total_notional: Decimal | None = None
    current_leader_total_notional: Decimal = Decimal("0")
    global_max_total_notional: Decimal | None = None
    current_total_notional: Decimal = Decimal("0")
    max_position_per_symbol: Decimal | None = None
    current_symbol_notional: Decimal = Decimal("0")
    max_daily_loss: Decimal | None = None
    current_daily_loss: Decimal = Decimal("0")
    cooldown_seconds: int = 0
    last_execution_at: datetime | None = None
    circuit_breaker_open: bool = False
    kill_switch_active: bool = False
    is_close_intent: bool = False
    follower_account_state_fresh: bool = True
    leader_account_state_fresh: bool = True
    follower_account_value: Decimal | None = Decimal("1")
    leader_account_value: Decimal | None = Decimal("1")
    follower_withdrawable_sufficient: bool = True
    leader_position_exists: bool = True
    allowed_coin: bool = True
    venue_route_ok: bool = True
    extra_blocks: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    live_allowed: bool
    reasons: tuple[str, ...]

    @property
    def dry_run_only(self) -> bool:
        return self.allowed and not self.live_allowed


def check_risk(
    config: RiskConfig,
    *,
    symbol: str,
    proposed_notional: Decimal | str | int | float,
    now: datetime | None = None,
) -> RiskDecision:
    reasons: list[str] = []
    now = now or datetime.now(timezone.utc)
    notional = _d(proposed_notional)

    if config.circuit_breaker_open:
        reasons.append("circuit breaker is open")
    if config.kill_switch_active and not config.is_close_intent:
        reasons.append("kill switch is active")
    if not config.follower_account_state_fresh and not config.is_close_intent:
        reasons.append("follower account state is stale")
    if not config.leader_account_state_fresh and not config.is_close_intent:
        reasons.append("leader account state is stale")
    if _d(config.follower_account_value) <= 0 and not config.is_close_intent:
        reasons.append("follower accountValue must be positive")
    if _d(config.leader_account_value) <= 0 and not config.is_close_intent:
        reasons.append("leader accountValue must be positive")
    if not config.follower_withdrawable_sufficient and not config.is_close_intent:
        reasons.append("follower withdrawable is insufficient")
    if not config.leader_position_exists and not config.is_close_intent:
        reasons.append("leader position does not exist")
    if not config.allowed_coin and not config.is_close_intent:
        reasons.append(f"{symbol} is not allowed for this leader")
    if not config.venue_route_ok:
        reasons.append("venue route is not available")
    if not config.leader_enabled:
        reasons.append("leader is disabled")
    if not config.symbol_enabled:
        reasons.append(f"{symbol} is disabled")
    if notional <= 0:
        reasons.append("notional must be positive")
    if config.max_notional_per_trade is not None and notional > _d(
        config.max_notional_per_trade
    ):
        reasons.append("max_notional_per_trade exceeded")
    if config.global_max_total_notional is not None and (
        _d(config.current_total_notional) + notional
    ) > _d(config.global_max_total_notional):
        reasons.append("global_max_total_notional exceeded")
    if config.leader_max_total_notional is not None and (
        _d(config.current_leader_total_notional) + notional
    ) > _d(config.leader_max_total_notional):
        reasons.append("leader_max_total_notional exceeded")
    if config.max_position_per_symbol is not None and (
        _d(config.current_symbol_notional) + notional
    ) > _d(config.max_position_per_symbol):
        reasons.append("max_position_per_symbol exceeded")
    if config.max_daily_loss is not None and abs(_d(config.current_daily_loss)) >= abs(
        _d(config.max_daily_loss)
    ):
        reasons.append("max_daily_loss exceeded")
    if config.last_execution_at and config.cooldown_seconds > 0:
        elapsed = (now - config.last_execution_at).total_seconds()
        if elapsed < config.cooldown_seconds:
            reasons.append("cooldown active")
    reasons.extend(config.extra_blocks)

    allowed = len(reasons) == 0
    live_allowed = (
        allowed
        and config.trading_enabled
        and config.frontend_live_confirmed
        and (not config.kill_switch_active or config.is_close_intent)
    )
    return RiskDecision(
        allowed=allowed,
        live_allowed=live_allowed,
        reasons=tuple(reasons),
    )
