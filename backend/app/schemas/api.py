from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    email: str
    password: str
    totp_code: str | None = None


class TotpVerifyRequest(BaseModel):
    code: str = Field(min_length=6, max_length=8)


class LeaderCreate(BaseModel):
    leader_address: str
    enabled: bool = True
    copy_multiplier: Decimal = Decimal("0.1")
    fixed_account_value: Decimal | None = None
    allowed_symbols: list[str] | None = None
    blocked_symbols: list[str] = Field(default_factory=list)
    max_notional_per_trade: Decimal | None = None
    max_total_notional: Decimal | None = None
    max_leverage: int = 1
    slippage_bps: int = 20
    preferred_venue: str = "HYPERLIQUID"
    fallback_venue: str = "NONE"
    enabled_venues: list[str] = Field(default_factory=lambda: ["HYPERLIQUID"])
    hyperliquid_account_id: str | None = None
    hyperliquid_vault_address: str | None = None


class LeaderPatch(BaseModel):
    enabled: bool | None = None
    copy_multiplier: Decimal | None = None
    fixed_account_value: Decimal | None = None
    allowed_symbols: list[str] | None = None
    blocked_symbols: list[str] | None = None
    max_notional_per_trade: Decimal | None = None
    max_total_notional: Decimal | None = None
    max_leverage: int | None = None
    slippage_bps: int | None = None
    frontend_live_confirmed: bool | None = None
    preferred_venue: str | None = None
    fallback_venue: str | None = None
    enabled_venues: list[str] | None = None
    hyperliquid_account_id: str | None = None
    hyperliquid_vault_address: str | None = None


class LeaderDelete(BaseModel):
    delete_reason: str | None = None


class LeaderReplace(BaseModel):
    leader_address: str
    copy_multiplier: Decimal | None = None
    fixed_account_value: Decimal
    allowed_symbols: list[str] | None = None
    blocked_symbols: list[str] | None = None
    max_notional_per_trade: Decimal | None = None
    max_total_notional: Decimal | None = None
    max_leverage: int | None = None
    slippage_bps: int | None = None
    preferred_venue: str | None = None
    fallback_venue: str | None = None
    enabled_venues: list[str] | None = None
    hyperliquid_account_id: str | None = None
    hyperliquid_vault_address: str | None = None
    allow_unmanaged_existing_allocations: bool = False


class SymbolMappingPatch(BaseModel):
    binance_symbol: str | None = None
    enabled: bool | None = None


class VenueMappingPatch(BaseModel):
    enabled: bool | None = None
    is_default: bool | None = None
    mapping_status: str | None = None
    reason: str | None = None


class ManualOrderRequest(BaseModel):
    symbol: str
    execution_venue: str = "BINANCE"
    side: str | None = None
    position_side: str = "LONG"
    action: str = "OPEN_OR_INCREASE"
    order_type: str = "MARKET"
    quantity: Decimal | None = None
    notional: Decimal | None = None
    price: Decimal | None = None
    # Deprecated: kept for older UI/API clients. Hedge Mode never sends reduceOnly to Binance.
    reduce_only: bool = False
    confirmation: str


class RiskPatch(BaseModel):
    kill_switch: bool | None = None
    global_max_daily_loss: Decimal | None = None
    global_max_total_notional: Decimal | None = None
