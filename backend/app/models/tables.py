from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class User(Base, TimestampMixin):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(Text)
    totp_secret_encrypted: Mapped[str | None] = mapped_column(Text)
    totp_enabled: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_admin: Mapped[bool] = mapped_column(Boolean, default=True)
    failed_login_count: Mapped[int] = mapped_column(Integer, default=0)
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    sessions: Mapped[list[Session]] = relationship(back_populates="user")


class Session(Base, TimestampMixin):
    __tablename__ = "sessions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ip_address: Mapped[str | None] = mapped_column(String(64))
    user_agent: Mapped[str | None] = mapped_column(Text)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    user: Mapped[User] = relationship(back_populates="sessions")


class LeaderConfig(Base, TimestampMixin):
    __tablename__ = "leader_configs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    delete_reason: Mapped[str | None] = mapped_column(Text)
    performance_started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )
    leader_address: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    copy_multiplier: Mapped[Decimal] = mapped_column(Numeric(20, 8), default=Decimal("0.1"))
    fixed_account_value: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    allowed_symbols: Mapped[list[str] | None] = mapped_column(JSON, default=None, nullable=True)
    blocked_symbols: Mapped[list[str]] = mapped_column(JSON, default=list)
    max_notional_per_trade: Mapped[Decimal | None] = mapped_column(Numeric(20, 8))
    max_total_notional: Mapped[Decimal | None] = mapped_column(Numeric(20, 8))
    max_leverage: Mapped[int] = mapped_column(Integer, default=1)
    slippage_bps: Mapped[int] = mapped_column(Integer, default=20)
    use_market_order: Mapped[bool] = mapped_column(Boolean, default=True)
    reduce_only_on_reduce: Mapped[bool] = mapped_column(Boolean, default=True)
    copy_mode: Mapped[str] = mapped_column(String(32), default="fill_delta_and_reconcile")
    frontend_live_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    preferred_venue: Mapped[str] = mapped_column(String(16), default="HYPERLIQUID")
    fallback_venue: Mapped[str] = mapped_column(String(16), default="NONE")
    enabled_venues: Mapped[list[str]] = mapped_column(JSON, default=lambda: ["HYPERLIQUID"])
    hyperliquid_account_id: Mapped[str | None] = mapped_column(String(64))
    hyperliquid_vault_address: Mapped[str | None] = mapped_column(String(64))

    symbol_configs: Mapped[list[LeaderSymbolConfig]] = relationship(back_populates="leader")


class LeaderSymbolConfig(Base, TimestampMixin):
    __tablename__ = "leader_symbol_configs"
    __table_args__ = (UniqueConstraint("leader_id", "coin", name="uq_leader_symbol_coin"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    leader_id: Mapped[int] = mapped_column(ForeignKey("leader_configs.id", ondelete="CASCADE"))
    coin: Mapped[str] = mapped_column(String(80), index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    max_notional_per_trade: Mapped[Decimal | None] = mapped_column(Numeric(20, 8))
    max_position_notional: Mapped[Decimal | None] = mapped_column(Numeric(20, 8))
    preferred_venue: Mapped[str | None] = mapped_column(String(16))
    fallback_venue: Mapped[str | None] = mapped_column(String(16))
    enabled_venues: Mapped[list[str] | None] = mapped_column(JSON)
    hyperliquid_account_id: Mapped[str | None] = mapped_column(String(64))
    hyperliquid_vault_address: Mapped[str | None] = mapped_column(String(64))

    leader: Mapped[LeaderConfig] = relationship(back_populates="symbol_configs")


class SymbolMapping(Base, TimestampMixin):
    __tablename__ = "symbol_mappings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    hyperliquid_coin: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    binance_symbol: Mapped[str] = mapped_column(String(32), index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    rule_snapshot: Mapped[dict | None] = mapped_column(JSON)
    last_validation_error: Mapped[str | None] = mapped_column(Text)


class VenueMapping(Base, TimestampMixin):
    __tablename__ = "venue_mappings"
    __table_args__ = (
        UniqueConstraint(
            "hyperliquid_coin",
            "execution_venue",
            "venue_symbol",
            name="uq_venue_mapping_coin_venue_symbol",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    hyperliquid_coin: Mapped[str] = mapped_column(String(80), index=True)
    execution_venue: Mapped[str] = mapped_column(String(16), index=True)
    venue_symbol: Mapped[str] = mapped_column(String(80), index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    mapping_status: Mapped[str] = mapped_column(String(32), default="UNKNOWN", index=True)
    reason: Mapped[str | None] = mapped_column(Text)


class SourceFill(Base, TimestampMixin):
    __tablename__ = "source_fills"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_fill_id: Mapped[str] = mapped_column(String(160), unique=True, index=True)
    execution_account: Mapped[str] = mapped_column(String(64), default="", server_default="", index=True)
    leader_address: Mapped[str] = mapped_column(String(64), index=True)
    coin: Mapped[str] = mapped_column(String(80), index=True)
    side: Mapped[str] = mapped_column(String(16))
    price: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    size: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    source_time_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    canonical_coin: Mapped[str | None] = mapped_column(String(160), index=True)
    raw_coin: Mapped[str | None] = mapped_column(String(160))
    asset_id: Mapped[int | None] = mapped_column(BigInteger)
    ws_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw_fill: Mapped[dict] = mapped_column(JSON)
    is_snapshot: Mapped[bool] = mapped_column(Boolean, default=False)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    last_processing_error: Mapped[str | None] = mapped_column(Text)


class SourceFillOutcome(Base, TimestampMixin):
    __tablename__ = "source_fill_outcomes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_fill_id: Mapped[str] = mapped_column(
        ForeignKey("source_fills.source_fill_id", ondelete="CASCADE"),
        unique=True,
        index=True,
    )
    execution_order_id: Mapped[int | None] = mapped_column(
        ForeignKey("execution_orders.id", ondelete="SET NULL"),
        index=True,
    )
    disposition: Mapped[str] = mapped_column(String(40), index=True)
    reason: Mapped[str | None] = mapped_column(Text)


class LeaderFillCursor(Base, TimestampMixin):
    __tablename__ = "leader_fill_cursors"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    leader_address: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    last_fill_time_ms: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")
    last_fill_tid: Mapped[int | None] = mapped_column(BigInteger)
    backfilled_through_ms: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")


class SignerNonceState(Base, TimestampMixin):
    __tablename__ = "signer_nonce_states"

    signer_scope: Mapped[str] = mapped_column(String(96), primary_key=True)
    last_nonce: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")


class FollowerMarketGuard(Base, TimestampMixin):
    __tablename__ = "follower_market_guards"
    __table_args__ = (
        UniqueConstraint(
            "execution_account",
            "execution_venue",
            "dex",
            "canonical_coin",
            name="uq_follower_market_guard_scope",
        ),
        Index(
            "ix_follower_market_guards_active_scope",
            "active",
            "execution_account",
            "execution_venue",
            "dex",
            "canonical_coin",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    execution_account: Mapped[str] = mapped_column(String(64), default="", server_default="", index=True)
    execution_venue: Mapped[str] = mapped_column(String(16), default="HYPERLIQUID", index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    canonical_coin: Mapped[str] = mapped_column(String(160), index=True)
    position_version: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0")
    active: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", index=True)
    reason: Mapped[str | None] = mapped_column(Text)
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_unmatched_fill_id: Mapped[str | None] = mapped_column(String(64), index=True)
    last_cloid: Mapped[str | None] = mapped_column(String(34))
    last_order_id: Mapped[str | None] = mapped_column(String(80))
    expected_position_side: Mapped[str | None] = mapped_column(String(8))
    expected_position_qty: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    expected_position_relation: Mapped[str | None] = mapped_column(String(16))
    position_change_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class UnmatchedFollowerFill(Base, TimestampMixin):
    __tablename__ = "unmatched_follower_fills"
    __table_args__ = (
        Index(
            "ix_unmatched_follower_fills_scope_time",
            "execution_venue",
            "execution_account",
            "dex",
            "canonical_coin",
            "observed_at",
        ),
    )

    follower_fill_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    execution_account: Mapped[str] = mapped_column(String(64), default="", server_default="", index=True)
    execution_venue: Mapped[str] = mapped_column(String(16), default="HYPERLIQUID", index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    canonical_coin: Mapped[str] = mapped_column(String(160), index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    cloid: Mapped[str | None] = mapped_column(String(34))
    order_id: Mapped[str | None] = mapped_column(String(80))


class LeaderFillChannelObservation(Base, TimestampMixin):
    __tablename__ = "leader_fill_channel_observations"
    __table_args__ = (
        UniqueConstraint("source_fill_id", "channel", name="uq_leader_fill_observation_fill_channel"),
        Index("ix_leader_fill_observations_leader_channel_time", "leader_address", "channel", "source_time_ms"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    source_fill_id: Mapped[str] = mapped_column(String(160), index=True)
    channel: Mapped[str] = mapped_column(String(32), index=True)
    leader_address: Mapped[str] = mapped_column(String(64), index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    canonical_coin: Mapped[str | None] = mapped_column(String(160), index=True)
    raw_coin: Mapped[str | None] = mapped_column(String(160))
    side: Mapped[str] = mapped_column(String(16))
    price: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    size: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    source_time_ms: Mapped[int] = mapped_column(BigInteger, index=True)
    ws_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    event_to_ws_ms: Mapped[int | None] = mapped_column(BigInteger, index=True)
    raw_fill: Mapped[dict] = mapped_column(JSON)
    is_snapshot: Mapped[bool] = mapped_column(Boolean, default=False)


class DesiredPosition(Base, TimestampMixin):
    __tablename__ = "desired_positions"
    __table_args__ = (UniqueConstraint("leader_address", "coin", name="uq_desired_leader_coin"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    leader_address: Mapped[str] = mapped_column(String(64), index=True)
    coin: Mapped[str] = mapped_column(String(80), index=True)
    binance_symbol: Mapped[str] = mapped_column(String(32), index=True)
    target_notional: Mapped[Decimal] = mapped_column(Numeric(30, 12), default=Decimal("0"))
    target_quantity: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    last_source_time_ms: Mapped[int | None] = mapped_column(BigInteger)
    last_execution_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class LeaderPositionAllocationRecord(Base, TimestampMixin):
    __tablename__ = "leader_position_allocations"
    __table_args__ = (
        UniqueConstraint(
            "leader_address",
            "binance_symbol",
            "position_side",
            name="uq_leader_position_allocation_side",
        ),
        Index(
            "ix_leader_position_allocations_symbol_side_status",
            "binance_symbol",
            "position_side",
            "status",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    leader_id: Mapped[int | None] = mapped_column(
        ForeignKey("leader_configs.id", ondelete="SET NULL"), index=True
    )
    leader_address: Mapped[str] = mapped_column(String(64), index=True)
    hyperliquid_coin: Mapped[str] = mapped_column(String(80), index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    canonical_coin: Mapped[str | None] = mapped_column(String(160), index=True)
    binance_symbol: Mapped[str | None] = mapped_column(String(32), index=True)
    execution_venue: Mapped[str] = mapped_column(String(16), default="BINANCE", index=True)
    venue_account: Mapped[str] = mapped_column(String(64), default="", server_default="", index=True)
    venue_symbol: Mapped[str] = mapped_column(String(80), index=True)
    position_side: Mapped[str] = mapped_column(String(8), index=True)
    target_notional: Mapped[Decimal] = mapped_column(Numeric(30, 12), default=Decimal("0"))
    allocated_notional: Mapped[Decimal] = mapped_column(Numeric(30, 12), default=Decimal("0"))
    allocated_qty: Mapped[Decimal] = mapped_column(Numeric(30, 12), default=Decimal("0"))
    avg_entry_price: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    last_leader_account_value: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    last_leader_position_notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    last_leader_position_size: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    copy_multiplier: Mapped[Decimal] = mapped_column(Numeric(20, 8), default=Decimal("0.1"))
    status: Mapped[str] = mapped_column(String(32), default="NEEDS_MANUAL_REVIEW", index=True)
    pending_reduce_qty: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    pending_reduce_notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    pending_reduce_reason: Mapped[str | None] = mapped_column(String(1000))
    pending_reduce_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    pending_reduce_source_fill_id: Mapped[str | None] = mapped_column(String(160), index=True)
    last_source_fill_id: Mapped[str | None] = mapped_column(String(160), index=True)
    last_reconcile_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MarketRiskSetting(Base, TimestampMixin):
    __tablename__ = "market_risk_settings"
    __table_args__ = (
        UniqueConstraint(
            "execution_venue",
            "account_address",
            "dex",
            "canonical_coin",
            name="uq_market_risk_setting_scope",
        ),
        Index("ix_market_risk_settings_scope", "execution_venue", "account_address", "dex", "canonical_coin"),
        Index("ix_market_risk_settings_status", "status"),
        Index("ix_market_risk_settings_last_confirmed_at", "last_confirmed_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    execution_venue: Mapped[str] = mapped_column(String(16), default="HYPERLIQUID", index=True)
    account_address: Mapped[str] = mapped_column(String(64), index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    canonical_coin: Mapped[str] = mapped_column(String(160), index=True)
    asset_id: Mapped[int | None] = mapped_column(BigInteger)
    desired_margin_mode: Mapped[str] = mapped_column(String(16), default="CROSS")
    desired_leverage: Mapped[int | None] = mapped_column(Integer)
    market_max_leverage: Mapped[int | None] = mapped_column(Integer)
    effective_leverage: Mapped[int | None] = mapped_column(Integer)
    actual_margin_mode: Mapped[str | None] = mapped_column(String(16))
    actual_leverage: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(24), default="UNKNOWN", index=True)
    last_set_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    error_message: Mapped[str | None] = mapped_column(Text)
    raw_response_masked: Mapped[dict | None] = mapped_column(JSON)


class LeaderPositionBaseline(Base, TimestampMixin):
    __tablename__ = "leader_position_baselines"
    __table_args__ = (
        UniqueConstraint(
            "leader_id",
            "execution_venue",
            "dex",
            "canonical_coin",
            name="uq_leader_position_baseline_scope",
        ),
        Index(
            "ix_leader_position_baselines_scope_status",
            "leader_id",
            "execution_venue",
            "dex",
            "canonical_coin",
            "baseline_status",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    leader_id: Mapped[int | None] = mapped_column(ForeignKey("leader_configs.id", ondelete="SET NULL"), index=True)
    leader_address: Mapped[str] = mapped_column(String(64), index=True)
    execution_venue: Mapped[str] = mapped_column(String(16), default="HYPERLIQUID", index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    canonical_coin: Mapped[str] = mapped_column(String(160), index=True)
    side_at_enable: Mapped[str] = mapped_column(String(8), default="FLAT")
    size_at_enable: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    notional_at_enable: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    entry_px_at_enable: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    mark_px_at_enable: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    account_value_at_enable: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    baseline_status: Mapped[str] = mapped_column(String(32), default="WAIT_UNTIL_FLAT", index=True)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    flat_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    copy_allowed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_leader_size: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    last_leader_notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    last_leader_entry_px: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    last_leader_mark_px: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    reason: Mapped[str | None] = mapped_column(Text)


class AllocationEvent(Base, TimestampMixin):
    __tablename__ = "allocation_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    allocation_id: Mapped[int | None] = mapped_column(
        ForeignKey("leader_position_allocations.id", ondelete="SET NULL"), index=True
    )
    execution_order_id: Mapped[int | None] = mapped_column(
        ForeignKey("execution_orders.id", ondelete="SET NULL"), index=True
    )
    leader_id: Mapped[int | None] = mapped_column(ForeignKey("leader_configs.id", ondelete="SET NULL"), index=True)
    leader_address: Mapped[str] = mapped_column(String(64), index=True)
    source_fill_id: Mapped[str | None] = mapped_column(String(160), index=True)
    execution_venue: Mapped[str] = mapped_column(String(16), index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    canonical_coin: Mapped[str | None] = mapped_column(String(160), index=True)
    position_side: Mapped[str | None] = mapped_column(String(8), index=True)
    action: Mapped[str] = mapped_column(String(40), index=True)
    before_notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    after_notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    before_qty: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    after_qty: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    metadata_json: Mapped[dict | None] = mapped_column(JSON)


class ExecutionOrder(Base, TimestampMixin):
    __tablename__ = "execution_orders"
    __table_args__ = (
        Index("ix_execution_orders_leader_symbol_status", "leader_address", "binance_symbol", "status"),
        UniqueConstraint(
            "submit_signer_scope",
            "submit_nonce",
            name="uq_execution_orders_signer_nonce",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    leader_id: Mapped[int | None] = mapped_column(ForeignKey("leader_configs.id", ondelete="SET NULL"), index=True)
    allocation_id: Mapped[int | None] = mapped_column(
        ForeignKey("leader_position_allocations.id", ondelete="SET NULL"), index=True
    )
    leader_address: Mapped[str] = mapped_column(String(64), index=True)
    source_fill_id: Mapped[str | None] = mapped_column(String(160), unique=True, index=True)
    source_type: Mapped[str] = mapped_column(String(24), default="AUTO_COPY", index=True)
    source_coin: Mapped[str] = mapped_column(String(80), index=True)
    execution_venue: Mapped[str] = mapped_column(String(16), default="BINANCE", index=True)
    venue_account: Mapped[str] = mapped_column(String(64), default="", server_default="", index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    canonical_coin: Mapped[str | None] = mapped_column(String(160), index=True)
    raw_coin_from_fill: Mapped[str | None] = mapped_column(String(160))
    asset_id: Mapped[int | None] = mapped_column(BigInteger)
    venue_symbol: Mapped[str | None] = mapped_column(String(80), index=True)
    hyperliquid_coin: Mapped[str | None] = mapped_column(String(80), index=True)
    binance_symbol: Mapped[str | None] = mapped_column(String(32), index=True)
    side: Mapped[str] = mapped_column(String(16))
    position_side: Mapped[str | None] = mapped_column(String(8), index=True)
    order_action: Mapped[str | None] = mapped_column(String(32))
    order_type: Mapped[str] = mapped_column(String(40), default="MARKET")
    client_order_id: Mapped[str | None] = mapped_column(String(36), unique=True, index=True)
    cloid: Mapped[str | None] = mapped_column(String(34), unique=True, index=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    executed_qty: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    price: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    estimated_price: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    avg_fill_price: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    leader_entry_px: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    follower_avg_entry_px: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    cum_quote: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    slippage_bps: Mapped[Decimal | None] = mapped_column(Numeric(20, 8))
    notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    order_id: Mapped[str | None] = mapped_column(String(80), index=True)
    venue_order_id: Mapped[str | None] = mapped_column(String(80), index=True)
    status: Mapped[str] = mapped_column(String(40), index=True)
    dry_run: Mapped[bool] = mapped_column(Boolean, default=True)
    reduce_only: Mapped[bool] = mapped_column(Boolean, default=False)
    is_close_intent: Mapped[bool] = mapped_column(Boolean, default=False)
    error_message: Mapped[str | None] = mapped_column(Text)
    raw_response: Mapped[dict | None] = mapped_column(JSON)
    request_payload_masked: Mapped[dict | None] = mapped_column(JSON)
    response_payload_masked: Mapped[dict | None] = mapped_column(JSON)
    signed_action_envelope: Mapped[dict | None] = mapped_column(JSON)
    signed_action_hash: Mapped[str | None] = mapped_column(String(64), unique=True, index=True)
    submit_signer_scope: Mapped[str | None] = mapped_column(String(96), index=True)
    submit_nonce: Mapped[int | None] = mapped_column(BigInteger, index=True)
    pre_trade_checklist: Mapped[dict | None] = mapped_column(JSON)
    hyperliquid_event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    event_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ws_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dedupe_done_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    debounce_released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decision_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decision_done_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    order_submit_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    order_submit_done_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    order_ack_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    binance_order_submit_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    binance_order_ack_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    order_finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    event_to_receive_ms: Mapped[int | None] = mapped_column(BigInteger)
    event_to_ws_ms: Mapped[int | None] = mapped_column(BigInteger)
    leader_event_to_ws_ms: Mapped[int | None] = mapped_column(BigInteger)
    ws_to_parse_ms: Mapped[int | None] = mapped_column(BigInteger)
    ws_to_dedupe_ms: Mapped[int | None] = mapped_column(BigInteger)
    parse_to_dedupe_ms: Mapped[int | None] = mapped_column(BigInteger)
    debounce_ms: Mapped[int | None] = mapped_column(BigInteger)
    lock_wait_ms: Mapped[int | None] = mapped_column(BigInteger)
    decision_ms: Mapped[int | None] = mapped_column(BigInteger)
    cache_read_ms: Mapped[int | None] = mapped_column(BigInteger)
    sizing_ms: Mapped[int | None] = mapped_column(BigInteger)
    checklist_ms: Mapped[int | None] = mapped_column(BigInteger)
    ws_to_submit_ms: Mapped[int | None] = mapped_column(BigInteger)
    receive_to_submit_ms: Mapped[int | None] = mapped_column(BigInteger)
    submit_to_ack_ms: Mapped[int | None] = mapped_column(BigInteger)
    event_to_ack_ms: Mapped[int | None] = mapped_column(BigInteger)
    event_to_final_ms: Mapped[int | None] = mapped_column(BigInteger)
    allocation_update_ms: Mapped[int | None] = mapped_column(BigInteger)
    total_hot_path_ms: Mapped[int | None] = mapped_column(BigInteger)
    latency_trace_id: Mapped[str | None] = mapped_column(String(80), index=True)
    latency_trace: Mapped[dict | None] = mapped_column(JSON)
    missing_latency_fields: Mapped[list[str] | None] = mapped_column(JSON)
    sizing_mode: Mapped[str | None] = mapped_column(String(32))
    leader_account_value: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    leader_account_value_source: Mapped[str | None] = mapped_column(String(64))
    leader_account_abstraction_mode: Mapped[str | None] = mapped_column(String(32))
    leader_position_notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    follower_account_value: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    follower_account_value_source: Mapped[str | None] = mapped_column(String(64))
    follower_account_abstraction_mode: Mapped[str | None] = mapped_column(String(32))
    leader_position_ratio: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    copy_multiplier: Mapped[Decimal | None] = mapped_column(Numeric(20, 8))
    target_notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    delta_notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))


class BinancePositionsSnapshot(Base, TimestampMixin):
    __tablename__ = "binance_positions_snapshots"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    account_equity: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    positions: Mapped[list[dict]] = mapped_column(JSON, default=list)


class LatestLeaderState(Base, TimestampMixin):
    __tablename__ = "latest_leader_states"

    leader_address: Mapped[str] = mapped_column(String(64), primary_key=True)
    account_value: Mapped[Decimal] = mapped_column(Numeric(30, 12))
    withdrawable: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    total_ntl_pos: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    total_margin_used: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    positions: Mapped[list[dict]] = mapped_column(JSON, default=list)
    websocket_status: Mapped[str] = mapped_column(String(32), default="unknown")
    last_update_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class LatestAccountState(Base, TimestampMixin):
    __tablename__ = "latest_account_states"
    __table_args__ = (UniqueConstraint("role", "address", "dex", name="uq_latest_account_states_role_address_dex"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    role: Mapped[str] = mapped_column(String(16), index=True)
    address: Mapped[str] = mapped_column(String(64), index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    dex_display_name: Mapped[str | None] = mapped_column(String(80))
    account_label: Mapped[str | None] = mapped_column(String(120))
    account_value: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    withdrawable: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    total_ntl_pos: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    total_raw_usd: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    total_margin_used: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    raw_payload_masked: Mapped[dict | None] = mapped_column(JSON)
    source: Mapped[str] = mapped_column(String(32), default="info_endpoint")
    # This is a high-frequency freshness heartbeat.  Keeping it out of an index
    # allows PostgreSQL HOT updates and avoids account-state churn competing
    # with durable order commits.
    last_update_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)

    positions: Mapped[list[LatestAccountPosition]] = relationship(
        back_populates="account_state",
        cascade="all, delete-orphan",
    )


class LatestAccountPosition(Base, TimestampMixin):
    __tablename__ = "latest_account_positions"
    __table_args__ = (
        Index(
            "ix_latest_account_positions_active_scope",
            "role",
            "address",
            "dex",
            "canonical_coin",
            "active",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    account_state_id: Mapped[int] = mapped_column(
        ForeignKey("latest_account_states.id", ondelete="CASCADE"), index=True
    )
    role: Mapped[str] = mapped_column(String(16), index=True)
    address: Mapped[str] = mapped_column(String(64), index=True)
    dex: Mapped[str] = mapped_column(String(32), default="", index=True)
    coin: Mapped[str] = mapped_column(String(80), index=True)
    canonical_coin: Mapped[str | None] = mapped_column(String(160), index=True)
    raw_coin: Mapped[str | None] = mapped_column(String(160))
    product_type: Mapped[str | None] = mapped_column(String(80))
    side: Mapped[str] = mapped_column(String(8))
    size: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    notional: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    entry_px: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    mark_px: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    mid_px: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    mark_px_source: Mapped[str | None] = mapped_column(String(32))
    unrealized_pnl: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    leverage: Mapped[Decimal | None] = mapped_column(Numeric(20, 8))
    margin_used: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    liquidation_px: Mapped[Decimal | None] = mapped_column(Numeric(30, 12))
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    status: Mapped[str] = mapped_column(String(16), default="OPEN", index=True)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    position_opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    open_time_source: Mapped[str | None] = mapped_column(String(32))
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    raw_payload_masked: Mapped[dict | None] = mapped_column(JSON)
    last_update_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    account_state: Mapped[LatestAccountState] = relationship(back_populates="positions")


class RiskEvent(Base, TimestampMixin):
    __tablename__ = "risk_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    severity: Mapped[str] = mapped_column(String(20), index=True)
    event_type: Mapped[str] = mapped_column(String(80), index=True)
    symbol: Mapped[str | None] = mapped_column(String(32), index=True)
    leader_address: Mapped[str | None] = mapped_column(String(64), index=True)
    message: Mapped[str] = mapped_column(Text)
    metadata_json: Mapped[dict | None] = mapped_column(JSON)


class AppSetting(Base, TimestampMixin):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(120), primary_key=True)
    value: Mapped[dict] = mapped_column(JSON, default=dict)


class AuditLog(Base, TimestampMixin):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    action: Mapped[str] = mapped_column(String(120), index=True)
    ip_address: Mapped[str | None] = mapped_column(String(64))
    metadata_json: Mapped[dict | None] = mapped_column(JSON)
