"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-04-24
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def id_col() -> sa.Column:
    return sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True)


def timestamps() -> list[sa.Column]:
    return [
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    ]


def upgrade() -> None:
    op.create_table(
        "users",
        id_col(),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("totp_secret_encrypted", sa.Text(), nullable=True),
        sa.Column("totp_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("is_admin", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("failed_login_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        *timestamps(),
        sa.UniqueConstraint("email"),
    )
    op.create_index("ix_users_email", "users", ["email"])

    op.create_table(
        "sessions",
        id_col(),
        sa.Column("user_id", sa.BigInteger(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.Text(), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        *timestamps(),
        sa.UniqueConstraint("token_hash"),
    )
    op.create_index("ix_sessions_user_id", "sessions", ["user_id"])
    op.create_index("ix_sessions_token_hash", "sessions", ["token_hash"])
    op.create_index("ix_sessions_expires_at", "sessions", ["expires_at"])

    op.create_table(
        "leader_configs",
        id_col(),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("leader_address", sa.String(length=64), nullable=False),
        sa.Column("copy_multiplier", sa.Numeric(20, 8), nullable=False, server_default="0.1"),
        sa.Column("allowed_symbols", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("blocked_symbols", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("max_notional_per_trade", sa.Numeric(20, 8), nullable=True),
        sa.Column("max_total_notional", sa.Numeric(20, 8), nullable=True),
        sa.Column("max_leverage", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("slippage_bps", sa.Integer(), nullable=False, server_default="20"),
        sa.Column("use_market_order", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("reduce_only_on_reduce", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("copy_mode", sa.String(length=32), nullable=False, server_default="fill_delta_and_reconcile"),
        sa.Column("frontend_live_confirmed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        *timestamps(),
        sa.UniqueConstraint("leader_address"),
    )
    op.create_index("ix_leader_configs_enabled", "leader_configs", ["enabled"])
    op.create_index("ix_leader_configs_leader_address", "leader_configs", ["leader_address"])

    op.create_table(
        "leader_symbol_configs",
        id_col(),
        sa.Column("leader_id", sa.BigInteger(), sa.ForeignKey("leader_configs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("coin", sa.String(length=32), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("max_notional_per_trade", sa.Numeric(20, 8), nullable=True),
        sa.Column("max_position_notional", sa.Numeric(20, 8), nullable=True),
        *timestamps(),
        sa.UniqueConstraint("leader_id", "coin", name="uq_leader_symbol_coin"),
    )
    op.create_index("ix_leader_symbol_configs_coin", "leader_symbol_configs", ["coin"])

    op.create_table(
        "symbol_mappings",
        id_col(),
        sa.Column("hyperliquid_coin", sa.String(length=32), nullable=False),
        sa.Column("binance_symbol", sa.String(length=32), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("rule_snapshot", sa.JSON(), nullable=True),
        sa.Column("last_validation_error", sa.Text(), nullable=True),
        *timestamps(),
        sa.UniqueConstraint("hyperliquid_coin"),
    )
    op.create_index("ix_symbol_mappings_hyperliquid_coin", "symbol_mappings", ["hyperliquid_coin"])
    op.create_index("ix_symbol_mappings_binance_symbol", "symbol_mappings", ["binance_symbol"])

    op.create_table(
        "source_fills",
        id_col(),
        sa.Column("source_fill_id", sa.String(length=160), nullable=False),
        sa.Column("leader_address", sa.String(length=64), nullable=False),
        sa.Column("coin", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("price", sa.Numeric(30, 12), nullable=False),
        sa.Column("size", sa.Numeric(30, 12), nullable=False),
        sa.Column("source_time_ms", sa.BigInteger(), nullable=False),
        sa.Column("raw_fill", sa.JSON(), nullable=False),
        sa.Column("is_snapshot", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        *timestamps(),
        sa.UniqueConstraint("source_fill_id"),
    )
    op.create_index("ix_source_fills_source_fill_id", "source_fills", ["source_fill_id"])
    op.create_index("ix_source_fills_leader_address", "source_fills", ["leader_address"])
    op.create_index("ix_source_fills_coin", "source_fills", ["coin"])
    op.create_index("ix_source_fills_source_time_ms", "source_fills", ["source_time_ms"])

    op.create_table(
        "desired_positions",
        id_col(),
        sa.Column("leader_address", sa.String(length=64), nullable=False),
        sa.Column("coin", sa.String(length=32), nullable=False),
        sa.Column("binance_symbol", sa.String(length=32), nullable=False),
        sa.Column("target_notional", sa.Numeric(30, 12), nullable=False, server_default="0"),
        sa.Column("target_quantity", sa.Numeric(30, 12), nullable=True),
        sa.Column("last_source_time_ms", sa.BigInteger(), nullable=True),
        sa.Column("last_execution_at", sa.DateTime(timezone=True), nullable=True),
        *timestamps(),
        sa.UniqueConstraint("leader_address", "coin", name="uq_desired_leader_coin"),
    )
    op.create_index("ix_desired_positions_leader_address", "desired_positions", ["leader_address"])
    op.create_index("ix_desired_positions_coin", "desired_positions", ["coin"])
    op.create_index("ix_desired_positions_binance_symbol", "desired_positions", ["binance_symbol"])

    op.create_table(
        "execution_orders",
        id_col(),
        sa.Column("leader_address", sa.String(length=64), nullable=False),
        sa.Column("source_fill_id", sa.String(length=160), nullable=True),
        sa.Column("source_coin", sa.String(length=32), nullable=False),
        sa.Column("binance_symbol", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("quantity", sa.Numeric(30, 12), nullable=False),
        sa.Column("price", sa.Numeric(30, 12), nullable=True),
        sa.Column("notional", sa.Numeric(30, 12), nullable=True),
        sa.Column("order_id", sa.String(length=80), nullable=True),
        sa.Column("status", sa.String(length=40), nullable=False),
        sa.Column("dry_run", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("reduce_only", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("raw_response", sa.JSON(), nullable=True),
        *timestamps(),
    )
    op.create_index("ix_execution_orders_source_fill_id", "execution_orders", ["source_fill_id"])
    op.create_index("ix_execution_orders_leader_address", "execution_orders", ["leader_address"])
    op.create_index("ix_execution_orders_source_coin", "execution_orders", ["source_coin"])
    op.create_index("ix_execution_orders_binance_symbol", "execution_orders", ["binance_symbol"])
    op.create_index("ix_execution_orders_order_id", "execution_orders", ["order_id"])
    op.create_index("ix_execution_orders_status", "execution_orders", ["status"])
    op.create_index(
        "ix_execution_orders_leader_symbol_status",
        "execution_orders",
        ["leader_address", "binance_symbol", "status"],
    )

    op.create_table(
        "binance_positions_snapshots",
        id_col(),
        sa.Column("account_equity", sa.Numeric(30, 12), nullable=True),
        sa.Column("positions", sa.JSON(), nullable=False, server_default="[]"),
        *timestamps(),
    )

    op.create_table(
        "risk_events",
        id_col(),
        sa.Column("severity", sa.String(length=20), nullable=False),
        sa.Column("event_type", sa.String(length=80), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=True),
        sa.Column("leader_address", sa.String(length=64), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        *timestamps(),
    )
    op.create_index("ix_risk_events_severity", "risk_events", ["severity"])
    op.create_index("ix_risk_events_event_type", "risk_events", ["event_type"])
    op.create_index("ix_risk_events_symbol", "risk_events", ["symbol"])
    op.create_index("ix_risk_events_leader_address", "risk_events", ["leader_address"])

    op.create_table(
        "app_settings",
        sa.Column("key", sa.String(length=120), primary_key=True),
        sa.Column("value", sa.JSON(), nullable=False, server_default="{}"),
        *timestamps(),
    )

    op.create_table(
        "audit_logs",
        id_col(),
        sa.Column("user_id", sa.BigInteger(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("action", sa.String(length=120), nullable=False),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        *timestamps(),
    )
    op.create_index("ix_audit_logs_action", "audit_logs", ["action"])


def downgrade() -> None:
    op.drop_index("ix_audit_logs_action", table_name="audit_logs")
    op.drop_table("audit_logs")
    op.drop_table("app_settings")
    op.drop_index("ix_risk_events_leader_address", table_name="risk_events")
    op.drop_index("ix_risk_events_symbol", table_name="risk_events")
    op.drop_index("ix_risk_events_event_type", table_name="risk_events")
    op.drop_index("ix_risk_events_severity", table_name="risk_events")
    op.drop_table("risk_events")
    op.drop_table("binance_positions_snapshots")
    op.drop_index("ix_execution_orders_leader_symbol_status", table_name="execution_orders")
    op.drop_index("ix_execution_orders_status", table_name="execution_orders")
    op.drop_index("ix_execution_orders_order_id", table_name="execution_orders")
    op.drop_index("ix_execution_orders_binance_symbol", table_name="execution_orders")
    op.drop_index("ix_execution_orders_source_coin", table_name="execution_orders")
    op.drop_index("ix_execution_orders_leader_address", table_name="execution_orders")
    op.drop_index("ix_execution_orders_source_fill_id", table_name="execution_orders")
    op.drop_table("execution_orders")
    op.drop_index("ix_desired_positions_binance_symbol", table_name="desired_positions")
    op.drop_index("ix_desired_positions_coin", table_name="desired_positions")
    op.drop_index("ix_desired_positions_leader_address", table_name="desired_positions")
    op.drop_table("desired_positions")
    op.drop_index("ix_source_fills_source_time_ms", table_name="source_fills")
    op.drop_index("ix_source_fills_coin", table_name="source_fills")
    op.drop_index("ix_source_fills_leader_address", table_name="source_fills")
    op.drop_index("ix_source_fills_source_fill_id", table_name="source_fills")
    op.drop_table("source_fills")
    op.drop_index("ix_symbol_mappings_binance_symbol", table_name="symbol_mappings")
    op.drop_index("ix_symbol_mappings_hyperliquid_coin", table_name="symbol_mappings")
    op.drop_table("symbol_mappings")
    op.drop_index("ix_leader_symbol_configs_coin", table_name="leader_symbol_configs")
    op.drop_table("leader_symbol_configs")
    op.drop_index("ix_leader_configs_leader_address", table_name="leader_configs")
    op.drop_index("ix_leader_configs_enabled", table_name="leader_configs")
    op.drop_table("leader_configs")
    op.drop_index("ix_sessions_expires_at", table_name="sessions")
    op.drop_index("ix_sessions_token_hash", table_name="sessions")
    op.drop_index("ix_sessions_user_id", table_name="sessions")
    op.drop_table("sessions")
    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")

