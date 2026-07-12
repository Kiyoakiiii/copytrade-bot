"""execution venues and hyperliquid routing

Revision ID: 0005_venues
Revises: 0004_market_latency
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0005_venues"
down_revision = "0004_market_latency"
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
    op.add_column("leader_configs", sa.Column("preferred_venue", sa.String(length=16), nullable=False, server_default="HYPERLIQUID"))
    op.add_column("leader_configs", sa.Column("fallback_venue", sa.String(length=16), nullable=False, server_default="NONE"))
    op.add_column("leader_configs", sa.Column("enabled_venues", sa.JSON(), nullable=False, server_default='["HYPERLIQUID"]'))
    op.add_column("leader_configs", sa.Column("hyperliquid_account_id", sa.String(length=64), nullable=True))
    op.add_column("leader_configs", sa.Column("hyperliquid_vault_address", sa.String(length=64), nullable=True))

    op.add_column("leader_symbol_configs", sa.Column("preferred_venue", sa.String(length=16), nullable=True))
    op.add_column("leader_symbol_configs", sa.Column("fallback_venue", sa.String(length=16), nullable=True))
    op.add_column("leader_symbol_configs", sa.Column("enabled_venues", sa.JSON(), nullable=True))
    op.add_column("leader_symbol_configs", sa.Column("hyperliquid_account_id", sa.String(length=64), nullable=True))
    op.add_column("leader_symbol_configs", sa.Column("hyperliquid_vault_address", sa.String(length=64), nullable=True))

    op.add_column("leader_position_allocations", sa.Column("execution_venue", sa.String(length=16), nullable=False, server_default="BINANCE"))
    op.add_column("leader_position_allocations", sa.Column("venue_account", sa.String(length=64), nullable=True))
    op.add_column("leader_position_allocations", sa.Column("venue_symbol", sa.String(length=32), nullable=True))
    op.alter_column("leader_position_allocations", "binance_symbol", nullable=True)
    op.execute("update leader_position_allocations set venue_symbol = coalesce(binance_symbol, hyperliquid_coin) where venue_symbol is null")
    op.create_index("ix_leader_position_allocations_execution_venue", "leader_position_allocations", ["execution_venue"])
    op.create_index("ix_leader_position_allocations_venue_account", "leader_position_allocations", ["venue_account"])
    op.create_index("ix_leader_position_allocations_venue_symbol", "leader_position_allocations", ["venue_symbol"])

    op.add_column("execution_orders", sa.Column("execution_venue", sa.String(length=16), nullable=False, server_default="BINANCE"))
    op.add_column("execution_orders", sa.Column("venue_symbol", sa.String(length=32), nullable=True))
    op.add_column("execution_orders", sa.Column("hyperliquid_coin", sa.String(length=32), nullable=True))
    op.add_column("execution_orders", sa.Column("venue_order_id", sa.String(length=80), nullable=True))
    op.add_column("execution_orders", sa.Column("cloid", sa.String(length=34), nullable=True))
    op.add_column("execution_orders", sa.Column("is_close_intent", sa.Boolean(), nullable=False, server_default=sa.text("false")))
    op.add_column("execution_orders", sa.Column("request_payload_masked", sa.JSON(), nullable=True))
    op.add_column("execution_orders", sa.Column("response_payload_masked", sa.JSON(), nullable=True))
    op.alter_column("execution_orders", "binance_symbol", nullable=True)
    op.execute("update execution_orders set venue_symbol = binance_symbol where venue_symbol is null")
    op.execute("update execution_orders set hyperliquid_coin = source_coin where hyperliquid_coin is null")
    op.create_index("ix_execution_orders_execution_venue", "execution_orders", ["execution_venue"])
    op.create_index("ix_execution_orders_venue_symbol", "execution_orders", ["venue_symbol"])
    op.create_index("ix_execution_orders_hyperliquid_coin", "execution_orders", ["hyperliquid_coin"])
    op.create_index("ix_execution_orders_venue_order_id", "execution_orders", ["venue_order_id"])
    op.create_index("ix_execution_orders_cloid", "execution_orders", ["cloid"], unique=True)

    op.create_table(
        "venue_mappings",
        id_col(),
        sa.Column("hyperliquid_coin", sa.String(length=32), nullable=False),
        sa.Column("execution_venue", sa.String(length=16), nullable=False),
        sa.Column("venue_symbol", sa.String(length=32), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("mapping_status", sa.String(length=32), nullable=False, server_default="UNKNOWN"),
        sa.Column("reason", sa.Text(), nullable=True),
        *timestamps(),
        sa.UniqueConstraint(
            "hyperliquid_coin",
            "execution_venue",
            "venue_symbol",
            name="uq_venue_mapping_coin_venue_symbol",
        ),
    )
    op.create_index("ix_venue_mappings_hyperliquid_coin", "venue_mappings", ["hyperliquid_coin"])
    op.create_index("ix_venue_mappings_execution_venue", "venue_mappings", ["execution_venue"])
    op.create_index("ix_venue_mappings_venue_symbol", "venue_mappings", ["venue_symbol"])
    op.create_index("ix_venue_mappings_enabled", "venue_mappings", ["enabled"])
    op.create_index("ix_venue_mappings_is_default", "venue_mappings", ["is_default"])
    op.create_index("ix_venue_mappings_mapping_status", "venue_mappings", ["mapping_status"])

    op.execute(
        """
        insert into venue_mappings (
            hyperliquid_coin, execution_venue, venue_symbol, enabled, is_default,
            mapping_status, reason, created_at, updated_at
        )
        select hyperliquid_coin, 'BINANCE', binance_symbol, enabled, false,
               case when enabled then 'OK' else 'DISABLED' end,
               last_validation_error, now(), now()
        from symbol_mappings
        on conflict on constraint uq_venue_mapping_coin_venue_symbol do nothing
        """
    )
    op.execute(
        """
        insert into venue_mappings (
            hyperliquid_coin, execution_venue, venue_symbol, enabled, is_default,
            mapping_status, reason, created_at, updated_at
        )
        select hyperliquid_coin, 'HYPERLIQUID', hyperliquid_coin, true, true,
               'UNKNOWN', 'requires Hyperliquid meta validation', now(), now()
        from symbol_mappings
        on conflict on constraint uq_venue_mapping_coin_venue_symbol do nothing
        """
    )


def downgrade() -> None:
    op.drop_table("venue_mappings")
    op.drop_index("ix_execution_orders_cloid", table_name="execution_orders")
    op.drop_index("ix_execution_orders_venue_order_id", table_name="execution_orders")
    op.drop_index("ix_execution_orders_hyperliquid_coin", table_name="execution_orders")
    op.drop_index("ix_execution_orders_venue_symbol", table_name="execution_orders")
    op.drop_index("ix_execution_orders_execution_venue", table_name="execution_orders")
    op.alter_column("execution_orders", "binance_symbol", nullable=False)
    for column in [
        "response_payload_masked",
        "request_payload_masked",
        "is_close_intent",
        "cloid",
        "venue_order_id",
        "hyperliquid_coin",
        "venue_symbol",
        "execution_venue",
    ]:
        op.drop_column("execution_orders", column)

    op.drop_index("ix_leader_position_allocations_venue_symbol", table_name="leader_position_allocations")
    op.drop_index("ix_leader_position_allocations_venue_account", table_name="leader_position_allocations")
    op.drop_index("ix_leader_position_allocations_execution_venue", table_name="leader_position_allocations")
    op.alter_column("leader_position_allocations", "binance_symbol", nullable=False)
    op.drop_column("leader_position_allocations", "venue_symbol")
    op.drop_column("leader_position_allocations", "venue_account")
    op.drop_column("leader_position_allocations", "execution_venue")

    for column in [
        "hyperliquid_vault_address",
        "hyperliquid_account_id",
        "enabled_venues",
        "fallback_venue",
        "preferred_venue",
    ]:
        op.drop_column("leader_symbol_configs", column)
        op.drop_column("leader_configs", column)
