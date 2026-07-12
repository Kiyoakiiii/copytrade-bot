"""hedge mode leader allocations

Revision ID: 0003_hedge_allocations
Revises: 0002_preflight_leader_state
Create Date: 2026-04-25
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0003_hedge_allocations"
down_revision = "0002_preflight_leader_state"
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
    op.add_column("execution_orders", sa.Column("position_side", sa.String(length=8), nullable=True))
    op.add_column("execution_orders", sa.Column("order_action", sa.String(length=32), nullable=True))
    op.create_index("ix_execution_orders_position_side", "execution_orders", ["position_side"])

    op.create_table(
        "leader_position_allocations",
        id_col(),
        sa.Column("leader_id", sa.BigInteger(), sa.ForeignKey("leader_configs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("leader_address", sa.String(length=64), nullable=False),
        sa.Column("hyperliquid_coin", sa.String(length=32), nullable=False),
        sa.Column("binance_symbol", sa.String(length=32), nullable=False),
        sa.Column("position_side", sa.String(length=8), nullable=False),
        sa.Column("target_notional", sa.Numeric(30, 12), nullable=False, server_default="0"),
        sa.Column("allocated_notional", sa.Numeric(30, 12), nullable=False, server_default="0"),
        sa.Column("allocated_qty", sa.Numeric(30, 12), nullable=False, server_default="0"),
        sa.Column("avg_entry_price", sa.Numeric(30, 12), nullable=True),
        sa.Column("last_leader_account_value", sa.Numeric(30, 12), nullable=True),
        sa.Column("last_leader_position_notional", sa.Numeric(30, 12), nullable=True),
        sa.Column("copy_multiplier", sa.Numeric(20, 8), nullable=False, server_default="0.1"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="NEEDS_MANUAL_REVIEW"),
        sa.Column("last_source_fill_id", sa.String(length=160), nullable=True),
        sa.Column("last_reconcile_at", sa.DateTime(timezone=True), nullable=True),
        *timestamps(),
        sa.UniqueConstraint(
            "leader_address",
            "binance_symbol",
            "position_side",
            name="uq_leader_position_allocation_side",
        ),
    )
    op.create_index("ix_leader_position_allocations_leader_id", "leader_position_allocations", ["leader_id"])
    op.create_index("ix_leader_position_allocations_leader_address", "leader_position_allocations", ["leader_address"])
    op.create_index("ix_leader_position_allocations_hyperliquid_coin", "leader_position_allocations", ["hyperliquid_coin"])
    op.create_index("ix_leader_position_allocations_binance_symbol", "leader_position_allocations", ["binance_symbol"])
    op.create_index("ix_leader_position_allocations_position_side", "leader_position_allocations", ["position_side"])
    op.create_index("ix_leader_position_allocations_status", "leader_position_allocations", ["status"])
    op.create_index("ix_leader_position_allocations_last_source_fill_id", "leader_position_allocations", ["last_source_fill_id"])
    op.create_index(
        "ix_leader_position_allocations_symbol_side_status",
        "leader_position_allocations",
        ["binance_symbol", "position_side", "status"],
    )

    op.execute(
        sa.text(
            """
            insert into leader_position_allocations (
                leader_id,
                leader_address,
                hyperliquid_coin,
                binance_symbol,
                position_side,
                target_notional,
                allocated_notional,
                allocated_qty,
                avg_entry_price,
                last_leader_account_value,
                last_leader_position_notional,
                copy_multiplier,
                status,
                created_at,
                updated_at
            )
            select
                lc.id,
                dp.leader_address,
                dp.coin,
                dp.binance_symbol,
                case when dp.target_notional < 0 then 'SHORT' else 'LONG' end,
                abs(dp.target_notional),
                0,
                0,
                null,
                null,
                dp.target_notional,
                coalesce(lc.copy_multiplier, 0.1),
                'NEEDS_MANUAL_REVIEW',
                now(),
                now()
            from desired_positions dp
            left join leader_configs lc on lower(lc.leader_address) = lower(dp.leader_address)
            where dp.target_notional <> 0
            on conflict on constraint uq_leader_position_allocation_side do nothing
            """
        )
    )


def downgrade() -> None:
    op.drop_table("leader_position_allocations")
    op.drop_index("ix_execution_orders_position_side", table_name="execution_orders")
    op.drop_column("execution_orders", "order_action")
    op.drop_column("execution_orders", "position_side")
