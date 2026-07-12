"""hyperliquid dex support

Revision ID: 0010_hyperliquid_dex_support
Revises: 0009_account_ratio_sizing
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0010_hyperliquid_dex_support"
down_revision = "0009_account_ratio_sizing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "latest_account_states",
        sa.Column("dex", sa.String(length=32), nullable=False, server_default=""),
    )
    op.add_column("latest_account_states", sa.Column("dex_display_name", sa.String(length=80), nullable=True))
    op.drop_constraint("uq_latest_account_states_role_address", "latest_account_states", type_="unique")
    op.create_unique_constraint(
        "uq_latest_account_states_role_address_dex",
        "latest_account_states",
        ["role", "address", "dex"],
    )
    op.create_index("ix_latest_account_states_dex", "latest_account_states", ["dex"])

    op.add_column(
        "latest_account_positions",
        sa.Column("dex", sa.String(length=32), nullable=False, server_default=""),
    )
    op.add_column("latest_account_positions", sa.Column("canonical_coin", sa.String(length=80), nullable=True))
    op.add_column("latest_account_positions", sa.Column("raw_coin", sa.String(length=80), nullable=True))
    op.add_column("latest_account_positions", sa.Column("product_type", sa.String(length=80), nullable=True))
    op.create_index("ix_latest_account_positions_dex", "latest_account_positions", ["dex"])
    op.create_index("ix_latest_account_positions_canonical_coin", "latest_account_positions", ["canonical_coin"])

    op.add_column(
        "leader_position_allocations",
        sa.Column("dex", sa.String(length=32), nullable=False, server_default=""),
    )
    op.add_column("leader_position_allocations", sa.Column("canonical_coin", sa.String(length=80), nullable=True))
    op.create_index("ix_leader_position_allocations_dex", "leader_position_allocations", ["dex"])
    op.create_index(
        "ix_leader_position_allocations_canonical_coin",
        "leader_position_allocations",
        ["canonical_coin"],
    )

    op.add_column(
        "execution_orders",
        sa.Column("dex", sa.String(length=32), nullable=False, server_default=""),
    )
    op.add_column("execution_orders", sa.Column("canonical_coin", sa.String(length=80), nullable=True))
    op.add_column("execution_orders", sa.Column("raw_coin_from_fill", sa.String(length=80), nullable=True))
    op.add_column("execution_orders", sa.Column("asset_id", sa.BigInteger(), nullable=True))
    op.create_index("ix_execution_orders_dex", "execution_orders", ["dex"])
    op.create_index("ix_execution_orders_canonical_coin", "execution_orders", ["canonical_coin"])


def downgrade() -> None:
    op.drop_index("ix_execution_orders_canonical_coin", table_name="execution_orders")
    op.drop_index("ix_execution_orders_dex", table_name="execution_orders")
    op.drop_column("execution_orders", "asset_id")
    op.drop_column("execution_orders", "raw_coin_from_fill")
    op.drop_column("execution_orders", "canonical_coin")
    op.drop_column("execution_orders", "dex")

    op.drop_index("ix_leader_position_allocations_canonical_coin", table_name="leader_position_allocations")
    op.drop_index("ix_leader_position_allocations_dex", table_name="leader_position_allocations")
    op.drop_column("leader_position_allocations", "canonical_coin")
    op.drop_column("leader_position_allocations", "dex")

    op.drop_index("ix_latest_account_positions_canonical_coin", table_name="latest_account_positions")
    op.drop_index("ix_latest_account_positions_dex", table_name="latest_account_positions")
    op.drop_column("latest_account_positions", "product_type")
    op.drop_column("latest_account_positions", "raw_coin")
    op.drop_column("latest_account_positions", "canonical_coin")
    op.drop_column("latest_account_positions", "dex")

    op.drop_index("ix_latest_account_states_dex", table_name="latest_account_states")
    op.drop_constraint("uq_latest_account_states_role_address_dex", "latest_account_states", type_="unique")
    op.create_unique_constraint(
        "uq_latest_account_states_role_address",
        "latest_account_states",
        ["role", "address"],
    )
    op.drop_column("latest_account_states", "dex_display_name")
    op.drop_column("latest_account_states", "dex")
