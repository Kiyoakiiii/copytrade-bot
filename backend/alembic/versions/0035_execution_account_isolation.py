"""isolate durable copy state by follower execution account

Revision ID: 0035_execution_account_isolation
Revises: 0034_economic_dust_reopen
Create Date: 2026-08-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0035_execution_account_isolation"
down_revision = "0034_economic_dust_reopen"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "source_fills",
        sa.Column("execution_account", sa.String(length=64), nullable=False, server_default=""),
    )
    op.create_index("ix_source_fills_execution_account", "source_fills", ["execution_account"])

    op.add_column(
        "execution_orders",
        sa.Column("venue_account", sa.String(length=64), nullable=False, server_default=""),
    )
    op.create_index("ix_execution_orders_venue_account", "execution_orders", ["venue_account"])

    op.execute("UPDATE leader_position_allocations SET venue_account = '' WHERE venue_account IS NULL")
    op.alter_column(
        "leader_position_allocations",
        "venue_account",
        existing_type=sa.String(length=64),
        nullable=False,
        server_default="",
    )

    op.add_column(
        "follower_market_guards",
        sa.Column("execution_account", sa.String(length=64), nullable=False, server_default=""),
    )
    op.create_index(
        "ix_follower_market_guards_execution_account",
        "follower_market_guards",
        ["execution_account"],
    )
    op.drop_constraint(
        "uq_follower_market_guard_scope",
        "follower_market_guards",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_follower_market_guard_scope",
        "follower_market_guards",
        ["execution_account", "execution_venue", "dex", "canonical_coin"],
    )
    op.drop_index("ix_follower_market_guards_active_scope", table_name="follower_market_guards")
    op.create_index(
        "ix_follower_market_guards_active_scope",
        "follower_market_guards",
        ["active", "execution_account", "execution_venue", "dex", "canonical_coin"],
    )

    op.add_column(
        "unmatched_follower_fills",
        sa.Column("execution_account", sa.String(length=64), nullable=False, server_default=""),
    )
    op.create_index(
        "ix_unmatched_follower_fills_execution_account",
        "unmatched_follower_fills",
        ["execution_account"],
    )
    op.drop_index("ix_unmatched_follower_fills_scope_time", table_name="unmatched_follower_fills")
    op.create_index(
        "ix_unmatched_follower_fills_scope_time",
        "unmatched_follower_fills",
        ["execution_venue", "execution_account", "dex", "canonical_coin", "observed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_unmatched_follower_fills_scope_time", table_name="unmatched_follower_fills")
    op.drop_index("ix_unmatched_follower_fills_execution_account", table_name="unmatched_follower_fills")
    op.drop_column("unmatched_follower_fills", "execution_account")
    op.create_index(
        "ix_unmatched_follower_fills_scope_time",
        "unmatched_follower_fills",
        ["execution_venue", "dex", "canonical_coin", "observed_at"],
    )

    op.drop_index("ix_follower_market_guards_active_scope", table_name="follower_market_guards")
    op.drop_constraint("uq_follower_market_guard_scope", "follower_market_guards", type_="unique")
    op.drop_index("ix_follower_market_guards_execution_account", table_name="follower_market_guards")
    op.drop_column("follower_market_guards", "execution_account")
    op.create_unique_constraint(
        "uq_follower_market_guard_scope",
        "follower_market_guards",
        ["execution_venue", "dex", "canonical_coin"],
    )
    op.create_index(
        "ix_follower_market_guards_active_scope",
        "follower_market_guards",
        ["active", "execution_venue", "dex", "canonical_coin"],
    )

    op.alter_column(
        "leader_position_allocations",
        "venue_account",
        existing_type=sa.String(length=64),
        nullable=True,
        server_default=None,
    )
    op.drop_index("ix_execution_orders_venue_account", table_name="execution_orders")
    op.drop_column("execution_orders", "venue_account")
    op.drop_index("ix_source_fills_execution_account", table_name="source_fills")
    op.drop_column("source_fills", "execution_account")
