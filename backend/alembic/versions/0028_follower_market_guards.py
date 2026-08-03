"""add durable follower market guards

Revision ID: 0028_follower_market_guards
Revises: 0027_exactly_once_pipeline
Create Date: 2026-07-14
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0028_follower_market_guards"
down_revision = "0027_exactly_once_pipeline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "follower_market_guards",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("execution_venue", sa.String(length=16), nullable=False),
        sa.Column("dex", sa.String(length=32), nullable=False, server_default=""),
        sa.Column("canonical_coin", sa.String(length=160), nullable=False),
        sa.Column("position_version", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconciled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_unmatched_fill_id", sa.String(length=64), nullable=True),
        sa.Column("last_cloid", sa.String(length=34), nullable=True),
        sa.Column("last_order_id", sa.String(length=80), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "execution_venue",
            "dex",
            "canonical_coin",
            name="uq_follower_market_guard_scope",
        ),
    )
    op.create_index(
        "ix_follower_market_guards_active_scope",
        "follower_market_guards",
        ["active", "execution_venue", "dex", "canonical_coin"],
        unique=False,
    )
    op.create_index(
        "ix_follower_market_guards_active",
        "follower_market_guards",
        ["active"],
        unique=False,
    )
    op.create_index(
        "ix_follower_market_guards_execution_venue",
        "follower_market_guards",
        ["execution_venue"],
        unique=False,
    )
    op.create_index(
        "ix_follower_market_guards_dex",
        "follower_market_guards",
        ["dex"],
        unique=False,
    )
    op.create_index(
        "ix_follower_market_guards_canonical_coin",
        "follower_market_guards",
        ["canonical_coin"],
        unique=False,
    )
    op.create_index(
        "ix_follower_market_guards_last_unmatched_fill_id",
        "follower_market_guards",
        ["last_unmatched_fill_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_follower_market_guards_last_unmatched_fill_id", table_name="follower_market_guards")
    op.drop_index("ix_follower_market_guards_canonical_coin", table_name="follower_market_guards")
    op.drop_index("ix_follower_market_guards_dex", table_name="follower_market_guards")
    op.drop_index("ix_follower_market_guards_execution_venue", table_name="follower_market_guards")
    op.drop_index("ix_follower_market_guards_active", table_name="follower_market_guards")
    op.drop_index("ix_follower_market_guards_active_scope", table_name="follower_market_guards")
    op.drop_table("follower_market_guards")
