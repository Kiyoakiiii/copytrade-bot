"""deduplicate every unmatched follower fill durably

Revision ID: 0029_unmatched_fill_dedupe
Revises: 0028_follower_market_guards
Create Date: 2026-07-14
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0029_unmatched_fill_dedupe"
down_revision = "0028_follower_market_guards"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "unmatched_follower_fills",
        sa.Column("follower_fill_id", sa.String(length=64), nullable=False),
        sa.Column("execution_venue", sa.String(length=16), nullable=False),
        sa.Column("dex", sa.String(length=32), nullable=False, server_default=""),
        sa.Column("canonical_coin", sa.String(length=160), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cloid", sa.String(length=34), nullable=True),
        sa.Column("order_id", sa.String(length=80), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("follower_fill_id"),
    )
    op.create_index(
        "ix_unmatched_follower_fills_scope_time",
        "unmatched_follower_fills",
        ["execution_venue", "dex", "canonical_coin", "observed_at"],
        unique=False,
    )
    op.create_index(
        "ix_unmatched_follower_fills_execution_venue",
        "unmatched_follower_fills",
        ["execution_venue"],
        unique=False,
    )
    op.create_index(
        "ix_unmatched_follower_fills_dex",
        "unmatched_follower_fills",
        ["dex"],
        unique=False,
    )
    op.create_index(
        "ix_unmatched_follower_fills_canonical_coin",
        "unmatched_follower_fills",
        ["canonical_coin"],
        unique=False,
    )
    op.create_index(
        "ix_unmatched_follower_fills_observed_at",
        "unmatched_follower_fills",
        ["observed_at"],
        unique=False,
    )
    op.execute(
        """
        INSERT INTO unmatched_follower_fills (
            follower_fill_id,
            execution_venue,
            dex,
            canonical_coin,
            observed_at,
            cloid,
            order_id,
            created_at,
            updated_at
        )
        SELECT
            last_unmatched_fill_id,
            execution_venue,
            dex,
            canonical_coin,
            COALESCE(observed_at, updated_at, created_at),
            last_cloid,
            last_order_id,
            COALESCE(observed_at, created_at),
            COALESCE(observed_at, updated_at)
        FROM follower_market_guards
        WHERE last_unmatched_fill_id IS NOT NULL
        ON CONFLICT (follower_fill_id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_index("ix_unmatched_follower_fills_observed_at", table_name="unmatched_follower_fills")
    op.drop_index("ix_unmatched_follower_fills_canonical_coin", table_name="unmatched_follower_fills")
    op.drop_index("ix_unmatched_follower_fills_dex", table_name="unmatched_follower_fills")
    op.drop_index("ix_unmatched_follower_fills_execution_venue", table_name="unmatched_follower_fills")
    op.drop_index("ix_unmatched_follower_fills_scope_time", table_name="unmatched_follower_fills")
    op.drop_table("unmatched_follower_fills")
