"""confirm unmatched follower fills against their implied position

Revision ID: 0031_manual_fill_confirm
Revises: 0030_market_fill_fifo_idx
Create Date: 2026-07-20
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0031_manual_fill_confirm"
down_revision = "0030_market_fill_fifo_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "follower_market_guards",
        sa.Column("expected_position_side", sa.String(length=8), nullable=True),
    )
    op.add_column(
        "follower_market_guards",
        sa.Column("expected_position_qty", sa.Numeric(30, 12), nullable=True),
    )
    op.add_column(
        "follower_market_guards",
        sa.Column("expected_position_relation", sa.String(length=16), nullable=True),
    )
    op.add_column(
        "follower_market_guards",
        sa.Column("position_change_confirmed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("follower_market_guards", "position_change_confirmed_at")
    op.drop_column("follower_market_guards", "expected_position_relation")
    op.drop_column("follower_market_guards", "expected_position_qty")
    op.drop_column("follower_market_guards", "expected_position_side")
