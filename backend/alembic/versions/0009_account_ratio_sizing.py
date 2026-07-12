"""account ratio sizing audit fields

Revision ID: 0009_account_ratio_sizing
Revises: 0008_account_states
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0009_account_ratio_sizing"
down_revision = "0008_account_states"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("execution_orders", sa.Column("sizing_mode", sa.String(length=32), nullable=True))
    op.add_column("execution_orders", sa.Column("leader_account_value", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("leader_position_notional", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("follower_account_value", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("leader_position_ratio", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("copy_multiplier", sa.Numeric(20, 8), nullable=True))
    op.add_column("execution_orders", sa.Column("target_notional", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("delta_notional", sa.Numeric(30, 12), nullable=True))


def downgrade() -> None:
    for column in [
        "delta_notional",
        "target_notional",
        "copy_multiplier",
        "leader_position_ratio",
        "follower_account_value",
        "leader_position_notional",
        "leader_account_value",
        "sizing_mode",
    ]:
        op.drop_column("execution_orders", column)
