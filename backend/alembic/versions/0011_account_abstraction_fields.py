"""account abstraction sizing source fields

Revision ID: 0011_account_abstraction_fields
Revises: 0010_hyperliquid_dex_support
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0011_account_abstraction_fields"
down_revision = "0010_hyperliquid_dex_support"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("execution_orders", sa.Column("leader_account_value_source", sa.String(length=64), nullable=True))
    op.add_column("execution_orders", sa.Column("leader_account_abstraction_mode", sa.String(length=32), nullable=True))
    op.add_column("execution_orders", sa.Column("follower_account_value_source", sa.String(length=64), nullable=True))
    op.add_column("execution_orders", sa.Column("follower_account_abstraction_mode", sa.String(length=32), nullable=True))


def downgrade() -> None:
    op.drop_column("execution_orders", "follower_account_abstraction_mode")
    op.drop_column("execution_orders", "follower_account_value_source")
    op.drop_column("execution_orders", "leader_account_abstraction_mode")
    op.drop_column("execution_orders", "leader_account_value_source")
