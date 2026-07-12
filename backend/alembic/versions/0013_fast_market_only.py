"""fast market only policy metadata

Revision ID: 0013_fast_market_only
Revises: 0012_low_latency_watcher
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0013_fast_market_only"
down_revision = "0012_low_latency_watcher"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("execution_orders", "order_type", type_=sa.String(length=40), existing_type=sa.String(length=16))


def downgrade() -> None:
    op.alter_column("execution_orders", "order_type", type_=sa.String(length=16), existing_type=sa.String(length=40))
