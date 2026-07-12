"""allocation leader size

Revision ID: 0019_allocation_leader_size
Revises: 0018_market_risk_settings
Create Date: 2026-05-03
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0019_allocation_leader_size"
down_revision = "0018_market_risk_settings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "leader_position_allocations",
        sa.Column("last_leader_position_size", sa.Numeric(30, 12), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("leader_position_allocations", "last_leader_position_size")
