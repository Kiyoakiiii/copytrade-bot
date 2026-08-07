"""add durable deduplication for execution risk alerts

Revision ID: 0036_leader_liquidation_safety
Revises: 0035_execution_account_isolation
Create Date: 2026-08-05
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0036_leader_liquidation_safety"
down_revision = "0035_execution_account_isolation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "risk_events",
        sa.Column("dedupe_key", sa.String(length=200), nullable=True),
    )
    op.create_index(
        "ix_risk_events_dedupe_key",
        "risk_events",
        ["dedupe_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_risk_events_dedupe_key", table_name="risk_events")
    op.drop_column("risk_events", "dedupe_key")
