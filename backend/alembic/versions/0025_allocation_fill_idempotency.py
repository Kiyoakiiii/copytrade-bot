"""harden allocation fill idempotency

Revision ID: 0025_alloc_fill_idempotency
Revises: 0024_fixed_leader_value
Create Date: 2026-07-09
"""

from __future__ import annotations

from alembic import op

revision = "0025_alloc_fill_idempotency"
down_revision = "0024_fixed_leader_value"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_allocation_events_fill_applied_order
            ON allocation_events (execution_order_id)
            WHERE action = 'FILL_APPLIED'
              AND execution_order_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_allocation_events_fill_applied_order")
