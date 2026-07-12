"""prevent duplicate active Hyperliquid allocations

Revision ID: 0022_unique_active_hl_alloc
Revises: 0021_unique_order_fill
Create Date: 2026-05-20
"""

from __future__ import annotations

from alembic import op

revision = "0022_unique_active_hl_alloc"
down_revision = "0021_unique_order_fill"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_active_hyperliquid_allocation_scope
            ON leader_position_allocations (
                leader_id,
                execution_venue,
                dex,
                canonical_coin,
                position_side
            )
            WHERE execution_venue = 'HYPERLIQUID'
              AND status <> 'CLOSED'
              AND leader_id IS NOT NULL
              AND canonical_coin IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_active_hyperliquid_allocation_scope")
