"""index durable market fill FIFO and arrival replay

Revision ID: 0030_market_fill_fifo_idx
Revises: 0029_unmatched_fill_dedupe
Create Date: 2026-07-15
"""

from __future__ import annotations

from alembic import op


revision = "0030_market_fill_fifo_idx"
down_revision = "0029_unmatched_fill_dedupe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_source_fills_pending_market_arrival
            ON source_fills (dex, UPPER(canonical_coin), id)
            WHERE processed_at IS NULL AND is_snapshot IS FALSE
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_source_fills_pending_arrival
            ON source_fills (id)
            WHERE processed_at IS NULL AND is_snapshot IS FALSE
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_source_fills_pending_arrival")
    op.execute("DROP INDEX IF EXISTS ix_source_fills_pending_market_arrival")
