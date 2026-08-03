"""add durable fill inbox retry state

Revision ID: 0026_durable_fill_inbox
Revises: 0025_alloc_fill_idempotency
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0026_durable_fill_inbox"
down_revision = "0025_alloc_fill_idempotency"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "source_fills",
        sa.Column("processing_attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "source_fills",
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "source_fills",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "source_fills",
        sa.Column("last_processing_error", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_source_fills_last_attempt_at",
        "source_fills",
        ["last_attempt_at"],
        unique=False,
    )
    op.create_index(
        "ix_source_fills_next_retry_at",
        "source_fills",
        ["next_retry_at"],
        unique=False,
    )
    op.execute(
        """
        CREATE INDEX ix_source_fills_pending_retry
            ON source_fills (COALESCE(next_retry_at, created_at), source_time_ms, id)
            WHERE processed_at IS NULL AND is_snapshot IS FALSE
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_source_fills_pending_retry")
    op.drop_index("ix_source_fills_next_retry_at", table_name="source_fills")
    op.drop_index("ix_source_fills_last_attempt_at", table_name="source_fills")
    op.drop_column("source_fills", "last_processing_error")
    op.drop_column("source_fills", "next_retry_at")
    op.drop_column("source_fills", "last_attempt_at")
    op.drop_column("source_fills", "processing_attempts")
