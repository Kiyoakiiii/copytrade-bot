"""track the current leader activation epoch for performance analytics

Revision ID: 0032_leader_perf_epoch
Revises: 0031_manual_fill_confirm
Create Date: 2026-07-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0032_leader_perf_epoch"
down_revision = "0031_manual_fill_confirm"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "leader_configs",
        sa.Column("performance_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    # A watcher records one snapshot batch whenever it starts tracking a leader.
    # The latest snapshot initialization is therefore the best durable recovery
    # point for leaders that were deleted and later added again. For leaders that
    # were never re-added, this is their original initialization batch.
    op.execute(
        """
        UPDATE leader_configs AS leader
        SET performance_started_at = COALESCE(
            (
                SELECT MAX(fill.created_at)
                FROM source_fills AS fill
                WHERE LOWER(fill.leader_address) = LOWER(leader.leader_address)
                  AND fill.is_snapshot IS TRUE
            ),
            leader.created_at
        )
        """
    )
    op.alter_column(
        "leader_configs",
        "performance_started_at",
        nullable=False,
        server_default=sa.text("now()"),
    )
    op.create_index(
        "ix_leader_configs_performance_started_at",
        "leader_configs",
        ["performance_started_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_leader_configs_performance_started_at", table_name="leader_configs")
    op.drop_column("leader_configs", "performance_started_at")
