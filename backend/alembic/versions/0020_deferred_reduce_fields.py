"""deferred reduce fields

Revision ID: 0020_deferred_reduce_fields
Revises: 0019_allocation_leader_size
Create Date: 2026-05-03
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0020_deferred_reduce_fields"
down_revision = "0019_allocation_leader_size"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "leader_position_allocations",
        sa.Column("pending_reduce_qty", sa.Numeric(30, 12), nullable=True),
    )
    op.add_column(
        "leader_position_allocations",
        sa.Column("pending_reduce_notional", sa.Numeric(30, 12), nullable=True),
    )
    op.add_column(
        "leader_position_allocations",
        sa.Column("pending_reduce_reason", sa.String(1000), nullable=True),
    )
    op.add_column(
        "leader_position_allocations",
        sa.Column("pending_reduce_since", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "leader_position_allocations",
        sa.Column("pending_reduce_source_fill_id", sa.String(160), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("leader_position_allocations", "pending_reduce_source_fill_id")
    op.drop_column("leader_position_allocations", "pending_reduce_since")
    op.drop_column("leader_position_allocations", "pending_reduce_reason")
    op.drop_column("leader_position_allocations", "pending_reduce_notional")
    op.drop_column("leader_position_allocations", "pending_reduce_qty")
