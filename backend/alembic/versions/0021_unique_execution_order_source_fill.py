"""make execution order source fills unique

Revision ID: 0021_unique_order_fill
Revises: 0020_deferred_reduce_fields
Create Date: 2026-05-09
"""

from __future__ import annotations

from alembic import op

revision = "0021_unique_order_fill"
down_revision = "0020_deferred_reduce_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_execution_orders_source_fill_id", table_name="execution_orders")
    op.create_index(
        "ix_execution_orders_source_fill_id",
        "execution_orders",
        ["source_fill_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_execution_orders_source_fill_id", table_name="execution_orders")
    op.create_index(
        "ix_execution_orders_source_fill_id",
        "execution_orders",
        ["source_fill_id"],
        unique=False,
    )
