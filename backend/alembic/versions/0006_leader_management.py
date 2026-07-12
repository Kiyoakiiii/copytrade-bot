"""leader management soft delete and allow-all mode

Revision ID: 0006_leader_management
Revises: 0005_venues
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0006_leader_management"
down_revision = "0005_venues"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("leader_configs", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("leader_configs", sa.Column("delete_reason", sa.Text(), nullable=True))
    op.create_index("ix_leader_configs_deleted_at", "leader_configs", ["deleted_at"])
    op.alter_column(
        "leader_configs",
        "allowed_symbols",
        existing_type=sa.JSON(),
        nullable=True,
        server_default=None,
    )


def downgrade() -> None:
    op.execute("update leader_configs set allowed_symbols = '[]'::json where allowed_symbols is null")
    op.alter_column(
        "leader_configs",
        "allowed_symbols",
        existing_type=sa.JSON(),
        nullable=False,
        server_default="[]",
    )
    op.drop_index("ix_leader_configs_deleted_at", table_name="leader_configs")
    op.drop_column("leader_configs", "delete_reason")
    op.drop_column("leader_configs", "deleted_at")
