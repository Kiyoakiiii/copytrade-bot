"""position lifecycle realtime fields

Revision ID: 0017_position_lifecycle_realtime
Revises: 0016_entry_price_market_coverage
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0017_position_lifecycle_realtime"
down_revision = "0016_entry_price_market_coverage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("latest_account_positions", sa.Column("mid_px", sa.Numeric(30, 12), nullable=True))
    op.add_column("latest_account_positions", sa.Column("mark_px_source", sa.String(length=32), nullable=True))
    op.add_column("latest_account_positions", sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()))
    op.add_column("latest_account_positions", sa.Column("status", sa.String(length=16), nullable=False, server_default="OPEN"))
    op.add_column("latest_account_positions", sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("latest_account_positions", sa.Column("position_opened_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("latest_account_positions", sa.Column("open_time_source", sa.String(length=32), nullable=True))
    op.add_column("latest_account_positions", sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True))
    op.execute(
        """
        update latest_account_positions
        set
            mid_px = coalesce(mid_px, mark_px),
            mark_px_source = coalesce(mark_px_source, 'ACCOUNT_STATE'),
            first_seen_at = coalesce(first_seen_at, last_update_at, created_at),
            open_time_source = coalesce(open_time_source, 'FIRST_SEEN')
        """
    )
    op.create_index(
        "ix_latest_account_positions_active_scope",
        "latest_account_positions",
        ["role", "address", "dex", "canonical_coin", "active"],
    )
    op.create_index("ix_latest_account_positions_active", "latest_account_positions", ["active"])
    op.create_index("ix_latest_account_positions_status", "latest_account_positions", ["status"])
    op.create_index("ix_latest_account_positions_first_seen_at", "latest_account_positions", ["first_seen_at"])
    op.create_index("ix_latest_account_positions_closed_at", "latest_account_positions", ["closed_at"])


def downgrade() -> None:
    op.drop_index("ix_latest_account_positions_closed_at", table_name="latest_account_positions")
    op.drop_index("ix_latest_account_positions_first_seen_at", table_name="latest_account_positions")
    op.drop_index("ix_latest_account_positions_status", table_name="latest_account_positions")
    op.drop_index("ix_latest_account_positions_active", table_name="latest_account_positions")
    op.drop_index("ix_latest_account_positions_active_scope", table_name="latest_account_positions")
    op.drop_column("latest_account_positions", "closed_at")
    op.drop_column("latest_account_positions", "open_time_source")
    op.drop_column("latest_account_positions", "position_opened_at")
    op.drop_column("latest_account_positions", "first_seen_at")
    op.drop_column("latest_account_positions", "status")
    op.drop_column("latest_account_positions", "active")
    op.drop_column("latest_account_positions", "mark_px_source")
    op.drop_column("latest_account_positions", "mid_px")
