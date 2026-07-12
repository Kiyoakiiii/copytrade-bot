"""preflight leader state

Revision ID: 0002_preflight_leader_state
Revises: 0001_initial
Create Date: 2026-04-25
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002_preflight_leader_state"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def timestamps() -> list[sa.Column]:
    return [
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    ]


def upgrade() -> None:
    op.add_column("execution_orders", sa.Column("pre_trade_checklist", sa.JSON(), nullable=True))
    op.create_table(
        "latest_leader_states",
        sa.Column("leader_address", sa.String(length=64), primary_key=True),
        sa.Column("account_value", sa.Numeric(30, 12), nullable=False),
        sa.Column("withdrawable", sa.Numeric(30, 12), nullable=True),
        sa.Column("total_ntl_pos", sa.Numeric(30, 12), nullable=True),
        sa.Column("total_margin_used", sa.Numeric(30, 12), nullable=True),
        sa.Column("positions", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("websocket_status", sa.String(length=32), nullable=False, server_default="unknown"),
        sa.Column("last_update_at", sa.DateTime(timezone=True), nullable=False),
        *timestamps(),
    )
    op.create_index(
        "ix_latest_leader_states_last_update_at",
        "latest_leader_states",
        ["last_update_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_latest_leader_states_last_update_at", table_name="latest_leader_states")
    op.drop_table("latest_leader_states")
    op.drop_column("execution_orders", "pre_trade_checklist")

