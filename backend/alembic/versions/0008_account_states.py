"""latest follower and leader account states

Revision ID: 0008_account_states
Revises: 0007_clear_legacy_allowed
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0008_account_states"
down_revision = "0007_clear_legacy_allowed"
branch_labels = None
depends_on = None


def id_col() -> sa.Column:
    return sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True)


def timestamps() -> list[sa.Column]:
    return [
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    ]


def upgrade() -> None:
    op.create_table(
        "latest_account_states",
        id_col(),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("address", sa.String(length=64), nullable=False),
        sa.Column("account_label", sa.String(length=120), nullable=True),
        sa.Column("account_value", sa.Numeric(30, 12), nullable=True),
        sa.Column("withdrawable", sa.Numeric(30, 12), nullable=True),
        sa.Column("total_ntl_pos", sa.Numeric(30, 12), nullable=True),
        sa.Column("total_raw_usd", sa.Numeric(30, 12), nullable=True),
        sa.Column("total_margin_used", sa.Numeric(30, 12), nullable=True),
        sa.Column("raw_payload_masked", sa.JSON(), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="info_endpoint"),
        sa.Column("last_update_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        *timestamps(),
        sa.UniqueConstraint("role", "address", name="uq_latest_account_states_role_address"),
    )
    op.create_index("ix_latest_account_states_role", "latest_account_states", ["role"])
    op.create_index("ix_latest_account_states_address", "latest_account_states", ["address"])
    op.create_index("ix_latest_account_states_last_update_at", "latest_account_states", ["last_update_at"])

    op.create_table(
        "latest_account_positions",
        id_col(),
        sa.Column(
            "account_state_id",
            sa.BigInteger(),
            sa.ForeignKey("latest_account_states.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("address", sa.String(length=64), nullable=False),
        sa.Column("coin", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("size", sa.Numeric(30, 12), nullable=True),
        sa.Column("notional", sa.Numeric(30, 12), nullable=True),
        sa.Column("entry_px", sa.Numeric(30, 12), nullable=True),
        sa.Column("mark_px", sa.Numeric(30, 12), nullable=True),
        sa.Column("unrealized_pnl", sa.Numeric(30, 12), nullable=True),
        sa.Column("leverage", sa.Numeric(20, 8), nullable=True),
        sa.Column("margin_used", sa.Numeric(30, 12), nullable=True),
        sa.Column("liquidation_px", sa.Numeric(30, 12), nullable=True),
        sa.Column("raw_payload_masked", sa.JSON(), nullable=True),
        sa.Column("last_update_at", sa.DateTime(timezone=True), nullable=True),
        *timestamps(),
    )
    op.create_index("ix_latest_account_positions_state_id", "latest_account_positions", ["account_state_id"])
    op.create_index("ix_latest_account_positions_role_address", "latest_account_positions", ["role", "address"])
    op.create_index("ix_latest_account_positions_coin", "latest_account_positions", ["coin"])


def downgrade() -> None:
    op.drop_index("ix_latest_account_positions_coin", table_name="latest_account_positions")
    op.drop_index("ix_latest_account_positions_role_address", table_name="latest_account_positions")
    op.drop_index("ix_latest_account_positions_state_id", table_name="latest_account_positions")
    op.drop_table("latest_account_positions")
    op.drop_index("ix_latest_account_states_last_update_at", table_name="latest_account_states")
    op.drop_index("ix_latest_account_states_address", table_name="latest_account_states")
    op.drop_index("ix_latest_account_states_role", table_name="latest_account_states")
    op.drop_table("latest_account_states")
