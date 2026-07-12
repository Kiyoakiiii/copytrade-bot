"""leader position baselines

Revision ID: 0015_leader_position_baselines
Revises: 0014_allocation_latency
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0015_leader_position_baselines"
down_revision = "0014_allocation_latency"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "leader_position_baselines",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("leader_id", sa.BigInteger(), nullable=True),
        sa.Column("leader_address", sa.String(length=64), nullable=False),
        sa.Column("execution_venue", sa.String(length=16), nullable=False, server_default="HYPERLIQUID"),
        sa.Column("dex", sa.String(length=32), nullable=False, server_default=""),
        sa.Column("canonical_coin", sa.String(length=80), nullable=False),
        sa.Column("side_at_enable", sa.String(length=8), nullable=False, server_default="FLAT"),
        sa.Column("size_at_enable", sa.Numeric(30, 12), nullable=True),
        sa.Column("notional_at_enable", sa.Numeric(30, 12), nullable=True),
        sa.Column("account_value_at_enable", sa.Numeric(30, 12), nullable=True),
        sa.Column("baseline_status", sa.String(length=32), nullable=False, server_default="WAIT_UNTIL_FLAT"),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("flat_confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("copy_allowed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_leader_size", sa.Numeric(30, 12), nullable=True),
        sa.Column("last_leader_notional", sa.Numeric(30, 12), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["leader_id"], ["leader_configs.id"], ondelete="SET NULL"),
        sa.UniqueConstraint(
            "leader_id",
            "execution_venue",
            "dex",
            "canonical_coin",
            name="uq_leader_position_baseline_scope",
        ),
    )
    for column in [
        "leader_id",
        "leader_address",
        "execution_venue",
        "dex",
        "canonical_coin",
        "baseline_status",
        "last_checked_at",
    ]:
        op.create_index(f"ix_leader_position_baselines_{column}", "leader_position_baselines", [column])
    op.create_index(
        "ix_leader_position_baselines_scope_status",
        "leader_position_baselines",
        ["leader_id", "execution_venue", "dex", "canonical_coin", "baseline_status"],
    )


def downgrade() -> None:
    op.drop_index("ix_leader_position_baselines_scope_status", table_name="leader_position_baselines")
    for column in [
        "last_checked_at",
        "baseline_status",
        "canonical_coin",
        "dex",
        "execution_venue",
        "leader_address",
        "leader_id",
    ]:
        op.drop_index(f"ix_leader_position_baselines_{column}", table_name="leader_position_baselines")
    op.drop_table("leader_position_baselines")
