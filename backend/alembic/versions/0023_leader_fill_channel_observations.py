"""record leader fill websocket channel observations

Revision ID: 0023_fill_channel_obs
Revises: 0022_unique_active_hl_alloc
Create Date: 2026-06-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0023_fill_channel_obs"
down_revision = "0022_unique_active_hl_alloc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "leader_fill_channel_observations",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("source_fill_id", sa.String(length=160), nullable=False),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("leader_address", sa.String(length=64), nullable=False),
        sa.Column("dex", sa.String(length=32), nullable=False, server_default=""),
        sa.Column("canonical_coin", sa.String(length=160), nullable=True),
        sa.Column("raw_coin", sa.String(length=160), nullable=True),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("price", sa.Numeric(30, 12), nullable=False),
        sa.Column("size", sa.Numeric(30, 12), nullable=False),
        sa.Column("source_time_ms", sa.BigInteger(), nullable=False),
        sa.Column("ws_received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_to_ws_ms", sa.BigInteger(), nullable=True),
        sa.Column("raw_fill", sa.JSON(), nullable=False),
        sa.Column("is_snapshot", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("source_fill_id", "channel", name="uq_leader_fill_observation_fill_channel"),
    )
    op.create_index(
        "ix_leader_fill_channel_observations_source_fill_id",
        "leader_fill_channel_observations",
        ["source_fill_id"],
    )
    op.create_index(
        "ix_leader_fill_channel_observations_channel",
        "leader_fill_channel_observations",
        ["channel"],
    )
    op.create_index(
        "ix_leader_fill_channel_observations_leader_address",
        "leader_fill_channel_observations",
        ["leader_address"],
    )
    op.create_index(
        "ix_leader_fill_channel_observations_dex",
        "leader_fill_channel_observations",
        ["dex"],
    )
    op.create_index(
        "ix_leader_fill_channel_observations_canonical_coin",
        "leader_fill_channel_observations",
        ["canonical_coin"],
    )
    op.create_index(
        "ix_leader_fill_channel_observations_source_time_ms",
        "leader_fill_channel_observations",
        ["source_time_ms"],
    )
    op.create_index(
        "ix_leader_fill_channel_observations_ws_received_at",
        "leader_fill_channel_observations",
        ["ws_received_at"],
    )
    op.create_index(
        "ix_leader_fill_channel_observations_event_to_ws_ms",
        "leader_fill_channel_observations",
        ["event_to_ws_ms"],
    )
    op.create_index(
        "ix_leader_fill_observations_leader_channel_time",
        "leader_fill_channel_observations",
        ["leader_address", "channel", "source_time_ms"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_leader_fill_observations_leader_channel_time",
        table_name="leader_fill_channel_observations",
    )
    op.drop_index(
        "ix_leader_fill_channel_observations_event_to_ws_ms",
        table_name="leader_fill_channel_observations",
    )
    op.drop_index(
        "ix_leader_fill_channel_observations_ws_received_at",
        table_name="leader_fill_channel_observations",
    )
    op.drop_index(
        "ix_leader_fill_channel_observations_source_time_ms",
        table_name="leader_fill_channel_observations",
    )
    op.drop_index(
        "ix_leader_fill_channel_observations_canonical_coin",
        table_name="leader_fill_channel_observations",
    )
    op.drop_index("ix_leader_fill_channel_observations_dex", table_name="leader_fill_channel_observations")
    op.drop_index(
        "ix_leader_fill_channel_observations_leader_address",
        table_name="leader_fill_channel_observations",
    )
    op.drop_index(
        "ix_leader_fill_channel_observations_channel",
        table_name="leader_fill_channel_observations",
    )
    op.drop_index(
        "ix_leader_fill_channel_observations_source_fill_id",
        table_name="leader_fill_channel_observations",
    )
    op.drop_table("leader_fill_channel_observations")
