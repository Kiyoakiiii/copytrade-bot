"""allocation transition and latency hardening

Revision ID: 0014_allocation_latency
Revises: 0013_fast_market_only
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0014_allocation_latency"
down_revision = "0013_fast_market_only"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "allocation_events",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("allocation_id", sa.BigInteger(), nullable=True),
        sa.Column("execution_order_id", sa.BigInteger(), nullable=True),
        sa.Column("leader_id", sa.BigInteger(), nullable=True),
        sa.Column("leader_address", sa.String(length=64), nullable=False),
        sa.Column("source_fill_id", sa.String(length=160), nullable=True),
        sa.Column("execution_venue", sa.String(length=16), nullable=False),
        sa.Column("dex", sa.String(length=32), nullable=False, server_default=""),
        sa.Column("canonical_coin", sa.String(length=80), nullable=True),
        sa.Column("position_side", sa.String(length=8), nullable=True),
        sa.Column("action", sa.String(length=40), nullable=False),
        sa.Column("before_notional", sa.Numeric(30, 12), nullable=True),
        sa.Column("after_notional", sa.Numeric(30, 12), nullable=True),
        sa.Column("before_qty", sa.Numeric(30, 12), nullable=True),
        sa.Column("after_qty", sa.Numeric(30, 12), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["allocation_id"], ["leader_position_allocations.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["execution_order_id"], ["execution_orders.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["leader_id"], ["leader_configs.id"], ondelete="SET NULL"),
    )
    for column in [
        "allocation_id",
        "execution_order_id",
        "leader_id",
        "leader_address",
        "source_fill_id",
        "execution_venue",
        "dex",
        "canonical_coin",
        "position_side",
        "action",
    ]:
        op.create_index(f"ix_allocation_events_{column}", "allocation_events", [column])

    for column in [
        "leader_event_to_ws_ms",
        "ws_to_parse_ms",
        "parse_to_dedupe_ms",
        "lock_wait_ms",
        "cache_read_ms",
        "sizing_ms",
        "checklist_ms",
        "allocation_update_ms",
        "total_hot_path_ms",
    ]:
        op.add_column("execution_orders", sa.Column(column, sa.BigInteger(), nullable=True))
    op.add_column("execution_orders", sa.Column("latency_trace_id", sa.String(length=80), nullable=True))
    op.add_column("execution_orders", sa.Column("latency_trace", sa.JSON(), nullable=True))
    op.add_column("execution_orders", sa.Column("missing_latency_fields", sa.JSON(), nullable=True))
    op.create_index("ix_execution_orders_latency_trace_id", "execution_orders", ["latency_trace_id"])


def downgrade() -> None:
    op.drop_index("ix_execution_orders_latency_trace_id", table_name="execution_orders")
    for column in ["missing_latency_fields", "latency_trace", "latency_trace_id"]:
        op.drop_column("execution_orders", column)
    for column in [
        "total_hot_path_ms",
        "allocation_update_ms",
        "checklist_ms",
        "sizing_ms",
        "cache_read_ms",
        "lock_wait_ms",
        "parse_to_dedupe_ms",
        "ws_to_parse_ms",
        "leader_event_to_ws_ms",
    ]:
        op.drop_column("execution_orders", column)

    for column in [
        "action",
        "position_side",
        "canonical_coin",
        "dex",
        "execution_venue",
        "source_fill_id",
        "leader_address",
        "leader_id",
        "execution_order_id",
        "allocation_id",
    ]:
        op.drop_index(f"ix_allocation_events_{column}", table_name="allocation_events")
    op.drop_table("allocation_events")
