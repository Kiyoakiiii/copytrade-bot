"""market order recovery and latency fields

Revision ID: 0004_market_latency
Revises: 0003_hedge_allocations
Create Date: 2026-04-25
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0004_market_latency"
down_revision = "0003_hedge_allocations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("execution_orders", sa.Column("leader_id", sa.BigInteger(), nullable=True))
    op.add_column("execution_orders", sa.Column("allocation_id", sa.BigInteger(), nullable=True))
    op.add_column("execution_orders", sa.Column("source_type", sa.String(length=24), nullable=False, server_default="AUTO_COPY"))
    op.add_column("execution_orders", sa.Column("order_type", sa.String(length=16), nullable=False, server_default="MARKET"))
    op.add_column("execution_orders", sa.Column("client_order_id", sa.String(length=36), nullable=True))
    op.add_column("execution_orders", sa.Column("executed_qty", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("estimated_price", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("avg_fill_price", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("cum_quote", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("slippage_bps", sa.Numeric(20, 8), nullable=True))
    op.add_column("execution_orders", sa.Column("hyperliquid_event_time", sa.DateTime(timezone=True), nullable=True))
    op.add_column("execution_orders", sa.Column("event_received_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("execution_orders", sa.Column("decision_started_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("execution_orders", sa.Column("binance_order_submit_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("execution_orders", sa.Column("binance_order_ack_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("execution_orders", sa.Column("order_finalized_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("execution_orders", sa.Column("event_to_receive_ms", sa.BigInteger(), nullable=True))
    op.add_column("execution_orders", sa.Column("receive_to_submit_ms", sa.BigInteger(), nullable=True))
    op.add_column("execution_orders", sa.Column("submit_to_ack_ms", sa.BigInteger(), nullable=True))
    op.add_column("execution_orders", sa.Column("event_to_ack_ms", sa.BigInteger(), nullable=True))
    op.add_column("execution_orders", sa.Column("event_to_final_ms", sa.BigInteger(), nullable=True))

    op.create_foreign_key(
        "fk_execution_orders_leader_id",
        "execution_orders",
        "leader_configs",
        ["leader_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_execution_orders_allocation_id",
        "execution_orders",
        "leader_position_allocations",
        ["allocation_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_execution_orders_leader_id", "execution_orders", ["leader_id"])
    op.create_index("ix_execution_orders_allocation_id", "execution_orders", ["allocation_id"])
    op.create_index("ix_execution_orders_source_type", "execution_orders", ["source_type"])
    op.create_index("ix_execution_orders_client_order_id", "execution_orders", ["client_order_id"], unique=True)
    op.create_index(
        "ix_execution_orders_recovery_status",
        "execution_orders",
        ["source_type", "status"],
    )

    op.execute("update execution_orders set source_type = 'MANUAL' where leader_address = 'manual'")


def downgrade() -> None:
    op.drop_index("ix_execution_orders_recovery_status", table_name="execution_orders")
    op.drop_index("ix_execution_orders_client_order_id", table_name="execution_orders")
    op.drop_index("ix_execution_orders_source_type", table_name="execution_orders")
    op.drop_index("ix_execution_orders_allocation_id", table_name="execution_orders")
    op.drop_index("ix_execution_orders_leader_id", table_name="execution_orders")
    op.drop_constraint("fk_execution_orders_allocation_id", "execution_orders", type_="foreignkey")
    op.drop_constraint("fk_execution_orders_leader_id", "execution_orders", type_="foreignkey")

    for column in [
        "event_to_final_ms",
        "event_to_ack_ms",
        "submit_to_ack_ms",
        "receive_to_submit_ms",
        "event_to_receive_ms",
        "order_finalized_at",
        "binance_order_ack_at",
        "binance_order_submit_at",
        "decision_started_at",
        "event_received_at",
        "hyperliquid_event_time",
        "slippage_bps",
        "cum_quote",
        "avg_fill_price",
        "estimated_price",
        "executed_qty",
        "client_order_id",
        "order_type",
        "source_type",
        "allocation_id",
        "leader_id",
    ]:
        op.drop_column("execution_orders", column)
