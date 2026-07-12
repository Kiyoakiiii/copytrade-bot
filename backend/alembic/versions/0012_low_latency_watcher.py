"""low latency watcher metadata

Revision ID: 0012_low_latency_watcher
Revises: 0011_account_abstraction_fields
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0012_low_latency_watcher"
down_revision = "0011_account_abstraction_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("source_fills", sa.Column("dex", sa.String(length=32), nullable=False, server_default=""))
    op.add_column("source_fills", sa.Column("canonical_coin", sa.String(length=80), nullable=True))
    op.add_column("source_fills", sa.Column("raw_coin", sa.String(length=80), nullable=True))
    op.add_column("source_fills", sa.Column("asset_id", sa.BigInteger(), nullable=True))
    op.add_column("source_fills", sa.Column("ws_received_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_source_fills_dex", "source_fills", ["dex"])
    op.create_index("ix_source_fills_canonical_coin", "source_fills", ["canonical_coin"])

    for name in [
        "ws_received_at",
        "dedupe_done_at",
        "debounce_released_at",
        "decision_done_at",
        "order_submit_started_at",
        "order_submit_done_at",
        "order_ack_at",
    ]:
        op.add_column("execution_orders", sa.Column(name, sa.DateTime(timezone=True), nullable=True))
    for name in [
        "event_to_ws_ms",
        "ws_to_dedupe_ms",
        "debounce_ms",
        "decision_ms",
        "ws_to_submit_ms",
    ]:
        op.add_column("execution_orders", sa.Column(name, sa.BigInteger(), nullable=True))


def downgrade() -> None:
    for name in [
        "ws_to_submit_ms",
        "decision_ms",
        "debounce_ms",
        "ws_to_dedupe_ms",
        "event_to_ws_ms",
    ]:
        op.drop_column("execution_orders", name)
    for name in [
        "order_ack_at",
        "order_submit_done_at",
        "order_submit_started_at",
        "decision_done_at",
        "debounce_released_at",
        "dedupe_done_at",
        "ws_received_at",
    ]:
        op.drop_column("execution_orders", name)

    op.drop_index("ix_source_fills_canonical_coin", table_name="source_fills")
    op.drop_index("ix_source_fills_dex", table_name="source_fills")
    op.drop_column("source_fills", "ws_received_at")
    op.drop_column("source_fills", "asset_id")
    op.drop_column("source_fills", "raw_coin")
    op.drop_column("source_fills", "canonical_coin")
    op.drop_column("source_fills", "dex")
