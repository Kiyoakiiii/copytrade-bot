"""add exactly-once fill outcomes, cursors, and signed outbox

Revision ID: 0027_exactly_once_pipeline
Revises: 0026_durable_fill_inbox
Create Date: 2026-07-13
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0027_exactly_once_pipeline"
down_revision = "0026_durable_fill_inbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "leader_fill_cursors",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("leader_address", sa.String(length=64), nullable=False),
        sa.Column("last_fill_time_ms", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("last_fill_tid", sa.BigInteger(), nullable=True),
        sa.Column("backfilled_through_ms", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("leader_address"),
    )
    op.create_index(
        "ix_leader_fill_cursors_leader_address",
        "leader_fill_cursors",
        ["leader_address"],
        unique=True,
    )
    op.create_table(
        "signer_nonce_states",
        sa.Column("signer_scope", sa.String(length=96), nullable=False),
        sa.Column("last_nonce", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("signer_scope"),
    )
    op.add_column("execution_orders", sa.Column("signed_action_envelope", sa.JSON(), nullable=True))
    op.add_column("execution_orders", sa.Column("signed_action_hash", sa.String(length=64), nullable=True))
    op.add_column("execution_orders", sa.Column("submit_signer_scope", sa.String(length=96), nullable=True))
    op.add_column("execution_orders", sa.Column("submit_nonce", sa.BigInteger(), nullable=True))
    op.create_index(
        "ix_execution_orders_signed_action_hash",
        "execution_orders",
        ["signed_action_hash"],
        unique=True,
    )
    op.create_index(
        "ix_execution_orders_submit_signer_scope",
        "execution_orders",
        ["submit_signer_scope"],
        unique=False,
    )
    op.create_index(
        "ix_execution_orders_submit_nonce",
        "execution_orders",
        ["submit_nonce"],
        unique=False,
    )
    op.create_unique_constraint(
        "uq_execution_orders_signer_nonce",
        "execution_orders",
        ["submit_signer_scope", "submit_nonce"],
    )
    op.create_table(
        "source_fill_outcomes",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("source_fill_id", sa.String(length=160), nullable=False),
        sa.Column("execution_order_id", sa.BigInteger(), nullable=True),
        sa.Column("disposition", sa.String(length=40), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["execution_order_id"], ["execution_orders.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_fill_id"], ["source_fills.source_fill_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_fill_id"),
    )
    op.create_index(
        "ix_source_fill_outcomes_source_fill_id",
        "source_fill_outcomes",
        ["source_fill_id"],
        unique=True,
    )
    op.create_index(
        "ix_source_fill_outcomes_execution_order_id",
        "source_fill_outcomes",
        ["execution_order_id"],
        unique=False,
    )
    op.create_index(
        "ix_source_fill_outcomes_disposition",
        "source_fill_outcomes",
        ["disposition"],
        unique=False,
    )
    op.execute(
        """
        INSERT INTO source_fill_outcomes (
            source_fill_id,
            execution_order_id,
            disposition,
            reason,
            created_at,
            updated_at
        )
        SELECT
            sf.source_fill_id,
            eo.id,
            CASE
                WHEN COALESCE(eo.executed_qty, 0) > 0 THEN 'EXECUTED'
                WHEN UPPER(COALESCE(eo.status, '')) IN (
                    'UNKNOWN', 'PENDING_SUBMIT', 'SUBMITTING', 'SUBMITTED', 'OPEN', 'RESTING'
                ) THEN 'SUBMISSION_UNKNOWN'
                WHEN COALESCE(eo.error_message, '') LIKE '%BELOW_MIN_ORDER_VALUE%'
                    THEN 'MIN_NOTIONAL_EXEMPT'
                WHEN eo.id IS NULL OR UPPER(COALESCE(eo.status, '')) IN ('NOOP', 'IGNORED')
                    THEN 'NO_ACTION_REQUIRED'
                ELSE 'MANUAL_REVIEW'
            END,
            CASE
                WHEN eo.id IS NULL THEN 'legacy processed fill imported during outcome-ledger migration'
                ELSE eo.error_message
            END,
            COALESCE(sf.processed_at, sf.created_at),
            COALESCE(sf.processed_at, sf.updated_at)
        FROM source_fills AS sf
        LEFT JOIN execution_orders AS eo ON eo.source_fill_id = sf.source_fill_id
        WHERE sf.processed_at IS NOT NULL
          AND sf.is_snapshot IS FALSE
        ON CONFLICT (source_fill_id) DO NOTHING
        """
    )
    op.execute("DROP INDEX IF EXISTS ix_source_fills_source_fill_id")


def downgrade() -> None:
    op.create_index(
        "ix_source_fills_source_fill_id",
        "source_fills",
        ["source_fill_id"],
        unique=False,
    )
    op.drop_index("ix_source_fill_outcomes_disposition", table_name="source_fill_outcomes")
    op.drop_index("ix_source_fill_outcomes_execution_order_id", table_name="source_fill_outcomes")
    op.drop_index("ix_source_fill_outcomes_source_fill_id", table_name="source_fill_outcomes")
    op.drop_table("source_fill_outcomes")
    op.drop_constraint("uq_execution_orders_signer_nonce", "execution_orders", type_="unique")
    op.drop_index("ix_execution_orders_submit_nonce", table_name="execution_orders")
    op.drop_index("ix_execution_orders_submit_signer_scope", table_name="execution_orders")
    op.drop_index("ix_execution_orders_signed_action_hash", table_name="execution_orders")
    op.drop_column("execution_orders", "submit_nonce")
    op.drop_column("execution_orders", "signed_action_hash")
    op.drop_column("execution_orders", "submit_signer_scope")
    op.drop_column("execution_orders", "signed_action_envelope")
    op.drop_table("signer_nonce_states")
    op.drop_index("ix_leader_fill_cursors_leader_address", table_name="leader_fill_cursors")
    op.drop_table("leader_fill_cursors")
