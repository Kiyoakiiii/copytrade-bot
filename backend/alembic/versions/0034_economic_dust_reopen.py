"""backfill durable minimum-residual lifecycle boundaries

Revision ID: 0034_economic_dust_reopen
Revises: 0033_hot_path_db_churn
Create Date: 2026-07-23
"""

from __future__ import annotations

from alembic import op


revision = "0034_economic_dust_reopen"
down_revision = "0033_hot_path_db_churn"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # New fills persist this marker when the minimum-residual close is applied.
    # Recover the same causal boundary for historical closed allocations using
    # only their latest filled order and its immutable planning audit field.
    op.execute(
        """
        WITH latest_order AS (
            SELECT DISTINCT ON (allocation_id)
                allocation_id,
                status,
                executed_qty,
                pre_trade_checklist
            FROM execution_orders
            WHERE allocation_id IS NOT NULL
            ORDER BY allocation_id, created_at DESC, id DESC
        )
        UPDATE leader_position_allocations AS allocation
        SET pending_reduce_reason = 'MINIMUM_RESIDUAL_ECONOMIC_FLAT'
        FROM latest_order AS latest
        WHERE latest.allocation_id = allocation.id
          AND allocation.status = 'CLOSED'
          AND ABS(COALESCE(allocation.allocated_qty, 0)) <= 0.00000001
          AND ABS(COALESCE(allocation.allocated_notional, 0)) <= 0.00000001
          AND ABS(COALESCE(allocation.last_leader_position_size, 0)) > 0.00000001
          AND latest.status = 'FILLED'
          AND COALESCE(latest.executed_qty, 0) > 0
          AND COALESCE(
                latest.pre_trade_checklist->>'minimum_residual_early_close',
                'false'
              ) = 'true'
        """
    )


def downgrade() -> None:
    # The marker is also written by live execution after this migration.  It is
    # unsafe to erase causal lifecycle state during a code rollback.
    pass
