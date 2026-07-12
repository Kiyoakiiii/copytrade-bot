"""add fixed account value per leader

Revision ID: 0024_fixed_leader_value
Revises: 0023_fill_channel_obs
Create Date: 2026-07-05
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0024_fixed_leader_value"
down_revision = "0023_fill_channel_obs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "leader_configs",
        sa.Column("fixed_account_value", sa.Numeric(30, 12), nullable=True),
    )
    op.execute(
        sa.text(
            """
            UPDATE leader_configs AS leader
            SET fixed_account_value = COALESCE(
                (
                    SELECT NULLIF(
                        setting.value -> 'resolved_by_dex' -> '' ->> 'account_value_used_for_sizing',
                        ''
                    )::numeric
                    FROM app_settings AS setting
                    WHERE setting.key = 'account_abstraction:LEADER:' || lower(leader.leader_address)
                ),
                (
                    SELECT state.account_value
                    FROM latest_leader_states AS state
                    WHERE lower(state.leader_address) = lower(leader.leader_address)
                )
            )
            WHERE leader.fixed_account_value IS NULL
            """
        )
    )


def downgrade() -> None:
    op.drop_column("leader_configs", "fixed_account_value")
