"""reduce background database churn on the execution hot path

Revision ID: 0033_hot_path_db_churn
Revises: 0032_leader_perf_epoch
Create Date: 2026-07-22
"""

from __future__ import annotations

from alembic import op


revision = "0033_hot_path_db_churn"
down_revision = "0032_leader_perf_epoch"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # last_update_at changes on every fresh account snapshot.  Indexing it made
    # every heartbeat a non-HOT update even though no production query searches
    # account states by that column.  The role/address/dex unique index remains
    # the authoritative lookup path.
    op.drop_index(
        "ix_latest_account_states_last_update_at",
        table_name="latest_account_states",
    )
    op.execute(
        "ALTER TABLE latest_account_states SET "
        "(fillfactor=70, autovacuum_vacuum_scale_factor=0.01, autovacuum_vacuum_threshold=25)"
    )
    op.execute(
        "ALTER TABLE latest_account_positions SET "
        "(fillfactor=70, autovacuum_vacuum_scale_factor=0.01, autovacuum_vacuum_threshold=50)"
    )
    op.execute(
        "ALTER TABLE app_settings SET "
        "(fillfactor=60, autovacuum_vacuum_scale_factor=0.01, autovacuum_vacuum_threshold=25)"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE app_settings RESET (fillfactor, autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold)")
    op.execute(
        "ALTER TABLE latest_account_positions RESET "
        "(fillfactor, autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold)"
    )
    op.execute(
        "ALTER TABLE latest_account_states RESET "
        "(fillfactor, autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold)"
    )
    op.create_index(
        "ix_latest_account_states_last_update_at",
        "latest_account_states",
        ["last_update_at"],
    )
