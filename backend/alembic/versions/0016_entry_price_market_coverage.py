"""entry price and market coverage

Revision ID: 0016_entry_price_market_coverage
Revises: 0015_leader_position_baselines
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0016_entry_price_market_coverage"
down_revision = "0015_leader_position_baselines"
branch_labels = None
depends_on = None


def upgrade() -> None:
    _widen_market_columns()
    op.add_column("execution_orders", sa.Column("leader_entry_px", sa.Numeric(30, 12), nullable=True))
    op.add_column("execution_orders", sa.Column("follower_avg_entry_px", sa.Numeric(30, 12), nullable=True))
    op.add_column("leader_position_baselines", sa.Column("entry_px_at_enable", sa.Numeric(30, 12), nullable=True))
    op.add_column("leader_position_baselines", sa.Column("mark_px_at_enable", sa.Numeric(30, 12), nullable=True))
    op.add_column("leader_position_baselines", sa.Column("last_leader_entry_px", sa.Numeric(30, 12), nullable=True))
    op.add_column("leader_position_baselines", sa.Column("last_leader_mark_px", sa.Numeric(30, 12), nullable=True))
    op.execute(
        """
        update leader_position_baselines b
        set
            entry_px_at_enable = coalesce(b.entry_px_at_enable, p.entry_px),
            mark_px_at_enable = coalesce(b.mark_px_at_enable, p.mark_px),
            last_leader_entry_px = coalesce(b.last_leader_entry_px, p.entry_px),
            last_leader_mark_px = coalesce(b.last_leader_mark_px, p.mark_px)
        from latest_account_states s
        join latest_account_positions p on p.account_state_id = s.id
        where s.role = 'LEADER'
          and lower(s.address) = lower(b.leader_address)
          and s.dex = b.dex
          and p.canonical_coin = b.canonical_coin
        """
    )


def downgrade() -> None:
    op.drop_column("leader_position_baselines", "last_leader_mark_px")
    op.drop_column("leader_position_baselines", "last_leader_entry_px")
    op.drop_column("leader_position_baselines", "mark_px_at_enable")
    op.drop_column("leader_position_baselines", "entry_px_at_enable")
    op.drop_column("execution_orders", "follower_avg_entry_px")
    op.drop_column("execution_orders", "leader_entry_px")
    _narrow_market_columns()


def _widen_market_columns() -> None:
    for table_name, column_name, old_len, new_len in [
        ("leader_symbol_configs", "coin", 32, 80),
        ("symbol_mappings", "hyperliquid_coin", 32, 80),
        ("venue_mappings", "hyperliquid_coin", 32, 80),
        ("venue_mappings", "venue_symbol", 32, 80),
        ("source_fills", "coin", 32, 80),
        ("source_fills", "canonical_coin", 80, 160),
        ("source_fills", "raw_coin", 80, 160),
        ("desired_positions", "coin", 32, 80),
        ("leader_position_allocations", "hyperliquid_coin", 32, 80),
        ("leader_position_allocations", "canonical_coin", 80, 160),
        ("leader_position_allocations", "venue_symbol", 32, 80),
        ("allocation_events", "canonical_coin", 80, 160),
        ("leader_position_baselines", "canonical_coin", 80, 160),
        ("execution_orders", "source_coin", 32, 80),
        ("execution_orders", "canonical_coin", 80, 160),
        ("execution_orders", "raw_coin_from_fill", 80, 160),
        ("execution_orders", "venue_symbol", 32, 80),
        ("execution_orders", "hyperliquid_coin", 32, 80),
        ("latest_account_positions", "coin", 64, 80),
        ("latest_account_positions", "canonical_coin", 80, 160),
        ("latest_account_positions", "raw_coin", 80, 160),
    ]:
        op.alter_column(
            table_name,
            column_name,
            type_=sa.String(length=new_len),
            existing_type=sa.String(length=old_len),
            existing_nullable=True,
        )


def _narrow_market_columns() -> None:
    for table_name, column_name, old_len, new_len in [
        ("latest_account_positions", "raw_coin", 160, 80),
        ("latest_account_positions", "canonical_coin", 160, 80),
        ("latest_account_positions", "coin", 80, 64),
        ("execution_orders", "hyperliquid_coin", 80, 32),
        ("execution_orders", "venue_symbol", 80, 32),
        ("execution_orders", "raw_coin_from_fill", 160, 80),
        ("execution_orders", "canonical_coin", 160, 80),
        ("execution_orders", "source_coin", 80, 32),
        ("leader_position_baselines", "canonical_coin", 160, 80),
        ("allocation_events", "canonical_coin", 160, 80),
        ("leader_position_allocations", "venue_symbol", 80, 32),
        ("leader_position_allocations", "canonical_coin", 160, 80),
        ("leader_position_allocations", "hyperliquid_coin", 80, 32),
        ("desired_positions", "coin", 80, 32),
        ("source_fills", "raw_coin", 160, 80),
        ("source_fills", "canonical_coin", 160, 80),
        ("source_fills", "coin", 80, 32),
        ("venue_mappings", "venue_symbol", 80, 32),
        ("venue_mappings", "hyperliquid_coin", 80, 32),
        ("symbol_mappings", "hyperliquid_coin", 80, 32),
        ("leader_symbol_configs", "coin", 80, 32),
    ]:
        op.alter_column(
            table_name,
            column_name,
            type_=sa.String(length=new_len),
            existing_type=sa.String(length=old_len),
            existing_nullable=True,
        )
