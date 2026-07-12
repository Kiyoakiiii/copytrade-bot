"""market risk settings

Revision ID: 0018_market_risk_settings
Revises: 0017_position_lifecycle_realtime
Create Date: 2026-05-03
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0018_market_risk_settings"
down_revision = "0017_position_lifecycle_realtime"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_risk_settings",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("execution_venue", sa.String(length=16), nullable=False),
        sa.Column("account_address", sa.String(length=64), nullable=False),
        sa.Column("dex", sa.String(length=32), nullable=False),
        sa.Column("canonical_coin", sa.String(length=160), nullable=False),
        sa.Column("asset_id", sa.BigInteger(), nullable=True),
        sa.Column("desired_margin_mode", sa.String(length=16), nullable=False),
        sa.Column("desired_leverage", sa.Integer(), nullable=True),
        sa.Column("market_max_leverage", sa.Integer(), nullable=True),
        sa.Column("effective_leverage", sa.Integer(), nullable=True),
        sa.Column("actual_margin_mode", sa.String(length=16), nullable=True),
        sa.Column("actual_leverage", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("last_set_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("raw_response_masked", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "execution_venue",
            "account_address",
            "dex",
            "canonical_coin",
            name="uq_market_risk_setting_scope",
        ),
    )
    op.create_index(
        "ix_market_risk_settings_scope",
        "market_risk_settings",
        ["execution_venue", "account_address", "dex", "canonical_coin"],
    )
    op.create_index("ix_market_risk_settings_status", "market_risk_settings", ["status"])
    op.create_index("ix_market_risk_settings_last_confirmed_at", "market_risk_settings", ["last_confirmed_at"])


def downgrade() -> None:
    op.drop_index("ix_market_risk_settings_last_confirmed_at", table_name="market_risk_settings")
    op.drop_index("ix_market_risk_settings_status", table_name="market_risk_settings")
    op.drop_index("ix_market_risk_settings_scope", table_name="market_risk_settings")
    op.drop_table("market_risk_settings")
