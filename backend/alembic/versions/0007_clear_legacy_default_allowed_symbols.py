"""clear legacy frontend default leader allowlist

Revision ID: 0007_clear_legacy_allowed
Revises: 0006_leader_management
Create Date: 2026-05-02
"""

from __future__ import annotations

from alembic import op

revision = "0007_clear_legacy_allowed"
down_revision = "0006_leader_management"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        update leader_configs
        set allowed_symbols = null
        where allowed_symbols::jsonb = '["BTC", "ETH", "SOL"]'::jsonb
        """
    )


def downgrade() -> None:
    pass
