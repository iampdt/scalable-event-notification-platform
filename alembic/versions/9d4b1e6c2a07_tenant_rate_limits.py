"""per-tenant ingestion rate limit overrides

Revision ID: 9d4b1e6c2a07
Revises: 7c2e9a4f1b58
"""

from alembic import op

revision = "9d4b1e6c2a07"
down_revision = "7c2e9a4f1b58"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # NULL means "use the platform default" (RATE_LIMIT_PER_SECOND / RATE_LIMIT_BURST),
    # so the default can change without touching every tenant row.
    op.execute(
        """
        ALTER TABLE tenants
            ADD COLUMN rate_limit_per_second numeric,
            ADD COLUMN rate_limit_burst integer,
            ADD CONSTRAINT tenants_rate_limit_positive CHECK (rate_limit_per_second IS NULL OR rate_limit_per_second > 0),
            ADD CONSTRAINT tenants_rate_limit_burst_positive CHECK (rate_limit_burst IS NULL OR rate_limit_burst > 0);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE tenants
            DROP CONSTRAINT IF EXISTS tenants_rate_limit_positive,
            DROP CONSTRAINT IF EXISTS tenants_rate_limit_burst_positive,
            DROP COLUMN IF EXISTS rate_limit_per_second,
            DROP COLUMN IF EXISTS rate_limit_burst;
        """
    )
