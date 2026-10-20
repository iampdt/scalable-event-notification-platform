"""dead-letter listing index and redrive audit trail

Revision ID: d6a1f4e9b273
Revises: b3f8d2a6c914
"""

from alembic import op

revision = "d6a1f4e9b273"
down_revision = "b3f8d2a6c914"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- Failed deliveries are the dead-letter queue; this serves the per-tenant
        -- "newest failures first" listing without scanning delivered rows.
        CREATE INDEX idx_deliveries_dead_letters ON deliveries (tenant_id, failed_at DESC, id DESC)
            WHERE status = 'failed';

        -- Every manual replay is recorded: who, when, why, and what state it
        -- replaced. Attempt history itself is never rewritten.
        CREATE TABLE delivery_redrives (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id uuid NOT NULL,
            delivery_id uuid NOT NULL,
            api_key_id uuid NOT NULL REFERENCES api_keys(id),
            reason text,
            previous_attempt_count integer NOT NULL,
            previous_error_code text,
            extra_attempts integer NOT NULL CHECK (extra_attempts > 0),
            created_at timestamptz NOT NULL DEFAULT now(),
            FOREIGN KEY (tenant_id, delivery_id) REFERENCES deliveries(tenant_id, id)
        );
        CREATE INDEX idx_delivery_redrives_delivery ON delivery_redrives (tenant_id, delivery_id, created_at);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS delivery_redrives;
        DROP INDEX IF EXISTS idx_deliveries_dead_letters;
        """
    )
