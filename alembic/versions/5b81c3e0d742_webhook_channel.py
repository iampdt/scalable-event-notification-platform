"""webhook channel, webhook endpoints, and missing V1 check constraints

Revision ID: 5b81c3e0d742
Revises: 2f4c7d9e1a30
"""

from alembic import op

revision = "5b81c3e0d742"
down_revision = "2f4c7d9e1a30"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # PostgreSQL refuses to *use* a newly added enum value inside the transaction
    # that added it ("unsafe use of new value"). The CHECK constraints below
    # compare against 'webhook', so the ADD VALUE must commit on its own first.
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE delivery_channel ADD VALUE IF NOT EXISTS 'webhook'")

    op.execute(
        """
        CREATE TABLE webhook_endpoints (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id uuid NOT NULL REFERENCES tenants(id),
            url text NOT NULL,
            secret_ciphertext bytea NOT NULL,
            active boolean NOT NULL DEFAULT true,
            is_default boolean NOT NULL DEFAULT false,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (tenant_id, id)
        );
        CREATE UNIQUE INDEX one_active_default_webhook_per_tenant
            ON webhook_endpoints (tenant_id) WHERE active AND is_default;

        ALTER TABLE deliveries ADD COLUMN webhook_endpoint_id uuid;
        ALTER TABLE deliveries
            ADD CONSTRAINT deliveries_webhook_endpoint_fkey
                FOREIGN KEY (tenant_id, webhook_endpoint_id) REFERENCES webhook_endpoints(tenant_id, id),
            ADD CONSTRAINT deliveries_webhook_endpoint_only_for_webhook
                CHECK (channel = 'webhook' OR webhook_endpoint_id IS NULL),
            ADD CONSTRAINT deliveries_webhook_endpoint_required
                CHECK (channel <> 'webhook' OR webhook_endpoint_id IS NOT NULL OR status = 'suppressed');

        ALTER TABLE tenants
            ADD CONSTRAINT tenants_slug_format CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,62}$'),
            ADD CONSTRAINT tenants_status_values CHECK (status IN ('active', 'disabled'));
        ALTER TABLE delivery_attempts
            ADD CONSTRAINT delivery_attempts_outcome_values
                CHECK (outcome IN ('success', 'retryable_failure', 'permanent_failure')),
            ADD CONSTRAINT delivery_attempts_attempt_number_positive CHECK (attempt_number > 0);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE delivery_attempts
            DROP CONSTRAINT IF EXISTS delivery_attempts_outcome_values,
            DROP CONSTRAINT IF EXISTS delivery_attempts_attempt_number_positive;
        ALTER TABLE tenants
            DROP CONSTRAINT IF EXISTS tenants_slug_format,
            DROP CONSTRAINT IF EXISTS tenants_status_values;
        DELETE FROM delivery_attempts WHERE delivery_id IN (SELECT id FROM deliveries WHERE channel = 'webhook');
        DELETE FROM deliveries WHERE channel = 'webhook';
        ALTER TABLE deliveries DROP COLUMN IF EXISTS webhook_endpoint_id;
        DROP TABLE IF EXISTS webhook_endpoints;
        """
    )
    # PostgreSQL cannot drop a value from an enum; 'webhook' stays in
    # delivery_channel but nothing can reference it after this downgrade.
