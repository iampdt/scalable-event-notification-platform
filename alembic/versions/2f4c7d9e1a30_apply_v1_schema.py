"""apply V1 schema after the originally empty bootstrap revision

Revision ID: 2f4c7d9e1a30
Revises: 88f6efa44e81

The DDL below is a frozen copy of the schema the V1 models produced. It is
written out explicitly (instead of importing app.db.models) so that later model
changes can never alter what this historical revision creates.
"""

import sqlalchemy as sa
from alembic import op

revision = "2f4c7d9e1a30"
down_revision = "88f6efa44e81"
branch_labels = None
depends_on = None


V1_SCHEMA = """
CREATE TYPE api_key_role AS ENUM ('producer', 'admin');
CREATE TYPE delivery_channel AS ENUM ('email', 'in_app');
CREATE TYPE delivery_status AS ENUM ('pending', 'processing', 'delivered', 'failed', 'suppressed');

CREATE TABLE tenants (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    slug text NOT NULL UNIQUE,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    key_prefix text NOT NULL UNIQUE,
    secret_hash text NOT NULL,
    role api_key_role NOT NULL DEFAULT 'producer',
    name text NOT NULL,
    last_used_at timestamptz,
    revoked_at timestamptz,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    external_id text NOT NULL,
    email text,
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, external_id),
    UNIQUE (tenant_id, id)
);

CREATE TABLE events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    event_type text NOT NULL,
    subject_type text NOT NULL,
    subject_id text NOT NULL,
    payload jsonb NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (tenant_id, id)
);

CREATE TABLE notifications (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    event_id uuid NOT NULL,
    user_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (event_id, user_id),
    FOREIGN KEY (tenant_id, event_id) REFERENCES events(tenant_id, id),
    FOREIGN KEY (tenant_id, user_id) REFERENCES users(tenant_id, id)
);

CREATE TABLE deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    notification_id uuid NOT NULL,
    channel delivery_channel NOT NULL,
    status delivery_status NOT NULL DEFAULT 'pending',
    target_snapshot jsonb NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_expires_at timestamptz,
    delivered_at timestamptz,
    failed_at timestamptz,
    read_at timestamptz,
    last_error_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id),
    UNIQUE (notification_id, channel),
    FOREIGN KEY (tenant_id, notification_id) REFERENCES notifications(tenant_id, id),
    CHECK (jsonb_typeof(target_snapshot) = 'object'),
    CHECK ((status <> 'delivered') OR delivered_at IS NOT NULL),
    CHECK ((status <> 'failed') OR failed_at IS NOT NULL)
);

CREATE TABLE delivery_attempts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    attempt_number integer NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    outcome text NOT NULL,
    provider_status_code integer,
    provider_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (delivery_id, attempt_number),
    FOREIGN KEY (tenant_id, delivery_id) REFERENCES deliveries(tenant_id, id)
);

CREATE INDEX idx_api_keys_active ON api_keys (key_prefix) WHERE revoked_at IS NULL;
CREATE INDEX idx_events_tenant_created ON events (tenant_id, created_at DESC);
CREATE INDEX idx_notifications_tenant_created ON notifications (tenant_id, created_at DESC);
CREATE INDEX idx_notifications_user_created ON notifications (tenant_id, user_id, created_at DESC);
CREATE INDEX idx_deliveries_due ON deliveries (next_attempt_at, id) WHERE status = 'pending';
CREATE INDEX idx_deliveries_expired_lease ON deliveries (lease_expires_at, id) WHERE status = 'processing';
CREATE INDEX idx_deliveries_notification ON deliveries (tenant_id, notification_id);
CREATE INDEX idx_attempts_delivery ON delivery_attempts (tenant_id, delivery_id, attempt_number);
"""


def upgrade() -> None:
    """Create V1 tables; skip when an older create_all-based revision already did."""
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    if sa.inspect(op.get_bind()).has_table("tenants"):
        return
    op.execute(V1_SCHEMA)


def downgrade() -> None:
    op.execute(
        """
        DROP TABLE IF EXISTS delivery_attempts, deliveries, notifications, events, users, api_keys, tenants;
        DROP TYPE IF EXISTS delivery_status, delivery_channel, api_key_role;
        """
    )
