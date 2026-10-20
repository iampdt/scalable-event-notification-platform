"""transactional outbox for Kafka publishing

Revision ID: b3f8d2a6c914
Revises: 9d4b1e6c2a07
"""

from alembic import op

revision = "b3f8d2a6c914"
down_revision = "9d4b1e6c2a07"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        -- Rows are written in the same transaction as the state change they
        -- announce, then published to Kafka by the relay and deleted. This is how
        -- PostgreSQL and Kafka stay consistent without a dual write.
        CREATE TABLE outbox (
            id bigserial PRIMARY KEY,
            topic text NOT NULL,
            message_key text NOT NULL,
            payload jsonb NOT NULL,
            aggregate_id uuid NOT NULL,
            -- Future for retries: PostgreSQL schedules, Kafka only transports.
            available_at timestamptz NOT NULL DEFAULT now(),
            created_at timestamptz NOT NULL DEFAULT now(),
            -- Per-row publish failures back off instead of blocking the rows behind
            -- them; a growing count is an alert (e.g. a topic that does not exist).
            publish_attempts integer NOT NULL DEFAULT 0,
            last_error text,
            CONSTRAINT outbox_payload_is_object CHECK (jsonb_typeof(payload) = 'object')
        );
        CREATE INDEX idx_outbox_available ON outbox (available_at, id);
        CREATE INDEX idx_outbox_aggregate ON outbox (aggregate_id);
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS outbox")
