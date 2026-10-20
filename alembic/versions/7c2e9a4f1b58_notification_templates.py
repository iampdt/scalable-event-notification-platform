"""notification templates and rendered delivery content

Revision ID: 7c2e9a4f1b58
Revises: 5b81c3e0d742
"""

from alembic import op

revision = "7c2e9a4f1b58"
down_revision = "5b81c3e0d742"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE notification_templates (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            tenant_id uuid NOT NULL REFERENCES tenants(id),
            event_type text NOT NULL,
            channel delivery_channel NOT NULL,
            subject text,
            body text NOT NULL,
            version integer NOT NULL DEFAULT 1,
            active boolean NOT NULL DEFAULT true,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            -- One template per event type and channel; the unique index also
            -- serves the ingestion lookup (tenant_id, event_type, channel).
            UNIQUE (tenant_id, event_type, channel),
            UNIQUE (tenant_id, id),
            CONSTRAINT notification_templates_channel_supported CHECK (channel IN ('email', 'in_app')),
            CONSTRAINT notification_templates_email_has_subject CHECK (channel <> 'email' OR subject IS NOT NULL),
            CONSTRAINT notification_templates_version_positive CHECK (version > 0)
        );

        -- Rendered at ingestion, like target_snapshot: a retry sends what was
        -- decided at planning time even if the template is edited afterwards.
        ALTER TABLE deliveries ADD COLUMN content jsonb;
        ALTER TABLE deliveries ADD CONSTRAINT deliveries_content_is_object
            CHECK (content IS NULL OR jsonb_typeof(content) = 'object');
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE deliveries DROP CONSTRAINT IF EXISTS deliveries_content_is_object;
        ALTER TABLE deliveries DROP COLUMN IF EXISTS content;
        DROP TABLE IF EXISTS notification_templates;
        """
    )
