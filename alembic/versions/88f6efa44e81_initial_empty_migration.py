"""initial empty migration

Revision ID: 88f6efa44e81
Revises: 
Create Date: 2026-10-12 23:46:35.111605

"""
from alembic import op
from app.db import models  # noqa: F401 - registers all migration metadata
from app.db.base import Base


# revision identifiers, used by Alembic.
revision = "88f6efa44e81"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the V1 tenant, event, delivery, and attempt tables."""
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    # Metadata is intentionally invoked only by this versioned Alembic migration;
    # application startup never creates or alters schema.
    Base.metadata.create_all(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    """Remove the V1 schema in dependency order."""
    Base.metadata.drop_all(bind=op.get_bind(), checkfirst=True)
