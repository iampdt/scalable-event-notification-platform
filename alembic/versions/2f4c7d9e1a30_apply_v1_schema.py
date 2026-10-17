"""apply V1 schema after the originally empty bootstrap revision

Revision ID: 2f4c7d9e1a30
Revises: 88f6efa44e81
"""

from alembic import op

from app.db import models  # noqa: F401 - registers all metadata
from app.db.base import Base

revision = "2f4c7d9e1a30"
down_revision = "88f6efa44e81"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create V1 tables for databases already stamped at the empty revision."""
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    Base.metadata.create_all(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    """Remove V1 schema when rolling back the V1 revision."""
    Base.metadata.drop_all(bind=op.get_bind(), checkfirst=True)
