"""initial empty migration

Revision ID: 88f6efa44e81
Revises:
Create Date: 2026-10-12 23:46:35.111605

"""

# revision identifiers, used by Alembic.
revision = "88f6efa44e81"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Intentionally empty bootstrap revision; the V1 schema lives in 2f4c7d9e1a30.

    An earlier version of this file called ``Base.metadata.create_all()``. That
    made the migration depend on whatever the models looked like *today*, so a
    fresh database would get tables from later revisions here and then fail when
    those later revisions ran. Migrations must be frozen snapshots of DDL.
    """


def downgrade() -> None:
    pass
