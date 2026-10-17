from __future__ import annotations

import os


def pytest_sessionstart(session):
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        return
    os.environ["DATABASE_URL"] = database_url
    from alembic import command
    from alembic.config import Config

    command.upgrade(Config("alembic.ini"), "head")


