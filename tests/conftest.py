from __future__ import annotations

import os


def pytest_sessionstart(session):
    # Tests never talk to a real SMTP server; individual tests inject providers.
    os.environ["EMAIL_PROVIDER"] = "fake"
    os.environ.setdefault("WEBHOOK_SECRET_ENCRYPTION_KEY", "UtdpRhP4WrBOSC0yYXp2TYAniAUkhyKZF8AhZphjqIQ=")
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        return
    os.environ["DATABASE_URL"] = database_url
    from alembic import command
    from alembic.config import Config

    command.upgrade(Config("alembic.ini"), "head")
