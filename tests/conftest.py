from __future__ import annotations

import os


def pytest_sessionstart(session):
    # Tests never talk to a real SMTP server; individual tests inject providers.
    os.environ["EMAIL_PROVIDER"] = "fake"
    os.environ.setdefault("WEBHOOK_SECRET_ENCRYPTION_KEY", "UtdpRhP4WrBOSC0yYXp2TYAniAUkhyKZF8AhZphjqIQ=")
    # Isolated Redis database for tests (never the dev cache in db 0). If Redis is
    # not running the app fails open, so non-Redis tests still pass.
    os.environ["REDIS_URL"] = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
    try:
        import redis

        redis.Redis.from_url(os.environ["REDIS_URL"], socket_timeout=0.2).flushdb()
    except Exception:
        pass
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        return
    os.environ["DATABASE_URL"] = database_url
    from alembic import command
    from alembic.config import Config

    command.upgrade(Config("alembic.ini"), "head")
