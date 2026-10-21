"""Environment-driven settings, read once at call time so tests can monkeypatch."""
from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def webhook_encryption_key() -> str:
    value = os.getenv("WEBHOOK_SECRET_ENCRYPTION_KEY")
    if not value:
        raise RuntimeError("WEBHOOK_SECRET_ENCRYPTION_KEY is not set")
    return value


def webhook_timeout_seconds() -> float:
    return float(os.getenv("WEBHOOK_TIMEOUT_SECONDS", "10"))


def webhook_allow_private_networks() -> bool:
    """Development escape hatch so a receiver on localhost can be used."""
    return env_flag("WEBHOOK_ALLOW_PRIVATE_NETWORKS")


def email_provider_name() -> str:
    return os.getenv("EMAIL_PROVIDER", "fake")


def smtp_settings() -> dict:
    return {
        "host": os.getenv("SMTP_HOST", "localhost"),
        "port": int(os.getenv("SMTP_PORT", "1025")),
        "username": os.getenv("SMTP_USERNAME") or None,
        "password": os.getenv("SMTP_PASSWORD") or None,
        "starttls": env_flag("SMTP_STARTTLS"),
        "sender": os.getenv("EMAIL_FROM", "notifications@example.test"),
        "timeout": float(os.getenv("SMTP_TIMEOUT_SECONDS", "10")),
    }


def redis_url() -> str | None:
    """Unset means "run without Redis": no cache, no rate limiting. Redis is never required."""
    return os.getenv("REDIS_URL") or None


def redis_timeout_seconds() -> float:
    """Socket timeout for every Redis call. Must sit above the environment's *tail*
    latency: 50 ms (the first value) tripped on a healthy Redis under load."""
    return float(os.getenv("REDIS_TIMEOUT_SECONDS", "0.25"))


def cache_ttl_seconds() -> int:
    return int(os.getenv("CACHE_TTL_SECONDS", "60"))


def rate_limit_defaults() -> tuple[float, int]:
    """Per-tenant ingestion limit: sustained events/second and burst size."""
    return float(os.getenv("RATE_LIMIT_PER_SECOND", "50")), int(os.getenv("RATE_LIMIT_BURST", "100"))


def db_pool_settings() -> dict:
    return {
        "pool_size": int(os.getenv("DB_POOL_SIZE", "10")),
        "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "10")),
        "pool_timeout": float(os.getenv("DB_POOL_TIMEOUT_SECONDS", "5")),
    }


def dispatch_mode() -> str:
    """How deliveries reach workers: "postgres" (SKIP LOCKED polling) or "kafka" (outbox -> Kafka)."""
    mode = os.getenv("DISPATCH_MODE", "postgres")
    if mode not in ("postgres", "kafka"):
        raise RuntimeError(f"DISPATCH_MODE must be 'postgres' or 'kafka', not {mode!r}")
    return mode


def kafka_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
