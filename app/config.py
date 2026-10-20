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
