"""Contract between the worker and channel adapters.

Adapters receive everything they need in a ClaimedDelivery and never touch the
database. That keeps provider I/O outside any transaction and makes adapters
unit-testable without PostgreSQL.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from app.db.models import DeliveryChannel


@dataclass(frozen=True)
class ClaimedDelivery:
    id: uuid.UUID
    tenant_id: uuid.UUID
    channel: DeliveryChannel
    target_snapshot: dict
    attempt_count: int
    max_attempts: int
    notification_id: uuid.UUID | None = None
    user_id: uuid.UUID | None = None
    user_external_id: str | None = None
    event_id: uuid.UUID | None = None
    event_type: str | None = None
    subject: dict | None = None
    payload: dict | None = None
    occurred_at: datetime | None = None
    webhook_active: bool | None = None
    # Rendered template ({subject, body, template_id, template_version}) or None.
    content: dict | None = None
    # repr=False keeps the (encrypted) secret out of logs and tracebacks.
    webhook_secret_ciphertext: bytes | None = field(default=None, repr=False)


class Provider(Protocol):
    def send(self, delivery: ClaimedDelivery) -> int | None:
        """Deliver once. Return a provider status code if any; raise ProviderFailure on failure."""
