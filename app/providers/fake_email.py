"""Development-only email adapter; it records no provider side effects."""
from __future__ import annotations

import logging

from app.providers.base import ClaimedDelivery

logger = logging.getLogger(__name__)


class FakeEmailProvider:
    def send(self, delivery: ClaimedDelivery) -> int | None:
        logger.info("fake_email_accepted delivery_id=%s", delivery.id)
        return None
