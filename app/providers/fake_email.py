"""Development-only email adapter; it records no provider side effects."""
from __future__ import annotations

import logging
import uuid

logger = logging.getLogger(__name__)


class FakeEmailProvider:
    def send(self, *, delivery_id: uuid.UUID, target: dict) -> None:
        logger.info("fake_email_accepted delivery_id=%s email=%s", delivery_id, target.get("email"))
