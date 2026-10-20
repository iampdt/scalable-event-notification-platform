"""In-app adapter. The delivery row *is* the inbox item, so there is nothing to send.

Routing in-app through the worker anyway keeps one state machine for every
channel; marking the row delivered is what makes it appear in the feed.
"""
from __future__ import annotations

from app.providers.base import ClaimedDelivery


class InAppProvider:
    def send(self, delivery: ClaimedDelivery) -> int | None:
        return None
