"""Notification status is derived from its deliveries, never stored.

Storing it would mean updating the notification row in every completion
transaction (a second hot row per delivery) and keeping two sources of truth in
sync. Deriving it costs an aggregate on read; revisit if list queries get slow.

Rule: delivered when at least one channel delivered and no work remains; failed
when every channel failed or was suppressed; otherwise processing/pending.
The Python and SQL forms below must stay equivalent.
"""
from __future__ import annotations

from sqlalchemy import and_, case, func, not_

from app.db.models import Delivery, DeliveryStatus

NOTIFICATION_STATUSES = ("pending", "processing", "delivered", "failed")


def notification_status(delivery_statuses: set[str]) -> str:
    if "delivered" in delivery_statuses and not delivery_statuses & {"pending", "processing"}:
        return "delivered"
    if delivery_statuses and delivery_statuses <= {"failed", "suppressed"}:
        return "failed"
    if "processing" in delivery_statuses:
        return "processing"
    return "pending"


def notification_status_sql():
    """Aggregate expression; use with GROUP BY notification."""
    work_remaining = func.bool_or(Delivery.status.in_([DeliveryStatus.pending, DeliveryStatus.processing]))
    return case(
        (and_(func.bool_or(Delivery.status == DeliveryStatus.delivered), not_(work_remaining)), "delivered"),
        (func.bool_and(Delivery.status.in_([DeliveryStatus.failed, DeliveryStatus.suppressed])), "failed"),
        (func.bool_or(Delivery.status == DeliveryStatus.processing), "processing"),
        else_="pending",
    )
