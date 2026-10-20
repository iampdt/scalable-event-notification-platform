"""In-app feed. The tenant authenticates, not the end user: the tenant's own app
must check that its caller is allowed to read this user_id."""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.api.auth import Principal, authenticated_key
from app.api.errors import ApiError
from app.api.pagination import apply_cursor, page
from app.db.models import Delivery, DeliveryChannel, DeliveryStatus, Event, Notification, User
from app.db.session import get_session

router = APIRouter(prefix="/v1")


def feed_item(delivery: Delivery, notification: Notification, event: Event) -> dict:
    return {"delivery_id": str(delivery.id), "notification_id": str(notification.id), "event_type": event.event_type, "subject": {"type": event.subject_type, "id": event.subject_id}, "title": (delivery.content or {}).get("subject"), "body": (delivery.content or {}).get("body"), "payload": event.payload, "read_at": delivery.read_at.isoformat() if delivery.read_at else None, "created_at": notification.created_at.isoformat()}


@router.get("/users/{user_id}/in-app-notifications")
def in_app_feed(
    user_id: uuid.UUID,
    unread_only: bool = False,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: str | None = None,
    key: Principal = Depends(authenticated_key),
    session: Session = Depends(get_session),
) -> dict:
    if not session.scalar(select(User.id).where(User.tenant_id == key.tenant_id, User.id == user_id)):
        raise ApiError(404, "not_found", "Resource was not found.")
    # Driven by idx_notifications_user_created; each notification has at most one
    # in_app delivery, found through the unique (notification_id, channel) index.
    query = (
        select(Delivery, Notification, Event)
        .join(Notification, (Notification.tenant_id == Delivery.tenant_id) & (Notification.id == Delivery.notification_id))
        .join(Event, (Event.tenant_id == Notification.tenant_id) & (Event.id == Notification.event_id))
        .where(Notification.tenant_id == key.tenant_id, Notification.user_id == user_id, Delivery.channel == DeliveryChannel.in_app, Delivery.status == DeliveryStatus.delivered)
    )
    if unread_only:
        query = query.where(Delivery.read_at.is_(None))
    query = apply_cursor(query, Notification.created_at, Notification.id, cursor).limit(limit + 1)
    rows, next_cursor = page(session.execute(query).all(), limit, lambda row: row[1].created_at, lambda row: row[1].id)
    return {"data": [feed_item(*row) for row in rows], "next_cursor": next_cursor}


@router.post("/in-app-notifications/{delivery_id}/read")
def mark_read(delivery_id: uuid.UUID, key: Principal = Depends(authenticated_key), session: Session = Depends(get_session)) -> dict:
    with session.begin():
        # COALESCE makes the call idempotent: a repeat keeps the first read time.
        updated = session.execute(
            update(Delivery)
            .where(Delivery.id == delivery_id, Delivery.tenant_id == key.tenant_id, Delivery.channel == DeliveryChannel.in_app, Delivery.status == DeliveryStatus.delivered)
            .values(read_at=func.coalesce(Delivery.read_at, func.now()))
            .returning(Delivery.id)
        ).first()
        if not updated:
            raise ApiError(404, "not_found", "Resource was not found.")
        row = session.execute(
            select(Delivery, Notification, Event)
            .join(Notification, (Notification.tenant_id == Delivery.tenant_id) & (Notification.id == Delivery.notification_id))
            .join(Event, (Event.tenant_id == Notification.tenant_id) & (Event.id == Notification.event_id))
            .where(Delivery.id == delivery_id)
            .execution_options(populate_existing=True)
        ).one()
        return feed_item(*row)
