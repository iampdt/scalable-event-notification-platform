"""V1 producer and status routes."""
from __future__ import annotations

import uuid
from collections import Counter
from typing import Annotated

from fastapi import APIRouter, Depends, Header
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.auth import authenticated_key
from app.api.errors import ApiError
from app.api.schemas import IngestEvent
from app.db.models import ApiKey, Delivery, Event, Notification
from app.db.session import get_session
from app.events.service import ingest

router = APIRouter(prefix="/v1")


def valid_idempotency_key(value: str | None) -> str:
    if not value or not (1 <= len(value) <= 128) or any(ord(char) < 33 or ord(char) > 126 for char in value):
        raise ApiError(400, "validation_error", "Idempotency-Key must be 1–128 visible ASCII characters.")
    return value


@router.post("/events", status_code=202)
def post_event(
    body: IngestEvent,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    key: ApiKey = Depends(authenticated_key),
    session: Session = Depends(get_session),
) -> dict:
    return ingest(session, key.tenant_id, valid_idempotency_key(idempotency_key), body)


@router.get("/events/{event_id}")
def get_event(event_id: uuid.UUID, key: ApiKey = Depends(authenticated_key), session: Session = Depends(get_session)) -> dict:
    event = session.scalar(select(Event).where(Event.id == event_id, Event.tenant_id == key.tenant_id))
    if not event:
        raise ApiError(404, "not_found", "Resource was not found.")
    statuses = Counter(
        status for status, in session.execute(
            select(Delivery.status).join(Notification, Delivery.notification_id == Notification.id).where(Notification.event_id == event.id, Delivery.tenant_id == key.tenant_id)
        )
    )
    return {"id": str(event.id), "event_type": event.event_type, "subject": {"type": event.subject_type, "id": event.subject_id}, "payload": event.payload, "metadata": event.metadata_, "status": "accepted", "created_at": event.created_at.isoformat(), "deliveries": {"total": sum(statuses.values()), **{name: statuses.get(name, 0) for name in ("pending", "processing", "delivered", "failed", "suppressed")}}}


@router.get("/notifications/{notification_id}")
def get_notification(notification_id: uuid.UUID, key: ApiKey = Depends(authenticated_key), session: Session = Depends(get_session)) -> dict:
    notification = session.scalar(select(Notification).where(Notification.id == notification_id, Notification.tenant_id == key.tenant_id))
    if not notification:
        raise ApiError(404, "not_found", "Resource was not found.")
    deliveries = session.scalars(select(Delivery).where(Delivery.notification_id == notification.id, Delivery.tenant_id == key.tenant_id).order_by(Delivery.channel)).all()
    states = {delivery.status.value for delivery in deliveries}
    if "delivered" in states and not states.intersection({"pending", "processing"}):
        state = "delivered"
    elif states and states.issubset({"failed", "suppressed"}):
        state = "failed"
    elif "processing" in states:
        state = "processing"
    else:
        state = "pending"
    return {"id": str(notification.id), "event_id": str(notification.event_id), "user_id": str(notification.user_id), "status": state, "deliveries": [{"id": str(item.id), "channel": item.channel.value, "status": item.status.value, "attempt_count": item.attempt_count, "next_attempt_at": item.next_attempt_at.isoformat() if item.next_attempt_at else None, "delivered_at": item.delivered_at.isoformat() if item.delivered_at else None, "failed_at": item.failed_at.isoformat() if item.failed_at else None} for item in deliveries]}
