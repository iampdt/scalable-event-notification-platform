"""Transactional event ingestion and fan-out."""
from __future__ import annotations

import hashlib
import json
import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.errors import ApiError
from app.api.schemas import IngestEvent
from app.db.models import Delivery, DeliveryChannel, DeliveryStatus, Event, Notification, User


def normalized_hash(body: IngestEvent) -> str:
    data = body.model_dump(mode="json")
    data["recipients"] = sorted(
        ({**item, "channels": sorted(item["channels"])} for item in data["recipients"]), key=lambda item: item["user_id"]
    )
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def event_summary(event: Event, notification_count: int, delivery_count: int) -> dict:
    return {"id": str(event.id), "event_type": event.event_type, "status": "accepted", "created_at": event.created_at.isoformat(), "notification_count": notification_count, "delivery_count": delivery_count, "links": {"self": f"/v1/events/{event.id}"}}


def ingest(session: Session, tenant_id: uuid.UUID, idempotency_key: str, body: IngestEvent) -> dict:
    request_hash = normalized_hash(body)
    created: Event | None = None
    try:
        with session.begin():
            existing = session.scalar(select(Event).where(Event.tenant_id == tenant_id, Event.idempotency_key == idempotency_key))
            if existing:
                if existing.request_hash != request_hash:
                    raise ApiError(409, "idempotency_conflict", "Idempotency-Key was already used with different content.")
                notifications = session.query(Notification).filter_by(tenant_id=tenant_id, event_id=existing.id).count()
                deliveries = session.query(Delivery).join(Notification, Delivery.notification_id == Notification.id).filter(Notification.event_id == existing.id).count()
                return event_summary(existing, notifications, deliveries)

            requested_ids = [recipient.user_id for recipient in body.recipients]
            users = session.scalars(select(User).where(User.tenant_id == tenant_id, User.id.in_(requested_ids), User.active.is_(True))).all()
            users_by_id = {user.id: user for user in users}
            unknown = [str(user_id) for user_id in requested_ids if user_id not in users_by_id]
            if unknown:
                raise ApiError(422, "unprocessable", "One or more recipients are not active tenant users.", [{"field": "recipients", "issue": f"unknown or inactive user IDs: {', '.join(unknown)}"}])

            created = Event(tenant_id=tenant_id, idempotency_key=idempotency_key, request_hash=request_hash, event_type=body.event_type, subject_type=body.subject.type, subject_id=body.subject.id, payload=body.payload, metadata_=body.metadata)
            session.add(created)
            session.flush()
            for recipient in body.recipients:
                user = users_by_id[recipient.user_id]
                notification = Notification(tenant_id=tenant_id, event_id=created.id, user_id=user.id)
                session.add(notification)
                session.flush()
                for channel in recipient.channels:
                    if channel == "email":
                        target, status = ({"email": user.email}, DeliveryStatus.pending) if user.email else ({}, DeliveryStatus.suppressed)
                    else:
                        target, status = {"user_id": str(user.id)}, DeliveryStatus.pending
                    session.add(Delivery(tenant_id=tenant_id, notification_id=notification.id, channel=DeliveryChannel(channel), status=status, target_snapshot=target))
            session.flush()
        assert created is not None
        return event_summary(created, len(body.recipients), sum(len(item.channels) for item in body.recipients))
    except IntegrityError:
        session.rollback()
        existing = session.scalar(select(Event).where(Event.tenant_id == tenant_id, Event.idempotency_key == idempotency_key))
        if existing and existing.request_hash == request_hash:
            notifications = session.query(Notification).filter_by(tenant_id=tenant_id, event_id=existing.id).count()
            deliveries = session.query(Delivery).join(Notification, Delivery.notification_id == Notification.id).filter(Notification.event_id == existing.id).count()
            return event_summary(existing, notifications, deliveries)
        raise
