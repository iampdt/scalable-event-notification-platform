"""Transactional event ingestion and fan-out."""
from __future__ import annotations

import hashlib
import json
import uuid

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.errors import ApiError
from app.api.schemas import IngestEvent
from app.db.models import Delivery, DeliveryChannel, DeliveryStatus, Event, Notification, NotificationTemplate, User, WebhookEndpoint
from app.notifications.templates import TemplateError, render


def normalized_hash(body: IngestEvent) -> str:
    """Hash of the request with order-insensitive lists sorted, so a retry that
    reorders recipients or channels is still recognised as the same request."""
    data = body.model_dump(mode="json")
    data["recipients"] = sorted(
        ({**item, "channels": sorted(item["channels"])} for item in data["recipients"]), key=lambda item: item["user_id"]
    )
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def event_summary(event: Event, notification_count: int, delivery_count: int) -> dict:
    return {"id": str(event.id), "event_type": event.event_type, "status": "accepted", "created_at": event.created_at.isoformat(), "notification_count": notification_count, "delivery_count": delivery_count, "links": {"self": f"/v1/events/{event.id}"}}


def replay_summary(session: Session, tenant_id: uuid.UUID, existing: Event) -> dict:
    notifications = session.scalar(select(func.count()).select_from(Notification).where(Notification.tenant_id == tenant_id, Notification.event_id == existing.id))
    deliveries = session.scalar(
        select(func.count()).select_from(Delivery).join(Notification, (Delivery.tenant_id == Notification.tenant_id) & (Delivery.notification_id == Notification.id)).where(Notification.tenant_id == tenant_id, Notification.event_id == existing.id)
    )
    return event_summary(existing, notifications, deliveries)


def plan_delivery(channel: str, user: User, endpoint: WebhookEndpoint | None) -> tuple[dict, DeliveryStatus, uuid.UUID | None]:
    """Resolve and snapshot the target now. A missing target is recorded as
    suppressed, never silently redirected or dropped."""
    if channel == "email":
        return ({"email": user.email}, DeliveryStatus.pending, None) if user.email else ({}, DeliveryStatus.suppressed, None)
    if channel == "webhook":
        return ({"url": endpoint.url}, DeliveryStatus.pending, endpoint.id) if endpoint else ({}, DeliveryStatus.suppressed, None)
    return {"user_id": str(user.id)}, DeliveryStatus.pending, None


def render_content(template: NotificationTemplate, body: IngestEvent, user: User) -> dict:
    context = {
        "event_type": body.event_type,
        "subject": {"type": body.subject.type, "id": body.subject.id},
        "payload": body.payload,
        "recipient": {"external_id": user.external_id, "email": user.email},
    }
    try:
        return {
            "subject": render(template.subject, context) if template.subject else None,
            "body": render(template.body, context),
            "template_id": str(template.id),
            "template_version": template.version,
        }
    except TemplateError as exc:
        # Rendering at ingestion means the producer hears about a missing field
        # now, as a 422, instead of a delivery failing silently later.
        raise ApiError(422, "unprocessable", "Event payload does not satisfy the notification template.", [{"field": f"template[{template.event_type}/{template.channel.value}]", "issue": str(exc)}]) from exc


def ingest(session: Session, tenant_id: uuid.UUID, idempotency_key: str, body: IngestEvent) -> dict:
    request_hash = normalized_hash(body)
    created: Event | None = None
    try:
        with session.begin():
            existing = session.scalar(select(Event).where(Event.tenant_id == tenant_id, Event.idempotency_key == idempotency_key))
            if existing:
                if existing.request_hash != request_hash:
                    raise ApiError(409, "idempotency_conflict", "Idempotency-Key was already used with different content.")
                return replay_summary(session, tenant_id, existing)

            requested_ids = [recipient.user_id for recipient in body.recipients]
            users = session.scalars(select(User).where(User.tenant_id == tenant_id, User.id.in_(requested_ids), User.active.is_(True))).all()
            users_by_id = {user.id: user for user in users}
            unknown = [str(user_id) for user_id in requested_ids if user_id not in users_by_id]
            if unknown:
                raise ApiError(422, "unprocessable", "One or more recipients are not active tenant users.", [{"field": "recipients", "issue": f"unknown or inactive user IDs: {', '.join(unknown)}"}])

            endpoint = None
            if any("webhook" in recipient.channels for recipient in body.recipients):
                endpoint = session.scalar(select(WebhookEndpoint).where(WebhookEndpoint.tenant_id == tenant_id, WebhookEndpoint.active.is_(True), WebhookEndpoint.is_default.is_(True)))

            requested_channels = {channel for recipient in body.recipients for channel in recipient.channels}
            templates = {
                template.channel.value: template
                for template in session.scalars(
                    select(NotificationTemplate).where(
                        NotificationTemplate.tenant_id == tenant_id,
                        NotificationTemplate.event_type == body.event_type,
                        NotificationTemplate.active.is_(True),
                        NotificationTemplate.channel.in_([DeliveryChannel(c) for c in requested_channels]),
                    )
                )
            }

            created = Event(tenant_id=tenant_id, idempotency_key=idempotency_key, request_hash=request_hash, event_type=body.event_type, subject_type=body.subject.type, subject_id=body.subject.id, payload=body.payload, metadata_=body.metadata)
            session.add(created)
            session.flush()
            # IDs are generated here rather than by the database so the whole
            # fan-out goes out in one batched flush instead of a round trip per row.
            notifications, deliveries = [], []
            for recipient in body.recipients:
                user = users_by_id[recipient.user_id]
                notification = Notification(id=uuid.uuid4(), tenant_id=tenant_id, event_id=created.id, user_id=user.id)
                notifications.append(notification)
                for channel in recipient.channels:
                    target, status, endpoint_id = plan_delivery(channel, user, endpoint)
                    template = templates.get(channel)
                    content = render_content(template, body, user) if template and status == DeliveryStatus.pending else None
                    deliveries.append(Delivery(tenant_id=tenant_id, notification_id=notification.id, channel=DeliveryChannel(channel), status=status, target_snapshot=target, webhook_endpoint_id=endpoint_id, content=content))
            session.add_all(notifications)
            session.flush()
            session.add_all(deliveries)
            session.flush()
        assert created is not None
        return event_summary(created, len(notifications), len(deliveries))
    except IntegrityError:
        # Two requests with the same key raced past the SELECT above; the unique
        # constraint let exactly one insert win. Answer the loser like a retry.
        session.rollback()
        existing = session.scalar(select(Event).where(Event.tenant_id == tenant_id, Event.idempotency_key == idempotency_key))
        if not existing:
            raise
        if existing.request_hash != request_hash:
            raise ApiError(409, "idempotency_conflict", "Idempotency-Key was already used with different content.")
        return replay_summary(session, tenant_id, existing)
