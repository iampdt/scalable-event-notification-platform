"""Helpers used only by local integration tests."""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.db.models import ApiKey, ApiKeyRole, Delivery, DeliveryAttempt, DeliveryChannel, DeliveryStatus, Event, Notification, Tenant, User
from app.db.session import SessionLocal
from app.security import generate_api_key


@dataclass(frozen=True)
class TenantFixture:
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    key: str


def create_tenant_with_key(slug: str) -> TenantFixture:
    token, prefix, secret_hash = generate_api_key()
    with SessionLocal.begin() as session:
        tenant = Tenant(slug=slug, name=slug)
        session.add(tenant)
        session.flush()
        user = User(tenant_id=tenant.id, external_id="user", email=f"{slug}@example.test")
        session.add(user)
        session.add(ApiKey(tenant_id=tenant.id, key_prefix=prefix, secret_hash=secret_hash, role=ApiKeyRole.producer, name="producer"))
        session.flush()
        return TenantFixture(tenant.id, user.id, token)


def notification_id_for_event(tenant_id: uuid.UUID, event_id: str) -> uuid.UUID:
    with SessionLocal() as session:
        return session.scalar(select(Notification.id).where(Notification.tenant_id == tenant_id, Notification.event_id == uuid.UUID(event_id)))


def create_delivery(channel: str) -> uuid.UUID:
    fixture = create_tenant_with_key(f"worker-{uuid.uuid4().hex[:12]}")
    with SessionLocal.begin() as session:
        event = Event(tenant_id=fixture.tenant_id, idempotency_key="test", request_hash="test", event_type="test.event", subject_type="test", subject_id="1", payload={}, metadata_={})
        session.add(event)
        session.flush()
        notification = Notification(tenant_id=fixture.tenant_id, event_id=event.id, user_id=fixture.user_id)
        session.add(notification)
        session.flush()
        delivery = Delivery(tenant_id=fixture.tenant_id, notification_id=notification.id, channel=DeliveryChannel(channel), status=DeliveryStatus.pending, target_snapshot={"email": "worker@example.test"} if channel == "email" else {"user_id": str(fixture.user_id)})
        session.add(delivery)
        session.flush()
        return delivery.id


def expire_lease(delivery_id: uuid.UUID) -> None:
    with SessionLocal.begin() as session:
        delivery = session.get(Delivery, delivery_id)
        delivery.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)


def get_delivery(delivery_id: uuid.UUID) -> Delivery:
    with SessionLocal() as session:
        return session.get(Delivery, delivery_id)


def get_attempts(delivery_id: uuid.UUID) -> list[DeliveryAttempt]:
    with SessionLocal() as session:
        return list(session.scalars(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == delivery_id).order_by(DeliveryAttempt.attempt_number)))


def create_api_key(tenant_id: uuid.UUID, role: ApiKeyRole, name: str) -> str:
    token, prefix, secret_hash = generate_api_key()
    with SessionLocal.begin() as session:
        session.add(ApiKey(tenant_id=tenant_id, key_prefix=prefix, secret_hash=secret_hash, role=role, name=name))
    return token


def deliveries_for_event(tenant_id: uuid.UUID, event_id: str) -> list[Delivery]:
    with SessionLocal() as session:
        return list(session.scalars(
            select(Delivery).join(Notification, Delivery.notification_id == Notification.id)
            .where(Notification.tenant_id == tenant_id, Notification.event_id == uuid.UUID(event_id))
            .order_by(Delivery.channel)
        ))


def make_due(delivery_id: uuid.UUID) -> None:
    """Skip a retry backoff so the next claim picks the delivery up."""
    with SessionLocal.begin() as session:
        session.get(Delivery, delivery_id).next_attempt_at = datetime.now(timezone.utc) - timedelta(seconds=1)
