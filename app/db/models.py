"""SQLAlchemy models for the durable notification queue.

These mirror the Alembic migrations, which are the source of truth for DDL.
Application code never calls create_all().
"""

from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, LargeBinary, DateTime, Enum, ForeignKey, ForeignKeyConstraint, Index, Integer, Text, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ApiKeyRole(str, enum.Enum):
    producer = "producer"
    admin = "admin"


class DeliveryChannel(str, enum.Enum):
    email = "email"
    webhook = "webhook"
    in_app = "in_app"


class DeliveryStatus(str, enum.Enum):
    pending = "pending"
    processing = "processing"
    delivered = "delivered"
    failed = "failed"
    suppressed = "suppressed"


class Tenant(Base):
    __tablename__ = "tenants"
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    slug: Mapped[str] = mapped_column(Text, unique=True)
    name: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, server_default=text("'active'"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class ApiKey(Base):
    __tablename__ = "api_keys"
    __table_args__ = (UniqueConstraint("tenant_id", "name"),)
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    key_prefix: Mapped[str] = mapped_column(Text, unique=True)
    secret_hash: Mapped[str] = mapped_column(Text)
    role: Mapped[ApiKeyRole] = mapped_column(Enum(ApiKeyRole, name="api_key_role"), server_default=text("'producer'"))
    name: Mapped[str] = mapped_column(Text)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class User(Base):
    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("tenant_id", "external_id"), UniqueConstraint("tenant_id", "id"))
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    external_id: Mapped[str] = mapped_column(Text)
    email: Mapped[str | None] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class WebhookEndpoint(Base):
    __tablename__ = "webhook_endpoints"
    __table_args__ = (UniqueConstraint("tenant_id", "id"),)
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    url: Mapped[str] = mapped_column(Text)
    secret_ciphertext: Mapped[bytes] = mapped_column(LargeBinary)
    active: Mapped[bool] = mapped_column(Boolean, server_default=text("true"))
    is_default: Mapped[bool] = mapped_column(Boolean, server_default=text("false"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Event(Base):
    __tablename__ = "events"
    __table_args__ = (UniqueConstraint("tenant_id", "idempotency_key"), UniqueConstraint("tenant_id", "id"))
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text)
    request_hash: Mapped[str] = mapped_column(Text)
    event_type: Mapped[str] = mapped_column(Text)
    subject_type: Mapped[str] = mapped_column(Text)
    subject_id: Mapped[str] = mapped_column(Text)
    payload: Mapped[dict] = mapped_column(JSONB)
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (UniqueConstraint("tenant_id", "id"), UniqueConstraint("event_id", "user_id"), ForeignKeyConstraint(["tenant_id", "event_id"], ["events.tenant_id", "events.id"]), ForeignKeyConstraint(["tenant_id", "user_id"], ["users.tenant_id", "users.id"]))
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Delivery(Base):
    __tablename__ = "deliveries"
    __table_args__ = (UniqueConstraint("tenant_id", "id"), UniqueConstraint("notification_id", "channel"), ForeignKeyConstraint(["tenant_id", "notification_id"], ["notifications.tenant_id", "notifications.id"]), CheckConstraint("attempt_count >= 0"), CheckConstraint("max_attempts > 0"), CheckConstraint("jsonb_typeof(target_snapshot) = 'object'"), CheckConstraint("(status <> 'delivered') OR delivered_at IS NOT NULL"), CheckConstraint("(status <> 'failed') OR failed_at IS NOT NULL"), ForeignKeyConstraint(["tenant_id", "webhook_endpoint_id"], ["webhook_endpoints.tenant_id", "webhook_endpoints.id"]), CheckConstraint("channel = 'webhook' OR webhook_endpoint_id IS NULL"), CheckConstraint("channel <> 'webhook' OR webhook_endpoint_id IS NOT NULL OR status = 'suppressed'"))
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    notification_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    channel: Mapped[DeliveryChannel] = mapped_column(Enum(DeliveryChannel, name="delivery_channel"), nullable=False)
    status: Mapped[DeliveryStatus] = mapped_column(Enum(DeliveryStatus, name="delivery_status"), server_default=text("'pending'"))
    target_snapshot: Mapped[dict] = mapped_column(JSONB)
    webhook_endpoint_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    attempt_count: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(Integer, server_default=text("5"))
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    __table_args__ = (UniqueConstraint("delivery_id", "attempt_number"), CheckConstraint("outcome IN ('success', 'retryable_failure', 'permanent_failure')"), CheckConstraint("attempt_number > 0"), ForeignKeyConstraint(["tenant_id", "delivery_id"], ["deliveries.tenant_id", "deliveries.id"]))
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    delivery_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str] = mapped_column(Text)
    provider_status_code: Mapped[int | None] = mapped_column(Integer)
    provider_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


Index("one_active_default_webhook_per_tenant", WebhookEndpoint.tenant_id, unique=True, postgresql_where=WebhookEndpoint.active & WebhookEndpoint.is_default)
Index("idx_api_keys_active", ApiKey.key_prefix, postgresql_where=ApiKey.revoked_at.is_(None))
Index("idx_events_tenant_created", Event.tenant_id, Event.created_at.desc())
Index("idx_notifications_tenant_created", Notification.tenant_id, Notification.created_at.desc())
Index("idx_notifications_user_created", Notification.tenant_id, Notification.user_id, Notification.created_at.desc())
Index("idx_deliveries_due", Delivery.next_attempt_at, Delivery.id, postgresql_where=Delivery.status == DeliveryStatus.pending)
Index("idx_deliveries_expired_lease", Delivery.lease_expires_at, Delivery.id, postgresql_where=Delivery.status == DeliveryStatus.processing)
Index("idx_deliveries_notification", Delivery.tenant_id, Delivery.notification_id)
Index("idx_attempts_delivery", DeliveryAttempt.tenant_id, DeliveryAttempt.delivery_id, DeliveryAttempt.attempt_number)
