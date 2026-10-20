"""Dead-letter queue API (admin): inspect failed deliveries and replay them."""
from __future__ import annotations

import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import Field
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import distinct_on
from sqlalchemy.orm import Session

from app.api.auth import Principal, admin_key
from app.api.errors import ApiError
from app.api.pagination import apply_cursor, page
from app.api.schemas import StrictModel
from app.db.models import Delivery, DeliveryAttempt, DeliveryChannel, DeliveryRedrive, DeliveryStatus, Event, Notification
from app.db.session import get_session
from app.notifications.redrive import MAX_BULK_REDRIVE, RedriveFilter, redrive

router = APIRouter(prefix="/v1/dead-letters")


class RedriveOne(StrictModel):
    reason: Annotated[str | None, Field(max_length=500)] = None
    extra_attempts: Annotated[int, Field(ge=1, le=10)] = 3


class RedriveMany(RedriveOne):
    channel: Literal["email", "webhook", "in_app"] | None = None
    error_code: Annotated[str | None, Field(max_length=100)] = None
    limit: Annotated[int, Field(ge=1, le=MAX_BULK_REDRIVE)] = 100


def iso(value):
    return value.isoformat() if value else None


def attempt_body(attempt: DeliveryAttempt) -> dict:
    return {"attempt_number": attempt.attempt_number, "outcome": attempt.outcome, "provider_status_code": attempt.provider_status_code, "provider_message": attempt.provider_message, "started_at": iso(attempt.started_at), "completed_at": iso(attempt.completed_at)}


def dead_letter_query(tenant_id: uuid.UUID):
    return (
        select(Delivery, Notification.user_id, Event.id.label("event_id"), Event.event_type)
        .join(Notification, (Notification.tenant_id == Delivery.tenant_id) & (Notification.id == Delivery.notification_id))
        .join(Event, (Event.tenant_id == Notification.tenant_id) & (Event.id == Notification.event_id))
        .where(Delivery.tenant_id == tenant_id, Delivery.status == DeliveryStatus.failed)
    )


def dead_letter_body(delivery: Delivery, user_id, event_id, event_type, last_attempt: DeliveryAttempt | None, redrive_count: int) -> dict:
    return {
        "delivery_id": str(delivery.id),
        "notification_id": str(delivery.notification_id),
        "event_id": str(event_id),
        "event_type": event_type,
        "user_id": str(user_id),
        "channel": delivery.channel.value,
        "attempt_count": delivery.attempt_count,
        "max_attempts": delivery.max_attempts,
        "last_error_code": delivery.last_error_code,
        "failed_at": iso(delivery.failed_at),
        "redrive_count": redrive_count,
        "last_attempt": attempt_body(last_attempt) if last_attempt else None,
    }


@router.get("")
def list_dead_letters(
    channel: Literal["email", "webhook", "in_app"] | None = None,
    error_code: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: str | None = None,
    key: Principal = Depends(admin_key),
    session: Session = Depends(get_session),
) -> dict:
    query = dead_letter_query(key.tenant_id)
    if channel:
        query = query.where(Delivery.channel == DeliveryChannel(channel))
    if error_code:
        query = query.where(Delivery.last_error_code == error_code)
    query = apply_cursor(query, Delivery.failed_at, Delivery.id, cursor).limit(limit + 1)
    rows, next_cursor = page(session.execute(query).all(), limit, lambda row: row[0].failed_at, lambda row: row[0].id)
    ids = [row[0].id for row in rows]
    # Two set-based lookups for the whole page instead of two queries per row.
    last_attempts = {
        attempt.delivery_id: attempt
        for attempt in session.scalars(
            select(DeliveryAttempt).where(DeliveryAttempt.tenant_id == key.tenant_id, DeliveryAttempt.delivery_id.in_(ids)).order_by(DeliveryAttempt.delivery_id, DeliveryAttempt.attempt_number.desc()).ext(distinct_on(DeliveryAttempt.delivery_id))
        )
    } if ids else {}
    redrives = dict(session.execute(select(DeliveryRedrive.delivery_id, func.count()).where(DeliveryRedrive.tenant_id == key.tenant_id, DeliveryRedrive.delivery_id.in_(ids)).group_by(DeliveryRedrive.delivery_id)).all()) if ids else {}
    return {"data": [dead_letter_body(*row, last_attempts.get(row[0].id), redrives.get(row[0].id, 0)) for row in rows], "next_cursor": next_cursor}


@router.get("/{delivery_id}")
def get_dead_letter(delivery_id: uuid.UUID, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    row = session.execute(dead_letter_query(key.tenant_id).where(Delivery.id == delivery_id)).first()
    if not row:
        raise ApiError(404, "not_found", "Resource was not found.")
    attempts = session.scalars(select(DeliveryAttempt).where(DeliveryAttempt.tenant_id == key.tenant_id, DeliveryAttempt.delivery_id == delivery_id).order_by(DeliveryAttempt.attempt_number)).all()
    history = session.scalars(select(DeliveryRedrive).where(DeliveryRedrive.tenant_id == key.tenant_id, DeliveryRedrive.delivery_id == delivery_id).order_by(DeliveryRedrive.created_at)).all()
    body = dead_letter_body(*row, attempts[-1] if attempts else None, len(history))
    body["attempts"] = [attempt_body(attempt) for attempt in attempts]
    body["redrives"] = [{"reason": r.reason, "previous_attempt_count": r.previous_attempt_count, "previous_error_code": r.previous_error_code, "extra_attempts": r.extra_attempts, "created_at": iso(r.created_at)} for r in history]
    return body


@router.post("/{delivery_id}/redrive", status_code=202)
def redrive_one(delivery_id: uuid.UUID, body: RedriveOne | None = None, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    body = body or RedriveOne()
    with session.begin():
        redriven = redrive(session, key.tenant_id, key.api_key_id, RedriveFilter(delivery_ids=[delivery_id], limit=1), body.extra_attempts, body.reason)
    if not redriven:
        # Unknown, other tenant's, or no longer failed (e.g. already redriven): a
        # 404 never reveals which, and a second redrive cannot double-enqueue.
        raise ApiError(404, "not_found", "No failed delivery with this id.")
    return {"redriven": [str(delivery_id)], "status": "pending"}


@router.post("/redrive", status_code=202)
def redrive_many(body: RedriveMany, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    """Bulk replay, e.g. after fixing a broken endpoint: filter by channel/error code, capped per call."""
    with session.begin():
        redriven = redrive(session, key.tenant_id, key.api_key_id, RedriveFilter(channel=body.channel, error_code=body.error_code, limit=body.limit), body.extra_attempts, body.reason)
    return {"redriven_count": len(redriven), "redriven": [str(i) for i in redriven]}
