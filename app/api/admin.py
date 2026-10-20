"""Tenant configuration endpoints (admin credential required).

Creating tenants and keys is deliberately *not* here: bootstrapping trust
belongs to deployment tooling (see app/scripts/seed.py), not to an API that
would itself need a credential to call.
"""
from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.auth import Principal, admin_key
from app.cache import get_cache
from app.api.errors import ApiError
from app.api.pagination import apply_cursor, page
from app.api.schemas import CreateWebhookEndpoint, UpdateWebhookEndpoint, UpsertTemplate, UpsertUser
from app.db.models import DeliveryChannel, NotificationTemplate, User, WebhookEndpoint
from app.db.session import get_session
from app.events.service import templates_cache_key
from app.providers.webhook import UnsafeWebhookUrl, validate_webhook_url
from app.security import encrypt_secret, generate_webhook_secret

router = APIRouter(prefix="/v1")


def user_body(user: User) -> dict:
    return {"id": str(user.id), "external_id": user.external_id, "email": user.email, "active": user.active, "created_at": user.created_at.isoformat()}


def endpoint_body(endpoint: WebhookEndpoint) -> dict:
    # Secret material is never echoed back after creation.
    return {"id": str(endpoint.id), "url": endpoint.url, "active": endpoint.active, "is_default": endpoint.is_default, "created_at": endpoint.created_at.isoformat()}


@router.post("/users")
def upsert_user(body: UpsertUser, response: Response, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    """Idempotent by (tenant, external_id): the tenant's own user ID is the natural key."""
    try:
        with session.begin():
            user = session.scalar(select(User).where(User.tenant_id == key.tenant_id, User.external_id == body.external_id).with_for_update())
            if user:
                user.email, user.active = body.email, body.active
                response.status_code = 200
            else:
                user = User(tenant_id=key.tenant_id, external_id=body.external_id, email=body.email, active=body.active)
                session.add(user)
                response.status_code = 201
            session.flush()
            session.refresh(user)
            return user_body(user)
    except IntegrityError as exc:
        # Two creates for the same new external_id raced; the unique constraint kept one.
        raise ApiError(409, "conflict", "User was created concurrently; retry.") from exc


def clear_default(session: Session, tenant_id: uuid.UUID, keep: uuid.UUID | None = None) -> None:
    query = update(WebhookEndpoint).where(WebhookEndpoint.tenant_id == tenant_id, WebhookEndpoint.is_default.is_(True))
    if keep:
        query = query.where(WebhookEndpoint.id != keep)
    session.execute(query.values(is_default=False))


@router.post("/webhook-endpoints", status_code=201)
def create_webhook_endpoint(body: CreateWebhookEndpoint, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    try:
        validate_webhook_url(body.url)
    except UnsafeWebhookUrl as exc:
        raise ApiError(422, "unprocessable", str(exc), [{"field": "url", "issue": str(exc)}]) from exc
    secret = body.secret or generate_webhook_secret()
    try:
        with session.begin():
            if body.is_default and body.active:
                clear_default(session, key.tenant_id)
            endpoint = WebhookEndpoint(tenant_id=key.tenant_id, url=body.url, secret_ciphertext=encrypt_secret(secret), active=body.active, is_default=body.is_default)
            session.add(endpoint)
            session.flush()
            session.refresh(endpoint)
    except IntegrityError as exc:
        # The partial unique index allows one active default per tenant; a
        # concurrent create won the race.
        raise ApiError(409, "conflict", "Another default webhook endpoint was created concurrently; retry.") from exc
    result = endpoint_body(endpoint)
    if not body.secret:
        result["secret"] = secret  # shown exactly once
    return result


@router.get("/webhook-endpoints")
def list_webhook_endpoints(limit: Annotated[int, Query(ge=1, le=100)] = 50, cursor: str | None = None, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    query = apply_cursor(select(WebhookEndpoint).where(WebhookEndpoint.tenant_id == key.tenant_id), WebhookEndpoint.created_at, WebhookEndpoint.id, cursor).limit(limit + 1)
    rows, next_cursor = page(session.scalars(query).all(), limit, lambda row: row.created_at, lambda row: row.id)
    return {"data": [endpoint_body(row) for row in rows], "next_cursor": next_cursor}


@router.patch("/webhook-endpoints/{endpoint_id}")
def update_webhook_endpoint(endpoint_id: uuid.UUID, body: UpdateWebhookEndpoint, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    try:
        with session.begin():
            endpoint = session.scalar(select(WebhookEndpoint).where(WebhookEndpoint.tenant_id == key.tenant_id, WebhookEndpoint.id == endpoint_id).with_for_update())
            if not endpoint:
                raise ApiError(404, "not_found", "Resource was not found.")
            if body.active is not None:
                endpoint.active = body.active
            if body.is_default is not None:
                endpoint.is_default = body.is_default
            if endpoint.active and endpoint.is_default:
                clear_default(session, key.tenant_id, keep=endpoint.id)
            session.flush()
            session.refresh(endpoint)
    except IntegrityError as exc:
        raise ApiError(409, "conflict", "Another default webhook endpoint was set concurrently; retry.") from exc
    return endpoint_body(endpoint)


def template_body(template: NotificationTemplate) -> dict:
    return {"id": str(template.id), "event_type": template.event_type, "channel": template.channel.value, "subject": template.subject, "body": template.body, "version": template.version, "active": template.active, "created_at": template.created_at.isoformat(), "updated_at": template.updated_at.isoformat()}


@router.post("/templates")
def upsert_template(body: UpsertTemplate, response: Response, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    """Idempotent by (tenant, event_type, channel). Editing bumps the version;
    deliveries already planned keep the content rendered from the old version."""
    try:
        with session.begin():
            template = session.scalar(
                select(NotificationTemplate)
                .where(NotificationTemplate.tenant_id == key.tenant_id, NotificationTemplate.event_type == body.event_type, NotificationTemplate.channel == DeliveryChannel(body.channel))
                .with_for_update()
            )
            if template:
                changed = (template.subject, template.body, template.active) != (body.subject, body.body, body.active)
                template.subject, template.body, template.active = body.subject, body.body, body.active
                if changed:
                    template.version += 1
                response.status_code = 200
            else:
                template = NotificationTemplate(tenant_id=key.tenant_id, event_type=body.event_type, channel=DeliveryChannel(body.channel), subject=body.subject, body=body.body, active=body.active)
                session.add(template)
                response.status_code = 201
            session.flush()
            session.refresh(template)
            result = template_body(template)
    except IntegrityError as exc:
        raise ApiError(409, "conflict", "Template was created concurrently; retry.") from exc
    # Invalidate *after* commit. Deleting before commit lets a concurrent reader
    # re-cache the old row in between; deleting after leaves only a small race
    # (a reader that loaded the old row before our commit writes it back late),
    # which the TTL bounds.
    get_cache().delete(templates_cache_key(key.tenant_id, body.event_type))
    return result


@router.get("/templates")
def list_templates(event_type: str | None = None, limit: Annotated[int, Query(ge=1, le=100)] = 50, cursor: str | None = None, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    query = select(NotificationTemplate).where(NotificationTemplate.tenant_id == key.tenant_id)
    if event_type:
        query = query.where(NotificationTemplate.event_type == event_type)
    query = apply_cursor(query, NotificationTemplate.created_at, NotificationTemplate.id, cursor).limit(limit + 1)
    rows, next_cursor = page(session.scalars(query).all(), limit, lambda row: row.created_at, lambda row: row.id)
    return {"data": [template_body(row) for row in rows], "next_cursor": next_cursor}


@router.get("/templates/{template_id}")
def get_template(template_id: uuid.UUID, key: Principal = Depends(admin_key), session: Session = Depends(get_session)) -> dict:
    template = session.scalar(select(NotificationTemplate).where(NotificationTemplate.tenant_id == key.tenant_id, NotificationTemplate.id == template_id))
    if not template:
        raise ApiError(404, "not_found", "Resource was not found.")
    return template_body(template)
