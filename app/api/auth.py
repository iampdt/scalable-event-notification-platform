"""Bearer authentication; credentials establish the tenant boundary."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.errors import ApiError
from app.db.models import ApiKey, Tenant
from app.db.session import get_session
from app.security import token_parts, verify_secret

bearer_scheme = HTTPBearer(auto_error=False)


def authenticated_key(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
    session: Session = Depends(get_session),
) -> ApiKey:
    if not credentials or credentials.scheme.lower() != "bearer":
        raise ApiError(401, "unauthorized", "Missing or invalid credential.")
    parts = token_parts(credentials.credentials)
    if not parts:
        raise ApiError(401, "unauthorized", "Missing or invalid credential.")
    prefix, secret = parts
    key = session.scalar(select(ApiKey).where(ApiKey.key_prefix == prefix, ApiKey.revoked_at.is_(None)))
    now = datetime.now(timezone.utc)
    if not key or (key.expires_at and key.expires_at <= now) or not verify_secret(secret, key.secret_hash):
        raise ApiError(401, "unauthorized", "Missing or invalid credential.")
    tenant = session.get(Tenant, key.tenant_id)
    if not tenant or tenant.status != "active":
        raise ApiError(401, "unauthorized", "Missing or invalid credential.")
    key.last_used_at = now
    session.commit()
    return key
