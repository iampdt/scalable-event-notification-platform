"""Bearer authentication; credentials establish the tenant boundary."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.errors import ApiError
from app.db.models import ApiKey, ApiKeyRole, Tenant
from app.db.session import get_session
from app.security import token_parts, verify_secret

bearer_scheme = HTTPBearer(auto_error=False)
# Writing last_used_at on every request turns each API call into a write on one
# hot row per key. Minute granularity is enough to answer "is this key in use?".
LAST_USED_GRANULARITY = timedelta(minutes=1)


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
    if key.last_used_at is None or now - key.last_used_at >= LAST_USED_GRANULARITY:
        key.last_used_at = now
    session.commit()
    return key


def admin_key(key: ApiKey = Depends(authenticated_key)) -> ApiKey:
    """Configuration endpoints need an admin credential; producer keys only send and read."""
    if key.role != ApiKeyRole.admin:
        raise ApiError(403, "forbidden", "Credential lacks the required role.")
    return key
