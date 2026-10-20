"""Bearer authentication; credentials establish the tenant boundary.

Every request needs the key's hash, role, and tenant status. That lookup is
cache-aside in Redis for CACHE_TTL_SECONDS:

- The secret is still verified on every request; only the *record* is cached.
- Staleness is bounded by the TTL: revoking a key, disabling a tenant, or
  changing its rate limit in the database takes effect within 60 s. That is the
  price of removing the per-request lookup; `invalidate_credential` makes it
  immediate where the change goes through code.
- Redis down = read from PostgreSQL, exactly as before caching existed.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app import ratelimit
from app.api.errors import ApiError
from app.cache import get_cache
from app.config import cache_ttl_seconds
from app.db.models import ApiKey, ApiKeyRole, Tenant
from app.db.session import get_session
from app.metrics import CACHE_REQUESTS, RATE_LIMITED
from app.security import token_parts, verify_secret

bearer_scheme = HTTPBearer(auto_error=False)
# Writing last_used_at on every request turns each API call into a write on one
# hot row per key. Minute granularity is enough to answer "is this key in use?".
LAST_USED_GRANULARITY = timedelta(minutes=1)
UNAUTHORIZED = ("unauthorized", "Missing or invalid credential.")


@dataclass(frozen=True)
class Principal:
    """Who is calling. Plain data (not an ORM row) so it can come from the cache."""

    api_key_id: uuid.UUID
    tenant_id: uuid.UUID
    role: ApiKeyRole
    rate_limit_per_second: float | None = None
    rate_limit_burst: int | None = None


def credential_cache_key(prefix: str) -> str:
    return f"auth:{prefix}"


def invalidate_credential(prefix: str) -> None:
    get_cache().delete(credential_cache_key(prefix))


def load_credential(session: Session, prefix: str) -> dict | None:
    cache = get_cache()
    cached = cache.get_json(credential_cache_key(prefix))
    if cached is not None:
        CACHE_REQUESTS.labels("credentials", "hit").inc()
        return cached
    CACHE_REQUESTS.labels("credentials", "miss" if cache.enabled else "bypass").inc()
    # One join instead of separate key and tenant lookups. Unknown prefixes are
    # not cached (no negative caching): random-prefix spraying would only fill
    # Redis, and the indexed prefix lookup is cheap.
    row = session.execute(
        select(ApiKey, Tenant).join(Tenant, Tenant.id == ApiKey.tenant_id).where(ApiKey.key_prefix == prefix, ApiKey.revoked_at.is_(None))
    ).first()
    if row is None:
        return None
    key, tenant = row
    record = {
        "api_key_id": str(key.id),
        "tenant_id": str(key.tenant_id),
        "role": key.role.value,
        "secret_hash": key.secret_hash,
        "expires_at": key.expires_at.isoformat() if key.expires_at else None,
        "last_used_at": key.last_used_at.isoformat() if key.last_used_at else None,
        "tenant_active": tenant.status == "active",
        "rate_limit_per_second": tenant.rate_limit_per_second,
        "rate_limit_burst": tenant.rate_limit_burst,
    }
    cache.set_json(credential_cache_key(prefix), record, cache_ttl_seconds())
    return record


def touch_last_used(session: Session, record: dict, now: datetime) -> None:
    key_id = uuid.UUID(record["api_key_id"])
    # SET NX EX: the first request per minute *across all API processes* wins the write.
    # bool(): redis-py returns None when NX loses, which must not look like "Redis unavailable".
    won = get_cache().call("lastused", lambda r: bool(r.set(f"lastused:{key_id}", 1, nx=True, ex=int(LAST_USED_GRANULARITY.total_seconds()))))
    if won is None:  # no Redis: fall back to the timestamp we just read
        last = datetime.fromisoformat(record["last_used_at"]) if record["last_used_at"] else None
        won = last is None or now - last >= LAST_USED_GRANULARITY
    if won:
        session.execute(update(ApiKey).where(ApiKey.id == key_id).values(last_used_at=now))


def authenticated_key(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
    session: Session = Depends(get_session),
) -> Principal:
    if not credentials or credentials.scheme.lower() != "bearer":
        raise ApiError(401, *UNAUTHORIZED)
    parts = token_parts(credentials.credentials)
    if not parts:
        raise ApiError(401, *UNAUTHORIZED)
    prefix, secret = parts
    record = load_credential(session, prefix)
    now = datetime.now(timezone.utc)
    if (
        record is None
        or not record["tenant_active"]
        or (record["expires_at"] and datetime.fromisoformat(record["expires_at"]) <= now)
        or not verify_secret(secret, record["secret_hash"])
    ):
        raise ApiError(401, *UNAUTHORIZED)
    touch_last_used(session, record, now)
    if session.in_transaction():
        session.commit()  # end the auth transaction before the route begins its own
    request.state.tenant_id = record["tenant_id"]  # for the access log (logs, not metric labels)
    return Principal(
        api_key_id=uuid.UUID(record["api_key_id"]),
        tenant_id=uuid.UUID(record["tenant_id"]),
        role=ApiKeyRole(record["role"]),
        rate_limit_per_second=record["rate_limit_per_second"],
        rate_limit_burst=record["rate_limit_burst"],
    )


def admin_key(key: Principal = Depends(authenticated_key)) -> Principal:
    """Configuration endpoints need an admin credential; producer keys only send and read."""
    if key.role != ApiKeyRole.admin:
        raise ApiError(403, "forbidden", "Credential lacks the required role.")
    return key


def rate_limited_key(key: Principal = Depends(authenticated_key)) -> Principal:
    """Per-tenant ingestion limit, so one tenant cannot fill the shared queue."""
    decision = ratelimit.check(key.tenant_id, key.rate_limit_per_second, key.rate_limit_burst)
    if not decision.allowed:
        RATE_LIMITED.inc()
        retry_after = max(1, int(decision.retry_after_seconds + 0.999))
        raise ApiError(429, "rate_limited", f"Tenant ingestion rate limit exceeded; retry after {retry_after}s.", headers={"Retry-After": str(retry_after)})
    return key
