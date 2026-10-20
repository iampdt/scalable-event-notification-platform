"""Redis: rate limiting, cache-aside, invalidation, and fail-open behaviour.

Needs TEST_DATABASE_URL and a reachable Redis (TEST_REDIS_URL, default db 15).
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")


def redis_reachable() -> bool:
    try:
        import redis

        return bool(redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/15"), socket_timeout=0.2).ping())
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not TEST_DATABASE_URL or not redis_reachable(), reason="needs TEST_DATABASE_URL and Redis")
DEAD_REDIS = "redis://127.0.0.1:1/0"  # nothing listens on port 1


@pytest.fixture(autouse=True)
def fresh_cache_client():
    from app.cache import reset_cache

    reset_cache()
    yield
    reset_cache()


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    from fastapi.testclient import TestClient
    from app.main import create_app

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture()
def tenant():
    from app.db.models import ApiKeyRole
    from app.testing import create_api_key, create_tenant_with_key

    fixture = create_tenant_with_key(f"redis-{uuid.uuid4().hex[:10]}")
    return fixture, create_api_key(fixture.tenant_id, ApiKeyRole.admin, "admin")


def auth(key: str, idem: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}"}
    if idem:
        headers["Idempotency-Key"] = idem
    return headers


def event(user_id, event_type: str = "order.paid", payload: dict | None = None, channels=("in_app",)) -> dict:
    return {"event_type": event_type, "subject": {"type": "order", "id": "o1"}, "payload": payload or {"amount": 1}, "recipients": [{"user_id": str(user_id), "channels": list(channels)}]}


def set_tenant_limit(tenant_id, rate: float, burst: int) -> None:
    from sqlalchemy import update
    from app.db.models import Tenant
    from app.db.session import SessionLocal

    with SessionLocal.begin() as session:
        session.execute(update(Tenant).where(Tenant.id == tenant_id).values(rate_limit_per_second=rate, rate_limit_burst=burst))


# --- rate limiting ---------------------------------------------------------------

def test_gcra_allows_the_burst_then_paces_to_the_rate():
    from app import ratelimit

    tenant_id = uuid.uuid4()
    decisions = [ratelimit.check(tenant_id, rate_per_second=2, burst=3) for _ in range(4)]
    assert [d.allowed for d in decisions] == [True, True, True, False]
    assert [d.remaining for d in decisions[:3]] == [2, 1, 0]
    assert 0 < decisions[3].retry_after_seconds <= 0.5  # one token every 0.5 s
    time.sleep(decisions[3].retry_after_seconds + 0.05)
    assert ratelimit.check(tenant_id, rate_per_second=2, burst=3).allowed


def test_limited_tenant_gets_429_with_retry_after_and_others_are_unaffected(client, tenant):
    from app.testing import create_tenant_with_key

    producer, _ = tenant
    set_tenant_limit(producer.tenant_id, rate=0.2, burst=2)
    statuses = [client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, f"rl-{i}")).status_code for i in range(3)]
    # The limit is read with the credential, so it must be set before the first call.
    assert statuses == [202, 202, 429]
    limited = client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, "rl-x"))
    assert limited.json()["error"]["code"] == "rate_limited"
    assert 1 <= int(limited.headers["Retry-After"]) <= 5

    other = create_tenant_with_key(f"redis-other-{uuid.uuid4().hex[:8]}")
    assert client.post("/v1/events", json=event(other.user_id), headers=auth(other.key, "rl-other")).status_code == 202


def test_reads_are_not_rate_limited(client, tenant):
    producer, _ = tenant
    set_tenant_limit(producer.tenant_id, rate=0.2, burst=1)
    assert all(client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key)).status_code == 404 for _ in range(5))


# --- cache-aside -------------------------------------------------------------------

def test_credentials_are_cached_and_the_ttl_bounds_revocation(client, tenant):
    """Documents the trade-off: a DB-only revocation is honoured once the entry is
    invalidated (immediately via code) or expires (CACHE_TTL_SECONDS)."""
    from sqlalchemy import update
    from app.api.auth import credential_cache_key, invalidate_credential
    from app.cache import get_cache
    from app.db.models import ApiKey
    from app.db.session import SessionLocal

    producer, _ = tenant
    prefix = "_".join(producer.key.split("_", 3)[:3])
    assert client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key)).status_code == 404
    assert get_cache().client.ttl(credential_cache_key(prefix)) > 0

    with SessionLocal.begin() as session:
        session.execute(update(ApiKey).where(ApiKey.key_prefix == prefix).values(revoked_at=ApiKey.created_at))
    assert client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key)).status_code == 404  # still cached
    invalidate_credential(prefix)
    assert client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key)).status_code == 401


def test_wrong_secret_is_rejected_even_with_a_cached_record(client, tenant):
    producer, _ = tenant
    client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key))  # populate cache
    prefix = "_".join(producer.key.split("_", 3)[:3])
    assert client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(prefix + "_wrong")).status_code == 401


def test_template_cache_is_negative_cached_and_invalidated_on_edit(client, tenant):
    from app.cache import get_cache
    from app.events.service import templates_cache_key
    from app.testing import deliveries_for_event

    producer, admin = tenant
    key = templates_cache_key(producer.tenant_id, "order.paid")
    client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, "tc-1"))
    assert get_cache().get_json(key) == {}  # "no templates" is cached too

    template = {"event_type": "order.paid", "channel": "in_app", "subject": "Paid", "body": "v1 {{ payload.amount }}"}
    client.post("/v1/templates", json=template, headers=auth(admin))
    assert get_cache().get_json(key) is None  # invalidated after commit
    first = client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, "tc-2")).json()
    client.post("/v1/templates", json={**template, "body": "v2 {{ payload.amount }}"}, headers=auth(admin))
    second = client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, "tc-3")).json()

    assert deliveries_for_event(producer.tenant_id, first["id"])[0].content["body"] == "v1 1"
    assert deliveries_for_event(producer.tenant_id, second["id"])[0].content["body"] == "v2 1"  # no stale read


def test_warm_cache_removes_lookup_queries(client, tenant):
    """Deterministic proof of the optimisation: count SQL statements per request."""
    from sqlalchemy import event as sa_event
    from app.db.session import engine

    producer, _ = tenant
    statements: list[str] = []
    listener = lambda conn, cursor, statement, *args: statements.append(statement.split()[0] + " " + statement.split()[-1])
    sa_event.listen(engine, "before_cursor_execute", listener)
    try:
        client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key))  # warm
        statements.clear()
        client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key))
        get_count = len(statements)
        client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, "qc-warm-1"))
        statements.clear()
        client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, "qc-warm-2"))
        post_statements = list(statements)
    finally:
        sa_event.remove(engine, "before_cursor_execute", listener)
    assert get_count == 1  # was 3: api_keys + tenants + events
    assert not any("api_keys" in s or "tenants" in s or "notification_templates" in s for s in post_statements)


def test_last_used_at_is_written_once_per_minute_across_requests(client, tenant):
    from sqlalchemy import select
    from app.db.models import ApiKey
    from app.db.session import SessionLocal

    producer, _ = tenant
    prefix = "_".join(producer.key.split("_", 3)[:3])
    for _ in range(3):
        client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key))
    with SessionLocal() as session:
        first = session.scalar(select(ApiKey.last_used_at).where(ApiKey.key_prefix == prefix))
    client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key))
    with SessionLocal() as session:
        assert session.scalar(select(ApiKey.last_used_at).where(ApiKey.key_prefix == prefix)) == first


# --- Redis unavailable --------------------------------------------------------------

def test_everything_still_works_when_redis_is_down(client, tenant, monkeypatch):
    from app.cache import get_cache, reset_cache

    producer, admin = tenant
    monkeypatch.setenv("REDIS_URL", DEAD_REDIS)
    reset_cache()
    started = time.perf_counter()
    assert client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, "down-1")).status_code == 202
    assert client.post("/v1/templates", json={"event_type": "x.y", "channel": "in_app", "body": "b"}, headers=auth(admin)).status_code == 201
    assert get_cache().enabled is False  # breaker open: later calls skip Redis entirely
    for i in range(20):
        assert client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, f"down-{i + 2}")).status_code == 202
    assert time.perf_counter() - started < 5  # no per-request timeout penalty


def test_rate_limiter_fails_open_without_redis(monkeypatch):
    from app import ratelimit
    from app.cache import reset_cache

    monkeypatch.setenv("REDIS_URL", DEAD_REDIS)
    reset_cache()
    assert all(ratelimit.check(uuid.uuid4(), rate_per_second=1, burst=1).allowed for _ in range(5))
