"""Authentication failures and database-enforced invariants; requires TEST_DATABASE_URL."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is required for integration tests")


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

    fixture = create_tenant_with_key(f"tpl-{uuid.uuid4().hex[:10]}")
    return fixture, create_api_key(fixture.tenant_id, ApiKeyRole.admin, "admin")


def auth(key: str, idem: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}"}
    if idem:
        headers["Idempotency-Key"] = idem
    return headers


# --- authentication ------------------------------------------------------------

@pytest.mark.parametrize("header", [None, "Basic dXNlcjpwYXNz", "Bearer not-a-key", "Bearer ntf_live_unknown_secret"])
def test_malformed_or_unknown_credentials_are_401(client, header):
    headers = {"Authorization": header} if header else {}
    response = client.get(f"/v1/events/{uuid.uuid4()}", headers=headers)
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


def test_wrong_secret_for_a_real_prefix_is_401(client, tenant):
    producer, _ = tenant
    # Keys look like ntf_live_<public id>_<secret>; keep the real prefix, swap the secret.
    prefix = "_".join(producer.key.split("_", 3)[:3])
    assert client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(prefix + "_wrongsecret")).status_code == 401


@pytest.mark.parametrize("state", ["revoked", "expired", "tenant_disabled"])
def test_revoked_expired_or_disabled_credentials_are_401(client, tenant, state):
    from sqlalchemy import update
    from app.db.models import ApiKey, Tenant
    from app.db.session import SessionLocal

    producer, _ = tenant
    past = datetime.now(timezone.utc) - timedelta(minutes=1)
    with SessionLocal.begin() as session:
        if state == "revoked":
            session.execute(update(ApiKey).where(ApiKey.tenant_id == producer.tenant_id).values(revoked_at=past))
        elif state == "expired":
            session.execute(update(ApiKey).where(ApiKey.tenant_id == producer.tenant_id).values(expires_at=past))
        else:
            session.execute(update(Tenant).where(Tenant.id == producer.tenant_id).values(status="disabled"))
    assert client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key)).status_code == 401


# --- database-enforced invariants -------------------------------------------------

def test_composite_foreign_key_blocks_cross_tenant_links(tenant):
    """Even a buggy query cannot attach tenant B's notification to tenant A's event."""
    from sqlalchemy.exc import IntegrityError
    from app.db.models import Event, Notification
    from app.db.session import SessionLocal
    from app.testing import create_tenant_with_key

    tenant_a, _ = tenant
    tenant_b = create_tenant_with_key(f"tpl-b-{uuid.uuid4().hex[:10]}")
    with SessionLocal.begin() as session:
        event_row = Event(tenant_id=tenant_a.tenant_id, idempotency_key="fk", request_hash="h", event_type="t", subject_type="s", subject_id="1", payload={}, metadata_={})
        session.add(event_row)
    with pytest.raises(IntegrityError) as caught, SessionLocal.begin() as session:
        session.add(Notification(tenant_id=tenant_b.tenant_id, event_id=event_row.id, user_id=tenant_b.user_id))
    assert caught.value.orig.sqlstate == FOREIGN_KEY_VIOLATION


# SQLSTATE classes: assert the *kind* of violation (names differ between databases
# created by the old create_all migration and the frozen DDL).
CHECK_VIOLATION, FOREIGN_KEY_VIOLATION, UNIQUE_VIOLATION = "23514", "23503", "23505"


@pytest.mark.parametrize(
    "sql,sqlstate",
    [
        ("UPDATE deliveries SET status = 'delivered', delivered_at = NULL WHERE id = :id", CHECK_VIOLATION),
        ("UPDATE deliveries SET status = 'failed', failed_at = NULL WHERE id = :id", CHECK_VIOLATION),
        ("UPDATE deliveries SET attempt_count = -1 WHERE id = :id", CHECK_VIOLATION),
        ("UPDATE deliveries SET webhook_endpoint_id = gen_random_uuid() WHERE id = :id", CHECK_VIOLATION),
        ("UPDATE deliveries SET content = '[1]'::jsonb WHERE id = :id", CHECK_VIOLATION),
        ("INSERT INTO deliveries (tenant_id, notification_id, channel, target_snapshot) SELECT tenant_id, notification_id, channel, target_snapshot FROM deliveries WHERE id = :id", UNIQUE_VIOLATION),
    ],
    ids=["delivered_needs_timestamp", "failed_needs_timestamp", "attempts_non_negative", "endpoint_only_for_webhook", "content_is_object", "one_delivery_per_channel"],
)
def test_delivery_invariants_are_enforced_by_the_database(sql, sqlstate):
    from sqlalchemy import text
    from sqlalchemy.exc import IntegrityError
    from app.db.session import SessionLocal
    from app.testing import create_delivery

    delivery_id = create_delivery(channel="email")
    with pytest.raises(IntegrityError) as caught, SessionLocal.begin() as session:
        session.execute(text(sql), {"id": delivery_id})
    assert caught.value.orig.sqlstate == sqlstate


def test_tenant_slug_and_attempt_outcome_checks():
    from sqlalchemy import text
    from sqlalchemy.exc import IntegrityError
    from app.db.session import SessionLocal

    for sql in ("INSERT INTO tenants (slug, name) VALUES ('Bad Slug!', 'x')", "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, outcome) SELECT tenant_id, id, 1, 'maybe' FROM deliveries LIMIT 1"):
        with pytest.raises(IntegrityError) as caught, SessionLocal.begin() as session:
            session.execute(text(sql))
        assert caught.value.orig.sqlstate == CHECK_VIOLATION
