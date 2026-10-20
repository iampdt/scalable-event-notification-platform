"""In-app feed, read receipts, notification listing, and the ingestion race; requires TEST_DATABASE_URL."""

from __future__ import annotations

import os
import threading
import time
import uuid

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
def tenants():
    from app.testing import create_tenant_with_key

    suffix = uuid.uuid4().hex[:10]
    return create_tenant_with_key(f"feed-a-{suffix}"), create_tenant_with_key(f"feed-b-{suffix}")


def auth(key: str, idem: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}"}
    if idem:
        headers["Idempotency-Key"] = idem
    return headers


def post(client, tenant, idem: str, channels=("in_app",), amount: int = 1) -> dict:
    body = {"event_type": "order.paid", "subject": {"type": "order", "id": idem}, "payload": {"amount": amount}, "recipients": [{"user_id": str(tenant.user_id), "channels": list(channels)}]}
    response = client.post("/v1/events", json=body, headers=auth(tenant.key, idem))
    assert response.status_code == 202, response.text
    return response.json()


def drain():
    from app.workers.delivery_worker import DeliveryWorker

    DeliveryWorker().run_once(batch_size=1000)


def test_feed_shows_only_delivered_in_app_items_newest_first(client, tenants):
    tenant, _ = tenants
    post(client, tenant, "feed-1")
    feed = client.get(f"/v1/users/{tenant.user_id}/in-app-notifications", headers=auth(tenant.key)).json()
    assert feed["data"] == []  # planned but not yet delivered by the worker

    post(client, tenant, "feed-2")
    drain()
    feed = client.get(f"/v1/users/{tenant.user_id}/in-app-notifications", headers=auth(tenant.key)).json()
    assert [item["subject"]["id"] for item in feed["data"]] == ["feed-2", "feed-1"]


def test_mark_read_is_idempotent_and_tenant_scoped(client, tenants):
    tenant, other = tenants
    post(client, tenant, "read-1")
    drain()
    item = client.get(f"/v1/users/{tenant.user_id}/in-app-notifications", headers=auth(tenant.key)).json()["data"][0]

    assert client.post(f"/v1/in-app-notifications/{item['delivery_id']}/read", headers=auth(other.key)).status_code == 404
    first = client.post(f"/v1/in-app-notifications/{item['delivery_id']}/read", headers=auth(tenant.key)).json()
    time.sleep(0.01)
    second = client.post(f"/v1/in-app-notifications/{item['delivery_id']}/read", headers=auth(tenant.key)).json()
    assert first["read_at"] is not None and first["read_at"] == second["read_at"]

    unread = client.get(f"/v1/users/{tenant.user_id}/in-app-notifications?unread_only=true", headers=auth(tenant.key)).json()
    assert unread["data"] == []


def test_feed_for_another_tenants_user_is_404(client, tenants):
    tenant, other = tenants
    assert client.get(f"/v1/users/{tenant.user_id}/in-app-notifications", headers=auth(other.key)).status_code == 404


def test_notification_list_paginates_with_cursor_and_filters_by_status(client, tenants):
    tenant, _ = tenants
    for index in range(3):
        post(client, tenant, f"page-{index}")
    drain()
    post(client, tenant, "page-pending")

    first = client.get("/v1/notifications?limit=2", headers=auth(tenant.key)).json()
    assert len(first["data"]) == 2 and first["next_cursor"]
    second = client.get(f"/v1/notifications?limit=2&cursor={first['next_cursor']}", headers=auth(tenant.key)).json()
    assert len(second["data"]) == 2 and second["next_cursor"] is None
    ids = [item["id"] for item in first["data"] + second["data"]]
    assert len(set(ids)) == 4

    pending = client.get("/v1/notifications?status=pending", headers=auth(tenant.key)).json()["data"]
    delivered = client.get("/v1/notifications?status=delivered", headers=auth(tenant.key)).json()["data"]
    assert len(pending) == 1 and len(delivered) == 3


def test_invalid_cursor_is_a_validation_error(client, tenants):
    tenant, _ = tenants
    response = client.get("/v1/notifications?cursor=not-a-cursor", headers=auth(tenant.key))
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "validation_error"


@pytest.mark.parametrize("same_body,expected", [(True, "replay"), (False, "conflict")])
def test_concurrent_same_key_resolves_through_unique_constraint(tenants, same_body, expected):
    """Deterministically force the race: request B's SELECT sees nothing because
    A has not committed, then B's INSERT blocks on A's unique key until A commits."""
    from app.api.errors import ApiError
    from app.api.schemas import IngestEvent
    from app.db.models import Event
    from app.db.session import SessionLocal
    from app.events.service import ingest, normalized_hash

    tenant, _ = tenants
    body = IngestEvent.model_validate({"event_type": "order.paid", "subject": {"type": "order", "id": "race"}, "payload": {"amount": 1}, "recipients": [{"user_id": str(tenant.user_id), "channels": ["in_app"]}]})
    other_body = body.model_copy(update={"payload": {"amount": 2}})
    result: dict = {}

    holder = SessionLocal()
    holder.begin()
    winner = Event(tenant_id=tenant.tenant_id, idempotency_key="race-key", request_hash=normalized_hash(body), event_type="order.paid", subject_type="order", subject_id="race", payload={"amount": 1}, metadata_={})
    holder.add(winner)
    holder.flush()

    def loser():
        with SessionLocal() as session:
            try:
                result["value"] = ingest(session, tenant.tenant_id, "race-key", body if same_body else other_body)
            except ApiError as exc:
                result["error"] = exc.status_code

    thread = threading.Thread(target=loser)
    thread.start()
    time.sleep(0.3)  # let B block on the unique index
    holder.commit()
    holder.close()
    thread.join(timeout=5)

    if expected == "replay":
        assert result["value"]["id"] == str(winner.id)
    else:
        assert result["error"] == 409
