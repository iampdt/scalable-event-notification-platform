"""Admin configuration, webhook fan-out, and worker retry behaviour; requires TEST_DATABASE_URL."""

from __future__ import annotations

import os
import uuid

import httpx
import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is required for integration tests")

HOOK_URL = "https://hooks.example.test/notify"


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    monkeypatch.setenv("WEBHOOK_ALLOW_PRIVATE_NETWORKS", "true")
    from fastapi.testclient import TestClient
    from app.main import create_app

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture()
def tenant():
    from app.db.models import ApiKeyRole
    from app.testing import create_api_key, create_tenant_with_key

    fixture = create_tenant_with_key(f"hooks-{uuid.uuid4().hex[:10]}")
    return fixture, create_api_key(fixture.tenant_id, ApiKeyRole.admin, "admin")


def auth(key: str, idem: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}"}
    if idem:
        headers["Idempotency-Key"] = idem
    return headers


def event(user_id, channels) -> dict:
    return {"event_type": "order.paid", "subject": {"type": "order", "id": "o1"}, "payload": {"amount": 1}, "recipients": [{"user_id": str(user_id), "channels": channels}]}


def worker_with_webhook_status(status: int, seen: list | None = None):
    from app.providers.webhook import WebhookProvider
    from app.workers.delivery_worker import DeliveryWorker

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(status)

    return DeliveryWorker(webhook_provider=WebhookProvider(client=httpx.Client(transport=httpx.MockTransport(handler)), timeout=1))


def test_producer_key_cannot_use_admin_endpoints(client, tenant):
    producer, _ = tenant
    response = client.post("/v1/users", json={"external_id": "x"}, headers=auth(producer.key))
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "forbidden"
    assert response.headers["X-Request-Id"]


def test_upsert_user_is_idempotent_by_external_id(client, tenant):
    _, admin = tenant
    created = client.post("/v1/users", json={"external_id": "u-1", "email": "a@example.test"}, headers=auth(admin))
    updated = client.post("/v1/users", json={"external_id": "u-1", "email": "b@example.test"}, headers=auth(admin))
    assert (created.status_code, updated.status_code) == (201, 200)
    assert created.json()["id"] == updated.json()["id"]
    assert updated.json()["email"] == "b@example.test"


def test_webhook_secret_is_shown_once_and_default_moves(client, tenant):
    _, admin = tenant
    first = client.post("/v1/webhook-endpoints", json={"url": HOOK_URL}, headers=auth(admin))
    assert first.status_code == 201 and first.json()["secret"].startswith("whsec_")
    second = client.post("/v1/webhook-endpoints", json={"url": HOOK_URL + "/v2", "secret": "s" * 32}, headers=auth(admin))
    assert "secret" not in second.json()

    listed = client.get("/v1/webhook-endpoints", headers=auth(admin)).json()["data"]
    assert all("secret" not in item for item in listed)
    defaults = [item["id"] for item in listed if item["is_default"]]
    assert defaults == [second.json()["id"]]

    patched = client.patch(f"/v1/webhook-endpoints/{first.json()['id']}", json={"is_default": True}, headers=auth(admin))
    assert patched.json()["is_default"] is True
    listed = client.get("/v1/webhook-endpoints", headers=auth(admin)).json()["data"]
    assert [item["id"] for item in listed if item["is_default"]] == [first.json()["id"]]


def test_webhook_rejects_unsafe_url_in_production_mode(client, tenant, monkeypatch):
    _, admin = tenant
    monkeypatch.setenv("WEBHOOK_ALLOW_PRIVATE_NETWORKS", "false")
    response = client.post("/v1/webhook-endpoints", json={"url": "http://169.254.169.254/latest"}, headers=auth(admin))
    assert response.status_code == 422


def test_webhook_without_endpoint_is_suppressed_not_dropped(client, tenant):
    from app.testing import deliveries_for_event

    producer, _ = tenant
    created = client.post("/v1/events", json=event(producer.user_id, ["webhook", "in_app"]), headers=auth(producer.key, "no-hook")).json()
    by_channel = {d.channel.value: d for d in deliveries_for_event(producer.tenant_id, created["id"])}
    assert by_channel["webhook"].status.value == "suppressed"
    assert by_channel["webhook"].webhook_endpoint_id is None
    assert by_channel["in_app"].status.value == "pending"


def test_webhook_is_signed_and_delivered_by_worker(client, tenant):
    from app.security import sign_webhook
    from app.testing import deliveries_for_event

    producer, admin = tenant
    secret = client.post("/v1/webhook-endpoints", json={"url": HOOK_URL}, headers=auth(admin)).json()["secret"]
    created = client.post("/v1/events", json=event(producer.user_id, ["webhook"]), headers=auth(producer.key, "hook-ok")).json()
    delivery = deliveries_for_event(producer.tenant_id, created["id"])[0]
    assert delivery.target_snapshot == {"url": HOOK_URL} and delivery.webhook_endpoint_id

    seen: list[httpx.Request] = []
    worker_with_webhook_status(200, seen).run_once(batch_size=500)
    ours = [r for r in seen if r.headers["X-Notification-Delivery-Id"] == str(delivery.id)]
    assert len(ours) == 1
    request = ours[0]
    assert request.headers["X-Notification-Signature"] == sign_webhook(secret, int(request.headers["X-Notification-Timestamp"]), request.content)
    assert deliveries_for_event(producer.tenant_id, created["id"])[0].status.value == "delivered"


def test_retryable_failures_back_off_then_exhaust(client, tenant):
    from datetime import datetime, timezone
    from app.testing import deliveries_for_event, get_attempts, make_due

    producer, admin = tenant
    client.post("/v1/webhook-endpoints", json={"url": HOOK_URL}, headers=auth(admin))
    created = client.post("/v1/events", json=event(producer.user_id, ["webhook"]), headers=auth(producer.key, "hook-503")).json()
    delivery_id = deliveries_for_event(producer.tenant_id, created["id"])[0].id
    worker = worker_with_webhook_status(503)

    worker.run_once(batch_size=500)
    delivery = deliveries_for_event(producer.tenant_id, created["id"])[0]
    assert delivery.status.value == "pending" and delivery.last_error_code == "503"
    assert delivery.next_attempt_at > datetime.now(timezone.utc)  # backed off, not hot-looping

    for _ in range(4):
        make_due(delivery_id)
        worker.run_once(batch_size=500)
    delivery = deliveries_for_event(producer.tenant_id, created["id"])[0]
    attempts = get_attempts(delivery_id)
    assert delivery.status.value == "failed" and delivery.failed_at is not None
    assert [a.attempt_number for a in attempts] == [1, 2, 3, 4, 5]
    assert {a.outcome for a in attempts} == {"retryable_failure"}  # failed because exhausted


def test_permanent_failure_is_not_retried(client, tenant):
    from app.testing import deliveries_for_event, get_attempts

    producer, admin = tenant
    client.post("/v1/webhook-endpoints", json={"url": HOOK_URL}, headers=auth(admin))
    created = client.post("/v1/events", json=event(producer.user_id, ["webhook"]), headers=auth(producer.key, "hook-410")).json()
    worker_with_webhook_status(410).run_once(batch_size=500)
    delivery = deliveries_for_event(producer.tenant_id, created["id"])[0]
    assert delivery.status.value == "failed" and delivery.attempt_count == 1
    assert [a.outcome for a in get_attempts(delivery.id)] == ["permanent_failure"]


def test_disabled_endpoint_fails_already_planned_deliveries(client, tenant):
    from app.testing import deliveries_for_event

    producer, admin = tenant
    endpoint = client.post("/v1/webhook-endpoints", json={"url": HOOK_URL}, headers=auth(admin)).json()
    created = client.post("/v1/events", json=event(producer.user_id, ["webhook"]), headers=auth(producer.key, "hook-off")).json()
    client.patch(f"/v1/webhook-endpoints/{endpoint['id']}", json={"active": False}, headers=auth(admin))
    seen: list = []
    worker_with_webhook_status(200, seen).run_once(batch_size=500)
    delivery = deliveries_for_event(producer.tenant_id, created["id"])[0]
    assert delivery.status.value == "failed"
    assert not [r for r in seen if r.headers["X-Notification-Delivery-Id"] == str(delivery.id)]
