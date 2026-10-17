"""Integration checks for the tenant boundary and ingestion idempotency.

Run with TEST_DATABASE_URL set to an isolated PostgreSQL database.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is required for integration tests"
)


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    from fastapi.testclient import TestClient
    from app.main import create_app

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture()
def tenant_data():
    """The test runner supplies these rows through the seed helpers in conftest."""
    from app.testing import create_tenant_with_key

    suffix = uuid.uuid4().hex[:10]
    first = create_tenant_with_key(f"tenant-a-{suffix}")
    second = create_tenant_with_key(f"tenant-b-{suffix}")
    return first, second


def event_body(user_id: str, amount: int = 4999) -> dict:
    return {
        "event_type": "order.paid",
        "subject": {"type": "order", "id": "ord_123"},
        "payload": {"order_id": "ord_123", "amount": amount, "currency": "INR"},
        "recipients": [{"user_id": user_id, "channels": ["email", "in_app"]}],
        "metadata": {"source": "test"},
    }


def headers(key: str, idem: str = "order-123-paid-v1") -> dict[str, str]:
    return {"Authorization": f"Bearer {key}", "Idempotency-Key": idem}


def test_same_key_and_body_returns_original_event(client, tenant_data):
    tenant, _ = tenant_data
    response = client.post("/v1/events", json=event_body(str(tenant.user_id)), headers=headers(tenant.key))
    assert response.status_code == 202

    repeated = client.post("/v1/events", json=event_body(str(tenant.user_id)), headers=headers(tenant.key))
    assert repeated.status_code == 202
    assert repeated.json()["id"] == response.json()["id"]
    assert repeated.json()["delivery_count"] == 2


def test_same_key_with_changed_body_conflicts(client, tenant_data):
    tenant, _ = tenant_data
    assert client.post("/v1/events", json=event_body(str(tenant.user_id)), headers=headers(tenant.key)).status_code == 202
    conflict = client.post(
        "/v1/events", json=event_body(str(tenant.user_id), amount=5000), headers=headers(tenant.key)
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_conflict"


def test_other_tenant_cannot_read_event_or_notification(client, tenant_data):
    tenant_a, tenant_b = tenant_data
    created = client.post(
        "/v1/events", json=event_body(str(tenant_a.user_id)), headers=headers(tenant_a.key)
    ).json()
    assert client.get(f"/v1/events/{created['id']}", headers=headers(tenant_b.key)).status_code == 404

    from app.testing import notification_id_for_event

    notification_id = notification_id_for_event(tenant_a.tenant_id, created["id"])
    assert client.get(f"/v1/notifications/{notification_id}", headers=headers(tenant_b.key)).status_code == 404


def test_worker_makes_planned_deliveries_visible_as_delivered(client, tenant_data):
    tenant, _ = tenant_data
    created = client.post(
        "/v1/events", json=event_body(str(tenant.user_id)), headers=headers(tenant.key, "worker-status")
    ).json()
    from app.testing import notification_id_for_event
    from app.workers.delivery_worker import DeliveryWorker

    DeliveryWorker().run_once(batch_size=100)
    notification_id = notification_id_for_event(tenant.tenant_id, created["id"])
    response = client.get(f"/v1/notifications/{notification_id}", headers=headers(tenant.key))
    assert response.status_code == 200
    assert {delivery["status"] for delivery in response.json()["deliveries"]} == {"delivered"}
