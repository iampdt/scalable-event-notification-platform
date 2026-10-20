"""Notification templates: admin API, rendering at ingestion, snapshots; requires TEST_DATABASE_URL."""

from __future__ import annotations

import os
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


def event(user_id, payload, channels=("email", "in_app")) -> dict:
    return {"event_type": "order.paid", "subject": {"type": "order", "id": "o1"}, "payload": payload, "recipients": [{"user_id": str(user_id), "channels": list(channels)}]}


EMAIL_TEMPLATE = {"event_type": "order.paid", "channel": "email", "subject": "Order {{ subject.id }} paid", "body": "Hi {{ recipient.external_id }}, we received {{ payload.amount }} {{ payload.currency }}."}
IN_APP_TEMPLATE = {"event_type": "order.paid", "channel": "in_app", "subject": "Payment received", "body": "{{ payload.amount }} {{ payload.currency }}"}



def test_template_upsert_versions_only_on_change(client, tenant):
    _, admin = tenant
    created = client.post("/v1/templates", json=EMAIL_TEMPLATE, headers=auth(admin))
    same = client.post("/v1/templates", json=EMAIL_TEMPLATE, headers=auth(admin))
    edited = client.post("/v1/templates", json={**EMAIL_TEMPLATE, "body": "v2 {{ payload.amount }}"}, headers=auth(admin))
    assert (created.status_code, same.status_code, edited.status_code) == (201, 200, 200)
    assert [r.json()["version"] for r in (created, same, edited)] == [1, 1, 2]
    assert len({r.json()["id"] for r in (created, same, edited)}) == 1
    listed = client.get("/v1/templates?event_type=order.paid", headers=auth(admin)).json()["data"]
    assert [t["version"] for t in listed] == [2]
    assert client.get(f"/v1/templates/{created.json()['id']}", headers=auth(admin)).json()["body"].startswith("v2")


def test_template_validation_and_roles(client, tenant):
    producer, admin = tenant
    assert client.post("/v1/templates", json=EMAIL_TEMPLATE, headers=auth(producer.key)).status_code == 403
    for bad in ({**EMAIL_TEMPLATE, "body": "{{ os.environ }}"}, {**EMAIL_TEMPLATE, "subject": None}, {**EMAIL_TEMPLATE, "channel": "webhook"}, {**EMAIL_TEMPLATE, "body": "{{ payload.x | safe }}"}):
        response = client.post("/v1/templates", json=bad, headers=auth(admin))
        assert response.status_code == 400, bad


def test_ingestion_renders_templates_into_deliveries(client, tenant):
    from app.testing import deliveries_for_event

    producer, admin = tenant
    client.post("/v1/templates", json=EMAIL_TEMPLATE, headers=auth(admin))
    client.post("/v1/templates", json=IN_APP_TEMPLATE, headers=auth(admin))
    created = client.post("/v1/events", json=event(producer.user_id, {"amount": 4999, "currency": "INR"}), headers=auth(producer.key, "tpl-1")).json()
    by_channel = {d.channel.value: d for d in deliveries_for_event(producer.tenant_id, created["id"])}
    assert by_channel["email"].content["subject"] == "Order o1 paid"
    assert by_channel["email"].content["body"] == "Hi user, we received 4999 INR."
    assert by_channel["email"].content["template_version"] == 1
    assert by_channel["in_app"].content == {"subject": "Payment received", "body": "4999 INR", "template_id": by_channel["in_app"].content["template_id"], "template_version": 1}

    from app.workers.delivery_worker import DeliveryWorker
    DeliveryWorker().run_once(batch_size=1000)
    item = client.get(f"/v1/users/{producer.user_id}/in-app-notifications", headers=auth(producer.key)).json()["data"][0]
    assert (item["title"], item["body"]) == ("Payment received", "4999 INR")


def test_missing_template_variable_rejects_the_whole_event(client, tenant):
    from app.db.models import Event
    from app.db.session import SessionLocal
    from sqlalchemy import func, select

    producer, admin = tenant
    client.post("/v1/templates", json=EMAIL_TEMPLATE, headers=auth(admin))
    response = client.post("/v1/events", json=event(producer.user_id, {"amount": 4999}), headers=auth(producer.key, "tpl-missing"))
    assert response.status_code == 422
    assert "payload.currency" in response.json()["error"]["details"][0]["issue"]
    with SessionLocal() as session:
        assert session.scalar(select(func.count()).select_from(Event).where(Event.tenant_id == producer.tenant_id)) == 0


def test_template_edits_do_not_change_already_planned_deliveries(client, tenant):
    from app.testing import deliveries_for_event

    producer, admin = tenant
    client.post("/v1/templates", json=IN_APP_TEMPLATE, headers=auth(admin))
    created = client.post("/v1/events", json=event(producer.user_id, {"amount": 1, "currency": "INR"}, ["in_app"]), headers=auth(producer.key, "tpl-snap")).json()
    client.post("/v1/templates", json={**IN_APP_TEMPLATE, "body": "changed"}, headers=auth(admin))
    assert deliveries_for_event(producer.tenant_id, created["id"])[0].content["body"] == "1 INR"


def test_inactive_template_falls_back_to_default_content(client, tenant):
    from app.testing import deliveries_for_event

    producer, admin = tenant
    client.post("/v1/templates", json={**EMAIL_TEMPLATE, "active": False}, headers=auth(admin))
    created = client.post("/v1/events", json=event(producer.user_id, {"amount": 1}, ["email"]), headers=auth(producer.key, "tpl-off")).json()
    assert deliveries_for_event(producer.tenant_id, created["id"])[0].content is None
