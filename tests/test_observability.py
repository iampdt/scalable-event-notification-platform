"""Metrics and structured logs; needs TEST_DATABASE_URL."""

from __future__ import annotations

import json
import logging
import os
import re
import uuid

import httpx
import pytest
from prometheus_client import REGISTRY

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is required")
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def sample(name: str, labels: dict | None = None) -> float:
    return REGISTRY.get_sample_value(name, labels or {}) or 0.0


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

    fixture = create_tenant_with_key(f"obs-{uuid.uuid4().hex[:10]}")
    return fixture, create_api_key(fixture.tenant_id, ApiKeyRole.admin, "admin")


def auth(key: str, idem: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}"}
    if idem:
        headers["Idempotency-Key"] = idem
    return headers


def event(user_id, channels=("in_app",), amount: int = 1) -> dict:
    return {"event_type": "obs.test", "subject": {"type": "t", "id": "1"}, "payload": {"amount": amount}, "recipients": [{"user_id": str(user_id), "channels": list(channels)}]}


def test_http_metrics_use_route_templates_not_raw_paths(client, tenant):
    producer, _ = tenant
    route = {"method": "GET", "route": "/v1/events/{event_id}", "status": "404"}
    before = sample("http_requests_total", route)
    client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key))
    assert sample("http_requests_total", route) == before + 1
    assert sample("http_request_duration_seconds_count", {"method": "GET", "route": "/v1/events/{event_id}"}) >= 1

    unmatched = {"method": "GET", "route": "unmatched", "status": "404"}
    before = sample("http_requests_total", unmatched)
    client.get("/wp-admin/" + uuid.uuid4().hex)  # scanners must not mint a series per URL
    assert sample("http_requests_total", unmatched) == before + 1


def test_metrics_endpoint_never_exposes_ids_as_label_values(client, tenant):
    producer, _ = tenant
    client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, f"card-{uuid.uuid4().hex[:6]}"))
    client.get(f"/v1/notifications/{uuid.uuid4()}", headers=auth(producer.key))
    body = client.get("/metrics").text
    label_values = re.findall(r'="([^"]*)"', body)
    assert label_values and not [value for value in label_values if UUID_RE.search(value)]


def test_queue_gauges_come_from_postgres(client):
    body = client.get("/metrics").text
    assert "queue_metrics_up 1.0" in body
    for series in ('deliveries_queue_depth{state="due"}', "deliveries_oldest_due_seconds", "deliveries_dead_letters", "outbox_depth"):
        assert series in body


def test_queue_collector_reports_down_instead_of_failing_the_scrape(monkeypatch):
    import app.db.session as session_module
    from app.metrics import QueueCollector

    class DeadEngine:
        def connect(self):
            raise ConnectionRefusedError("down")

    monkeypatch.setattr(session_module, "engine", DeadEngine())
    families = list(QueueCollector().collect())
    assert [f.name for f in families] == ["queue_metrics_up"] and families[0].samples[0].value == 0


def test_ingestion_outcomes_are_counted(client, tenant):
    producer, _ = tenant
    counts = {result: sample("events_ingested_total", {"result": result}) for result in ("accepted", "replayed", "conflict", "rejected")}
    planned = sample("deliveries_planned_total", {"channel": "in_app", "status": "pending"})
    key = f"cnt-{uuid.uuid4().hex[:6]}"
    client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, key))
    client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, key))
    client.post("/v1/events", json=event(producer.user_id, amount=2), headers=auth(producer.key, key))
    client.post("/v1/events", json=event(uuid.uuid4()), headers=auth(producer.key, f"{key}-bad"))
    assert {result: sample("events_ingested_total", {"result": result}) - counts[result] for result in counts} == {"accepted": 1, "replayed": 1, "conflict": 1, "rejected": 1}
    assert sample("deliveries_planned_total", {"channel": "in_app", "status": "pending"}) == planned + 1


def test_cache_hit_rate_is_measured_per_cache(client, tenant):
    producer, _ = tenant
    hits = sample("cache_requests_total", {"cache": "credentials", "result": "hit"})
    for _ in range(3):
        client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key))
    # Exactly 2 hits if Redis is up (first lookup misses); with Redis down all are "bypass".
    gained = sample("cache_requests_total", {"cache": "credentials", "result": "hit"}) - hits
    bypass = sample("cache_requests_total", {"cache": "credentials", "result": "bypass"})
    assert gained == 2 or bypass >= 3


def test_worker_metrics_cover_success_latency_and_dead_letters(client, tenant):
    from app.providers.webhook import WebhookProvider
    from app.testing import deliveries_for_event
    from app.workers.delivery_worker import DeliveryWorker

    producer, admin = tenant
    client.post("/v1/webhook-endpoints", json={"url": "https://hooks.example.test/x"}, headers=auth(admin))
    ok_event = client.post("/v1/events", json=event(producer.user_id), headers=auth(producer.key, f"w-{uuid.uuid4().hex[:6]}")).json()["id"]
    bad_event = client.post("/v1/events", json=event(producer.user_id, channels=("webhook",)), headers=auth(producer.key, f"w-{uuid.uuid4().hex[:6]}")).json()["id"]
    ids = [d.id for d in deliveries_for_event(producer.tenant_id, ok_event) + deliveries_for_event(producer.tenant_id, bad_event)]

    success = sample("delivery_attempts_total", {"channel": "in_app", "outcome": "success"})
    e2e = sample("delivery_end_to_end_seconds_count", {"channel": "in_app"})
    dead = sample("deliveries_dead_lettered_total", {"channel": "webhook", "reason": "permanent"})
    worker = DeliveryWorker(webhook_provider=WebhookProvider(client=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(410))), timeout=1))
    worker._process(worker.claim_ids(ids), None)

    assert sample("delivery_attempts_total", {"channel": "in_app", "outcome": "success"}) == success + 1
    assert sample("delivery_end_to_end_seconds_count", {"channel": "in_app"}) == e2e + 1
    assert sample("deliveries_dead_lettered_total", {"channel": "webhook", "reason": "permanent"}) == dead + 1
    assert sample("provider_send_duration_seconds_count", {"channel": "webhook"}) >= 1


def test_json_logs_carry_request_id_and_extra_fields():
    from app.logging_config import JsonFormatter, request_id_var

    token = request_id_var.set("req-123")
    try:
        record = logging.LogRecord("app.access", logging.INFO, __file__, 1, "request", (), None)
        record.route, record.status, record.tenant_id = "/v1/events", 202, "t-1"
        entry = json.loads(JsonFormatter("api").format(record))
    finally:
        request_id_var.reset(token)
    assert entry["request_id"] == "req-123" and entry["service"] == "api" and entry["message"] == "request"
    assert entry["route"] == "/v1/events" and entry["status"] == 202 and entry["tenant_id"] == "t-1"


def test_access_log_has_one_line_per_request_with_tenant_and_route(client, tenant):
    producer, _ = tenant
    records: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = Capture()
    logging.getLogger("app.access").addHandler(handler)
    try:
        response = client.get(f"/v1/events/{uuid.uuid4()}", headers={**auth(producer.key), "X-Request-Id": "trace-me"})
    finally:
        logging.getLogger("app.access").removeHandler(handler)
    [line] = [r for r in records if getattr(r, "route", None) == "/v1/events/{event_id}"]
    assert line.status == 404 and line.tenant_id == str(producer.tenant_id) and line.duration_ms >= 0
    assert response.headers["X-Request-Id"] == "trace-me"
