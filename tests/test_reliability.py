"""Reliability: database outages, readiness, dead-letter replay, circuit breakers.

Needs TEST_DATABASE_URL; circuit-breaker tests also need Redis (db 15).
"""

from __future__ import annotations

import os
import time
import uuid

import httpx
import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is required")
HOOK_URL = "https://hooks.example.test/notify"


def redis_reachable() -> bool:
    try:
        import redis

        return bool(redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/15"), socket_timeout=0.2).ping())
    except Exception:
        return False


needs_redis = pytest.mark.skipif(not redis_reachable(), reason="needs Redis")


@pytest.fixture(autouse=True)
def fresh_cache_client():
    from app.cache import reset_cache

    reset_cache()
    yield
    reset_cache()


@pytest.fixture()
def app_and_client(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    monkeypatch.setenv("WEBHOOK_ALLOW_PRIVATE_NETWORKS", "true")
    from fastapi.testclient import TestClient
    from app.main import create_app

    application = create_app()
    with TestClient(application, raise_server_exceptions=False) as test_client:
        yield application, test_client


@pytest.fixture()
def client(app_and_client):
    return app_and_client[1]


@pytest.fixture()
def tenant():
    from app.db.models import ApiKeyRole
    from app.testing import create_api_key, create_tenant_with_key

    fixture = create_tenant_with_key(f"rel-{uuid.uuid4().hex[:10]}")
    return fixture, create_api_key(fixture.tenant_id, ApiKeyRole.admin, "admin")


def auth(key: str, idem: str | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}"}
    if idem:
        headers["Idempotency-Key"] = idem
    return headers


def post_webhook_event(client, producer, idem: str) -> str:
    body = {"event_type": "rel.test", "subject": {"type": "t", "id": idem}, "payload": {}, "recipients": [{"user_id": str(producer.user_id), "channels": ["webhook"]}]}
    response = client.post("/v1/events", json=body, headers=auth(producer.key, idem))
    assert response.status_code == 202, response.text
    return response.json()["id"]


def webhook_worker(status: int | Exception, seen: list | None = None):
    from app.providers.webhook import WebhookProvider
    from app.workers.delivery_worker import DeliveryWorker

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        if isinstance(status, Exception):
            raise status
        return httpx.Response(status)

    return DeliveryWorker(webhook_provider=WebhookProvider(client=httpx.Client(transport=httpx.MockTransport(handler)), timeout=1))


def run_until_settled(worker, delivery_ids, rounds: int = 3) -> None:
    """Process only these deliveries (the shared test DB holds other tests' rows)."""
    for _ in range(rounds):
        worker._process(worker.claim_ids(list(delivery_ids)), None)


# --- PostgreSQL unavailable ----------------------------------------------------------

def test_database_outage_returns_503_envelope_with_retry_after(app_and_client, tenant):
    from sqlalchemy.exc import OperationalError
    from app.db.session import get_session

    application, client = app_and_client
    producer, _ = tenant

    def broken_session():
        raise OperationalError("SELECT 1", {}, ConnectionRefusedError("database down"))
        yield  # pragma: no cover

    application.dependency_overrides[get_session] = broken_session
    response = client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key))
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"
    assert response.json()["error"]["code"] == "service_unavailable"
    assert response.json()["error"]["request_id"]


def test_unexpected_errors_keep_the_error_envelope(app_and_client, tenant):
    from app.db.session import get_session

    application, client = app_and_client
    producer, _ = tenant

    def buggy_session():
        raise RuntimeError("bug")
        yield  # pragma: no cover

    application.dependency_overrides[get_session] = buggy_session
    response = client.get(f"/v1/events/{uuid.uuid4()}", headers=auth(producer.key))
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
    assert "bug" not in response.text  # details stay in logs


def test_readiness_reflects_postgres_but_liveness_does_not(client, monkeypatch):
    import app.db.session as session_module

    assert client.get("/readyz").status_code == 200

    class DeadEngine:
        def connect(self):
            raise ConnectionRefusedError("down")

    monkeypatch.setattr(session_module, "engine", DeadEngine())
    ready = client.get("/readyz")
    assert ready.status_code == 503 and ready.json()["checks"]["postgres"].startswith("unavailable")
    assert client.get("/healthz").status_code == 200  # don't restart healthy processes for a DB outage


def test_worker_backoff_doubles_and_caps(monkeypatch):
    from app.workers.delivery_worker import Backoff

    sleeps: list[float] = []
    monkeypatch.setattr("app.workers.delivery_worker.time.sleep", sleeps.append)
    backoff = Backoff(base=1, cap=8)
    for _ in range(6):
        backoff.failed("database", RuntimeError("down"))
    assert sleeps == [1, 2, 4, 8, 8, 8]
    backoff.succeeded()
    backoff.failed("database", RuntimeError("down"))
    assert sleeps[-1] == 1


# --- dead-letter queue -------------------------------------------------------------------

@pytest.fixture()
def dead_letter(client, tenant):
    """A webhook delivery that failed permanently (410)."""
    from app.testing import deliveries_for_event

    producer, admin = tenant
    client.post("/v1/webhook-endpoints", json={"url": HOOK_URL}, headers=auth(admin))
    event_id = post_webhook_event(client, producer, f"dlq-{uuid.uuid4().hex[:6]}")
    delivery = deliveries_for_event(producer.tenant_id, event_id)[0]
    run_until_settled(webhook_worker(410), [delivery.id], rounds=1)
    return producer, admin, event_id, delivery.id


def test_dead_letters_list_and_detail_explain_the_failure(client, dead_letter):
    producer, admin, event_id, delivery_id = dead_letter
    assert client.get("/v1/dead-letters", headers=auth(producer.key)).status_code == 403
    listed = client.get("/v1/dead-letters?channel=webhook", headers=auth(admin)).json()["data"]
    [item] = [d for d in listed if d["delivery_id"] == str(delivery_id)]
    assert item["event_id"] == event_id and item["last_error_code"] == "410"
    assert item["last_attempt"]["outcome"] == "permanent_failure" and item["last_attempt"]["provider_status_code"] == 410
    detail = client.get(f"/v1/dead-letters/{delivery_id}", headers=auth(admin)).json()
    assert [a["attempt_number"] for a in detail["attempts"]] == [1] and detail["redrives"] == []


def test_redrive_reopens_with_fresh_budget_audit_and_history(client, dead_letter):
    from app.testing import get_attempts, get_delivery

    producer, admin, _, delivery_id = dead_letter
    response = client.post(f"/v1/dead-letters/{delivery_id}/redrive", json={"reason": "receiver fixed", "extra_attempts": 2}, headers=auth(admin))
    assert response.status_code == 202
    delivery = get_delivery(delivery_id)
    assert delivery.status.value == "pending" and delivery.max_attempts == 3 and delivery.failed_at is None
    assert client.post(f"/v1/dead-letters/{delivery_id}/redrive", headers=auth(admin)).status_code == 404  # not failed anymore

    run_until_settled(webhook_worker(200), [delivery_id], rounds=1)
    assert get_delivery(delivery_id).status.value == "delivered"
    assert [(a.attempt_number, a.outcome) for a in get_attempts(delivery_id)] == [(1, "permanent_failure"), (2, "success")]  # history kept


def test_redrive_audit_trail_is_visible_after_a_second_failure(client, dead_letter):
    producer, admin, _, delivery_id = dead_letter
    client.post(f"/v1/dead-letters/{delivery_id}/redrive", json={"reason": "try again"}, headers=auth(admin))
    run_until_settled(webhook_worker(410), [delivery_id], rounds=1)
    detail = client.get(f"/v1/dead-letters/{delivery_id}", headers=auth(admin)).json()
    assert detail["redrive_count"] == 1
    assert detail["redrives"][0] == {**detail["redrives"][0], "reason": "try again", "previous_attempt_count": 1, "previous_error_code": "410", "extra_attempts": 3}


def test_bulk_redrive_by_error_code_is_tenant_scoped(client, dead_letter):
    from app.testing import create_api_key, create_tenant_with_key, get_delivery
    from app.db.models import ApiKeyRole

    producer, admin, _, delivery_id = dead_letter
    other = create_tenant_with_key(f"rel-other-{uuid.uuid4().hex[:8]}")
    other_admin = create_api_key(other.tenant_id, ApiKeyRole.admin, "admin")
    assert client.post("/v1/dead-letters/redrive", json={"error_code": "410"}, headers=auth(other_admin)).json()["redriven_count"] == 0
    assert client.post(f"/v1/dead-letters/{delivery_id}/redrive", headers=auth(other_admin)).status_code == 404
    result = client.post("/v1/dead-letters/redrive", json={"error_code": "410", "channel": "webhook"}, headers=auth(admin)).json()
    assert str(delivery_id) in result["redriven"]
    assert get_delivery(delivery_id).status.value == "pending"


def test_redrive_in_kafka_mode_enqueues_through_the_outbox(client, dead_letter, monkeypatch):
    from sqlalchemy import select
    from app.db.models import OutboxMessage
    from app.db.session import SessionLocal

    producer, admin, _, delivery_id = dead_letter
    monkeypatch.setenv("DISPATCH_MODE", "kafka")
    client.post(f"/v1/dead-letters/{delivery_id}/redrive", headers=auth(admin))
    with SessionLocal.begin() as session:
        rows = session.scalars(select(OutboxMessage).where(OutboxMessage.aggregate_id == delivery_id)).all()
        assert len(rows) == 1
        session.delete(rows[0])


# --- circuit breaker ----------------------------------------------------------------------

@pytest.fixture()
def fast_circuit(monkeypatch):
    monkeypatch.setenv("CIRCUIT_FAILURE_THRESHOLD", "3")
    monkeypatch.setenv("CIRCUIT_COOLDOWN_SECONDS", "1")
    monkeypatch.setenv("CIRCUIT_MAX_COOLDOWN_SECONDS", "4")
    return f"test:{uuid.uuid4().hex[:8]}"


@needs_redis
def test_circuit_opens_after_threshold_and_half_opens_for_one_probe(fast_circuit):
    from app import circuit

    scope = fast_circuit
    for i in range(3):
        permit = circuit.before(scope)
        assert permit.allowed
        transition = circuit.after(scope, permit, healthy=False)
    assert transition == "opened"
    denied = circuit.before(scope)
    assert not denied.allowed and 0 < denied.retry_after_seconds <= 1

    time.sleep(1.05)
    probe = circuit.before(scope)
    assert probe.allowed and probe.probe
    assert not circuit.before(scope).allowed  # only one probe at a time
    assert circuit.after(scope, probe, healthy=True) == "closed"
    assert circuit.before(scope).allowed and not circuit.before(scope).probe


@needs_redis
def test_failed_probe_reopens_with_doubled_cooldown(fast_circuit):
    from app import circuit

    scope = fast_circuit
    for _ in range(3):
        circuit.after(scope, circuit.before(scope), healthy=False)
    time.sleep(1.05)
    probe = circuit.before(scope)
    assert circuit.after(scope, probe, healthy=False) == "opened"
    assert 1.0 < circuit.before(scope).retry_after_seconds <= 2.0


@needs_redis
def test_late_results_and_healthy_answers_do_not_move_the_circuit(fast_circuit):
    from app import circuit

    scope = fast_circuit
    in_flight = circuit.before(scope)  # permit taken before the circuit opened
    for _ in range(3):
        circuit.after(scope, circuit.before(scope), healthy=False)
    assert circuit.after(scope, in_flight, healthy=True) is None  # late success can't close it
    assert not circuit.before(scope).allowed

    other = f"{scope}-b"
    for _ in range(5):
        circuit.after(other, circuit.before(other), healthy=True)  # e.g. 4xx: the endpoint answered
    assert circuit.before(other).allowed


def test_circuit_fails_open_without_redis(monkeypatch):
    from app import circuit
    from app.cache import reset_cache

    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1/0")
    reset_cache()
    permit = circuit.before("anything")
    assert permit.allowed
    assert circuit.after("anything", permit, healthy=False) is None


@needs_redis
def test_open_circuit_defers_without_spending_attempts_and_isolates_tenants(client, tenant, fast_circuit):
    from app.testing import create_api_key, create_tenant_with_key, deliveries_for_event, get_attempts, get_delivery
    from app.db.models import ApiKeyRole

    producer, admin = tenant
    client.post("/v1/webhook-endpoints", json={"url": HOOK_URL}, headers=auth(admin))
    ids = [deliveries_for_event(producer.tenant_id, post_webhook_event(client, producer, f"cb-{i}"))[0].id for i in range(6)]

    seen: list = []
    dead = webhook_worker(httpx.ConnectError("refused"), seen)
    for delivery_id in ids:  # one at a time, so the order of failures is deterministic
        run_until_settled(dead, [delivery_id], rounds=1)
    assert len(seen) == 3  # threshold reached: the other three were never sent
    deferred = [get_delivery(i) for i in ids[3:]]
    assert all(d.status.value == "pending" and d.attempt_count == 0 for d in deferred)  # no attempt spent
    assert all(get_attempts(d.id) == [] for d in deferred)

    other = create_tenant_with_key(f"rel-iso-{uuid.uuid4().hex[:8]}")
    other_admin = create_api_key(other.tenant_id, ApiKeyRole.admin, "admin")
    client.post("/v1/webhook-endpoints", json={"url": HOOK_URL + "/other"}, headers=auth(other_admin))
    other_id = deliveries_for_event(other.tenant_id, post_webhook_event(client, other, "cb-other"))[0].id
    run_until_settled(webhook_worker(200), [other_id], rounds=1)
    assert get_delivery(other_id).status.value == "delivered"  # different endpoint, different circuit
