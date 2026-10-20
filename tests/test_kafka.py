"""Transactional outbox and Kafka dispatch.

Outbox tests need TEST_DATABASE_URL. Kafka tests also need a broker
(TEST_KAFKA_BOOTSTRAP_SERVERS, default localhost:9092); they use a throwaway
topic and a fresh consumer group per test, so they never touch dev traffic.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
BOOTSTRAP = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
pytestmark = pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is required")


def kafka_reachable() -> bool:
    try:
        from confluent_kafka.admin import AdminClient

        AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=3)
        return True
    except Exception:
        return False


needs_kafka = pytest.mark.skipif(not kafka_reachable(), reason="needs a Kafka broker")


@pytest.fixture(scope="module")
def topic():
    """A throwaway topic for this test module, deleted afterwards."""
    from confluent_kafka.admin import AdminClient

    name = f"test.deliveries.{uuid.uuid4().hex[:8]}"
    previous = {key: os.environ.get(key) for key in ("KAFKA_DELIVERY_TOPIC", "KAFKA_BOOTSTRAP_SERVERS")}
    os.environ["KAFKA_DELIVERY_TOPIC"] = name
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = BOOTSTRAP
    if kafka_reachable():
        from app.scripts.kafka_topics import ensure_topics

        ensure_topics()
    yield name
    if kafka_reachable():
        # delete_topics is async: keep the client alive and wait for the result.
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        for future in admin.delete_topics([name]).values():
            future.result(timeout=15)
    # Outbox rows addressed to the deleted topic would otherwise linger in the test DB.
    from sqlalchemy import delete
    from app.db.models import OutboxMessage
    from app.db.session import SessionLocal

    with SessionLocal.begin() as session:
        session.execute(delete(OutboxMessage).where(OutboxMessage.topic == name))
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@pytest.fixture()
def kafka_mode(monkeypatch, topic):
    monkeypatch.setenv("DISPATCH_MODE", "kafka")
    monkeypatch.setenv("KAFKA_CONSUMER_GROUP", f"test-group-{uuid.uuid4().hex[:8]}")
    return topic


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    from fastapi.testclient import TestClient
    from app.main import create_app

    with TestClient(create_app()) as test_client:
        yield test_client


@pytest.fixture()
def tenant():
    from app.testing import create_tenant_with_key

    return create_tenant_with_key(f"kafka-{uuid.uuid4().hex[:10]}")


def post(client, tenant, idem: str, channels=("email", "in_app")) -> dict:
    body = {"event_type": "kafka.test", "subject": {"type": "t", "id": idem}, "payload": {"x": 1}, "recipients": [{"user_id": str(tenant.user_id), "channels": list(channels)}]}
    return client.post("/v1/events", json=body, headers={"Authorization": f"Bearer {tenant.key}", "Idempotency-Key": idem})


def outbox_rows(delivery_ids) -> list:
    from sqlalchemy import select
    from app.db.models import OutboxMessage
    from app.db.session import SessionLocal

    with SessionLocal() as session:
        return list(session.scalars(select(OutboxMessage).where(OutboxMessage.aggregate_id.in_(list(delivery_ids)))))


def drain_outbox(relay) -> None:
    while relay.publish_batch()[0]:
        pass


def consume_until(predicate, consumer, worker, deadline_seconds: float = 30.0) -> None:
    from app.workers.kafka_consumer import consume_once

    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        consume_once(consumer, worker, batch_size=100, timeout=0.5)
        if predicate():
            return
    raise AssertionError("condition not met before deadline")


def new_consumer():
    from confluent_kafka import Consumer
    from app.messaging import consumer_config, delivery_topic

    consumer = Consumer(consumer_config())
    consumer.subscribe([delivery_topic()])
    return consumer


# --- outbox (database only) ------------------------------------------------------

def test_kafka_mode_writes_one_outbox_row_per_pending_delivery(client, tenant, kafka_mode):
    from app.testing import deliveries_for_event

    created = post(client, tenant, "ob-1", channels=("email", "in_app", "webhook"))
    assert created.status_code == 202
    deliveries = {d.channel.value: d for d in deliveries_for_event(tenant.tenant_id, created.json()["id"])}
    assert deliveries["webhook"].status.value == "suppressed"  # no endpoint: nothing to dispatch
    rows = outbox_rows(d.id for d in deliveries.values())
    assert {row.aggregate_id for row in rows} == {deliveries["email"].id, deliveries["in_app"].id}
    assert all(row.topic == kafka_mode and row.message_key == str(row.aggregate_id) for row in rows)
    assert {row.payload["delivery_id"] for row in rows} == {str(deliveries["email"].id), str(deliveries["in_app"].id)}


def test_postgres_mode_writes_no_outbox_rows(client, tenant, monkeypatch):
    from app.testing import deliveries_for_event

    monkeypatch.setenv("DISPATCH_MODE", "postgres")
    created = post(client, tenant, "ob-pg")
    assert outbox_rows(d.id for d in deliveries_for_event(tenant.tenant_id, created.json()["id"])) == []


def test_rejected_event_writes_nothing_to_the_outbox(client, tenant, kafka_mode):
    from sqlalchemy import func, select
    from app.db.models import OutboxMessage
    from app.db.session import SessionLocal

    with SessionLocal() as session:
        before = session.scalar(select(func.count()).select_from(OutboxMessage))
    body = {"event_type": "kafka.test", "subject": {"type": "t", "id": "x"}, "payload": {}, "recipients": [{"user_id": str(uuid.uuid4()), "channels": ["in_app"]}]}
    assert client.post("/v1/events", json=body, headers={"Authorization": f"Bearer {tenant.key}", "Idempotency-Key": "ob-bad"}).status_code == 422
    with SessionLocal() as session:
        assert session.scalar(select(func.count()).select_from(OutboxMessage)) == before


def test_retry_schedules_an_outbox_row_at_next_attempt_time(kafka_mode):
    from app.notifications.retry import ProviderFailure
    from app.testing import create_delivery, get_delivery
    from app.workers.delivery_worker import DeliveryWorker

    delivery_id = create_delivery(channel="email")
    worker = DeliveryWorker()
    claimed = worker.claim_ids([delivery_id])
    assert len(claimed) == 1
    worker.complete_failure(claimed[0], ProviderFailure("503", retryable=True, status_code=503))
    rows = outbox_rows([delivery_id])
    assert len(rows) == 1
    assert rows[0].available_at == get_delivery(delivery_id).next_attempt_at  # same transaction, same now()


def test_claim_ids_ignores_duplicates_and_rows_not_due(kafka_mode):
    from app.testing import create_delivery
    from app.workers.delivery_worker import DeliveryWorker

    delivery_id = create_delivery(channel="in_app")
    worker = DeliveryWorker()
    assert [c.id for c in worker.claim_ids([delivery_id, delivery_id])] == [delivery_id]
    assert worker.claim_ids([delivery_id]) == []  # already processing: a duplicate message is a no-op


# --- Kafka -------------------------------------------------------------------------

@needs_kafka
def test_outbox_to_kafka_to_delivered_end_to_end(client, tenant, kafka_mode):
    from app.testing import deliveries_for_event, get_attempts
    from app.workers.delivery_worker import DeliveryWorker
    from app.workers.outbox_relay import OutboxRelay

    created = post(client, tenant, "e2e-1").json()
    deliveries = deliveries_for_event(tenant.tenant_id, created["id"])
    drain_outbox(OutboxRelay())
    assert outbox_rows(d.id for d in deliveries) == []  # deleted only after broker ack

    consumer = new_consumer()
    try:
        consume_until(lambda: {d.status.value for d in deliveries_for_event(tenant.tenant_id, created["id"])} == {"delivered"}, consumer, DeliveryWorker())
    finally:
        consumer.close()
    assert all(len(get_attempts(d.id)) == 1 for d in deliveries)


@needs_kafka
def test_duplicate_kafka_messages_deliver_once(tenant, kafka_mode):
    from confluent_kafka import Producer
    from app.messaging import delivery_topic, producer_config
    from app.testing import create_delivery, get_attempts, get_delivery
    from app.workers.delivery_worker import DeliveryWorker

    delivery_id = create_delivery(channel="in_app")
    producer = Producer(producer_config())
    value = f'{{"delivery_id": "{delivery_id}", "tenant_id": "x"}}'
    for _ in range(3):  # e.g. relay crashed after the broker ack, before deleting the rows
        producer.produce(delivery_topic(), key="dup", value=value)
    producer.flush(10)

    consumer = new_consumer()
    try:
        consume_until(lambda: get_delivery(delivery_id).status.value == "delivered", consumer, DeliveryWorker())
        consume_until(lambda: True, consumer, DeliveryWorker(), deadline_seconds=2)  # drain the remaining copies
    finally:
        consumer.close()
    assert [a.attempt_number for a in get_attempts(delivery_id)] == [1]


def release_backoff(delivery_ids) -> None:
    from sqlalchemy import func, update
    from app.db.models import OutboxMessage
    from app.db.session import SessionLocal

    with SessionLocal.begin() as session:
        session.execute(update(OutboxMessage).where(OutboxMessage.aggregate_id.in_(list(delivery_ids))).values(available_at=func.now()))


@needs_kafka
def test_relay_keeps_and_backs_off_rows_when_kafka_is_unreachable(client, tenant, kafka_mode):
    from confluent_kafka import Producer
    from app.testing import deliveries_for_event
    from app.workers.outbox_relay import OutboxRelay

    created = post(client, tenant, "down-1", channels=("in_app",)).json()
    delivery_ids = [d.id for d in deliveries_for_event(tenant.tenant_id, created["id"])]
    dead = OutboxRelay(producer=Producer({"bootstrap.servers": "127.0.0.1:1", "message.timeout.ms": 1500}), flush_timeout=4)
    published, failed = dead.publish_batch()
    assert published == 0 and failed >= 1
    [row] = outbox_rows(delivery_ids)  # kept: nothing lost
    assert row.publish_attempts == 1 and row.last_error
    release_backoff(delivery_ids)
    drain_outbox(OutboxRelay())  # broker reachable again: it goes out
    assert outbox_rows(delivery_ids) == []


@needs_kafka
def test_one_unpublishable_row_does_not_block_the_others(client, tenant, kafka_mode):
    """Regression: an all-or-nothing batch let one row for a missing topic block everything."""
    from sqlalchemy import insert
    from app.db.models import OutboxMessage
    from app.db.session import SessionLocal
    from app.testing import deliveries_for_event
    from app.workers.outbox_relay import OutboxRelay

    poison_id = uuid.uuid4()
    with SessionLocal.begin() as session:
        session.execute(insert(OutboxMessage).values(topic="no.such.topic", message_key="k", payload={"delivery_id": str(poison_id)}, aggregate_id=poison_id))
    created = post(client, tenant, "hol-1", channels=("in_app",)).json()
    good_ids = [d.id for d in deliveries_for_event(tenant.tenant_id, created["id"])]

    from confluent_kafka import Producer
    from app.messaging import producer_config

    # Short message timeout so the missing topic is reported quickly.
    relay = OutboxRelay(producer=Producer({**producer_config(), "message.timeout.ms": 2000}), flush_timeout=5)
    published, failed = relay.publish_batch()
    assert published >= 1 and failed >= 1
    assert outbox_rows(good_ids) == []
    [poison] = outbox_rows([poison_id])
    # librdkafka reports a missing topic as a message timeout (it waits for metadata).
    assert poison.publish_attempts == 1 and poison.last_error
    with SessionLocal.begin() as session:
        session.delete(session.get(OutboxMessage, poison.id))


@needs_kafka
def test_consumer_rewinds_and_redelivers_when_processing_fails(client, tenant, kafka_mode):
    from app.testing import deliveries_for_event
    from app.workers.delivery_worker import DeliveryWorker
    from app.workers.kafka_consumer import consume_once
    from app.workers.outbox_relay import OutboxRelay

    created = post(client, tenant, "rewind-1", channels=("in_app",)).json()
    delivery_id = deliveries_for_event(tenant.tenant_id, created["id"])[0].id
    drain_outbox(OutboxRelay())

    class DatabaseDown(DeliveryWorker):
        calls = 0

        def process_ids(self, ids, executor=None):
            DatabaseDown.calls += 1
            if delivery_id in ids and DatabaseDown.calls == 1:
                raise RuntimeError("database unavailable")
            return super().process_ids(ids, executor)

    consumer = new_consumer()
    worker = DatabaseDown()
    try:
        deadline = time.monotonic() + 30
        while DatabaseDown.calls == 0 and time.monotonic() < deadline:
            consume_once(consumer, worker, batch_size=100, timeout=0.5)
        consume_until(lambda: deliveries_for_event(tenant.tenant_id, created["id"])[0].status.value == "delivered", consumer, worker)
    finally:
        consumer.close()
    assert DatabaseDown.calls >= 2  # failed once, the same message came back


def test_sweeper_requeues_deliveries_whose_consumer_died(kafka_mode):
    from app.testing import create_delivery, expire_lease, get_delivery
    from app.workers.delivery_worker import DeliveryWorker
    from app.workers.outbox_relay import OutboxRelay

    delivery_id = create_delivery(channel="email")
    DeliveryWorker().claim_ids([delivery_id])  # consumer claims... then dies
    expire_lease(delivery_id)
    OutboxRelay(producer=object()).sweep()  # sweep never touches the producer
    assert get_delivery(delivery_id).status.value == "pending"
    assert len(outbox_rows([delivery_id])) == 1


def test_backfill_enqueues_pending_deliveries_that_have_no_outbox_row(kafka_mode):
    """For switching DISPATCH_MODE from postgres to kafka with work already queued."""
    from app.testing import create_delivery
    from app.workers.outbox_relay import OutboxRelay

    delivery_id = create_delivery(channel="in_app")  # created without an outbox row
    relay = OutboxRelay(producer=object())
    assert relay.backfill() >= 1
    assert len(outbox_rows([delivery_id])) == 1
    relay.backfill()  # idempotent: no second row
    assert len(outbox_rows([delivery_id])) == 1
