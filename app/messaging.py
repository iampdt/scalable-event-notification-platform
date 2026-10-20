"""Kafka topic design and client configuration.

Topic `notification.deliveries.v1`: one small message per delivery attempt that
is due, `{"delivery_id": ..., "tenant_id": ...}`. It is a *pointer* (claim
check): the consumer reads the authoritative row from PostgreSQL and claims it
there, so Kafka never holds state that could disagree with the database, and a
duplicate message is harmless (the claim finds the row no longer pending).

Key `<delivery_id>`: spreads load evenly across partitions, and a delivery's
duplicate messages land on the same partition. The first design keyed by
`<tenant_id>:<user_id>` for per-recipient ordering; the load test showed the
cost -- a 3-recipient workload used only 3 of 12 partitions, so at most 3
consumers worked -- and ordering is an explicit non-goal (retries break it
anyway). Keying by tenant alone would be worse: one big tenant, one partition.
"""
from __future__ import annotations

import json
import os
import uuid

from app.config import kafka_bootstrap_servers

DEFAULT_DELIVERY_TOPIC = "notification.deliveries.v1"
# Partitions cap consumer parallelism (one consumer per partition per group).
# 12 allows scaling to 12 consumers; increasing later reshuffles key -> partition.
DELIVERY_TOPIC_PARTITIONS = 12
DELIVERY_TOPIC_RETENTION_MS = 7 * 24 * 3600 * 1000  # a week of replay
DEFAULT_CONSUMER_GROUP = "notification-delivery-workers"


def delivery_topic() -> str:
    # Overridable so tests (and parallel environments) never share a topic.
    return os.getenv("KAFKA_DELIVERY_TOPIC", DEFAULT_DELIVERY_TOPIC)


def consumer_group() -> str:
    return os.getenv("KAFKA_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP)


def producer_config() -> dict:
    return {
        "bootstrap.servers": kafka_bootstrap_servers(),
        # acks=all + idempotence: a broker-side retry can never duplicate or
        # reorder a message within a partition. (End-to-end duplicates are still
        # possible -- relay crash after ack -- and handled by the consumer.)
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "compression.type": "lz4",
        # Must be below the relay's flush timeout: every message then gets a
        # definite success/failure callback within one flush, and nothing we have
        # given up on can still be sent later in the background.
        "message.timeout.ms": 10000,
    }


def consumer_config() -> dict:
    return {
        "bootstrap.servers": kafka_bootstrap_servers(),
        "group.id": consumer_group(),
        # Commit only after the batch is recorded in PostgreSQL: at-least-once.
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        # Faster failover than the 45 s default when a consumer dies.
        "session.timeout.ms": 10000,
        # Incremental rebalancing: a consumer joining or leaving moves only the
        # partitions that must move, instead of the default "eager" protocol that
        # stops every consumer while the whole group is reassigned. Measured: four
        # consumers joining eagerly idled the group for ~2 s.
        "partition.assignment.strategy": "cooperative-sticky",
    }


def delivery_message(delivery_id: uuid.UUID, tenant_id: uuid.UUID, available_at=None) -> dict:
    """An outbox row announcing that a delivery is (or will be) due."""
    row = {
        "topic": delivery_topic(),
        "message_key": str(delivery_id),
        "payload": {"delivery_id": str(delivery_id), "tenant_id": str(tenant_id)},
        "aggregate_id": delivery_id,
    }
    if available_at is not None:
        row["available_at"] = available_at
    return row


def parse_delivery_message(value: bytes) -> uuid.UUID:
    return uuid.UUID(json.loads(value)["delivery_id"])
