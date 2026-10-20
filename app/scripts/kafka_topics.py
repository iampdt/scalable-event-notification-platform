"""Create Kafka topics explicitly (auto-creation is disabled). Safe to re-run.

    python -m app.scripts.kafka_topics [--replication-factor 1]
"""
from __future__ import annotations

import argparse

from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

from app.config import kafka_bootstrap_servers
from app.messaging import DEAD_LETTER_RETENTION_MS, DELIVERY_TOPIC_PARTITIONS, DELIVERY_TOPIC_RETENTION_MS, dead_letter_topic, delivery_topic


def ensure_topics(replication_factor: int = 1) -> None:
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap_servers()})
    topics = [
        NewTopic(delivery_topic(), num_partitions=DELIVERY_TOPIC_PARTITIONS, replication_factor=replication_factor, config={"retention.ms": str(DELIVERY_TOPIC_RETENTION_MS), "cleanup.policy": "delete"}),
        # Low volume by definition; one partition keeps it in arrival order for humans.
        NewTopic(dead_letter_topic(), num_partitions=1, replication_factor=replication_factor, config={"retention.ms": str(DEAD_LETTER_RETENTION_MS), "cleanup.policy": "delete"}),
    ]
    for name, future in admin.create_topics(topics, request_timeout=15).items():
        try:
            future.result()
            print(f"created topic {name}")
        except Exception as exc:  # noqa: BLE001 - KafkaException wraps the error code
            if exc.args and getattr(exc.args[0], "code", lambda: None)() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"topic {name} already exists")
            else:
                raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    # Production: 3 replicas with min.insync.replicas=2, so acks=all survives one broker loss.
    parser.add_argument("--replication-factor", type=int, default=1)
    ensure_topics(parser.parse_args().replication_factor)


if __name__ == "__main__":
    main()
