"""Inspect or replay the Kafka dead-letter topic.

    python -m app.scripts.kafka_dlq --list [--max 20]
    python -m app.scripts.kafka_dlq --replay      # after fixing the consumer bug that rejected them

Replay re-publishes the original key and value to the main topic. The consumer's
fenced claim makes a replay of an already-handled delivery a no-op.
"""
from __future__ import annotations

import argparse
import json
import uuid

from confluent_kafka import Consumer, Producer

from app.messaging import consumer_config, dead_letter_topic, delivery_topic, producer_config


def read(max_messages: int, group: str) -> list:
    consumer = Consumer({**consumer_config(), "group.id": group, "auto.offset.reset": "earliest"})
    consumer.subscribe([dead_letter_topic()])
    messages = []
    empty_polls = 0
    while len(messages) < max_messages and empty_polls < 5:
        batch = consumer.consume(num_messages=max_messages - len(messages), timeout=1.0)
        empty_polls = 0 if batch else empty_polls + 1
        messages += [m for m in batch if not m.error()]
    return messages, consumer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--list", action="store_true")
    action.add_argument("--replay", action="store_true")
    parser.add_argument("--max", type=int, default=20)
    args = parser.parse_args()

    if args.list:
        # A throwaway group: listing must not move the replay group's position.
        messages, consumer = read(args.max, f"dlq-inspect-{uuid.uuid4().hex[:8]}")
        for m in messages:
            headers = {k: v.decode() for k, v in (m.headers() or [])}
            print(json.dumps({"offset": m.offset(), "key": m.key().decode() if m.key() else None, "value": (m.value() or b"").decode(errors="replace"), **headers}))
        consumer.close()
        return
    messages, consumer = read(args.max, "dlq-replay")
    producer = Producer(producer_config())
    for m in messages:
        producer.produce(delivery_topic(), key=m.key(), value=m.value())
    if producer.flush(10):
        raise SystemExit("replay not acknowledged; nothing committed, safe to rerun")
    consumer.commit(asynchronous=False)
    consumer.close()
    print(f"replayed {len(messages)} message(s) to {delivery_topic()}")


if __name__ == "__main__":
    main()
