"""Kafka delivery consumer (DISPATCH_MODE=kafka). Run N of these; the consumer
group spreads the topic's partitions across them.

    python -m app.workers.kafka_consumer [--batch-size 50 --concurrency 50]

Per batch: consume -> claim in PostgreSQL (fenced; duplicates drop out) -> send
concurrently -> record outcomes in one transaction -> commit offsets.

Offsets are committed only after PostgreSQL has the results, so a crash replays
the batch (at-least-once) instead of losing it. If processing fails (e.g. the
database is down) we seek back to the batch's first offsets: the consumer's
in-memory position has already moved past these messages, and without the seek
the next poll would silently skip them.
"""
from __future__ import annotations

import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

from app.messaging import consumer_config, dead_letter_topic, delivery_topic, parse_delivery_message, producer_config
from app.workers.delivery_worker import DeliveryWorker, GracefulExit

logger = logging.getLogger(__name__)


def rewind(consumer: Consumer, messages: list) -> None:
    first: dict[tuple[str, int], int] = {}
    for message in messages:
        key = (message.topic(), message.partition())
        first[key] = min(first.get(key, message.offset()), message.offset())
    for (topic, partition), offset in first.items():
        consumer.seek(TopicPartition(topic, partition, offset))


def dead_letter(producer: Producer, message, error: str) -> None:
    """Copy an unprocessable message to the DLQ topic, with where it came from and why."""
    producer.produce(
        dead_letter_topic(),
        key=message.key(),
        value=message.value(),
        headers={
            "dlq.error": error,
            "dlq.source.topic": message.topic(),
            "dlq.source.partition": str(message.partition()),
            "dlq.source.offset": str(message.offset()),
            "dlq.failed_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def consume_once(consumer: Consumer, worker: DeliveryWorker, batch_size: int, executor: ThreadPoolExecutor | None = None, timeout: float = 1.0, dlq: Producer | None = None) -> int:
    """One batch. Returns messages handled; raises nothing for per-message problems."""
    # Two-phase fetch: block for the first message, then take whatever else is
    # already buffered without waiting. consume(n, timeout) alone waits up to
    # `timeout` to *fill* the batch, which added ~1 s of latency at light load.
    messages = consumer.consume(num_messages=1, timeout=timeout)
    if not messages:
        return 0
    if batch_size > 1:
        messages += consumer.consume(num_messages=batch_size - 1, timeout=0)
    data, ids, poison = [], [], []
    for message in messages:
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                logger.error("consume_error %s", message.error())
            continue
        data.append(message)
        try:
            ids.append(parse_delivery_message(message.value()))
        except (ValueError, KeyError, TypeError) as exc:
            # A malformed message can never succeed. Retrying it would block the
            # partition forever, so it goes to the dead-letter topic instead.
            logger.error("poison_message topic=%s partition=%s offset=%s", message.topic(), message.partition(), message.offset())
            poison.append((message, f"{type(exc).__name__}: {exc}"[:300]))
    if not data:
        return 0
    try:
        worker.process_ids(ids, executor)
        if poison:
            if dlq is None:
                # Never commit past a poison message without parking it somewhere.
                raise RuntimeError("poison message but no dead-letter producer configured")
            for message, error in poison:
                dead_letter(dlq, message, error)
            if dlq.flush(10):
                # The DLQ copy isn't acknowledged: don't commit, or the poison
                # message would be lost rather than parked.
                raise RuntimeError("dead-letter publish not acknowledged")
    except Exception:
        logger.exception("batch_failed size=%s; rewinding for redelivery", len(data))
        rewind(consumer, data)
        time.sleep(1)
        return 0
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        # Usually a rebalance took our partitions mid-batch. The new owner will
        # replay these messages; the fenced claim makes that a no-op.
        logger.warning("commit_failed %s", exc)
    return len(data)


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume delivery messages from Kafka")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--lease-seconds", type=int, default=60)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    worker = DeliveryWorker(lease_seconds=args.lease_seconds, concurrency=args.concurrency)
    worker.check_lease_budget(args.batch_size)
    consumer = Consumer(consumer_config())
    dlq = Producer(producer_config())
    consumer.subscribe(
        [delivery_topic()],
        on_assign=lambda c, partitions: logger.info("partitions_assigned %s", [p.partition for p in partitions]),
        on_revoke=lambda c, partitions: logger.info("partitions_revoked %s", [p.partition for p in partitions]),
    )
    stop = GracefulExit()
    batches = handled = 0
    report_at = time.monotonic() + 10
    logger.info("consumer_started topic=%s batch_size=%s concurrency=%s", delivery_topic(), args.batch_size, args.concurrency)
    try:
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            while not stop.requested:
                count = consume_once(consumer, worker, args.batch_size, executor, dlq=dlq)
                if count:
                    batches, handled = batches + 1, handled + count
                if time.monotonic() >= report_at:
                    if batches:
                        logger.info("throughput messages=%s batches=%s avg_batch=%.1f per_second=%.0f", handled, batches, handled / batches, handled / 10)
                    batches = handled = 0
                    report_at = time.monotonic() + 10
    finally:
        # close() leaves the group cleanly, so partitions move to the remaining
        # consumers immediately instead of after session.timeout.ms.
        consumer.close()
        logger.info("consumer_stopped")


if __name__ == "__main__":
    main()
