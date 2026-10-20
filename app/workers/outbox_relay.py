"""Outbox relay: publish due outbox rows to Kafka, then delete them.

    python -m app.workers.outbox_relay            # run forever
    python -m app.workers.outbox_relay --backfill # enqueue pending deliveries that have no outbox row

Ordering of operations is what makes this safe:
lock rows (SKIP LOCKED) -> produce -> wait for broker acks -> delete acked rows -> commit.
A crash after the acks but before the commit re-publishes those rows: Kafka gets
a duplicate, which the consumer's fenced claim ignores. Nothing is ever lost,
because a row is only deleted once Kafka has acknowledged it. Several relays can
run at once; SKIP LOCKED gives each a disjoint batch.

Each row gets its own verdict. A row that fails (unknown topic, broker down)
backs off with an attempt counter instead of failing the batch: an all-or-nothing
batch let one poison row block every row behind it forever.

The relay also runs the sweeper: a consumer that dies mid-send leaves its rows
`processing` with a lease. Kafka will redeliver the message, but the claim
skips non-pending rows, so the sweeper resets expired leases to pending and
enqueues them again (or fails them, if they have used every attempt).
"""
from __future__ import annotations

import argparse
import json
import logging
import time

from confluent_kafka import KafkaException, Producer
from sqlalchemy import insert, text

from app.db.models import OutboxMessage
from app.db.session import SessionLocal
from app.messaging import delivery_message, producer_config
from app.workers.delivery_worker import FAIL_ABANDONED_SQL, GracefulExit

logger = logging.getLogger(__name__)

FETCH_SQL = text(
    """
    SELECT id, topic, message_key, payload FROM outbox
    WHERE available_at <= now()
    ORDER BY available_at, id
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
    """
)
DELETE_SQL = text("DELETE FROM outbox WHERE id = ANY(:ids)")
BACKOFF_SQL = text(
    """
    UPDATE outbox
    SET publish_attempts = publish_attempts + 1,
        last_error = :error,
        available_at = now() + make_interval(secs => least(power(2, publish_attempts + 1), 300))
    WHERE id = :id
    """
)

REQUEUE_EXPIRED_SQL = text(
    """
    WITH expired AS (
        SELECT id FROM deliveries
        WHERE status = 'processing' AND lease_expires_at < now() AND attempt_count < max_attempts
        ORDER BY lease_expires_at, id
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE deliveries d
    SET status = 'pending', lease_expires_at = NULL, next_attempt_at = now(), updated_at = now()
    FROM expired
    WHERE d.id = expired.id
    RETURNING d.id, d.tenant_id
    """
)

BACKFILL_SQL = text(
    """
    SELECT d.id, d.tenant_id, d.next_attempt_at
    FROM deliveries d
    WHERE d.status = 'pending' AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.aggregate_id = d.id)
    """
)


class OutboxRelay:
    def __init__(self, producer: Producer | None = None, batch_size: int = 500, flush_timeout: float = 15.0):
        self.producer = producer or Producer(producer_config())
        self.batch_size = batch_size
        self.flush_timeout = flush_timeout

    def publish_batch(self) -> tuple[int, int]:
        """Publish up to batch_size due rows. Returns (published, failed)."""
        with SessionLocal.begin() as session:
            rows = session.execute(FETCH_SQL, {"limit": self.batch_size}).all()
            if not rows:
                return 0, 0
            acked: set[int] = set()
            errors: dict[int, str] = {}

            def on_delivery_for(row_id: int):
                def on_delivery(err, msg) -> None:
                    if err is None:
                        acked.add(row_id)
                    else:
                        errors[row_id] = str(err)
                return on_delivery

            for row in rows:
                while True:
                    try:
                        self.producer.produce(row.topic, key=row.message_key, value=json.dumps(row.payload, separators=(",", ":")), on_delivery=on_delivery_for(row.id))
                        break
                    except BufferError:  # local queue full: let it drain, then retry
                        self.producer.poll(0.5)
                    except KafkaException as exc:  # rejected locally, e.g. invalid topic name
                        errors[row.id] = str(exc)
                        break
            self.producer.flush(self.flush_timeout)
            if acked:
                session.execute(DELETE_SQL, {"ids": list(acked)})
            failed = [row for row in rows if row.id not in acked]
            for row in failed:
                session.execute(BACKOFF_SQL, {"id": row.id, "error": errors.get(row.id, "no delivery report before flush timeout")[:500]})
            if failed:
                logger.error("publish_failed count=%s first_error=%s", len(failed), errors.get(failed[0].id))
            return len(acked), len(failed)

    def sweep(self, limit: int = 500) -> tuple[int, int]:
        """Fail poison deliveries and re-enqueue ones whose consumer died mid-send."""
        with SessionLocal.begin() as session:
            abandoned = session.execute(FAIL_ABANDONED_SQL, {"limit": limit}).all()
            if abandoned:
                logger.error("abandoned_deliveries_failed count=%s ids=%s", len(abandoned), [str(row.id) for row in abandoned])
            expired = session.execute(REQUEUE_EXPIRED_SQL, {"limit": limit}).all()
            if expired:
                session.execute(insert(OutboxMessage), [delivery_message(row.id, row.tenant_id) for row in expired])
                logger.warning("requeued_expired_leases count=%s", len(expired))
            return len(abandoned), len(expired)

    def backfill(self) -> int:
        """One-off, e.g. after switching DISPATCH_MODE to kafka: enqueue pending rows with no outbox row."""
        with SessionLocal.begin() as session:
            rows = session.execute(BACKFILL_SQL).all()
            if rows:
                session.execute(insert(OutboxMessage), [delivery_message(row.id, row.tenant_id, available_at=row.next_attempt_at) for row in rows])
            return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish the transactional outbox to Kafka")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--poll-seconds", type=float, default=0.1)
    parser.add_argument("--sweep-seconds", type=float, default=10.0)
    parser.add_argument("--backfill", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    relay = OutboxRelay(batch_size=args.batch_size)
    if args.backfill:
        logger.info("backfilled count=%s", relay.backfill())
        return
    stop = GracefulExit()
    next_sweep = 0.0
    logger.info("relay_started batch_size=%s", args.batch_size)
    while not stop.requested:
        published, failed = relay.publish_batch()
        if failed and not published:
            time.sleep(1)  # likely the broker is down: don't spin
        if time.monotonic() >= next_sweep:
            relay.sweep()
            next_sweep = time.monotonic() + args.sweep_seconds
        if not published:
            time.sleep(args.poll_seconds)
    relay.producer.flush(10)
    logger.info("relay_stopped")


if __name__ == "__main__":
    main()
