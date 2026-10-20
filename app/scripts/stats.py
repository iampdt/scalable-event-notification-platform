"""Queue health and delivery measurements, straight from PostgreSQL.

    python -m app.scripts.stats [--minutes 60] [--tenant-slug demo]

These are the numbers that should justify (or not) adding Redis, Kafka, or more
workers: queue depth, oldest due age, delivery lag, retry and failure rates.
"""
from __future__ import annotations

import argparse
import json
import uuid

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import dispatch_mode
from app.db.session import SessionLocal

QUEUE_SQL = text(
    """
    SELECT
      count(*) FILTER (WHERE status = 'pending' AND next_attempt_at <= now()) AS due_now,
      count(*) FILTER (WHERE status = 'pending' AND next_attempt_at > now()) AS waiting_backoff,
      count(*) FILTER (WHERE status = 'processing' AND lease_expires_at >= now()) AS in_flight,
      count(*) FILTER (WHERE status = 'processing' AND lease_expires_at < now()) AS expired_leases,
      coalesce(extract(epoch FROM now() - min(next_attempt_at) FILTER (WHERE status = 'pending' AND next_attempt_at <= now())), 0) AS oldest_due_seconds
    FROM deliveries
    WHERE (CAST(:tenant_id AS uuid) IS NULL OR tenant_id = CAST(:tenant_id AS uuid))
    """
)

WINDOW_SQL = text(
    """
    SELECT channel::text AS channel,
           count(*) AS total,
           count(*) FILTER (WHERE status = 'delivered') AS delivered,
           count(*) FILTER (WHERE status = 'failed') AS failed,
           count(*) FILTER (WHERE status = 'suppressed') AS suppressed,
           count(*) FILTER (WHERE attempt_count > 1) AS retried,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM delivered_at - created_at)) AS lag_p50,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM delivered_at - created_at)) AS lag_p95,
           percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM delivered_at - created_at)) AS lag_p99
    FROM deliveries
    WHERE created_at >= now() - make_interval(mins => :minutes)
      AND (CAST(:tenant_id AS uuid) IS NULL OR tenant_id = CAST(:tenant_id AS uuid))
    GROUP BY channel
    ORDER BY channel
    """
)


def collect(session: Session, minutes: int = 60, tenant_id: uuid.UUID | None = None) -> dict:
    params = {"minutes": minutes, "tenant_id": str(tenant_id) if tenant_id else None}
    queue = dict(session.execute(QUEUE_SQL, params).mappings().one())
    channels = []
    for row in session.execute(WINDOW_SQL, params).mappings():
        row = dict(row)
        row["retry_rate"] = row["retried"] / row["total"] if row["total"] else 0.0
        row["terminal_failure_rate"] = row["failed"] / row["total"] if row["total"] else 0.0
        channels.append(row)
    return {"queue": queue, "window_minutes": minutes, "by_channel": channels}


DEAD_LETTERS_SQL = text(
    """
    SELECT channel::text AS channel, coalesce(last_error_code, 'unknown') AS error_code, count(*) AS failed
    FROM deliveries
    WHERE status = 'failed' AND failed_at >= now() - make_interval(mins => :minutes)
      AND (CAST(:tenant_id AS uuid) IS NULL OR tenant_id = CAST(:tenant_id AS uuid))
    GROUP BY 1, 2
    ORDER BY failed DESC
    LIMIT 20
    """
)

OUTBOX_SQL = text(
    """
    SELECT count(*) AS depth,
           count(*) FILTER (WHERE available_at <= now()) AS due,
           count(*) FILTER (WHERE publish_attempts > 0) AS failing,
           coalesce(extract(epoch FROM now() - min(available_at) FILTER (WHERE available_at <= now())), 0) AS oldest_due_seconds,
           max(last_error) FILTER (WHERE publish_attempts > 0) AS sample_error
    FROM outbox
    """
)


def kafka_stats() -> dict | None:
    try:
        from confluent_kafka import Consumer

        from app.messaging import consumer_config, delivery_topic, group_lag

        client = Consumer({**consumer_config(), "enable.auto.commit": False})
        lag = group_lag(client)
        client.close()
        return {"topic": delivery_topic(), "total_lag": sum(lag.values()), "lag_by_partition": {p: n for p, n in sorted(lag.items()) if n}}
    except Exception as exc:  # stats must never crash on a missing broker
        return {"error": str(exc)}


def redis_stats() -> dict | None:
    """Server-wide numbers. Note keyspace hit rate mixes cache lookups with
    rate-limiter keys (which mostly miss by design), so it understates the cache."""
    from app.cache import get_cache

    cache = get_cache()
    info = cache.call("info", lambda r: {**r.info("stats"), **r.info("memory")})
    if info is None:
        return None
    count = lambda pattern: sum(1 for _ in cache.client.scan_iter(match=pattern, count=1000))
    hits, misses = info["keyspace_hits"], info["keyspace_misses"]
    return {
        "keyspace_hits": hits,
        "keyspace_misses": misses,
        "keyspace_hit_rate": round(hits / (hits + misses), 4) if hits + misses else None,
        "used_memory_human": info["used_memory_human"],
        "cached_credentials": count("auth:*"),
        "cached_template_sets": count("templates:*"),
        "rate_limited_tenants_tracked": count("ratelimit:*"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Print delivery queue health")
    parser.add_argument("--minutes", type=int, default=60)
    parser.add_argument("--tenant-slug")
    args = parser.parse_args()
    with SessionLocal() as session:
        tenant_id = session.scalar(text("SELECT id FROM tenants WHERE slug = :slug"), {"slug": args.tenant_slug}) if args.tenant_slug else None
        report = {**collect(session, args.minutes, tenant_id), "redis": redis_stats()}
        # New dead letters by cause: the first thing to look at before a redrive.
        report["dead_letters"] = [dict(row) for row in session.execute(DEAD_LETTERS_SQL, {"minutes": args.minutes, "tenant_id": str(tenant_id) if tenant_id else None}).mappings()]
        if dispatch_mode() == "kafka":
            report["outbox"] = dict(session.execute(OUTBOX_SQL).mappings().one())
            report["kafka"] = kafka_stats()
        print(json.dumps(report, indent=2, default=float))


if __name__ == "__main__":
    main()
