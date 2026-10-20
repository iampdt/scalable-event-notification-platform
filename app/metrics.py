"""Prometheus metrics: one catalog for the API and all workers.

Rules this module follows:
- **Low-cardinality labels only.** Route *templates* (/v1/events/{event_id}),
  channel, outcome, cache name. Never tenant_id, delivery_id, or raw paths:
  every distinct label value is a separate time series, so an id label grows
  Prometheus memory without bound. Per-tenant and per-delivery detail belongs in
  logs, which are built for high cardinality.
- **RED for the API** (rate, errors, duration), **USE-style saturation** for the
  pipeline (queue depth, oldest due age, outbox depth, consumer lag).
- **Shared state is measured once.** Queue/outbox/dead-letter gauges come from
  PostgreSQL at scrape time in the API process only (QueueCollector). If every
  worker exported them, a sum across targets would double count.
- Each worker process exposes its own counters on METRICS_PORT; Prometheus sums
  across processes. Under `uvicorn --workers N`, set PROMETHEUS_MULTIPROC_DIR so
  the N API processes share one view (the classic multiprocess gotcha).
"""
from __future__ import annotations

import logging
import os

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server
from prometheus_client.core import GaugeMetricFamily

logger = logging.getLogger(__name__)

# --- API (RED) ---------------------------------------------------------------------
HTTP_REQUESTS = Counter("http_requests_total", "HTTP requests by route template and status.", ["method", "route", "status"])
HTTP_DURATION = Histogram(
    "http_request_duration_seconds", "HTTP request latency by route template.", ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
EVENTS_INGESTED = Counter("events_ingested_total", "POST /v1/events outcomes.", ["result"])  # accepted, replayed, conflict, rejected
DELIVERIES_PLANNED = Counter("deliveries_planned_total", "Deliveries created at ingestion.", ["channel", "status"])  # pending | suppressed
RATE_LIMITED = Counter("rate_limited_total", "Ingestion requests rejected by the per-tenant limiter.")
CACHE_REQUESTS = Counter("cache_requests_total", "Cache lookups by cache and result (the per-cache hit rate Redis INFO can't give).", ["cache", "result"])
REDIS_UNAVAILABLE = Counter("redis_unavailable_total", "Times the Redis client opened its breaker.")

# --- Workers -------------------------------------------------------------------------
DELIVERY_ATTEMPTS = Counter("delivery_attempts_total", "Provider attempts by channel and outcome.", ["channel", "outcome"])
DELIVERIES_DEAD_LETTERED = Counter("deliveries_dead_lettered_total", "Deliveries that reached failed (the dead-letter queue).", ["channel", "reason"])
DELIVERIES_DEFERRED = Counter("deliveries_deferred_total", "Deliveries deferred by an open circuit (no attempt spent).", ["channel"])
CIRCUIT_TRANSITIONS = Counter("circuit_transitions_total", "Circuit breaker transitions.", ["channel", "transition"])
PROVIDER_DURATION = Histogram(
    "provider_send_duration_seconds", "Time spent in the provider call.", ["channel"],
    buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15),
)
# histogram_quantile interpolates linearly *inside* a bucket, so precision only
# exists where buckets are dense. The first version jumped 10 s -> 30 s and
# reported p95 = 28 s for a true (SQL) p95 of 11 s. Dense through 1-60 s, where
# first attempts and short backlogs land; coarse beyond, where retries live.
END_TO_END = Histogram(
    "delivery_end_to_end_seconds", "Event accepted -> delivery succeeded.", ["channel"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30, 45, 60, 120, 300, 900, 3600),
)
BATCH_SIZE = Histogram("worker_batch_size", "Deliveries per claimed batch.", ["mode"], buckets=(1, 2, 5, 10, 20, 50, 100))
STALE_COMPLETIONS = Counter("stale_completions_total", "Late results dropped by the attempt_count fence.")
LEASES_RECLAIMED = Counter("leases_reclaimed_total", "Expired leases reclaimed or re-queued.")

# --- Kafka ---------------------------------------------------------------------------
OUTBOX_PUBLISHED = Counter("outbox_published_total", "Outbox rows acknowledged by Kafka.")
OUTBOX_PUBLISH_FAILURES = Counter("outbox_publish_failures_total", "Outbox rows that failed to publish (they back off).")
KAFKA_MESSAGES = Counter("kafka_messages_consumed_total", "Messages consumed by result.", ["result"])  # processed, poison
KAFKA_CONSUMER_LAG = Gauge("kafka_consumer_lag", "Consumer-group lag per partition (log-end minus committed), measured by the relay from outside the consumers.", ["partition"], multiprocess_mode="max")


class QueueCollector:
    """Pipeline saturation read from PostgreSQL at scrape time (API process only)."""

    QUERY = """
        SELECT
          (SELECT count(*) FROM deliveries WHERE status = 'pending' AND next_attempt_at <= now()) AS due,
          (SELECT count(*) FROM deliveries WHERE status = 'pending' AND next_attempt_at > now()) AS waiting_retry,
          (SELECT count(*) FROM deliveries WHERE status = 'processing') AS processing,
          (SELECT coalesce(extract(epoch FROM now() - min(next_attempt_at)), 0) FROM deliveries WHERE status = 'pending' AND next_attempt_at <= now()) AS oldest_due_seconds,
          (SELECT count(*) FROM deliveries WHERE status = 'failed') AS dead_letters,
          (SELECT count(*) FROM outbox) AS outbox_depth,
          (SELECT count(*) FROM outbox WHERE publish_attempts > 0) AS outbox_failing,
          (SELECT coalesce(extract(epoch FROM now() - min(available_at)), 0) FROM outbox WHERE available_at <= now()) AS outbox_oldest_due_seconds
    """

    def describe(self):
        return []  # skip the probe query at registration time

    def collect(self):
        from sqlalchemy import text

        from app.db.session import engine

        up = GaugeMetricFamily("queue_metrics_up", "1 if the queue gauges could be read from PostgreSQL.")
        try:
            with engine.connect() as connection:
                row = connection.execute(text(self.QUERY)).mappings().one()
        except Exception as exc:  # noqa: BLE001 - a scrape must never raise
            logger.warning("queue_metrics_unavailable error=%s", type(exc).__name__)
            up.add_metric([], 0)
            yield up
            return
        up.add_metric([], 1)
        yield up
        depth = GaugeMetricFamily("deliveries_queue_depth", "Deliveries by queue state.", labels=["state"])
        for state in ("due", "waiting_retry", "processing"):
            depth.add_metric([state], float(row[state]))
        yield depth
        yield GaugeMetricFamily("deliveries_oldest_due_seconds", "Age of the oldest delivery that is due but unclaimed.", value=float(row["oldest_due_seconds"]))
        yield GaugeMetricFamily("deliveries_dead_letters", "Deliveries currently in the dead-letter queue (failed).", value=float(row["dead_letters"]))
        yield GaugeMetricFamily("outbox_depth", "Outbox rows waiting to be published.", value=float(row["outbox_depth"]))
        yield GaugeMetricFamily("outbox_failing_rows", "Outbox rows with at least one failed publish.", value=float(row["outbox_failing"]))
        yield GaugeMetricFamily("outbox_oldest_due_seconds", "Age of the oldest publishable outbox row.", value=float(row["outbox_oldest_due_seconds"]))


def api_registry() -> CollectorRegistry | None:
    """Registry for the API's /metrics. None = the default registry (single process)."""
    if not os.getenv("PROMETHEUS_MULTIPROC_DIR"):
        return None
    from prometheus_client import multiprocess

    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return registry


def start_worker_metrics_server() -> None:
    """Workers serve /metrics on METRICS_PORT if set (unset in load tests: no port clashes)."""
    port = os.getenv("METRICS_PORT")
    if port:
        start_http_server(int(port))
        logger.info("metrics_server_started port=%s", port)


def _initialize_series() -> None:
    """Create every known label combination at 0 at import time.

    prometheus_client only creates a labelled series on first use. A series that
    *appears* at 1 is invisible to rate()/increase(), so the first dead letter or
    circuit trip would never fire its alert, and the dashboard shows "No data"
    instead of 0. All label sets here are small and fixed.
    """
    channels = ("email", "webhook", "in_app")
    for channel in channels:
        for outcome in ("success", "retryable_failure", "permanent_failure", "provider_error"):
            DELIVERY_ATTEMPTS.labels(channel, outcome)
        for reason in ("exhausted", "permanent", "worker_lost"):
            DELIVERIES_DEAD_LETTERED.labels(channel, reason)
        DELIVERIES_DEFERRED.labels(channel)
        for status in ("pending", "suppressed"):
            DELIVERIES_PLANNED.labels(channel, status)
    for channel in ("email", "webhook"):
        for transition in ("opened", "closed"):
            CIRCUIT_TRANSITIONS.labels(channel, transition)
    for result in ("accepted", "replayed", "conflict", "rejected"):
        EVENTS_INGESTED.labels(result)
    for result in ("processed", "poison"):
        KAFKA_MESSAGES.labels(result)
    for cache in ("credentials", "templates"):
        for result in ("hit", "miss", "bypass"):
            CACHE_REQUESTS.labels(cache, result)


_initialize_series()
