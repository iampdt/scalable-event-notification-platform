# Observability

How to see what the platform is doing: metrics (Prometheus), dashboards
(Grafana), alerts (Prometheus rules), and structured logs. Everything starts
with the Docker stack:

```sh
docker compose up -d --build
open http://localhost:3000      # Grafana: "Notification Platform" dashboard (anonymous viewer)
open http://localhost:9090      # Prometheus: targets, queries, /alerts
curl localhost:8000/metrics     # raw API metrics
```

## Metrics, logs, traces: what goes where

| Signal | Answers | Here |
| --- | --- | --- |
| **Metrics** | "How much, how fast, how often?" Aggregates that are cheap to store and alert on | Prometheus. Low-cardinality labels only |
| **Logs** | "What exactly happened to *this* request, tenant or delivery?" | JSON lines with `request_id`, `tenant_id`, `route`, `status`, `duration_ms` |
| **Traces** | "Where did the time go across services?" | Not implemented: the platform is one service plus workers, and `request_id` / `event_id` / `delivery_id` correlate logs well enough. OpenTelemetry is the next step if the services split. |

**The cardinality rule.** Every distinct combination of label values is a
separate time series in Prometheus memory. Labels are therefore limited to small
fixed sets: route *template*, method, status, channel, outcome, cache name,
partition (12). `tenant_id`, `event_id`, `delivery_id` and raw paths are **never**
labels. They go in logs. A test scans `/metrics` and fails if any label value
looks like a UUID, and unknown paths are labelled `unmatched` so a 404 scanner
can't create a series per URL.

## Metrics catalog

**API (RED: rate, errors, duration)**

| Metric | Type | Labels |
| --- | --- | --- |
| `http_requests_total` | counter | `method`, `route`, `status` |
| `http_request_duration_seconds` | histogram | `method`, `route` |
| `events_ingested_total` | counter | `result` = accepted / replayed / conflict / rejected |
| `deliveries_planned_total` | counter | `channel`, `status` = pending / suppressed |
| `rate_limited_total` | counter | — |
| `cache_requests_total` | counter | `cache` = credentials / templates, `result` = hit / miss / bypass |
| `redis_unavailable_total` | counter | — |

**Pipeline saturation** (read from PostgreSQL at scrape time, in the API process only)

| Metric | Meaning |
| --- | --- |
| `deliveries_queue_depth{state}` | `due`, `waiting_retry`, `processing` |
| `deliveries_oldest_due_seconds` | Age of the oldest due-but-unclaimed delivery: the "are workers keeping up?" number |
| `deliveries_dead_letters` | Current size of the dead-letter queue |
| `outbox_depth`, `outbox_failing_rows`, `outbox_oldest_due_seconds` | Kafka publishing health |
| `queue_metrics_up` | 0 if PostgreSQL couldn't be read (the scrape itself still succeeds) |

These are measured once, centrally. If every worker exported them, summing across
targets would multiply them.

**Workers**

| Metric | Type | Labels |
| --- | --- | --- |
| `delivery_attempts_total` | counter | `channel`, `outcome` |
| `provider_send_duration_seconds` | histogram | `channel` |
| `delivery_end_to_end_seconds` | histogram | `channel` (event accepted → delivered) |
| `deliveries_dead_lettered_total` | counter | `channel`, `reason` = exhausted / permanent / worker_lost |
| `deliveries_deferred_total` | counter | `channel` (circuit open) |
| `circuit_transitions_total` | counter | `channel`, `transition` = opened / closed |
| `worker_batch_size` | histogram | `mode` = poll / kafka |
| `leases_reclaimed_total`, `stale_completions_total` | counter | — |

**Kafka**

| Metric | Notes |
| --- | --- |
| `outbox_published_total`, `outbox_publish_failures_total` | From the relay |
| `kafka_messages_consumed_total{result}` | processed / poison |
| `kafka_consumer_lag{partition}` | Log-end minus **committed** offset, measured by the **relay**, outside the consumers. Self-reported lag vanishes when every consumer is dead, which is exactly when it matters. Verified: with the consumers stopped, lag rose to 1,800 and stayed visible. |

### Design details worth knowing

- **Series are pre-created at 0.** prometheus-client creates a labelled series
  on first use. A series that *appears* at 1 is invisible to `rate()` and
  `increase()`, so the first dead letter or circuit trip would never fire an
  alert. Every known label combination is initialised at import time.
- **Histogram buckets decide precision.** `histogram_quantile` interpolates
  linearly inside a bucket. With buckets jumping from 10 s to 30 s, end-to-end
  p95 read 28.3 s when the true (SQL) p95 was 11.0 s. With dense buckets through
  1–60 s it reads 19.33 s against a true 19.44 s.
- **Multiple API processes** (`uvicorn --workers N`) need
  `PROMETHEUS_MULTIPROC_DIR`, an empty directory at startup. Without it, a scrape
  reaches one process and reports only its share: measured, each of three scrapes
  of a 2-process API reported 100 of 200 events.
- **Worker processes** serve `/metrics` on `METRICS_PORT` (relay 9101, worker
  9102 in Compose). It's unset in load tests so several workers don't collide on
  a port. Prometheus finds scaled workers by DNS (`docker compose up --scale worker=3`).

## Dashboard

`observability/build_dashboard.py` generates
`observability/grafana/dashboards/notification-platform.json`: dashboards as
code, so edit the script, not the UI. The 23 panels are grouped in six rows:

- **API:** requests/s by status, 5xx ratio, `POST /v1/events` p50/p95/p99 (target p95 < 250 ms)
- **Ingestion:** events/s by result, deliveries planned/s, 429s/s
- **Delivery pipeline:** attempts/s by outcome, end-to-end p50/p95/p99, queue depth, oldest due age, provider p95 by channel, batch size
- **Reliability:** dead-lettered/s by reason, DLQ size, circuit transitions and deferrals, leases reclaimed and stale completions
- **Kafka:** outbox depth and failing rows, publish rate, consumer lag by partition, messages consumed
- **Redis:** hit ratio per cache, lookups by result, outages

Every panel query was checked against a live Prometheus after a load test: 31 of
31 return data. That check found two bugs, the empty-series problem and a 5xx
ratio that showed "No data" instead of 0.

## Alerts

`observability/alerts.yml`, validated with `promtool check rules`. Firing alerts
are visible at `localhost:9090/alerts` (there's no Alertmanager locally).

| Alert | Condition | Severity | First thing to check |
| --- | --- | --- | --- |
| `TargetDown` | a scrape target down 1 min | page | `docker compose ps` |
| `ApiErrorRateHigh` | > 5% 5xx over 5 min | page | `/readyz`, API logs (`level=ERROR`) |
| `IngestLatencyAboveTarget` | `POST /v1/events` p95 > 250 ms for 10 min | ticket | DB latency, host CPU |
| `DeliveryBacklogStale` | oldest due delivery > 60 s for 2 min | page | worker/consumer up? `kafka_consumer_lag`, provider latency |
| `DeadLettersGrowing` | > 10 dead letters in 15 min | ticket | `GET /v1/dead-letters` grouped by `last_error_code` |
| `CircuitOpened` | any circuit opened in 10 min | info | which endpoint; tenant notified? |
| `OutboxNotDraining` | oldest publishable row > 60 s, or failing rows, for 5 min | page | relay logs, Kafka health |
| `KafkaConsumerLagHigh` | total lag > 1,000 for 5 min | ticket | consumers running? scale them (≤ 12 partitions) |
| `RedisUnavailable` | Redis client breaker opened | info | expected degradation: no cache or limits |
| `QueueMetricsUnavailable` | `queue_metrics_up == 0` for 1 min | page | PostgreSQL |

## Logs

With `LOG_FORMAT=json` (set in Compose), every line is one JSON object:

```json
{"ts": "2026-10-20T19:30:05.132+00:00", "level": "INFO", "service": "api", "logger": "app.access",
 "message": "request", "request_id": "723cdb2a-…", "method": "POST", "route": "/v1/events",
 "status": 202, "duration_ms": 41.3, "tenant_id": "a4739cb0-…"}
```

- `request_id` comes from the client's `X-Request-Id` or is generated, is echoed
  in the response header and in error bodies, and is attached to every line logged
  while serving that request (a `contextvar`).
- One access line per request. Probes (`/healthz`, `/readyz`, `/metrics`) are
  left out of the log but still counted in metrics.
- Locally, `LOG_FORMAT=text` (the default) stays human-readable.

## A short runbook

1. **"Deliveries are late."** Start with `deliveries_oldest_due_seconds` and
   `deliveries_queue_depth`. A growing `due` count with flat attempts/s means
   workers are down or stuck (check `kafka_consumer_lag` and `TargetDown`). A
   high provider p95 for one channel means a slow provider; check circuit
   transitions.
2. **"A tenant says notifications failed."** Search the logs by `tenant_id`, then
   `GET /v1/dead-letters` with that tenant's admin key, then the attempt history.
   Redrive once the cause is fixed.
3. **"Ingestion is slow."** Look at `POST /v1/events` p95, then `/readyz`, then
   host CPU and the database. The scale experiments in [benchmarks.md](benchmarks.md)
   show what saturates first.

## Troubleshooting

**Grafana shows "Failed to load home dashboard" (HTTP 500) after switching git
branches.** The `observability/` directories are bind-mounted. If git deletes and
recreates them (checking out a branch without them, then coming back), the
running container keeps pointing at the old, deleted directory and sees it as
empty. Grafana stays up, but finds no dashboard. Recreate the containers so they
mount the current directory:

```sh
docker compose up -d --force-recreate grafana prometheus
```
