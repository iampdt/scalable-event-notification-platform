# Scalable Event Notification Platform

[![ci](https://github.com/iampdt/scalable-event-notification-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/iampdt/scalable-event-notification-platform/actions/workflows/ci.yml)

A multi-tenant notification service. A tenant's backend posts an event; the
platform durably records it, fans it out to recipients, and delivers it over
**email**, **signed webhooks** and an **in-app feed**, with idempotency, retries,
crash recovery, dead-letter replay, circuit breakers and strict tenant isolation.

It's built learning-first: every component had to earn its place, and every
performance claim was measured, including the ones that turned out wrong. The
story is in **[the engineering write-up](docs/engineering-writeup.md)**.

**Stack:** Python 3.12, FastAPI, PostgreSQL 16 (source of truth *and* job
queue), Redis (optional, fail-open), Kafka (optional dispatch through a
transactional outbox), Prometheus, Grafana, Docker Compose, GitHub Actions.

```mermaid
flowchart LR
  P[Tenant backend] -->|POST /v1/events<br/>API key + Idempotency-Key| API[FastAPI]
  API -->|one transaction:<br/>event + notifications + deliveries| DB[(PostgreSQL<br/>state + queue)]
  API -->|202 after commit| P
  API <-.->|cache + rate limit<br/>fail-open| R[(Redis)]
  DB -->|outbox rows| RL[Outbox relay]
  RL -->|publish| K[[Kafka topic]]
  K -->|consumer group| W[Workers xN]
  W -->|fenced claim + batch completion| DB
  W -->|SMTP| M[Email]
  W -->|HMAC-signed POST| H[Tenant webhook]
  W -->|mark delivered| F[In-app feed]
  U[Tenant app] -->|status, feed, dead letters| API
```

## Results at a glance

All numbers were measured on one 8-core laptop; method and caveats are in [benchmarks.md](docs/benchmarks.md).

| What | Result |
| --- | --- |
| Ingest latency (target p95 < 250 ms) | p95 130–240 ms; a 100-recipient event at p95 229 ms |
| Delivery throughput, 4 workers | 824 → **about 1,650/s** after finding that per-delivery commits contended on WAL writes |
| Redis cache | DB round trips per ingest **9 → 6**; throughput **+15%** |
| Kafka vs Postgres polling | Equal latency, **no throughput gain** (the ceiling is Postgres state writes), so the docs were corrected |
| Circuit breaker vs a dead endpoint | Healthy tenant's wait **41 s → 11 s**; requests into the dead endpoint 46 → 10 |
| Worker SIGKILLed mid-batch | **6,000/6,000 delivered**, in-flight batch re-sent after its lease |
| Kafka / Redis / PostgreSQL outage | Ingestion survives Kafka and Redis outages; Postgres outage → `503` + `Retry-After`, automatic recovery |
| API horizontal scaling | 1 → 4 processes: 138–155 → 213–229 events/s, until the host CPU saturated |

## What it does well

- **Idempotent ingestion:** a tenant-scoped `Idempotency-Key` plus a normalized
  request hash; a concurrent duplicate is resolved by a unique constraint (replay or `409`).
- **At-least-once delivery, effectively-once processing:** a stable delivery ID,
  leases with `attempt_count` as a **fencing token**, and duplicate messages
  absorbed by a fenced claim.
- **Failure handling:** retryable vs permanent classification, exponential backoff
  with jitter, `Retry-After`, a poison-message guard, a **dead-letter queue with
  audited replay**, a Kafka DLQ topic, and **per-endpoint circuit breakers** shared
  through Redis.
- **Multi-tenancy:** the tenant comes from the credential, composite foreign keys
  enforce tenant boundaries in the schema, cross-tenant reads return `404`, and
  ingestion is rate limited per tenant (GCRA in Redis Lua).
- **Webhook security:** HMAC over `timestamp.body`, Fernet-encrypted secrets
  shown once, an SSRF guard, no redirects.
- **Templates:** logic-free `{{ payload.field }}` (no SSTI surface), rendered at
  ingestion, so a missing field is a `422` up front.
- **Operable:** Prometheus metrics (RED plus pipeline saturation, consumer lag
  measured from outside the group), 10 alert rules, a Grafana dashboard as code,
  JSON logs keyed by `request_id`, `/healthz` vs `/readyz`.
- **Tested:** 137 tests against real PostgreSQL, Redis and Kafka in CI. CI fails if
  any test is skipped, and checks migrations from scratch, the downgrade path, and
  model drift.

## Documentation

| Doc | What's in it |
| --- | --- |
| [Engineering write-up](docs/engineering-writeup.md) | **Start here:** bottlenecks, experiments, wrong predictions, lessons |
| [Architecture](docs/architecture.md) | Requirements, lifecycle, reliability model, Redis/Kafka designs, roadmap |
| [Benchmarks](docs/benchmarks.md) | Every measurement, with method and the investigations behind it |
| [Failure experiments](docs/failure-experiments.md) | What fails, what users see, what's retried, how it recovers, for each failure mode |
| [Observability](docs/observability.md) | Metrics catalog, dashboard, alerts, logs, runbook |
| [Deployment](docs/deployment.md) | Production topology, release process, configuration, sizing, checklist |
| [API](docs/api.md) | HTTP contract, error model, dead-letter and admin endpoints |
| [Database](docs/database.md) | Schema, indexes, transactions, claim/completion/outbox SQL |
| [Build log](docs/build.md) | Milestones mapped to the learning plan |

## Quickstart

```sh
docker compose up -d --build     # Postgres, Redis, Kafka, Mailpit, migrations, topics, API :8000,
                                 # relay, consumer, Prometheus :9090, Grafana :3000
./scripts/demo.sh                # narrated end-to-end demo (about 1 minute)
```

The demo bootstraps a tenant, registers a template and a webhook (with a local
receiver that verifies signatures), sends an event to three channels, shows
idempotent replay and a `409`, follows delivery through the outbox and Kafka,
lands a `410` in the dead-letter queue, trips the rate limiter, and prints the
matching metrics. `vhs scripts/demo.tape` records it as a GIF.

Then open:
- **Grafana:** http://localhost:3000 (dashboard "Notification Platform")
- **Mailpit:** http://localhost:8025 (the delivered email)
- **API docs:** http://localhost:8000/docs

<details>
<summary>Run the API and worker from a virtualenv instead</summary>

```sh
docker compose up -d postgres redis mailpit      # add kafka for DISPATCH_MODE=kafka
python -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/alembic upgrade head
.venv/bin/python -m app.scripts.seed             # prints producer + admin keys once
.venv/bin/uvicorn app.main:app --reload          # API on :8000
.venv/bin/python -m app.workers.delivery_worker  # in another terminal (Postgres polling mode)
```

`.env.example` sets `WEBHOOK_ALLOW_PRIVATE_NETWORKS=true` so a local receiver
works. In any shared environment set it to `false` and generate a new
`WEBHOOK_SECRET_ENCRYPTION_KEY` (see [deployment.md](docs/deployment.md)).
</details>

## Tests

```sh
docker exec notification-postgres psql -U notification_app -d notifications -c 'create database notifications_test'
TEST_DATABASE_URL=postgresql+psycopg://notification_app:local_dev_password@localhost:5433/notifications_test \
  .venv/bin/pytest -q
```

Provider and retry unit tests need nothing. Integration tests (idempotency
races, tenant isolation, leases and fencing, retries, webhooks, Redis, Kafka,
outages, metrics) skip when a service is missing locally. In CI, a skip fails
the run.

## Benchmarks and experiments

```sh
python -m benchmarks.load_test --events 1000 --recipients 3 --workers 4 --worker-batch 50 --processes 4
python -m benchmarks.load_test --dispatch kafka ...      # API must run with DISPATCH_MODE=kafka
python -m benchmarks.query_count                         # DB round trips per request
python -m benchmarks.blackhole_experiment                # circuit breaker vs a dead endpoint
python -m app.scripts.stats                              # queue, Redis, outbox and lag health
```

## Project layout

```
app/
  api/            routes, auth (cached, rate limited), admin, feed, dead letters, pagination
  events/         idempotent ingestion + transactional fan-out + template rendering
  notifications/  retry policy, derived status, templates, dead-letter redrive
  providers/      SMTP, webhook (HMAC, SSRF guard), in-app, fake email
  workers/        polling worker, outbox relay, Kafka consumer
  cache.py  ratelimit.py  circuit.py  messaging.py  metrics.py  logging_config.py
alembic/          frozen-DDL migrations
benchmarks/       load test, query counter, black-hole experiment
observability/    Prometheus config, alert rules, Grafana provisioning, dashboard generator
scripts/          demo.sh, demo.tape
tests/            unit + integration (Postgres, Redis, Kafka)
```
