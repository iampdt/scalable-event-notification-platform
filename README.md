# Scalable Event Notification Platform

A multi-tenant notification service. A tenant's backend posts an event; the
platform durably records it, fans it out to recipients, and delivers it over
**email**, **signed webhooks**, and an **in-app feed**, with retries, crash
recovery, idempotency, and strict tenant isolation.

Built as a modular monolith on **FastAPI + PostgreSQL + Redis**. Postgres is both
the source of truth and the job queue (`FOR UPDATE SKIP LOCKED`). Redis is an
optional accelerator for caching and per-tenant rate limiting: the platform keeps
working if it's down. **Kafka** is an alternative dispatch path
(`DISPATCH_MODE=kafka`): a transactional outbox, a relay, and a consumer group.
Ingestion keeps working even while Kafka is down.

```mermaid
flowchart LR
  P[Tenant backend] -->|POST /v1/events<br/>API key + Idempotency-Key| API[FastAPI]
  API -->|one transaction:<br/>event + notifications + deliveries| DB[(PostgreSQL<br/>state + queue)]
  API -->|202 after commit| P
  API <-.->|cache + rate limit<br/>fail-open| R[(Redis)]
  DB -->|outbox rows| RL[Outbox relay]
  RL -->|publish| K[[Kafka topic<br/>notification.deliveries.v1]]
  K -->|consumer group| W[Workers xN]
  W -->|fenced claim in Postgres| DB
  W -->|SMTP| M[Email / Mailpit]
  W -->|HMAC-signed POST| H[Tenant webhook]
  W -->|mark delivered| F[In-app feed]
  W -->|fenced batch completion| DB
  U[Tenant app] -->|status, feed, mark read| API
```

## Highlights

- **Idempotent ingestion.** A tenant-scoped `Idempotency-Key` plus a normalized
  request hash; concurrent duplicates are resolved by a unique constraint.
- **At-least-once delivery** with a stable delivery ID for receiver deduplication.
- **Crash recovery** through leases, with `attempt_count` as a fencing token
  against stale workers.
- **Retry policy:** retryable vs permanent classification, exponential backoff
  with jitter, `Retry-After`, 5 attempts.
- **Dead-letter queue with audited replay** (admin API), plus a Kafka DLQ topic
  for malformed messages.
- **Circuit breakers** per webhook endpoint: a dead endpoint defers its
  deliveries instead of tying up workers. In the black-hole experiment, a
  healthy tenant's wait fell from 41 s to 11 s.
- **Per-tenant rate limiting:** GCRA (a token-bucket equivalent) in an atomic
  Redis Lua script, returning `429` + `Retry-After`; it held to within 1% of the
  configured limit under load.
- **Redis cache-aside:** credentials and templates are cached, cutting database
  round trips per ingest from 9 to 6 and per authenticated read from 3 to 1.
  A Redis outage degrades to "no cache, no limit", not to errors.
- **Kafka with a transactional outbox:** no dual write. Messages are pointers
  claimed in Postgres, so duplicates are harmless. Measured honestly: Kafka
  matched Postgres polling on latency and did *not* add throughput, because the
  ceiling is database state writes. Its value here is push instead of polling,
  replay, buffering, and room for more consumers.
- **Templates:** tenant-authored, logic-free `{{ payload.field }}` templates for
  email and in-app, rendered at ingestion, so a missing field is a `422` for the
  producer instead of a broken email later.
- **Webhook security:** HMAC over `timestamp.body`, Fernet-encrypted secrets,
  SSRF guard, no redirects.
- **Load-tested:** ingest p95 under 250 ms; about 1,700 deliveries/s with 4 workers on a
  laptop, after finding and fixing the real bottleneck
  ([benchmarks](docs/benchmarks.md)).

## Documentation

| Doc | What's in it |
| --- | --- |
| [docs/benchmarks.md](docs/benchmarks.md) | Load-test method, results, and the bottleneck investigation |
| [docs/failure-experiments.md](docs/failure-experiments.md) | What happens when each dependency or component fails, tested live |
| [docs/architecture.md](docs/architecture.md) | Requirements, lifecycle, reliability model, roadmap |
| [docs/database.md](docs/database.md) | Schema, indexes, transactions, claim/completion SQL |
| [docs/api.md](docs/api.md) | HTTP contract and error model |
| [docs/build.md](docs/build.md) | Milestones and what's next |

## Quickstart

**Everything in Docker:**

```sh
docker compose up -d --build              # Postgres, Redis, Kafka, Mailpit, migrations, topics, API :8000, relay, consumer
docker compose run --rm api python -m app.scripts.seed   # prints producer + admin keys ONCE
docker compose logs -f worker
```

The `migrate` service applies migrations and exits; the API and worker only start
after it succeeds. In this mode, a webhook receiver running on your machine is
reachable from the worker at `http://host.docker.internal:9123/...`, not `localhost`.

**Or run the API and worker from a virtualenv** (infrastructure only in Docker):

```sh
docker compose up -d postgres redis mailpit   # Postgres :5433, Redis :6379, Mailpit :1025 (web UI :8025); add `kafka` for Kafka mode
python -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/alembic upgrade head
.venv/bin/python -m app.scripts.seed       # prints producer + admin keys ONCE
.venv/bin/uvicorn app.main:app --reload    # API on :8000, docs at /docs
.venv/bin/python -m app.workers.delivery_worker   # in another terminal
```

## End-to-end demo

```sh
PRODUCER=ntf_live_...   # from the seed output
ADMIN=ntf_live_...
USER_ID=$(docker exec notification-postgres psql -U notification_app -d notifications -tAc \
  "select u.id from users u join tenants t on t.id=u.tenant_id where t.slug='demo' and external_id='ada'")

# 1. Register a webhook endpoint (the secret is returned once)
curl -s -X POST localhost:8000/v1/webhook-endpoints -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -d '{"url":"http://localhost:9123/hooks"}'

# 2. Run the reference receiver; it verifies signatures, and fails the first request to show a retry
.venv/bin/python -m app.scripts.webhook_receiver --secret whsec_... --fail-first 1

# 3. (Optional) Template the email; without one, the email body is the raw payload
curl -s -X POST localhost:8000/v1/templates -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -d '{"event_type":"order.paid","channel":"email",
    "subject":"Order {{ subject.id }} is paid","body":"Hi {{ recipient.external_id }}, we received {{ payload.amount }} {{ payload.currency }}."}'

# 4. Send an event to all three channels
curl -s -X POST localhost:8000/v1/events -H "Authorization: Bearer $PRODUCER" \
  -H 'Idempotency-Key: order-123-paid' -H 'Content-Type: application/json' -d "{
    \"event_type\": \"order.paid\", \"subject\": {\"type\": \"order\", \"id\": \"ord_123\"},
    \"payload\": {\"amount\": 4999, \"currency\": \"INR\"},
    \"recipients\": [{\"user_id\": \"$USER_ID\", \"channels\": [\"email\", \"webhook\", \"in_app\"]}]}"

# 5. Watch it land
curl -s localhost:8000/v1/events/<event_id> -H "Authorization: Bearer $PRODUCER"
curl -s localhost:8000/v1/users/$USER_ID/in-app-notifications -H "Authorization: Bearer $PRODUCER"
open http://localhost:8025                 # the email in Mailpit
.venv/bin/python -m app.scripts.stats      # queue depth, lag percentiles, retry rate
```

The webhook's first attempt gets a 503 and is retried after about 30 s. The
attempt history is in `delivery_attempts`.

`.env.example` sets `WEBHOOK_ALLOW_PRIVATE_NETWORKS=true` so a localhost
receiver works. In any shared environment, set it to `false` and generate a new
`WEBHOOK_SECRET_ENCRYPTION_KEY`.

## Tests

```sh
docker exec notification-postgres psql -U notification_app -d notifications -c 'create database notifications_test'
TEST_DATABASE_URL=postgresql+psycopg://notification_app:local_dev_password@localhost:5433/notifications_test \
  .venv/bin/pytest -q
```

There are 128 tests. Provider and retry unit tests run without a database; the
integration tests (idempotency races, tenant isolation, leases, fencing, retries,
webhooks, feed) are skipped unless `TEST_DATABASE_URL` is set. CI also checks the
migration chain from scratch and that the models match it (`alembic check`).

## Load test

```sh
.venv/bin/python -m benchmarks.load_test --events 1000 --recipients 3 --workers 4 --worker-batch 50
```
