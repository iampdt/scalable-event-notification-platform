# Scalable Event Notification Platform

A multi-tenant notification service. A tenant's backend posts an event; the
platform durably records it, fans it out to recipients, and delivers it over
**email**, **signed webhooks**, and an **in-app feed**, with retries, crash
recovery, idempotency, and strict tenant isolation.

Built as a modular monolith on **FastAPI + PostgreSQL**. Postgres is both the
source of truth and the job queue (`FOR UPDATE SKIP LOCKED`). No Kafka or Redis
until a measurement says otherwise.

```mermaid
flowchart LR
  P[Tenant backend] -->|POST /v1/events<br/>API key + Idempotency-Key| API[FastAPI]
  API -->|one transaction:<br/>event + notifications + deliveries| DB[(PostgreSQL<br/>state + queue)]
  API -->|202 after commit| P
  W[Workers xN] -->|SKIP LOCKED claim + lease| DB
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
- **Webhook security:** HMAC over `timestamp.body`, Fernet-encrypted secrets,
  SSRF guard, no redirects.
- **Load-tested:** ingest p95 under 250 ms; about 1,700 deliveries/s with 4 workers on a
  laptop, after finding and fixing the real bottleneck
  ([benchmarks](docs/benchmarks.md)).

## Documentation

| Doc | What's in it |
| --- | --- |
| [docs/benchmarks.md](docs/benchmarks.md) | Load-test method, results, and the bottleneck investigation |
| [docs/architecture.md](docs/architecture.md) | Requirements, lifecycle, reliability model, roadmap |
| [docs/database.md](docs/database.md) | Schema, indexes, transactions, claim/completion SQL |
| [docs/api.md](docs/api.md) | HTTP contract and error model |
| [docs/build.md](docs/build.md) | Milestones and what's next |

## Quickstart

```sh
docker compose up -d                      # Postgres on :5433, Mailpit on :1025 (web UI :8025)
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

# 3. Send an event to all three channels
curl -s -X POST localhost:8000/v1/events -H "Authorization: Bearer $PRODUCER" \
  -H 'Idempotency-Key: order-123-paid' -H 'Content-Type: application/json' -d "{
    \"event_type\": \"order.paid\", \"subject\": {\"type\": \"order\", \"id\": \"ord_123\"},
    \"payload\": {\"amount\": 4999, \"currency\": \"INR\"},
    \"recipients\": [{\"user_id\": \"$USER_ID\", \"channels\": [\"email\", \"webhook\", \"in_app\"]}]}"

# 4. Watch it land
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

There are 61 tests. Provider and retry unit tests run without a database; the
integration tests (idempotency races, tenant isolation, leases, fencing, retries,
webhooks, feed) are skipped unless `TEST_DATABASE_URL` is set. CI also checks the
migration chain from scratch and that the models match it (`alembic check`).

## Load test

```sh
.venv/bin/python -m benchmarks.load_test --events 1000 --recipients 3 --workers 4 --worker-batch 50
```
