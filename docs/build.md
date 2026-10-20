# BUILD.md

## Goal
Build V1 of the multi-tenant notification platform: an event goes in via API,
gets durably stored, fanned out, and delivered asynchronously by a worker,
with retries and tenant isolation.

Design docs (reference only, do not re-read all of them each time):
docs/api.md, docs/database.md, docs/architecture.md

Load-test evidence: docs/benchmarks.md.

## Scope

### Build first (V1)
- Postgres via Docker Compose, Alembic migrations
- Tables: tenants, api_keys, users, events, notifications, deliveries, delivery_attempts
- Channels: in_app + fake email adapter (logs only)
- One producer API key, created by a seed script
- Endpoints: POST /v1/events, GET /v1/events/{id}, GET /v1/notifications/{id}
- Worker with SKIP LOCKED claiming, retries, lease reclaim

### V1.x (done)
- Webhook channel + HMAC signing, encrypted secrets, SSRF guard
- Admin endpoints: users upsert, webhook endpoints create/list/update (admin role)
- In-app feed + mark-as-read endpoint; notification list with status filter + cursors
- Real email provider: SMTP adapter (Mailpit locally, any SMTP relay in production)

### Not doing
- Row-level security, retention policy, Kafka (until plan step 3),
  Kubernetes, subscriptions/preferences

## Milestones

### M1: Schema and seed
Done when: `alembic upgrade head` works on an empty DB and the seed script
creates a tenant, an API key and 3 users.
- [Done] Docker Compose with Postgres
- [Done] Alembic set up
- [Done] Migration for the tables above
- [Done] Seed script
- [Done] Migrations rewritten as frozen DDL (they used `create_all()`, which broke fresh databases once a table was added)

### M2: Ingest endpoint
Done when: `curl POST /v1/events` returns 202 and psql shows 1 event,
N notifications, N x channels deliveries.
- [Done] API key auth (derive tenant_id)
- [Done] Request validation
- [Done] Single-transaction fan-out
- [Done] Return 202 only after commit

### M3: Idempotency + tenant isolation
Done when: tests pass for same key + same body, same key + changed body (409),
and tenant B reading tenant A's IDs (404).
- [Done] Tests written first
- [Done] Implementation
- [Done] Concurrent same-key race returns 409/replay instead of 500 (forced-interleaving test)

### M4: Worker
Done when: posting an event leads to `delivered` visible via GET.
- [Done] Claim query with SKIP LOCKED
- [Done] Fake provider
- [Done] Completion transaction
- [Done] Status endpoints

### M5: Failure handling
Done when: killing the worker mid-send results in attempt 2 of the SAME delivery,
and two workers never claim the same row.
- [Done] delivery_attempts written
- [Done] Backoff + retryable vs permanent errors
- [Done] Lease reclaim test
- [Done] Two-worker test
- [Done] Fencing test: a late completion from an expired lease is dropped

### M6: Real channels
Done when: an event with email + webhook + in_app is delivered end to end:
email visible in Mailpit, webhook signature verified by an independent receiver,
in-app item in the feed; a 503 from the receiver is retried and then succeeds.
- [Done] Migration: webhook enum value, webhook_endpoints, delivery FK + checks
- [Done] Webhook adapter: HMAC `v1=` signature over `timestamp.body`, no redirects
- [Done] SSRF guard at registration and send time (dev override flag)
- [Done] Webhook secrets encrypted with Fernet; shown once
- [Done] SMTP adapter + Mailpit in Compose; SMTP code classification
- [Done] Admin endpoints + admin role (403 for producer keys)
- [Done] In-app feed, mark read (idempotent), notification list + keyset cursors
- [Done] Reference webhook receiver (`app/scripts/webhook_receiver.py`)
- [Done] Verified end to end manually on 2026-10-20

### M7: Load test and metrics
Done when: a reproducible load test reports ingest p50/p95, delivery lag
p50/p95/p99, retry and failure rates, and the bottleneck is identified.
- [Done] `benchmarks/load_test.py` (self-contained tenant, spawns workers)
- [Done] `app/scripts/stats.py` (queue depth, oldest due, lag, retry/failure rate)
- [Done] Bottleneck found and fixed: split claim query, batched completion
- [Done] API-key hashing switched from scrypt to SHA-256 after measuring 35 ms/request
- [Done] Results in docs/benchmarks.md

### M8: README and diagram
- [Done] README with architecture diagram, quickstart, end-to-end demo
- [Done] CI: fresh migrate, downgrade/upgrade, `alembic check`, pytest against Postgres

### Plan alignment, step 1 (templates, Docker, auth tests)
Done when: a tenant can register templates and events render through them;
the API, worker, and migrations run with `docker compose up`; auth failures and
schema invariants have tests.
- [Done] `notification_templates` + `deliveries.content` migration
- [Done] Logic-free `{{ path }}` renderer; validation at save time; render at ingestion (422 on missing field)
- [Done] Admin template API (upsert with versioning, list, get)
- [Done] Dockerfile (one image, non-root) + Compose `migrate` / `api` / `worker` services
- [Done] Auth tests (401 cases) and SQLSTATE-precise constraint tests
- [Done] Verified end to end inside Docker on 2026-10-20 (rendered email in Mailpit, feed title/body, graceful worker stop)
- [Deliberately not done] `/auth/register` + `/auth/login`: callers are tenant backends, so the credential is an API key, not a user password session
- [Deliberately not done] `POST /notifications`: `POST /v1/events` already creates notifications; a second write path would duplicate idempotency and fan-out

### Plan alignment, step 2 (Redis + performance)
Done when: a measured baseline, at least one optimization, and a before/after
comparison exist; rate limiting enforces per-tenant fairness; Redis failure is
tested.
- [Done] Baseline recorded first: round trips per request (deterministic) and load-test latency
- [Done] Fail-open Redis client (50 ms timeouts, 5 s circuit breaker)
- [Done] Cache-aside: credential records (60 s TTL) and active templates (negative caching, invalidated after commit)
- [Done] Per-tenant GCRA rate limiting (atomic Lua, Redis clock), `429` + `Retry-After`, per-tenant overrides
- [Done] Explicit DB pool sizing (`DB_POOL_SIZE`, `DB_MAX_OVERFLOW`, `DB_POOL_TIMEOUT_SECONDS`)
- [Done] Before/after: ingest round trips 9 → 6, authenticated GET 3 → 1; ingest about 15% more throughput, p50 108 → 91–95 ms
- [Done] Limiter accuracy under load (203 accepted vs 209 allowed); chaos check with Redis stopped mid-traffic
- [Already done earlier] Cursor pagination, indexes justified by query plans

## Current focus
Next from the plan: step 3, Kafka via a transactional outbox (the Postgres queue measured about 1,600 deliveries/s against about 2,000/s for the worst-case fan-out).
Other candidate next steps, in priority order:
1. Per-cache hit/miss metrics (Redis's server-wide hit rate mixes cache and limiter keys)
2. Retention: time-partition `events`/`deliveries`/`delivery_attempts`
3. Pin resolved IP for webhook connections (DNS-rebinding gap)
4. Row-level security as defense in depth
5. Manual redrive endpoint for failed deliveries (keeps attempt history)

## Open questions
- Should webhook retries run longer than ~1.2 h for receivers that are down for a long time (with circuit breaking)?
- Allow arbitrary email recipients, or only tenant-managed users (current)?
- Which production email provider; do we need bounce/complaint webhooks?
- Retention period for payloads (they may contain personal data)?
