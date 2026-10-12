# BUILD.md

## Goal
Build V1 of the multi-tenant notification platform: an event goes in via API,
gets durably stored, fanned out, and delivered asynchronously by a worker,
with retries and tenant isolation.

Design docs (reference only, do not re-read all of them each time):
docs/api.md, docs/database.md, docs/architecture.md

## Scope

### Build first (V1)
- Postgres via Docker Compose, Alembic migrations
- Tables: tenants, api_keys, users, events, notifications, deliveries, delivery_attempts
- Channels: in_app + fake email adapter (logs only)
- One producer API key, created by a seed script
- Endpoints: POST /v1/events, GET /v1/events/{id}, GET /v1/notifications/{id}
- Worker with SKIP LOCKED claiming, retries, lease reclaim

### Later (V1.x)
- Webhook channel + HMAC signing
- Admin endpoints (use seed script / SQL until then)
- In-app feed + mark-as-read endpoint
- Real email provider

### Not doing
- Row-level security, retention policy, rate limiting, Redis, Kafka,
  Kubernetes, subscriptions/preferences

## Milestones

### M1: Schema and seed
Done when: `alembic upgrade head` works on an empty DB and the seed script
creates a tenant, an API key and 3 users.
- [Done] Docker Compose with Postgres
- [ ] Alembic set up
- [ ] Migration for the tables above
- [ ] Seed script

### M2: Ingest endpoint
Done when: `curl POST /v1/events` returns 202 and psql shows 1 event,
N notifications, N x channels deliveries.
- [ ] API key auth (derive tenant_id)
- [ ] Request validation
- [ ] Single-transaction fan-out
- [ ] Return 202 only after commit

### M3: Idempotency + tenant isolation
Done when: tests pass for same key + same body, same key + changed body (409),
and tenant B reading tenant A's IDs (404).
- [ ] Tests written first
- [ ] Implementation

### M4: Worker
Done when: posting an event leads to `delivered` visible via GET.
- [ ] Claim query with SKIP LOCKED
- [ ] Fake provider
- [ ] Completion transaction
- [ ] Status endpoints

### M5: Failure handling
Done when: killing the worker mid-send results in attempt 2 of the SAME delivery,
and two workers never claim the same row.
- [ ] delivery_attempts written
- [ ] Backoff + retryable vs permanent errors
- [ ] Lease reclaim test
- [ ] Two-worker test

### M6: Real channels (later)
### M7: Load test and metrics (later)
### M8: README and diagram (later)

## Current focus
M1. Next task: ______

## Open questions
- (anything you're unsure about goes here, one line each)