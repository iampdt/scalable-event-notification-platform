# AGENTS.md

Working notes for anyone (or any coding agent) changing the notification platform. Start with
`docs/architecture.md`; failure behaviour is in `docs/failure-experiments.md` and performance
work in `docs/benchmarks.md`.

## What it is

Multi-tenant notification delivery (email, webhook, in-app): idempotent ingestion, a PostgreSQL
`SKIP LOCKED` job queue, retries with backoff, a Kafka outbox, circuit breakers and per-tenant
rate limits, with Prometheus and Grafana.

## Layout

| Path | Holds |
| --- | --- |
| `app/api`, `app/notifications` | Ingestion API and the domain |
| `app/workers`, `app/messaging.py` | Delivery workers, the outbox relay and the consumer |
| `app/providers`, `app/circuit.py`, `app/ratelimit.py` | Channel providers, circuit breaker, rate limiting |
| `alembic/` | Migrations |
| `tests/` | Unit tests (no services) and integration tests (Postgres) |
| `benchmarks/`, `observability/` | Load tests; Prometheus and Grafana setup |
| `scripts/demo.sh` | The narrated end-to-end demo |

## Commands

```sh
docker compose up -d --build     # Postgres, Redis, Kafka, Mailpit, API :8000, relay, consumer, Prometheus, Grafana
./scripts/demo.sh                # end-to-end demo (about a minute)
TEST_DATABASE_URL=postgresql+psycopg://notification_app:local_dev_password@localhost:5433/notifications_test \
  .venv/bin/pytest -q            # see the README's Tests section for creating the test database
```

GitHub Actions is not running for this account, so run the tests locally before pushing.

## Rules

- Ingestion is idempotent per tenant and key; a repeat returns the first result, a conflicting
  repeat is a 409.
- Delivery is at least once: every provider call must be safe to repeat, and webhooks are signed.
- Schema changes are new Alembic revisions that downgrade cleanly (`alembic check` must pass).
- Benchmarks and failure experiments are documented with how they were run; re-run before
  changing a published number.
