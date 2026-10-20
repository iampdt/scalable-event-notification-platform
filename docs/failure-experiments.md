# Failure Experiments

The system was deliberately broken, one dependency or failure mode at a time,
on the local Docker stack (PostgreSQL 16, Redis 7.4, Kafka 3.9 KRaft, API,
outbox relay, Kafka consumer) between 2026-10-20 and 2026-10-21. For each
failure, the table answers five questions:

1. What fails?
2. What does the producer see?
3. What gets retried?
4. Where does the message go?
5. How does the system recover?

Every row has either a live experiment (below) or an automated test, and
usually both.

## Summary

| Failure | What fails | Producer sees | Retried | Message goes | Recovery |
| --- | --- | --- | --- | --- | --- |
| **PostgreSQL down** | Ingestion, status reads, all delivery state changes | `503 service_unavailable`, `Retry-After: 5` (same `Idempotency-Key` on retry) | The producer's request; worker loops back off 1 → 2 → 4 … 30 s | Nowhere new; nothing is accepted that isn't durable | Automatic. `/readyz` goes back to 200, workers log `dependency_recovered` |
| **Redis down** | Cache, rate limiting, circuit breakers | Nothing: `202`s continue | — | Normal path (cache misses read Postgres) | Automatic. The Redis client's own breaker retries after 5 s |
| **Kafka down** (Kafka mode) | Publishing and consuming | Nothing: `202`s continue | Outbox rows back off per row | Waits in the `outbox` table | Automatic. The relay drains the outbox when the broker returns |
| **Worker SIGKILLed mid-batch** | The in-flight batch | Nothing | The in-flight deliveries (at-least-once: may be sent twice) | Uncommitted Kafka offsets are redelivered; claimed rows are re-queued by the lease sweeper | Automatic, after the lease (60 s) plus a sweep (≤ 10 s) |
| **Duplicate event** (producer retry) | — | `202` with the *original* event id; `409` if the body changed | — | No new rows | — |
| **Duplicate Kafka message** | — | — | — | The fenced claim (`status = 'pending'`) makes it a no-op | — |
| **Provider timeout / dead endpoint** | Deliveries to that endpoint | Nothing (async) | Backoff 30 s → 2 min → 10 min → 1 h; the circuit opens after 5 failures and defers *without spending attempts* | Stays `pending`; `failed` (dead-letter queue) after max attempts | When the circuit's half-open probe succeeds |
| **Provider 5xx / 429** | Same | Same | Same; `Retry-After` honoured | Same | Same |
| **Provider permanent 4xx** | That one delivery | Status shows `failed` with the code | Not retried | Dead-letter queue (`failed`), with attempt history | Manual, audited **redrive** after the cause is fixed |
| **Malformed Kafka message** | That message only | — | Never (can't succeed) | Kafka DLQ topic `<topic>.dlq`, with provenance headers | Inspect or replay with `app.scripts.kafka_dlq` |
| **Delivery that crashes workers** (poison) | Each worker that picks it up | — | Until `max_attempts` | `failed`, `last_error_code = 'worker_lost'` | Investigate, then redrive |
| **Relay crash after Kafka ack** | — | — | The batch is republished | Duplicates in Kafka, absorbed by the fenced claim | Automatic |
| **Outbox row for a missing topic** | That row only | — | Per-row backoff, up to 5 min | Stays in the outbox (`publish_attempts`, `last_error`) | Fix the topic; the row then publishes |

## Experiments

### PostgreSQL unavailable: before and after the fix

`docker compose stop postgres` with the stack running, then `POST /v1/events`.

| | Before | After |
| --- | --- | --- |
| Producer response | `500` plain text `Internal Server Error` (broke the documented error contract) | `503` JSON envelope, `service_unavailable`, `Retry-After: 5`, `request_id` |
| `/healthz` | 200 (no readiness signal at all) | 200, deliberately: liveness must not restart healthy processes |
| `/readyz` | didn't exist | `503 {"postgres": "unavailable: OperationalError", "redis": "ok"}` |
| Outbox relay | crash-looped (`Restarting (1)`), relying on Docker restarts | stayed up; backed off 2 s → 4 s → 8 s |
| Kafka consumer | survived (rewinds the failed batch) | unchanged |
| After `docker compose start postgres` | recovered after restarts | `dependency_recovered after 4 failures`; the producer's retry with the same key got `202` |

### Redis unavailable

From the Redis step: 20 events with Redis up, then `docker compose stop redis`, 20
more, then a restart and 20 more. All **60/60 were accepted**, and latency didn't
rise while Redis was down (average 22 → 13 → 11 ms). The API logged one
`redis_unavailable … bypassing for 5.0s` line rather than one per request, and
every delivery completed. Rate limits aren't enforced while Redis is down
(fail open, by design).

### Kafka unavailable

From the Kafka step: 10 events with Kafka up, then `docker compose stop kafka` and
10 more. All **20/20 were accepted**. The second batch's 20 deliveries waited in
the outbox, the relay logged one `publish_failed … Message timed out` and backed
off, and the consumer's partitions were revoked. After the restart the
partitions were reassigned, the outbox drained, and the result was
`delivered=40 pending=0`.

### Worker crash (SIGKILL) mid-batch

The test sent 1,000 events × 3 recipients × 2 channels (6,000 deliveries)
through Kafka. Four seconds in, the consumer container got `docker kill
--signal SIGKILL`, a crash with no graceful shutdown. It was restarted 20 s
later.

| Observation | Value |
| --- | --- |
| Deliveries in flight (`processing`) when killed | 51 |
| Final state | **6,000 / 6,000 delivered**, none lost |
| Deliveries that needed a second attempt | 50 (the in-flight batch was re-sent, so at-least-once) |
| Re-queued by the lease sweeper | 51 (`requeued_expired_leases count=50`, then `count=1`) |
| Recovery latency for the in-flight work | about 70 s = 60 s lease + sweep cycle |

**Trade-off made visible:** the lease length *is* the crash-recovery latency for
in-flight work. A shorter lease recovers faster but risks re-sending to slow,
live workers. The worker refuses to start with a lease shorter than twice its
worst-case batch time.

### Provider timeout: a black-hole endpoint, with and without the circuit breaker

`python -m benchmarks.blackhole_experiment`. A "black hole" accepts TCP
connections and never answers, so every request waits out the full 10 s
webhook timeout.
- Tenant A sends 40 events to its webhook, which points at the black hole.
- Tenant B then sends 40 in-app events. In-app is healthy.
- One polling worker runs, with batch 10 and concurrency 10.
- Each configuration ran twice.

| | Breaker off | **Breaker on** (threshold 5) |
| --- | --- | --- |
| Tenant B waited (delivery lag) | 41.0–41.4 s | **10.9–11.0 s** |
| Requests sent into the black hole | 46 | **10** |
| Tenant A deliveries deferred without spending an attempt | 0 | 30 |

Without the breaker, the healthy tenant waited behind four batches of 10 s
timeouts, and 6 retries had already started by the time B finished. With it, the
first batch's timeouts opened the circuit, A's other 30 deliveries were deferred
in milliseconds, and B was delivered right after.

**A methodology note.** The first breaker-on result was 21 s, with 12 requests.
The worker log showed the circuit opening at about 20 s, and a *different*
endpoint's circuit closing. Leftover pending rows from the previous run sat at
the head of the queue, so the first batch held only 2 new requests to the black
hole (too few to trip the breaker). The script now retires leftover rows and
refuses to run unless the queue is empty. Both reported runs are clean.

### Malformed Kafka message

```sh
echo '{this is not json' | kafka-console-producer.sh --topic notification.deliveries.v1
```

The consumer logged `poison_message … partition=3 offset=10263`, and the message
was parked in the DLQ topic with its full provenance:

```json
{"value": "{this is not json",
 "dlq.error": "JSONDecodeError: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
 "dlq.source.topic": "notification.deliveries.v1", "dlq.source.partition": "3",
 "dlq.source.offset": "10263", "dlq.failed_at": "2026-10-20T19:13:59.849961+00:00"}
```

Consumer lag afterwards was **0**: the partition wasn't blocked.

## Bugs these experiments and their tests found

| Bug | Found by | Fix |
| --- | --- | --- |
| Database outage returned a plain-text 500 | Stopping Postgres | `503` + `Retry-After` handler; a catch-all `500` keeps the envelope |
| No readiness signal | Stopping Postgres | `/readyz` (Postgres required; Redis reported) |
| Relay crash-looped on a DB outage | Stopping Postgres | In-process exponential backoff in the relay and polling worker |
| `consume_once(dlq=None)` logged a poison message and committed past it, so it was lost | DLQ test | Never commit a poison message without a DLQ producer; seek back instead |
| Experiment contaminated by leftover rows | Worker log during the breaker experiment | Retire leftovers; require an empty queue |

## Automated coverage

`tests/test_reliability.py`, `tests/test_kafka.py`, `tests/test_redis.py`,
`tests/test_worker.py` and `tests/test_idempotency_and_isolation.py` cover every
row of the summary table. Examples:

- `test_database_outage_returns_503_envelope_with_retry_after`
- `test_open_circuit_defers_without_spending_attempts_and_isolates_tenants`
- `test_redrive_reopens_with_fresh_budget_audit_and_history`
- `test_malformed_message_goes_to_the_dead_letter_topic_with_provenance`
- `test_late_completion_from_expired_lease_is_fenced_off`
- `test_delivery_that_keeps_killing_workers_is_failed_not_reclaimed_forever`
