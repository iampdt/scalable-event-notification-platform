# Benchmarks (M7)

Numbers measured with `benchmarks/load_test.py` on 2026-10-20, and the
investigation they triggered. These are **laptop numbers**: they show
*relative* effects and *where the bottleneck is*, not production capacity.

## Setup

| Item | Value |
| --- | --- |
| Machine | MacBook, 8 CPUs; API, load generator, workers, and Docker all on one host |
| PostgreSQL | 16 in Docker Desktop (VM: 8 CPUs, 4 GB), default config |
| API | One `uvicorn` process (no `--workers`), sync endpoints on the default threadpool |
| Workers | Spawned by the script; fake email adapter + in-app (measures the platform, not a mail server) |
| Shape | 1,000 events × 3 recipients × 2 channels (email, in_app) = **6,000 deliveries** unless noted |

```sh
# API running on :8000, then:
python -m benchmarks.load_test --events 1000 --concurrency 20 --recipients 3 --workers 4 --worker-batch 50
python -m benchmarks.load_test --events 1000 --concurrency 4 --workers 2 --worker-batch 50 --start-workers-first  # steady state
python -m app.scripts.stats --minutes 60   # queue health at any time
```

**Backlog mode** (the default) ingests everything first, then starts workers and
times the drain. That measures raw delivery throughput.
**`--start-workers-first`** runs workers during ingestion and measures realistic lag.

## Results summary (final code)

| Measurement | Result | Target (architecture.md) |
| --- | --- | --- |
| Ingest throughput, 3 recipients × 2 channels, concurrency 20 | 145–193 events/s | 10 sustained, 50 burst ✅ |
| Ingest latency, same | p50 ≈ 100–118 ms, **p95 131–233 ms** | p95 < 250 ms ✅ |
| Ingest, **100 recipients** × 2 channels (200 deliveries/event), concurrency 5 | 25.8 events/s, **p95 229 ms**, p99 292 ms | p95 < 250 ms ✅ |
| Delivery drain, 1 worker | 400–535 deliveries/s | — |
| Delivery drain, 4 workers (batch 50 or 10) | **1,617–1,719 deliveries/s** | — |
| Steady state, 2 workers, 144 events/s (≈ 860 deliveries/s) | lag **p50 0.40 s, p95 0.52 s, p99 0.55 s** | first attempt < 30 s ✅ |
| Worst-case fan-out: 40,000 deliveries, 4 workers | ≈ 1,460 deliveries/s overall | 10 ev/s × 200 = 2,000/s ❌ |

**Honest gap.** The worst case in the requirements (10 events/s *sustained*,
every one fanning out to 100 recipients × 2–3 channels) needs 2,000–3,000
deliveries/s. Four laptop workers do about 1,500–1,700/s. The next levers, in
order: more worker processes or hosts (claims scale horizontally), a bigger
database host, and bulk `UPDATE … FROM (VALUES …)` completion.

## The investigation (why the worker looks the way it does)

| Step | Change | 1 worker | 4 workers | Conclusion |
| --- | --- | --- | --- | --- |
| 0 | Baseline: single `OR` claim, one commit per delivery, batch 10 | 17.1 s (350/s) | 10.9 s (550/s) | 4× workers ≠ 4× speed. Why? |
| 1 | Batch 50 | 11.4 s (530/s) | 7.6 s (785/s) | Helps, still sublinear |
| 2 | *Diagnostic:* `synchronous_commit = off` | 12.2 s | 7.3 s | fsync waits are **not** the limit |
| 3 | Completion as Core `UPDATE … RETURNING` instead of ORM load+mutate | 11.9 s | 10.8 s | ORM overhead is **not** the limit (kept: simpler) |
| 4 | Split claim into two index-ordered queries | 12.9 s | 7.3 s (824/s) | Real inefficiency, small win |
| 5 | **One completion transaction per batch** | 11.2–15.0 s | **3.5–3.7 s (1,617–1,719/s)** | **The bottleneck** |

What pointed to each step:

- **Postgres container CPU was 400–500%** during the 4-worker drain, and a worker process
  was at about 220% CPU. So both sides were busy, and the database was the shared resource.
- **`EXPLAIN (ANALYZE, BUFFERS)` on the old claim** showed `BitmapOr → Bitmap Heap Scan → Sort →
  LockRows → Limit`: every due row was sorted before `LIMIT` applied. On an empty queue the
  partial index still yielded **11,400 dead entries**, which is queue churn waiting for vacuum.
- **Sampling `pg_stat_activity` wait events** (before step 5), about 56 active-session samples:

  | Wait | Samples |
  | --- | --- |
  | `LWLock:WALWrite` on `COMMIT` | 26 |
  | `Client:ClientRead` on `COMMIT` | 7 |
  | `IO:WALSync` on `COMMIT` | 5 |
  | CPU: claim / update / insert / job-load queries | 13 |
  | autovacuum | 3 |

  About two thirds of active time was spent *committing*. With about 6,000 tiny commits per
  drain, sessions queue on the WAL-write lock even with fsync off (step 2), which is why
  only *fewer commits* (step 5) helped.

## Things to remember from this

1. **Measure before optimizing.** Steps 2 and 3 were plausible fixes that did nothing.
2. **Fixing an inefficiency isn't the same as fixing the bottleneck** (step 4).
3. **Wait events tell you what the database is waiting *on*.** CPU graphs only show *that* it's busy.
4. **Commit frequency is a resource.** Batch writes where the semantics allow it.
5. **Queue tables churn.** Watch dead tuples and tune autovacuum per table.

## Redis: before/after (plan step 2)

Measured on 2026-10-20 with the same setup as above, the API running from a
virtualenv, and Redis 7.4 in Docker. The baseline was recorded **before** any Redis code
was written.

### Database round trips per request (deterministic)

`python -m benchmarks.query_count` counts SQL statements for a warm request.

| Request | Before | New code, Redis off | New code, Redis on |
| --- | --- | --- | --- |
| Authenticated `GET /v1/events/{id}` | 3 (api_keys, tenants, events) | 2 (key+tenant join, events) | **1** (events) |
| `POST /v1/events`, 1 recipient × 2 channels, with template | 9 | 8 | **6** |

The 3 removed lookups (credential, tenant, templates) run on *every* ingest,
regardless of payload size. They hit the shared database, which is the resource
the worker benchmarks showed saturating first.

### Ingest latency (1,000 events, 3 recipients × 2 channels, concurrency 20, two runs each)

| Configuration | Throughput | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| Before (baseline code) | 165–180 ev/s | 108–109 ms | 139–194 ms | 152–307 ms |
| New code, `REDIS_URL` unset | 174–183 ev/s | 105–110 ms | 139–150 ms | 211–223 ms |
| **New code, Redis on** | **197–211 ev/s** | **91–95 ms** | **117–134 ms** | 177–281 ms |

About 15% more throughput and 13% lower p50. I had predicted almost no latency
change, reasoning that local queries cost well under a millisecond and the API is
CPU-bound in Python. The prediction was wrong: each skipped statement also skips
SQLAlchemy ORM work and a session round trip, and that is CPU in the API process.

### Cache hit rate, and why Redis's own number misleads

Redis `INFO stats` during the Redis-on runs reported 4,964 hits and 1,036
misses (83%). Isolating the cache (200 authenticated GETs, which skip the
limiter) gave **199 hits, 1 miss (99.5%)**, with one cold miss per key. The ~1,000
"misses" were the rate limiter: the load-test tenant's limit is effectively
unlimited, so its GCRA key expires within microseconds and each request's `GET`
misses by design. A server-wide hit rate mixes different key families; per-cache
metrics are needed (planned for the observability step).

### Rate limiter accuracy

`--rate-limit N` gives the tenant `N`/s with burst `N`, then sends 1,000 events at
concurrency 20. GCRA allows at most `burst + rate × elapsed`.

| Limit | Duration | Accepted (202) | Rejected (429) | Allowed maximum |
| --- | --- | --- | --- | --- |
| 50/s | 3.18 s | 203 | 797 | 209 |
| 20/s | 3.10 s | 81 | 919 | 82 |

Never above the limit. Rejected requests are cheap (cached auth plus one Lua
call, no database work), so the limiter also shields PostgreSQL from a noisy
tenant.

### Redis outage (Docker stack, live)

20 events with Redis up, 20 with `docker compose stop redis`, 20 after restart:
**60/60 accepted** (`202`). Average latency 22 → 13 → 11 ms (no timeout penalty
while down), one `redis_unavailable … bypassing for 5.0s` log line, and every
delivery completed.

## Kafka vs Postgres dispatch (plan step 3)

Measured 2026-10-20/21 with the same laptop setup, plus a single-node Kafka 3.9
(KRaft) in Docker. Workload: 1,000–1,500 events × 3 recipients × 2 channels.
Kafka mode runs **1 relay + 4 consumers**; Postgres mode runs **4 polling workers**.
Batch 50 everywhere.

### Final comparison

| Scenario | Postgres polling | Kafka (outbox → relay → consumers) |
| --- | --- | --- |
| Backlog drain, workers started cold | 1,307–1,463 deliveries/s | 745–862 deliveries/s (includes ~2 s consumer-group join) |
| Backlog drain, **consumers already warm** | — | **1,240 deliveries/s** (full 48–50-message batches) |
| Light steady load (2 workers), lag p95 | 0.39 s | **0.49 s** (was 3.27 s before a fix) |
| Saturated ingest, warm group, lag p95 | 0.47 s | **0.43 s** |
| Ingest throughput (API on the same laptop) | 118–203 events/s | 101–169 events/s |

**Conclusion.** Warm Kafka dispatch is about as fast as Postgres polling, and no
faster. Every delivery still costs the same PostgreSQL claim and completion
writes, *plus* an outbox insert and delete, plus a Kafka round trip and an offset
commit. The ceiling is database state writes, and Kafka doesn't remove them. The
earlier roadmap's throughput justification for Kafka was **wrong**, and the docs
were corrected. Kafka's real benefits here: push delivery (idle workers stop
polling the database), 7-day replay, buffering during outages, and room for more
consumer groups.

### The investigation (Kafka started out much slower)

| Step | Observation | Cause | Fix | Result |
| --- | --- | --- | --- | --- |
| 1 | Backlog drain 778–825/s vs 1,300–1,460/s; steady lag p95 3.27 s vs 0.39 s | — | — | Kafka slower everywhere |
| 2 | Per-partition offsets: each run's messages on exactly **3 of 12** partitions | Key `tenant:user` with a 3-recipient workload means only 3 consumers can work | Key by `delivery_id` (ordering is a non-goal anyway) | Even spread; throughput unchanged, so not the bottleneck |
| 3 | Light-load lag ~1 s+ per batch | `consume(50, timeout=1.0)` waits up to 1 s to *fill* a batch | Two-phase fetch: block for the first message, then take only what's buffered | Steady lag p95 3.27 → **0.49 s** |
| 4 | Sampling outbox vs pending: relay drained 6,000 rows in **< 1 s**, then ~2 s with nothing consumed | Consumers joining one by one trigger eager stop-the-world rebalances | `partition.assignment.strategy=cooperative-sticky`; measure warm | Warm backlog **1,240/s** |
| 5 | Consumer batch size under moderate load: 13–15 messages (poller: 50) | Two-phase fetch takes only what's buffered | Accepted: low latency at light load; batches fill under backlog (48–50) | — |

Two measurements were discarded as invalid and are recorded here so they aren't
reused:
- A "warm backlog" run where the load test also spawned its own relay, so the
  backlog never formed (it reported a meaningless 27,000/s).
- The first warm runs, where consumers started at the same instant as ingest, so
  rebalancing overlapped the traffic.

### Kafka outage (Docker stack, live)

10 events with Kafka up, then `docker compose stop kafka` and 10 more:
**20/20 accepted**. The second batch's 20 deliveries waited in the outbox
(`outbox=20 pending=20`), the relay logged one `publish_failed … Message timed out`
and backed off, and the consumer's partitions were revoked. After
`docker compose start kafka`: partitions reassigned, outbox drained,
`delivered=40 pending=0`.

## Observability and scale experiments (plan step 5)

Measured on 2026-10-21 with Prometheus scraping every 5 s.

### Metrics cross-checked against ground truth

1,500 events × 3 recipients × 2 channels through the Docker stack:

| Measure | Load test / SQL | Prometheus | Verdict |
| --- | --- | --- | --- |
| Events accepted | 1,500 | 1,506 | `increase()` extrapolates to the window edges (expected) |
| Delivery attempts | 9,000 | 9,178 | same |
| Ingest p95 | 238.8 ms | 241 ms | ✓ |
| End-to-end p95, first bucket layout | 11.0 s | **28.35 s** | ✗ buckets jumped 10 s → 30 s |
| End-to-end p50 / p95 / p99, dense buckets | 14.1 / 19.44 / 19.6 s | 13.9 / 19.33 / 19.87 s | ✓ |

**Lesson:** a histogram quantile is only as precise as the buckets around it.
Put dense buckets where your SLO lives.

### The load generator was the bottleneck

All earlier "API throughput" numbers were really load-generator numbers. With
40 configured connections, a single asyncio+httpx process held only **12** open.
Against a 4-process API:

| Generator processes | Throughput | p95 |
| --- | --- | --- |
| 1 | 129 ev/s | 854 ms |
| 2 | 195 ev/s | 427 ms |
| 4 | 203 ev/s | 312 ms |

`load_test.py --processes N` now splits the load.

### API horizontal scaling (`uvicorn --workers N`)

3,000 events × 3 recipients × 2 channels, concurrency 40, 4 generator processes,
ingest only, two runs each:

| API processes | Throughput | p50 | p95 |
| --- | --- | --- | --- |
| 1 | 138–155 ev/s | 237–257 ms | 342–446 ms |
| 2 | 179–180 ev/s | 188–193 ms | 344–350 ms |
| 4 | **213–229 ev/s** | 142–151 ms | 292–375 ms |

About 1.5× for 4× the processes. During the 4-process run the host had **0.1–0.3%
idle CPU**: API processes about 277%, PostgreSQL about 196% (in the Docker VM, 35%
system time), and the rest the load generators. The API scales horizontally; the
limit here is that generator, API and database all share one 8-core laptop. A
real scaling curve needs them on separate hosts.

Connections were spread evenly across the four API processes (4/3/3/2 sampled),
so uneven `accept()` distribution, a suspect on macOS, was ruled out.

### Consumer lag with every consumer down

`docker compose stop worker`, then 300 events (1,800 deliveries). The
relay-measured `sum(kafka_consumer_lag)` read 0 → **1,800** (held) → after a restart
1,250 → 0 within 20 s. A lag metric reported by the consumers themselves would
have disappeared the moment they stopped.

## Reproducing

Numbers vary by about ±20% between runs on a laptop (other processes, Docker VM
scheduling). Run each configuration twice. The script creates a throwaway
`load-*` tenant per run, so runs never collide with seeded data.
