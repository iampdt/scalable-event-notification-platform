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

## Reproducing

Numbers vary by about ±20% between runs on a laptop (other processes, Docker VM
scheduling). Run each configuration twice. The script creates a throwaway
`load-*` tenant per run, so runs never collide with seeded data.
