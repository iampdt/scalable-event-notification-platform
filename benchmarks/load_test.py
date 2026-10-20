"""End-to-end load test: ingest N events over HTTP, then measure delivery lag.

    # API must be running (uvicorn app.main:app). Workers are spawned by this script.
    python -m benchmarks.load_test --events 1000 --concurrency 20 --recipients 3 --workers 2

It creates its own throwaway tenant, users, and producer key, so it never
touches seeded data. Spawned workers use the fake email adapter so we measure
the platform, not a mail server.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
import uuid

import httpx

from app.db.models import ApiKey, ApiKeyRole, Tenant, User
from app.db.session import SessionLocal
from app.scripts.stats import collect
from app.security import generate_api_key


def create_tenant(recipients: int, rate_limit: float | None = None) -> tuple[uuid.UUID, str, list[str]]:
    token, prefix, secret_hash = generate_api_key()
    with SessionLocal.begin() as session:
        # Effectively unlimited unless --rate-limit is given, so throughput runs measure the platform.
        tenant = Tenant(slug=f"load-{uuid.uuid4().hex[:10]}", name="load test", rate_limit_per_second=rate_limit or 1_000_000, rate_limit_burst=int(rate_limit or 1_000_000))
        session.add(tenant)
        session.flush()
        users = [User(tenant_id=tenant.id, external_id=f"user-{i}", email=f"user-{i}@example.test") for i in range(recipients)]
        session.add_all(users)
        session.add(ApiKey(tenant_id=tenant.id, key_prefix=prefix, secret_hash=secret_hash, role=ApiKeyRole.producer, name="load"))
        session.flush()
        return tenant.id, token, [str(user.id) for user in users]


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(pct / 100 * (len(ordered) - 1))))]


async def ingest(base_url: str, token: str, user_ids: list[str], channels: list[str], events: int, concurrency: int) -> tuple[list[float], dict[int, int], float]:
    latencies: list[float] = []
    statuses: dict[int, int] = {}
    semaphore = asyncio.Semaphore(concurrency)
    recipients = [{"user_id": user_id, "channels": channels} for user_id in user_ids]

    async with httpx.AsyncClient(base_url=base_url, timeout=30, limits=httpx.Limits(max_connections=concurrency)) as client:
        async def one(index: int) -> None:
            body = {"event_type": "load.test", "subject": {"type": "load", "id": str(index)}, "payload": {"index": index}, "recipients": recipients}
            headers = {"Authorization": f"Bearer {token}", "Idempotency-Key": f"load-{index}"}
            async with semaphore:
                started = time.perf_counter()
                response = await client.post("/v1/events", json=body, headers=headers)
                latencies.append((time.perf_counter() - started) * 1000)
                statuses[response.status_code] = statuses.get(response.status_code, 0) + 1

        started = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(events)))
        return latencies, statuses, time.perf_counter() - started


def spawn_workers(count: int, batch_size: int, dispatch: str = "postgres") -> list[subprocess.Popen]:
    env = {**os.environ, "EMAIL_PROVIDER": "fake", "DISPATCH_MODE": dispatch}
    if dispatch == "kafka":
        # One relay publishes the outbox; `count` consumers share the topic's partitions.
        commands = [[sys.executable, "-m", "app.workers.outbox_relay"]]
        commands += [[sys.executable, "-m", "app.workers.kafka_consumer", "--batch-size", str(batch_size), "--concurrency", str(batch_size)]] * count
    else:
        commands = [[sys.executable, "-m", "app.workers.delivery_worker", "--batch-size", str(batch_size), "--concurrency", str(batch_size), "--poll-seconds", "0.2"]] * count
    return [subprocess.Popen(command, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for command in commands]


def wait_for_drain(tenant_id: uuid.UUID, timeout: float) -> float:
    started = time.perf_counter()
    while time.perf_counter() - started < timeout:
        with SessionLocal() as session:
            queue = collect(session, minutes=120, tenant_id=tenant_id)["queue"]
        if queue["due_now"] + queue["in_flight"] + queue["expired_leases"] == 0:
            return time.perf_counter() - started
        time.sleep(0.25)
    raise TimeoutError("deliveries did not drain in time")


def main() -> None:
    parser = argparse.ArgumentParser(description="Notification platform load test")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--recipients", type=int, default=3)
    parser.add_argument("--channels", default="email,in_app")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--worker-batch", type=int, default=10)
    parser.add_argument("--start-workers-first", action="store_true", help="measure steady-state lag instead of backlog drain")
    parser.add_argument("--dispatch", choices=["postgres", "kafka"], default="postgres", help="must match the API's DISPATCH_MODE")
    parser.add_argument("--rate-limit", type=float, help="give the tenant this events/s limit (burst = same) and report how the limiter behaves")
    args = parser.parse_args()

    tenant_id, token, user_ids = create_tenant(args.recipients, args.rate_limit)
    channels = args.channels.split(",")
    workers = spawn_workers(args.workers, args.worker_batch, args.dispatch) if args.start_workers_first else []
    try:
        latencies, statuses, elapsed = asyncio.run(ingest(args.base_url, token, user_ids, channels, args.events, args.concurrency))
        if args.rate_limit:
            accepted = statuses.get(202, 0)
            print(json.dumps({"rate_limit": args.rate_limit, "seconds": round(elapsed, 2), "statuses": statuses, "accepted_per_second": round(accepted / elapsed, 1), "expected_max": round(args.rate_limit + args.rate_limit / elapsed, 1)}))
            return
        ingest_done = time.perf_counter()
        if not workers:
            workers = spawn_workers(args.workers, args.worker_batch, args.dispatch)
        drain_seconds = wait_for_drain(tenant_id, timeout=600)
        total_seconds = time.perf_counter() - ingest_done + elapsed
    finally:
        for worker in workers:
            worker.terminate()
        for worker in workers:
            worker.wait(timeout=30)

    with SessionLocal() as session:
        stats = collect(session, minutes=120, tenant_id=tenant_id)
    deliveries = args.events * args.recipients * len(channels)
    report = {
        "config": vars(args),
        "ingest": {
            "events": args.events,
            "statuses": statuses,
            "seconds": round(elapsed, 2),
            "events_per_second": round(args.events / elapsed, 1),
            "latency_ms": {"p50": round(percentile(latencies, 50), 1), "p95": round(percentile(latencies, 95), 1), "p99": round(percentile(latencies, 99), 1), "mean": round(statistics.mean(latencies), 1)},
        },
        "delivery": {
            "deliveries": deliveries,
            "drain_seconds_after_ingest": round(drain_seconds, 2),
            "deliveries_per_second_overall": round(deliveries / total_seconds, 1),
            "by_channel": stats["by_channel"],
        },
    }
    print(json.dumps(report, indent=2, default=lambda value: round(float(value), 3)))


if __name__ == "__main__":
    main()
