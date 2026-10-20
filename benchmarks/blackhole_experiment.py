"""Failure isolation under a black-hole webhook endpoint, with and without the circuit breaker.

    python -m benchmarks.blackhole_experiment [--a-events 40 --b-events 40]

A "black hole" accepts TCP connections and never answers, so every request to
it waits out the full webhook timeout (10 s). Tenant A's webhook points at it;
tenant B only uses in-app (healthy). A's events are ingested first, then B's.
One polling worker (batch 10, concurrency 10) runs in each configuration:

  breaker OFF: CIRCUIT_FAILURE_THRESHOLD=100000
  breaker ON:  CIRCUIT_FAILURE_THRESHOLD=5

Reported: how long B waited (delivery lag), and how many requests the worker
sent into the black hole. Needs PostgreSQL and Redis; run nothing else that
polls the same database while it runs.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import threading
import time
import uuid

from sqlalchemy import func, select, text, update

from app.api.schemas import IngestEvent
from app.db.models import ApiKeyRole, Delivery, DeliveryAttempt, Notification, WebhookEndpoint
from app.db.session import SessionLocal
from app.events.service import ingest
from app.security import encrypt_secret
from app.testing import create_api_key, create_tenant_with_key


def black_hole(port: int, stop: threading.Event) -> None:
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen(512)
    server.settimeout(0.2)
    held = []
    while not stop.is_set():
        try:
            connection, _ = server.accept()
            held.append(connection)  # never read, never answer
        except socket.timeout:
            pass
    for connection in held:
        connection.close()
    server.close()


def post(tenant, idem: str, channels: list[str]) -> None:
    body = IngestEvent.model_validate({"event_type": "blackhole.test", "subject": {"type": "t", "id": idem}, "payload": {}, "recipients": [{"user_id": str(tenant.user_id), "channels": channels}]})
    with SessionLocal() as session:
        ingest(session, tenant.tenant_id, idem, body)


def retire_experiment_rows() -> None:
    """Fail leftover work from earlier runs: a polling worker claims the oldest due
    rows first, so leftovers would sit at the head of the queue and skew timing."""
    with SessionLocal.begin() as session:
        session.execute(text(
            """
            UPDATE deliveries d SET status = 'failed', failed_at = now(), lease_expires_at = NULL, last_error_code = 'experiment_cleanup'
            FROM tenants t WHERE t.id = d.tenant_id AND t.slug LIKE 'bh-%' AND d.status IN ('pending', 'processing')
            """
        ))


def assert_queue_empty() -> None:
    with SessionLocal() as session:
        due = session.scalar(text("SELECT count(*) FROM deliveries WHERE status IN ('pending', 'processing')"))
    if due:
        raise SystemExit(f"{due} other deliveries are pending/processing; the experiment needs an empty queue")


def run(threshold: int, a_events: int, b_events: int, port: int, worker_log: str | None = None) -> dict:
    retire_experiment_rows()
    assert_queue_empty()
    a = create_tenant_with_key(f"bh-a-{uuid.uuid4().hex[:8]}")
    b = create_tenant_with_key(f"bh-b-{uuid.uuid4().hex[:8]}")
    create_api_key(a.tenant_id, ApiKeyRole.admin, "admin")
    with SessionLocal.begin() as session:
        session.add(WebhookEndpoint(tenant_id=a.tenant_id, url=f"http://127.0.0.1:{port}/hook", secret_ciphertext=encrypt_secret("s" * 32), active=True, is_default=True))
    for i in range(a_events):
        post(a, f"a-{i}", ["webhook"])
    for i in range(b_events):
        post(b, f"b-{i}", ["in_app"])

    env = {**os.environ, "DISPATCH_MODE": "postgres", "EMAIL_PROVIDER": "fake", "WEBHOOK_ALLOW_PRIVATE_NETWORKS": "true", "WEBHOOK_TIMEOUT_SECONDS": "10", "CIRCUIT_FAILURE_THRESHOLD": str(threshold), "CIRCUIT_COOLDOWN_SECONDS": "30"}
    log = open(f"{worker_log}.{threshold}.log", "w") if worker_log else subprocess.DEVNULL
    worker = subprocess.Popen([sys.executable, "-m", "app.workers.delivery_worker", "--batch-size", "10", "--concurrency", "10", "--poll-seconds", "0.2"], env=env, stdout=log, stderr=log)
    started = time.monotonic()
    b_done_at = None
    try:
        while time.monotonic() - started < 300:
            with SessionLocal() as session:
                pending_b = session.scalar(select(func.count()).select_from(Delivery).where(Delivery.tenant_id == b.tenant_id, Delivery.status != "delivered"))
            if pending_b == 0:
                b_done_at = time.monotonic()
                break
            time.sleep(0.25)
    finally:
        worker.terminate()
        worker.wait(timeout=60)
    with SessionLocal() as session:
        lags = [row[0] for row in session.execute(select(func.extract("epoch", Delivery.delivered_at - Delivery.created_at)).where(Delivery.tenant_id == b.tenant_id))]
        a_attempts = session.scalar(select(func.count()).select_from(DeliveryAttempt).where(DeliveryAttempt.tenant_id == a.tenant_id))
        a_deferred = session.scalar(select(func.count()).select_from(Delivery).where(Delivery.tenant_id == a.tenant_id, Delivery.status == "pending", Delivery.attempt_count == 0))
        # Stop A's leftover retries from hitting anything after the experiment.
        session.execute(update(WebhookEndpoint).where(WebhookEndpoint.tenant_id == a.tenant_id).values(active=False))
        session.commit()
    retire_experiment_rows()
    lags = sorted(float(lag) for lag in lags)
    return {
        "circuit_threshold": threshold,
        "tenant_b_all_delivered_after_s": round(b_done_at - started, 1) if b_done_at else None,
        "tenant_b_lag_s": {"p50": round(statistics.median(lags), 1), "max": round(lags[-1], 1)} if lags else None,
        "requests_into_black_hole": a_attempts,
        "tenant_a_deferred_without_spending_an_attempt": a_deferred,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--a-events", type=int, default=40)
    parser.add_argument("--b-events", type=int, default=40)
    parser.add_argument("--port", type=int, default=9124)
    parser.add_argument("--worker-log", help="path prefix for worker logs (one file per run)")
    parser.add_argument("--breaker-on-only", action="store_true")
    args = parser.parse_args()
    stop = threading.Event()
    threading.Thread(target=black_hole, args=(args.port, stop), daemon=True).start()
    try:
        on = run(5, args.a_events, args.b_events, args.port, args.worker_log)
        off = None if args.breaker_on_only else run(100_000, args.a_events, args.b_events, args.port, args.worker_log)
    finally:
        stop.set()
    print(json.dumps({"breaker_off": off, "breaker_on": on}, indent=2))


if __name__ == "__main__":
    main()
