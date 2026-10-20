"""PostgreSQL SKIP LOCKED worker for at-least-once delivery.

One cycle: claim (short txn) -> send concurrently (no txn) -> complete (one short txn per batch).
Never hold a database transaction open across provider I/O.
"""
from __future__ import annotations

import argparse
import logging
import math
import random
import signal
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, insert, text, update
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from app import circuit, metrics
from app.logging_config import configure_logging
from app.config import dispatch_mode, email_provider_name, webhook_timeout_seconds
from app.db.models import Delivery, DeliveryAttempt, DeliveryChannel, DeliveryStatus, OutboxMessage
from app.messaging import delivery_message
from app.db.session import SessionLocal
from app.notifications.retry import ProviderFailure, retry_delay_seconds
from app.providers.base import ClaimedDelivery, Provider
from app.providers.fake_email import FakeEmailProvider
from app.providers.in_app import InAppProvider
from app.providers.smtp_email import SmtpEmailProvider
from app.providers.webhook import WebhookProvider

logger = logging.getLogger(__name__)

# Claiming is split into two statements, one per partial index. A single query
# with "pending-and-due OR processing-and-expired" made PostgreSQL BitmapOr both
# indexes, fetch and sort *every* due row, then keep :limit of them, so each
# claim cost O(backlog). Each statement below is an ordered index scan that stops
# after :limit unlocked rows. Leases use the *database* clock so workers on different hosts
# cannot disagree about whether a lease has expired.
CLAIM_TEMPLATE = """
    WITH picked AS (
        SELECT id FROM deliveries
        WHERE status = '{status}' AND {due_column} {comparison} now(){extra}
        ORDER BY {due_column}, id
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE deliveries d
    SET status = 'processing',
        attempt_count = d.attempt_count + 1,
        lease_expires_at = now() + make_interval(secs => :lease_seconds),
        updated_at = now()
    FROM picked
    WHERE d.id = picked.id
    RETURNING d.id
"""
RECLAIM_EXPIRED_SQL = text(CLAIM_TEMPLATE.format(status="processing", due_column="lease_expires_at", comparison="<", extra=" AND attempt_count < max_attempts"))

# Poison-message guard: a delivery whose attempts keep killing the worker (OOM,
# segfault, hang) never reaches the normal "attempts exhausted" check, because no
# failure is ever recorded. Without this it would be reclaimed forever.
FAIL_ABANDONED_SQL = text(
    """
    WITH abandoned AS (
        SELECT id FROM deliveries
        WHERE status = 'processing' AND lease_expires_at < now() AND attempt_count >= max_attempts
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE deliveries d
    SET status = 'failed', failed_at = now(), lease_expires_at = NULL,
        last_error_code = 'worker_lost', updated_at = now()
    FROM abandoned
    WHERE d.id = abandoned.id
    RETURNING d.id, d.channel
    """
)
CLAIM_DUE_SQL = text(CLAIM_TEMPLATE.format(status="pending", due_column="next_attempt_at", comparison="<=", extra=""))

# Kafka mode: claim exactly the deliveries named by consumed messages. The WHERE
# clause makes duplicate or early messages harmless: a row that is already
# processing/delivered, or whose retry is not due yet, is simply not returned.
CLAIM_IDS_SQL = text(
    """
    UPDATE deliveries
    SET status = 'processing',
        attempt_count = attempt_count + 1,
        lease_expires_at = now() + make_interval(secs => :lease_seconds),
        updated_at = now()
    WHERE id = ANY(:ids) AND status = 'pending' AND next_attempt_at <= now()
    RETURNING id
    """
)

# Everything an adapter needs, loaded in the claim transaction so adapters stay DB-free.
JOB_SQL = text(
    """
    SELECT d.id, d.tenant_id, d.channel, d.target_snapshot, d.content, d.attempt_count, d.max_attempts, d.webhook_endpoint_id,
           n.id AS notification_id, u.id AS user_id, u.external_id,
           e.id AS event_id, e.event_type, e.subject_type, e.subject_id, e.payload, e.created_at AS occurred_at,
           w.active AS webhook_active, w.secret_ciphertext
    FROM deliveries d
    JOIN notifications n ON n.tenant_id = d.tenant_id AND n.id = d.notification_id
    JOIN users u ON u.tenant_id = n.tenant_id AND u.id = n.user_id
    JOIN events e ON e.tenant_id = n.tenant_id AND e.id = n.event_id
    LEFT JOIN webhook_endpoints w ON w.tenant_id = d.tenant_id AND w.id = d.webhook_endpoint_id
    WHERE d.id = ANY(:ids)
    ORDER BY d.next_attempt_at, d.id
    """
)


def default_email_provider() -> Provider:
    return SmtpEmailProvider() if email_provider_name() == "smtp" else FakeEmailProvider()


@dataclass(frozen=True)
class Outcome:
    claimed: ClaimedDelivery
    started_at: datetime
    completed_at: datetime
    status_code: int | None = None
    failure: ProviderFailure | None = None
    # Set when a circuit breaker refused the send: retry later, no attempt spent.
    deferred_seconds: float | None = None


class DeliveryWorker:
    def __init__(
        self,
        email_provider: Provider | None = None,
        webhook_provider: Provider | None = None,
        lease_seconds: int = 60,
        concurrency: int = 10,
        provider_timeout_seconds: float | None = None,
    ):
        self.providers: dict[DeliveryChannel, Provider] = {
            DeliveryChannel.email: email_provider or default_email_provider(),
            DeliveryChannel.webhook: webhook_provider or WebhookProvider(),
            DeliveryChannel.in_app: InAppProvider(),
        }
        self.lease_seconds = lease_seconds
        self.concurrency = concurrency
        self.provider_timeout_seconds = provider_timeout_seconds if provider_timeout_seconds is not None else webhook_timeout_seconds()
        self.reclaimed_total = 0

    def check_lease_budget(self, batch_size: int) -> None:
        """A lease that expires while we are still sending causes a duplicate send.

        Worst case a batch takes ceil(batch/concurrency) sequential provider
        timeouts; require the lease to cover twice that.
        """
        worst_case = math.ceil(batch_size / self.concurrency) * self.provider_timeout_seconds
        if self.lease_seconds < 2 * worst_case:
            raise ValueError(
                f"lease_seconds={self.lease_seconds} is too short for batch_size={batch_size}, "
                f"concurrency={self.concurrency}, provider timeout={self.provider_timeout_seconds}s "
                f"(needs >= {2 * worst_case:.0f}s)"
            )

    def claim(self, batch_size: int = 10) -> list[ClaimedDelivery]:
        with SessionLocal.begin() as session:
            params = {"limit": batch_size, "lease_seconds": self.lease_seconds}
            abandoned = session.execute(FAIL_ABANDONED_SQL, params).all()
            if abandoned:
                logger.error("abandoned_deliveries_failed count=%s ids=%s", len(abandoned), [str(row.id) for row in abandoned])
                for row in abandoned:
                    metrics.DELIVERIES_DEAD_LETTERED.labels(str(row.channel), "worker_lost").inc()
            # Crashed workers' rows first; they have already waited a full lease.
            reclaimed = [row.id for row in session.execute(RECLAIM_EXPIRED_SQL, params)]
            if reclaimed:
                self.reclaimed_total += len(reclaimed)
                metrics.LEASES_RECLAIMED.inc(len(reclaimed))
                logger.warning("reclaimed_expired_leases count=%s", len(reclaimed))
            due = [row.id for row in session.execute(CLAIM_DUE_SQL, {**params, "limit": batch_size - len(reclaimed)})] if len(reclaimed) < batch_size else []
            ids = reclaimed + due
            if not ids:
                return []
            return self._load_jobs(session, ids)

    def claim_ids(self, ids: list[uuid.UUID]) -> list[ClaimedDelivery]:
        """Kafka mode: claim the deliveries named by consumed messages (duplicates drop out)."""
        if not ids:
            return []
        with SessionLocal.begin() as session:
            claimed = [row.id for row in session.execute(CLAIM_IDS_SQL, {"ids": ids, "lease_seconds": self.lease_seconds})]
            if len(claimed) < len(set(ids)):
                logger.info("duplicate_or_not_due_messages_skipped count=%s", len(set(ids)) - len(claimed))
            return self._load_jobs(session, claimed) if claimed else []

    def _load_jobs(self, session, ids: list[uuid.UUID]) -> list[ClaimedDelivery]:
        rows = session.execute(JOB_SQL, {"ids": ids}).all()
        return [
            ClaimedDelivery(
                id=row.id,
                tenant_id=row.tenant_id,
                channel=DeliveryChannel(row.channel),
                target_snapshot=row.target_snapshot,
                content=row.content,
                attempt_count=row.attempt_count,
                max_attempts=row.max_attempts,
                notification_id=row.notification_id,
                user_id=row.user_id,
                user_external_id=row.external_id,
                event_id=row.event_id,
                event_type=row.event_type,
                subject={"type": row.subject_type, "id": row.subject_id},
                payload=row.payload,
                occurred_at=row.occurred_at,
                webhook_active=row.webhook_active,
                webhook_endpoint_id=row.webhook_endpoint_id,
                webhook_secret_ciphertext=row.secret_ciphertext,
            )
            for row in rows
        ]

    def _apply(self, session, outcome: Outcome) -> bool:
        """Fenced state transition + attempt row for one outcome.

        The WHERE clause is the fence: attempt_count acts as a fencing token. If
        our lease expired and another worker re-claimed the row, attempt_count
        has moved on, zero rows match, and our late result is dropped.
        """
        claimed, failure = outcome.claimed, outcome.failure
        if outcome.deferred_seconds is not None:
            return self._defer(session, outcome)
        if failure is None:
            values = {"status": DeliveryStatus.delivered, "delivered_at": func.now(), "last_error_code": None}
            attempt = {"outcome": "success", "provider_status_code": outcome.status_code}
        else:
            # The attempt row records what the provider said; the delivery status
            # records our decision. "retryable_failure" + failed = retries exhausted.
            retry = failure.retryable and claimed.attempt_count < claimed.max_attempts
            values = {"last_error_code": str(failure.status_code or ("retryable_failure" if failure.retryable else "permanent_failure"))}
            if retry:
                next_attempt_at = func.now() + timedelta(seconds=retry_delay_seconds(claimed.attempt_count, failure.retry_after))
                values.update(status=DeliveryStatus.pending, failed_at=None, next_attempt_at=next_attempt_at)
            else:
                values.update(status=DeliveryStatus.failed, failed_at=func.now())
            attempt = {"outcome": "retryable_failure" if failure.retryable else "permanent_failure", "provider_status_code": failure.status_code, "provider_message": str(failure)[:512]}

        updated = session.execute(
            update(Delivery)
            .where(Delivery.id == claimed.id, Delivery.status == DeliveryStatus.processing, Delivery.attempt_count == claimed.attempt_count)
            .values(lease_expires_at=None, updated_at=func.now(), **values)
            .returning(Delivery.id)
        ).first()
        if not updated:
            logger.warning("stale_completion_dropped delivery_id=%s attempt=%s", claimed.id, claimed.attempt_count)
            return False
        session.execute(insert(DeliveryAttempt).values(tenant_id=claimed.tenant_id, delivery_id=claimed.id, attempt_number=claimed.attempt_count, started_at=outcome.started_at, completed_at=outcome.completed_at, **attempt))
        if values.get("status") == DeliveryStatus.pending and dispatch_mode() == "kafka":
            # Kafka has no delayed delivery, so the retry is scheduled here: an
            # outbox row that becomes publishable at next_attempt_at, written in
            # the same transaction as the state change.
            session.execute(insert(OutboxMessage).values(**delivery_message(claimed.id, claimed.tenant_id, available_at=values["next_attempt_at"])))
        return True

    def _defer(self, session, outcome: Outcome) -> bool:
        """Circuit open: hand the attempt back (attempt_count - 1) and retry later.

        Burning retries against a target we already know is down would push
        healthy-but-unlucky deliveries into the dead-letter queue.
        """
        claimed = outcome.claimed
        next_attempt_at = func.now() + timedelta(seconds=outcome.deferred_seconds)
        updated = session.execute(
            update(Delivery)
            .where(Delivery.id == claimed.id, Delivery.status == DeliveryStatus.processing, Delivery.attempt_count == claimed.attempt_count)
            .values(status=DeliveryStatus.pending, attempt_count=Delivery.attempt_count - 1, next_attempt_at=next_attempt_at, lease_expires_at=None, updated_at=func.now())
            .returning(Delivery.id)
        ).first()
        if updated and dispatch_mode() == "kafka":
            session.execute(insert(OutboxMessage).values(**delivery_message(claimed.id, claimed.tenant_id, available_at=next_attempt_at)))
        return bool(updated)

    def complete(self, outcomes: list[Outcome]) -> list[bool]:
        """Record a whole batch in ONE transaction.

        Measured: with a commit per delivery, PostgreSQL sessions spent most of
        their time waiting on WALWrite/WALSync at COMMIT. One commit per batch
        removes that ceiling. If the batch transaction fails for any reason we
        fall back to a commit per outcome so one bad row cannot block the rest.
        """
        if not outcomes:
            return []
        try:
            with SessionLocal.begin() as session:
                results = [self._apply(session, outcome) for outcome in outcomes]
            self._record_committed(outcomes, results)
            return results
        except SQLAlchemyError:
            logger.exception("batch_completion_failed size=%s; retrying one by one", len(outcomes))
        results = []
        for outcome in outcomes:
            try:
                with SessionLocal.begin() as session:
                    results.append(self._apply(session, outcome))
            except SQLAlchemyError:
                # Left in processing; lease expiry will hand it to another attempt.
                logger.exception("completion_failed delivery_id=%s", outcome.claimed.id)
                results.append(False)
        self._record_committed(outcomes, results)
        return results

    @staticmethod
    def _record_committed(outcomes: list[Outcome], results: list[bool]) -> None:
        """Metrics for state that is now committed (so a rolled-back batch that is
        re-applied row by row is not counted twice)."""
        for outcome, applied in zip(outcomes, results):
            if not applied:
                if outcome.deferred_seconds is None:
                    metrics.STALE_COMPLETIONS.inc()
                continue
            failure, claimed = outcome.failure, outcome.claimed
            if failure is not None and not (failure.retryable and claimed.attempt_count < claimed.max_attempts):
                metrics.DELIVERIES_DEAD_LETTERED.labels(claimed.channel.value, "exhausted" if failure.retryable else "permanent").inc()

    def complete_success(self, claimed: ClaimedDelivery, started_at: datetime | None = None, status_code: int | None = None) -> bool:
        now = datetime.now(timezone.utc)
        return self.complete([Outcome(claimed, started_at or now, now, status_code=status_code)])[0]

    def complete_failure(self, claimed: ClaimedDelivery, failure: ProviderFailure, started_at: datetime | None = None) -> bool:
        now = datetime.now(timezone.utc)
        return self.complete([Outcome(claimed, started_at or now, now, failure=failure)])[0]

    def circuit_scope(self, claimed: ClaimedDelivery) -> str | None:
        """One breaker per webhook endpoint / email provider; in-app has nothing to protect."""
        if claimed.channel == DeliveryChannel.webhook and claimed.webhook_endpoint_id:
            return f"webhook:{claimed.webhook_endpoint_id}"
        if claimed.channel == DeliveryChannel.email:
            return f"email:{type(self.providers[DeliveryChannel.email]).__name__}"
        return None

    def attempt(self, claimed: ClaimedDelivery) -> Outcome:
        """Call the provider. No database access here, by design."""
        started_at = datetime.now(timezone.utc)
        scope = self.circuit_scope(claimed)
        permit = circuit.before(scope) if scope else circuit.ALLOW
        channel = claimed.channel.value
        if not permit.allowed:
            metrics.DELIVERIES_DEFERRED.labels(channel).inc()
            # Jitter spreads the deferred deliveries so they don't all return at
            # the instant the circuit half-opens.
            return Outcome(claimed, started_at, started_at, deferred_seconds=permit.retry_after_seconds + random.uniform(0, 5))
        timer = time.perf_counter()
        try:
            status_code = self.providers[claimed.channel].send(claimed)
            outcome = Outcome(claimed, started_at, datetime.now(timezone.utc), status_code=status_code)
            healthy = True
            metrics.DELIVERY_ATTEMPTS.labels(channel, "success").inc()
            if claimed.occurred_at:
                metrics.END_TO_END.labels(channel).observe((outcome.completed_at - claimed.occurred_at).total_seconds())
        except ProviderFailure as failure:
            outcome = Outcome(claimed, started_at, datetime.now(timezone.utc), failure=failure)
            # Only "is the target up?" failures count; a 4xx means it answered.
            healthy = not failure.retryable
            metrics.DELIVERY_ATTEMPTS.labels(channel, "retryable_failure" if failure.retryable else "permanent_failure").inc()
        except Exception:
            # Bugs or unexpected library errors: retry rather than lose the message,
            # and keep the exception detail in logs rather than in the attempt row.
            logger.exception("provider_crashed delivery_id=%s channel=%s", claimed.id, channel)
            metrics.DELIVERY_ATTEMPTS.labels(channel, "provider_error").inc()
            return Outcome(claimed, started_at, datetime.now(timezone.utc), failure=ProviderFailure("internal provider error", retryable=True))
        finally:
            metrics.PROVIDER_DURATION.labels(channel).observe(time.perf_counter() - timer)
        if scope:
            transition = circuit.after(scope, permit, healthy)
            if transition:
                metrics.CIRCUIT_TRANSITIONS.labels(channel, transition).inc()
                logger.warning("circuit_%s scope=%s", transition, scope)
        return outcome

    def deliver(self, claimed: ClaimedDelivery) -> bool:
        return self.complete([self.attempt(claimed)])[0]

    def process_ids(self, ids: list[uuid.UUID], executor: ThreadPoolExecutor | None = None) -> int:
        """Kafka mode: claim, send, and record the deliveries named by a consumed batch."""
        return self._process(self.claim_ids(ids), executor, mode="kafka")

    def run_once(self, batch_size: int = 10, executor: ThreadPoolExecutor | None = None) -> int:
        return self._process(self.claim(batch_size), executor, mode="poll")

    def _process(self, claimed: list[ClaimedDelivery], executor: ThreadPoolExecutor | None, mode: str = "poll") -> int:
        if claimed:
            metrics.BATCH_SIZE.labels(mode).observe(len(claimed))
        if executor is None or len(claimed) <= 1:
            outcomes = [self.attempt(delivery) for delivery in claimed]
        else:
            outcomes = list(executor.map(self.attempt, claimed))
        self.complete(outcomes)
        return len(claimed)


class Backoff:
    """In-process exponential backoff for a dependency outage (1 s doubling to 30 s).

    Without it, a worker loop that hits a database error crashes and relies on
    the container restart policy: noisy restarts, lost graceful shutdown, and
    every restart re-pays connection and consumer-group setup.
    """

    def __init__(self, base: float = 1.0, cap: float = 30.0):
        self.base, self.cap, self.failures = base, cap, 0

    def failed(self, what: str, exc: Exception) -> None:
        delay = min(self.cap, self.base * 2 ** self.failures)
        self.failures += 1
        logger.error("%s_unavailable error=%s; retrying in %.0fs (failure %s)", what, type(exc).__name__, delay, self.failures)
        time.sleep(delay)

    def succeeded(self) -> None:
        if self.failures:
            logger.info("dependency_recovered after %s failures", self.failures)
        self.failures = 0


class GracefulExit:
    """SIGTERM/SIGINT finish the in-flight batch; anything cut short is covered by lease reclaim."""

    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGTERM, self._request)
        signal.signal(signal.SIGINT, self._request)

    def _request(self, signum, frame) -> None:
        logger.info("shutdown_requested signal=%s", signum)
        self.requested = True


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the notification delivery worker")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--lease-seconds", type=int, default=60)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    args = parser.parse_args()
    configure_logging("delivery-worker")
    metrics.start_worker_metrics_server()

    worker = DeliveryWorker(lease_seconds=args.lease_seconds, concurrency=args.concurrency)
    worker.check_lease_budget(args.batch_size)
    stop = GracefulExit()
    logger.info("worker_started batch_size=%s concurrency=%s lease_seconds=%s", args.batch_size, args.concurrency, args.lease_seconds)
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        backoff = Backoff()
        while not stop.requested:
            try:
                processed = worker.run_once(args.batch_size, executor)
            except OperationalError as exc:
                backoff.failed("database", exc)
                continue
            backoff.succeeded()
            if args.once:
                return
            if not processed:
                time.sleep(args.poll_seconds)
    logger.info("worker_stopped")


if __name__ == "__main__":
    main()
