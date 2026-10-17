"""PostgreSQL SKIP LOCKED worker for at-least-once delivery."""
from __future__ import annotations

import argparse
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select

from app.db.models import Delivery, DeliveryAttempt, DeliveryChannel, DeliveryStatus
from app.db.session import SessionLocal
from app.notifications.retry import ProviderFailure, retry_delay_seconds
from app.providers.fake_email import FakeEmailProvider


@dataclass(frozen=True)
class ClaimedDelivery:
    id: uuid.UUID
    tenant_id: uuid.UUID
    channel: DeliveryChannel
    target_snapshot: dict
    attempt_count: int
    max_attempts: int


class DeliveryWorker:
    def __init__(self, email_provider: FakeEmailProvider | None = None, lease_seconds: int = 60):
        self.email_provider = email_provider or FakeEmailProvider()
        self.lease_seconds = lease_seconds

    def claim(self, batch_size: int = 25) -> list[ClaimedDelivery]:
        now = datetime.now(timezone.utc)
        with SessionLocal.begin() as session:
            due = session.scalars(
                select(Delivery)
                .where(or_((Delivery.status == DeliveryStatus.pending) & (Delivery.next_attempt_at <= now), (Delivery.status == DeliveryStatus.processing) & (Delivery.lease_expires_at < now)))
                .order_by(Delivery.next_attempt_at, Delivery.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            ).all()
            for delivery in due:
                delivery.status = DeliveryStatus.processing
                delivery.attempt_count += 1
                delivery.lease_expires_at = now + timedelta(seconds=self.lease_seconds)
                delivery.updated_at = now
            session.flush()
            return [ClaimedDelivery(item.id, item.tenant_id, item.channel, item.target_snapshot, item.attempt_count, item.max_attempts) for item in due]

    def complete_success(self, claimed: ClaimedDelivery) -> bool:
        with SessionLocal.begin() as session:
            delivery = session.scalar(select(Delivery).where(Delivery.id == claimed.id, Delivery.status == DeliveryStatus.processing, Delivery.attempt_count == claimed.attempt_count).with_for_update())
            if not delivery:
                return False
            now = datetime.now(timezone.utc)
            delivery.status = DeliveryStatus.delivered
            delivery.delivered_at = now
            delivery.lease_expires_at = None
            delivery.last_error_code = None
            session.add(DeliveryAttempt(tenant_id=delivery.tenant_id, delivery_id=delivery.id, attempt_number=claimed.attempt_count, started_at=now, completed_at=now, outcome="success"))
            return True

    def complete_failure(self, claimed: ClaimedDelivery, failure: ProviderFailure) -> bool:
        with SessionLocal.begin() as session:
            delivery = session.scalar(select(Delivery).where(Delivery.id == claimed.id, Delivery.status == DeliveryStatus.processing, Delivery.attempt_count == claimed.attempt_count).with_for_update())
            if not delivery:
                return False
            now = datetime.now(timezone.utc)
            retry = failure.retryable and claimed.attempt_count < claimed.max_attempts
            delivery.status = DeliveryStatus.pending if retry else DeliveryStatus.failed
            delivery.next_attempt_at = now + timedelta(seconds=retry_delay_seconds(claimed.attempt_count)) if retry else delivery.next_attempt_at
            delivery.failed_at = None if retry else now
            delivery.lease_expires_at = None
            delivery.last_error_code = str(failure.status_code or ("retryable_failure" if retry else "permanent_failure"))
            session.add(DeliveryAttempt(tenant_id=delivery.tenant_id, delivery_id=delivery.id, attempt_number=claimed.attempt_count, started_at=now, completed_at=now, outcome="retryable_failure" if retry else "permanent_failure", provider_status_code=failure.status_code, provider_message=str(failure)[:512]))
            return True

    def deliver(self, claimed: ClaimedDelivery) -> bool:
        try:
            if claimed.channel == DeliveryChannel.email:
                self.email_provider.send(delivery_id=claimed.id, target=claimed.target_snapshot)
            return self.complete_success(claimed)
        except ProviderFailure as failure:
            return self.complete_failure(claimed, failure)
        except Exception as exc:
            return self.complete_failure(claimed, ProviderFailure("provider error", retryable=True))

    def run_once(self, batch_size: int = 25) -> int:
        claimed = self.claim(batch_size)
        for delivery in claimed:
            self.deliver(delivery)
        return len(claimed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the notification delivery worker")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    args = parser.parse_args()
    worker = DeliveryWorker()
    while True:
        processed = worker.run_once(args.batch_size)
        if args.once:
            return
        if not processed:
            time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
