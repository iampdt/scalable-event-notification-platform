"""Worker integration checks; requires TEST_DATABASE_URL."""

from __future__ import annotations

import os
import uuid

import pytest

from app.notifications.retry import ProviderFailure

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is required for integration tests"
)


def test_expired_lease_is_reclaimed_as_the_same_delivery():
    from app.testing import create_delivery, expire_lease, get_delivery, get_attempts
    from app.workers.delivery_worker import DeliveryWorker

    delivery = create_delivery(channel="email")
    first = DeliveryWorker().claim(batch_size=100)
    assert delivery in [item.id for item in first]
    expire_lease(delivery)

    second = DeliveryWorker().claim(batch_size=100)
    reclaimed = next(item for item in second if item.id == delivery)
    assert reclaimed.attempt_count == 2
    DeliveryWorker().complete_success(reclaimed)
    assert get_delivery(delivery).attempt_count == 2
    assert [attempt.attempt_number for attempt in get_attempts(delivery)] == [2]


def test_two_workers_do_not_claim_the_same_delivery():
    from app.testing import create_delivery
    from app.workers.delivery_worker import DeliveryWorker

    delivery = create_delivery(channel="in_app")
    worker_one = DeliveryWorker()
    worker_two = DeliveryWorker()
    first = worker_one.claim(batch_size=100)
    second = worker_two.claim(batch_size=100)
    claimed_ids = [item.id for item in first + second]
    assert claimed_ids.count(delivery) == 1


def test_late_completion_from_expired_lease_is_fenced_off():
    """Worker 1 stalls past its lease, worker 2 re-claims; worker 1's late result must not win."""
    from app.testing import create_delivery, expire_lease, get_attempts, get_delivery
    from app.workers.delivery_worker import DeliveryWorker

    delivery = create_delivery(channel="email")
    stalled = next(item for item in DeliveryWorker().claim(batch_size=1000) if item.id == delivery)
    expire_lease(delivery)
    reclaimed = next(item for item in DeliveryWorker().claim(batch_size=1000) if item.id == delivery)

    assert DeliveryWorker().complete_failure(stalled, ProviderFailure("late", retryable=False)) is False
    assert DeliveryWorker().complete_success(reclaimed) is True
    assert get_delivery(delivery).status.value == "delivered"
    assert [attempt.attempt_number for attempt in get_attempts(delivery)] == [2]


def test_provider_crash_is_retried_not_lost():
    from app.testing import create_delivery, get_attempts, get_delivery
    from app.workers.delivery_worker import DeliveryWorker

    class Exploding:
        def send(self, delivery):
            raise RuntimeError("bug in adapter")

    delivery = create_delivery(channel="email")
    worker = DeliveryWorker(email_provider=Exploding())
    claimed = next(item for item in worker.claim(batch_size=1000) if item.id == delivery)
    worker.deliver(claimed)
    assert get_delivery(delivery).status.value == "pending"
    assert get_attempts(delivery)[0].provider_message == "internal provider error"


def test_lease_budget_rejects_unsafe_configuration():
    from app.workers.delivery_worker import DeliveryWorker

    DeliveryWorker(lease_seconds=60, concurrency=10, provider_timeout_seconds=10).check_lease_budget(10)
    with pytest.raises(ValueError):
        DeliveryWorker(lease_seconds=60, concurrency=1, provider_timeout_seconds=10).check_lease_budget(25)


def test_one_bad_row_does_not_block_the_rest_of_a_batch():
    """Batch completion falls back to per-row commits when the batch transaction fails."""
    from datetime import datetime, timezone

    from app.db.models import DeliveryAttempt
    from app.db.session import SessionLocal
    from app.testing import create_delivery, get_delivery
    from app.workers.delivery_worker import DeliveryWorker, Outcome

    good, poisoned = create_delivery(channel="in_app"), create_delivery(channel="in_app")
    worker = DeliveryWorker()
    claimed = {item.id: item for item in worker.claim(batch_size=1000) if item.id in (good, poisoned)}
    # Pre-insert the attempt row the worker is about to write, forcing a unique violation.
    with SessionLocal.begin() as session:
        session.add(DeliveryAttempt(tenant_id=claimed[poisoned].tenant_id, delivery_id=poisoned, attempt_number=claimed[poisoned].attempt_count, outcome="success"))

    now = datetime.now(timezone.utc)
    results = worker.complete([Outcome(claimed[good], now, now), Outcome(claimed[poisoned], now, now)])
    assert results == [True, False]
    assert get_delivery(good).status.value == "delivered"
    assert get_delivery(poisoned).status.value == "processing"  # lease expiry will retry it


def test_delivery_that_keeps_killing_workers_is_failed_not_reclaimed_forever():
    from app.db.session import SessionLocal
    from app.testing import create_delivery, expire_lease, get_delivery
    from app.workers.delivery_worker import DeliveryWorker

    delivery = create_delivery(channel="email")
    with SessionLocal.begin() as session:
        from app.db.models import Delivery
        session.get(Delivery, delivery).max_attempts = 2
    for _ in range(2):  # two attempts, each "crashes" (never completes)
        assert delivery in [item.id for item in DeliveryWorker().claim(batch_size=1000)]
        expire_lease(delivery)

    assert delivery not in [item.id for item in DeliveryWorker().claim(batch_size=1000)]
    final = get_delivery(delivery)
    assert final.status.value == "failed" and final.last_error_code == "worker_lost"
    assert final.attempt_count == 2
