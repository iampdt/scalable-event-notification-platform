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
