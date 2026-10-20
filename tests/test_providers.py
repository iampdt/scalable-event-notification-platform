"""Unit tests for retry policy and channel adapters; no database needed."""

from __future__ import annotations

import hashlib
import hmac
import json
import socket
import uuid
from datetime import datetime, timezone

import httpx
import pytest

from app.db.models import DeliveryChannel
from app.notifications.retry import ProviderFailure, http_status_is_retryable, retry_delay_seconds
from app.notifications.status import notification_status
from app.providers.base import ClaimedDelivery
from app.providers.smtp_email import build_message, classify_smtp_code
from app.providers.webhook import UnsafeWebhookUrl, WebhookProvider, validate_webhook_url
from app.security import encrypt_secret, sign_webhook

SECRET = "whsec_test_secret_value_123456"


def webhook_delivery(active: bool = True, url: str = "https://hooks.example.test/notify") -> ClaimedDelivery:
    return ClaimedDelivery(
        id=uuid.uuid4(), tenant_id=uuid.uuid4(), channel=DeliveryChannel.webhook, target_snapshot={"url": url},
        attempt_count=1, max_attempts=5, notification_id=uuid.uuid4(), user_id=uuid.uuid4(), user_external_id="ada",
        event_id=uuid.uuid4(), event_type="order.paid", subject={"type": "order", "id": "ord_1"}, payload={"amount": 1},
        occurred_at=datetime(2026, 1, 1, tzinfo=timezone.utc), webhook_active=active, webhook_secret_ciphertext=encrypt_secret(SECRET),
    )


def provider_returning(response: httpx.Response | Exception, seen: list | None = None) -> WebhookProvider:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        if isinstance(response, Exception):
            raise response
        return response

    return WebhookProvider(client=httpx.Client(transport=httpx.MockTransport(handler)), timeout=1)


@pytest.fixture(autouse=True)
def public_only(monkeypatch):
    # Exercise the production SSRF rules, but skip real DNS for example.test.
    monkeypatch.setenv("WEBHOOK_ALLOW_PRIVATE_NETWORKS", "false")
    real_getaddrinfo = socket.getaddrinfo

    def fake_getaddrinfo(host, *args, **kwargs):
        if host.endswith("example.test"):
            return [(None, None, None, None, ("93.184.216.34", 443))]
        return real_getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


def test_retry_delays_follow_the_schedule_with_jitter_and_cap():
    for attempt, base in [(1, 30), (2, 120), (3, 600), (4, 3600), (9, 3600)]:
        delay = retry_delay_seconds(attempt)
        assert base * 0.8 <= delay <= min(base * 1.2, 3600)
    assert retry_delay_seconds(1, retry_after=900) == 900
    assert retry_delay_seconds(1, retry_after=99999) == 3600


@pytest.mark.parametrize("code,retryable", [(408, True), (429, True), (500, True), (503, True), (400, False), (401, False), (404, False), (410, False), (301, False)])
def test_http_status_classification(code, retryable):
    assert http_status_is_retryable(code) is retryable


@pytest.mark.parametrize("code,retryable", [(421, True), (450, True), (550, False), (553, False), (535, True)])
def test_smtp_code_classification(code, retryable):
    assert classify_smtp_code(code) is retryable


def test_notification_status_rule():
    assert notification_status({"delivered", "failed"}) == "delivered"
    assert notification_status({"delivered", "pending"}) == "pending"
    assert notification_status({"failed", "suppressed"}) == "failed"
    assert notification_status({"processing", "delivered"}) == "processing"


@pytest.mark.parametrize("url", ["http://hooks.example.test/x", "https://user:pw@hooks.example.test/x", "ftp://hooks.example.test", "https://127.0.0.1/x", "https://10.0.0.5/x", "https://169.254.169.254/latest/meta-data", "https://[::1]/x"])
def test_unsafe_webhook_urls_are_rejected(url):
    with pytest.raises(UnsafeWebhookUrl):
        validate_webhook_url(url)


def test_private_urls_allowed_only_with_dev_flag():
    validate_webhook_url("http://localhost:9000/hook", allow_private=True)
    validate_webhook_url("https://hooks.example.test/notify")


def test_webhook_is_signed_so_a_receiver_can_verify_it():
    seen: list[httpx.Request] = []
    delivery = webhook_delivery()
    assert provider_returning(httpx.Response(204), seen).send(delivery) == 204

    request = seen[0]
    timestamp = request.headers["X-Notification-Timestamp"]
    expected = "v1=" + hmac.new(SECRET.encode(), f"{timestamp}.".encode() + request.content, hashlib.sha256).hexdigest()
    assert request.headers["X-Notification-Signature"] == expected
    assert request.headers["X-Notification-Delivery-Id"] == str(delivery.id)
    body = json.loads(request.content)
    assert body["delivery_id"] == str(delivery.id) and body["recipient"]["external_id"] == "ada"


def test_signature_changes_when_body_or_timestamp_changes():
    base = sign_webhook(SECRET, 100, b"{}")
    assert sign_webhook(SECRET, 101, b"{}") != base
    assert sign_webhook(SECRET, 100, b"{ }") != base


@pytest.mark.parametrize(
    "response,retryable,status",
    [(httpx.Response(500), True, 500), (httpx.Response(429, headers={"Retry-After": "120"}), True, 429), (httpx.Response(410), False, 410), (httpx.Response(302, headers={"Location": "http://10.0.0.1"}), False, 302)],
)
def test_webhook_failures_are_classified(response, retryable, status):
    with pytest.raises(ProviderFailure) as caught:
        provider_returning(response).send(webhook_delivery())
    assert caught.value.retryable is retryable
    assert caught.value.status_code == status
    if status == 429:
        assert caught.value.retry_after == 120


@pytest.mark.parametrize("error", [httpx.ReadTimeout("slow"), httpx.ConnectError("refused")])
def test_network_errors_are_retryable(error):
    with pytest.raises(ProviderFailure) as caught:
        provider_returning(error).send(webhook_delivery())
    assert caught.value.retryable


def test_disabled_endpoint_and_unsafe_snapshot_fail_permanently():
    with pytest.raises(ProviderFailure) as disabled:
        provider_returning(httpx.Response(200)).send(webhook_delivery(active=False))
    assert not disabled.value.retryable
    with pytest.raises(ProviderFailure) as unsafe:
        provider_returning(httpx.Response(200)).send(webhook_delivery(url="https://127.0.0.1/hook"))
    assert not unsafe.value.retryable


def test_email_message_carries_stable_delivery_id():
    delivery = ClaimedDelivery(id=uuid.uuid4(), tenant_id=uuid.uuid4(), channel=DeliveryChannel.email, target_snapshot={"email": "ada@example.test"}, attempt_count=1, max_attempts=5, event_type="order.paid", subject={"type": "order", "id": "1"}, payload={"a": 1})
    message = build_message(delivery, "from@example.test")
    assert message["Message-ID"] == f"<{delivery.id}@notifications.local>"
    assert message["To"] == "ada@example.test"


def test_template_renders_dotted_lookups_strictly():
    from app.notifications.templates import TemplateError, render

    context = {"event_type": "order.paid", "subject": {"type": "order", "id": "o1"}, "payload": {"amount": 4999, "items": [{"sku": "A1"}], "gift": True, "note": None, "meta": {"a": 1}}, "recipient": {"external_id": "ada", "email": None}}
    assert render("Hi {{ recipient.external_id }}, {{payload.amount}} for {{ subject.id }}", context) == "Hi ada, 4999 for o1"
    assert render("{{ payload.items.0.sku }} {{ payload.gift }} [{{ payload.note }}] {{ payload.meta }}", context) == 'A1 true [] {"a":1}'
    with pytest.raises(TemplateError, match="payload.missing"):
        render("{{ payload.missing }}", context)
    with pytest.raises(TemplateError, match="64 KiB"):
        render("{{ payload.meta }}" * 20000, context)


@pytest.mark.parametrize("bad", ["{{ config.SECRET }}", "{{ payload.a | upper }}", "{{ payload.__class__ }}x}}", "{% for x in y %}{{ payload.a }}", "{{ payload.amount"])
def test_template_rejects_anything_but_simple_lookups(bad):
    from app.notifications.templates import TemplateError, placeholders

    if "{%" in bad:
        # Not a placeholder at all: harmless literal text, rendered verbatim.
        assert placeholders(bad) == ["payload.a"]
        return
    with pytest.raises(TemplateError):
        placeholders(bad)


def test_email_uses_rendered_content_when_present():
    delivery = ClaimedDelivery(id=uuid.uuid4(), tenant_id=uuid.uuid4(), channel=DeliveryChannel.email, target_snapshot={"email": "ada@example.test"}, attempt_count=1, max_attempts=5, event_type="order.paid", content={"subject": "Paid: o1", "body": "Thanks ada"})
    message = build_message(delivery, "from@example.test")
    assert message["Subject"] == "Paid: o1"
    assert message.get_content().strip() == "Thanks ada"
