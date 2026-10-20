"""Outbound webhook adapter: HMAC-signed POST to the tenant's endpoint."""
from __future__ import annotations

import ipaddress
import json
import socket
import time
from urllib.parse import urlsplit

import httpx

from app.config import webhook_allow_private_networks, webhook_timeout_seconds
from app.notifications.retry import ProviderFailure, http_status_is_retryable, parse_retry_after
from app.providers.base import ClaimedDelivery
from app.security import decrypt_secret, sign_webhook


class UnsafeWebhookUrl(ValueError):
    pass


def validate_webhook_url(url: str, *, allow_private: bool | None = None) -> None:
    """Reject URLs that would let a tenant aim our worker at internal infrastructure (SSRF).

    Checked at registration and again before every send, because DNS can change.
    A resolver that answers differently between this check and the connect
    (DNS rebinding) is still possible; pinning the resolved IP is the next step.
    """
    allow_private = webhook_allow_private_networks() if allow_private is None else allow_private
    parts = urlsplit(url)
    allowed_schemes = {"https", "http"} if allow_private else {"https"}
    if parts.scheme not in allowed_schemes or not parts.hostname:
        raise UnsafeWebhookUrl("webhook URL must be an absolute https:// URL")
    if parts.username or parts.password:
        raise UnsafeWebhookUrl("webhook URL must not contain credentials")
    if allow_private:
        return
    try:
        infos = socket.getaddrinfo(parts.hostname, parts.port or 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UnsafeWebhookUrl("webhook host does not resolve") from exc
    for info in infos:
        address = ipaddress.ip_address(info[4][0])
        if not address.is_global:
            raise UnsafeWebhookUrl("webhook host resolves to a private or reserved address")


def webhook_body(delivery: ClaimedDelivery) -> bytes:
    body = {
        "delivery_id": str(delivery.id),
        "event_id": str(delivery.event_id),
        "notification_id": str(delivery.notification_id),
        "recipient": {"user_id": str(delivery.user_id), "external_id": delivery.user_external_id},
        "event_type": delivery.event_type,
        "subject": delivery.subject,
        "payload": delivery.payload,
        "occurred_at": delivery.occurred_at.isoformat() if delivery.occurred_at else None,
    }
    return json.dumps(body, separators=(",", ":"), sort_keys=True).encode()


class WebhookProvider:
    def __init__(self, client: httpx.Client | None = None, timeout: float | None = None):
        self.timeout = timeout if timeout is not None else webhook_timeout_seconds()
        # Redirects are not followed: a 3xx could bounce the request to an internal host.
        self.client = client or httpx.Client(timeout=self.timeout, follow_redirects=False)

    def send(self, delivery: ClaimedDelivery) -> int | None:
        if not delivery.webhook_active or delivery.webhook_secret_ciphertext is None:
            raise ProviderFailure("webhook endpoint is disabled", retryable=False)
        url = delivery.target_snapshot.get("url", "")
        try:
            validate_webhook_url(url)
        except UnsafeWebhookUrl as exc:
            raise ProviderFailure(str(exc), retryable=False) from exc

        body = webhook_body(delivery)
        timestamp = int(time.time())
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "notification-platform-webhooks/1",
            "X-Notification-Delivery-Id": str(delivery.id),
            "X-Notification-Timestamp": str(timestamp),
            "X-Notification-Signature": sign_webhook(decrypt_secret(delivery.webhook_secret_ciphertext), timestamp, body),
        }
        try:
            response = self.client.post(url, content=body, headers=headers)
        except httpx.TimeoutException as exc:
            raise ProviderFailure("webhook request timed out", retryable=True) from exc
        except httpx.HTTPError as exc:
            raise ProviderFailure(f"webhook connection failed: {type(exc).__name__}", retryable=True) from exc

        if 200 <= response.status_code < 300:
            return response.status_code
        # Only the status is stored; receiver response bodies may contain anything.
        raise ProviderFailure(
            f"webhook responded HTTP {response.status_code}",
            retryable=http_status_is_retryable(response.status_code),
            status_code=response.status_code,
            retry_after=parse_retry_after(response.headers.get("Retry-After")),
        )
