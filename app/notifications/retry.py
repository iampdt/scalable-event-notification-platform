"""Provider failure classification and bounded retry timing."""
from __future__ import annotations

import random

MAX_RETRY_DELAY_SECONDS = 3600


class ProviderFailure(Exception):
    def __init__(self, message: str, *, retryable: bool, status_code: int | None = None, retry_after: float | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code
        self.retry_after = retry_after


def retry_delay_seconds(attempt_number: int, retry_after: float | None = None) -> float:
    """30s, 2m, 10m, then hourly, with ±20% jitter so retries from one outage spread out.

    A provider's Retry-After is honoured when it asks for longer than our own
    backoff, but never beyond the hourly cap.
    """
    base = (30, 120, 600, 3600)[min(max(attempt_number - 1, 0), 3)]
    delay = base * random.uniform(0.8, 1.2)
    if retry_after is not None:
        delay = max(delay, retry_after)
    return min(delay, MAX_RETRY_DELAY_SECONDS)


def http_status_is_retryable(status_code: int) -> bool:
    """408/429/5xx mean "try later"; other 4xx (and unfollowed 3xx) won't fix themselves."""
    return status_code in (408, 429) or status_code >= 500


def parse_retry_after(value: str | None) -> float | None:
    # Only the delta-seconds form; the HTTP-date form is rare for webhooks.
    if not value:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        return None
