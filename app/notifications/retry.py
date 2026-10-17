"""Provider failure classification and bounded retry timing."""
from __future__ import annotations

import random


class ProviderFailure(Exception):
    def __init__(self, message: str, *, retryable: bool, status_code: int | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.status_code = status_code


def retry_delay_seconds(attempt_number: int) -> float:
    base = (30, 120, 600, 3600)[min(max(attempt_number - 1, 0), 3)]
    return base * random.uniform(0.8, 1.2)
