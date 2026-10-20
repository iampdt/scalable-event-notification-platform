"""Redis access that can never take the platform down.

PostgreSQL is the source of truth; Redis only makes things faster (cache) or
fairer (rate limits). So every Redis failure degrades to "no cache / no limit"
instead of an error:

- Tight socket timeouts (50 ms) bound how much latency a sick Redis can add.
- A small circuit breaker: after a failure, skip Redis entirely for a cool-off
  period, so an outage costs one timeout per process, not one per request.
"""
from __future__ import annotations

import json
import logging
import threading
import time

import redis

from app.config import redis_url

logger = logging.getLogger(__name__)

COOL_OFF_SECONDS = 5.0


class Cache:
    def __init__(self, url: str | None, timeout: float = 0.05):
        self.client = redis.Redis.from_url(url, socket_timeout=timeout, socket_connect_timeout=timeout, decode_responses=True) if url else None
        self._down_until = 0.0
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self.client is not None and time.monotonic() >= self._down_until

    def _failed(self, operation: str, exc: Exception) -> None:
        with self._lock:
            already_down = time.monotonic() < self._down_until
            self._down_until = time.monotonic() + COOL_OFF_SECONDS
        if not already_down:
            logger.warning("redis_unavailable operation=%s error=%s; bypassing for %ss", operation, type(exc).__name__, COOL_OFF_SECONDS)

    def call(self, operation: str, fn, default=None):
        """Run fn(client); on any Redis error, open the breaker and return default."""
        if not self.enabled:
            return default
        try:
            return fn(self.client)
        except redis.RedisError as exc:
            self._failed(operation, exc)
            return default

    def get_json(self, key: str):
        raw = self.call("get", lambda r: r.get(key))
        return json.loads(raw) if raw is not None else None

    def set_json(self, key: str, value, ttl: int) -> None:
        self.call("set", lambda r: r.set(key, json.dumps(value, separators=(",", ":")), ex=ttl))

    def delete(self, *keys: str) -> None:
        self.call("delete", lambda r: r.delete(*keys))


_cache: Cache | None = None
_cache_lock = threading.Lock()


def get_cache() -> Cache:
    """Process-wide client (redis-py pools connections and is thread-safe)."""
    global _cache
    if _cache is None:
        with _cache_lock:
            if _cache is None:
                _cache = Cache(redis_url())
    return _cache


def reset_cache() -> None:
    """Tests: re-read REDIS_URL."""
    global _cache
    _cache = None
