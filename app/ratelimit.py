"""Per-tenant ingestion rate limiting with GCRA in Redis.

GCRA (generic cell rate algorithm) behaves like a token bucket -- `burst`
requests at once, then `rate` per second -- but stores a single number per
tenant: the "theoretical arrival time" (TAT) of the next request. No refill
loop, no background job.

- One Lua script = one atomic round trip, so concurrent API processes cannot
  race between reading and writing the tenant's state.
- Time comes from Redis (`TIME`), not each API host, so clock skew between
  hosts cannot hand a tenant extra capacity (same reasoning as using the
  database clock for leases).
- If Redis is unavailable the request is allowed (fail open): rate limiting
  protects fairness, and losing it briefly is better than rejecting all traffic.
"""
from __future__ import annotations

import math
import uuid
from dataclasses import dataclass

from app.cache import get_cache
from app.config import rate_limit_defaults

GCRA_LUA = """
local key = KEYS[1]
local interval = tonumber(ARGV[1])          -- microseconds between requests at the sustained rate
local burst_window = tonumber(ARGV[2])      -- interval * burst
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local tat = tonumber(redis.call('GET', key) or now)
if tat < now then tat = now end
local new_tat = tat + interval
local allow_at = new_tat - burst_window
if allow_at > now then
  return {0, allow_at - now, 0}
end
redis.call('SET', key, new_tat, 'PX', math.ceil((new_tat - now) / 1000) + 1)
local remaining = math.floor((burst_window - (new_tat - now)) / interval)
return {1, 0, remaining}
"""


@dataclass(frozen=True)
class Decision:
    allowed: bool
    retry_after_seconds: float
    remaining: int | None
    limit: int


def check(tenant_id: uuid.UUID, rate_per_second: float | None = None, burst: int | None = None) -> Decision:
    default_rate, default_burst = rate_limit_defaults()
    rate = rate_per_second or default_rate
    burst = burst or default_burst
    interval = int(1_000_000 / rate)
    cache = get_cache()
    result = cache.call(
        "ratelimit",
        lambda r: r.eval(GCRA_LUA, 1, f"ratelimit:ingest:{tenant_id}", interval, interval * burst),
    )
    if result is None:  # Redis disabled or down: fail open
        return Decision(True, 0.0, None, burst)
    allowed, retry_after_us, remaining = (int(value) for value in result)
    return Decision(bool(allowed), math.ceil(retry_after_us / 1000) / 1000, remaining if allowed else 0, burst)
