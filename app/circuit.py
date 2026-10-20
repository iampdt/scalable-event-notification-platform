"""Circuit breakers for delivery targets, shared by all workers through Redis.

Without a breaker, a dead webhook endpoint costs every delivery to it a full
timeout (10 s of a worker thread) on each of its 5 attempts, starving other
tenants' deliveries that share the batch. With one:

  closed --N consecutive health failures--> open --cooldown--> half-open
     ^                                                         | one probe
     +---------------- probe succeeds --------------------------+
                       probe fails -> open again, cooldown doubled (capped)

- Scope is one webhook endpoint or one email provider, so one tenant's broken
  endpoint never trips anyone else's (failure isolation).
- Only "is the target healthy?" failures count: timeouts, connection errors,
  5xx, 429. A 4xx means the endpoint answered; that is the message's problem.
- While open, deliveries are *deferred* without spending a retry attempt.
- State lives in Redis so every worker sees the same circuit; each transition is
  one Lua script on Redis time. No Redis = no breaker (fail open), never errors.
- Only the half-open probe may close or reopen the circuit; late results from
  requests that were already in flight when it opened are ignored.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from app.cache import get_cache

BEFORE_LUA = """
local open_key, probe_key = KEYS[1], KEYS[2]
local probe_ttl = tonumber(ARGV[1])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local open_until = tonumber(redis.call('GET', open_key) or '0')
if open_until == 0 then return {1, 0} end
if now < open_until then return {0, open_until - now} end
if redis.call('SET', probe_key, '1', 'NX', 'PX', probe_ttl) then return {2, 0} end
return {0, probe_ttl}
"""

AFTER_LUA = """
local open_key, probe_key, fail_key, cool_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local healthy, is_probe = ARGV[1] == '1', ARGV[2] == '1'
local threshold, base_cool, max_cool, window = tonumber(ARGV[3]), tonumber(ARGV[4]), tonumber(ARGV[5]), tonumber(ARGV[6])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local is_open = redis.call('EXISTS', open_key) == 1
if is_open and not is_probe then return {0} end
if healthy then
  redis.call('DEL', open_key, probe_key, fail_key, cool_key)
  return {is_open and 2 or 0}
end
local cool
if is_open then
  cool = math.min(tonumber(redis.call('GET', cool_key) or base_cool) * 2, max_cool)
else
  local failures = redis.call('INCR', fail_key)
  redis.call('PEXPIRE', fail_key, window)
  if failures < threshold then return {0} end
  cool = base_cool
end
redis.call('SET', cool_key, cool, 'PX', max_cool * 10)
redis.call('SET', open_key, now + cool, 'PX', cool + max_cool)
redis.call('DEL', probe_key, fail_key)
return {1, cool}
"""


@dataclass(frozen=True)
class Permit:
    allowed: bool
    probe: bool = False
    retry_after_seconds: float = 0.0


ALLOW = Permit(True)


def settings() -> dict:
    return {
        "threshold": int(os.getenv("CIRCUIT_FAILURE_THRESHOLD", "5")),
        "cooldown_ms": int(float(os.getenv("CIRCUIT_COOLDOWN_SECONDS", "30")) * 1000),
        "max_cooldown_ms": int(float(os.getenv("CIRCUIT_MAX_COOLDOWN_SECONDS", "300")) * 1000),
        "window_ms": 60_000,
        "probe_ttl_ms": 30_000,  # > provider timeout: a probe that dies frees the slot
    }


def keys(scope: str) -> list[str]:
    return [f"circuit:{scope}:open_until", f"circuit:{scope}:probe", f"circuit:{scope}:failures", f"circuit:{scope}:cooldown"]


def before(scope: str) -> Permit:
    result = get_cache().call("circuit", lambda r: r.eval(BEFORE_LUA, 2, *keys(scope)[:2], settings()["probe_ttl_ms"]))
    if result is None:  # Redis disabled or down: no breaker
        return ALLOW
    state, wait_ms = int(result[0]), int(result[1])
    if state == 0:
        return Permit(False, retry_after_seconds=wait_ms / 1000)
    return Permit(True, probe=state == 2)


def after(scope: str, permit: Permit, healthy: bool) -> str | None:
    """Record a result. Returns "opened" / "closed" on a transition, else None."""
    s = settings()
    result = get_cache().call(
        "circuit",
        lambda r: r.eval(AFTER_LUA, 4, *keys(scope), "1" if healthy else "0", "1" if permit.probe else "0", s["threshold"], s["cooldown_ms"], s["max_cooldown_ms"], s["window_ms"]),
    )
    if not result:
        return None
    return {1: "opened", 2: "closed"}.get(int(result[0]))
