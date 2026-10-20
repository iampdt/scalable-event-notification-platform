"""Generate the Grafana dashboard JSON (dashboards as code: edit here, not in the UI).

    python observability/build_dashboard.py
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "grafana" / "dashboards" / "notification-platform.json"
DS = {"type": "prometheus", "uid": "prometheus"}


def q(expr: str, legend: str) -> dict:
    return {"datasource": DS, "expr": expr, "legendFormat": legend}


def panel(title: str, targets: list[dict], unit: str = "short", description: str = "", stack: bool = False) -> dict:
    return {
        "type": "timeseries",
        "title": title,
        "description": description,
        "datasource": DS,
        "targets": targets,
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"stacking": {"mode": "normal" if stack else "none"}, "fillOpacity": 15 if stack else 5}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
    }


def quantiles(metric: str, selector: str, by: str = "le") -> list[dict]:
    return [q(f"histogram_quantile({p}, sum by ({by}) (rate({metric}_bucket{{{selector}}}[1m])))", f"p{int(p * 100)}") for p in (0.5, 0.95, 0.99)]


ROWS: list[tuple[str, list[dict]]] = [
    ("API — rate, errors, duration", [
        panel("Requests/s by status", [q("sum by (status) (rate(http_requests_total[1m]))", "{{status}}")], "reqps", stack=True),
        panel("5xx error ratio", [q('(sum(rate(http_requests_total{status=~"5.."}[1m])) or vector(0)) / clamp_min(sum(rate(http_requests_total[1m])), 1e-9)', "5xx ratio")], "percentunit"),
        panel("POST /v1/events latency", quantiles("http_request_duration_seconds", 'method="POST", route="/v1/events"'), "s", "Target: p95 < 250 ms"),
    ]),
    ("Ingestion", [
        panel("Events/s by result", [q("sum by (result) (rate(events_ingested_total[1m]))", "{{result}}")], "ops", stack=True),
        panel("Deliveries planned/s", [q("sum by (channel, status) (rate(deliveries_planned_total[1m]))", "{{channel}} {{status}}")], "ops", stack=True),
        panel("Rate-limited requests/s", [q("sum(rate(rate_limited_total[1m]))", "429s")], "reqps"),
    ]),
    ("Delivery pipeline", [
        panel("Attempts/s by outcome", [q("sum by (outcome) (rate(delivery_attempts_total[1m]))", "{{outcome}}")], "ops", stack=True),
        panel("End-to-end latency (accepted → delivered)", quantiles("delivery_end_to_end_seconds", ""), "s"),
        panel("Queue depth", [q("deliveries_queue_depth", "{{state}}")], "short", "From PostgreSQL at scrape time"),
        panel("Oldest due delivery age", [q("deliveries_oldest_due_seconds", "oldest due")], "s", "Alert: > 60 s for 2 min"),
        panel("Provider latency p95 by channel", [q("histogram_quantile(0.95, sum by (le, channel) (rate(provider_send_duration_seconds_bucket[1m])))", "{{channel}}")], "s"),
        panel("Batch size p50 by mode", [q("histogram_quantile(0.5, sum by (le, mode) (rate(worker_batch_size_bucket[1m])))", "{{mode}}")], "short"),
    ]),
    ("Reliability", [
        panel("Dead-lettered/s by reason", [q("sum by (channel, reason) (rate(deliveries_dead_lettered_total[5m]))", "{{channel}} {{reason}}")], "ops"),
        panel("Dead-letter queue size", [q("deliveries_dead_letters", "failed deliveries")], "short"),
        panel("Circuit breakers", [q("sum by (channel, transition) (increase(circuit_transitions_total[5m]))", "{{channel}} {{transition}}"), q("sum by (channel) (rate(deliveries_deferred_total[1m]))", "deferred/s {{channel}}")], "short"),
        panel("Leases reclaimed / stale completions", [q("sum(increase(leases_reclaimed_total[5m]))", "leases reclaimed"), q("sum(increase(stale_completions_total[5m]))", "stale completions")], "short"),
    ]),
    ("Kafka", [
        panel("Outbox", [q("outbox_depth", "depth"), q("outbox_failing_rows", "failing rows")], "short"),
        panel("Outbox publish rate", [q("sum(rate(outbox_published_total[1m]))", "published/s"), q("sum(rate(outbox_publish_failures_total[1m]))", "failures/s")], "ops"),
        panel("Consumer lag by partition", [q("max by (partition) (kafka_consumer_lag)", "p{{partition}}")], "short", stack=True),
        panel("Messages consumed/s", [q("sum by (result) (rate(kafka_messages_consumed_total[1m]))", "{{result}}")], "ops"),
    ]),
    ("Redis", [
        panel("Cache hit ratio by cache", [q('sum by (cache) (rate(cache_requests_total{result="hit"}[5m])) / clamp_min(sum by (cache) (rate(cache_requests_total[5m])), 1e-9)', "{{cache}}")], "percentunit", "Per cache; Redis INFO mixes in rate-limiter keys"),
        panel("Cache lookups/s by result", [q("sum by (cache, result) (rate(cache_requests_total[1m]))", "{{cache}} {{result}}")], "ops", stack=True),
        panel("Redis outages", [q("sum(increase(redis_unavailable_total[5m]))", "breaker opened")], "short"),
    ]),
]


def build() -> dict:
    panels, y, next_id = [], 0, 1
    for title, row in ROWS:
        panels.append({"type": "row", "title": title, "id": next_id, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
        next_id, y = next_id + 1, y + 1
        width = 24 // min(len(row), 3)
        for index, p in enumerate(row):
            p["id"] = next_id
            p["gridPos"] = {"x": (index % 3) * width, "y": y + (index // 3) * 8, "w": width, "h": 8}
            panels.append(p)
            next_id += 1
        y += 8 * ((len(row) + 2) // 3)
    return {
        "uid": "notification-platform",
        "title": "Notification Platform",
        "tags": ["notifications"],
        "timezone": "browser",
        "schemaVersion": 39,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "panels": panels,
    }


if __name__ == "__main__":
    OUT.write_text(json.dumps(build(), indent=2) + "\n")
    print(f"wrote {OUT} ({sum(1 for p in build()['panels'] if p['type'] != 'row')} panels)")
