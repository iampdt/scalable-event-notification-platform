"""Structured logging.

LOG_FORMAT=json (containers) emits one JSON object per line, so a log pipeline
can filter on fields instead of regex-parsing prose; LOG_FORMAT=text (default
locally) stays human-readable. A contextvar carries the request id into every
log line written while serving that request, so all of a request's lines join
on `request_id`. High-cardinality fields (tenant_id, event_id, delivery_id)
belong here, not in metric labels.
"""
from __future__ import annotations

import contextvars
import json
import logging
import os
from datetime import datetime, timezone

request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar("request_id", default=None)

# Attributes every LogRecord has; anything else was passed via `extra=` and is a field.
_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        request_id = request_id_var.get()
        if request_id:
            entry["request_id"] = request_id
        for key, value in vars(record).items():
            if key not in _STANDARD and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(service: str) -> None:
    """Idempotent; call once at process start (API factory, worker main)."""
    root = logging.getLogger()
    if getattr(root, "_configured_by_app", False):
        return
    handler = logging.StreamHandler()
    if os.getenv("LOG_FORMAT", "text") == "json":
        handler.setFormatter(JsonFormatter(service))
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.handlers = [handler]
    root.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    root._configured_by_app = True  # type: ignore[attr-defined]
