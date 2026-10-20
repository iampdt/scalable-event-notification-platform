"""Opaque keyset cursors over (created_at, id), newest first.

Keyset instead of OFFSET: OFFSET re-scans every skipped row and shifts when new
rows arrive. The id tie-breaker matters because every row written in one
ingestion transaction shares the same created_at (now() is transaction time).
"""
from __future__ import annotations

import base64
import json
import uuid
from datetime import datetime

from sqlalchemy import tuple_

from app.api.errors import ApiError


def encode_cursor(created_at: datetime, row_id: uuid.UUID) -> str:
    raw = json.dumps({"t": created_at.isoformat(), "id": str(row_id)}, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        data = json.loads(raw)
        return datetime.fromisoformat(data["t"]), uuid.UUID(data["id"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ApiError(400, "validation_error", "cursor is invalid.", [{"field": "cursor", "issue": "malformed cursor"}]) from exc


def apply_cursor(query, created_column, id_column, cursor: str | None):
    if cursor:
        created_at, row_id = decode_cursor(cursor)
        query = query.where(tuple_(created_column, id_column) < tuple_(created_at, row_id))
    return query.order_by(created_column.desc(), id_column.desc())


def page(rows: list, limit: int, created_at_of, id_of) -> tuple[list, str | None]:
    """Callers fetch limit + 1 rows; the extra row only signals that another page exists."""
    if len(rows) <= limit:
        return rows, None
    rows = rows[:limit]
    return rows, encode_cursor(created_at_of(rows[-1]), id_of(rows[-1]))
