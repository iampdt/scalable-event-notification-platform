"""Dead-letter replay ("redrive").

A delivery that exhausted its retries or failed permanently stays in
`deliveries` with status `failed` and its full attempt history: that table *is*
the dead-letter queue. Redrive reopens it:

- status -> pending, due now, with `extra_attempts` more tries
  (max_attempts = attempt_count + extra), so attempt numbers keep increasing
  and history is never rewritten;
- one `delivery_redrives` audit row per delivery (who, why, prior state);
- in Kafka mode, an outbox row in the same transaction.

Rows are locked with SKIP LOCKED, so two operators redriving at once never
double-enqueue the same delivery.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import insert, text
from sqlalchemy.orm import Session

from app.config import dispatch_mode
from app.db.models import DeliveryRedrive, OutboxMessage
from app.messaging import delivery_message

MAX_BULK_REDRIVE = 1000


@dataclass(frozen=True)
class RedriveFilter:
    delivery_ids: list[uuid.UUID] | None = None
    channel: str | None = None
    error_code: str | None = None
    limit: int = 100


def redrive(session: Session, tenant_id: uuid.UUID, api_key_id: uuid.UUID, flt: RedriveFilter, extra_attempts: int = 3, reason: str | None = None) -> list[uuid.UUID]:
    """Reopen matching failed deliveries. Caller owns the transaction."""
    conditions = ["tenant_id = :tenant_id", "status = 'failed'"]
    params: dict = {"tenant_id": tenant_id, "extra": extra_attempts, "limit": min(flt.limit, MAX_BULK_REDRIVE)}
    if flt.delivery_ids is not None:
        conditions.append("id = ANY(:ids)")
        params["ids"] = flt.delivery_ids
    if flt.channel:
        conditions.append("channel::text = :channel")
        params["channel"] = flt.channel
    if flt.error_code:
        conditions.append("last_error_code = :error_code")
        params["error_code"] = flt.error_code
    # Conditions are fixed strings chosen above; all values are bound parameters.
    rows = session.execute(
        text(
            f"""
            WITH target AS (
                SELECT id, attempt_count, last_error_code FROM deliveries
                WHERE {' AND '.join(conditions)}
                ORDER BY failed_at DESC, id DESC
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            )
            UPDATE deliveries d
            SET status = 'pending', failed_at = NULL, lease_expires_at = NULL,
                next_attempt_at = now(), max_attempts = d.attempt_count + :extra, updated_at = now()
            FROM target
            WHERE d.id = target.id
            RETURNING d.id, target.attempt_count AS previous_attempt_count, target.last_error_code AS previous_error_code
            """
        ),
        params,
    ).all()
    if not rows:
        return []
    session.execute(
        insert(DeliveryRedrive),
        [{"tenant_id": tenant_id, "delivery_id": row.id, "api_key_id": api_key_id, "reason": reason, "previous_attempt_count": row.previous_attempt_count, "previous_error_code": row.previous_error_code, "extra_attempts": extra_attempts} for row in rows],
    )
    if dispatch_mode() == "kafka":
        session.execute(insert(OutboxMessage), [delivery_message(row.id, tenant_id) for row in rows])
    return [row.id for row in rows]
