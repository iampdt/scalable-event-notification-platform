"""Count SQL round trips per API request (deterministic, unlike laptop latency).

    TEST_DATABASE_URL=... python -m benchmarks.query_count

Uses a fresh throwaway tenant. Prints statements for: a warm authenticated GET,
and a POST /v1/events with an email template, both after one warm-up request.
"""
from __future__ import annotations

import os
import uuid

from sqlalchemy import event

os.environ["DATABASE_URL"] = os.environ.get("TEST_DATABASE_URL", os.environ.get("DATABASE_URL", ""))
os.environ.setdefault("EMAIL_PROVIDER", "fake")

from fastapi.testclient import TestClient  # noqa: E402

from app.db.models import ApiKeyRole  # noqa: E402
from app.db.session import engine  # noqa: E402
from app.main import create_app  # noqa: E402
from app.testing import create_api_key, create_tenant_with_key  # noqa: E402


def main() -> None:
    tenant = create_tenant_with_key(f"qc-{uuid.uuid4().hex[:10]}")
    admin = create_api_key(tenant.tenant_id, ApiKeyRole.admin, "admin")
    client = TestClient(create_app())
    producer = {"Authorization": f"Bearer {tenant.key}"}
    client.post("/v1/templates", json={"event_type": "order.paid", "channel": "email", "subject": "Paid {{ subject.id }}", "body": "{{ payload.amount }}"}, headers={"Authorization": f"Bearer {admin}"})

    statements: list[str] = []
    event.listen(engine, "before_cursor_execute", lambda conn, cursor, statement, *args: statements.append(statement.split()[0] + " " + " ".join(statement.split()[1:6])))

    def measure(label: str, call) -> None:
        call()  # warm-up (fills any cache)
        statements.clear()
        call()
        print(f"{label}: {len(statements)} statements")
        for statement in statements:
            print("   ", statement[:110])

    body = lambda i: {"event_type": "order.paid", "subject": {"type": "order", "id": str(i)}, "payload": {"amount": 1}, "recipients": [{"user_id": str(tenant.user_id), "channels": ["email", "in_app"]}]}
    counter = iter(range(10**6))
    measure("GET /v1/events/{random}", lambda: client.get(f"/v1/events/{uuid.uuid4()}", headers=producer))
    measure("POST /v1/events (new key, template)", lambda: client.post("/v1/events", json=body(next(counter)), headers={**producer, "Idempotency-Key": f"qc-{uuid.uuid4()}"}))


if __name__ == "__main__":
    main()
