# Version 0 API Contract

## Conventions

- Base path: `/v1`; deployed transport is HTTPS; bodies are JSON.
- Platform IDs are UUID strings; timestamps are ISO 8601 UTC values.
- Authenticate with `Authorization: Bearer ntf_live_<secret>`. A credential maps
  to one tenant; clients never submit `tenant_id`.
- Write endpoints reject unknown fields. Initial limits are 64 KiB payloads and
  100 recipients per event.
- List results are newest-first and cursor-paginated. Cursors are opaque.

## Error envelope

```json
{
  "error": {
    "code": "validation_error",
    "message": "One or more fields are invalid.",
    "details": [{"field": "recipients[0].user_id", "issue": "must be a UUID"}],
    "request_id": "018f3ec0-5d61-7b55-a3ba-c2a50dc1f804"
  }
}
```

| Status | Code | Meaning |
| --- | --- | --- |
| 400 | `validation_error` | Invalid JSON or request shape. |
| 401 | `unauthorized` | Missing, invalid, expired, or disabled credential. |
| 403 | `forbidden` | Credential lacks the required role. |
| 404 | `not_found` | Resource is absent in this tenant. |
| 409 | `idempotency_conflict` | Key was reused with different normalized content. |
| 413 | `payload_too_large` | Body or payload exceeded a limit. |
| 422 | `unprocessable` | JSON is valid but violates a domain rule. |
| 429 | `rate_limited` | Future quota control; not implemented in V0.1. |
| 500 | `internal_error` | Unexpected error; retry only with the same idempotency key. |

Tenant isolation intentionally returns `404` rather than exposing whether another
tenant owns a supplied identifier.

## Ingest event

`POST /v1/events`

Required headers:

```http
Authorization: Bearer ntf_live_<secret>
Idempotency-Key: order-123-paid-v1
Content-Type: application/json
```

`Idempotency-Key` is 1–128 visible ASCII characters and unique per tenant. The
platform hashes the normalized request. Repeating the same request returns the
original event; using the key with changed content returns `409`.

```json
{
  "event_type": "order.paid",
  "subject": {"type": "order", "id": "ord_123"},
  "payload": {"order_id": "ord_123", "amount": 4999, "currency": "INR"},
  "recipients": [
    {
      "user_id": "4b19534d-2440-4b73-889b-9f1d01c477aa",
      "channels": ["email", "in_app"]
    }
  ],
  "metadata": {"source": "checkout-api", "correlation_id": "req_9aa3"}
}
```

| Field | Rules |
| --- | --- |
| `event_type` | Required dotted identifier, 1–100 characters; e.g. `order.paid`. |
| `subject` | Required object; `type` and `id` are each 1–100 characters. |
| `payload` | Required JSON object, maximum 64 KiB, and must not contain secrets. |
| `recipients` | Required non-empty array of 1–100 distinct active tenant user UUIDs. |
| `channels` | Non-empty unique values from `email`, `webhook`, `in_app`. A missing usable target produces a recorded suppression; it is never redirected. |
| `metadata` | Optional JSON object, maximum 8 KiB, for tracing only—not authorization or routing. |

Response: `202 Accepted`

```json
{
  "id": "76db480d-5d35-4c39-92fa-a9d8cc74be8c",
  "event_type": "order.paid",
  "status": "accepted",
  "created_at": "2026-09-14T12:30:00Z",
  "notification_count": 1,
  "delivery_count": 2,
  "links": {"self": "/v1/events/76db480d-5d35-4c39-92fa-a9d8cc74be8c"}
}
```

`202` means the event and initial delivery work are durably committed. It does not
mean a provider or recipient has accepted anything.

## Read event status

`GET /v1/events/{event_id}`

```json
{
  "id": "76db480d-5d35-4c39-92fa-a9d8cc74be8c",
  "event_type": "order.paid",
  "subject": {"type": "order", "id": "ord_123"},
  "payload": {"order_id": "ord_123", "amount": 4999, "currency": "INR"},
  "metadata": {"source": "checkout-api", "correlation_id": "req_9aa3"},
  "status": "accepted",
  "created_at": "2026-09-14T12:30:00Z",
  "deliveries": {"total": 2, "pending": 1, "processing": 0, "delivered": 1, "failed": 0, "suppressed": 0}
}
```

## Notifications

`GET /v1/notifications?status=pending&limit=50&cursor=<opaque-cursor>`

`status` is optional: `pending`, `processing`, `delivered`, `failed`, or
`suppressed`. `limit` defaults to 50 and is at most 100.

```json
{
  "data": [
    {
      "id": "d1e58638-6f17-4d24-baaa-31f6948a4c21",
      "event_id": "76db480d-5d35-4c39-92fa-a9d8cc74be8c",
      "user_id": "4b19534d-2440-4b73-889b-9f1d01c477aa",
      "status": "pending",
      "created_at": "2026-09-14T12:30:00Z"
    }
  ],
  "next_cursor": null
}
```

`GET /v1/notifications/{notification_id}` returns the recipient, event ID, and
every channel delivery. A notification is `delivered` when at least one channel
is delivered and no work remains; it is `failed` when every requested channel is
failed or suppressed; otherwise it remains pending or processing.

```json
{
  "id": "d1e58638-6f17-4d24-baaa-31f6948a4c21",
  "event_id": "76db480d-5d35-4c39-92fa-a9d8cc74be8c",
  "user_id": "4b19534d-2440-4b73-889b-9f1d01c477aa",
  "status": "pending",
  "deliveries": [
    {"id": "82017374-5ebd-47f2-9447-ffd6229fb05e", "channel": "email", "status": "delivered", "attempt_count": 1, "delivered_at": "2026-09-14T12:30:05Z"},
    {"id": "17d414a6-ec52-4375-95ff-28a299e7411f", "channel": "in_app", "status": "pending", "attempt_count": 0, "next_attempt_at": "2026-09-14T12:30:00Z"}
  ]
}
```

## In-app feed

`GET /v1/users/{user_id}/in-app-notifications?unread_only=true&limit=50&cursor=<opaque-cursor>`

Returns only `in_app` deliveries in the authenticated tenant. V0 authenticates a
tenant rather than an individual end user, so the tenant-facing application must
enforce whether its caller may read this `user_id`.

```json
{
  "data": [
    {
      "delivery_id": "17d414a6-ec52-4375-95ff-28a299e7411f",
      "notification_id": "d1e58638-6f17-4d24-baaa-31f6948a4c21",
      "event_type": "order.paid",
      "payload": {"order_id": "ord_123", "amount": 4999, "currency": "INR"},
      "read_at": null,
      "created_at": "2026-09-14T12:30:00Z"
    }
  ],
  "next_cursor": null
}
```

`POST /v1/in-app-notifications/{delivery_id}/read` is idempotent and returns the
in-app delivery with its `read_at` timestamp; a non-in-app or cross-tenant ID is
`404`.

## Tenant administration

The first implementation needs configuration endpoints. They require an `admin`
credential; initial tenant/key bootstrap belongs to deployment, not this API.

| Endpoint | Purpose | Key fields |
| --- | --- | --- |
| `POST /v1/users` | Register or update a recipient. | `external_id`, optional `email`, `active` |
| `POST /v1/webhook-endpoints` | Register outbound destination. | `url`, `secret`, `active` |
| `GET /v1/webhook-endpoints` | List destinations without secret material. | Pagination fields |

Webhook body:

```json
{
  "delivery_id": "82017374-5ebd-47f2-9447-ffd6229fb05e",
  "event_id": "76db480d-5d35-4c39-92fa-a9d8cc74be8c",
  "event_type": "order.paid",
  "subject": {"type": "order", "id": "ord_123"},
  "payload": {"order_id": "ord_123", "amount": 4999, "currency": "INR"},
  "occurred_at": "2026-09-14T12:30:00Z"
}
```

Headers include `X-Notification-Delivery-Id`, `X-Notification-Timestamp`, and
`X-Notification-Signature: v1=<hex-hmac-sha256>`. The HMAC covers the timestamp,
a period, and exact body. Receivers should reject stale timestamps and deduplicate
delivery IDs.

## Trade-offs

- Explicit recipients make V0 fan-out understandable; subscription rules are
  deferred.
- `202` makes asynchronous delivery semantics clear even though data is committed.
- Status reads arrive before callback features so producers can recover from
  uncertainty without coupling to provider details.
- Provider response IDs and diagnostic bodies remain internal to avoid leaking
  sensitive data and binding consumers to a provider.
