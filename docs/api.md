# API Contract

Originally written as the V0 design contract. It is now kept in sync with the
implemented V1.x API; "Implementation note" lines mark where the build deviated
from the original design and why.

| Endpoint | Role | Status |
| --- | --- | --- |
| `POST /v1/events` | producer or admin | Implemented |
| `GET /v1/events/{id}` | any | Implemented |
| `GET /v1/notifications` | any | Implemented |
| `GET /v1/notifications/{id}` | any | Implemented |
| `GET /v1/users/{user_id}/in-app-notifications` | any | Implemented |
| `POST /v1/in-app-notifications/{delivery_id}/read` | any | Implemented |
| `POST /v1/users` | admin | Implemented |
| `POST /v1/templates`, `GET /v1/templates`, `GET /v1/templates/{id}` | admin | Implemented |
| `POST/GET /v1/webhook-endpoints`, `PATCH /v1/webhook-endpoints/{id}` | admin | Implemented |
| `GET /healthz` | none | Implemented (liveness only; not in OpenAPI schema) |

Interactive docs are served by FastAPI at `/docs` while the API is running.

## Conventions

- Base path: `/v1`; deployed transport is HTTPS; bodies are JSON.
- Platform IDs are UUID strings; timestamps are ISO 8601 UTC values.
- Authenticate with `Authorization: Bearer ntf_live_<secret>`. A credential maps
  to one tenant; clients never submit `tenant_id`.
- Write endpoints reject unknown fields. Initial limits are 64 KiB payloads and
  100 recipients per event.
- List results are newest-first and cursor-paginated. Cursors are opaque
  (base64 of `(created_at, id)`; keyset, not OFFSET). A malformed
  cursor is `400 validation_error`.
- Every response carries `X-Request-Id` (echoed from the request or generated);
  error bodies include the same value as `request_id`.
- Credential records are cached for up to 60 s, so a key revoked or a tenant
  disabled directly in the database stops working within that window.
- Roles: `producer` keys may ingest and read; `admin` keys may additionally call
  the tenant administration endpoints. Producer keys get `403 forbidden` there.

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
| 409 | `conflict` | A concurrent admin write won (e.g. two endpoints made default at once); retry. |
| 413 | `payload_too_large` | Body or payload exceeded a limit. |
| 422 | `unprocessable` | JSON is valid but violates a domain rule. |
| 429 | `rate_limited` | Tenant exceeded its ingestion rate limit (`POST /v1/events` only). Includes `Retry-After` (seconds). |
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
| `channels` | Non-empty unique values from `email`, `webhook`, `in_app`. A missing usable target (user without email, tenant without an active default webhook) produces a recorded `suppressed` delivery; it is never redirected. |
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

A repeat with the same key and body returns `202` with the original `id` and
counts. Two simultaneous requests with the same key are resolved by the database
unique constraint: one creates, the other replays (or gets `409` if its body differs).
An unknown or inactive recipient returns `422` and nothing is written.

### Rate limiting

`POST /v1/events` is limited per tenant (not per key) with GCRA, a token-bucket
equivalent: up to `burst` events at once, refilling at `rate` per second.
Defaults are 50/s with burst 100 (`RATE_LIMIT_PER_SECOND`, `RATE_LIMIT_BURST`).
A tenant can be given its own limit in `tenants.rate_limit_per_second` /
`rate_limit_burst`. Over the limit:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1

{"error": {"code": "rate_limited", "message": "Tenant ingestion rate limit exceeded; retry after 1s.", "request_id": "..."}}
```

Retry with the **same** `Idempotency-Key`. Reads are not rate limited. If Redis
is unavailable the limit is not enforced (fail open); ingestion keeps working.

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

`status` is optional: `pending`, `processing`, `delivered`, or `failed`. `limit`
defaults to 50 and is at most 100.

Implementation note: the original contract also listed `suppressed`, but by the
aggregation rule below a notification whose channels are all suppressed is
`failed`, so a `suppressed` notification can never exist. Status is derived from
deliveries on read, not stored.

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
    {"id": "82017374-5ebd-47f2-9447-ffd6229fb05e", "channel": "email", "status": "delivered", "attempt_count": 1, "next_attempt_at": null, "delivered_at": "2026-09-14T12:30:05Z", "failed_at": null, "last_error_code": null},
    {"id": "17d414a6-ec52-4375-95ff-28a299e7411f", "channel": "webhook", "status": "pending", "attempt_count": 1, "next_attempt_at": "2026-09-14T12:30:35Z", "delivered_at": null, "failed_at": null, "last_error_code": "503"}
  ],
  "created_at": "2026-09-14T12:30:00Z"
}
```

`next_attempt_at` is only shown for pending deliveries. `last_error_code` is the
provider status code (`503`, `550`) or `retryable_failure` / `permanent_failure`;
provider response bodies are never exposed.

## In-app feed

`GET /v1/users/{user_id}/in-app-notifications?unread_only=true&limit=50&cursor=<opaque-cursor>`

Returns only `in_app` deliveries in the authenticated tenant that the worker has
marked `delivered` (a planned-but-unprocessed item is not yet visible).
An unknown or other-tenant `user_id` is `404`. V0 authenticates a tenant rather
than an individual end user, so the tenant-facing application must enforce
whether its caller may read this `user_id`.

```json
{
  "data": [
    {
      "delivery_id": "17d414a6-ec52-4375-95ff-28a299e7411f",
      "notification_id": "d1e58638-6f17-4d24-baaa-31f6948a4c21",
      "event_type": "order.paid",
      "subject": {"type": "order", "id": "ord_123"},
      "title": "Payment received",
      "body": "4999 INR",
      "payload": {"order_id": "ord_123", "amount": 4999, "currency": "INR"},
      "read_at": null,
      "created_at": "2026-09-14T12:30:00Z"
    }
  ],
  "next_cursor": null
}
```

`POST /v1/in-app-notifications/{delivery_id}/read` is idempotent and returns the
feed item with its `read_at` timestamp; repeating it keeps the first `read_at`
(`COALESCE(read_at, now())`). A non-in-app, not-yet-delivered, or cross-tenant ID
is `404`.

## Notification templates

`POST /v1/templates` (admin) creates or updates the template for one
`(event_type, channel)` pair. `201` on create, `200` on update; `version` goes up
only when the content actually changes.

```json
{
  "event_type": "order.paid",
  "channel": "email",
  "subject": "Order {{ subject.id }} is paid",
  "body": "Hi {{ recipient.external_id }}, we received {{ payload.amount }} {{ payload.currency }}.",
  "active": true
}
```

| Field | Rules |
| --- | --- |
| `channel` | `email` or `in_app`. Webhooks always receive the structured JSON body, never a template. |
| `subject` | Required for email (the email subject); optional for in-app (the item's `title`). Max 200 characters. |
| `body` | Required, max 16 KiB. |
| Placeholders | `{{ path }}` where `path` starts with `event_type`, `subject`, `payload`, or `recipient` (`recipient.external_id`, `recipient.email`). Dotted keys and list indexes (`payload.items.0.sku`) only: no filters, expressions, or loops. Invalid syntax or an unknown root is `400` at save time. |

Rendering happens **at ingestion**:

- If the active template for an event's type and channel references a field the
  payload lacks, `POST /v1/events` returns `422` naming the template and the
  missing variable, and nothing is written.
- Rendered text is stored on the delivery. Editing a template later changes
  future events only; queued and retried deliveries keep their original content.
- Rendered output is capped at 64 KiB.
- With no active template, email falls back to a generic body containing the
  payload JSON, and in-app items have `title` and `body` set to `null`.

`GET /v1/templates?event_type=…&limit=&cursor=` lists templates; `GET
/v1/templates/{id}` returns one. Both require an admin key.

## Tenant administration

The first implementation needs configuration endpoints. They require an `admin`
credential; initial tenant/key bootstrap belongs to deployment, not this API.

| Endpoint | Purpose | Key fields |
| --- | --- | --- |
| `POST /v1/users` | Register or update a recipient, keyed by `(tenant, external_id)`. `201` on create, `200` on update. | `external_id`, optional `email`, `active` |
| `POST /v1/webhook-endpoints` | Register outbound destination. `201`. | `url`, optional `secret` (≥ 24 chars), `active` (default true), `is_default` (default true) |
| `GET /v1/webhook-endpoints` | List destinations without secret material. | `limit`, `cursor` |
| `PATCH /v1/webhook-endpoints/{id}` | Activate/deactivate or make default. | `active`, `is_default` |

Webhook endpoint rules:
- If `secret` is omitted the platform generates `whsec_…` and returns it **once**
  in the create response. Secrets are stored encrypted and never returned again.
- URL must be `https://`, contain no credentials, and resolve only to public IP
  addresses (SSRF guard). `WEBHOOK_ALLOW_PRIVATE_NETWORKS=true` relaxes this for
  local development. Rejections are `422`.
- At most one active default per tenant (partial unique index); making an endpoint
  default clears the previous one in the same transaction.
- Deliveries use the tenant's active default endpoint at ingestion time. If an
  endpoint is deactivated, its already-planned deliveries fail permanently rather
  than being sent.

Webhook body:

```json
{
  "delivery_id": "82017374-5ebd-47f2-9447-ffd6229fb05e",
  "event_id": "76db480d-5d35-4c39-92fa-a9d8cc74be8c",
  "notification_id": "d1e58638-6f17-4d24-baaa-31f6948a4c21",
  "recipient": {"user_id": "4b19534d-2440-4b73-889b-9f1d01c477aa", "external_id": "ada"},
  "event_type": "order.paid",
  "subject": {"type": "order", "id": "ord_123"},
  "payload": {"order_id": "ord_123", "amount": 4999, "currency": "INR"},
  "occurred_at": "2026-09-14T12:30:00Z"
}
```

Implementation note: `notification_id` and `recipient` were added to the
original body. A webhook delivery is created per recipient, so without them the
receiver could not tell which user a delivery is for.

Headers include `X-Notification-Delivery-Id`, `X-Notification-Timestamp`, and
`X-Notification-Signature: v1=<hex-hmac-sha256>`. The HMAC covers the timestamp,
a period, and exact body. Receivers should reject stale timestamps and deduplicate
delivery IDs. `app/scripts/webhook_receiver.py` is a working reference receiver.

Response handling: `2xx` is success. `408`, `429`, `5xx`, timeouts and connection
errors are retried with backoff (`Retry-After` is honoured when longer). Other
`4xx` and any `3xx` are permanent — redirects are not followed so a receiver
cannot bounce the worker to an internal address. Timeout is 10 s.

## Trade-offs

- Explicit recipients make V0 fan-out understandable; subscription rules are
  deferred.
- `202` makes asynchronous delivery semantics clear even though data is committed.
- Status reads arrive before callback features so producers can recover from
  uncertainty without coupling to provider details.
- Provider response IDs and diagnostic bodies remain internal to avoid leaking
  sensitive data and binding consumers to a provider.
- Tenant/key creation is not an API endpoint: minting credentials would require a
  super-admin credential with the largest blast radius in the system.
