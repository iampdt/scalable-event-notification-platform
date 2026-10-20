#!/usr/bin/env bash
# End-to-end demo against the Docker stack (`docker compose up -d --build`).
#
#   ./scripts/demo.sh            # narrated, pauses between steps
#   DEMO_PAUSE=0 ./scripts/demo.sh
#
# Shows: ingestion + idempotency, three channels (email, signed webhook, in-app),
# templates, the dead-letter queue, per-tenant rate limiting, and metrics.
set -euo pipefail
cd "$(dirname "$0")/.."

API=${API:-http://localhost:8000}
PAUSE=${DEMO_PAUSE:-2}
PY=${PYTHON:-python3}
SLUG="demo-$(date +%s)"
RECEIVER_PORT=9123

say()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; sleep "$PAUSE"; }
run()  { printf '\033[2m$ %s\033[0m\n' "$*"; eval "$@"; }
json() { $PY -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
psql_() { docker exec notification-postgres psql -U notification_app -d notifications -tAc "$1"; }

say "Is the stack ready? (/readyz checks PostgreSQL; Redis is optional)"
run "curl -s $API/readyz"; echo

say "Bootstrap a tenant: producer + admin API keys are printed once, only hashes are stored"
SEED=$(docker compose run --rm -e SEED_TENANT_SLUG="$SLUG" api python -m app.scripts.seed 2>/dev/null)
echo "$SEED" | sed -E 's/(ntf_live_.{6})[^ ]*/\1…(redacted)/'  # keys may contain - and _
PRODUCER=$(echo "$SEED" | awk '/Producer API key/ {print $NF}')
ADMIN=$(echo "$SEED" | awk '/Admin API key/ {print $NF}')
USER_ID=$(psql_ "select u.id from users u join tenants t on t.id=u.tenant_id where t.slug='$SLUG' and external_id='ada'")

say "Register an email template (logic-free {{ placeholders }}, rendered at ingestion)"
run "curl -s -X POST $API/v1/templates -H \"Authorization: Bearer \$ADMIN\" -H 'Content-Type: application/json' \
  -d '{\"event_type\":\"order.paid\",\"channel\":\"email\",\"subject\":\"Order {{ subject.id }} is paid\",\"body\":\"Hi {{ recipient.external_id }}, we received {{ payload.amount }} {{ payload.currency }}.\"}' | json 'd[\"version\"]' | sed 's/^/template version: /'"

say "Register a webhook endpoint (the secret is returned exactly once) and start a receiver that verifies signatures"
SECRET=$(curl -s -X POST "$API/v1/webhook-endpoints" -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://host.docker.internal:$RECEIVER_PORT/hooks\"}" | json 'd["secret"]')
echo "secret: ${SECRET:0:10}…"
$PY -m app.scripts.webhook_receiver --secret "$SECRET" --port $RECEIVER_PORT > /tmp/demo-receiver.log 2>&1 &
RECEIVER=$!
trap 'kill $RECEIVER 2>/dev/null || true' EXIT
sleep 1

EVENT_BODY="{\"event_type\":\"order.paid\",\"subject\":{\"type\":\"order\",\"id\":\"ord_42\"},\"payload\":{\"amount\":4999,\"currency\":\"INR\"},\"recipients\":[{\"user_id\":\"$USER_ID\",\"channels\":[\"email\",\"webhook\",\"in_app\"]}]}"

say "Send one event to three channels: 202 = durably accepted, not yet delivered"
EVENT_ID=$(curl -s -X POST "$API/v1/events" -H "Authorization: Bearer $PRODUCER" -H 'Idempotency-Key: ord_42-paid' \
  -H 'Content-Type: application/json' -d "$EVENT_BODY" | json 'd["id"]')
echo "event id: $EVENT_ID"

say "Producer retries (same key, same body): same event back, nothing duplicated"
curl -s -X POST "$API/v1/events" -H "Authorization: Bearer $PRODUCER" -H 'Idempotency-Key: ord_42-paid' \
  -H 'Content-Type: application/json' -d "$EVENT_BODY" | json '"same id" if d["id"] == "'"$EVENT_ID"'" else "DIFFERENT"'

say "Same key, different body: 409, never a silent overwrite"
curl -s -o /dev/null -w 'HTTP %{http_code}\n' -X POST "$API/v1/events" -H "Authorization: Bearer $PRODUCER" -H 'Idempotency-Key: ord_42-paid' \
  -H 'Content-Type: application/json' -d "${EVENT_BODY/4999/5000}"

say "Outbox → Kafka → consumer → providers. Delivery status:"
sleep 3
run "curl -s $API/v1/events/$EVENT_ID -H \"Authorization: Bearer \$PRODUCER\" | json 'd[\"deliveries\"]'"
echo "receiver: $(tail -1 /tmp/demo-receiver.log)"
echo "mailpit:  $(curl -s localhost:8025/api/v1/messages | json 'd["messages"][0]["Subject"]')"
echo "in-app:   $(curl -s "$API/v1/users/$USER_ID/in-app-notifications" -H "Authorization: Bearer $PRODUCER" | json 'len(d["data"])') item(s) in the feed"

say "A permanent failure (410) lands in the dead-letter queue, with its attempt history"
GONE_ID=$(curl -s -X POST "$API/v1/webhook-endpoints" -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://host.docker.internal:$RECEIVER_PORT/gone\",\"secret\":\"$SECRET\"}" | json 'd["id"]')
curl -s -o /dev/null -X POST "$API/v1/events" -H "Authorization: Bearer $PRODUCER" -H 'Idempotency-Key: ord_43-paid' \
  -H 'Content-Type: application/json' -d "{\"event_type\":\"order.shipped\",\"subject\":{\"type\":\"order\",\"id\":\"ord_43\"},\"payload\":{},\"recipients\":[{\"user_id\":\"$USER_ID\",\"channels\":[\"webhook\"]}]}"
sleep 3
run "curl -s $API/v1/dead-letters -H \"Authorization: Bearer \$ADMIN\" | json '[(x[\"channel\"], x[\"last_error_code\"], x[\"last_attempt\"][\"outcome\"]) for x in d[\"data\"]]'"

say "Per-tenant rate limit (GCRA in Redis): set 1/s, burst 2, then fire 5 requests"
psql_ "update tenants set rate_limit_per_second=1, rate_limit_burst=2 where slug='$SLUG'" > /dev/null
docker exec notification-redis redis-cli --scan --pattern 'auth:*' | xargs -r docker exec -i notification-redis redis-cli del > /dev/null  # credential cache holds limits for 60 s
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w '%{http_code} ' -X POST "$API/v1/events" -H "Authorization: Bearer $PRODUCER" -H "Idempotency-Key: burst-$i" \
    -H 'Content-Type: application/json' -d "{\"event_type\":\"burst\",\"subject\":{\"type\":\"t\",\"id\":\"$i\"},\"payload\":{},\"recipients\":[{\"user_id\":\"$USER_ID\",\"channels\":[\"in_app\"]}]}"
done; echo

say "Everything above is measured: a few Prometheus series (dashboard: http://localhost:3000)"
curl -s "$API/metrics" | grep -E '^(events_ingested_total\{result="(accepted|replayed|conflict)"\}|rate_limited_total|deliveries_dead_letters) '

say "Done. Tenant: $SLUG"
