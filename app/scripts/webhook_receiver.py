"""Reference webhook receiver: what a tenant should implement on their side.

    python -m app.scripts.webhook_receiver --secret whsec_... [--port 9123] [--fail-first 2]

It verifies the HMAC signature, rejects stale timestamps (replay protection),
and de-duplicates by delivery ID because the platform delivers at-least-once.
--fail-first N answers 503 to the first N requests to demonstrate retries.
"""
from __future__ import annotations

import argparse
import hmac
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.security import sign_webhook

TOLERANCE_SECONDS = 300


def make_handler(secret: str, fail_first: int):
    seen_delivery_ids: set[str] = set()
    state = {"remaining_failures": fail_first}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            delivery_id = self.headers.get("X-Notification-Delivery-Id", "")
            try:
                timestamp = int(self.headers.get("X-Notification-Timestamp", ""))
            except ValueError:
                return self.reply(400, "missing timestamp")
            if abs(time.time() - timestamp) > TOLERANCE_SECONDS:
                return self.reply(400, "stale timestamp")
            if not hmac.compare_digest(sign_webhook(secret, timestamp, body), self.headers.get("X-Notification-Signature", "")):
                return self.reply(401, "bad signature")
            if state["remaining_failures"] > 0:
                state["remaining_failures"] -= 1
                return self.reply(503, "simulated outage")
            if delivery_id in seen_delivery_ids:
                print(f"duplicate delivery {delivery_id} acknowledged without reprocessing", flush=True)
                return self.reply(200, "duplicate")
            seen_delivery_ids.add(delivery_id)
            print(f"verified delivery {delivery_id}: {json.loads(body)['event_type']}", flush=True)
            self.reply(200, "ok")

        def reply(self, status: int, message: str) -> None:
            self.send_response(status)
            self.end_headers()
            self.wfile.write(message.encode())

        def log_message(self, *args) -> None:
            pass

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--secret", required=True)
    parser.add_argument("--port", type=int, default=9123)
    parser.add_argument("--fail-first", type=int, default=0)
    args = parser.parse_args()
    print(f"listening on http://localhost:{args.port}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(args.secret, args.fail_first)).serve_forever()


if __name__ == "__main__":
    main()
