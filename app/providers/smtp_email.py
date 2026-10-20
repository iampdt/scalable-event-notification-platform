"""SMTP email adapter. Works with Mailpit locally and any SMTP relay (SES, Postmark...) in production."""
from __future__ import annotations

import json
import smtplib
from email.message import EmailMessage

from app.config import smtp_settings
from app.notifications.retry import ProviderFailure
from app.providers.base import ClaimedDelivery

# Authentication/config problems are *our* fault, not the recipient's. Treating
# them as permanent would fail every queued email during a credential rotation.
OPERATOR_ERROR_CODES = {530, 534, 535}


def classify_smtp_code(code: int) -> bool:
    """Return True when an SMTP reply code is worth retrying."""
    return code < 500 or code in OPERATOR_ERROR_CODES


def build_message(delivery: ClaimedDelivery, sender: str) -> EmailMessage:
    message = EmailMessage()
    message["From"] = sender
    message["To"] = delivery.target_snapshot["email"]
    message["Subject"] = f"Notification: {delivery.event_type}"
    # A stable Message-ID lets downstream systems spot an at-least-once duplicate.
    message["Message-ID"] = f"<{delivery.id}@notifications.local>"
    message["X-Notification-Delivery-Id"] = str(delivery.id)
    message.set_content(
        f"Event: {delivery.event_type}\n"
        f"Subject: {(delivery.subject or {}).get('type')} {(delivery.subject or {}).get('id')}\n\n"
        f"{json.dumps(delivery.payload, indent=2, sort_keys=True)}\n"
    )
    return message


class SmtpEmailProvider:
    def __init__(self, settings: dict | None = None):
        self.settings = settings or smtp_settings()

    def send(self, delivery: ClaimedDelivery) -> int | None:
        settings = self.settings
        message = build_message(delivery, settings["sender"])
        try:
            # One connection per message: simple and failure-isolated. Pooling is
            # the first optimisation if email throughput becomes the bottleneck.
            with smtplib.SMTP(settings["host"], settings["port"], timeout=settings["timeout"]) as smtp:
                if settings["starttls"]:
                    smtp.starttls()
                if settings["username"]:
                    smtp.login(settings["username"], settings["password"] or "")
                refused = smtp.send_message(message)
        except smtplib.SMTPRecipientsRefused as exc:
            code = next(iter(exc.recipients.values()))[0]
            raise ProviderFailure(f"recipient refused: SMTP {code}", retryable=classify_smtp_code(code), status_code=code) from exc
        except smtplib.SMTPResponseException as exc:
            raise ProviderFailure(f"SMTP {exc.smtp_code}", retryable=classify_smtp_code(exc.smtp_code), status_code=exc.smtp_code) from exc
        except (smtplib.SMTPException, OSError) as exc:
            raise ProviderFailure(f"SMTP connection failed: {type(exc).__name__}", retryable=True) from exc
        if refused:
            code = next(iter(refused.values()))[0]
            raise ProviderFailure(f"recipient refused: SMTP {code}", retryable=classify_smtp_code(code), status_code=code)
        return 250
