"""API-key generation and verification without retaining cleartext secrets."""
from __future__ import annotations

import base64
import hashlib
import hmac
import secrets


def generate_api_key() -> tuple[str, str, str]:
    """Return (bearer_token, database_prefix, slow_hash)."""
    identifier = secrets.token_urlsafe(9).replace("_", "x")
    secret = secrets.token_urlsafe(32)
    prefix = f"ntf_live_{identifier}"
    token = f"{prefix}_{secret}"
    return token, prefix, hash_secret(secret)


def hash_secret(secret: str) -> str:
    """Fast hash for API-key secrets.

    These secrets are 256 bits of randomness, not human passwords, so brute force
    is infeasible even against a fast hash. A slow KDF (scrypt, ~35 ms of CPU)
    on every authenticated request would cap one core at ~28 requests/s.
    """
    return f"sha256${hashlib.sha256(secret.encode()).hexdigest()}"


def hash_password_like_secret(secret: str) -> str:
    """scrypt: right for low-entropy secrets. Kept so pre-existing V1 keys still verify."""
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(secret.encode(), salt=salt, n=2**14, r=8, p=1)
    return f"scrypt$16384$8$1${base64.urlsafe_b64encode(salt).decode()}${base64.urlsafe_b64encode(digest).decode()}"


def token_parts(token: str) -> tuple[str, str] | None:
    parts = token.split("_", 3)
    if len(parts) != 4 or parts[0] != "ntf" or parts[1] != "live" or not parts[2] or not parts[3]:
        return None
    return f"ntf_live_{parts[2]}", parts[3]


def verify_secret(secret: str, stored: str) -> bool:
    try:
        if stored.startswith("sha256$"):
            expected = stored.split("$", 1)[1]
            return hmac.compare_digest(hashlib.sha256(secret.encode()).hexdigest(), expected)
        algorithm, n, r, p, encoded_salt, encoded_hash = stored.split("$")
        if algorithm != "scrypt":
            return False
        digest = hashlib.scrypt(secret.encode(), salt=base64.urlsafe_b64decode(encoded_salt), n=int(n), r=int(r), p=int(p))
        return hmac.compare_digest(digest, base64.urlsafe_b64decode(encoded_hash))
    except (ValueError, TypeError):
        return False


def _fernet():
    from cryptography.fernet import Fernet

    from app.config import webhook_encryption_key

    return Fernet(webhook_encryption_key().encode())


def encrypt_secret(secret: str) -> bytes:
    """Webhook secrets must be recoverable to sign requests, so they are encrypted, not hashed."""
    return _fernet().encrypt(secret.encode())


def decrypt_secret(ciphertext: bytes) -> str:
    return _fernet().decrypt(bytes(ciphertext)).decode()


def generate_webhook_secret() -> str:
    return f"whsec_{secrets.token_urlsafe(32)}"


def sign_webhook(secret: str, timestamp: int, body: bytes) -> str:
    """Signature over "<timestamp>.<body>"; binding the timestamp stops replays of old bodies."""
    message = str(timestamp).encode() + b"." + body
    return "v1=" + hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()
