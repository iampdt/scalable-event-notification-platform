"""Create a local tenant, producer key, and three sample users."""
from __future__ import annotations

import os

from sqlalchemy import select

from app.db.models import ApiKey, ApiKeyRole, Tenant, User
from app.db.session import SessionLocal
from app.security import generate_api_key


def main() -> None:
    slug = os.getenv("SEED_TENANT_SLUG", "demo")
    with SessionLocal.begin() as session:
        tenant = session.scalar(select(Tenant).where(Tenant.slug == slug))
        if not tenant:
            tenant = Tenant(slug=slug, name=os.getenv("SEED_TENANT_NAME", "Demo tenant"))
            session.add(tenant)
            session.flush()
        users = [("ada", "ada@example.test"), ("grace", "grace@example.test"), ("linus", "linus@example.test")]
        for external_id, email in users:
            if not session.scalar(select(User).where(User.tenant_id == tenant.id, User.external_id == external_id)):
                session.add(User(tenant_id=tenant.id, external_id=external_id, email=email))
        key = session.scalar(select(ApiKey).where(ApiKey.tenant_id == tenant.id, ApiKey.name == "local-producer"))
        token = None
        if not key:
            token, prefix, secret_hash = generate_api_key()
            session.add(ApiKey(tenant_id=tenant.id, key_prefix=prefix, secret_hash=secret_hash, role=ApiKeyRole.producer, name="local-producer"))
    print(f"Seeded tenant: {slug}")
    print("Seeded users: ada, grace, linus")
    if token:
        print(f"Producer API key (shown once): {token}")
    else:
        print("Producer API key already exists and cannot be shown again; create a new key in the database for a replacement.")


if __name__ == "__main__":
    main()
