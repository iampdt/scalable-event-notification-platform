"""Create a local tenant, producer and admin keys, and three sample users. Safe to re-run."""
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
        tokens = {}
        for name, role in (("local-producer", ApiKeyRole.producer), ("local-admin", ApiKeyRole.admin)):
            if not session.scalar(select(ApiKey).where(ApiKey.tenant_id == tenant.id, ApiKey.name == name)):
                token, prefix, secret_hash = generate_api_key()
                session.add(ApiKey(tenant_id=tenant.id, key_prefix=prefix, secret_hash=secret_hash, role=role, name=name))
                tokens[role.value] = token
    print(f"Seeded tenant: {slug}")
    print("Seeded users: ada, grace, linus")
    for role in ("producer", "admin"):
        if role in tokens:
            print(f"{role.title()} API key (shown once): {tokens[role]}")
        else:
            print(f"{role.title()} API key already exists and cannot be shown again; only its hash is stored.")


if __name__ == "__main__":
    main()
