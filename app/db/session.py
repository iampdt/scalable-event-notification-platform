"""Database engine and short-lived session helpers."""
from __future__ import annotations
import os
from collections.abc import Generator
from dotenv import load_dotenv
from sqlalchemy import create_engine

from app.config import db_pool_settings
from sqlalchemy.orm import Session, sessionmaker

load_dotenv()

def database_url() -> str:
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL is not set")
    return value

# Pool sizing: every API process holds up to pool_size + max_overflow connections,
# and every worker process the same. Their sum across all processes must stay
# under PostgreSQL's max_connections (100 by default). pool_timeout fails fast
# (5 s) instead of letting requests hang 30 s when the pool is exhausted.
engine = create_engine(database_url(), pool_pre_ping=True, **db_pool_settings())
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
