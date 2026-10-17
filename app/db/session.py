"""Database engine and short-lived session helpers."""
from __future__ import annotations
import os
from collections.abc import Generator
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

load_dotenv()

def database_url() -> str:
    value = os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("DATABASE_URL is not set")
    return value

engine = create_engine(database_url(), pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

def get_session() -> Generator[Session, None, None]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
