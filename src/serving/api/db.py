"""Conexão com o PostgreSQL da serving layer (SQLAlchemy)."""

from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.common.config import settings

engine = create_engine(settings.postgres.url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """Dependency do FastAPI: uma sessão por request, sempre fechada ao final."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
