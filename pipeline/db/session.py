"""
Database session management — the one place that knows how to turn
DATABASE_URL into a SQLAlchemy Session. DAG tasks import get_session()
instead of constructing engines/sessionmakers themselves, so connection
pooling settings live in one file, not copy-pasted across six DAGs.
"""
from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from pipeline.config.settings import Settings

_engine_cache: dict[str, object] = {}


def _get_engine(database_url: str):
    # Cached per URL rather than per call — Airflow tasks in the same
    # worker process would otherwise open a fresh connection pool every
    # task invocation.
    if database_url not in _engine_cache:
        _engine_cache[database_url] = create_engine(database_url, pool_pre_ping=True)
    return _engine_cache[database_url]


@contextmanager
def get_session(settings: Settings) -> Session:
    engine = _get_engine(settings.database_url)
    session_local = sessionmaker(bind=engine)
    session = session_local()
    try:
        yield session
    finally:
        session.close()
