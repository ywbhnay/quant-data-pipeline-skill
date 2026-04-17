"""Unified database engine / session factory."""

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import DatabaseConfig


def create_engine_from_config(config: DatabaseConfig):
    return create_engine(
        config.url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
    )


def make_session_factory(engine=None):
    eng = engine or create_engine_from_config(DatabaseConfig())
    return sessionmaker(bind=eng, expire_on_commit=False)


@contextmanager
def get_session(engine) -> Session:
    factory = make_session_factory(engine)
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
