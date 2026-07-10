"""
Hub database session factory.

The hub runs its OWN database, separate from any client's POS/OASIS store. It
defaults to a local SQLite file for development and single-node deployments, and
takes a full ``OASIS_HUB_DB_URL`` (e.g. PostgreSQL) in production.

We use ``create_all`` to bootstrap the schema for now — the hub schema is young
and iterating. Alembic migrations come once it stabilises.
"""

import os
import logging
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from .models import Base

logger = logging.getLogger("OASIS.Hub.DB")

_DEFAULT_URL = "sqlite:///oasis_hub.db"

_engine = None
_SessionLocal = None


def get_db_url() -> str:
    url = os.getenv("OASIS_HUB_DB_URL", _DEFAULT_URL)
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def get_engine():
    global _engine, _SessionLocal
    if _engine is None:
        url = get_db_url()
        kwargs = {"future": True}
        if url.startswith("sqlite"):
            # check_same_thread=False so the FastAPI threadpool can share it;
            # each request still gets its own Session.
            kwargs["connect_args"] = {"check_same_thread": False}
        _engine = create_engine(url, **kwargs)
        _SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False,
                                     class_=Session, future=True)
        logger.info("Hub DB engine created: %s", url.split("@")[-1])
    return _engine


def init_db() -> None:
    """Create all hub tables if absent. Safe to call repeatedly."""
    Base.metadata.create_all(bind=get_engine())
    logger.info("Hub schema ensured (%d tables).", len(Base.metadata.tables))


def _session_factory():
    if _SessionLocal is None:
        get_engine()
    return _SessionLocal


@contextmanager
def session_scope():
    """Transactional scope: commit on success, rollback on error, always close."""
    sess = _session_factory()()
    try:
        yield sess
        sess.commit()
    except Exception:
        sess.rollback()
        raise
    finally:
        sess.close()


def get_session() -> Session:
    """FastAPI dependency: yields a session, closes it after the request.

    Usage:  def handler(db: Session = Depends(get_session)): ...
    """
    sess = _session_factory()()
    try:
        yield sess
        sess.commit()
    except Exception:
        sess.rollback()
        raise
    finally:
        sess.close()
