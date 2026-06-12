"""
OASIS Database Connection Factory
==================================
Centralized database connection management supporting SQLite (default)
and PostgreSQL (enterprise). All modules should use get_connection()
instead of raw sqlite3.connect().

Configuration via environment:
    OASIS_DB_URL  — Full database URL. Examples:
        sqlite:///oasis/data/mock_pos_erp.db   (default)
        postgresql://oasis:pass@localhost/oasis
    OASIS_DB_PATH — Legacy shortcut for SQLite path (used if OASIS_DB_URL is unset)
"""

import os
import sqlite3
import logging
from contextlib import contextmanager
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger("OasisDB")

_DEFAULT_SQLITE_PATH = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'mock_pos_erp.db'
)


def get_db_url() -> str:
    url = os.getenv("OASIS_DB_URL")
    if url:
        return url
    path = os.getenv("OASIS_DB_PATH", os.path.abspath(_DEFAULT_SQLITE_PATH))
    return f"sqlite:///{path}"


def is_sqlite() -> bool:
    return get_db_url().startswith("sqlite")


def _configure_sqlite(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")


def get_raw_connection(db_url: Optional[str] = None, timeout: float = 30.0):
    """
    Return a raw DB-API connection.

    For SQLite: returns sqlite3.Connection with WAL/busy_timeout configured.
    For PostgreSQL: returns psycopg2 connection.

    Callers are responsible for closing the connection.
    """
    url = db_url or get_db_url()

    if url.startswith("sqlite"):
        path = url.replace("sqlite:///", "").replace("sqlite://", "")
        if not path:
            path = os.path.abspath(_DEFAULT_SQLITE_PATH)
        conn = sqlite3.connect(path, timeout=timeout)
        conn.row_factory = sqlite3.Row
        _configure_sqlite(conn)
        return conn

    parsed = urlparse(url)
    if parsed.scheme in ("postgresql", "postgres", "postgresql+psycopg2"):
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for PostgreSQL connections. "
                "Install it with: pip install psycopg2-binary"
            )
        return psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            dbname=parsed.path.lstrip("/"),
            user=parsed.username,
            password=parsed.password,
        )

    raise ValueError(f"Unsupported database URL scheme: {url}")


@contextmanager
def connection(db_url: Optional[str] = None, timeout: float = 30.0):
    """
    Context manager for database connections.

    Usage:
        with db.connection() as conn:
            conn.execute("SELECT ...")
    """
    conn = get_raw_connection(db_url, timeout)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_sqlalchemy_url(db_url: Optional[str] = None) -> str:
    """
    Return a SQLAlchemy-compatible connection string.
    Normalizes sqlite:/// paths and postgres:// → postgresql://.
    """
    url = db_url or get_db_url()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url
