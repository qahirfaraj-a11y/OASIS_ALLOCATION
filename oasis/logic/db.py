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


def sqlite_path_from_url(url: Optional[str]) -> Optional[str]:
    """The local file behind a ``sqlite:///`` URL, else None (non-SQLite URLs)."""
    u = str(url or "")
    if not u.startswith("sqlite") or "///" not in u:
        return None
    path = u.split("///", 1)[1].split("?", 1)[0]
    return os.path.abspath(path) if path else None


def onboarded_pos_url() -> Optional[str]:
    """POS URL recorded by the first-run wizard's "Connect a POS" choice.

    Read lazily (and defensively) so this module stays importable on its own —
    oasis.logic.onboarding imports db for its connection check.
    """
    try:
        from .onboarding import load_onboarding
        ob = load_onboarding()
        if ob.get("source") == "connect" and ob.get("db_url"):
            return str(ob["db_url"])
    except Exception:
        pass
    return None


def get_pos_db_url() -> str:
    """URL of the POS/ERP *source* database (read-only client system).

    Separating this from OASIS's own operational store (get_db_url) lets a client
    install read the POS database read-only while OASIS keeps its users/audit/
    config/integration tables in its own store.

    Priority: OASIS_POS_DB_URL → the URL the wizard recorded → OASIS_DB_URL →
    the SQLite default (so the single-DB demo keeps working unchanged). The
    middle tier is what makes "Connect a POS" actually reach the runtime: before
    it existed the wizard recorded a URL nothing ever read, and the consoles
    opened a default SQLite file that a connect-only install does not have
    (deep-analysis finding S2).
    """
    return os.getenv("OASIS_POS_DB_URL") or onboarded_pos_url() or get_db_url()


def has_distinct_pos() -> bool:
    """True when the POS source is a database separate from the OASIS store.

    Use this instead of testing ``os.getenv("OASIS_POS_DB_URL")`` directly —
    the env var is only one of the two ways a distinct POS gets configured.
    """
    return bool(os.getenv("OASIS_POS_DB_URL") or onboarded_pos_url())


def get_pos_sqlalchemy_url(db_url: Optional[str] = None) -> str:
    """SQLAlchemy URL for the POS source DB (normalizes postgres://)."""
    url = db_url or get_pos_db_url()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


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


def to_sqlalchemy_url(path_or_url: Optional[str]) -> str:
    """Return a SQLAlchemy URL for either a plain path or an existing URL.

    Accepts a full database URL (``sqlite:///...``, ``postgresql://...``) or a
    plain filesystem path — which is interpreted as a SQLite file — so callers
    can hand either form to SQLAlchemy engines. This closes the scheduler
    wiring gap (finding D-3): job handlers and the engine runner previously
    passed a raw path to ``UniversalConnector``, whose ``create_engine`` then
    raised ``ArgumentError: Could not parse SQLAlchemy URL``.
    """
    raw = str(path_or_url or "").strip()
    if not raw:
        return get_db_url()
    if "://" in raw:
        if raw.startswith("postgres://"):
            raw = raw.replace("postgres://", "postgresql://", 1)
        return raw
    return f"sqlite:///{raw}"
