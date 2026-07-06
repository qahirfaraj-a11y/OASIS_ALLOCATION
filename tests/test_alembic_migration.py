"""Tests that the Alembic baseline migration produces the full OASIS schema."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

alembic = pytest.importorskip("alembic")
from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402

EXPECTED_TABLES = {
    "ORGANIZATION_MST", "ITEM_MST", "STOCK_MASTER", "BASIC_SP_MST",
    "BASIC_CP_MST", "CUSTOMER_MST", "POS_SALES_HDR", "POS_SALES_DTL",
    "BI_SALES_REPORT", "COUNTER_MST", "BASE_SYSTEM_PREFERENCES",
    "TAX_PLAN_HDR", "TAX_MST", "SUPPLIER_MST", "GRN_HDR",
    "INTEGRATION_PURCHASE_ORDERS", "OASIS_USERS", "OASIS_SESSIONS",
    "OASIS_AUDIT_LOG", "OASIS_SYSTEM_CONFIG", "INTEGRATION_TRANSFER_ORDERS",
}


@pytest.fixture
def temp_db_url(monkeypatch):
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)  # let SQLite create it fresh
    url = f"sqlite:///{path}"
    monkeypatch.setenv("OASIS_DB_URL", url)
    yield url
    if os.path.exists(path):
        os.unlink(path)


def _alembic_config(db_url: str) -> Config:
    cfg = Config(os.path.join(PROJECT_ROOT, "alembic.ini"))
    # migrations/ (renamed from alembic/, which shadowed the alembic package)
    cfg.set_main_option("script_location", os.path.join(PROJECT_ROOT, "migrations"))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def test_upgrade_head_creates_all_tables(temp_db_url):
    import sqlite3
    command.upgrade(_alembic_config(temp_db_url), "head")

    path = temp_db_url.replace("sqlite:///", "")
    conn = sqlite3.connect(path)
    tables = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    conn.close()

    missing = EXPECTED_TABLES - tables
    assert not missing, f"Migration did not create: {missing}"
    assert "alembic_version" in tables


def test_downgrade_base_drops_all_tables(temp_db_url):
    import sqlite3
    cfg = _alembic_config(temp_db_url)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")

    path = temp_db_url.replace("sqlite:///", "")
    conn = sqlite3.connect(path)
    tables = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    conn.close()

    leftover = EXPECTED_TABLES & tables
    assert not leftover, f"Downgrade left tables behind: {leftover}"


def test_models_metadata_matches_expected_tables():
    from oasis.models import metadata
    assert set(metadata.tables.keys()) == EXPECTED_TABLES
