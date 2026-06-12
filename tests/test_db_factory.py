"""Tests for the centralized database connection factory (oasis.logic.db)."""

import os
import sys
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import db as oasis_db


class TestDBURLResolution:
    def test_default_is_sqlite(self):
        os.environ.pop("OASIS_DB_URL", None)
        os.environ.pop("OASIS_DB_PATH", None)
        url = oasis_db.get_db_url()
        assert url.startswith("sqlite:///")

    def test_explicit_url_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("OASIS_DB_URL", "postgresql://u:p@host/db")
        monkeypatch.delenv("OASIS_DB_PATH", raising=False)
        assert oasis_db.get_db_url() == "postgresql://u:p@host/db"

    def test_legacy_path_becomes_sqlite_url(self, monkeypatch):
        monkeypatch.delenv("OASIS_DB_URL", raising=False)
        monkeypatch.setenv("OASIS_DB_PATH", "/tmp/test.db")
        url = oasis_db.get_db_url()
        assert url == "sqlite:////tmp/test.db"

    def test_is_sqlite_true_for_default(self, monkeypatch):
        monkeypatch.delenv("OASIS_DB_URL", raising=False)
        monkeypatch.delenv("OASIS_DB_PATH", raising=False)
        assert oasis_db.is_sqlite() is True

    def test_is_sqlite_false_for_postgres(self, monkeypatch):
        monkeypatch.setenv("OASIS_DB_URL", "postgresql://localhost/oasis")
        assert oasis_db.is_sqlite() is False


class TestSQLiteConnection:
    def test_raw_connection_creates_file(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            url = f"sqlite:///{path}"
            conn = oasis_db.get_raw_connection(db_url=url)
            conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            conn.execute("INSERT INTO test VALUES (1)")
            conn.commit()
            row = conn.execute("SELECT id FROM test").fetchone()
            assert row[0] == 1
            conn.close()
        finally:
            os.unlink(path)

    def test_context_manager_commits(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            url = f"sqlite:///{path}"
            with oasis_db.connection(db_url=url) as conn:
                conn.execute("CREATE TABLE t (v TEXT)")
                conn.execute("INSERT INTO t VALUES ('hello')")

            conn2 = oasis_db.get_raw_connection(db_url=url)
            row = conn2.execute("SELECT v FROM t").fetchone()
            assert row[0] == "hello"
            conn2.close()
        finally:
            os.unlink(path)

    def test_context_manager_rolls_back_on_error(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            url = f"sqlite:///{path}"
            with oasis_db.connection(db_url=url) as conn:
                conn.execute("CREATE TABLE t2 (v TEXT)")

            with pytest.raises(Exception):
                with oasis_db.connection(db_url=url) as conn:
                    conn.execute("INSERT INTO t2 VALUES ('ok')")
                    raise RuntimeError("force rollback")

            conn2 = oasis_db.get_raw_connection(db_url=url)
            count = conn2.execute("SELECT COUNT(*) FROM t2").fetchone()[0]
            assert count == 0
            conn2.close()
        finally:
            os.unlink(path)


class TestSQLAlchemyURL:
    def test_postgres_shorthand_normalized(self):
        url = oasis_db.get_sqlalchemy_url("postgres://u:p@h/d")
        assert url.startswith("postgresql://")

    def test_sqlite_passthrough(self):
        url = oasis_db.get_sqlalchemy_url("sqlite:///test.db")
        assert url == "sqlite:///test.db"


class TestUnsupportedScheme:
    def test_mysql_raises(self):
        with pytest.raises(ValueError, match="Unsupported"):
            oasis_db.get_raw_connection(db_url="mysql://localhost/db")
