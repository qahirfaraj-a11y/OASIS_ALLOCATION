"""Tests for POS-source vs OASIS-store DB role separation."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import db as oasis_db


class TestPosDbUrl:
    def test_explicit_pos_url_wins(self, monkeypatch):
        monkeypatch.setenv("OASIS_POS_DB_URL", "mssql+pyodbc://u:p@host/posdb")
        assert oasis_db.get_pos_db_url() == "mssql+pyodbc://u:p@host/posdb"

    def test_falls_back_to_oasis_db_url(self, monkeypatch):
        monkeypatch.delenv("OASIS_POS_DB_URL", raising=False)
        monkeypatch.setenv("OASIS_DB_URL", "postgresql://u:p@host/oasis")
        assert oasis_db.get_pos_db_url() == "postgresql://u:p@host/oasis"

    def test_sqlalchemy_url_normalizes_postgres(self, monkeypatch):
        monkeypatch.setenv("OASIS_POS_DB_URL", "postgres://u:p@host/db")
        assert oasis_db.get_pos_sqlalchemy_url().startswith("postgresql://")


class _FakeConnector:
    def __init__(self, name):
        self.engine = f"engine::{name}"


class TestAdapterStoreEngine:
    def test_store_defaults_to_source(self):
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        a = PosErpAdapter(_FakeConnector("pos"))
        # back-compat: single DB -> store engine IS the source engine
        assert a.engine == "engine::pos"
        assert a.store_engine == "engine::pos"

    def test_store_overridden_when_separated(self):
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        a = PosErpAdapter(_FakeConnector("pos"), _FakeConnector("store"))
        assert a.engine == "engine::pos"        # reads hit the POS source
        assert a.store_engine == "engine::store"  # queues hit OASIS's store
