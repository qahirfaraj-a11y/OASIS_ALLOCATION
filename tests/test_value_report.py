"""Tests for the Value Report (monthly ROI artifact) + usage metering."""

import os
import sqlite3
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.value_report import (
    compute_value_metrics, usage_summary, write_value_report,
)


def _mini(path):
    today = datetime.now().strftime("%Y-%m-%d")
    old = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    c = sqlite3.connect(path)
    c.executescript("""
        CREATE TABLE POS_SALES_HDR (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT,
            NET_AMT REAL, VOID_FLAG TEXT);
        CREATE TABLE POS_SALES_DTL (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT,
            ITM_CD TEXT, QTY REAL, VOID_FLAG TEXT);
        CREATE TABLE INTEGRATION_PURCHASE_ORDERS (PO_ID INTEGER, ORG_CD TEXT,
            TOTAL_COST REAL, STATUS TEXT, CREATED_DT TEXT);
        CREATE TABLE STOCK_MASTER (SM_ORG_CD TEXT, SM_ITM_CD TEXT, SM_QTY REAL,
            SM_WAC REAL);
        CREATE TABLE OASIS_AUDIT_LOG (USERNAME TEXT, ACTION TEXT, CREATED_DT TEXT);
    """)
    # this-period sale of item A; item B has stock but NO sales -> dead stock
    c.execute("INSERT INTO POS_SALES_HDR VALUES ('ORG001','B1',?,1000,'F')", (today,))
    c.execute("INSERT INTO POS_SALES_DTL VALUES ('ORG001','B1',?,'A',2,'F')", (today,))
    c.execute("INSERT INTO POS_SALES_HDR VALUES ('ORG001','OLD',?,555,'F')", (old,))
    c.execute("INSERT INTO STOCK_MASTER VALUES ('ORG001','A',10,50)")   # live seller
    c.execute("INSERT INTO STOCK_MASTER VALUES ('ORG001','B',4,100)")   # dead: 400
    c.execute("INSERT INTO STOCK_MASTER VALUES ('ORG001','C',0,10)")    # stockout
    c.execute("INSERT INTO INTEGRATION_PURCHASE_ORDERS VALUES (1,'ORG001',2500,'APPROVED',?)", (today,))
    c.execute("INSERT INTO OASIS_AUDIT_LOG VALUES ('ops_admin','PAGE_VIEW',?)", (today,))
    c.execute("INSERT INTO OASIS_AUDIT_LOG VALUES ('ops_admin','APPROVE_PO',?)", (today,))
    c.commit()
    c.close()


class TestComputeMetrics:
    def test_period_scoping_and_stock_math(self, tmp_path):
        db = str(tmp_path / "v.db")
        _mini(db)
        m = compute_value_metrics(db, period_days=30)
        assert m["sales_revenue"] == 1000       # OLD bill excluded
        assert m["bills"] == 1
        assert m["po_generated"] == 1 and m["po_approved"] == 1
        assert m["stockouts"] == 1              # C
        assert m["dead_stock_value"] == 400     # B: 4 * 100 (A sold, excluded)
        assert m["inventory_value"] == 10 * 50 + 400
        assert m["page_views"] == 1 and m["operator_actions"] == 1
        assert m["active_users"] == 1

    def test_missing_tables_graceful(self, tmp_path):
        db = str(tmp_path / "empty.db")
        sqlite3.connect(db).close()
        m = compute_value_metrics(db)
        assert m["sales_revenue"] == 0 and m["stockouts"] == 0


class TestWriteReport:
    def test_writes_md_and_csv(self, tmp_path):
        db = str(tmp_path / "v.db")
        _mini(db)
        res = write_value_report(db, str(tmp_path / "reports"), tenant="RHAPTA")
        assert os.path.exists(res["markdown"]) and os.path.exists(res["csv"])
        text = open(res["markdown"], encoding="utf-8").read()
        assert "RHAPTA" in text and "Dead stock" in text


class TestUsageSummary:
    def test_counts(self, tmp_path):
        db = str(tmp_path / "v.db")
        _mini(db)
        s = usage_summary(db, "1970-01-01")
        assert s == {"page_views": 1, "operator_actions": 1, "active_users": 1}
