"""Tests for live POS activity queries (demo Live Feed)."""

import datetime
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.pos_activity import (
    activity_summary, recent_bills, recently_depleted,
)


def _mini(path):
    today = datetime.date.today().isoformat()
    c = sqlite3.connect(path)
    c.executescript("""
        CREATE TABLE POS_SALES_HDR (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT,
                                    TOTAL_QTY REAL, NET_AMT REAL);
        CREATE TABLE POS_SALES_DTL (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT,
                                    ITM_CD TEXT, ITEM_NAME TEXT, QTY REAL);
        CREATE TABLE STOCK_MASTER (SM_ORG_CD TEXT, SM_ITM_CD TEXT, SM_QTY REAL);
    """)
    # one bill today: sold X (now depleted) + Y (still stocked)
    c.execute("INSERT INTO POS_SALES_HDR VALUES ('ORG001','INJ1',?,8,500)", (today,))
    c.execute("INSERT INTO POS_SALES_DTL VALUES ('ORG001','INJ1',?,'X','Milk',5)", (today,))
    c.execute("INSERT INTO POS_SALES_DTL VALUES ('ORG001','INJ1',?,'Y','Bread',3)", (today,))
    # an OLD bill (not today) — must be excluded from 'today' metrics
    c.execute("INSERT INTO POS_SALES_HDR VALUES ('ORG001','OLD1','2025-01-01',2,99)")
    c.execute("INSERT INTO STOCK_MASTER VALUES ('ORG001','X',0)")   # depleted
    c.execute("INSERT INTO STOCK_MASTER VALUES ('ORG001','Y',50)")  # fine
    c.commit()
    c.close()


class TestActivitySummary:
    def test_today_only_and_depletion(self, tmp_path):
        db = str(tmp_path / "a.db")
        _mini(db)
        s = activity_summary(db, org="ORG001")
        assert s["bills_today"] == 1          # OLD1 excluded
        assert s["units_today"] == 8
        assert s["stockouts"] == 1            # X
        assert s["depleted_today"] == 1       # X sold today AND now <1

    def test_bad_path_is_graceful(self):
        s = activity_summary("/no/such.db")
        assert s == {"bills_today": 0, "units_today": 0, "stockouts": 0, "depleted_today": 0}


class TestRecentBillsAndDepleted:
    def test_recent_bills_today_first(self, tmp_path):
        db = str(tmp_path / "b.db")
        _mini(db)
        bills = recent_bills(db, org="ORG001", limit=5)
        assert bills[0]["Bill"] == "INJ1"     # today sorts first
        assert any(b["Bill"] == "OLD1" for b in bills)

    def test_recently_depleted(self, tmp_path):
        db = str(tmp_path / "c.db")
        _mini(db)
        dep = recently_depleted(db, org="ORG001")
        codes = {d["Code"] for d in dep}
        assert "X" in codes and "Y" not in codes  # only the sold-today-and-empty SKU
