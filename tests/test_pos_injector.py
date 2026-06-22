"""Tests for the mock POS injector."""

import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.pos_injector import SaleLine, build_bill, inject_once


class TestBuildBill:
    def test_totals_and_lines(self):
        lines = [SaleLine("A", "Apple", 2, 50.0), SaleLine("B", "Bread", 1, 80.0)]
        hdr, dtl = build_bill("ORG001", "INJ1", "2026-06-20", lines)
        assert hdr["TOTAL_QTY"] == 3
        assert hdr["NET_AMT"] == 180.0
        assert hdr["VOID_FLAG"] == "F" and hdr["ORG_CD"] == "ORG001"
        assert len(dtl) == 2
        assert dtl[0]["SERIAL_NO"] == 1 and dtl[0]["TOTAL_VALUE"] == 100.0
        assert dtl[1]["ITM_CD"] == "B"


def _make_mini_db(path):
    c = sqlite3.connect(path)
    c.executescript("""
        CREATE TABLE ORGANIZATION_MST (ORG_CD TEXT, ACTIVE_FLAG TEXT);
        CREATE TABLE ITEM_MST (ITM_CD TEXT, ITM_LONG_NAME TEXT);
        CREATE TABLE BASIC_SP_MST (BSP_ITEM_CD TEXT, BSP_ORG_CD TEXT, BSP_SP REAL);
        CREATE TABLE STOCK_MASTER (SM_ORG_CD TEXT, SM_ITM_CD TEXT, SM_QTY REAL,
                                   SM_WAC REAL, SM_LAST_RECV_DT TEXT, SM_LAST_ISSUE_DT TEXT);
        CREATE TABLE POS_SALES_HDR (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT, CUST_CD TEXT,
            COUNTER_CD TEXT, LEVEL_NUMBER INT, TOTAL_QTY REAL, TOTAL_AMT REAL, NET_AMT REAL,
            TAX_AMT REAL, DISC_AMT REAL, PAYMENT_MODE TEXT, VOID_FLAG TEXT,
            CUS_REF_CODE TEXT, CUS_REF_REMARKS TEXT);
        CREATE TABLE POS_SALES_DTL (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT, SERIAL_NO INT,
            ITM_CD TEXT, ITEM_NAME TEXT, QTY REAL, SELL_PRICE REAL, NET_AMT REAL, TAX_AMT REAL,
            DISC_AMT REAL, NET_TAX_AMT REAL, TOTAL_VALUE REAL, UOM_CD TEXT, UOM_DESC TEXT,
            VOID_FLAG TEXT, PROMO_ITEM_FLAG TEXT, SCAN_ITM_CD TEXT, TAX_PLAN_CD TEXT);
        INSERT INTO ORGANIZATION_MST VALUES ('ORG001','Y');
        INSERT INTO ITEM_MST VALUES ('I1','Milk'),('I2','Bread'),('I3','Eggs');
        INSERT INTO BASIC_SP_MST VALUES ('I1','ORG001',60),('I2','ORG001',55),('I3','ORG001',300);
        INSERT INTO STOCK_MASTER VALUES
            ('ORG001','I1',100,40,NULL,NULL),
            ('ORG001','I2',50,30,NULL,NULL),
            ('ORG001','I3',20,200,NULL,NULL);
    """)
    c.commit()
    c.close()


class TestInjectOnce:
    def test_injects_bill_and_decrements_stock(self, tmp_path):
        db = str(tmp_path / "mini.db")
        _make_mini_db(db)
        before = dict(sqlite3.connect(db).execute(
            "SELECT SM_ITM_CD, SM_QTY FROM STOCK_MASTER").fetchall())

        res = inject_once(db, org="ORG001", sku_count=3, max_qty=5,
                          deplete=False, restock=0, seed=1)
        assert res["lines"] >= 1 and res["units_sold"] > 0

        c = sqlite3.connect(db)
        # a bill landed
        assert c.execute("SELECT COUNT(*) FROM POS_SALES_HDR").fetchone()[0] == 1
        n_dtl = c.execute("SELECT COUNT(*) FROM POS_SALES_DTL").fetchone()[0]
        assert n_dtl == res["lines"]
        # total stock strictly decreased by the units sold
        after = dict(c.execute("SELECT SM_ITM_CD, SM_QTY FROM STOCK_MASTER").fetchall())
        assert sum(after.values()) == sum(before.values()) - res["units_sold"]
        c.close()
