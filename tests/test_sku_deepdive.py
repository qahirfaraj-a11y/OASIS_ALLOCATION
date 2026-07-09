"""Tests for the SKU deep dive (verdict rules + snapshot dedupe)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.sku_deepdive import classify_sku, infer_dept, load_snapshot


class TestClassify:
    def test_restock_now(self):
        v, _ = classify_sku(ads=0.5, stock=0, days_cover=None,
                            months_sold=9, units_total=137, n_months=9)
        assert v == "RESTOCK NOW"

    def test_paper_delist(self):
        v, _ = classify_sku(0.0, 0, None, 0, 0, 9)
        assert v == "DELIST (paper)"

    def test_clear_and_delist(self):
        v, _ = classify_sku(0.0, 12, None, 0, 0, 9)
        assert v == "CLEAR & DELIST"

    def test_review_sporadic(self):
        # sold twice, 4 units total, has stock → rationalisation candidate
        v, why = classify_sku(0.015, 10, 680.0, 2, 4, 9)
        assert v == "REVIEW" and "sporadic" in why

    def test_reduce_overstock(self):
        v, _ = classify_sku(1.0, 200, 200.0, 9, 273, 9)
        assert v == "REDUCE"

    def test_retain_core_thin_cover(self):
        v, _ = classify_sku(10.0, 40, 4.0, 9, 2736, 9)
        assert v == "RETAIN-CORE"

    def test_retain_default(self):
        v, _ = classify_sku(1.0, 30, 30.0, 9, 273, 9)
        assert v == "RETAIN"


class TestSnapshot:
    def test_infer_dept(self):
        assert infer_dept("06.7.2026_beer.xlsx") == "BEER"
        assert infer_dept("spirits_06_jul_2026.xlsx") == "SPIRITS"
        assert infer_dept("6.7.2026_wine.xlsx") == "WINES"
        assert infer_dept("06.7.2026_ciders.xlsx") == "CIDERS"

    def test_dedupe_first_file_wins(self, tmp_path):
        import pandas as pd
        cols = ["VENDOR_NAME", "BARCODE", "ITM_NAME", "SellPrice Prev",
                "SellPrice", "STOCK"]
        beer = tmp_path / "x_beer.xlsx"
        cider = tmp_path / "x_cider.xlsx"
        pd.DataFrame([["KBL", "111", "Tusker", 209, 209, 50]],
                     columns=cols).to_excel(beer, index=False)
        pd.DataFrame([["KBL", "111", "Tusker", 209, 209, 50],
                      ["SOM", "222", "Somersby", 299, 299, 27]],
                     columns=cols).to_excel(cider, index=False)
        rows, dupes = load_snapshot([str(beer), str(cider)])
        assert dupes == 1
        by_bc = {r["barcode"]: r for r in rows}
        assert by_bc["111"]["dept"] == "BEER"       # first file claimed it
        assert by_bc["222"]["dept"] == "CIDERS"


class TestLiveDb:
    """User-picked department deep-dive on a live OASIS install."""

    def _mini_db(self, tmp_path):
        import sqlite3
        db = str(tmp_path / "mini.db")
        c = sqlite3.connect(db)
        c.executescript("""
            CREATE TABLE ITEM_MST (ITM_CD TEXT PRIMARY KEY, ITM_LONG_NAME TEXT,
                                   DEPARTMENT TEXT, SUPPLIER_CD TEXT);
            CREATE TABLE STOCK_MASTER (SM_ITM_CD TEXT, SM_ORG_CD TEXT, SM_QTY REAL);
            CREATE TABLE BASIC_SP_MST (BSP_ITEM_CD TEXT, BSP_ORG_CD TEXT, BSP_SP REAL);
            CREATE TABLE SUPPLIER_MST (SUPPLIER_CD TEXT, SUPPLIER_NAME TEXT);
            CREATE TABLE POS_SALES_DTL (ORG_CD TEXT, BILL_DT TEXT, ITM_CD TEXT,
                                        QTY REAL, VOID_FLAG TEXT);
            INSERT INTO ITEM_MST VALUES ('A','Milk','DAIRY','S1');
            INSERT INTO ITEM_MST VALUES ('B','Cheese','DAIRY','S1');
            INSERT INTO ITEM_MST VALUES ('C','Bread','BAKERY','S2');
            INSERT INTO STOCK_MASTER VALUES ('A','ORG001',10);
            INSERT INTO STOCK_MASTER VALUES ('B','ORG001',0);
            INSERT INTO STOCK_MASTER VALUES ('C','ORG001',5);
            INSERT INTO BASIC_SP_MST VALUES ('A','ORG001',100);
            INSERT INTO BASIC_SP_MST VALUES ('B','ORG001',300);
            INSERT INTO BASIC_SP_MST VALUES ('C','ORG001',50);
            INSERT INTO SUPPLIER_MST VALUES ('S1','DairyCo');
            INSERT INTO POS_SALES_DTL VALUES ('ORG001','2026-06-01','A',30,'F');
            INSERT INTO POS_SALES_DTL VALUES ('ORG001','2026-07-01','A',30,'F');
            INSERT INTO POS_SALES_DTL VALUES ('ORG001','2026-06-01','B',15,'F');
        """)
        c.commit()
        c.close()
        return db

    def test_list_departments(self, tmp_path):
        from oasis.logic.sku_deepdive import list_departments_from_db
        db = self._mini_db(tmp_path)
        depts = {d["dept"]: d for d in list_departments_from_db(db)}
        assert set(depts) == {"DAIRY", "BAKERY"}
        assert depts["DAIRY"]["skus"] == 2 and depts["DAIRY"]["in_stock"] == 1

    def test_live_deepdive_verdicts(self, tmp_path):
        from oasis.logic.sku_deepdive import build_sku_deepdive_live
        db = self._mini_db(tmp_path)
        a = build_sku_deepdive_live(db, ["DAIRY"])
        by = {s["Barcode"]: s for s in a["skus"]}
        assert set(by) == {"A", "B"}         # BAKERY excluded
        # A sells (60 units, 10 on hand, ~2 mo) — has cover, verdict is one of
        # the sold-with-stock buckets
        assert by["A"]["Verdict"] in ("RETAIN", "RETAIN-CORE", "REDUCE", "REVIEW")
        assert by["A"]["ADS"] > 0
        # B: sold once (15 units, 1 month) but stock=0 -> RESTOCK NOW
        assert by["B"]["Verdict"] == "RESTOCK NOW"

    def test_multi_department_selection(self, tmp_path):
        from oasis.logic.sku_deepdive import build_sku_deepdive_live
        db = self._mini_db(tmp_path)
        a = build_sku_deepdive_live(db, ["DAIRY", "BAKERY"])
        assert a["n_skus"] == 3   # all 3 SKUs across both depts
