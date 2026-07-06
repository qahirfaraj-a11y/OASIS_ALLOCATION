"""Tests for GRN cost aggregation + injection + PDF rendering."""

import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.grn_cost import aggregate_costs, inject_costs_to_db
from oasis.logic.report_pdf import markdown_to_pdf


class TestAggregateCosts:
    def test_quantity_weighted_average(self):
        # 10 @ 100 and 30 @ 200 → WAC = (1000+6000)/40 = 175
        recs = [
            {"barcode": "X", "cost": 100, "qty": 10, "sp": 250, "vendor": "V1"},
            {"barcode": "X", "cost": 200, "qty": 30, "sp": 260, "vendor": "V2"},
        ]
        out = aggregate_costs(recs)
        assert abs(out["X"]["cost"] - 175.0) < 1e-6
        assert out["X"]["qty_received"] == 40 and out["X"]["receipts"] == 2
        assert out["X"]["sp"] == 260.0 and out["X"]["vendor"] == "V2"  # last seen

    def test_skips_bad_rows(self):
        recs = [
            {"barcode": "", "cost": 100, "qty": 1},       # no barcode
            {"barcode": "Y", "cost": 0, "qty": 1},        # zero cost
            {"barcode": "Y", "cost": "n/a", "qty": 1},    # non-numeric
            {"barcode": "Y", "cost": 50, "qty": 0},       # zero qty → counts as 1
        ]
        out = aggregate_costs(recs)
        assert out["Y"]["cost"] == 50.0 and out["Y"]["qty_received"] == 1


class TestInject:
    def test_updates_matched_barcodes(self, tmp_path):
        db = str(tmp_path / "s.db")
        c = sqlite3.connect(db)
        c.executescript("""
            CREATE TABLE ITEM_MST (ITM_CD TEXT);
            CREATE TABLE BASIC_CP_MST (BCP_ITEM_CD TEXT, BCP_CP REAL);
            CREATE TABLE STOCK_MASTER (SM_ITM_CD TEXT, SM_WAC REAL);
            INSERT INTO ITEM_MST VALUES ('X'),('Z');
            INSERT INTO BASIC_CP_MST VALUES ('X',0),('Z',0);
            INSERT INTO STOCK_MASTER VALUES ('X',0),('Z',0);
        """)
        c.commit()
        c.close()
        res = inject_costs_to_db(db, {"X": {"cost": 175.0}, "GHOST": {"cost": 9.0}})
        assert res["matched"] == 1                        # only X is in the catalog
        c = sqlite3.connect(db)
        assert c.execute("SELECT BCP_CP FROM BASIC_CP_MST WHERE BCP_ITEM_CD='X'").fetchone()[0] == 175.0
        assert c.execute("SELECT SM_WAC FROM STOCK_MASTER WHERE SM_ITM_CD='X'").fetchone()[0] == 175.0
        c.close()


class TestPdf:
    def test_renders_pdf_with_table(self, tmp_path):
        import pytest
        pytest.importorskip("fpdf")   # fpdf2 is an optional report dependency
        md = ("# Title\n\n## Section\n\nSome **bold** text.\n\n"
              "| A | B |\n|---|---|\n| 1 | 2 |\n| 3 | 4 |\n")
        out = str(tmp_path / "r.pdf")
        markdown_to_pdf(md, out, title="Test")
        assert os.path.exists(out)
        with open(out, "rb") as f:
            assert f.read(5) == b"%PDF-"
        assert os.path.getsize(out) > 500
