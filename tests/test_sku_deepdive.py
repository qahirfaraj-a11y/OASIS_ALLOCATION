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
