"""Tests for the category deep-dive report (pure aggregation)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.category_report import build_category_report


def _write_cash(path, rows):
    """Write a *_cash.xlsx in the real 2-header-row layout (Item Name/Itm Code/Qty)."""
    import pandas as pd
    data = [["", "", "027 - Store", ""],
            ["Item Name", "Itm Code", "Qty", "Cashier"]]
    for name, qty in rows:
        data.append([name, "000", qty, "1"])
    pd.DataFrame(data).to_excel(path, header=False, index=False)


def _catalog():
    return [
        {"name": "TUSKER 500ML LAGER", "dept": "BEER", "vendor": "KBL",
         "price": 200.0, "stock": 100.0},   # sells + stock → mover
        {"name": "GILBEYS GIN 750ML", "dept": "SPIRITS", "vendor": "UDV",
         "price": 1500.0, "stock": 0.0},     # sells, no stock → ghost
        {"name": "OLD DUSTY WINE", "dept": "WINES", "vendor": "ACME",
         "price": 900.0, "stock": 10.0},     # stock, no sales → dead 9000
        {"name": "BREAD 400G", "dept": "BAKERY", "vendor": "X",
         "price": 60.0, "stock": 50.0},      # not alcohol → excluded
    ]


class TestBuildCategoryReport:
    def test_full_section_analysis(self, tmp_path):
        # 3 months: two normal, one truncated (partial) → excluded from ADS
        _write_cash(str(tmp_path / "jan_cash.xlsx"),
                    [("TUSKER 500ML LAGER", 300), ("GILBEYS GIN 750ML", 30)])
        _write_cash(str(tmp_path / "feb_cash.xlsx"),
                    [("TUSKER 500ML LAGER", 300), ("GILBEYS GIN 750ML", 30)])
        _write_cash(str(tmp_path / "mar_cash.xlsx"),
                    [("TUSKER 500ML LAGER", 5)])          # truncated month

        a = build_category_report(_catalog(), str(tmp_path),
                                  ["WINES", "SPIRITS", "BEER", "CIDERS"])
        assert a["skus"] == 3 and a["in_stock"] == 2       # bread excluded
        assert "Mar" in a["partial_months"] and a["months_used"] == 2
        # dead stock = OLD DUSTY WINE 10 × 900
        assert a["dead_skus"] == 1 and a["dead_value"] == 9000.0
        # ghost = GILBEYS (sells, zero stock)
        assert a["ghost_sellers"] == 1
        assert a["top_ghost"][0]["Item"].startswith("GILBEYS")
        # top mover is TUSKER by daily revenue
        assert a["top_movers"][0]["Item"].startswith("TUSKER")
        # department rollup covers the 4 depts present as catalogue rows
        depts = {d["name"] for d in a["dept_rollup"]}
        assert {"BEER", "SPIRITS", "WINES"} <= depts

    def test_seasonality_flags_partial(self, tmp_path):
        _write_cash(str(tmp_path / "jan_cash.xlsx"), [("TUSKER 500ML LAGER", 300)])
        _write_cash(str(tmp_path / "feb_cash.xlsx"), [("TUSKER 500ML LAGER", 300)])
        _write_cash(str(tmp_path / "mar_cash.xlsx"), [("TUSKER 500ML LAGER", 5)])
        a = build_category_report(_catalog(), str(tmp_path), ["BEER"])
        sea = {s["month"]: s for s in a["seasonality"]}
        assert sea["Mar"]["partial"] is True
        assert sea["Jan"]["partial"] is False and sea["Jan"]["units"] == 300
