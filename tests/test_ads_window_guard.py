"""Observed-window guard for weighted ADS (short-history normalisation).

A fresh install with N days of sales must divide by N, not a fixed 30 —
otherwise demand is understated ~30/N times and first orders come out tiny.
"""

import os
import sqlite3
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.db_connector import SchemaMapper, UniversalConnector
from oasis.logic.pos_erp_adapter import PosErpAdapter


def _db_with_days(path, days, qty_per_day=10.0):
    """Mini POS DB where item X sells qty_per_day units on each of `days` days."""
    c = sqlite3.connect(path)
    c.executescript("""
        CREATE TABLE POS_SALES_DTL (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT,
            SERIAL_NO INT, ITM_CD TEXT, ITEM_NAME TEXT, QTY REAL, SELL_PRICE REAL,
            NET_AMT REAL, VOID_FLAG TEXT);
    """)
    today = datetime.now().date()
    for d in range(days):
        bdt = (today - timedelta(days=d)).isoformat()
        c.execute("INSERT INTO POS_SALES_DTL VALUES ('ORG001',?,?,1,'X','Item X',?,50,500,'F')",
                  (f"B{d}", bdt, qty_per_day))
    c.commit()
    c.close()


def _adapter(path):
    return PosErpAdapter(UniversalConnector(f"sqlite:///{path}", SchemaMapper.for_pos_erp()))


class TestObservedWindowGuard:
    def test_two_day_history_yields_true_rate(self, tmp_path):
        db = str(tmp_path / "short.db")
        _db_with_days(db, days=2, qty_per_day=10)
        ads = _adapter(db)._calc_weighted_ads("ORG001")
        # true rate is 10/day; the old fixed-30 divisor reported 20/30*0.6 = 0.4
        assert abs(ads["X"]["weighted_ads"] - 10.0) < 0.01

    def test_full_history_unchanged(self, tmp_path):
        db = str(tmp_path / "full.db")
        _db_with_days(db, days=90, qty_per_day=10)
        ads = _adapter(db)._calc_weighted_ads("ORG001")
        # steady 10/day across all buckets → weighted stays 10/day
        assert abs(ads["X"]["weighted_ads"] - 10.0) < 0.35  # bucket-boundary rounding

    def test_forty_day_history_renormalises(self, tmp_path):
        db = str(tmp_path / "mid.db")
        _db_with_days(db, days=40, qty_per_day=10)
        ads = _adapter(db)._calc_weighted_ads("ORG001")
        # buckets: 30d full (10/day), 30-60d has 10 observed days (10/day),
        # 60-90 unobserved → renormalised weighted = 10/day
        assert abs(ads["X"]["weighted_ads"] - 10.0) < 0.35
