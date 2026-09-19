"""Sell-out correction: demand from sales the shelf censored.

A day the shelf ran out records less than was wanted. On a one-unit shelf with
true demand 1.1 a day raw sales read ~0.67, and keeping only the days the shelf
never ran out reads 0 -- those are exactly the days nobody came. The censored
estimator reads a sell-out day as demand AT LEAST the sales and recovers the
rate; the shelf's opened/sold-out days are rebuilt from daily receipts and
sales. Replayed on the bread shelf it lifted lines under one a day from 87.8%
to 89.4% fill, above the bakeries' 88.4%.
"""
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import censored_demand as cd


def order_up_to_shelf(S, lam, n=90, seed=0, start=date(2026, 6, 17)):
    """A shelf topped up to S each morning; returns (sales, receipts) by day."""
    rng = np.random.default_rng(seed)
    D = rng.poisson(lam, n)
    left, rec, sold = 0.0, {}, {}
    for i in range(n):
        day = start + timedelta(days=i)
        q = max(0.0, S - left)
        rec[day] = q
        stock = left + q
        s = float(min(D[i], stock))
        sold[day] = s
        left = stock - s
    return sold, rec, start, start + timedelta(days=n - 1)


class TestTheEstimator:
    # Under the heaviest censoring (a one-unit shelf sells out three days in
    # four) the MLE runs a few percent high -- +3.5% over 200 samples -- where
    # raw sales run 40% low; hence the wider tolerance on that case.
    @pytest.mark.parametrize("S,lam,tol", [(1, 1.1, 0.10), (2, 1.8, 0.06), (12, 10.0, 0.06)])
    def test_recovers_the_rate_a_censored_shelf_hides(self, S, lam, tol):
        est = []
        for seed in range(40):
            sold, rec, a, b = order_up_to_shelf(S, lam, seed=seed)
            est.append(cd.censored_weighted_rate(*cd.shelf_signals(sold, rec, a, b, life=5)))
        assert abs(np.mean(est) / lam - 1) < tol

    def test_a_line_that_never_ran_out_reads_its_plain_average(self):
        sold, rec, a, b = order_up_to_shelf(100, 44.0)
        s, o, c = cd.shelf_signals(sold, rec, a, b, life=5)
        assert not any(c)
        assert cd.censored_weighted_rate(s, o, c) == pytest.approx(np.mean(s[-30:]) * 0.6 / 0.6, rel=0.05)

    def test_days_the_line_opened_empty_say_nothing(self):
        assert cd.censored_poisson_rate([0, 0, 0], [False] * 3, [False] * 3) is None
        assert cd.censored_poisson_rate([2, 0, 0], [True, False, False], [False] * 3) == 2.0

    def test_keeping_only_fully_served_days_is_the_wrong_answer(self):
        # the tempting fix: on a one-unit shelf it keeps only zero-demand days
        sold, rec, a, b = order_up_to_shelf(1, 1.1, seed=3)
        s, o, c = cd.shelf_signals(sold, rec, a, b, life=5)
        served_only = np.mean([x for x, cc in zip(s, c) if not cc])
        assert served_only == 0.0
        assert cd.censored_weighted_rate(s, o, c) > 0.8


class TestTheShelfRebuild:
    d0 = date(2026, 9, 1)

    def day(self, i):
        return self.d0 + timedelta(days=i)

    def test_stock_left_is_not_a_sell_out_and_zero_is(self):
        s, o, c = cd.shelf_signals({self.day(0): 2, self.day(1): 3}, {self.day(0): 5}, self.day(0), self.day(1))
        assert o == [True, True] and c == [False, True]

    def test_an_empty_morning_is_not_opened(self):
        s, o, c = cd.shelf_signals({}, {self.day(1): 1}, self.day(0), self.day(1))
        assert o == [False, True]

    def test_units_past_their_life_leave_the_shelf(self):
        # 5 received on day 0, life 2: by day 2 they are gone
        s, o, c = cd.shelf_signals({}, {self.day(0): 5}, self.day(0), self.day(2), life=2)
        assert o == [True, True, False]


class TestTheGuards:
    d0 = date(2026, 6, 17)

    def test_needs_thirty_covered_days(self):
        sold, rec, a, b = order_up_to_shelf(1, 1.1)
        assert cd.correct_line(sold, rec, 0.5, a, b, (b - timedelta(days=20), b), 5) is None

    def test_walks_only_the_covered_days(self):
        # a source that stops early: later sales must not read as sell-outs.
        # The shelf never runs out, so any sell-out would be the uncovered tail.
        sold, rec, a, b = order_up_to_shelf(100, 44.0)
        cut = a + timedelta(days=59)
        rec_cut = {d: q for d, q in rec.items() if d <= cut}
        assert cd.correct_line(sold, rec_cut, 44.0, a, b, (a, cut), 5) is None
        # ...whereas walking the whole window over those receipts would invent them
        s, o, c = cd.shelf_signals(sold, rec_cut, a, b, life=5)
        assert sum(c) > 20

    def test_never_lowers_the_measured_rate(self):
        sold, rec, a, b = order_up_to_shelf(1, 1.1, seed=3)
        assert cd.correct_line(sold, rec, 5.0, a, b, (a, b), 5) is None

    def test_raises_a_censored_line(self):
        sold, rec, a, b = order_up_to_shelf(1, 1.1, seed=3)
        lam, n = cd.correct_line(sold, rec, 0.67, a, b, (a, b), 5)
        assert lam > 0.9 and n > 30


def _grn_files(tmp_path, rows, pos=None):
    g = pd.DataFrame(rows, columns=["GRN No", "GRN Date", "Bar Code", "GRN Qty", "FOC Qty", "PO No"])
    gp = tmp_path / "grn.xlsx"
    g.to_excel(gp, index=False)
    pp = None
    if pos:
        pp = tmp_path / "po.xlsx"
        pd.DataFrame(pos, columns=["PO NO", "PO DATE"]).to_excel(pp, index=False)
    return str(gp), (str(pp) if pp else None)


class TestTheGrnExport:
    def test_counts_free_units_and_moves_sunday_deliveries_back(self, tmp_path):
        gp, pp = _grn_files(tmp_path, [
            ["G1", "07-Sep-2026", "B1", 4, 1, "P1"],     # Monday GRN, Saturday PO -> Sunday 6 Sep
            ["G2", "07-Sep-2026", "B1", 2, 0, "P2"],     # Monday GRN, Sunday PO   -> stays Monday
        ], pos=[["P1", "05-Sep-2026"], ["P2", "06-Sep-2026"]])
        src = cd.GrnExportReceipts(gp, pp)
        got = src.daily_receipts(["B1"], date(2026, 9, 1), date(2026, 9, 30))["B1"]
        assert got == {date(2026, 9, 6): 5.0, date(2026, 9, 7): 2.0}
        assert src.coverage() == (date(2026, 9, 6), date(2026, 9, 7))


class TestTheAdapter:
    """End to end: a POS whose one-unit bread line sold out most days."""

    def test_raises_the_censored_line_and_leaves_the_rest(self, tmp_path, monkeypatch):
        from oasis.logic.db_connector import SchemaMapper, UniversalConnector
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        sold, rec, a, b = order_up_to_shelf(1, 1.1, seed=5)
        db = tmp_path / "pos.db"
        c = sqlite3.connect(db)
        c.execute("CREATE TABLE POS_SALES_DTL (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT, SERIAL_NO INT, ITM_CD TEXT,"
                  " ITEM_NAME TEXT, QTY REAL, SELL_PRICE REAL, NET_AMT REAL, VOID_FLAG TEXT)")
        for i, (d, q) in enumerate(sold.items()):
            if q:
                c.execute("INSERT INTO POS_SALES_DTL VALUES ('ORG001',?,?,1,'B1','LOAF',?,50,50,'F')", (f"L{i}", d.isoformat(), q))
                c.execute("INSERT INTO POS_SALES_DTL VALUES ('ORG001',?,?,1,'X9','OTHER',?,50,50,'F')", (f"O{i}", d.isoformat(), 3))
        c.commit(); c.close()
        gp, _ = _grn_files(tmp_path, [[f"G{i}", d.strftime("%d-%b-%Y"), "B1", q, 0, f"P{i}"]
                                      for i, (d, q) in enumerate(rec.items()) if q])
        monkeypatch.setenv("OASIS_GRN_EXPORT", gp)
        monkeypatch.setenv("OASIS_AS_OF", b.isoformat())
        adapter = PosErpAdapter(UniversalConnector(f"sqlite:///{db}", SchemaMapper.for_pos_erp()))
        products = [{"item_code": "B1", "product_name": "LOAF", "department": "BREAD", "avg_daily_sales": 0.67,
                     "ads_source": "pos_weighted"},
                    {"item_code": "X9", "product_name": "OTHER", "department": "BREAD", "avg_daily_sales": 3.0,
                     "ads_source": "pos_weighted"}]
        assert adapter._apply_censored_demand("ORG001", products) == 1
        loaf, other = products
        assert loaf["avg_daily_sales"] > 0.9 and loaf["ads_uncensored"] == 0.67
        assert loaf["ads_source"] == "pos_weighted+censored" and loaf["sellout_days"] > 30
        assert other["avg_daily_sales"] == 3.0 and "ads_uncensored" not in other

    def test_off_without_a_receipts_source(self, tmp_path, monkeypatch):
        from oasis.logic.db_connector import SchemaMapper, UniversalConnector
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        monkeypatch.delenv("OASIS_GRN_EXPORT", raising=False)
        db = tmp_path / "empty.db"
        sqlite3.connect(db).close()
        adapter = PosErpAdapter(UniversalConnector(f"sqlite:///{db}", SchemaMapper.for_pos_erp()))
        p = [{"item_code": "B1", "avg_daily_sales": 0.67}]
        assert adapter._apply_censored_demand("ORG001", p) == 0 and p[0]["avg_daily_sales"] == 0.67
