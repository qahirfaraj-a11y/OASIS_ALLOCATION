"""A line is measured over the days it has existed, not the store's.

The observed-window guard was per store (MIN(BILL_DT) over the whole org), so in
an established store a line launched 20 days ago was divided over 90 and the 70
days before it existed counted as zero sales: a unit a day read as 0.4. Now both
adapters measure each line from the later of its first sale and the store's
(demand_rate.line_observed_days). A line that sold 60-90 days ago existed all
window and keeps the store's window.
"""
import os
import sqlite3
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import demand_rate as dr
from oasis.logic.db_connector import SchemaMapper, UniversalConnector
from oasis.logic.pos_erp_adapter import PosErpAdapter


def _db(path, lines):
    """lines: {itm_cd: [(days_ago, qty), ...]}"""
    c = sqlite3.connect(path)
    c.executescript("""
        CREATE TABLE POS_SALES_DTL (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT,
            SERIAL_NO INT, ITM_CD TEXT, ITEM_NAME TEXT, QTY REAL, SELL_PRICE REAL,
            NET_AMT REAL, VOID_FLAG TEXT);
    """)
    today = datetime.now().date()
    n = 0
    for itm, sales in lines.items():
        for ago, qty in sales:
            n += 1
            c.execute("INSERT INTO POS_SALES_DTL VALUES ('ORG001',?,?,1,?,?,?,50,500,'F')",
                      (f"B{n}", (today - timedelta(days=ago)).isoformat(), itm, itm, qty))
    c.commit(); c.close()


def _ads(path):
    return PosErpAdapter(UniversalConnector(f"sqlite:///{path}", SchemaMapper.for_pos_erp()))._calc_weighted_ads("ORG001")


STORE = {"OLD": [(d, 10.0) for d in range(200)]}          # the store has 200 days of history


class TestThePosAdapter:
    def test_a_line_launched_twenty_days_ago_reads_its_true_rate(self, tmp_path):
        db = str(tmp_path / "launch.db")
        _db(db, dict(STORE, NEW=[(d, 1.0) for d in range(20)]))
        ads = _ads(db)
        assert abs(ads["NEW"]["weighted_ads"] - 1.0) < 0.02        # was 0.4
        assert abs(ads["NEW"]["ads_30d"] - 1.0) < 0.02

    def test_an_established_line_keeps_the_store_window(self, tmp_path):
        db = str(tmp_path / "old.db")
        _db(db, STORE)
        assert abs(_ads(db)["OLD"]["weighted_ads"] - 10.0) < 0.35

    def test_a_slow_line_that_sold_before_the_window_is_not_new(self, tmp_path):
        # nothing 60-90 days ago, but it sold 120 days ago: it existed all window
        db = str(tmp_path / "slow.db")
        _db(db, dict(STORE, SLOW=[(120, 3.0), (10, 3.0)]))
        slow = _ads(db)["SLOW"]["weighted_ads"]
        assert abs(slow - 0.6 * 3.0 / 30) < 0.01                 # divided over the full window


class TestTheRule:
    now = datetime(2026, 9, 15)

    def test_the_later_of_the_two_starts_counts(self):
        assert dr.line_observed_days(self.now - timedelta(days=19), self.now - timedelta(days=400), self.now) == 20
        assert dr.line_observed_days(self.now - timedelta(days=400), self.now - timedelta(days=19), self.now) == 20

    def test_unknown_starts_fall_back_to_the_full_window(self):
        assert dr.line_observed_days(None, None, self.now) == dr.WINDOW_DAYS


class TestTheOdooAdapter:
    def _adapter(self, rows=None, fail=False):
        from oasis.logic.odoo_adapter import OdooAdapter
        a = OdooAdapter.__new__(OdooAdapter)
        a._warehouse_scope = lambda org: None

        def ex(model, method, args, kw=None):
            if fail:
                raise RuntimeError("read denied")
            assert (model, method) == ("stock.move", "read_group")
            return rows
        a._ex = ex
        return a

    def test_reads_each_products_first_customer_move(self):
        a = self._adapter([{"product_id": [7, "Loaf"], "date": "2026-08-27 06:10:00"}])
        assert a._line_first_sales("ORG", [7]) == {7: datetime(2026, 8, 27, 6, 10)}

    def test_a_read_failure_falls_back_to_the_store_window(self):
        assert self._adapter(fail=True)._line_first_sales("ORG", [7]) == {}
