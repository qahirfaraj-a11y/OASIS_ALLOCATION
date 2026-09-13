"""ITM_CD and SCAN_ITM_CD are two identities, and the build must keep both.

The catalogue builder wrote the barcode into BOTH columns, so all 39,728 rows
had ITM_CD == SCAN_ITM_CD. The ordering methodology matches item code, THEN
barcode, then name -- with one value in both slots the first two branches are
the same test run twice, which is why nothing ever joined to the cashier
exports, whose "Itm Code" is the till's article number and shares no digits
with a barcode.

Two traps found while fixing it, both pinned below:

  ITM_CD is the primary key and the insert is INSERT OR REPLACE, so a till
  code claimed by two catalogue rows silently DELETES one. 114 such codes cost
  645 items on the first attempt.

  Prices and stock are looked up by ITM_CD. The moment ITM_CD changed they had
  to change with it, or every price and every on-hand quantity orphans.
"""
import os
import sqlite3

import pytest

from oasis.logic.mock_pos_build import build_pos_db_from_catalog


def _rows():
    return [
        # barcode, name, till code assigned later
        {"itm_cd": "5011417565452", "name": "AIRWICK 375ML CITRUS",
         "dept": "CLEANING", "vendor": "ACME", "price": 495.0, "stock": 12.0},
        {"itm_cd": "6009610256313", "name": "BIC ORANGE FINE BLACK PEN",
         "dept": "STATIONERY", "vendor": "ACME", "price": 30.0, "stock": 4.0},
        {"itm_cd": "0003004575833", "name": "VGG 500G HONEY",
         "dept": "GROCERY", "vendor": "BETA", "price": 800.0, "stock": 0.0},
    ]


@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "t.db")


def _read(db):
    c = sqlite3.connect(db)
    try:
        items = list(c.execute(
            "SELECT ITM_CD, SCAN_ITM_CD, ITM_LONG_NAME FROM ITEM_MST"))
        stock = dict(c.execute(
            "SELECT SM_ITM_CD, SM_LAST_RECV_DT FROM STOCK_MASTER"))
        joined = c.execute(
            "SELECT COUNT(*) FROM STOCK_MASTER s JOIN ITEM_MST i "
            "ON s.SM_ITM_CD = i.ITM_CD").fetchone()[0]
        priced = c.execute(
            "SELECT COUNT(*) FROM BASIC_SP_MST p JOIN ITEM_MST i "
            "ON p.BSP_ITEM_CD = i.ITM_CD").fetchone()[0]
        return items, stock, joined, priced
    finally:
        c.close()


class TestTheTwoCodesStaySeparate:
    def test_a_till_code_becomes_ITM_CD_and_the_barcode_stays_scannable(self, db):
        rows = _rows()
        rows[0]["till_cd"] = "1002000900832"
        build_pos_db_from_catalog(rows, db, seed_password="x")
        items, _, _, _ = _read(db)
        by_name = {n: (i, s) for i, s, n in items}
        itm, scan = by_name["AIRWICK 375ML CITRUS"]
        assert itm == "1002000900832", "ITM_CD did not take the till code"
        assert scan == "5011417565452", "the barcode was lost"
        assert itm != scan

    def test_without_a_till_code_the_barcode_does_both_jobs(self, db):
        """An item absent from ten months of sales still has to exist."""
        build_pos_db_from_catalog(_rows(), db, seed_password="x")
        items, _, _, _ = _read(db)
        for itm, scan, _ in items:
            assert itm == scan


class TestNothingIsLost:
    def test_every_catalogue_row_survives(self, db):
        rows = _rows()
        rows[0]["till_cd"] = "1002000900832"
        build_pos_db_from_catalog(rows, db, seed_password="x")
        items, _, _, _ = _read(db)
        assert len(items) == len(rows)

    def test_prices_and_stock_follow_ITM_CD(self, db):
        """They are looked up BY ITM_CD; if they keep the barcode when ITM_CD
        becomes the till code, every price and quantity orphans."""
        rows = _rows()
        rows[0]["till_cd"] = "1002000900832"
        build_pos_db_from_catalog(rows, db, seed_password="x")
        _, _, joined, priced = _read(db)
        assert joined == len(rows), "stock rows orphaned from ITEM_MST"
        assert priced == len(rows), "price rows orphaned from ITEM_MST"


class TestReceiptDatesVaryPerSku:
    def test_a_supplied_date_is_written_per_item(self, db):
        recv = {"AIRWICK 375ML CITRUS": "2025-03-04",
                "VGG 500G HONEY": "2025-11-30"}
        build_pos_db_from_catalog(_rows(), db, seed_password="x",
                                  recv_dates=recv)
        _, stock, _, _ = _read(db)
        assert stock["5011417565452"] == "2025-03-04"
        assert stock["0003004575833"] == "2025-11-30"
        assert len({v for v in stock.values()}) > 1, (
            "one flat receipt date makes days_since_delivery a constant, and "
            "the stale-fresh and dead-stock gates can then never fire")

    def test_an_unknown_item_falls_back_to_the_build_date(self, db):
        build_pos_db_from_catalog(_rows(), db, seed_password="x",
                                  recv_dates={"AIRWICK 375ML CITRUS": "2025-03-04"})
        _, stock, _, _ = _read(db)
        assert stock["5011417565452"] == "2025-03-04"
        assert stock["6009610256313"] not in (None, "")

    def test_no_receipt_history_still_builds(self, db):
        build_pos_db_from_catalog(_rows(), db, seed_password="x", recv_dates=None)
        items, stock, _, _ = _read(db)
        assert len(items) == 3 and len(stock) == 3
