"""Deriving LATA's supplier rhythm from Odoo goods receipts.

Every horizon the transfer engine uses comes from this rhythm. lata_shield only
ENRICHES it, and the only thing that ever produced it scanned po_*.xlsx off
disk — so a customer whose history lives in Odoo had nothing to enrich and the
engine answered a flat 14 to every horizon.
"""

import json
import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.odoo_supplier_rhythm import (
    GAPS_FILE, PATTERNS_FILE, PER_STORE_FILE, derive, format_report,
)


def _picks(supplier, days_apart, count, warehouse_type=1, start_days_ago=400,
           po_id=None):
    """`count` receipts from one supplier, `days_apart` apart."""
    base = datetime.now() - timedelta(days=start_days_ago)
    out = []
    for i in range(count):
        when = base + timedelta(days=i * days_apart)
        out.append({
            "id": 1000 + i,
            "partner_id": [7, supplier],
            "date_done": when.strftime("%Y-%m-%d %H:%M:%S"),
            "picking_type_id": [warehouse_type, "Receipts"],
            "purchase_id": [po_id, "PO1"] if po_id else False,
            "location_dest_id": [8, "WH/Stock"],
        })
    return out


class FakeOdoo:
    SUPPLIERINFO_READ_LIMIT = 20000

    def __init__(self, picks, supplierinfo=None, orders=None):
        self.url, self.db = "http://fake", "fake"
        self._picks = picks
        self._supplierinfo = supplierinfo or []
        self._orders = orders or []
        self.calls = []

    def _ex(self, model, method, args, kw=None):
        self.calls.append((model, method))
        if model == "stock.picking":
            return self._picks
        if model == "stock.picking.type":
            return [{"id": 1, "warehouse_id": [1, "Alpha"]},
                    {"id": 2, "warehouse_id": [2, "Beta"]}]
        if model == "stock.warehouse":
            return [{"id": 1, "code": "A1"}, {"id": 2, "code": "B2"}]
        if model == "product.supplierinfo":
            return self._supplierinfo
        if model == "purchase.order":
            return self._orders
        return []


class TestItDerivesARhythm:

    def test_a_weekly_supplier_reads_as_weekly(self):
        r = derive(adapter=FakeOdoo(_picks("Brookside", 7, 10)), write=False)
        assert r["suppliers"] == 1
        assert r["patterns"]["BROOKSIDE"]["median_gap_days"] == 7

    def test_a_daily_supplier_reads_as_daily(self):
        r = derive(adapter=FakeOdoo(_picks("Daily Co", 1, 20)), write=False)
        assert r["patterns"]["DAILY CO"]["median_gap_days"] == 1

    def test_too_few_receipts_yield_no_rhythm(self):
        """One gap is an anecdote. The engine's own fallback is safer."""
        r = derive(adapter=FakeOdoo(_picks("Rare", 30, 2)), write=False)
        assert r["suppliers"] == 0

    def test_confidence_reflects_how_much_evidence_there_was(self):
        thin = derive(adapter=FakeOdoo(_picks("Thin", 7, 3)), write=False)
        thick = derive(adapter=FakeOdoo(_picks("Thick", 7, 12)), write=False)
        assert thin["patterns"]["THIN"]["confidence"] == "LOW"
        assert thick["patterns"]["THICK"]["confidence"] == "HIGH"

    def test_one_delivery_split_across_pickings_is_one_arrival(self):
        """Three pickings on the SAME day is one delivery. Counting them as
        three arrivals would report a daily cadence for a weekly supplier."""
        same_day = _picks("Split", 0, 3) + _picks("Split", 7, 5,
                                                  start_days_ago=380)
        r = derive(adapter=FakeOdoo(same_day), write=False)
        assert r["patterns"]["SPLIT"]["median_gap_days"] == 7

    def test_an_absurd_gap_is_excluded(self):
        """A supplier that last delivered a year ago has a discontinued line or
        a data hole, not a 365-day cadence — and a median must not swallow it."""
        picks = _picks("Gappy", 7, 6)
        picks += _picks("Gappy", 7, 4, start_days_ago=20)   # a long hole between
        r = derive(adapter=FakeOdoo(picks), write=False)
        assert r["patterns"]["GAPPY"]["median_gap_days"] <= 14


class TestLeadTimeProvenance:

    def test_a_measured_lead_beats_the_suppliers_own_claim(self):
        """PO placed -> goods arrived is the only place promise and arrival can
        be compared. The stated delay is the number LATA exists to distrust."""
        ordered = (datetime.now() - timedelta(days=405)).strftime("%Y-%m-%d %H:%M:%S")
        fake = FakeOdoo(
            _picks("Measured", 7, 6, po_id=55),
            supplierinfo=[{"partner_id": [7, "Measured"], "delay": 99}],
            orders=[{"id": 55, "date_order": ordered}])
        r = derive(adapter=fake, write=False)
        rec = r["patterns"]["MEASURED"]
        assert rec["lead_time_source"] == "measured"
        assert rec["estimated_delivery_days"] != 99

    def test_the_stated_lead_is_used_only_when_nothing_was_measured(self):
        fake = FakeOdoo(_picks("Claimed", 7, 6),
                        supplierinfo=[{"partner_id": [7, "Claimed"], "delay": 4}])
        rec = derive(adapter=fake, write=False)["patterns"]["CLAIMED"]
        assert rec["lead_time_source"] == "stated"
        assert rec["estimated_delivery_days"] == 4


class TestPerStore:

    def test_the_same_supplier_can_have_two_rhythms(self):
        """A supplier delivering daily to the flagship and fortnightly to the
        forecourt is one supplier with two rhythms — the whole reason per-store
        cadence is worth having."""
        picks = (_picks("Split Co", 1, 12, warehouse_type=1)
                 + _picks("Split Co", 14, 8, warehouse_type=2))
        r = derive(adapter=FakeOdoo(picks), write=False)
        per_store = r["per_store"]
        assert per_store["A1"]["SPLIT CO"]["median_gap_days"] == 1
        assert per_store["B2"]["SPLIT CO"]["median_gap_days"] == 14

    def test_store_pairs_are_counted(self):
        picks = (_picks("S", 7, 6, warehouse_type=1)
                 + _picks("S", 7, 6, warehouse_type=2))
        r = derive(adapter=FakeOdoo(picks), write=False)
        assert r["store_supplier_pairs"] == 2
        assert r["stores_with_rhythm"] == 2


class TestItNeverDestroysGoodData:
    """It wrote an EMPTY patterns file over a working 599-supplier one.

    Nothing errored. The engine would simply have started answering 14 to every
    horizon, on a customer site, with the evidence gone. A derivation that finds
    less than what is there is a fact about the READ, not about the suppliers.
    """

    def _existing(self, tmp_path, n):
        payload = {f"supplier {i}": {"median_gap_days": 3} for i in range(n)}
        (tmp_path / PATTERNS_FILE).write_text(json.dumps(payload), encoding="utf-8")
        return payload

    def test_an_empty_derivation_refuses_to_write(self, tmp_path):
        self._existing(tmp_path, 599)
        r = derive(adapter=FakeOdoo([]), data_dir=str(tmp_path), write=True)
        assert r["refused"], "it wrote nothing over something"
        after = json.loads((tmp_path / PATTERNS_FILE).read_text(encoding="utf-8"))
        assert len(after) == 599, "the existing rhythm was destroyed"

    def test_a_thinner_derivation_refuses_to_replace_a_richer_one(self, tmp_path):
        self._existing(tmp_path, 599)
        r = derive(adapter=FakeOdoo(_picks("Only One", 7, 6)),
                   data_dir=str(tmp_path), write=True)
        assert r["refused"] and "richer" in r["refused"]
        after = json.loads((tmp_path / PATTERNS_FILE).read_text(encoding="utf-8"))
        assert len(after) == 599

    def test_force_is_the_deliberate_override(self, tmp_path):
        self._existing(tmp_path, 599)
        r = derive(adapter=FakeOdoo(_picks("Only One", 7, 6)),
                   data_dir=str(tmp_path), write=True, force=True)
        assert not r["refused"]
        after = json.loads((tmp_path / PATTERNS_FILE).read_text(encoding="utf-8"))
        assert len(after) == 1

    def test_the_previous_file_is_backed_up_before_replacement(self, tmp_path):
        self._existing(tmp_path, 2)
        derive(adapter=FakeOdoo(_picks("A", 7, 6) + _picks("B", 3, 6)),
               data_dir=str(tmp_path), write=True)
        assert (tmp_path / (PATTERNS_FILE + ".bak")).exists()

    def test_a_healthy_derivation_writes_all_three_files(self, tmp_path):
        r = derive(adapter=FakeOdoo(_picks("A", 7, 6)), data_dir=str(tmp_path),
                   write=True)
        assert not r["refused"]
        for name in (PATTERNS_FILE, GAPS_FILE, PER_STORE_FILE):
            assert (tmp_path / name).exists(), f"{name} was not written"
        assert not list(tmp_path.glob("*.tmp")), "a temp file was left behind"


class TestTheOutputFeedsTheEngine:

    def test_the_shape_is_what_the_transfer_service_reads(self, tmp_path):
        """Written straight into the file load_supplier_rhythm reads, in the
        shape _relief_days indexes — or the derivation is decorative."""
        from oasis.logic.consolidated_transfer_service import (
            ConsolidatedTransferService as CTS, load_supplier_rhythm)

        derive(adapter=FakeOdoo(_picks("Brookside", 7, 8)),
               data_dir=str(tmp_path), write=True)
        rhythm = load_supplier_rhythm(str(tmp_path))
        assert "brookside" in rhythm

        svc = CTS(org_names={"S": "S"}, stock_data={}, supplier_rhythm=rhythm)
        relief = svc._relief_days("Brookside", 2.0, "DRY GOODS")
        assert relief is not None, "the engine could not use what was derived"
        assert relief > 0

    def test_a_customer_with_no_receipts_is_told_exactly_why(self):
        text = format_report(derive(adapter=FakeOdoo([]), write=False))
        assert "NOTHING DERIVED" in text
        # the report wraps to a console width, so normalise before matching
        flat = " ".join(text.split())
        assert "purchase orders so they carry a supplier" in flat
        text.encode("cp1252")          # a customer's Windows console


class TestTheDerivedPerStoreFileReachesTheEngine:
    """The loader path, end to end: Odoo -> file -> engine horizon.

    The unit tests for the engine inject the per-store dict directly, so they
    would pass even if nothing ever read supplier_patterns_by_store.json off
    disk. This walks the whole path the way a customer install does.
    """

    def test_a_derived_per_store_cadence_changes_that_stores_horizon(self, tmp_path):
        from oasis.logic.consolidated_transfer_service import (
            ConsolidatedTransferService as CTS)

        # one supplier, daily into A1 and fortnightly into B2
        picks = (_picks("Split Co", 1, 14, warehouse_type=1)
                 + _picks("Split Co", 14, 8, warehouse_type=2))
        r = derive(adapter=FakeOdoo(picks), data_dir=str(tmp_path), write=True)
        assert not r["refused"]
        assert (tmp_path / PER_STORE_FILE).exists()

        # constructed exactly as the Odoo scripts do: from data_dir alone
        svc = CTS(org_names={"A1": "Alpha", "B2": "Beta"}, stock_data={},
                  data_dir=str(tmp_path))
        assert svc.supplier_rhythm_by_store, "the per-store file was not loaded"

        a1 = svc._relief_days("Split Co", 0, "DRY GOODS", "A1")
        b2 = svc._relief_days("Split Co", 0, "DRY GOODS", "B2")
        assert a1 is not None and b2 is not None
        assert b2 > a1, (
            f"the fortnightly site got no longer a horizon than the daily one "
            f"({b2} vs {a1}) — per-store cadence is not reaching the engine")

    def test_sigma_differs_by_site_through_the_same_path(self, tmp_path):
        from oasis.logic.consolidated_transfer_service import (
            ConsolidatedTransferService as CTS)

        picks = (_picks("Split Co", 1, 14, warehouse_type=1)
                 + _picks("Split Co", 14, 8, warehouse_type=2))
        derive(adapter=FakeOdoo(picks), data_dir=str(tmp_path), write=True)
        svc = CTS(org_names={"A1": "A", "B2": "B"}, stock_data={},
                  data_dir=str(tmp_path))
        product = {"supplier_name": "Split Co", "estimated_delivery_days": 0,
                   "department": "DRY GOODS"}
        assert svc._safety_days("B2", product) > svc._safety_days("A1", product)
