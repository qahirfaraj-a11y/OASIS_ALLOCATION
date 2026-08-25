"""The rhythm file must be readable by the engine that consumes it.

THE BUG THIS GUARDS. odoo_supplier_rhythm wrote supplier names in lower case.
Every consumer looks them up in UPPER case -- enrichment uses
`supplier_name.upper().strip()`, and its fallback, normalize_product_name,
also uppercases. Of 486 derived keys, ZERO matched the 599 in the file they
would have replaced.

Nothing errored. The file existed, parsed, and looked correct. At the first
customer where the derivation had enough history to write, every supplier
lookup would have missed and the engine would have run on
estimated_delivery_days = 7.0 for all of them -- the flat constant the whole
derivation exists to remove -- while a freshly written file sat on disk
proving it had worked.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.odoo_supplier_rhythm import _summarise, _supplier_key


class TestTheKeyIsWhatTheEngineLooksUp:

    def test_the_key_is_upper_case(self):
        assert _supplier_key("Brookside Dairy Limited") == "BROOKSIDE DAIRY LIMITED"

    def test_it_matches_what_enrichment_does_to_a_supplier_name(self):
        """Enrichment: str(p.get('supplier_name')).upper().strip()."""
        raw = "  Kamili Packers Sugar Only  "
        engine_side = str(raw).upper().strip()
        assert _supplier_key(raw) == engine_side

    def test_it_matches_the_normalize_fallback_too(self):
        """The second lookup path, normalize_product_name, also uppercases and
        collapses doubled spaces."""
        raw = "West  Kenya  Sugar"
        normalised = raw.upper().strip().replace('  ', ' ')
        assert _supplier_key(raw) == normalised

    def test_inner_whitespace_is_collapsed_not_just_trimmed(self):
        assert _supplier_key("Mini  Bakeries   Nbi") == "MINI BAKERIES NBI"

    def test_it_is_idempotent(self):
        once = _supplier_key("Unga Ltd")
        assert _supplier_key(once) == once

    def test_empty_and_none_do_not_raise(self):
        assert _supplier_key("") == ""
        assert _supplier_key(None) == ""


class TestASummaryIsShapedForItsConsumers:

    def _dates(self, n, step=7):
        from datetime import datetime, timedelta
        base = datetime(2025, 1, 6)
        return [base + timedelta(days=i * step) for i in range(n)]

    def test_a_summary_carries_the_fields_enrichment_reads(self):
        """enrich_product_data reads these off the pattern by name; a rename
        breaks the engine silently, exactly the way the casing did."""
        rec = _summarise(self._dates(10), [3.0] * 9, None)
        for field in ("estimated_delivery_days", "median_gap_days"):
            assert field in rec, "enrichment reads %s and it is missing" % field

    def test_evidence_is_recorded_so_a_consumer_can_judge_it(self):
        rec = _summarise(self._dates(12), [2.0] * 11, None)
        assert rec["receipt_count"] == 12
        assert rec["confidence"] == "HIGH"

    def test_thin_history_is_marked_low_not_hidden(self):
        rec = _summarise(self._dates(5), [2.0] * 4, None)
        assert rec["confidence"] == "LOW"
        assert rec["receipt_count"] == 5

    def test_provenance_is_stamped(self):
        """How we knew the file in oasis/data had NOT come from Odoo."""
        rec = _summarise(self._dates(10), [1.0] * 9, None)
        assert rec["derived_from"] == "odoo_goods_receipts"

    def test_a_measured_lead_beats_the_suppliers_own_claim(self):
        rec = _summarise(self._dates(10), [4.0] * 9, stated_lead=30.0)
        assert rec["estimated_delivery_days"] == 4.0
        assert rec["lead_time_source"] == "measured"

    def test_the_stated_lead_is_used_only_when_nothing_was_measured(self):
        rec = _summarise(self._dates(10), [], stated_lead=30.0)
        assert rec["estimated_delivery_days"] == 30.0
        assert rec["lead_time_source"] == "stated"
