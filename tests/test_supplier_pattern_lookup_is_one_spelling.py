"""A supplier is spelled one way everywhere in the ordering engine.

The GRN export that builds supplier_patterns_2025.json writes
'DPL FESTIVE  LIMITED' with two spaces; the product's vendor field carries one.
The enrichment looked the pattern up by exact key and missed, so 42 vendors
covering 2,555 SKUs -- 8% of the shelf -- fell through to the no-pattern
defaults: a 7-day lead where LATA had measured 1 from 5,634 receipts, and no
daily rhythm, which then gave the line a 365-day shelf life and no cover
ceiling. order_up_to resolved the same supplier correctly for sigma_L, so one
order line paired a measured spread with a fabricated mean.
"""
import json
import os

import pytest

from oasis.logic.intelligence_mixin import IntelligenceMixin
from oasis.logic.order_up_to import supplier_key

FEST = "DPL FESTIVE  LIMITED"          # as the export writes it
FEST_1 = "DPL FESTIVE LIMITED"         # as the product carries it


def _shipped():
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "oasis", "data", "supplier_patterns_2025.json")


PATTERN = {"order_frequency": "daily", "median_gap_days": 1,
           "estimated_delivery_days": 1, "lata_variance_multiplier": 2.462,
           "reliability_score": 21}


class Engine(IntelligenceMixin):
    def __init__(self, patterns):
        self.databases = {"supplier_patterns": dict(patterns)}

    def normalize_product_name(self, s):        # the engine's own helper
        return " ".join(str(s or "").upper().split())


@pytest.fixture
def eng():
    return Engine({FEST: PATTERN, "KENAFRIC BAKERY LTD": {"estimated_delivery_days": 2}})


class TestTheLookupReaches:
    def test_a_double_spaced_key_is_found_by_the_single_spaced_name(self, eng):
        assert eng.supplier_pattern_for(FEST_1) == PATTERN

    def test_the_exact_key_still_works(self, eng):
        assert eng.supplier_pattern_for(FEST) == PATTERN

    def test_case_and_padding_do_not_matter(self, eng):
        assert eng.supplier_pattern_for("  dpl festive limited ") == PATTERN

    def test_a_vendor_code_prefix_is_stripped(self, eng):
        assert eng.supplier_pattern_for("SD0029 - DPL FESTIVE LIMITED") == PATTERN

    def test_an_unknown_supplier_returns_empty_not_a_wrong_match(self, eng):
        assert eng.supplier_pattern_for("BROADWAYS BAKERY LTD") == {}
        assert eng.supplier_pattern_for(None) == {}

    def test_it_is_the_same_spelling_order_up_to_uses(self, eng):
        assert supplier_key(FEST) == supplier_key(FEST_1)

    def test_a_canonical_key_wins_a_tie(self):
        e = Engine({FEST: PATTERN, FEST_1: {"estimated_delivery_days": 9}})
        assert e.supplier_pattern_for("dpl  festive  limited")["estimated_delivery_days"] == 9

    def test_non_dict_entries_are_ignored(self):
        e = Engine({FEST: "not a record"})
        assert e.supplier_pattern_for(FEST_1) == {}

    def test_the_index_follows_a_rebuilt_database(self, eng):
        assert eng.supplier_pattern_for(FEST_1) == PATTERN
        eng.databases["supplier_patterns"] = {"MINI BAKERIES NBI  LTD": {"estimated_delivery_days": 1}}
        assert eng.supplier_pattern_for(FEST_1) == {}
        assert eng.supplier_pattern_for("MINI BAKERIES NBI LTD")["estimated_delivery_days"] == 1


class TestTheShippedFileIsReachable:
    """Every vendor the shipped pattern file describes can be found."""

    def test_every_key_resolves_to_its_own_record(self):
        src = json.load(open(_shipped(), encoding="utf-8"))
        e = Engine(src)
        for k, v in src.items():
            if isinstance(v, dict):
                assert e.supplier_pattern_for(" ".join(k.split())) is v, k

    def test_the_bakery_that_delivers_daily_is_not_read_as_weekly(self):
        e = Engine(json.load(open(_shipped(), encoding="utf-8")))
        pat = e.supplier_pattern_for(FEST_1)
        assert pat.get("order_frequency") == "daily"
        assert float(pat.get("estimated_delivery_days")) <= 1.0
