"""Tests for real-demand derivation from the monthly cash files."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.real_demand import derive_ads, normalise_name


class TestNormaliseName:
    def test_strips_leading_dept_code_and_punctuation(self):
        assert normalise_name("151  AIRWICK 375ML CITRUS AIR FRESHNER") == \
            "AIRWICK 375ML CITRUS AIR FRESHNER"
        assert normalise_name("BROOKSIDE 500ML DAIRY BEST (POUCH)") == \
            "BROOKSIDE 500ML DAIRY BEST POUCH"

    def test_strips_embedded_short_codes(self):
        # trailing / leading / hashed product codes are dropped so names align
        assert normalise_name("AIRWICK 375ML FRESH WATER AIR FRESHNER AIR223") == \
            "AIRWICK 375ML FRESH WATER AIR FRESHNER"
        assert normalise_name("ELY1006 ELYSIUM 1KG SPA EUCALYPTUS EPSOM SALTS") == \
            "ELYSIUM 1KG SPA EUCALYPTUS EPSOM SALTS"
        assert normalise_name("ELY SPA EPSOM 450G SALT #ELY1135") == \
            "ELY SPA EPSOM 450G SALT"

    def test_case_and_whitespace_canonical(self):
        assert normalise_name("Brookside 1Lt Uht  Whole   Milk") == \
            normalise_name("BROOKSIDE 1LT UHT WHOLE MILK")


class TestDeriveAds:
    def test_total_units_over_observed_days(self):
        # 304 units over 10 months (~304 days) → ~1/day
        ads = derive_ads({"X": 304.0}, months=10, days_per_month=30.4)
        assert abs(ads["X"] - 1.0) < 1e-6

    def test_zero_qty_dropped(self):
        assert "Y" not in derive_ads({"Y": 0.0}, months=10)
