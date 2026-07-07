"""Tests for the multi-store demo layer (profiles + stream invariants)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.multi_store_pos import _store_tag
from oasis.logic.multi_store_profiles import STORE_PROFILES


class TestStoreProfiles:
    def test_five_distinct_stores(self):
        assert len(STORE_PROFILES) == 5
        assert len({p.org_cd for p in STORE_PROFILES}) == 5
        assert len({p.short_name for p in STORE_PROFILES}) == 5

    def test_traffic_params_sane(self):
        for p in STORE_PROFILES:
            assert p.history_bills_per_day > 0, p.org_cd
            assert 0 < p.interval_seconds() < 120, p.org_cd
            assert p.max_attach >= 1 and p.max_qty >= 1, p.org_cd
            assert p.pop_exponent >= 1.0, p.org_cd

    def test_profiles_are_differentiated(self):
        # the demo's point: stores must NOT be clones of each other
        cadences = {p.history_bills_per_day for p in STORE_PROFILES}
        assert len(cadences) >= 4, "store traffic should differ across the network"

    def test_store_tag_format(self):
        tags = [_store_tag(p) for p in STORE_PROFILES]
        assert all("/" in t for t in tags)
        assert len(set(tags)) == 5
