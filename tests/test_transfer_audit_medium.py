"""The MEDIUM findings from OASIS_Transfer_Pipeline_Audit_2026-08.md.

M1 donor release cap breached by sub-unit rounding
M2 capped reads truncating in silence
M4 per-store safety_days read zero times

M3 (the scan is not one snapshot) and M5 (a stale approval failing
confusingly) are exercised against live Odoo, not here: M3's fix is which
instant the queue is stamped with, and M5's lives on an Odoo model that needs
a server to instantiate.
"""

import logging
import os
import sys
from contextlib import contextmanager

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
from oasis.logic.fulfillment_decider import (
    _releasable_transfer_qty, _round_transfer_qty,
)


class TestM1ReleaseCapIsBinding:
    """A donor may never ship more than its pool, whatever the rounding."""

    def test_sub_unit_pool_ships_nothing(self):
        # the case measured in the audit: 0.44 releasable became a shipped 1.0
        assert _round_transfer_qty(0.44) == 1.0          # the old answer
        assert _releasable_transfer_qty(0.44) == 0.0     # the correct one

    def test_never_rounds_past_the_pool(self):
        for pool in (0.01, 0.44, 0.99, 1.0, 1.4, 1.99, 2.0, 17.6):
            assert _releasable_transfer_qty(pool) <= pool

    def test_whole_units_are_untouched(self):
        assert _releasable_transfer_qty(3.0) == 3.0
        assert _releasable_transfer_qty(17.6) == 17.0

    def test_kg_items_keep_one_decimal_and_still_do_not_exceed(self):
        assert _releasable_transfer_qty(0.44, "BUTCHERY") == 0.4
        assert _releasable_transfer_qty(2.37, "MEAT") == 2.3
        assert _releasable_transfer_qty(2.37, "MEAT") <= 2.37

    def test_zero_and_negative_pools_are_empty(self):
        assert _releasable_transfer_qty(0.0) == 0.0
        assert _releasable_transfer_qty(-5.0) == 0.0

    def test_ceiling_is_still_right_for_a_need(self):
        # _round_transfer_qty is not being changed -- covering a shortfall of
        # 3.2 with 4 units is correct; only the pool-capped use was wrong.
        assert _round_transfer_qty(3.2) == 4.0


class TestM4SafetyDaysIsLive:
    """The per-store floor is read, and absence still means 14."""

    def _svc(self, **kw):
        return ConsolidatedTransferService(org_names={"C001": "One"},
                                           stock_data={}, **kw)

    def test_default_is_unchanged_when_nothing_is_passed(self):
        assert self._svc()._safety_days("C001") == 14.0

    def test_per_store_value_is_used(self):
        svc = self._svc(safety_days_by_org={"C001": 21.0, "C002": 5.0})
        assert svc._safety_days("C001") == 21.0
        assert svc._safety_days("C002") == 5.0

    def test_unknown_store_falls_back(self):
        svc = self._svc(safety_days_by_org={"C001": 21.0})
        assert svc._safety_days("C999") == 14.0

    def test_zero_or_junk_never_makes_a_store_fully_drainable(self):
        # a store whose field is 0/None/"" must keep a floor, not lose it
        svc = self._svc(safety_days_by_org={"A": 0, "B": None, "C": ""})
        for org in ("A", "B", "C"):
            assert svc._safety_days(org) == 14.0

    def test_a_lower_floor_frees_more_excess(self):
        # 100 on hand, ADS 2 -> 50 days cover, past the 30d dry gate
        at14 = ConsolidatedTransferService._excess_units(2.0, 100.0, False, 14.0)
        at10 = ConsolidatedTransferService._excess_units(2.0, 100.0, False, 10.0)
        assert at14 == 100.0 - 2.0 * 14.0
        assert at10 == 100.0 - 2.0 * 10.0
        assert at10 > at14

    def test_excess_default_matches_the_old_literal(self):
        assert (ConsolidatedTransferService._excess_units(2.0, 100.0, False)
                == ConsolidatedTransferService._excess_units(2.0, 100.0, False, 14.0))


@contextmanager
def _captured(name="OdooAdapter"):
    """Records from ONE logger, whatever the global logging config happens to be.

    Not caplog: caplog attaches to the ROOT logger, so a sibling test that has
    called basicConfig, set propagate=False, or called logging.disable() can
    silence it. This test passed alone and failed in the full suite for exactly
    that reason — an order-dependence in the test, not in the code.
    """
    lg = logging.getLogger(name)
    records = []

    class _Sink(logging.Handler):
        def emit(self, record):
            records.append(record)

    sink = _Sink()
    was = (lg.level, lg.propagate, lg.disabled, logging.root.manager.disable)
    lg.addHandler(sink)
    lg.setLevel(logging.WARNING)
    lg.disabled = False
    logging.disable(logging.NOTSET)
    try:
        yield records
    finally:
        lg.removeHandler(sink)
        lg.level, lg.propagate, lg.disabled = was[0], was[1], was[2]
        logging.disable(was[3])


class TestM2TruncationIsAnnounced:
    """A capped read that comes back full must say so."""

    def _adapter(self):
        from oasis.logic.odoo_adapter import OdooAdapter
        return OdooAdapter.__new__(OdooAdapter)   # no connection needed

    def test_a_full_read_warns(self):
        a = self._adapter()
        with _captured() as recs:
            hit = a._warn_if_truncated([0] * 500, 500, "product catalogue",
                                       "C003", "consequence text")
        assert hit is True
        text = "\n".join(r.getMessage() for r in recs)
        assert "TRUNCATED" in text
        assert "product catalogue" in text
        assert "C003" in text
        assert "consequence text" in text

    def test_a_short_read_is_silent(self):
        a = self._adapter()
        with _captured() as recs:
            hit = a._warn_if_truncated([0] * 499, 500, "product catalogue")
        assert hit is False
        assert recs == []

    def test_every_capped_read_has_a_named_limit(self):
        from oasis.logic.odoo_adapter import OdooAdapter
        for name in ("RECEIPT_READ_LIMIT", "POS_LINE_READ_LIMIT",
                     "SALES_MOVE_READ_LIMIT", "PRODUCT_READ_LIMIT",
                     "SUPPLIERINFO_READ_LIMIT"):
            assert isinstance(getattr(OdooAdapter, name), int)
