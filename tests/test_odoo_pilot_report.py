"""The pilot scorecard — the thing that makes a trial falsifiable.

A pilot that cannot be measured is a demo, and a scorecard that reports the
wrong thing is worse than none: the donor-short line in particular is the one
number that would make a chain stop trusting the queue.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.odoo_pilot_report import (
    HEALTHY_ACCEPTANCE, collect, format_report,
)


def _row(state, kind="pull", fresh=False, value=100.0, qty=5.0,
         to=(1, "Alpha (A1)"), frm=(2, "Beta (B2)"), categ=(3, "Drinks"),
         created="2026-08-01 08:00:00", written="2026-08-01 12:00:00"):
    return {"state": state, "kind": kind, "is_fresh": fresh,
            "value_kes": value, "quantity": qty, "from_warehouse_id": frm,
            "to_warehouse_id": to, "categ_id": categ,
            "create_date": created, "write_date": written,
            "computed_on": created, "product_id": (9, "[SKU1] Widget")}


class FakeOdoo:
    def __init__(self, rows, warehouses=None, products=None):
        self.url, self.db = "http://fake", "fake"
        self._rows = rows
        self._warehouses = warehouses or [{"id": 2, "code": "B2", "name": "Beta"}]
        self._products = products or []
        self.sites_read = []

    def _ex(self, model, method, args, kw=None):
        if model == "oasis.transfer.suggestion":
            return self._rows
        if model == "stock.warehouse":
            return self._warehouses
        return []

    def fetch_enriched_products(self, org_cd=None):
        self.sites_read.append(org_cd)
        return self._products


class TestAcceptance:

    def test_acceptance_counts_only_lines_somebody_decided(self):
        """A queue nobody has read is not a 0% acceptance rate."""
        r = collect(adapter=FakeOdoo([_row("new"), _row("new"), _row("approved")]))
        assert r["decided"] == 1
        assert r["acceptance"] == 1.0
        assert r["awaiting"] == 2

    def test_completed_counts_as_accepted(self):
        r = collect(adapter=FakeOdoo([_row("done"), _row("rejected")]))
        assert r["acceptance"] == 0.5

    def test_no_decisions_reads_as_unknown_not_zero(self):
        r = collect(adapter=FakeOdoo([_row("new")]))
        assert r["acceptance"] is None
        text = format_report(r)
        assert "nobody has worked the queue" in text

    def test_a_low_rate_is_reported_as_the_review_working(self):
        rows = [_row("rejected") for _ in range(8)] + [_row("approved")]
        text = format_report(collect(adapter=FakeOdoo(rows)))
        assert "review is doing real work" in text

    def test_a_high_rate_reads_as_agreement(self):
        rows = [_row("approved") for _ in range(9)] + [_row("rejected")]
        r = collect(adapter=FakeOdoo(rows))
        assert r["acceptance"] >= HEALTHY_ACCEPTANCE
        assert "agrees with the people" in format_report(r)


class TestWhereItIsOverruled:

    def test_rejection_is_broken_down_by_store(self):
        """A rate concentrated in one store is a data problem in that store;
        a uniform rate is a threshold problem. Different fixes."""
        bad = [(1, "Problem Store (P1)")]
        rows = [_row("rejected", to=bad[0]) for _ in range(4)]
        rows += [_row("approved", to=(2, "Fine Store (F1)")) for _ in range(4)]
        r = collect(adapter=FakeOdoo(rows))
        worst = r["by_store"][0]
        assert "Problem Store" in worst[0]
        assert worst[3] == 1.0

    def test_groups_with_too_little_evidence_are_not_reported(self):
        """Two rejections out of two is not a 100% rejection rate worth acting
        on — it is two lines."""
        rows = [_row("rejected", to=(5, "Tiny (T1)")),
                _row("rejected", to=(5, "Tiny (T1)"))]
        rows += [_row("approved") for _ in range(4)]
        r = collect(adapter=FakeOdoo(rows))
        assert not any("Tiny" in str(k) for k, *_ in r["by_store"])

    def test_perishables_are_split_out(self):
        rows = [_row("rejected", fresh=True) for _ in range(3)]
        rows += [_row("approved", fresh=False) for _ in range(3)]
        r = collect(adapter=FakeOdoo(rows))
        labels = {k for k, *_ in r["by_fresh"]}
        assert labels == {"perishable", "dry"}


class TestTheDonorAlarmIsTrustworthy:
    """The one number that would make a chain stop trusting the queue.

    The first version parsed the warehouse code out of a DISPLAY LABEL that had
    already been truncated to 28 characters, so "Chandarana Diamond Plaza
    (CFP-009)" became "CF", the site lookup missed, the adapter fell back to
    company-wide stock, and the scorecard raised a donor alarm that was not
    real.
    """

    def test_the_site_is_looked_up_by_code_not_parsed_from_a_label(self):
        long_name = "Chandarana Diamond Plaza Extremely Long Site Name"
        rows = [_row("done", frm=(2, f"{long_name} (CFP-009)"))]
        fake = FakeOdoo(rows, warehouses=[{"id": 2, "code": "CFP-009",
                                           "name": long_name}])
        collect(adapter=fake)
        assert fake.sites_read == ["CFP-009"], (
            f"read stock for {fake.sites_read} — a truncated label, not the code")

    def test_a_donor_still_above_its_floor_raises_nothing(self):
        rows = [_row("done", frm=(2, "Beta (B2)"))]
        fake = FakeOdoo(rows, products=[
            {"item_code": "SKU1", "avg_daily_sales": 2.0,
             "current_stocks": 500.0, "supplier_name": "x", "department": "d"}])
        r = collect(adapter=fake)
        assert r["donors_short"] == []
        assert "none left below" in format_report(r)

    def test_a_donor_genuinely_short_is_reported(self):
        rows = [_row("done", frm=(2, "Beta (B2)"))]
        fake = FakeOdoo(rows, products=[
            {"item_code": "SKU1", "avg_daily_sales": 10.0,
             "current_stocks": 3.0, "supplier_name": "x", "department": "d"}])
        r = collect(adapter=fake)
        assert len(r["donors_short"]) == 1
        assert "BELOW THEIR OWN SAFETY FLOOR" in format_report(r)

    def test_a_warehouse_with_no_code_is_skipped_not_guessed(self):
        rows = [_row("done", frm=(2, "Nameless"))]
        fake = FakeOdoo(rows, warehouses=[{"id": 2, "code": "", "name": "Nameless"}])
        collect(adapter=fake)
        assert fake.sites_read == [], "guessed a site code from a blank"


class TestReport:

    def test_it_is_console_safe(self):
        rows = [_row("done"), _row("rejected"), _row("new")]
        format_report(collect(adapter=FakeOdoo(rows))).encode("cp1252")

    def test_it_states_what_moved_and_what_is_waiting(self):
        rows = [_row("done", value=500.0, qty=7.0), _row("new", value=250.0)]
        text = format_report(collect(adapter=FakeOdoo(rows)))
        assert "MOVED" in text and "PENDING" in text
