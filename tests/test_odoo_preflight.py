"""The pre-pilot readiness check — and the guarantee that it writes nothing.

This runs against a CUSTOMER'S production Odoo before anyone has agreed to a
pilot, which makes "read-only" a promise that has to be enforced rather than
documented.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.odoo_preflight import (
    FAIL, PASS, WARN, format_report, run_preflight,
)

#: Everything Odoo's ORM exposes that changes data. If run_preflight reaches
#: for any of these the promise is broken.
_WRITE_METHODS = {
    "create", "write", "unlink", "copy", "action_confirm", "action_cancel",
    "button_immediate_install", "button_immediate_upgrade", "action_apply",
    "set_param", "action_done", "_action_done", "action_assign",
}


class FakeOdoo:
    """Records every call, answers plausibly, and refuses to be written to."""

    def __init__(self, counts=None, warehouses=None, companies=1, rows=10):
        self.url = "http://fake:8069"
        self.db = "fakedb"
        self.calls = []
        self._counts = counts or {}
        self._warehouses = warehouses if warehouses is not None else [
            {"id": 1, "name": "Alpha", "code": "A1", "company_id": [1, "Co"]},
            {"id": 2, "name": "Beta", "code": "B2", "company_id": [1, "Co"]},
        ]
        self._companies = companies
        self._rows = rows
        # the caps the real adapter declares
        from oasis.logic.odoo_adapter import OdooAdapter
        for name in dir(OdooAdapter):
            if name.endswith("_LIMIT"):
                setattr(self, name, getattr(OdooAdapter, name))

    def health_check(self):
        return {"connected": True, "latency_ms": 12.0}

    def fetch_enriched_products(self, org_cd=None):
        self.calls.append(("fetch_enriched_products", org_cd))
        return [{"item_code": f"P{i}"} for i in range(self._rows)]

    def _ex(self, model, method, args, kw=None):
        self.calls.append((model, method))
        if method == "search_count":
            return self._counts.get(model, 0)
        if method == "read":
            return [{"view_location_id": [99, "WH/Stock"]}]
        if model == "res.company":
            return [{"id": i, "name": f"Co{i}"} for i in range(self._companies)]
        if model == "stock.warehouse":
            return self._warehouses
        return []


def _levels(report):
    return {c["label"]: c["level"] for c in report["checks"]}


class TestItWritesNothing:

    def test_no_write_method_is_ever_called(self):
        """The promise, enforced. This runs against production."""
        fake = FakeOdoo()
        run_preflight(adapter=fake)
        used = {method for _model, method in fake.calls if _model != "fetch_enriched_products"}
        offenders = used & _WRITE_METHODS
        assert not offenders, f"preflight called write methods: {offenders}"

    def test_it_only_reads(self):
        fake = FakeOdoo()
        run_preflight(adapter=fake)
        allowed = {"search_count", "search_read", "read", "fields_get"}
        used = {m for model, m in fake.calls if model != "fetch_enriched_products"}
        assert used <= allowed, f"unexpected ORM methods: {used - allowed}"


class TestFindings:

    def test_a_healthy_single_company_instance_passes(self):
        report = run_preflight(adapter=FakeOdoo())
        assert report["overall"] in (PASS, WARN)
        assert _levels(report)["connection"] == PASS

    def test_a_dead_connection_fails_immediately(self):
        fake = FakeOdoo()
        fake.health_check = lambda: {"connected": False, "error": "refused"}
        report = run_preflight(adapter=fake)
        assert report["overall"] == FAIL
        assert len(report["checks"]) == 1, "it kept probing a dead connection"

    def test_multi_company_is_flagged(self):
        report = run_preflight(adapter=FakeOdoo(companies=3))
        assert _levels(report)["companies"] == WARN

    def test_a_single_warehouse_cannot_transfer(self):
        one = [{"id": 1, "name": "Only", "code": "X", "company_id": [1, "Co"]}]
        report = run_preflight(adapter=FakeOdoo(warehouses=one))
        assert _levels(report)["warehouses"] == FAIL

    def test_duplicate_warehouse_codes_fail(self):
        dupe = [
            {"id": 1, "name": "A", "code": "SAME", "company_id": [1, "Co"]},
            {"id": 2, "name": "B", "code": "SAME", "company_id": [1, "Co"]},
        ]
        report = run_preflight(adapter=FakeOdoo(warehouses=dupe))
        assert _levels(report)["warehouse codes"] == FAIL

    def test_a_missing_code_is_flagged_not_fatal(self):
        nocode = [
            {"id": 1, "name": "A", "code": "A1", "company_id": [1, "Co"]},
            {"id": 2, "name": "Nameless", "code": "", "company_id": [1, "Co"]},
        ]
        report = run_preflight(adapter=FakeOdoo(warehouses=nocode))
        assert _levels(report)["warehouse codes"] == WARN

    def test_a_read_over_its_cap_fails(self):
        """The finding that matters: over the cap the numbers go quietly wrong."""
        from oasis.logic.odoo_adapter import OdooAdapter
        fake = FakeOdoo(counts={"product.product": OdooAdapter.PRODUCT_READ_LIMIT + 1})
        report = run_preflight(adapter=fake)
        breached = [c for c in report["checks"] if "EXCEEDS" in c["label"]]
        assert breached, "a read past its cap was not reported"
        assert report["overall"] == FAIL

    def test_a_read_near_its_cap_warns_before_it_bites(self):
        from oasis.logic.odoo_adapter import OdooAdapter
        near = int(OdooAdapter.PRODUCT_READ_LIMIT * 0.85)
        report = run_preflight(adapter=FakeOdoo(counts={"product.product": near}))
        assert any("near its cap" in c["label"] for c in report["checks"])

    def test_site_scoped_reads_are_judged_per_site_not_company_wide(self):
        """Counting a whole chain against a per-site cap fails healthy instances.

        Measured on the depot: 28,125 receipts company-wide against a 20,000
        cap reads as a breach, while the busiest single site holds 2,971.
        """
        fake = FakeOdoo(counts={"stock.move": 500})
        report = run_preflight(adapter=fake)
        # the per-site labels carry the site they were worst at
        assert any("busiest site" in c["label"] for c in report["checks"])
        assert not any("EXCEEDS" in c["label"] for c in report["checks"])


class TestReport:

    def test_the_report_is_console_safe(self):
        """It prints to a customer's Windows console, whose cp1252 codec
        cannot encode an arrow — the trap this codebase already documents."""
        text = format_report(run_preflight(adapter=FakeOdoo()))
        text.encode("cp1252")          # raises if a stray glyph creeps back

    def test_the_verdict_states_what_to_do(self):
        text = format_report(run_preflight(adapter=FakeOdoo()))
        assert "READ ONLY" in text
        assert any(v in text for v in ("READY", "NOT READY"))
