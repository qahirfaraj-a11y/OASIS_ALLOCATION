"""
OdooAdapter: every org_cd on the contract must actually SCOPE something.

`fetch_enriched_products` once accepted org_cd and ignored it, so every store
in a chain read the whole company's stock and ordered as though it held it —
systematic under-ordering, no error. Four more methods had the same shape.
These tests assert on the DOMAIN the adapter sends to Odoo, because that is
where the bug lived: the call succeeded and returned rows, they were just the
wrong site's rows.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.odoo_adapter import OdooAdapter

#: two sites, so "scoped" is distinguishable from "happened to match"
WAREHOUSES = {
    "WH": {"id": 1, "name": "Main", "code": "WH",
           "view_location_id": [10, "WH"], "in_type_id": [100, "WH: Receipts"]},
    "CHIC1": {"id": 2, "name": "Chicago", "code": "CHIC1",
              "view_location_id": [20, "CHIC1"], "in_type_id": [200, "CHIC1: Receipts"]},
}


class _Recorder:
    """Stands in for Odoo, remembering every call so domains can be asserted."""

    def __init__(self):
        self.calls = []

    def __call__(self, model, method, args, kw=None):
        self.calls.append({"model": model, "method": method,
                           "args": args, "kw": kw or {}})
        if model == "stock.warehouse" and method == "search_read":
            dom = args[0]
            wanted = {c[2] for c in dom if isinstance(c, list) and len(c) == 3}
            return [w for w in WAREHOUSES.values()
                    if w["code"] in wanted or w["name"] in wanted][:1]
        if method == "create":
            return 999
        if model == "res.partner" and method == "search":
            return [77]
        if model == "product.product" and method == "search_read":
            return [{"id": 5, "default_code": "COKE_500"}]
        return []

    def domain_for(self, model, method="search_read"):
        for c in self.calls:
            if c["model"] == model and c["method"] == method:
                return c["args"][0]
        raise AssertionError(f"{model}.{method} was never called: {self.calls}")

    def vals_for(self, model):
        for c in self.calls:
            if c["model"] == model and c["method"] == "create":
                return c["args"][0]
        raise AssertionError(f"{model}.create was never called")


@pytest.fixture
def adapter(monkeypatch):
    a = OdooAdapter(url="http://stub", db="stub", user="u", password="p")
    rec = _Recorder()
    monkeypatch.setattr(a, "_ex", rec)
    a.recorder = rec
    return a


def _flat(domain):
    """Every leaf in a domain as (field, op, value) — operators dropped."""
    return [tuple(c) for c in domain if isinstance(c, list) and len(c) == 3]


# ── the write path ───────────────────────────────────────────────────────
def test_purchase_order_is_aimed_at_the_ordering_site():
    """A PO computed from CHIC1's stock must be RECEIVED at CHIC1.

    Odoo silently applies the default warehouse's receipt type when none is
    given, so without this the goods physically arrive at the wrong store.
    """
    for org, expected_type in (("WH", 100), ("CHIC1", 200)):
        a = OdooAdapter(url="http://stub", db="stub", user="u", password="p")
        rec = _Recorder()
        a._ex = rec
        a.push_purchase_order(org, [{"item_code": "COKE_500", "supplier_cd": "77",
                                     "recommended_quantity": 10, "cost_price": 5.0,
                                     "product_name": "Coke"}])
        assert rec.vals_for("purchase.order")["picking_type_id"] == expected_type


def test_single_site_install_sets_no_picking_type(adapter):
    """org_cd=None must stay company-wide, not invent a destination."""
    adapter.push_purchase_order(None, [{"item_code": "COKE_500", "supplier_cd": "77",
                                        "recommended_quantity": 10, "cost_price": 5.0,
                                        "product_name": "Coke"}])
    assert "picking_type_id" not in adapter.recorder.vals_for("purchase.order")


# ── the read paths that silently dropped org_cd ──────────────────────────
def test_sales_history_is_scoped_to_the_site(adapter):
    """fetch_sales_history passed `days` through but dropped org_cd entirely."""
    adapter.fetch_sales_history("CHIC1", days=30)
    dom = _flat(adapter.recorder.domain_for("stock.move"))
    assert ("location_id", "child_of", 20) in dom, (
        "demand read company-wide: every site would report the same ADS")


def test_pending_pos_are_scoped_to_the_site(adapter):
    adapter.fetch_pending_pos("CHIC1")
    dom = _flat(adapter.recorder.domain_for("purchase.order.line"))
    assert ("order_id.picking_type_id.warehouse_id", "=", 2) in dom


def test_on_order_qty_is_scoped_to_the_site(adapter):
    """The costly one: unscoped, stock inbound to ONE store suppresses
    ordering at EVERY store — silent chain-wide under-ordering."""
    adapter.fetch_pending_po_by_sku("CHIC1")
    dom = _flat(adapter.recorder.domain_for("purchase.order.line"))
    assert ("order_id.picking_type_id.warehouse_id", "=", 2) in dom


def test_stock_and_receipts_stay_scoped(adapter):
    """Regression cover for the fix that started this."""
    adapter._on_hand("CHIC1")
    assert ("location_id", "child_of", 20) in _flat(
        adapter.recorder.domain_for("stock.quant", "read_group"))

    adapter.recorder.calls.clear()
    adapter._last_receipt("CHIC1")
    assert ("location_dest_id", "child_of", 20) in _flat(
        adapter.recorder.domain_for("stock.move"))


# ── behaviour when the site is not resolvable ────────────────────────────
def test_unknown_warehouse_falls_back_company_wide_and_warns(adapter, caplog):
    """Better a whole-company read than an empty store, but say so."""
    with caplog.at_level("WARNING"):
        assert adapter._warehouse_scope("NOPE") is None
    assert "NOPE" in caplog.text


def test_a_broken_warehouse_lookup_does_not_blank_the_catalogue(adapter, caplog):
    """If this Odoo lacks a field we ask for, say so — do not return an empty
    store. Callers sit inside a try/except that would report a schema error as
    'no products', which is the silent-zero failure this codebase keeps hitting.
    """
    def boom(model, method, args, kw=None):
        if model == "stock.warehouse":
            raise ValueError("Invalid field 'in_type_id' on model 'stock.warehouse'")
        return []
    adapter._ex = boom
    with caplog.at_level("ERROR"):
        assert adapter._warehouse_scope("WH") is None
    assert "site scoping is NOT in effect" in caplog.text


def test_the_warehouse_is_resolved_once_per_site(adapter):
    """One fetch resolves the site three times over; it cannot change mid-call."""
    for _ in range(3):
        adapter._warehouse_scope("WH")
    lookups = [c for c in adapter.recorder.calls if c["model"] == "stock.warehouse"]
    assert len(lookups) == 1
