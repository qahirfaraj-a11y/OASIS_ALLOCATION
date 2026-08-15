"""
OdooAdapter contract tests: site scoping, and the approval write path.

`fetch_enriched_products` once accepted org_cd and ignored it, so every store
in a chain read the whole company's stock and ordered as though it held it —
systematic under-ordering, no error. Four more methods had the same shape.
These tests assert on the DOMAIN the adapter sends to Odoo, because that is
where the bug lived: the call succeeded and returned rows, they were just the
wrong site's rows.

The `update_po_status` half pins the decisions that would be easy to regress
into something dangerous — above all that approving in OASIS does not confirm
a purchase order in Odoo. Both halves drive a stub; the live proof against
Odoo 16 is recorded in the vault.
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


# ── update_po_status ─────────────────────────────────────────────────────
# po_id here is a purchase.order.LINE id — what fetch_pending_pos returns and
# what the console hands back. Live behaviour is proven in the vault; these
# pin the decisions that are easy to regress.
class _POStub(_Recorder):
    """An Odoo with one draft order (id 50) holding two lines (60, 61)."""

    def __init__(self, state="draft", lines=(60, 61)):
        super().__init__()
        self.state = state
        self.lines = list(lines)

    def __call__(self, model, method, args, kw=None):
        self.calls.append({"model": model, "method": method,
                           "args": args, "kw": kw or {}})
        if model == "purchase.order.line" and method == "read":
            wanted = args[0][0]
            if wanted not in self.lines:
                return []
            return [{"id": wanted, "product_qty": 10.0, "state": self.state,
                     "order_id": [50, "P00099"], "product_id": [5, "Coke"]}]
        if model == "purchase.order.line" and method == "unlink":
            for i in args[0]:
                if i in self.lines:
                    self.lines.remove(i)
            return True
        if model == "purchase.order.line" and method == "search_count":
            return len(self.lines)
        if model == "purchase.order" and method == "read":
            return [{"id": 50, "state": self.state}]
        if model == "purchase.order" and method == "button_cancel":
            self.state = "cancel"
            return None
        return True

    def did(self, model, method):
        return any(c["model"] == model and c["method"] == method for c in self.calls)


def _adapter_with(stub):
    a = OdooAdapter(url="http://stub", db="stub", user="u", password="p")
    a._ex = stub
    return a


def test_approval_never_confirms_the_order():
    """The invariant: OASIS proposes, a human confirms in Odoo. Approving from
    the console must not commit a client's money."""
    stub = _POStub()
    assert _adapter_with(stub).update_po_status(60, "APPROVED", "qahir") is True
    assert not stub.did("purchase.order", "button_confirm")
    assert stub.state == "draft"


def test_approval_applies_a_quantity_override(adapter=None):
    stub = _POStub()
    _adapter_with(stub).update_po_status(60, "APPROVED", "qahir", new_quantity=42)
    write = [c for c in stub.calls if c["method"] == "write"][0]
    assert write["args"][1] == {"product_qty": 42.0}


def test_rejection_removes_the_line_but_keeps_a_non_empty_order():
    stub = _POStub(lines=(60, 61))
    assert _adapter_with(stub).update_po_status(60, "REJECTED", "qahir") is True
    assert stub.lines == [61]
    assert not stub.did("purchase.order", "button_cancel")


def test_rejecting_the_last_line_cancels_the_empty_order():
    """An empty draft PO is litter that still reads as a real order."""
    stub = _POStub(lines=(60,))
    _adapter_with(stub).update_po_status(60, "REJECTED", "qahir")
    assert stub.lines == []
    assert stub.state == "cancel"


def test_a_committed_order_is_refused():
    """Confirmed spend is somebody's money — refuse, do not silently edit."""
    stub = _POStub(state="purchase")
    assert _adapter_with(stub).update_po_status(60, "REJECTED", "qahir") is False
    assert stub.lines == [60, 61], "a committed order was modified"


def test_unknown_status_is_refused_not_guessed():
    stub = _POStub()
    assert _adapter_with(stub).update_po_status(60, "MAYBE", "qahir") is False
    assert not stub.did("purchase.order.line", "unlink")
    assert not stub.did("purchase.order.line", "write")


def test_a_missing_line_does_not_fall_back_to_the_store_table():
    """po_id is a purchase.order.line id here, but PosErpAdapter's PO_ID is an
    INTEGRATION_PURCHASE_ORDERS key. Same parameter, different id spaces — a
    fallback would hit an unrelated row with an equal id and report success."""
    stub = _POStub()
    assert _adapter_with(stub).update_po_status(99999999, "APPROVED", "q") is False


def test_a_successful_cancel_that_raises_is_not_treated_as_failure():
    """button_cancel returns None and Odoo's XML-RPC dumps with
    allow_none=False, so a cancel that COMMITTED still raises."""
    import xmlrpc.client

    class _Marshalling(_POStub):
        def __call__(self, model, method, args, kw=None):
            if model == "purchase.order" and method == "button_cancel":
                self.state = "cancel"          # the write DID land
                raise xmlrpc.client.Fault(
                    1, "TypeError: cannot marshal None unless allow_none is enabled")
            return super().__call__(model, method, args, kw)

    stub = _Marshalling(lines=(60,))
    assert _adapter_with(stub).update_po_status(60, "REJECTED", "qahir") is True
    assert stub.state == "cancel"
