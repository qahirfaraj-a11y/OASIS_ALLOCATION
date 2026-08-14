"""Desktop Phase 2 views — the coverage the Flet layer never had.

Phase 1 shipped views written against an assumed backend (authenticate_user,
license_posture) that did not exist. Nothing caught it because building a Flet
control tree was never exercised. It does not need a display — only a running
app does — so these tests construct the real trees against a real store.

They assert three things the desktop must not get wrong:
  * every accessor in oasis.desktop.data returns a usable shape, never raises
  * the views build for a store, for NO store, and for a multi-store network
  * "no data" and "could not read data" stay visibly different
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.desktop import data as D
from oasis.desktop.views.intel_view import build_intel_view
from oasis.desktop.views.ops_view import build_ops_view
from oasis.desktop.views.market_view import build_market_view


def _count(control) -> int:
    """Walk a Flet control tree; proves every node constructed."""
    n = 0
    stack = [control]
    while stack:
        c = stack.pop()
        n += 1
        for attr in ("controls", "content", "tabs"):
            v = getattr(c, attr, None)
            if isinstance(v, list):
                stack.extend(v)
            elif v is not None and hasattr(v, "_get_control_name"):
                stack.append(v)
    return n


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A real sample store, with the adapter cache cleared around it."""
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.setenv("OASIS_DB_PATH",
                       str(tmp_path / "oasis" / "data" / "store.db"))
    monkeypatch.delenv("OASIS_DB_URL", raising=False)
    monkeypatch.delenv("OASIS_POS_DB_URL", raising=False)
    D.reset_adapter()
    from oasis.logic import onboarding as OB
    OB.apply_demo(root=str(tmp_path))
    D.reset_adapter()
    yield str(tmp_path)
    D.reset_adapter()


@pytest.fixture
def empty(tmp_path, monkeypatch):
    """An install with no store at all."""
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.setenv("OASIS_DB_PATH",
                       str(tmp_path / "oasis" / "data" / "nothing.db"))
    D.reset_adapter()
    yield str(tmp_path)
    D.reset_adapter()


# ── data layer ───────────────────────────────────────────────────────────
def test_accessors_return_usable_shapes(store):
    org = D.default_org(store)
    assert org, "the sample store must expose an organization"

    stock = D.stock_overview(org, store)
    assert stock["error"] is None
    assert stock["skus"] > 0 and stock["stock_value"] > 0

    sup = D.supplier_overview(org, store)
    assert sup["error"] is None and sup["suppliers"] > 0
    assert all({"name", "skus"} <= set(s) for s in sup["top"])

    pend = D.pending_orders(org, store)
    assert pend["error"] is None and isinstance(pend["rows"], list)


def test_accessors_never_raise_on_a_missing_store(empty):
    """A view asking for numbers gets an error field, not a traceback."""
    assert D.list_stores(empty) == []
    assert D.default_org(empty) is None
    stock = D.stock_overview("ORG001", empty)
    assert stock["skus"] == 0            # shape intact even on failure
    assert D.supplier_overview("ORG001", empty)["suppliers"] == 0


def test_engine_posture_reports_the_resolved_tier(store):
    eng = D.engine_posture(store)
    assert eng["error"] is None
    assert eng["tier"] in ("live", "default", "none")
    assert eng["engines"], "the engine layer must be reported, not silently empty"


def test_provenance_flags_sample_data(store):
    prov = D.data_provenance(store)
    assert prov["source"] == "demo" and prov["is_sample"] is True


# ── views ────────────────────────────────────────────────────────────────
def test_both_views_build_against_a_real_store(store):
    assert _count(build_ops_view(None, store)) > 50
    assert _count(build_intel_view(None, store)) > 40
    assert _count(build_market_view(None, store)) > 20


def test_both_views_build_with_no_store(empty):
    """Must degrade to an explanation, not crash the window."""
    assert _count(build_ops_view(None, empty)) > 0
    assert _count(build_intel_view(None, empty)) > 0
    assert _count(build_market_view(None, empty)) > 0


def test_multi_store_network_builds_the_transfer_branch(tmp_path, monkeypatch):
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.setenv("OASIS_DATA_DIR", str(tmp_path / "oasis" / "data"))
    monkeypatch.setenv("OASIS_DB_PATH",
                       str(tmp_path / "oasis" / "data" / "multi.db"))
    monkeypatch.setattr("oasis.logic.multi_store_pos.seed_multi_store_history",
                        lambda db, **kw: {})
    D.reset_adapter()
    from oasis.logic import onboarding as OB
    OB.apply_multi_demo(root=str(tmp_path))
    D.reset_adapter()
    try:
        assert len(D.list_stores(str(tmp_path))) == 5
        assert _count(build_ops_view(None, str(tmp_path))) > 50
    finally:
        D.reset_adapter()


def test_zero_pending_orders_reads_differently_from_a_read_failure():
    """'queue clear' and 'could not read orders' must not look the same."""
    from oasis.desktop.views.ops_view import _pending_state
    val_ok, status_ok, sub_ok = _pending_state({"count": 0, "error": None})
    val_err, status_err, sub_err = _pending_state({"count": 0, "error": "boom"})
    assert (val_ok, status_ok) == ("0", "success")
    assert val_err == "—" and status_err == "danger"
    assert sub_ok != sub_err


# ── ordering has ONE front door ──────────────────────────────────────────
#: the three write paths in oasis.desktop.data. Ordering is generated, pushed
#: and approved in the Command Center's Smart Ordering tab and NOWHERE else, so
#: there is a single audit trail and an operator cannot push the same PO twice
#: from two screens. Operations reports on the order book; it never writes to it.
_ORDER_WRITES = ("generate_smart_orders", "push_purchase_order",
                 "update_po_status")


def test_operations_view_never_writes_to_the_order_book():
    """/ops is read-only for orders. A re-added button must fail here."""
    import inspect
    from oasis.desktop.views import ops_view
    src = inspect.getsource(ops_view)
    for fn in _ORDER_WRITES:
        assert fn not in src, (
            f"ops_view calls {fn}: ordering must stay in the Command Center"
        )


def test_the_command_center_still_owns_those_writes():
    """The other half of the invariant — read-only /ops must not mean nobody
    can order at all. If this fails, the capability was lost, not moved."""
    import inspect
    from oasis.desktop.views.command_tabs import smart_ordering_tab
    src = inspect.getsource(smart_ordering_tab)
    for fn in _ORDER_WRITES:
        assert fn in src, f"Smart Ordering lost {fn} — ordering has no front door"


def test_operations_still_shows_the_order_book_read_only(store):
    """Read-only is not blank: the pending queue is still reported."""
    from tests.test_desktop_license_gate import _text
    body = _text(build_ops_view(None, store))
    assert "pending approvals" in body
    assert "command center" in body, "must say where the actions moved to"
