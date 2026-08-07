"""Desktop Phase 3 / P3.1 — the native Command Center.

Two properties, both of which the first draft of this view failed:

  * **it is licensed.** The Command Center is where Smart Ordering and Transfer
    Intelligence actually live in the native window. Shipped ungated it is the
    paid-module bypass of finding R-2 — the unlicensed window doing the work
    the licensed console refuses. The gate must stop the PIPELINE, not merely
    decline to draw its output.
  * **it is not a second source of truth.** Every number comes from an accessor
    in ``oasis.desktop.data``. An earlier draft imported ``simulation_bridge``
    and ``gnn_service`` into the tab and re-derived the ordering pipeline; that
    is how the two front doors drift.

Built as real Flet control trees, headless — "it launched" is not evidence.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.desktop import data as D
from oasis.desktop.views.command_view import TAB_MODULES, build_command_view
from oasis.logic import license_manager as LM


def _walk(control):
    stack, seen = [control], []
    while stack:
        c = stack.pop()
        seen.append(c)
        for attr in ("controls", "content", "tabs"):
            v = getattr(c, attr, None)
            if isinstance(v, list):
                stack.extend(v)
            elif v is not None and hasattr(v, "_get_control_name"):
                stack.append(v)
    return seen


def _text(control) -> str:
    out = []
    for c in _walk(control):
        for attr in ("value", "text", "tooltip", "label"):
            v = getattr(c, attr, None)
            if isinstance(v, str):
                out.append(v)
    return " ".join(out).lower()


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A real sample store with a controllable licence."""
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
    monkeypatch.setenv("OASIS_DB_PATH", str(tmp_path / "nope.db"))
    monkeypatch.delenv("OASIS_DB_URL", raising=False)
    D.reset_adapter()
    yield str(tmp_path)
    D.reset_adapter()


# ── the view builds ──────────────────────────────────────────────────────
def test_command_view_builds_against_a_real_store(store, monkeypatch):
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    view = build_command_view(None, store)
    assert len(_walk(view)) > 20
    body = _text(view)
    for tab in ("live sales", "transfers", "stock review", "ordering"):
        assert tab in body


def test_command_view_builds_with_no_store(empty, monkeypatch):
    """No store must read as 'no store', not crash the window."""
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    view = build_command_view(None, empty)
    assert "no stores" in _text(view)


# ── licensing ────────────────────────────────────────────────────────────
def test_every_gated_tab_names_a_real_module_sku():
    """A typo here would paywall a tab forever — nothing could ever grant it."""
    assert set(TAB_MODULES.values()) <= set(LM.KNOWN_MODULES)


def test_command_center_paywalls_match_the_operations_view():
    """Both native surfaces must draw the paywall on the same capabilities."""
    from oasis.desktop.views.ops_view import TAB_MODULES as OPS
    assert TAB_MODULES["ordering"] == OPS["ordering_actions"]
    assert TAB_MODULES["transfers"] == OPS["transfers"]


def test_core_only_licence_locks_ordering_and_transfers(store, monkeypatch):
    monkeypatch.setattr(D, "allowed_modules", lambda: {"core"})
    body = _text(build_command_view(None, store))
    assert "smart ordering module" in body
    assert "network (transfers & allocation) module" in body
    # …while the core tabs still render their own content.
    assert "top movers" in body or "no sales recorded" in body
    assert "product detail" in body or "no product data" in body


def test_a_locked_tab_never_runs_the_pipeline(store, monkeypatch):
    """The gate stops the WORK, not just the drawing.

    Rendering an upsell over a pipeline that already ran would still have done
    the unlicensed store the paid computation — and paid for it in seconds.
    """
    calls = []
    monkeypatch.setattr(D, "allowed_modules", lambda: {"core"})
    monkeypatch.setattr(D, "generate_smart_orders",
                        lambda *a, **k: calls.append("generate") or {})
    monkeypatch.setattr(D, "network_risk",
                        lambda *a, **k: calls.append("risk") or {})
    monkeypatch.setattr(D, "network_stockout_risk",
                        lambda *a, **k: calls.append("scan") or {})
    build_command_view(None, store)
    assert calls == []


def test_full_licence_shows_the_real_panels(store, monkeypatch):
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    body = _text(build_command_view(None, store))
    assert "smart ordering (po generation)" in body
    assert "store risk scores" in body
    assert "contact ilink to activate" not in body


def test_the_ordering_tab_offers_generate_and_approve(store, monkeypatch):
    """The 'runnable day' milestone: the window can act, not only read."""
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    from oasis.desktop.views.command_tabs.smart_ordering_tab import (
        build_smart_ordering_tab)
    tab = build_smart_ordering_tab(None, store)
    body = _text(tab)
    assert "generate orders" in body
    assert "pending approvals" in body
    clickable = [c for c in _walk(tab) if getattr(c, "on_click", None)]
    assert clickable, "the ordering tab has no actionable control"


# ── the accessors the tabs were re-deriving ──────────────────────────────
def test_command_accessors_return_usable_shapes(store):
    org = D.default_org(store)
    health = D.stock_health(org, store)
    assert health["error"] is None
    assert set(health["counts"]) == {"HEALTHY", "CRITICAL", "STOCKOUT", "OVERSTOCK"}
    assert sum(health["counts"].values()) == len(health["items"])

    scan = D.network_stockout_risk(store)
    assert scan["error"] is None
    covers = [i["days_cover"] for i in scan["items"]]
    assert all(c < 3.0 for c in covers)

    risk = D.network_risk(store)
    assert risk["error"] is None
    assert all(0.0 <= s["risk"] <= 1.0 for s in risk["stores"])


def test_command_accessors_never_raise_on_a_missing_store(empty):
    """A missing store must surface as an error field, never an exception."""
    for res in (D.stock_health("NOPE", empty),
                D.network_stockout_risk(empty),
                D.network_risk(empty),
                D.transfer_status("NOPE", empty),
                D.live_sales("NOPE", root=empty)):
        assert isinstance(res, dict)


def test_stockout_scan_is_sorted_worst_first(store):
    order = {"DEPLETED": 0, "CRITICAL": 1, "URGENT": 2, "LOW": 3}
    ranks = [order[i["severity"]] for i in D.network_stockout_risk(store)["items"]]
    assert ranks == sorted(ranks)


def test_live_sales_never_invents_a_basket_value(store):
    """A feed with no bill identifier yields None — not line_count x 5.

    The first draft multiplied the line count by a hardcoded 5 to produce an
    'Avg Basket Value'. That is a fabricated KPI on a client's dashboard.
    """
    s = D.live_sales(D.default_org(store), root=store)
    assert s["error"] is None
    if s["baskets"] is None:
        assert s["basket_value"] is None
    else:
        assert s["basket_value"] == pytest.approx(s["revenue"] / s["baskets"])


def test_live_sales_reports_line_items_as_line_items(store):
    s = D.live_sales(D.default_org(store), root=store)
    assert s["lines"] >= 0
    assert s["skus"] <= s["lines"]


def test_stock_health_uses_a_shorter_horizon_for_perishables(store):
    """14 days of cover on milk is spoilage; on ambient stock it is depth."""
    items = D.stock_health(D.default_org(store), store)["items"]
    for i in items:
        if i["health"] == "OVERSTOCK" and i["days_cover"] is not None:
            assert i["days_cover"] > (14.0 if i["is_fresh"] else 30.0)


# ── no parallel implementations ──────────────────────────────────────────
def test_the_tabs_do_not_re_derive_the_pipeline():
    """Presentation only. Logic belongs in data.py, next to its twin."""
    import pathlib
    tabs = pathlib.Path(__file__).parent.parent / "oasis" / "desktop" / "views" / "command_tabs"
    banned = ("simulation_bridge", "order_engine", "consolidated_transfer_service")
    for path in tabs.glob("*.py"):
        src = path.read_text(encoding="utf-8")
        for name in banned:
            assert f"import {name}" not in src and f"{name} import" not in src, \
                f"{path.name} re-derives the pipeline via {name}"
