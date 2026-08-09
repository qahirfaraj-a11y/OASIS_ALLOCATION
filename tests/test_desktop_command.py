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
    """Every node in a Flet tree.

    Includes ``rows``/``cells`` — a DataTable keeps its contents there, not in
    ``controls``, so a walker that skips them reports a table of 200 product
    rows as a single node and proves nothing about what is in it.
    """
    stack, seen = [control], []
    while stack:
        c = stack.pop()
        seen.append(c)
        for attr in ("controls", "content", "tabs", "rows", "cells", "columns"):
            v = getattr(c, attr, None)
            if isinstance(v, list):
                stack.extend(x for x in v if hasattr(x, "_get_control_name"))
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
    assert "network (transfers) module" in body
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


# ── layout that Flutter can actually lay out ─────────────────────────────
def _wrapping_rows_with_expanding_children(control):
    """Rows that wrap AND hold an expanding child — a guaranteed grey void.

    Flet raises nothing for this; Flutter fails to lay the Wrap out and paints
    a blank block where the content should be. Only a structural check catches
    it, which is why "the view built" was not enough.
    """
    bad = []
    for c in _walk(control):
        if getattr(c, "wrap", False):
            for child in (getattr(c, "controls", None) or []):
                if getattr(child, "expand", None):
                    bad.append((c, child))
    return bad


def test_no_wrapping_row_holds_an_expanding_child(store, monkeypatch):
    """The Command Center's store-risk strip shipped exactly this defect.

    ``metric_card`` expands by default; the risk strip wraps because a demo
    network is 14 stores. Together they rendered as a grey rectangle where the
    scores should have been.
    """
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    assert _wrapping_rows_with_expanding_children(
        build_command_view(None, store)) == []


def test_a_multi_store_network_still_lays_out(tmp_path, monkeypatch):
    """The wrap only triggers past one line of cards — so build the wide case."""
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.setenv("OASIS_DB_PATH", str(tmp_path / "oasis" / "data" / "s.db"))
    monkeypatch.delenv("OASIS_DB_URL", raising=False)
    D.reset_adapter()
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    monkeypatch.setattr(D, "list_stores", lambda root=None: [
        {"org_cd": f"ORG{i:03d}", "name": f"Store {i}"} for i in range(1, 15)])
    monkeypatch.setattr(D, "network_risk", lambda root=None: {
        "status": "trained", "error": None,
        "stores": [{"org_cd": f"ORG{i:03d}", "name": f"Store {i}",
                    "risk": i / 20.0} for i in range(1, 15)]})
    from oasis.desktop.views.command_tabs.transfer_intel_tab import (
        build_transfer_intel_tab)
    tab = build_transfer_intel_tab(None, str(tmp_path))
    assert _wrapping_rows_with_expanding_children(tab) == []
    assert "store 14" in _text(tab)
    D.reset_adapter()


def test_metric_card_drops_expand_when_given_a_width():
    from oasis.desktop import theme as T
    assert T.metric_card("a", "1").expand is True
    fixed = T.metric_card("a", "1", width=200)
    assert fixed.width == 200 and not fixed.expand


# ── surfaces a client install can actually reach ─────────────────────────
def test_store_intelligence_reads_the_store_not_a_simulator(store, monkeypatch):
    """It answered from the GNN NetworkSimulator, so it was dead on arrival.

    Two defects in one: a client's store numbers were sourced from a
    *simulation* rather than their POS, and the simulator's dependencies
    (network_simulation, models/) are not on the release whitelist — so on
    every client install the tab could only say "GNN resources unavailable".
    """
    monkeypatch.setattr("oasis.logic.gnn_service.get_gnn_resources",
                        lambda: (None, None))          # exactly a client install
    si = D.store_intelligence(D.default_org(store), root=store)
    assert si["error"] is None
    assert si["top_qty"], "no top movers from a store that has sales history"
    assert all(e["Units"] >= 0 for e in si["top_qty"])
    assert si["top_qty"][0]["Units"] >= si["top_qty"][-1]["Units"]
    assert si["categories"], "no category mix"
    # The catalogue keys a SKU as `item_code` and the sales feed as `itm_cd`.
    # Joining on the wrong one files everything under "Uncategorised" and the
    # category mix silently becomes one meaningless bar.
    named = [c["Category"] for c in si["categories"]
             if c["Category"] != "Uncategorised"]
    assert named, "every product fell through to Uncategorised — bad SKU join"
    # market_view renders these keys directly; a missing one is a KeyError in
    # the view, not a degraded number.
    assert {"Product", "Category", "Units", "Revenue", "OnHand"} <= set(
        si["top_qty"][0])


def test_market_view_renders_store_intelligence_without_a_graph(store,
                                                                monkeypatch):
    """The view and the accessor must agree on field names.

    store_intelligence used to emit ``Stockouts`` (the simulator's stockout-day
    counter) and market_view indexed it directly, so re-sourcing the accessor
    from real sales broke the tab with a KeyError.
    """
    monkeypatch.setattr("oasis.logic.gnn_service.get_gnn_resources",
                        lambda: (None, None))
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    from oasis.desktop.views.market_view import build_market_view
    body = _text(build_market_view(None, store))
    assert "failed to read store intelligence" not in body
    assert "on hand" in body          # the real column replaced the simulated one
    assert "stockouts" not in body


def test_root_is_still_the_second_positional_arg_of_store_intelligence():
    """market_view calls store_intelligence(org, project_root) positionally."""
    import inspect
    params = list(inspect.signature(D.store_intelligence).parameters)
    assert params[:2] == ["org_cd", "root"]


def test_transfer_intelligence_keeps_risk_when_the_graph_is_absent(store,
                                                                   monkeypatch):
    """Losing the ST-GAT proposals must not cost the client the risk view."""
    monkeypatch.setattr("oasis.logic.gnn_service.get_gnn_resources",
                        lambda: (None, None))
    ti = D.transfer_intelligence(store)
    assert ti["error"], "should say the graph is missing"
    assert "graph" in ti["error"].lower()
    assert ti["recs"] == []
    assert ti["risks"], "risk went dark with the proposals"


def test_graph_dependent_surfaces_return_their_full_shape_on_failure(store,
                                                                     monkeypatch):
    """A caller reading res['clusters'] must not hit a KeyError on the sad path."""
    monkeypatch.setattr("oasis.logic.gnn_service.get_gnn_resources",
                        lambda: (None, None))
    assert "clusters" in D.cluster_analysis(store)
    assert {"risks", "recs"} <= set(D.transfer_intelligence(store))


# ── no parallel implementations ──────────────────────────────────────────
def test_every_mode_the_desktop_tells_a_client_to_run_exists():
    """Placeholder cards send clients somewhere. It has to be somewhere real.

    The Intelligence view told a client to run ``--mode shadow`` for
    backtesting. P3.3 stripped the dev-only modes, so that instruction produced
    an argparse usage error — the same E-3 defect as the dead demo submenu,
    wearing UI copy instead of a menu entry.
    """
    import pathlib
    import re
    repo = pathlib.Path(__file__).parent.parent
    entry = (repo / "entrypoint.py").read_text(encoding="utf-8")
    choices = set(re.findall(
        r'"([a-z0-9-]+)"',
        re.search(r'--mode",\s*\n?\s*choices=\[(.*?)\]', entry, re.S).group(1)))
    assert "desktop" in choices, "failed to parse --mode choices"

    referenced = set()
    for path in (repo / "oasis" / "desktop").rglob("*.py"):
        referenced |= set(re.findall(r'--mode\s+([a-z0-9-]+)',
                                     path.read_text(encoding="utf-8")))
    assert referenced <= choices, (
        f"the desktop app references --mode {sorted(referenced - choices)}, "
        f"which entrypoint.py does not accept")


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
