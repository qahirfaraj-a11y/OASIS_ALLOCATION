"""Command Center parity: the Flet app against the Streamlit console.

``run_command_center_multi.bat`` (``--mode dashboard --dashboard command`` →
``ops_dashboard.py``) is the REFERENCE architecture. The native Command Center
is being brought up to it tab by tab. This file is the ratchet: each parity
property, once matched, is pinned here so it cannot silently regress.

The console is deliberately NOT imported or modified. It is a 3,400-line
Streamlit script that executes on import — so where a rule has to be shared,
this file reads the console's SOURCE and asserts the native side agrees. That
way the reference stays the reference and drift is caught from either side.
"""

import os
import pathlib
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.desktop import data as D

CONSOLE = pathlib.Path(__file__).parent.parent / "ops_dashboard.py"


@pytest.fixture(scope="module")
def console_src() -> str:
    return CONSOLE.read_text(encoding="utf-8", errors="replace")


@pytest.fixture
def store(tmp_path, monkeypatch):
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


# ── End-of-Day Stock ─────────────────────────────────────────────────────
def _console_health_bands(src: str) -> dict:
    """The thresholds inside ops_dashboard's get_health()."""
    body = re.search(r"def get_health\(row\):(.*?)\n\s*df_p\[", src, re.S)
    assert body, "could not find get_health() in the console"
    body = body.group(1)
    return {
        "stockout": float(re.search(r'if d < ([\d.]+): return "🔴 Stockout"', body).group(1)),
        "critical": float(re.search(r'if d < ([\d.]+): return "🟡 Critical"', body).group(1)),
        "fresh": float(re.search(r"overstock_limit = ([\d.]+) if is_fresh", body).group(1)),
        "ambient": float(re.search(r"if is_fresh else ([\d.]+)", body).group(1)),
    }


def test_overstock_horizons_match_the_console(console_src):
    """Where we agree with the console, stay agreeing with it."""
    c = _console_health_bands(console_src)
    assert D.OVERSTOCK_COVER_DAYS_FRESH == c["fresh"]
    assert D.OVERSTOCK_COVER_DAYS_AMBIENT == c["ambient"]


def test_stockout_and_critical_deliberately_diverge_from_the_console(console_src):
    """A DECIDED difference, pinned so it is not "fixed" back.

    The console calls a line a stockout under half a day of cover and critical
    under two. A shelf with stock on it is not a stockout — it is about to be
    one, and collapsing the two costs the operator the distinction they act on.
    So STOCKOUT means nothing on hand and CRITICAL means under a day of cover.
    Reviewed and chosen 2026-08-08.

    This test exists to make the divergence loud: if someone aligns the native
    bands to the console's ladder, it fails and they have to re-make the call.
    """
    c = _console_health_bands(console_src)
    assert c["stockout"] == 0.5 and c["critical"] == 2.0, \
        "the console's ladder moved — re-decide the divergence deliberately"
    assert D.CRITICAL_COVER_DAYS == 1.0
    assert not hasattr(D, "STOCKOUT_COVER_DAYS"), \
        "stockout is an on-hand test here, not a cover threshold"


def test_stock_on_the_shelf_is_never_a_stockout():
    """The whole point of the divergence."""
    assert D.classify_health(4.0, 0.2, False) == "CRITICAL"   # console: STOCKOUT
    assert D.classify_health(0.0, 0.0, False) == "STOCKOUT"
    assert D.classify_health(0.0, None, False) == "STOCKOUT"  # empty, never sold


def test_the_console_still_uses_the_fresh_keywords_we_mirror(console_src):
    """is_fresh_line copies the console's department keywords."""
    for key in ("MILK", "DAIRY", "FRESH", "MEAT", "BREAD", "BAKERY"):
        assert key in console_src, f"console no longer treats {key} as fresh"
        assert D.is_fresh_line({"department": f"X {key} Y"})


@pytest.mark.parametrize("on_hand,cover,fresh,expected", [
    (0.0, 0.0, False, "STOCKOUT"),     # nothing on the shelf
    (0.0, None, False, "STOCKOUT"),
    (4.0, 0.2, False, "CRITICAL"),     # nearly gone, but still sellable
    (4.0, 0.9, False, "CRITICAL"),
    (4.0, 1.0, False, "HEALTHY"),
    (4.0, 29.9, False, "HEALTHY"),
    (4.0, 30.1, False, "OVERSTOCK"),
    (4.0, 13.9, True, "HEALTHY"),
    (4.0, 14.1, True, "OVERSTOCK"),    # perishables overstock two weeks sooner
    (4.0, None, False, "HEALTHY"),     # stocked, no demand signal
])
def test_classify_health_ladder(on_hand, cover, fresh, expected):
    assert D.classify_health(on_hand, cover, fresh) == expected


def test_a_line_with_no_demand_has_no_cover_figure(store):
    """No ADS means cover is unknown, not zero and not 999.

    Reporting a sentinel as a number puts "999.0 days" in front of an operator
    as though it were measured.
    """
    items = D.stock_health(D.default_org(store), store)["items"]
    for i in items:
        assert (i["days_cover"] is None) == (not i["has_demand"])
        if i["days_cover"] is None and i["stock"] > 0:
            assert i["health"] == "HEALTHY"


def test_stock_health_counts_add_up(store):
    h = D.stock_health(D.default_org(store), store)
    assert sum(h["counts"].values()) == len(h["items"])


def test_stock_detail_carries_every_console_column(store):
    """The console's Stock Detail table is Product/Department/Qty/ADS/Cover/Health."""
    items = D.stock_health(D.default_org(store), store)["items"]
    assert items
    assert {"name", "dept", "stock", "ads", "days_cover", "health"} <= set(items[0])


# ── Live Sales ───────────────────────────────────────────────────────────
def test_the_console_hourly_pattern_is_synthetic(console_src):
    """Documents why the native app does NOT port the Hourly Revenue Pattern.

    The POS schema has no time-of-day at all — BILL_DT is a date and there is
    no BILL_TIME — so the console manufactures the hour with
    `np.random.normal(14, 3)`. Porting that would move a fabricated
    distribution into a second product surface. If this assertion ever fails,
    the console has gained a real clock and the chart becomes portable.
    """
    assert "np.random.normal(14, 3" in console_src


def test_sales_history_exposes_the_bill_number(store):
    """Baskets are only countable with a bill id — see fetch_sales_history."""
    adapter = D.get_adapter(store)
    df = adapter.fetch_sales_history(D.default_org(store), days=90)
    assert "bill_no" in [str(c).lower() for c in df.columns]


def test_basket_value_is_real_now_that_bills_are_countable(store):
    s = D.live_sales(D.default_org(store), root=store)
    assert s["baskets"], "no baskets counted despite a bill id in the feed"
    assert s["basket_value"] == pytest.approx(s["revenue"] / s["baskets"])
    assert s["baskets"] <= s["lines"], "more baskets than line items"


def test_velocity_matches_the_console_formula_at_day_end(console_src, store):
    """units / (ads * elapsed/14) with elapsed = the full trading day.

    The console divides by a synthetic intra-day clock; over a COMPLETED day
    the term is 1 and the ratio is units over the daily average. Same formula,
    no invented hour.
    """
    assert "elapsed_hours / 14.0" in console_src, \
        "console changed its velocity denominator"
    assert D.TRADING_DAY_HOURS == 14.0

    s = D.live_sales(D.default_org(store), root=store)
    rated = [t for t in s["top"] if t["velocity_ratio"] is not None]
    assert rated, "no line carried a velocity ratio"
    for t in rated:
        assert t["velocity_ratio"] == pytest.approx(round(t["units"] / t["ads"], 1))


def test_the_spike_threshold_matches_the_console(console_src):
    m = re.search(r"AlertMonitor\(spike_threshold_pct=([\d.]+)\)", console_src)
    assert m, "console no longer constructs AlertMonitor with a threshold"
    assert D.VELOCITY_SPIKE_PCT == float(m.group(1))


def test_velocity_alerts_run_the_same_engine_as_the_console(store):
    """Not a re-implementation: alerts come from AlertMonitor itself."""
    import oasis.logic.alert_monitor as AMon
    calls = []
    real = AMon.AlertMonitor.check_velocity_spikes

    def spy(self, batch, stats, elapsed_hours=1.0):
        calls.append(elapsed_hours)
        return real(self, batch, stats, elapsed_hours)

    AMon.AlertMonitor.check_velocity_spikes = spy
    try:
        D.live_sales(D.default_org(store), root=store)
    finally:
        AMon.AlertMonitor.check_velocity_spikes = real
    assert calls == [D.TRADING_DAY_HOURS]


def test_an_alert_carries_what_the_console_card_shows(store):
    """type, product, message and recommended action — the alert-card fields."""
    alerts = D.live_sales(D.default_org(store), root=store)["alerts"]
    if not alerts:
        pytest.skip("no spike on this demo day")
    a = alerts[0]
    assert {"type", "product_name", "message", "recommended_action"} <= set(a)
    assert a["velocity_ratio"] >= 2.0, "alerted below the spike threshold"


def test_alerts_are_worst_first(store):
    ratios = [a["velocity_ratio"] for a
              in D.live_sales(D.default_org(store), root=store)["alerts"]]
    assert ratios == sorted(ratios, reverse=True)


def test_the_day_over_day_trend_is_real_dates(store):
    trend = D.live_sales(D.default_org(store), root=store)["trend"]
    assert len(trend) > 1
    assert [t["day"] for t in trend] == sorted(t["day"] for t in trend)
    assert all(t["revenue"] >= 0 and t["units"] >= 0 for t in trend)


# ── Transfer Intelligence ────────────────────────────────────────────────
@pytest.fixture
def network(tmp_path, monkeypatch):
    """A 5-store demo network — transfers need somewhere to transfer to."""
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.setenv("OASIS_DB_PATH",
                       str(tmp_path / "oasis" / "data" / "net.db"))
    monkeypatch.delenv("OASIS_DB_URL", raising=False)
    monkeypatch.delenv("OASIS_POS_DB_URL", raising=False)
    D.reset_adapter()
    from oasis.logic import onboarding as OB
    OB.apply_multi_demo(root=str(tmp_path))
    D.reset_adapter()
    yield str(tmp_path)
    D.reset_adapter()


def test_the_scan_runs_the_shared_service_not_a_copy(console_src, network):
    """Both surfaces call ConsolidatedTransferService.scan_network_opportunities."""
    assert "scan_network_opportunities" in console_src
    import oasis.logic.consolidated_transfer_service as CTS
    calls = []
    real = CTS.ConsolidatedTransferService.scan_network_opportunities

    def spy(self, **kw):
        calls.append(kw)
        return real(self, **kw)

    CTS.ConsolidatedTransferService.scan_network_opportunities = spy
    try:
        scan = D.network_transfer_scan(network)
    finally:
        CTS.ConsolidatedTransferService.scan_network_opportunities = real
    assert scan["error"] is None
    assert len(calls) == 1
    # The two inputs that make the scan honest, both of which the console passes.
    assert "moq_failures" in calls[0] and "pending_transfers" in calls[0]


def test_store_health_carries_the_console_columns(network):
    scan = D.network_transfer_scan(network)
    assert scan["store_health"]
    assert {"store", "org_cd", "total_skus", "overstock", "deficits",
            "push_from", "risk", "status"} <= set(scan["store_health"][0])
    deficits = [h["deficits"] for h in scan["store_health"]]
    assert deficits == sorted(deficits, reverse=True), "not worst-first"


def test_opportunities_carry_the_console_columns(network):
    opps = D.network_transfer_scan(network)["opportunities"]
    assert opps, "no transfer opportunities in a 5-store demo network"
    assert {"type", "product", "from", "to", "qty", "donor_cover",
            "recipient_cover", "donor_excess", "value", "department",
            "supplier", "itm_cd", "manual_only"} <= set(opps[0])
    assert all(o["type"] in ("PULL", "PUSH") for o in opps)


def test_a_single_store_install_says_so_instead_of_failing(store):
    scan = D.network_transfer_scan(store)
    assert scan["error"] and "single-store" in scan["error"].lower()
    assert scan["opportunities"] == [] and scan["store_health"] == []


def test_fresh_lines_are_shown_but_never_auto_queued(network):
    """The console skips `_is_fresh` when queuing. Perishables are a judgement."""
    opps = D.network_transfer_scan(network)["opportunities"]
    fresh = [o for o in opps if o["manual_only"]]
    if not fresh:
        pytest.skip("no perishable opportunity in this network")
    res = D.queue_transfers(fresh, "tester", "ORG001", limit=len(fresh),
                            root=network)
    assert res["queued"] == 0
    assert res["skipped"] == len(fresh)


def test_queue_then_advance_a_transfer_end_to_end(network):
    """scan → queue → the row exists → status advances. The whole loop."""
    rows = _queue_some(network)
    assert rows, "queued transfers did not land in the store"
    # The identifier the operator types into Update Status must be visible.
    assert rows[0].get("TRANSFER_ID") is not None

    tid = rows[0]["TRANSFER_ID"]
    assert D.set_transfer_status(tid, "IN_TRANSIT", "tester", "ORG001",
                                 root=network)["success"]
    after = {r["TRANSFER_ID"]: r["STATUS"]
             for r in D.transfer_status(None, network)["rows"]}
    assert after[tid] == "IN_TRANSIT"


def test_an_unknown_status_is_refused(network):
    assert not D.set_transfer_status(1, "NONSENSE", "t", "ORG001",
                                     root=network)["success"]


def _queue_some(network, n=6):
    """Queue actionable (non-fresh) opportunities and return the rows."""
    opps = [o for o in D.network_transfer_scan(network)["opportunities"]
            if not o["manual_only"]]
    assert opps, "network produced only manual-only opportunities"
    res = D.queue_transfers(opps, "tester", "ORG001", limit=n, root=network)
    assert res["queued"] > 0, res
    rows = D.transfer_status(None, network)["rows"]
    assert rows, "queued transfers are not visible network-wide"
    return rows


def test_transfer_rows_use_the_real_column_names(network):
    """ID / FROM_ORG / TO_ORG / QTY do not exist on this table.

    An earlier native draft read those names, so every cell rendered blank and
    there was no transfer id to act on.
    """
    rows = _queue_some(network)
    assert rows
    assert {"TRANSFER_ID", "FROM_ORG_CD", "TO_ORG_CD", "QUANTITY", "STATUS",
            "VALUE_KES"} <= set(rows[0])


def test_transfers_are_shown_network_wide_not_just_the_first_store(network):
    """A transfer has two ends. The console passes org_filter=None for anyone
    who can view all stores; filtering to store one hides every movement
    between the other four."""
    _queue_some(network)
    everywhere = D.transfer_status(None, network)["rows"]
    orgs = {r["FROM_ORG_CD"] for r in everywhere} | {r["TO_ORG_CD"] for r in everywhere}
    assert len(orgs) > 1


def test_transfer_tab_exposes_scan_queue_and_status(network):
    from oasis.desktop.views.command_tabs.transfer_intel_tab import (
        build_transfer_intel_tab)
    _queue_some(network)
    tab = build_transfer_intel_tab(None, network)
    body = _text(tab)
    assert "live network transfer opportunities" in body
    assert "scan network for transfers" in body
    assert "update status" in body and "transfer id" in body

    scan_btn = [c for c in _walk(tab)
                if getattr(c, "text", "") == "Scan network for transfers"]
    assert scan_btn, "no way to run the network scan"
    scan_btn[0].on_click(None)
    after = _text(tab)
    assert "store-level inventory health" in after
    assert "recommended item-level transfers" in after
    assert "queue transfers to database" in after


# ── the tabs render what the accessors provide ───────────────────────────
def test_live_sales_tab_shows_alerts_and_velocity(store):
    from oasis.desktop.views.command_tabs.live_sales_tab import (
        build_live_sales_tab)
    body = _text(build_live_sales_tab(None, store))
    assert "velocity alerts" in body
    assert "top movers" in body
    assert "multi-day trend" in body
    assert "hourly" not in body, "ported the console's synthetic hourly chart"


def test_stock_review_tab_plots_stock_against_demand(store):
    import flet as ft
    from oasis.desktop.views.command_tabs.stock_review_tab import (
        build_stock_review_tab)
    tab = build_stock_review_tab(None, store)
    charts = [c for c in _walk(tab) if isinstance(c, ft.LineChart)]
    assert charts, "no Stock Volume vs Demand plot"
    points = [c for c in _walk(tab) if isinstance(c, ft.LineChartDataPoint)]
    assert points, "the plot has no points"
    assert "stock volume vs demand" in _text(tab)


def _walk(control):
    stack, seen = [control], []
    while stack:
        c = stack.pop()
        seen.append(c)
        for attr in ("controls", "content", "tabs", "rows", "cells", "columns",
                     "data_series", "data_points", "labels", "options"):
            v = getattr(c, attr, None)
            if isinstance(v, list):
                stack.extend(x for x in v if hasattr(x, "_get_control_name"))
            elif v is not None and hasattr(v, "_get_control_name"):
                stack.append(v)
    return seen


def _text(control) -> str:
    out = []
    for c in _walk(control):
        # `label` matters: a TextField's prompt ("Transfer ID") is the only
        # text an operator sees for that control.
        for attr in ("value", "text", "label", "tooltip"):
            v = getattr(c, attr, None)
            if isinstance(v, str):
                out.append(v)
    return " ".join(out).lower()
