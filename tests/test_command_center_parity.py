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
from oasis.desktop.views.command_view import build_command_view
from oasis.logic import license_manager as LM

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


# ── Executive ROI ────────────────────────────────────────────────────────
def test_roi_uses_the_consoles_dead_stock_rule(console_src, store):
    """AMIT: ADS < 0.2 with more than 15 on hand is dead capital."""
    assert "_ads < 0.2 and _soh > 15" in console_src, \
        "console changed its dead-stock rule"
    assert "ads > 0 and soh < 1" in \
        __import__("inspect").getsource(D.executive_roi).replace("_", "")
    roi = D.executive_roi(D.default_org(store), store)
    assert roi["error"] is None
    assert 0 <= roi["dead_pct"] <= 100 and 0 <= roi["so_pct"] <= 100
    assert roi["avail"] == pytest.approx(100.0 - roi["so_pct"])


def test_weekly_revenue_buckets_real_iso_weeks(store):
    wk = D.weekly_revenue(D.default_org(store), root=store)
    assert wk["error"] is None and wk["weeks"]
    keys = [w["week"] for w in wk["weeks"]]
    assert keys == sorted(keys)
    assert all(re.fullmatch(r"\d{4}-W\d{2}", k) for k in keys)
    assert wk["latest"] is wk["weeks"][-1]
    assert wk["avg"] == pytest.approx(
        sum(w["revenue"] for w in wk["weeks"]) / len(wk["weeks"]), rel=1e-6)


def test_roi_tab_renders_the_console_panels(store):
    from oasis.desktop.views.command_tabs.executive_roi_tab import (
        build_executive_roi_tab)
    body = _text(build_executive_roi_tab(None, store))
    assert "executive roi overview" in body
    assert "dead stock" in body and "recoverable capital" in body
    assert "weekly revenue trend" in body
    # The console's showcase override substitutes a configured savings headline
    # for a measured one. Not ported.
    assert "showcase" not in body


# ── Analytics ────────────────────────────────────────────────────────────
def test_analytics_and_roi_agree_on_the_weeks(store):
    """Both read data.weekly_revenue, so they cannot disagree."""
    org = D.default_org(store)
    a = D.weekly_revenue(org, root=store)
    b = D.weekly_revenue(org, root=store)
    assert [w["week"] for w in a["weeks"]] == [w["week"] for w in b["weeks"]]


def test_analytics_tab_shows_trend_and_departments(store):
    from oasis.desktop.views.command_tabs.analytics_tab import build_analytics_tab
    body = _text(build_analytics_tab(None, store))
    assert "weekly revenue" in body
    assert "wow change" in body
    assert "department breakdown" in body


# ── Supplier Intelligence ────────────────────────────────────────────────
def test_supplier_concentration_does_not_need_the_unshipped_scorecard(store,
                                                                      monkeypatch):
    """The console reads a scorecard CSV that is not in the release whitelist.

    On a client install that tab could only raise FileNotFoundError, so the
    native one answers from the catalogue every install actually has.
    """
    from oasis.logic.release_packager import should_ship_clean
    assert not should_ship_clean("Full_Product_Allocation_Scorecard_v7.csv")[0]

    import oasis.analytics.supplier_analytics as SA
    monkeypatch.setattr(SA, "load_scorecard_data",
                        lambda: (_ for _ in ()).throw(FileNotFoundError()))
    conc = D.supplier_concentration(D.default_org(store), root=store)
    assert conc["error"] is None
    assert conc["suppliers"], "no suppliers from the carried catalogue"


def test_hhi_bands_follow_the_console(console_src, store):
    assert "hhi > 2500" in console_src and "hhi > 1500" in console_src
    assert D.HHI_HIGH == 2500.0 and D.HHI_MODERATE == 1500.0
    conc = D.supplier_concentration(D.default_org(store), root=store)
    shares = [s["share_pct"] for s in conc["suppliers"]]
    assert conc["hhi"] == pytest.approx(sum(s * s for s in shares), rel=1e-3)
    assert shares == sorted(shares, reverse=True)
    # Each share is rounded to 1dp for display, so the sum drifts with the
    # number of suppliers — 318 of them on the hot catalogue.
    assert sum(shares) == pytest.approx(100.0, abs=max(1.0, len(shares) * 0.05))


def test_supplier_failure_impact_scales_with_share(store):
    org = D.default_org(store)
    sups = D.supplier_concentration(org, root=store)["suppliers"]
    imp = D.supplier_failure_impact(org, sups[0]["supplier"], root=store)
    assert imp["error"] is None
    assert imp["severity"] in ("CRITICAL", "HIGH", "MEDIUM", "LOW")
    assert imp["affected_skus"] == sups[0]["skus"]
    assert D.supplier_failure_impact(org, "No Such Vendor", root=store)["error"]


def test_supplier_tab_renders_hhi_and_the_simulator(store):
    from oasis.desktop.views.command_tabs.supplier_intel_tab import (
        build_supplier_intel_tab)
    body = _text(build_supplier_intel_tab(None, store))
    assert "hhi score" in body
    assert "top suppliers by share" in body
    assert "supplier failure impact simulator" in body


# ── Smart Ordering scenario levers ───────────────────────────────────────
def test_the_scenario_engine_ships(console_src):
    """All three levers come from oasis/simulation, which is on the whitelist."""
    from oasis.logic.release_packager import should_ship_clean
    assert "black_swan_events import SupplierRiskAnalyzer" in console_src
    assert should_ship_clean("oasis/simulation/black_swan_events.py")[0]


def test_supplier_disruption_only_touches_the_target(store):
    """Every other line must come through untouched, or the delta is noise."""
    rows = [{"supplier_name": "BIDCO AFRICA", "lead_time_days": 3, "demand_cv": 0.5},
            {"supplier_name": "OTHER CO", "lead_time_days": 3, "demand_cv": 0.5}]
    out, affected = D._apply_disruption(rows, "Bidco Africa",
                                        D.FAILURE_MODES[0], 14)
    assert affected == 1
    assert out[0]["lead_time_days"] == 17 and out[0]["demand_cv"] == 1.0
    assert out[1] == rows[1], "an unrelated supplier was modified"


@pytest.mark.parametrize("mode,lead,cv", [
    (D.FAILURE_MODES[0], 3 + 14, 1.0),      # Complete
    (D.FAILURE_MODES[1], 3 + 7, 0.75),      # Partial
    (D.FAILURE_MODES[2], 3 * 2, 0.5),       # Delayed: lead time only
])
def test_failure_modes_match_the_console_maths(mode, lead, cv):
    out, _ = D._apply_disruption(
        [{"supplier_name": "X", "lead_time_days": 3, "demand_cv": 0.5}],
        "X", mode, 14)
    assert out[0]["lead_time_days"] == lead
    assert out[0]["demand_cv"] == pytest.approx(cv)


def test_a_supplier_failure_raises_the_order(store):
    """Longer lead time and higher variance means more safety stock, not less."""
    org = D.default_org(store)
    crit = D.critical_suppliers(org, store)["suppliers"]
    if not crit:
        pytest.skip("no concentrated supplier in this store")
    r = D.simulate_ordering_scenario(org, "supplier",
                                     supplier=crit[0]["supplier"],
                                     mode=D.FAILURE_MODES[0], root=store)
    assert r["error"] is None
    assert r["affected"] > 0
    assert r["adjusted_qty"] >= r["baseline_qty"]


def test_a_competitor_arriving_lowers_the_order(store):
    """Negative YoY impact scales demand down, so the PO should shrink."""
    r = D.simulate_ordering_scenario(D.default_org(store), "competitor",
                                     template="carrefour_100m", root=store)
    assert r["error"] is None
    assert 0 < r["multiplier"] < 1, "expected a demand-suppressing multiplier"
    assert r["adjusted_qty"] <= r["baseline_qty"]
    assert "carrefour" in r["label"].lower()


def test_a_price_war_lowers_the_order(store):
    r = D.simulate_ordering_scenario(D.default_org(store), "price_war",
                                     root=store)
    assert r["error"] is None and r["multiplier"] < 1
    assert r["adjusted_qty"] <= r["baseline_qty"]


def test_an_unknown_scenario_is_refused(store):
    assert D.simulate_ordering_scenario(D.default_org(store), "nonsense",
                                        root=store)["error"]


def test_a_scenario_writes_nothing(store):
    """A scenario is a question. Nothing may reach the approvals queue."""
    org = D.default_org(store)
    before = D.pending_orders(org, store)["count"]
    D.simulate_ordering_scenario(org, "price_war", root=store)
    assert D.pending_orders(org, store)["count"] == before


def test_ordering_tab_offers_all_three_levers(store):
    from oasis.desktop.views.command_tabs.smart_ordering_tab import (
        build_smart_ordering_tab)
    body = _text(build_smart_ordering_tab(None, store))
    assert "scenario levers" in body
    assert "supplier disruption" in body
    assert "competitor entry" in body
    assert "price war" in body


# ── OASIS Processor ──────────────────────────────────────────────────────
def test_the_decision_engine_ships_now(console_src):
    """ops_dashboard imports RuleBasedLLM at MODULE level.

    oasis/llm was not on the release whitelist, so the Streamlit Command Center
    — the reference architecture itself — died with ModuleNotFoundError before
    rendering a single tab on every client install.
    """
    from oasis.logic.release_packager import should_ship_clean
    assert "from oasis.llm.inference import RuleBasedLLM" in console_src
    assert should_ship_clean("oasis/llm/inference.py")[0], \
        "the console's decision engine is missing from client releases"


def test_processor_reports_each_file_separately(store, tmp_path):
    """One unreadable sheet must not sink the batch."""
    bad = tmp_path / "not_a_spreadsheet.csv"
    bad.write_text("this is not, a valid[ inventory file\n", encoding="utf-8")
    res = D.process_inventory_files([str(bad), str(tmp_path / "missing.xlsx")],
                                    "tester", D.default_org(store), root=store)
    assert res["error"] is None, "the batch itself failed"
    assert len(res["results"]) == 2
    assert all("file" in r and "error" in r for r in res["results"])
    assert res["processed"] + res["failed"] == 2


def test_processing_leaves_the_event_loop_as_it_found_it(store, tmp_path):
    """asyncio.run() closes its loop and leaves the thread with none.

    The desktop app is one long-lived process, and anything later that reaches
    for get_event_loop().run_until_complete(...) then dies with "There is no
    current event loop" — which is exactly what a batch run did to the whole
    ordering-logic suite.
    """
    import asyncio
    bad = tmp_path / "x.csv"
    bad.write_text("nope\n", encoding="utf-8")
    D.process_inventory_files([str(bad)], "tester", D.default_org(store),
                              root=store)
    loop = asyncio.get_event_loop_policy().get_event_loop()
    assert not loop.is_closed(), "a batch run closed the caller's event loop"
    assert loop.run_until_complete(asyncio.sleep(0)) is None


def test_processor_tab_builds_without_a_page(store):
    """No page means no file picker overlay — the tab must still construct."""
    from oasis.desktop.views.command_tabs.processor_tab import build_processor_tab
    body = _text(build_processor_tab(None, store))
    assert "batch inventory processor" in body
    assert "choose files" in body
    assert "no files selected" in body


# ── Simulation Lab ───────────────────────────────────────────────────────
def test_the_simulator_ships_now():
    """It lived at the repo root, so default-deny kept it out of every release.

    Two shipped scripts import it by that name: ops_dashboard (Simulation Lab)
    and intraday_sim, the latter at MODULE level — so that script could not
    start at all on a client install.
    """
    from oasis.logic.release_packager import should_ship_clean
    assert should_ship_clean("oasis/simulation/retail_simulator.py")[0]
    assert should_ship_clean("retail_simulator.py")[0], "the shim must ship too"


def test_the_shim_and_the_package_are_the_same_module():
    import retail_simulator as shim
    from oasis.simulation import retail_simulator as pkg
    assert shim.STORE_UNIVERSES is pkg.STORE_UNIVERSES
    assert shim.RetailSimulator is pkg.RetailSimulator
    assert shim.SKUState is pkg.SKUState


def test_the_simulator_has_no_hardcoded_developer_paths():
    """DATA_DIR and SCORECARD_FILE were absolute paths on one machine."""
    from oasis.simulation import retail_simulator as R
    src = pathlib.Path(R.__file__).read_text(encoding="utf-8", errors="replace")
    assert "c:\\Users" not in src and "C:\\Users" not in src, \
        "a developer's absolute path is still baked into the simulator"
    assert R.DATA_DIR.lower().endswith(os.path.join("oasis", "data").lower())


def test_the_simulator_does_not_require_the_client_scorecard(store, monkeypatch):
    """The scorecard is one retailer's P&L and ships nowhere.

    A simulation must run from the store's own products regardless.
    """
    from oasis.simulation import retail_simulator as R
    monkeypatch.setattr(R, "SCORECARD_FILE", "/definitely/not/here.csv")
    r = D.run_simulation_comparison(D.default_org(store), days=7, root=store)
    assert r["error"] is None, r["error"]
    assert r["skus"] > 0


def test_both_runs_differ_because_risk_actually_reaches_the_bridge(store):
    """The console once computed the risk and never passed it, so the two runs
    were identical and the whole comparison was theatre."""
    r = D.run_simulation_comparison(D.default_org(store), days=7, root=store)
    assert r["error"] is None
    assert r["heuristic"] and r["adjusted"]
    assert set(r["heuristic"]) == {"fill_rate", "stockout_rate", "revenue",
                                   "turnover", "capital_efficiency"}
    if r["risk"] > 0:
        assert r["heuristic"] != r["adjusted"], \
            "risk-adjusted run is identical to the heuristic one"


def test_simulation_tiers_are_config_not_client_data():
    tiers = D.simulation_tiers()
    assert tiers["error"] is None and tiers["tiers"]
    for t in tiers["tiers"]:
        assert {"key", "budget", "max_skus", "safety_days"} <= set(t)


def test_simulation_tab_runs_and_reports(store):
    from oasis.desktop.views.command_tabs.simulation_lab_tab import (
        build_simulation_lab_tab)
    tab = build_simulation_lab_tab(None, store)
    btn = [c for c in _walk(tab)
           if getattr(c, "text", "") == "Run comparison simulation"]
    assert btn, "no way to run the simulation"
    btn[0].on_click(None)
    body = _text(tab)
    assert "side-by-side comparison" in body
    assert "fill rate" in body and "stockout rate" in body


# ── the tab set ──────────────────────────────────────────────────────────
def test_every_command_tab_names_a_real_module_sku():
    from oasis.desktop.views.command_view import TAB_MODULES
    from oasis.logic import license_manager as LM
    assert set(TAB_MODULES.values()) <= set(LM.KNOWN_MODULES)


def test_supplier_tab_is_paywalled_like_the_console(console_src):
    """ops_dashboard puts supplier_intelligence behind the ordering module."""
    from oasis.desktop.views.command_view import TAB_MODULES
    m = re.search(r'"supplier_intelligence":\s*"(\w+)"', console_src)
    assert m, "console no longer gates supplier_intelligence"
    assert TAB_MODULES["supplier_intelligence"] == m.group(1)


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


# ── the sample dataset must not identify the reference customer ──────────
def test_the_sample_network_names_no_real_retailer():
    """The demo estate was the reference customer's actual branch list.

    Chain name plus a real branch list is what made it identifiable — and the
    release ships source, so a client could read who it was and which of their
    sites was the flagship. The store SHAPES are the product; the identity is
    not.
    """
    from oasis.logic.demo_identity import IDENTIFYING_TOKENS
    from oasis.logic.multi_store_profiles import STORE_PROFILES
    blob = " ".join(f"{p.name} {p.short_name} {p.address}"
                    for p in STORE_PROFILES).lower()
    for token in IDENTIFYING_TOKENS:
        assert token not in blob, f"sample estate still names '{token}'"
    assert len({p.name for p in STORE_PROFILES}) == len(STORE_PROFILES)


def test_a_built_sample_store_names_no_real_retailer(store):
    import sqlite3
    from oasis.logic.demo_identity import IDENTIFYING_TOKENS
    rows = sqlite3.connect(D.store_db_path(store)).execute(
        "SELECT ORG_NAME, ORG_SHORT_NAME, ORG_ADDRESS FROM ORGANIZATION_MST"
    ).fetchall()
    assert rows
    users = sqlite3.connect(D.store_db_path(store)).execute(
        "SELECT DISPLAY_NAME, EMAIL FROM OASIS_USERS").fetchall()
    blob = " ".join(str(c) for r in list(rows) + list(users) for c in r).lower()
    for token in IDENTIFYING_TOKENS:
        assert token not in blob, f"the built sample store still names '{token}'"


def test_the_sample_network_uses_the_fictional_chain(tmp_path, monkeypatch):
    """apply_multi_demo names its outlets from demo_identity, not a real chain."""
    import sqlite3
    from oasis.logic.demo_identity import DEMO_CHAIN, IDENTIFYING_TOKENS
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.setenv("OASIS_DB_PATH", str(tmp_path / "oasis" / "data" / "n.db"))
    D.reset_adapter()
    from oasis.logic import onboarding as OB
    OB.apply_multi_demo(root=str(tmp_path))
    D.reset_adapter()
    rows = sqlite3.connect(D.store_db_path(str(tmp_path))).execute(
        "SELECT ORG_NAME, ORG_SHORT_NAME, ORG_ADDRESS FROM ORGANIZATION_MST").fetchall()
    blob = " ".join(str(c) for r in rows for c in r).lower()
    assert DEMO_CHAIN.lower() in blob
    for token in IDENTIFYING_TOKENS:
        assert token not in blob, f"the sample network still names '{token}'"
    D.reset_adapter()


def test_the_ui_that_names_the_sample_network_is_clean():
    """Tooltips and captions listed the real branches by name."""
    import pathlib
    from oasis.logic.demo_identity import IDENTIFYING_TOKENS
    repo = pathlib.Path(__file__).parent.parent
    # Scoped to the sample dataset and the UI that names it. Internal column
    # keys elsewhere ("rhapta_fill_rate") mirror a real client's spreadsheet
    # format and are read from files they supply — renaming those would break
    # reading their data to hide a string no client sees.
    for rel in ("oasis/desktop/views/home_view.py", "oasis/ui/onboarding.py",
                "oasis/logic/multi_store_profiles.py",
                "oasis/logic/mock_pos_build.py",
                "oasis/logic/demo_seed.py"):
        src = (repo / rel).read_text(encoding="utf-8", errors="replace").lower()
        for token in IDENTIFYING_TOKENS:
            assert token not in src, f"{rel} still mentions '{token}'"


# ── Allocation Engine / greenfield scorecard ─────────────────────────────
def test_the_scorecard_builder_needs_no_shipped_client_file():
    """The whole reason this module exists.

    The console reads a 23,000-row CSV of one retailer's per-SKU revenue,
    margins and named supplier terms. It cannot ship, so the console's
    Allocation tab could only ever raise FileNotFoundError on a client install.
    """
    from oasis.logic.release_packager import should_ship_clean
    assert should_ship_clean("oasis/logic/scorecard_builder.py")[0]
    assert not should_ship_clean("Full_Product_Allocation_Scorecard_v7.csv")[0]


def test_network_demand_is_averaged_over_carriers_not_summed():
    """A new store behaves like an average store, not the whole chain at once.

    Averaging over ALL outlets (rather than the ones that carry a line) would
    understate a legitimately regional product into nonexistence.
    """
    from oasis.logic.scorecard_builder import build_recommendations
    stock = {
        "A": [{"item_code": "X", "product_name": "X", "avg_daily_sales": 10,
               "selling_price": 100, "cost_price": 80, "department": "D",
               "supplier_name": "S"}],
        "B": [{"item_code": "X", "product_name": "X", "avg_daily_sales": 20,
               "selling_price": 100, "cost_price": 80, "department": "D",
               "supplier_name": "S"}],
        "C": [{"item_code": "Y", "product_name": "Y", "avg_daily_sales": 5,
               "selling_price": 50, "cost_price": 40, "department": "D",
               "supplier_name": "S"}],
    }
    recs = {r["item_code"]: r for r in
            build_recommendations(stock, mode="network")["recs"]}
    assert recs["X"]["avg_daily_sales"] == pytest.approx(15.0)   # not 30, not 10
    assert recs["X"]["carried_by"] == 2
    assert recs["Y"]["avg_daily_sales"] == pytest.approx(5.0)


def test_staple_is_revealed_by_carriage_not_asserted():
    """The old scorecard shipped an Is_Staple column. Carriage is observable."""
    from oasis.logic.scorecard_builder import (build_recommendations,
                                               STAPLE_CARRIAGE_RATIO)
    def _row(code, ads=5):
        return {"item_code": code, "product_name": code, "avg_daily_sales": ads,
                "selling_price": 10, "cost_price": 8, "department": "D",
                "supplier_name": "S"}
    stock = {f"ORG{i}": [_row("EVERYWHERE")] + ([_row("RARE")] if i == 1 else [])
             for i in range(1, 6)}
    recs = {r["item_code"]: r for r in
            build_recommendations(stock, mode="network")["recs"]}
    assert recs["EVERYWHERE"]["is_staple_override"] is True
    assert recs["RARE"]["is_staple_override"] is False
    assert recs["EVERYWHERE"]["carriage_ratio"] >= STAPLE_CARRIAGE_RATIO


def test_store_mode_claims_no_staples():
    """One outlet cannot reveal a chain-wide range decision."""
    from oasis.logic.scorecard_builder import build_recommendations
    stock = {"A": [{"item_code": "X", "product_name": "X",
                    "avg_daily_sales": 9, "selling_price": 10,
                    "cost_price": 8, "department": "D", "supplier_name": "S"}]}
    recs = build_recommendations(stock, mode="store")["recs"]
    assert recs and all(not r["is_staple_override"] for r in recs)


def test_a_new_site_carries_no_order_history():
    """Leaving a live store's count here makes the engine treat an unopened
    shop as an established buyer."""
    from oasis.logic.scorecard_builder import build_recommendations
    stock = {"A": [{"item_code": "X", "product_name": "X",
                    "avg_daily_sales": 9, "selling_price": 10, "cost_price": 8,
                    "department": "D", "supplier_name": "S",
                    "historical_order_count": 42}]}
    assert all(r["historical_order_count"] == 0
               for r in build_recommendations(stock)["recs"])


def test_the_builder_output_matches_the_engine_contract(store):
    """Field-for-field what load_scorecard_recommendations produces."""
    card = D.greenfield_scorecard(root=store)
    assert card["error"] is None and card["recs"]
    required = {"product_name", "selling_price", "avg_daily_sales",
                "product_category", "pack_size", "moq_floor",
                "historical_order_count", "is_staple_override", "margin_pct",
                "supplier_name", "recommended_quantity", "reasoning"}
    assert required <= set(card["recs"][0])


def test_greenfield_allocation_runs_without_any_csv(store):
    res = D.run_greenfield(1_000_000, root=store)
    assert res["error"] is None, res["error"]
    assert res["rows"], "empty opening basket"
    assert 0 < res["cash_spend"] <= res["budget"]
    # The basket DataFrame's own column names — reading the wrong ones renders
    # a table of blanks, which is how the transfers table shipped empty once.
    assert {"Product", "Qty", "Department"} <= set(res["rows"][0])


def test_the_basket_is_a_dataframe_and_must_be_converted(store):
    """`df or fallback` raises 'truth value of a DataFrame is ambiguous'."""
    res = D.run_greenfield(500_000, root=store)
    assert isinstance(res["rows"], list)
    assert res["error"] is None


def test_allocation_tab_renders_the_basket(store):
    from oasis.desktop.views.command_tabs.allocation_tab import (
        build_allocation_tab)
    tab = build_allocation_tab(None, store)
    btn = [c for c in _walk(tab) if getattr(c, "text", "") == "Run allocation"]
    assert btn, "no way to run the allocation"
    btn[0].on_click(None)
    body = _text(tab)
    assert "opening basket" in body
    assert "committed" in body and "lines stocked" in body
    assert "allocation failed" not in body


# ── the hot-node sample catalogue ────────────────────────────────────────
def test_the_sample_catalogue_carries_no_financials():
    """Hot = selling. Selecting the selling lines must not drag the book along.

    The scorecard those lines came from holds revenue, margin, gross profit and
    GMROI per SKU. What ships is identity, department, supplier and shelf price
    — all publicly observable — plus synthesised opening stock.
    """
    import gzip
    import json
    import pathlib
    from oasis.logic.demo_seed import CATALOG_FILE
    path = (pathlib.Path(__file__).parent.parent / "oasis" / "data" / CATALOG_FILE)
    if not path.exists():
        pytest.skip("generated catalogue not built in this checkout")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        payload = json.load(f)
    banned = {"revenue", "margin", "margin_pct", "gross_profit", "gmroi",
              "avg_daily_sales", "total_revenue", "sales_rank"}
    for row in payload["rows"][:200]:
        assert not (banned & {k.lower() for k in row}), row
    assert payload["rows"], "catalogue is empty"


def test_no_catalogue_line_names_the_retailer():
    """The field-level check was not enough.

    The earlier guards asserted the catalogue carried no FINANCIAL fields and
    that the store estate was renamed — and both passed while two own-brand
    lines ("... FOODPLUS CARRIER BAG", "... PRINTED") sat in the product names,
    which is the text a client actually reads. Check the words, not the schema.
    """
    import gzip
    import json
    import pathlib as _p
    from oasis.logic.demo_seed import CATALOG_FILE
    from oasis.logic.demo_identity import IDENTIFYING_TOKENS
    path = (_p.Path(__file__).parent.parent / "oasis" / "data" / CATALOG_FILE)
    if not path.exists():
        pytest.skip("generated catalogue not built in this checkout")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        payload = json.load(f)
    offenders = [r for r in payload["rows"]
                 if any(tok in f"{r['name']} {r['vendor']} {r['dept']}".lower()
                        for tok in IDENTIFYING_TOKENS)]
    assert not offenders, [r["name"] for r in offenders[:5]]


def test_a_built_store_names_the_retailer_nowhere(store):
    """End of the chain: the DATABASE a client browses, not the source."""
    import sqlite3
    from oasis.logic.demo_identity import IDENTIFYING_TOKENS
    conn = sqlite3.connect(D.store_db_path(store))
    blob = " ".join(
        str(cell)
        for table in ("ORGANIZATION_MST", "OASIS_USERS", "ITEM_MST")
        for row in conn.execute(f"SELECT * FROM {table}")
        for cell in row).lower()
    assert not [t for t in IDENTIFYING_TOKENS if t in blob]


def test_the_sample_catalogue_excludes_dead_lines():
    """D-tier is ~16,600 near-zero-velocity lines. A sample store carries the
    range that moves, not the archive."""
    import gzip
    import json
    import pathlib
    from oasis.logic.demo_seed import CATALOG_FILE
    path = (pathlib.Path(__file__).parent.parent / "oasis" / "data" / CATALOG_FILE)
    if not path.exists():
        pytest.skip("generated catalogue not built in this checkout")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        payload = json.load(f)
    assert "D" not in set(payload.get("tiers", []))
    assert all(r.get("tier") != "D" for r in payload["rows"])


def test_seeding_scales_with_the_catalogue():
    """60 bills/day was tuned for 34 lines; against 4,000 it left 87% unsold."""
    from oasis.logic.onboarding import demo_seeding_params
    small = demo_seeding_params(34)["bills_per_day"]
    large = demo_seeding_params(4000)["bills_per_day"]
    assert large > small * 5, "seeding does not scale with catalogue size"


def test_a_slow_mover_cannot_raise_a_velocity_alert(store):
    """One sale of a 0.02-a-day line is a 50x ratio and no information.

    On a realistic long tail that noise buried the signal — 184 of 266 alerts
    on the sample store were lines selling under a unit a day.
    """
    alerts = D.velocity_alerts([
        {"code": "SLOW", "name": "Sells once a month", "units": 1.0,
         "ads": 0.02, "velocity_ratio": 50.0},
        {"code": "TINY", "name": "Two units, low base", "units": 2.0,
         "ads": 0.5, "velocity_ratio": 4.0},
    ])
    assert alerts == []


def test_a_real_mover_still_raises_one(store):
    alerts = D.velocity_alerts([
        {"code": "FAST", "name": "Genuine spike", "units": 40.0,
         "ads": 5.0, "velocity_ratio": 8.0},
    ])
    assert len(alerts) == 1
    assert alerts[0]["product_name"] == "Genuine spike"


def test_every_alert_clears_both_floors(store):
    s = D.live_sales(D.default_org(store), root=store)
    for a in s["alerts"]:
        assert a["ads"] >= D.VELOCITY_MIN_ADS
        assert a["units"] >= D.VELOCITY_MIN_UNITS


# ── role-based tab visibility ────────────────────────────────────────────
class _RolePage:
    """Enough ft.Page for the view to read a role, headless."""
    class _S:
        def __init__(self, role): self.role = role
        def get(self, key): return self.role if key == "role" else None
        def contains_key(self, key): return key == "role"
    def __init__(self, role): self.session = self._S(role)
    def update(self): pass


def _tab_labels(view):
    return sorted(c.text for c in _walk(view) if type(c).__name__ == "Tab")


def test_every_native_tab_maps_to_a_real_permission_key():
    """A typo would silently hide a tab from every role."""
    from oasis.desktop.views.command_view import TAB_MODULES, TAB_ROLE_KEYS
    from oasis.logic.auth_manager import ROLE_PERMISSIONS
    known = set(ROLE_PERMISSIONS["ops_admin"]["tabs"])
    assert set(TAB_ROLE_KEYS.values()) <= known
    assert set(TAB_ROLE_KEYS) == set(TAB_MODULES), \
        "a tab is gated by module but not by role, or the reverse"


def test_executive_roi_and_suppliers_are_now_in_the_permission_table():
    """They were absent, so user_perms['tabs'].get() returned None for every
    role — which made the CONSOLE's Executive ROI tab unreachable outside
    showcase mode. Fixing it in the shared table fixes both surfaces."""
    from oasis.logic.auth_manager import ROLE_PERMISSIONS
    for role, perms in ROLE_PERMISSIONS.items():
        assert "executive_roi" in perms["tabs"], role
        assert "supplier_intelligence" in perms["tabs"], role


def test_a_branch_manager_sees_fewer_tabs_than_an_admin(store, monkeypatch):
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    admin = _tab_labels(build_command_view(_RolePage("ops_admin"), store))
    branch = _tab_labels(build_command_view(_RolePage("branch_manager"), store))
    assert len(admin) > len(branch)
    assert set(branch) < set(admin)
    # The console denies a branch manager transfers, simulation and analytics.
    for denied in ("Transfers", "Simulation", "Analytics", "Suppliers"):
        assert denied not in branch


def test_role_visibility_matches_the_permission_table(store, monkeypatch):
    from oasis.desktop.views.command_view import TAB_ROLE_KEYS, TAB_SPEC
    from oasis.logic.auth_manager import ROLE_PERMISSIONS
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    labels = {key: label for key, label, _i, _b in TAB_SPEC}
    for role, perms in ROLE_PERMISSIONS.items():
        shown = set(_tab_labels(build_command_view(_RolePage(role), store)))
        for key, perm_key in TAB_ROLE_KEYS.items():
            expected = bool(perms["tabs"].get(perm_key, False))
            assert (labels[key] in shown) is expected, (role, key)


def test_an_unknown_role_gets_the_least_privilege(store, monkeypatch):
    """get_user_permissions falls back to branch_manager. Fail closed."""
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    unknown = _tab_labels(build_command_view(_RolePage("nonsense_role"), store))
    branch = _tab_labels(build_command_view(_RolePage("branch_manager"), store))
    assert unknown == branch


def test_role_and_module_are_independent_gates(store, monkeypatch):
    """Both must pass. A role cannot buy a module; a licence cannot grant a role."""
    monkeypatch.setattr(D, "allowed_modules", lambda: {"core"})
    body = _text(build_command_view(_RolePage("ops_admin"), store))
    # Licensed-out tabs still appear, as upsells — they can be bought.
    assert "smart ordering module" in body
    # A role-denied tab is not built at all.
    branch = _tab_labels(build_command_view(_RolePage("branch_manager"), store))
    assert "Suppliers" not in branch


def test_a_role_with_no_command_tabs_is_told_why(store, monkeypatch):
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    monkeypatch.setattr(D, "role_tabs", lambda role: {})
    body = _text(build_command_view(_RolePage("stranger"), store))
    assert "no command center access" in body
    assert "administrator" in body


# ── the greenfield SKU ───────────────────────────────────────────────────
def test_greenfield_is_its_own_sellable_module():
    """Allocation rode the network SKU, so a chain could not buy site planning
    without also buying inter-store transfers — different buyer, different
    cadence, different data situation."""
    assert "greenfield" in LM.KNOWN_MODULES
    assert LM.MODULE_LABELS["greenfield"]
    assert LM.PAGE_MODULES["allocation_engine"] == "greenfield"
    assert LM.PAGE_MODULES["transfer_intelligence"] == "network"
    assert "expansion" in LM.BUNDLES
    assert "greenfield" in LM.BUNDLES["expansion"]
    assert "greenfield" in LM.BUNDLES["enterprise"]


def test_the_allocation_tab_is_gated_on_greenfield(store, monkeypatch):
    from oasis.desktop.views.command_view import TAB_MODULES
    assert TAB_MODULES["allocation"] == "greenfield"
    monkeypatch.setattr(D, "allowed_modules", lambda: {"core", "network"})
    body = _text(build_command_view(_RolePage("ops_admin"), store))
    assert "greenfield (site planning" in body, \
        "a network-only licence must not unlock site planning"


def test_a_network_licence_still_unlocks_transfers(store, monkeypatch):
    monkeypatch.setattr(D, "allowed_modules", lambda: {"core", "network"})
    body = _text(build_command_view(_RolePage("ops_admin"), store))
    assert "network (transfers) module" not in body


def test_the_customer_name_survives_only_where_it_must(console_src):
    """What is left is deliberate, and each case has a reason.

    Everything user-facing is gone. The residue is: the guard's own vocabulary,
    a one-release back-compat alias for a field WE write (graph_export emits
    it, three engines read it), the legacy store-file names so an existing
    install is not orphaned, one genuine client spreadsheet column header, and
    the Streamlit console — the untouched reference.
    """
    import pathlib
    import re
    from oasis.logic.demo_identity import IDENTIFYING_TOKENS
    repo = pathlib.Path(__file__).parent.parent
    allowed = {
        "oasis/logic/demo_identity.py",        # the token list itself
        "oasis/logic/amit_gatekeeper.py",      # back-compat read alias
        "oasis/logic/dharam_revenue.py",
        "oasis/logic/graph_export.py",
        "oasis/logic/lata_shield.py",
        "oasis/logic/data_mixin.py",           # a client's own column header
        "oasis/logic/onboarding.py",           # LEGACY_DB_NAMES
        "oasis/logic/release_packager.py",     # legacy exclusion glob
        "ops_dashboard.py",                    # the untouched reference
    }
    pat = re.compile("|".join(IDENTIFYING_TOKENS), re.I)
    offenders = []
    for path in list((repo / "oasis").rglob("*.py")) + [repo / "ops_dashboard.py"]:
        rel = path.relative_to(repo).as_posix()
        if rel in allowed or "__pycache__" in rel:
            continue
        if pat.search(path.read_text(encoding="utf-8", errors="replace")):
            offenders.append(rel)
    assert not offenders, f"new customer-identifying references: {offenders}"


# ── third-party geographic data (ODbL) ───────────────────────────────────
def test_no_openstreetmap_extract_is_redistributed():
    """OSM is ODbL. Shipping an extract means shipping a Derivative Database,
    which obliges us to license THAT database under ODbL and attribute.
    A score computed from it is a Produced Work and carries no such obligation
    — so OASIS ships the scoring and the client fetches their own region."""
    from oasis.logic.release_packager import should_ship_clean
    for f in ("competitor_network.csv", "oasis/data/competitor_network.csv",
              "competitor_fetcher.py"):
        assert not should_ship_clean(f)[0], f


def test_attribution_exists_and_names_osm():
    from oasis.logic.geo_sources import OSM_ATTRIBUTION
    assert "OpenStreetMap" in OSM_ATTRIBUTION
    assert "ODbL" in OSM_ATTRIBUTION


def test_a_missing_competitor_cache_reads_as_not_fetched_yet(tmp_path):
    """Absent data is a client who has not fetched, not an absence of
    competition — the caller must be able to say which."""
    from oasis.logic.geo_sources import load_competitors
    res = load_competitors(root=str(tmp_path))
    assert res["rows"] == []
    assert res["error"] and "fetch" in res["error"].lower()
    assert res["attribution"]


def test_a_cached_extract_loads_with_its_attribution(tmp_path):
    from oasis.logic.geo_sources import load_competitors, cache_path
    p = cache_path(str(tmp_path))
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        f.write("Store_Name,Latitude,Longitude,Chain,Source\n"
                "Rival A,-1.3,36.8,Rival,OSM_Overpass\n")
    res = load_competitors(root=str(tmp_path))
    assert len(res["rows"]) == 1 and res["error"] is None
    assert "OpenStreetMap" in res["attribution"]


# ── the location pillar ──────────────────────────────────────────────────
def test_the_scoring_uses_no_model_and_ships():
    """The expansion RandomForest was trained on np.random against a
    hand-written target. It leaks nothing but predicts nothing, and costs 20x
    the release. The geography is interpretable instead."""
    from oasis.logic.release_packager import should_ship_clean
    import oasis.logic.site_scoring as SS
    assert should_ship_clean("oasis/logic/site_scoring.py")[0]
    assert not should_ship_clean("expansion_model.joblib")[0]
    assert not should_ship_clean("store_coords.json")[0]
    # Check the CODE, not the prose — the module's own docstring explains at
    # length why there is no model, and naming the thing you refuse to use is
    # not using it.
    src = pathlib.Path(SS.__file__).read_text(encoding="utf-8")
    code = chr(10).join(line for line in src.splitlines()
                        if not line.lstrip().startswith("#"))
    code = re.sub(r'""".*?"""', "", code, flags=re.S)
    for banned in ("import joblib", "import sklearn", "from sklearn",
                   ".predict(", "torch"):
        assert banned not in code, f"site scoring pulled in {banned}"


def test_haversine_is_a_real_distance():
    from oasis.logic.site_scoring import haversine_km
    assert haversine_km(-1.2641, 36.7865, -1.2936, 36.7800) == pytest.approx(3.36, abs=0.1)
    assert haversine_km(0, 0, 0, 0) == 0.0


def test_demand_is_sampled_around_the_site_not_on_it():
    """Placing the demand point ON the candidate gives it distance ~0 and so
    near-infinite utility — the site beats every competitor by construction and
    an empty desert scores 100%. The console's expansion engine does exactly
    that (`{'S': ..., 'dist': 0.1}`); this must not."""
    from oasis.logic.site_scoring import score_site
    own = [{"lat": -1.2641, "lon": 36.7865, "size_sqft": 12000}]
    crowded = score_site(-1.2645, 36.7870, own,
                         [{"Latitude": -1.2650, "Longitude": 36.7875,
                           "Chain": "Naivas"}])
    open_ground = score_site(-1.2000, 36.8600, own, [])
    assert crowded["capture_pct"] < open_ground["capture_pct"], \
        "competitor proximity did not reduce capture"
    assert crowded["capture_pct"] < 50.0


def test_an_isolated_site_says_it_cannot_know():
    """100% of an empty catchment is still 100%, and means nothing — OASIS has
    no population data and cannot tell a suburb from a field."""
    from oasis.logic.site_scoring import rank_sites
    out = rank_sites([{"name": "Middle of nowhere", "lat": -3.5, "lon": 39.9}],
                     [{"lat": -1.26, "lon": 36.78}], [])
    site = out[0]
    assert site["isolated"] is True
    assert "no evidence of demand" in site["verdict"].lower()
    assert "unknown" in site["format"].lower(), \
        "an empty field was recommended a store format"


def test_cannibalisation_is_reported_separately_from_capture():
    """A site can score well and still be a bad decision if the trade merely
    moves from your own store across the road."""
    from oasis.logic.site_scoring import score_site
    own = [{"lat": -1.2641, "lon": 36.7865, "size_sqft": 12000}]
    next_door = score_site(-1.2645, 36.7870, own, [])
    far = score_site(-1.1500, 36.9000, own, [])
    assert next_door["cannibalisation_pct"] > far["cannibalisation_pct"]
    assert "your own store" in next_door["verdict"].lower()


def test_sites_cannot_be_scored_before_the_estate_is_placed(store):
    """Every distance is measured from the client's stores; without them there
    is nothing to measure against."""
    res = D.score_sites([{"name": "X", "lat": -1.23, "lon": 36.82}], root=store)
    assert res["sites"] == []
    assert res["error"] and "place your existing stores" in res["error"].lower()


def test_placing_a_store_validates_its_coordinates(store):
    assert not D.set_store_location("ORG001", 999, 36.8, root=store)["saved"]
    assert not D.set_store_location("ORG001", "abc", 36.8, root=store)["saved"]
    assert D.set_store_location("ORG001", -1.26, 36.78, root=store)["saved"]


def test_a_store_without_coordinates_is_listed_not_dropped(store):
    """Silently omitting it would leave it out of every distance calculation
    while the map looked complete."""
    D.set_store_location(D.default_org(store), -1.26, 36.78, root=store)
    m = D.store_map(store)
    assert m["located"]
    assert all("org_cd" in s for s in m["missing"])


def test_the_location_tab_builds_and_is_greenfield_gated(store, monkeypatch):
    from oasis.desktop.views.command_view import TAB_MODULES
    assert TAB_MODULES["location"] == "greenfield"
    monkeypatch.setattr(D, "allowed_modules", lambda: set(LM.KNOWN_MODULES))
    from oasis.desktop.views.command_tabs.location_tab import build_location_tab
    body = _text(build_location_tab(None, store))
    assert "your estate" in body
    assert "score a candidate site" in body
    assert "place store" in body


# ── the alert floors are one definition, not two ─────────────────────────
def test_both_front_doors_share_the_velocity_floors(console_src):
    """The fix landed natively first; the console kept emitting the noise.

    Defining the floors in the shared engine means the two surfaces cannot
    disagree about what an alert is — which is the same discipline the parity
    tests exist to enforce everywhere else.
    """
    from oasis.logic import alert_monitor as AM
    assert AM.VELOCITY_MIN_ADS == 1.0 and AM.VELOCITY_MIN_UNITS == 3.0
    # the desktop re-exports the very same objects
    assert D.VELOCITY_MIN_ADS is AM.VELOCITY_MIN_ADS
    assert D.VELOCITY_MIN_UNITS is AM.VELOCITY_MIN_UNITS
    # …and the console imports them rather than hardcoding its own
    assert "VELOCITY_MIN_ADS" in console_src, "console does not use the floors"
    assert "VELOCITY_MIN_UNITS" in console_src
    assert "from oasis.logic.alert_monitor import" in console_src


def test_the_console_filters_slow_movers_out_of_its_spike_set(console_src):
    """The mask must sit on the candidate selection, not after the fact."""
    import re as _re
    m = _re.search(r"spike_items = all_items\[(.*?)\]\.sort_values", console_src,
                   _re.S)
    assert m, "console no longer selects spike_items the way this test expects"
    mask = m.group(1)
    assert "VELOCITY_MIN_ADS" in mask and "VELOCITY_MIN_UNITS" in mask


def test_is_alertable_agrees_with_the_mask():
    from oasis.logic.alert_monitor import is_alertable
    assert not is_alertable(0.02, 1)      # sells once a month
    assert not is_alertable(5.0, 2)       # real mover, trivial volume
    assert not is_alertable(0.9, 40)      # high volume, no baseline
    assert is_alertable(1.0, 3)           # exactly on both floors
    assert is_alertable(5.0, 40)


# ── licence + machine-state hygiene ──────────────────────────────────────
def test_third_party_notices_records_the_odbl_position():
    import pathlib
    p = pathlib.Path(__file__).parent.parent / "THIRD_PARTY_NOTICES.md"
    assert p.exists(), "no third-party notices file"
    text = p.read_text(encoding="utf-8")
    for required in ("ODbL", "OpenStreetMap", "Derivative Database",
                     "Produced Work", "Overpass"):
        assert required in text, f"notices omit {required}"


def test_client_and_machine_state_files_are_gitignored():
    """network_registry is written at run time; the other two are the client's
    own data. None are source, and a stray `git add -A` must not take them."""
    import pathlib
    ignored = (pathlib.Path(__file__).parent.parent / ".gitignore"
               ).read_text(encoding="utf-8")
    for f in ("oasis/data/network_registry.json",
              "oasis/data/store_locations.json",
              "oasis/data/competitor_network.csv"):
        assert f in ignored, f"{f} is not gitignored"


def test_the_attribution_notice_ships_with_the_product():
    """An OSM notice that stays in the source repo does not discharge ODbL 4.3
    for a client who only ever sees the zip."""
    from oasis.logic.release_packager import should_ship_clean
    assert should_ship_clean("THIRD_PARTY_NOTICES.md")[0]
