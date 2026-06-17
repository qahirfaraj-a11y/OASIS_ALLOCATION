"""
OASIS Intelligence Console (the monitoring counterpart to the Operations shell).

Same foundation as app.py (theme, auth, components, telemetry, role model) via
shell.run_console — different page registry, focused on *understanding* the
network rather than *acting* on it: Pulse, Velocity & Stockout Alerts, Stock
Review (native), with Live Sales / Network Intelligence / Executive ROI /
Simulation Lab bridging to the legacy command center + ST-GAT dashboard until
migrated.

Pure helpers (classify_cover, velocity_alert_rows, stock_review_summary) are
import-safe and unit tested.
"""

from __future__ import annotations

from typing import Dict, List

from .shell import (
    Page, compute_health_metrics, _pos_adapter,
    _OPERATOR, _OVERSIGHT,
)

_ALL: tuple = ()


# ── pure helpers ─────────────────────────────────────────────────────────
def classify_cover(ads: float, soh: float) -> str:
    """Days-of-cover severity for one SKU (pure).

    ads>0:  DEPLETED (<1 unit) · CRITICAL (<0.5d) · URGENT (<1d) · LOW (<3d)
            · OVERSTOCK (>30d) · OK
    ads==0: DEAD (SOH>15) · OK
    """
    ads = float(ads or 0)
    soh = float(soh or 0)
    if ads <= 0:
        return "DEAD" if soh > 15 else "OK"
    if soh < 1:
        return "DEPLETED"
    days = soh / ads
    if days < 0.5:
        return "CRITICAL"
    if days < 1.0:
        return "URGENT"
    if days < 3.0:
        return "LOW"
    if days > 30.0:
        return "OVERSTOCK"
    return "OK"


_ALERT_SEVERITY = {"DEPLETED": 0, "CRITICAL": 1, "URGENT": 2, "LOW": 3}


def velocity_alert_rows(products_by_org: dict, org_names: dict = None,
                        cover_threshold_days: float = 3.0) -> List[dict]:
    """At-risk SKUs (ADS>0 and days-cover < threshold), worst first (pure)."""
    org_names = org_names or {}
    rows = []
    for org, products in (products_by_org or {}).items():
        for p in products or []:
            ads = float(p.get("avg_daily_sales", 0) or 0)
            soh = float(p.get("current_stocks", p.get("current_stock", 0)) or 0)
            if ads <= 0:
                continue
            days = soh / ads
            if days >= cover_threshold_days:
                continue
            sev = classify_cover(ads, soh)
            rows.append({
                "Severity": sev,
                "Store": org_names.get(org, org),
                "Product": str(p.get("product_name", ""))[:45],
                "Days Cover": round(days, 2),
                "ADS": round(ads, 2),
                "Stock": round(soh, 1),
                "_rank": _ALERT_SEVERITY.get(sev, 9),
            })
    rows.sort(key=lambda r: (r["_rank"], r["Days Cover"]))
    return rows


def risk_band(risk: float) -> str:
    """Display band for a 0..1 store-risk score (pure): HIGH / ELEVATED / OK."""
    r = float(risk or 0)
    if r >= 0.66:
        return "HIGH"
    if r >= 0.33:
        return "ELEVATED"
    return "OK"


def per_store_health(products_by_org: dict, org_names: dict = None,
                     risk_by_org: dict = None) -> List[dict]:
    """Per-store cover-class rollup (pure): SKUs / at-risk / overstock / dead.

    When ``risk_by_org`` ({org_cd: 0..1} from gnn_service.store_risk) is given,
    each row gains a Risk band + Risk Score and rows sort by that score (highest
    risk first); otherwise rows sort by at-risk SKU count as before.
    """
    org_names = org_names or {}
    risk_by_org = risk_by_org or {}
    rows = []
    for org, products in (products_by_org or {}).items():
        s = stock_review_summary({org: products})
        at_risk = s["DEPLETED"] + s["CRITICAL"] + s["URGENT"] + s["LOW"]
        row = {
            "Store": org_names.get(org, org),
            "SKUs": s["total"],
            "At-Risk (≤3d)": at_risk,
            "Overstock": s["OVERSTOCK"],
            "Dead": s["DEAD"],
            "_risk": at_risk,
        }
        if org in risk_by_org:
            score = float(risk_by_org[org] or 0)
            row["Risk"] = risk_band(score)
            row["Risk Score"] = round(score, 3)
            row["_riskscore"] = score
        rows.append(row)
    if risk_by_org:
        rows.sort(key=lambda r: -r.get("_riskscore", 0.0))
    else:
        rows.sort(key=lambda r: -r["_risk"])
    return rows


def top_movers(sales_df, n: int = 10) -> List[dict]:
    """Top-selling items by revenue from a sales-history DataFrame (pure)."""
    if sales_df is None or getattr(sales_df, "empty", True):
        return []
    g = (sales_df.groupby("item_name")
         .agg(Units=("qty", "sum"), Revenue=("net_amt", "sum"))
         .reset_index().sort_values("Revenue", ascending=False).head(n))
    return [{"Product": str(r["item_name"])[:45],
             "Units": round(float(r["Units"]), 1),
             "Revenue": round(float(r["Revenue"]), 0)} for _, r in g.iterrows()]


def stock_review_summary(products_by_org: dict) -> Dict[str, int]:
    """Network-wide counts by cover class (pure)."""
    out = {k: 0 for k in
           ("DEPLETED", "CRITICAL", "URGENT", "LOW", "OVERSTOCK", "DEAD", "OK")}
    out["total"] = 0
    for products in (products_by_org or {}).values():
        for p in products or []:
            out["total"] += 1
            cls = classify_cover(
                p.get("avg_daily_sales", 0),
                p.get("current_stocks", p.get("current_stock", 0)))
            out[cls] = out.get(cls, 0) + 1
    return out


def model_status_note(status: str) -> tuple:
    """(kind, message) for the GNN model-status banner (pure).

    kind is "warning" (untrained — actively misleading if trusted) or "caption"
    (trained / unavailable — informational).
    """
    if status == "trained":
        return ("caption", "Store risk blends the trained GNN with live inventory signal.")
    if status == "untrained":
        return ("warning", "GNN model is untrained — store risk shown is inventory-only.")
    return ("caption", "GNN unavailable — store risk shown is inventory-only.")


# ── data ─────────────────────────────────────────────────────────────────
def _network_stock(ctx) -> dict:
    """{org_cd: [enriched products]} for the whole network, cached per session."""
    st = ctx["st"]
    if "_intel_netstock" not in st.session_state:
        adapter = _pos_adapter(ctx)
        orgs = adapter.fetch_all_organizations()
        st.session_state["_intel_orgnames"] = {
            o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}
        st.session_state["_intel_netstock"] = {
            o["ORG_CD"]: adapter.fetch_enriched_products(o["ORG_CD"]) for o in orgs}
    return st.session_state["_intel_netstock"]


# ── pages ──────────────────────────────────────────────────────────────────
def render_pulse(ctx) -> None:
    """Network pulse: mode/phase, health vs targets, live alert count.

    SH-B: this page is the feeder for the journey value thread — when network
    stock is loaded it folds the live trapped-capital figure into journey_state
    so the Home / Executive-ROI value meter reflects real recovery progress.
    """
    st = ctx["st"]
    from . import components as C
    from ..logic import journey_state as JS

    if "_intel_netstock" not in st.session_state and not st.button("⚙️ Load network pulse"):
        st.markdown("### Operational Pulse")
        state = JS.load_state(ctx.get("journey_state_path"))
        C.mode_phase_badge(state["mode"], state["phase"], state["phase_name"],
                           state["value_recovered"], st_module=st)
        C.empty_state("Load the pulse", "Pull live network stock to compute health.", st_module=st)
        return

    stock = _network_stock(ctx)
    names = st.session_state.get("_intel_orgnames", {})

    # SH-B: feed value_recovered from trapped capital before rendering the badge.
    try:
        from ..logic.capital_recovery import trapped_capital_network, update_journey_recovery
        state = update_journey_recovery(
            trapped_capital_network(stock), ctx.get("journey_state_path"))
    except Exception:
        state = JS.load_state(ctx.get("journey_state_path"))

    st.markdown("### Operational Pulse")
    C.mode_phase_badge(state["mode"], state["phase"], state["phase_name"],
                       state["value_recovered"], st_module=st)
    C.value_recovered_meter(state["value_recovered"],
                            state.get("value_target") or 0, st_module=st)

    health = compute_health_metrics(stock)
    alerts = velocity_alert_rows(stock, names)

    # SH-A S5: high-risk store count from the shared risk service (GNN when
    # trained, else inventory-only).
    from ..logic import gnn_service
    try:
        risks = gnn_service.store_risk(stock)
    except Exception:
        risks = {}
    high_risk = sum(1 for v in risks.values() if risk_band(v) == "HIGH")

    C.kpi_row([
        {"label": "Stores", "value": len(stock)},
        {"label": "SKUs", "value": f"{health['total_skus']:,}"},
        {"label": "Dead Stock %", "value": f"{health['dead_stock_pct']}%",
         "status": "success" if health["dead_stock_pct"] < 5 else "danger"},
        {"label": "At-Risk (≤3d)", "value": len(alerts),
         "status": "danger" if alerts else "success"},
        {"label": "High-Risk Stores", "value": high_risk,
         "status": "danger" if high_risk else "success"},
    ], st_module=st)
    kind, msg = model_status_note(gnn_service.model_status())
    (st.warning if kind == "warning" else st.caption)(msg)


def render_velocity_alerts(ctx) -> None:
    """Velocity & stockout alerts: SKUs burning down below 3 days of cover."""
    st = ctx["st"]
    from . import components as C
    if "_intel_netstock" not in st.session_state and not st.button("⚙️ Scan velocity"):
        C.empty_state("Scan velocity", "Pull live network stock to find at-risk SKUs.", st_module=st)
        return
    stock = _network_stock(ctx)
    names = st.session_state.get("_intel_orgnames", {})
    thresh = st.slider("Days-cover alert threshold", 1.0, 7.0, 3.0, step=0.5)
    rows = velocity_alert_rows(stock, names, cover_threshold_days=thresh)
    st.markdown("### Velocity & Stockout Alerts")
    if not rows:
        C.empty_state("No velocity alerts", f"No SKUs below {thresh:g} days of cover.", st_module=st)
        return
    n = lambda s: sum(1 for r in rows if r["Severity"] == s)  # noqa: E731
    C.kpi_row([
        {"label": "Depleted", "value": n("DEPLETED"), "status": "danger"},
        {"label": "Critical", "value": n("CRITICAL"), "status": "danger"},
        {"label": "Urgent", "value": n("URGENT"), "status": "warning"},
        {"label": "Low", "value": n("LOW"), "status": "warning"},
    ], st_module=st)
    import pandas as pd
    df = pd.DataFrame(rows).drop(columns=["_rank"])
    st.dataframe(df, use_container_width=True, hide_index=True, height=460)


def render_stock_review(ctx) -> None:
    """End-of-day stock review: cover-class breakdown across the network."""
    st = ctx["st"]
    from . import components as C
    if "_intel_netstock" not in st.session_state and not st.button("⚙️ Load stock review"):
        C.empty_state("Load stock review", "Pull live network stock to review cover.", st_module=st)
        return
    stock = _network_stock(ctx)
    s = stock_review_summary(stock)
    st.markdown("### End-of-Day Stock Review")
    C.kpi_row([
        {"label": "Total SKUs", "value": f"{s['total']:,}"},
        {"label": "Depleted/Critical", "value": s["DEPLETED"] + s["CRITICAL"], "status": "danger"},
        {"label": "Overstock", "value": s["OVERSTOCK"], "status": "warning"},
        {"label": "Dead", "value": s["DEAD"], "status": "warning"},
    ], st_module=st)
    import pandas as pd
    order = ["DEPLETED", "CRITICAL", "URGENT", "LOW", "OK", "OVERSTOCK", "DEAD"]
    st.dataframe(pd.DataFrame([{"Cover Class": k, "SKUs": s[k]} for k in order]),
                 use_container_width=True, hide_index=True)


def render_live_sales(ctx) -> None:
    """Live sales feed for a store: revenue/units KPIs + top movers + daily trend."""
    st = ctx["st"]
    from . import components as C
    adapter = _pos_adapter(ctx)
    try:
        orgs = adapter.fetch_all_organizations()
    except Exception as e:
        C.error_panel("Could not load stores.", str(e), st_module=st)
        return
    if not orgs:
        C.empty_state("No stores", "Connect store data to view sales.", st_module=st)
        return
    names = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}
    st.markdown("### Live Sales")
    org = st.selectbox("Store", list(names), format_func=lambda x: f"{names[x]} ({x})")
    days = st.slider("Window (days)", 7, 90, 30, step=7)
    df = adapter.fetch_sales_history(org, days=days)
    if df is None or df.empty:
        C.empty_state("No sales in window", "No POS rows for this store/period.", st_module=st)
        return
    C.kpi_row([
        {"label": "Revenue", "value": f"KES {float(df['net_amt'].sum()):,.0f}"},
        {"label": "Units", "value": f"{float(df['qty'].sum()):,.0f}"},
        {"label": "Lines", "value": f"{len(df):,}"},
        {"label": "Avg Line", "value": f"KES {float(df['net_amt'].mean()):,.0f}"},
    ], st_module=st)
    import pandas as pd
    movers = top_movers(df, n=15)
    if movers:
        st.markdown("#### Top Movers")
        st.dataframe(pd.DataFrame(movers), use_container_width=True, hide_index=True)
    try:
        trend = df.groupby("bill_dt")["net_amt"].sum().reset_index()
        if len(trend) > 1:
            st.markdown("#### Daily Revenue")
            st.line_chart(trend.set_index("bill_dt"))
    except Exception:
        pass


def render_network_intel(ctx) -> None:
    """Network intelligence: per-store inventory health + read-only transfer
    opportunities (inventory-based; the deep ST-GAT/GNN view stays in the
    dedicated dashboard)."""
    st = ctx["st"]
    from . import components as C
    if "_intel_netstock" not in st.session_state and not st.button("⚙️ Load network intelligence"):
        C.empty_state("Load network intelligence",
                      "Pull live network stock to assess store health.", st_module=st)
        return
    stock = _network_stock(ctx)
    names = st.session_state.get("_intel_orgnames", {})

    # SH-A S5: per-store risk from the shared service (GNN when trained, else
    # inventory-only). Closes G-INT-1 — the Intelligence Console now surfaces the
    # platform's headline model. Read-only; ordering is unaffected.
    from ..logic import gnn_service
    status = gnn_service.model_status()
    try:
        risks = gnn_service.store_risk(stock)
    except Exception:
        risks = {}

    st.markdown("### Network Intelligence")
    kind, msg = model_status_note(status)
    (st.warning if kind == "warning" else st.caption)(msg)

    st.markdown("#### Store Health")
    import pandas as pd
    rows = per_store_health(stock, names, risk_by_org=risks)
    st.dataframe(pd.DataFrame([{k: v for k, v in r.items() if not k.startswith("_")}
                               for r in rows]),
                 use_container_width=True, hide_index=True)

    st.markdown("#### Transfer Opportunities (read-only)")
    try:
        from ..logic.consolidated_transfer_service import ConsolidatedTransferService
        cts = ConsolidatedTransferService(org_names=names, stock_data=stock,
                                          cold_node_days=60, hot_node_days=14)
        opps = cts.scan_network_opportunities().opportunities
        if opps:
            st.caption(f"{len(opps)} opportunities · act on them in the Operations Console → Transfers.")
            st.dataframe(pd.DataFrame([{
                "Type": o.type, "Product": o.product_name[:40],
                "From": names.get(o.from_org, o.from_org),
                "To": names.get(o.to_org, o.to_org),
                "Qty": o.transfer_qty, "Value (KES)": o.value_kes,
            } for o in opps[:50]]), use_container_width=True, hide_index=True, height=360)
        else:
            C.empty_state("No transfer opportunities", "Network is balanced.", st_module=st)
    except Exception as e:
        C.error_panel("Transfer scan unavailable.", str(e), st_module=st)


def roi_scorecard_rows(health: dict, value_recovered: float) -> List[dict]:
    """Pre→Post target scorecard rows (pure) — Playbook value table with live
    figures where computable."""
    dead = float(health.get("dead_stock_pct", 0))
    so = float(health.get("stockout_pct", 0))
    return [
        {"Metric": "Dead Stock %", "Live": f"{dead}%", "Target": "< 5%", "On Target": dead < 5.0},
        {"Metric": "Stockout %", "Live": f"{so}%", "Target": "< 2%", "On Target": so < 2.0},
        {"Metric": "Capital Recovered", "Live": f"KES {float(value_recovered or 0):,.0f}",
         "Target": "maximize", "On Target": float(value_recovered or 0) > 0},
    ]


def render_exec_roi(ctx) -> None:
    """Executive ROI: the capital-recovery showcase — journey value + live
    network health against the Playbook's Pre→Post targets."""
    st = ctx["st"]
    from . import components as C
    from ..logic import journey_state as JS

    state = JS.load_state(ctx.get("journey_state_path"))
    st.markdown("### Executive ROI")
    C.mode_phase_badge(state["mode"], state["phase"], state["phase_name"],
                       state["value_recovered"], st_module=st)
    C.value_recovered_meter(state["value_recovered"],
                            state.get("value_target") or 0, st_module=st)

    if "_intel_netstock" not in st.session_state and not st.button("⚙️ Load ROI scorecard"):
        C.empty_state("Load the scorecard", "Pull live network health to score against targets.", st_module=st)
        return
    health = compute_health_metrics(_network_stock(ctx))
    import pandas as pd
    st.markdown("#### Network Health vs Targets")
    st.dataframe(pd.DataFrame(roi_scorecard_rows(health, state["value_recovered"])),
                 use_container_width=True, hide_index=True)
    st.caption("Targets per the Implementation Playbook: dead stock < 5%, "
               "fast-mover stockout < 2%, capital utilization > 95%.")


def render_sim_lab(ctx) -> None:
    """Simulation Lab: competitive/chaos what-if — scenario demand multiplier
    and its effect on a store's recommended order quantities."""
    import os
    st = ctx["st"]
    from . import components as C
    try:
        from ..simulation.black_swan_events import SCENARIO_TEMPLATES
    except Exception as e:
        C.error_panel("Simulation templates unavailable.", str(e), st_module=st)
        return

    st.markdown("### Simulation Lab")
    keys = list(SCENARIO_TEMPLATES.keys())
    key = st.selectbox("Scenario", keys,
                       format_func=lambda k: f"{SCENARIO_TEMPLATES[k].competitor_name} "
                                             f"({SCENARIO_TEMPLATES[k].impact_pct:+.0f}%)")
    event = SCENARIO_TEMPLATES[key]
    day = st.slider("Day into the event", 1, max(2, event.ramp_up_days), min(15, event.ramp_up_days))
    mult = event.get_multiplier_for_day(day)
    C.kpi_row([
        {"label": "Competitor", "value": event.competitor_name},
        {"label": "YoY Impact", "value": f"{event.impact_pct:+.1f}%"},
        {"label": f"Demand × (day {day})", "value": f"{mult:.3f}"},
        {"label": "Distance", "value": f"{event.distance_meters} m"},
    ], st_module=st)

    st.markdown("#### What-if on a store's order")
    adapter = _pos_adapter(ctx)
    try:
        orgs = adapter.fetch_all_organizations()
    except Exception:
        orgs = []
    if not orgs:
        C.empty_state("No store data", "Connect store data to run a what-if.", st_module=st)
        return
    names = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}
    org = st.selectbox("Store", list(names), format_func=lambda x: names[x], key="sim_org")
    if st.button("Run what-if", type="primary"):
        with st.spinner("Recomputing orders under the scenario…"):
            try:
                from ..logic.order_engine import OrderEngine
                from ..logic.simulation_bridge import SimulationOrderUtil
                data_dir = os.path.join(ctx["project_root"], "oasis", "data")
                engine = st.session_state.get("_oasis_engine")
                if engine is None:
                    engine = OrderEngine(data_dir)
                    engine.load_local_databases()
                    st.session_state["_oasis_engine"] = engine
                sim = SimulationOrderUtil(data_dir, engine=engine)
                products = adapter.fetch_enriched_products(org)
                base = sim.prepare_sku_data(products)
                base_qty = sum(float(r.get("recommended_quantity", 0) or 0)
                               for r in sim.calculate_order_quantity(base, use_real_date=True))
                shocked = []
                for p in products:
                    q = dict(p)
                    q["avg_daily_sales"] = float(q.get("avg_daily_sales", 0) or 0) * mult
                    shocked.append(q)
                shock_prep = sim.prepare_sku_data(shocked, skip_enrichment=True)
                shock_qty = sum(float(r.get("recommended_quantity", 0) or 0)
                                for r in sim.calculate_order_quantity(shock_prep, use_real_date=True))
                delta = shock_qty - base_qty
                C.kpi_row([
                    {"label": "Baseline order qty", "value": f"{base_qty:,.0f}"},
                    {"label": "Scenario order qty", "value": f"{shock_qty:,.0f}"},
                    {"label": "Change", "value": f"{delta:+,.0f}",
                     "status": "warning" if abs(delta) > 0 else "success"},
                ], st_module=st)
            except Exception as e:
                C.error_panel("What-if simulation failed.", str(e), st_module=st)


# Every Intelligence Console page is now native — the _intel_bridge helper
# (which pointed at the legacy command center) has been retired.


def build_intel_registry() -> List[Page]:
    """Monitoring-focused registry for the Intelligence Console."""
    return [
        Page("pulse", "Pulse", "◎", render_pulse, _ALL),
        Page("velocity", "Velocity Alerts", "⚡", render_velocity_alerts, _OVERSIGHT),
        Page("stock_review", "Stock Review", "▦", render_stock_review, _OVERSIGHT),
        Page("live_sales", "Live Sales", "▤", render_live_sales, _OVERSIGHT),
        Page("network", "Network Intel", "⇄", render_network_intel, _OVERSIGHT),
        Page("exec_roi", "Executive ROI", "▣", render_exec_roi, _OVERSIGHT),
        Page("sim_lab", "Simulation Lab", "🧪", render_sim_lab, _OPERATOR),
    ]
