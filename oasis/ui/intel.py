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

from typing import Callable, Dict, List

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
    """Network pulse: mode/phase, health vs targets, live alert count."""
    st = ctx["st"]
    from . import components as C
    from ..logic import journey_state as JS

    state = JS.load_state(ctx.get("journey_state_path"))
    st.markdown("### Operational Pulse")
    C.mode_phase_badge(state["mode"], state["phase"], state["phase_name"],
                       state["value_recovered"], st_module=st)

    if "_intel_netstock" not in st.session_state and not st.button("⚙️ Load network pulse"):
        C.empty_state("Load the pulse", "Pull live network stock to compute health.", st_module=st)
        return
    stock = _network_stock(ctx)
    names = st.session_state.get("_intel_orgnames", {})
    health = compute_health_metrics(stock)
    alerts = velocity_alert_rows(stock, names)
    C.kpi_row([
        {"label": "Stores", "value": len(stock)},
        {"label": "SKUs", "value": f"{health['total_skus']:,}"},
        {"label": "Dead Stock %", "value": f"{health['dead_stock_pct']}%",
         "status": "success" if health["dead_stock_pct"] < 5 else "danger"},
        {"label": "At-Risk (≤3d)", "value": len(alerts),
         "status": "danger" if alerts else "success"},
    ], st_module=st)


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


def _intel_bridge(title: str, legacy: str, blurb: str) -> Callable:
    def _render(ctx) -> None:
        from . import components as C
        C.empty_state(f"{title} — in the legacy console",
                      f"{blurb}  Runs in {legacy} until migrated to the "
                      f"Intelligence Console.", st_module=ctx["st"])
    return _render


def build_intel_registry() -> List[Page]:
    """Monitoring-focused registry for the Intelligence Console."""
    return [
        Page("pulse", "Pulse", "◎", render_pulse, _ALL),
        Page("velocity", "Velocity Alerts", "⚡", render_velocity_alerts, _OVERSIGHT),
        Page("stock_review", "Stock Review", "▦", render_stock_review, _OVERSIGHT),
        Page("live_sales", "Live Sales", "▤",
             _intel_bridge("Live Sales", "ops_dashboard.py",
                           "Real-time sales feed (Phase 2+)."), _OVERSIGHT),
        Page("network", "Network Intel", "⇄",
             _intel_bridge("Network Intelligence", "st_gat_dashboard.py",
                           "ST-GAT/GNN store-risk & transfer pulse."), _OVERSIGHT),
        Page("exec_roi", "Executive ROI", "▣",
             _intel_bridge("Executive ROI", "ops_dashboard.py",
                           "The capital-recovery showcase."), _OVERSIGHT),
        Page("sim_lab", "Simulation Lab", "🧪",
             _intel_bridge("Simulation Lab", "ops_dashboard.py",
                           "Chaos / what-if scenarios."), _OPERATOR),
    ]
