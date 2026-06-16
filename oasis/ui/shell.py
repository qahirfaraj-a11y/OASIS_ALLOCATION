"""
OASIS unified shell (U3).

One front door: a single authenticated Streamlit app whose journey-driven,
role-gated navigation replaces the ~10 standalone dashboards. This module
holds the page registry, role-visibility logic (pure, tested), and the
native page renderers. ``app.py`` is the thin entry that wires auth + theme
+ this registry together.

Migration posture (per the UI plan): pages that are cheap and already have
unified logic are rendered natively here (Home/Journey on journey_state,
Allocation on greenfield_runner — collapsing the 4× duplication). Heavier
surfaces still living in their legacy app are shown as honest *bridge* pages
until they are migrated; the legacy launchers stay until parity is confirmed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional, Sequence


@dataclass(frozen=True)
class Page:
    key: str
    label: str
    icon: str
    render: Callable          # render(ctx) -> None
    roles: Sequence[str] = field(default_factory=tuple)  # empty = all roles

    def visible_to(self, role: Optional[str]) -> bool:
        return not self.roles or role in set(self.roles)


# Role groups span the legacy roles (ops_admin / regional_manager /
# branch_manager) and the journey role model (ilink_operator / executive /
# finance / approval_manager), so both work side by side.
_ALL = ()  # visible to any authenticated role
_OPERATOR = ("ops_admin", "ilink_operator")                       # full / admin
_OVERSIGHT = _OPERATOR + ("regional_manager", "executive")        # view + approve gates
_OPERATIONS = _OPERATOR + ("regional_manager", "approval_manager")  # act on orders/transfers


def build_registry() -> List[Page]:
    """The journey-ordered page registry (Customer Journey §5 IA), with
    per-page role visibility spanning legacy + journey roles."""
    return [
        Page("home", "Home / Journey", "◎", render_home, _ALL),
        Page("diagnose", "Diagnose", "⊙", render_diagnose, _OPERATOR),
        Page("shadow", "Shadow", "◑", render_shadow, _OVERSIGHT),
        Page("ordering", "Ordering", "▤", render_ordering, _OPERATIONS + ("branch_manager",)),
        Page("transfers", "Transfers", "⇄", render_transfers, _OPERATIONS),
        Page("suppliers", "Suppliers", "◇", render_suppliers, _OPERATIONS),
        Page("allocation", "Allocation", "▦", render_allocation, _ALL),
        Page("analytics", "Analytics", "▣", render_analytics, _OVERSIGHT + ("finance",)),
        Page("settings", "Settings", "⚙", render_settings, _OPERATOR),
    ]


def visible_pages(pages: Sequence[Page], role: Optional[str]) -> List[Page]:
    return [p for p in pages if p.visible_to(role)]


# ── page renderers ───────────────────────────────────────────────────────
def render_home(ctx) -> None:
    """The Journey home: mode/phase badge, value meter, the 7-stage rail, and
    (for operators/exec) the human-confirmed advance gate."""
    st = ctx["st"]
    from . import components as C
    from ..logic import journey_state as JS

    state = JS.load_state(ctx.get("journey_state_path"))
    st.markdown("### Journey")
    C.mode_phase_badge(state["mode"], state["phase"], state["phase_name"],
                       state["value_recovered"], st_module=st)
    C.value_recovered_meter(state["value_recovered"],
                            state.get("value_target") or 0, st_module=st)
    C.journey_rail(state["phase"], st_module=st)

    nxt = JS.next_phase(state["phase"])
    role = ctx.get("role")
    can_operate = role in ("ops_admin", "regional_manager")
    if nxt is not None and can_operate:
        # Human-confirmed advancement (never automatic).
        approved = C.decision_gate_card(
            title=f"Advance to Phase {nxt}: {JS.phase_name(nxt)}?",
            evidence="Confirm the decision gate for this phase has been met.",
            next_stage=JS.phase_name(nxt),
            can_advance=True,
            key="journey_advance",
            st_module=st,
        )
        if approved:
            JS.advance_phase(ctx.get("username", "unknown"),
                             ctx.get("journey_state_path"))
            st.rerun()
    elif nxt is None:
        st.success("Sustain — the operation is self-driving.")


def render_allocation(ctx) -> None:
    """Greenfield allocation, native in the shell (one page; replaces the
    4× duplicated allocation UIs — runs on the shared greenfield_runner)."""
    st = ctx["st"]
    import os
    from ..logic.greenfield_runner import (
        find_latest_scorecard, load_scorecard_recommendations,
        run_greenfield_allocation,
    )
    from ..logic.order_engine import OrderEngine

    st.markdown("### Allocation Engine")
    search_dir = ctx.get("project_root", os.getcwd())
    scorecard = find_latest_scorecard(search_dir)
    if not scorecard or not os.path.exists(scorecard):
        from . import components as C
        C.empty_state("No scorecard found",
                      "Place a Full_Product_Allocation_Scorecard_v*.csv in the "
                      "project directory to run a greenfield allocation.",
                      st_module=st)
        return

    budget = st.slider("Capital Budget (KES)", 50_000, 200_000_000, 3_000_000,
                       step=100_000)
    if st.button("Run Allocation", type="primary"):
        with st.spinner("Running greenfield allocation…"):
            engine = ctx.get("engine") or OrderEngine(
                os.path.join(search_dir, "oasis", "data"))
            recs = load_scorecard_recommendations(scorecard)
            res = run_greenfield_allocation(engine, recs, float(budget))
        if res.is_empty:
            st.warning("No items allocated. Try a larger budget.")
            return
        from . import components as C
        s = res.summary
        C.kpi_row([
            {"label": "Budget", "value": f"KES {budget:,.0f}"},
            {"label": "Cash Used", "value": f"KES {res.cash_spend:,.0f}"},
            {"label": "Utilization", "value": f"{s.get('utilization_pct', 0):.1f}%"},
            {"label": "SKUs", "value": len(res.basket)},
        ], st_module=st)
        st.dataframe(res.basket.sort_values("Allocated_Cost", ascending=False),
                     use_container_width=True, hide_index=True)


def group_recs_by_supplier(recs: Sequence[dict]) -> dict:
    """Group positive-quantity recommendations by supplier (pure, testable)."""
    out: dict = {}
    for r in recs:
        if float(r.get("recommended_quantity", 0) or 0) <= 0:
            continue
        supp = str(r.get("supplier_name") or "UNKNOWN").strip() or "UNKNOWN"
        out.setdefault(supp, []).append(r)
    return out


def _pos_adapter(ctx):
    """Cached PosErpAdapter via the central DB factory (honors OASIS_DB_URL)."""
    st = ctx["st"]
    if "_oasis_adapter" not in st.session_state:
        import os
        from ..logic.db_connector import UniversalConnector, SchemaMapper
        from ..logic.pos_erp_adapter import PosErpAdapter
        if os.getenv("OASIS_DB_URL"):
            from ..logic import db as oasis_db
            uri = oasis_db.get_sqlalchemy_url()
        else:
            uri = f"sqlite:///{ctx['db_path']}"
        st.session_state["_oasis_adapter"] = PosErpAdapter(
            UniversalConnector(uri, SchemaMapper.for_pos_erp()))
    return st.session_state["_oasis_adapter"]


def render_ordering(ctx) -> None:
    """Native daily-driver: generate a store's PO (engine → network → MOQ gate),
    review by supplier, push to approvals; plus an Approvals queue for approvers.

    Reuses the exact unified logic (SimulationOrderUtil, ConsolidatedTransferService,
    moq_failure_store) — no divergence from the fixes already landed. Advanced
    features (chaos scenarios, GNN risk overlay) remain in the legacy command
    center until migrated.
    """
    import os
    st = ctx["st"]
    from . import components as C
    from ..logic.order_engine import OrderEngine
    from ..logic.simulation_bridge import SimulationOrderUtil
    from ..logic.consolidated_transfer_service import ConsolidatedTransferService
    from ..logic.moq_failure_store import record_moq_failures

    adapter = _pos_adapter(ctx)
    try:
        orgs = adapter.fetch_all_organizations()
    except Exception as e:
        C.error_panel("Could not load stores from the database.", str(e), st_module=st)
        return
    if not orgs:
        C.empty_state("No stores found", "Seed or connect store data to order.", st_module=st)
        return

    name_map = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}
    # Role scoping: branch managers see only their assigned store.
    user = ctx.get("user", {})
    if ctx.get("role") == "branch_manager" and user.get("assigned_org"):
        org_ids = [user["assigned_org"]]
    else:
        org_ids = list(name_map.keys())

    can_approve = ctx.get("role") in ("ops_admin", "regional_manager")
    tabs = st.tabs(["🛠️ Generate Orders", "✅ Approvals"] if can_approve else ["🛠️ Generate Orders"])

    with tabs[0]:
        org = st.selectbox("Store", org_ids, format_func=lambda x: f"{name_map.get(x, x)} ({x})")
        data_dir = os.path.join(ctx["project_root"], "oasis", "data")
        key = f"_ordering_{org}"
        c1, c2 = st.columns([3, 1])
        with c2:
            if st.button("⚙️ Regenerate", key=f"regen_{org}"):
                st.session_state.pop(key, None)
        if key not in st.session_state:
            with st.spinner("Running ordering pipeline (engine → network → MOQ gate)…"):
                try:
                    engine = st.session_state.get("_oasis_engine")
                    if engine is None:
                        engine = OrderEngine(data_dir)
                        engine.load_local_databases()
                        st.session_state["_oasis_engine"] = engine
                    products = adapter.fetch_enriched_products(org)
                    sim = SimulationOrderUtil(data_dir, engine=engine)
                    enriched = sim.prepare_sku_data(products)
                    final = sim.finalize_orders(
                        sim.calculate_order_quantity(enriched, use_real_date=True))
                    cts = ConsolidatedTransferService(
                        org_names=name_map,
                        stock_data={o: adapter.fetch_enriched_products(o) for o in org_ids},
                        cold_node_days=60, hot_node_days=14)
                    plan = cts.optimize_network({org: final})
                    adjusted = plan.adjusted_orders.get(org, final)
                    mot = sim.apply_minimum_order_gate(adjusted)
                    try:
                        record_moq_failures(os.path.join(data_dir, "moq_failures.json"),
                                            org, mot["transfer_recs"] or [])
                    except Exception:
                        pass
                    st.session_state[key] = {"po_recs": mot["po_recs"]}
                except Exception as e:
                    C.error_panel("Ordering pipeline failed.", str(e), st_module=st)
                    return
        po_recs = st.session_state[key]["po_recs"]
        positive = [r for r in po_recs if float(r.get("recommended_quantity", 0) or 0) > 0]
        if not positive:
            C.empty_state("No orders recommended",
                          "Current stock and on-order cover demand for this store.", st_module=st)
        else:
            import pandas as pd
            grouped = group_recs_by_supplier(positive)
            C.kpi_row([
                {"label": "Store", "value": name_map.get(org, org)},
                {"label": "PO Lines", "value": len(positive)},
                {"label": "Suppliers", "value": len(grouped)},
            ], st_module=st)
            for supp, items in grouped.items():
                with st.expander(f"{supp} — {len(items)} items", expanded=False):
                    st.dataframe(pd.DataFrame([{
                        "Product": r.get("product_name", ""),
                        "Qty": r.get("recommended_quantity", 0),
                        "Reasoning": r.get("reasoning", ""),
                    } for r in items]), use_container_width=True, hide_index=True)
            if st.button("🚀 Push to Pending Approvals", type="primary"):
                try:
                    n = adapter.push_purchase_order(org, positive)
                    st.success(f"Sent {n} PO lines to approvals.")
                except Exception as e:
                    C.error_panel("Could not push the purchase order.", str(e), st_module=st)

    if can_approve and len(tabs) > 1:
        with tabs[1]:
            _render_approvals(ctx, adapter, org_ids)


def _render_approvals(ctx, adapter, org_ids) -> None:
    st = ctx["st"]
    from . import components as C
    org_filter = org_ids[0] if ctx.get("role") == "branch_manager" else None
    try:
        df = adapter.fetch_pending_pos(org_filter)
    except Exception as e:
        C.error_panel("Could not load pending POs.", str(e), st_module=st)
        return
    if df.empty:
        C.empty_state("Nothing to approve", "No purchase orders are awaiting approval.", st_module=st)
        return
    st.caption("Approve or reject pending purchase orders.")
    st.dataframe(df, use_container_width=True, hide_index=True)
    col1, col2 = st.columns(2)
    po_id = col1.number_input("PO_ID", min_value=1, step=1)
    if col1.button("✅ Approve", type="primary"):
        if adapter.update_po_status(int(po_id), "APPROVED", ctx.get("username", "")):
            st.success(f"Approved PO {int(po_id)}.")
            st.rerun()
        else:
            st.error("PO not found.")
    if col2.button("❌ Reject"):
        if adapter.update_po_status(int(po_id), "REJECTED", ctx.get("username", "")):
            st.success(f"Rejected PO {int(po_id)}.")
            st.rerun()
        else:
            st.error("PO not found.")


def render_transfers(ctx) -> None:
    """Native network-transfer opportunities, on the unified CTS scan
    (PULL/PUSH with pending-transfer awareness — the same scan_network_
    opportunities that replaced the inline dashboard logic). Mgmt-only.
    Queue selected non-fresh transfers to the DB for dispatch."""
    import os
    st = ctx["st"]
    from . import components as C
    from ..logic.consolidated_transfer_service import ConsolidatedTransferService
    from ..logic.moq_failure_store import load_moq_failures

    adapter = _pos_adapter(ctx)
    try:
        orgs = adapter.fetch_all_organizations()
    except Exception as e:
        C.error_panel("Could not load stores from the database.", str(e), st_module=st)
        return
    if not orgs:
        C.empty_state("No stores found", "Connect store data to scan transfers.", st_module=st)
        return

    name_map = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}
    org_ids = list(name_map.keys())
    data_dir = os.path.join(ctx["project_root"], "oasis", "data")

    st.markdown("### Network Transfer Intelligence")
    if "_transfers_scan" not in st.session_state or st.button("⚙️ Re-scan network"):
        with st.spinner("Scanning the network (PULL + PUSH, pending-aware)…"):
            try:
                moq = {}
                try:
                    moq = load_moq_failures(os.path.join(data_dir, "moq_failures.json"))
                except Exception:
                    pass
                pending = []
                try:
                    pdf = adapter.fetch_transfers(None)
                    if not pdf.empty:
                        pending = pdf.to_dict("records")
                except Exception:
                    pass
                cts = ConsolidatedTransferService(
                    org_names=name_map,
                    stock_data={o: adapter.fetch_enriched_products(o) for o in org_ids},
                    cold_node_days=60, hot_node_days=14)
                scan = cts.scan_network_opportunities(
                    moq_failures=moq, pending_transfers=pending)
                st.session_state["_transfers_scan"] = scan.opportunities
            except Exception as e:
                C.error_panel("Transfer scan failed.", str(e), st_module=st)
                return

    opps = st.session_state.get("_transfers_scan", [])
    if not opps:
        C.empty_state("No transfer opportunities",
                      "Every store is balanced, or no donor has excess above safety stock.",
                      st_module=st)
        return

    import pandas as pd
    rows = [{
        "Type": ("PULL" if o.type == "PULL" else "PUSH") + (" (manual)" if o.manual_only else ""),
        "Product": o.product_name[:45],
        "From": name_map.get(o.from_org, o.from_org),
        "To": name_map.get(o.to_org, o.to_org),
        "Qty": o.transfer_qty,
        "Value (KES)": o.value_kes,
        "Department": o.department,
    } for o in opps]
    C.kpi_row([
        {"label": "Opportunities", "value": len(opps)},
        {"label": "Total Value", "value": f"KES {sum(o.value_kes for o in opps):,.0f}"},
        {"label": "Store Pairs", "value": len({(o.from_org, o.to_org) for o in opps})},
    ], st_module=st)
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True, height=380)

    n = st.number_input("Queue top N (non-fresh) transfers", 1, 500,
                        min(50, len(opps)), step=10)
    if st.button("🚀 Queue Transfers", type="primary"):
        queued = 0
        for o in opps[:int(n)]:
            if o.manual_only:
                continue  # fresh items are manual-dispatch only
            try:
                if adapter.push_transfer_request(o.from_org, o.to_org, [{
                    "item_code": o.itm_cd, "product_name": o.product_name,
                    "transfer_qty": o.transfer_qty, "transfer_value": o.value_kes,
                    "urgency": "HIGH" if o.recipient_days_cover <= 1 else "MEDIUM",
                }]):
                    queued += 1
            except Exception:
                pass
        if queued:
            st.success(f"Queued {queued} transfers for dispatch.")
            st.session_state.pop("_transfers_scan", None)
            st.rerun()
        else:
            st.warning("No transfers queued (all selected were fresh/manual-only or rejected).")


def classify_supplier(multiplier: float) -> str:
    """Map a LATA variance multiplier to a Playbook reliability class (pure).

    LATA inflates safety stock for unreliable suppliers (>1.0) and releases
    capital for reliable ones (<=1.0). Bands mirror the Implementation
    Playbook: RELIABLE 1.2x / WATCH 1.5x / HOSTILE 2.0x+.
    """
    m = float(multiplier or 1.0)
    if m >= 1.5:
        return "HOSTILE"
    if m > 1.0:
        return "WATCH"
    return "RELIABLE"


def _load_supplier_patterns(data_dir: str) -> dict:
    import glob
    import json
    import os
    matches = sorted(glob.glob(os.path.join(data_dir, "supplier_patterns*.json")))
    if not matches:
        return {}
    try:
        with open(matches[0], "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def render_suppliers(ctx) -> None:
    """LATA Supplier Shield scorecard (Phase 5). Runs the LATA engine on
    demand and shows per-supplier reliability class + safety-stock multiplier."""
    import os
    st = ctx["st"]
    from . import components as C
    from ..logic.lata_shield import run_lata

    data_dir = os.path.join(ctx["project_root"], "oasis", "data")
    st.markdown("### Supplier Shield (LATA)")
    if st.button("⚙️ Run LATA scan"):
        with st.spinner("Scoring supplier reliability from delivery history…"):
            nn = os.path.join(ctx["project_root"], "neutral_network_export")
            run_lata(data_dir, nn if os.path.isdir(nn) else None)

    patterns = _load_supplier_patterns(data_dir)
    rows = []
    for supplier, d in patterns.items():
        if not isinstance(d, dict):
            continue
        mult = d.get("lata_variance_multiplier")
        if mult is None:
            continue
        rows.append({
            "Supplier": supplier,
            "Classification": classify_supplier(mult),
            "Safety Multiplier": round(float(mult), 2),
            "Confidence": d.get("lata_confidence", "—"),
            "Reason": d.get("lata_reason", ""),
        })
    if not rows:
        C.empty_state("No supplier scores yet",
                      "Run a LATA scan once GRN/delivery data has been ingested.",
                      st_module=st)
        return

    import pandas as pd
    counts = {"RELIABLE": 0, "WATCH": 0, "HOSTILE": 0}
    for r in rows:
        counts[r["Classification"]] += 1
    C.kpi_row([
        {"label": "Reliable", "value": counts["RELIABLE"], "status": "success"},
        {"label": "Watch", "value": counts["WATCH"], "status": "warning"},
        {"label": "Hostile", "value": counts["HOSTILE"], "status": "danger"},
    ], st_module=st)
    df = pd.DataFrame(rows).sort_values("Safety Multiplier", ascending=False)
    st.dataframe(df, use_container_width=True, hide_index=True, height=420)


def render_shadow(ctx) -> None:
    """Shadow Review (Phase 2): the human-vs-OASIS divergence that builds trust
    before the engine takes control. Read-only view of aggregated shadow logs."""
    import os
    st = ctx["st"]
    from . import components as C
    from ..logic.shadow_mode import ShadowModeEngine

    data_dir = os.path.join(ctx["project_root"], "oasis", "data")
    st.markdown("### Shadow Review — Human vs OASIS")
    try:
        logs = ShadowModeEngine(data_dir).load_aggregated_shadow_logs(14)
    except Exception:
        logs = None
    if logs is None or logs.empty:
        C.empty_state(
            "No shadow comparisons yet",
            "Shadow mode generates daily human-vs-OASIS divergence; run the "
            "shadow daemon (Phase 2) to populate this review.", st_module=st)
        return

    if "Divergence" in logs.columns:
        vc = logs["Divergence"].value_counts().to_dict()
        total = max(int(len(logs)), 1)
        C.kpi_row([
            {"label": "Aligned", "value": vc.get("ALIGNED", 0), "status": "success"},
            {"label": "Human Missed", "value": vc.get("HUMAN_MISSED", 0), "status": "danger"},
            {"label": "Over-ordered", "value": vc.get("HUMAN_OVER_ORDERED", 0), "status": "warning"},
            {"label": "Accuracy", "value": f"{vc.get('ALIGNED', 0) / total * 100:.0f}%"},
        ], st_module=st)
    st.dataframe(logs, use_container_width=True, hide_index=True, height=420)


def compute_health_metrics(products_by_org: dict,
                           dead_ads: float = 0.2, dead_soh: float = 15.0) -> dict:
    """Network health metrics from enriched stock (pure, testable).

    Uses the Playbook's documented detection rules:
      - dead stock: ADS < dead_ads AND stock > dead_soh   (AMIT)
      - stockout:   ADS > 0 AND stock < 1                 (DHARAM-style)
    Returns counts and percentages over all SKUs in the network.
    """
    total = dead = stockout = 0
    for products in (products_by_org or {}).values():
        for p in products or []:
            ads = float(p.get("avg_daily_sales", 0) or 0)
            soh = float(p.get("current_stocks", p.get("current_stock", 0)) or 0)
            total += 1
            if ads < dead_ads and soh > dead_soh:
                dead += 1
            if ads > 0 and soh < 1.0:
                stockout += 1
    pct = lambda n: round(n / total * 100, 1) if total else 0.0  # noqa: E731
    return {
        "total_skus": total,
        "dead_stock": dead,
        "dead_stock_pct": pct(dead),
        "stockouts": stockout,
        "stockout_pct": pct(stockout),
    }


def render_analytics(ctx) -> None:
    """Pre→Post target scorecard (Phase 7): live network health vs Playbook targets."""
    st = ctx["st"]
    from . import components as C
    adapter = _pos_adapter(ctx)
    try:
        orgs = adapter.fetch_all_organizations()
    except Exception as e:
        C.error_panel("Could not load stores from the database.", str(e), st_module=st)
        return
    if not orgs:
        C.empty_state("No data yet", "Connect store data to compute health.", st_module=st)
        return

    st.markdown("### Analytics — Network Health vs Targets")
    if "_analytics" not in st.session_state or st.button("⚙️ Recompute"):
        with st.spinner("Computing network health…"):
            stock = {o["ORG_CD"]: adapter.fetch_enriched_products(o["ORG_CD"]) for o in orgs}
            st.session_state["_analytics"] = compute_health_metrics(stock)
    m = st.session_state["_analytics"]

    # Live vs Playbook targets (dead stock < 5%, fast-mover stockout < 2%)
    dead_ok = m["dead_stock_pct"] < 5.0
    so_ok = m["stockout_pct"] < 2.0
    C.kpi_row([
        {"label": "Total SKUs", "value": f"{m['total_skus']:,}"},
        {"label": "Dead Stock %", "value": f"{m['dead_stock_pct']}%",
         "sub": "target < 5%", "status": "success" if dead_ok else "danger"},
        {"label": "Stockout %", "value": f"{m['stockout_pct']}%",
         "sub": "target < 2%", "status": "success" if so_ok else "danger"},
    ], st_module=st)
    st.caption(f"Dead stock: {m['dead_stock']:,} SKUs · Stockouts: {m['stockouts']:,} SKUs "
               f"(rules: dead = ADS<0.2 & SOH>15; stockout = ADS>0 & SOH<1)")


def render_settings(ctx) -> None:
    """Admin: users, engine feature flags, and journey/mode state (read-mostly)."""
    import os
    import json
    st = ctx["st"]
    from . import components as C
    from ..logic import journey_state as JS

    st.markdown("### Settings")
    # Journey / mode
    state = JS.load_state(ctx.get("journey_state_path"))
    C.mode_phase_badge(state["mode"], state["phase"], state["phase_name"],
                       state["value_recovered"], st_module=st)
    st.caption("Advance the journey phase from the Home screen (human-confirmed).")

    # Users
    st.markdown("#### Users")
    try:
        from ..logic.auth_manager import get_all_users
        users = get_all_users(ctx["db_path"])
        if users:
            import pandas as pd
            st.dataframe(pd.DataFrame(users)[["USERNAME", "DISPLAY_NAME", "ROLE",
                         "ASSIGNED_ORG", "ACTIVE_FLAG", "LAST_LOGIN_DT"]],
                         use_container_width=True, hide_index=True)
        else:
            C.empty_state("No users", "Seed accounts via OASIS_SEED_PASSWORD.", st_module=st)
    except Exception as e:
        C.error_panel("Could not load users.", str(e), st_module=st)

    # Engine feature flags (read-only view)
    st.markdown("#### Engine Flags")
    cfg_path = os.path.join(ctx["project_root"], "oasis", "data", "oasis_engines_config.json")
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                engines = json.load(f).get("engines", {})
            rows = [{"Engine": k, "Enabled": bool(v.get("enabled"))}
                    for k, v in engines.items()]
            if rows:
                import pandas as pd
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        except Exception:
            C.empty_state("Engine config unreadable", "Check oasis_engines_config.json.", st_module=st)
    else:
        C.empty_state("No engine config", "oasis_engines_config.json not found.", st_module=st)

    # U5: adoption — which shell pages are used (last 7 days)
    st.markdown("#### Adoption (last 7 days)")
    try:
        from .telemetry import page_view_counts
        counts = page_view_counts(ctx["db_path"], days=7)
        if counts:
            import pandas as pd
            st.dataframe(pd.DataFrame(
                [{"Page": k, "Views": v} for k, v in counts.items()]),
                use_container_width=True, hide_index=True)
        else:
            C.empty_state("No usage recorded yet", "Page views appear here as the shell is used.", st_module=st)
    except Exception:
        pass


def render_diagnose(ctx) -> None:
    """Operator-only forensic audit entry (Phase 1). The full upload/ingestion
    pipeline is the dedicated pitch app; this surfaces the latest audit output
    if present, and points operators to the ingestion tool otherwise."""
    import os
    import glob
    st = ctx["st"]
    from . import components as C

    st.markdown("### Forensic Diagnosis (operator)")
    root = ctx["project_root"]
    outputs = sorted(glob.glob(os.path.join(root, "OASIS_Forensic_Audit_Data*.xlsx")) +
                     glob.glob(os.path.join(root, "OASIS_Executive_Diagnostic*.docx")))
    if outputs:
        st.success(f"{len(outputs)} forensic audit output(s) on disk:")
        for o in outputs[-5:]:
            st.caption("• " + os.path.basename(o))
    C.empty_state(
        "Run the full forensic audit in the ingestion tool",
        "The Phase-1 audit (messy-data upload → ForensicOperationsIngestor → "
        "AMIT/DHARAM/LATA/MANDE → Word + Excel) is operated via pitch_app_v2.py. "
        "Outputs are handed to the prospect; this shell view tracks them.",
        st_module=st)

# All journey pages are now native in the shell — the interim _bridge helper
# (which pointed at legacy dashboards) has been retired.
