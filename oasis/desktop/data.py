"""
Data access for the desktop views — the Streamlit-free half of the shell.

The consoles reach their data through ``shell._pos_adapter(ctx)``, which caches
a PosErpAdapter in ``st.session_state``. The desktop app has no Streamlit, so
this module builds the same adapter the same way (same URIs, same SchemaMapper)
and caches it per process.

Everything here returns plain dicts/lists and NEVER raises: a view asking for
numbers should get an ``error`` key it can render honestly, not a traceback
that blanks the window. The one rule this module exists to enforce is that the
desktop reads the SAME data the Streamlit consoles do — no parallel queries, no
invented API. Phase 1 shipped views written against an assumed backend
(``authenticate_user``, ``license_posture``) that did not exist; going through
one verified accessor is how that stops happening.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

_ADAPTER = None
_ADAPTER_KEY = None


def project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def store_db_path(root: Optional[str] = None) -> str:
    """The active store, resolved the one true way (see onboarding W-7/S7)."""
    from oasis.logic.onboarding import resolved_db_path
    return resolved_db_path(root or project_root())


def get_adapter(root: Optional[str] = None):
    """PosErpAdapter for the active store, built exactly as shell._pos_adapter.

    Cached per resolved DB path, so re-onboarding to a different store rebuilds
    it instead of serving a stale handle.
    """
    global _ADAPTER, _ADAPTER_KEY
    db = store_db_path(root)
    if _ADAPTER is not None and _ADAPTER_KEY == db:
        return _ADAPTER

    from oasis.logic import db as oasis_db
    from oasis.logic.db_connector import SchemaMapper, UniversalConnector
    from oasis.logic.pos_erp_adapter import PosErpAdapter

    store_uri = (oasis_db.get_sqlalchemy_url() if os.getenv("OASIS_DB_URL")
                 else f"sqlite:///{db}")
    pos_uri = (oasis_db.get_pos_sqlalchemy_url()
               if oasis_db.has_distinct_pos() else store_uri)
    mapper = SchemaMapper.for_pos_erp()
    pos_conn = UniversalConnector(pos_uri, mapper)
    store_conn = (pos_conn if pos_uri == store_uri
                  else UniversalConnector(store_uri, mapper))
    _ADAPTER = PosErpAdapter(pos_conn, store_conn)
    _ADAPTER_KEY = db
    return _ADAPTER


def reset_adapter() -> None:
    """Drop the cached adapter (after re-onboarding / a data-source change)."""
    global _ADAPTER, _ADAPTER_KEY
    _ADAPTER, _ADAPTER_KEY = None, None


def list_stores(root: Optional[str] = None) -> List[dict]:
    """[{org_cd, name}] for the active install — [] when unreadable."""
    try:
        orgs = get_adapter(root).fetch_all_organizations() or []
        return [{"org_cd": o.get("ORG_CD"), "name": o.get("ORG_NAME") or o.get("ORG_CD")}
                for o in orgs]
    except Exception:
        return []


def default_org(root: Optional[str] = None) -> Optional[str]:
    stores = list_stores(root)
    return stores[0]["org_cd"] if stores else None


# ── view payloads ────────────────────────────────────────────────────────
def stock_overview(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Headline stock position for one store.

    ``{skus, stockouts, low_stock, stock_value, error}``. ``stock_value`` uses
    WAC (what the stock cost), matching how the consoles value inventory.
    """
    try:
        rows = get_adapter(root).fetch_stock_snapshot(org_cd) or []
    except Exception as e:
        return {"skus": 0, "stockouts": 0, "low_stock": 0, "stock_value": 0.0,
                "error": str(e)[:200]}

    skus = len(rows)
    stockouts = low = 0
    value = 0.0
    for r in rows:
        try:
            qty = float(r.get("current_stocks") or 0)
            wac = float(r.get("wac") or 0)
        except (TypeError, ValueError):
            continue
        value += qty * wac
        if qty <= 0:
            stockouts += 1
        elif qty < 5:
            low += 1
    return {"skus": skus, "stockouts": stockouts, "low_stock": low,
            "stock_value": round(value, 2), "error": None}


def pending_orders(org_cd: Optional[str] = None,
                   root: Optional[str] = None) -> Dict[str, Any]:
    """Open purchase orders: ``{count, rows, error}`` (rows are plain dicts)."""
    try:
        df = get_adapter(root).fetch_pending_pos(org_cd)
        rows = df.to_dict("records") if hasattr(df, "to_dict") else list(df or [])
        return {"count": len(rows), "rows": rows[:200], "error": None}
    except Exception as e:
        return {"count": 0, "rows": [], "error": str(e)[:200]}


def supplier_overview(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Supplier spread derived from the catalogue the store actually carries."""
    try:
        rows = get_adapter(root).fetch_enriched_products(org_cd) or []
    except Exception as e:
        return {"suppliers": 0, "top": [], "error": str(e)[:200]}

    counts: Dict[str, int] = {}
    for r in rows:
        name = (r.get("supplier_name") or r.get("SUPPLIER_NAME")
                or r.get("vendor") or "").strip()
        if name:
            counts[name] = counts.get(name, 0) + 1
    top = sorted(counts.items(), key=lambda kv: -kv[1])[:8]
    return {"suppliers": len(counts),
            "top": [{"name": n, "skus": c} for n, c in top], "error": None}


def engine_posture(root: Optional[str] = None) -> Dict[str, Any]:
    """Which Chapter-11 engines are live, and from which config tier (S1)."""
    try:
        from oasis.logic.engines_config import (KNOWN_ENGINES, is_engine_enabled,
                                                resolve_source)
        tier, path = resolve_source()
        return {"tier": tier or "none",
                "file": os.path.basename(path) if path else None,
                "engines": {name: bool(is_engine_enabled(name))
                            for name in KNOWN_ENGINES},
                "error": None}
    except Exception as e:
        return {"tier": "unknown", "file": None, "engines": {},
                "error": str(e)[:200]}


def license_gate(module: str = "core") -> Dict[str, Any]:
    """The licensing decision for this window — the console gate's twin (P3.0).

    Fails CLOSED, for the same reason ``app._needs_auth`` does: if we cannot
    prove this install is entitled, it is not. ``OfflineLicenseManager.status``
    already turns an unreadable or forged key into "locked" on its own; this
    guard covers the subsystem itself failing to load.
    """
    try:
        from oasis.logic.license_manager import gate_status
        return gate_status(module)
    except Exception as e:
        reason = f"license subsystem unavailable: {str(e)[:120]}"
        return {"mode": "locked", "blocked": True, "reason": reason,
                "notice": ("error", f"O.A.S.I.S. is locked — {reason}."),
                "tenant": None, "expiry": None, "days_left": None,
                "trial_days_left": 0}


def role_tabs(role: Optional[str]) -> Dict[str, bool]:
    """Which tabs this ROLE may see — the console's ROLE_PERMISSIONS table.

    Distinct from :func:`allowed_modules`, and both must pass. A module SKU
    answers "has this install bought the capability"; a role answers "is this
    person allowed to use it". Conflating them would let a branch manager see
    a licensed capability that is not theirs, or paywall an admin who simply
    has not bought a module.

    Fails CLOSED to the least-privileged role, for the same reason the licence
    gate does: an unknown role is not an authorised one.
    """
    try:
        from oasis.logic.auth_manager import ROLE_PERMISSIONS, get_user_permissions
        perms = get_user_permissions(role or "")
        return dict(perms.get("tabs") or {})
    except Exception:
        return {}


def allowed_modules() -> set:
    """Module SKUs this install may use right now — ``{"core"}`` if unknowable.

    Degrading to core (rather than to everything) keeps an error path from
    handing out paid modules, which is the mirror of the fail-closed rule above.
    """
    try:
        from oasis.logic.license_manager import allowed_modules as _allowed
        return set(_allowed())
    except Exception:
        return {"core"}


def data_provenance(root: Optional[str] = None) -> Dict[str, Any]:
    """What this install is looking at — the desktop's provenance chip (C1/G1)."""
    try:
        from oasis.logic.onboarding import load_onboarding
        rec = load_onboarding(root or project_root())
        return {"source": rec.get("source") or "none",
                "store_name": rec.get("store_name"),
                "is_sample": rec.get("source") == "demo",
                "db": os.path.basename(store_db_path(root))}
    except Exception:
        return {"source": "none", "store_name": None, "is_sample": False, "db": ""}


def generate_smart_orders(org_cd: str, thresholds: Optional[Dict[str, Any]] = None, root: Optional[str] = None) -> Dict[str, Any]:
    """Run the Smart Ordering pipeline (engine -> network -> MOQ gate) completely offline."""
    try:
        import os
        import json
        from datetime import datetime
        from oasis.logic.order_engine import OrderEngine
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
        from oasis.logic import gnn_service
        from oasis.logic.moq_failure_store import record_moq_failures
        
        proj_root = root or project_root()
        data_dir = os.path.join(proj_root, "oasis", "data")
        adapter = get_adapter(proj_root)
        
        products = adapter.fetch_enriched_products(org_cd) or []
        engine = OrderEngine(data_dir)
        engine.load_local_databases()
        
        sim_util = SimulationOrderUtil(data_dir, thresholds=thresholds, engine=engine)
        enriched = sim_util.prepare_sku_data(products)
        
        _ordering_risk = gnn_service.ordering_risk(products, gnn_risk_score=0.0)
        raw_recs = sim_util.calculate_order_quantity(enriched, gnn_risk_score=_ordering_risk, use_real_date=True)
        finalized_recs = sim_util.finalize_orders(raw_recs)
        
        all_orgs = adapter.fetch_all_organizations() or []
        all_org_cds = [o.get("ORG_CD") for o in all_orgs]
        org_name_map = {o.get("ORG_CD"): (o.get("ORG_NAME") or o.get("ORG_CD")) for o in all_orgs}
        
        enriched_network_stock = {}
        for o_cd in all_org_cds:
            if o_cd:
                enriched_network_stock[o_cd] = adapter.fetch_enriched_products(o_cd) or []
                
        distance_map = {}
        coords_path = os.path.join(proj_root, "store_coords.json")
        if os.path.exists(coords_path):
            with open(coords_path, "r") as f:
                distance_map = json.load(f)
                
        registry_path = os.path.join(data_dir, "network_registry.json")
        cts = ConsolidatedTransferService(
            org_names=org_name_map,
            stock_data=enriched_network_stock,
            registry_path=registry_path,
            distance_map=distance_map,
            cold_node_days=60,
            hot_node_days=14
        )
        
        network_plan = cts.optimize_network({org_cd: finalized_recs}, risk_scores={})
        network_adjusted_recs = network_plan.adjusted_orders.get(org_cd, [])
        mot_result = sim_util.apply_minimum_order_gate(network_adjusted_recs)
        
        dropped_recs = mot_result["transfer_recs"]
        if dropped_recs:
            try:
                moq_path = os.path.join(data_dir, "moq_failures.json")
                record_moq_failures(moq_path, org_cd, dropped_recs)
            except Exception:
                pass
                
        return {
            "po_recs": mot_result["po_recs"],
            "dropped_recs": dropped_recs,
            "network_plan": network_plan,
            "org_name_map": org_name_map,
            "enriched_network_stock": enriched_network_stock,
            "generated_at": datetime.now().strftime("%H:%M:%S"),
            "error": None
        }
    except Exception as e:
        return {"po_recs": [], "dropped_recs": [], "network_plan": None, "error": str(e)[:200]}


def store_map(root: Optional[str] = None) -> Dict[str, Any]:
    """Where this client's stores are, and which still need placing."""
    try:
        from oasis.logic.store_locations import merge_with_stores
        return merge_with_stores(list_stores(root), root=root)
    except Exception as e:
        return {"located": [], "missing": [], "error": str(e)[:200]}


def set_store_location(org_cd: str, lat: float, lon: float,
                       size_sqft: float = 10_000.0,
                       root: Optional[str] = None) -> Dict[str, Any]:
    try:
        from oasis.logic.store_locations import save_location
        return save_location(org_cd, lat, lon, size_sqft, root=root)
    except Exception as e:
        return {"saved": False, "error": str(e)[:200]}


def competitor_set(root: Optional[str] = None) -> Dict[str, Any]:
    """The client's own OSM competitor extract. See oasis.logic.geo_sources."""
    try:
        from oasis.logic.geo_sources import load_competitors
        return load_competitors(root=root)
    except Exception as e:
        return {"rows": [], "attribution": None, "error": str(e)[:200]}


def score_sites(candidates: List[Dict[str, Any]],
                size_sqft: float = 10_000.0,
                root: Optional[str] = None) -> Dict[str, Any]:
    """Rank candidate sites against this client's estate and competitors.

    Interpretable geography only — no model. See ``oasis.logic.site_scoring``
    for what this deliberately cannot tell you (it has no population data, so
    it ranks how contested a catchment is, not how big it is).
    """
    try:
        from oasis.logic.site_scoring import rank_sites
        placed = store_map(root)
        if placed.get("error"):
            return {"sites": [], "error": placed["error"]}
        if not placed["located"]:
            return {"sites": [], "missing": placed["missing"],
                    "error": "No store locations recorded yet — place your "
                             "existing stores first, so a candidate can be "
                             "scored against them."}
        comps = competitor_set(root)
        ranked = rank_sites(candidates, placed["located"],
                            comps.get("rows") or [], size_sqft=size_sqft)
        return {"sites": ranked, "own_stores": len(placed["located"]),
                "missing": placed["missing"],
                "competitors": len(comps.get("rows") or []),
                "attribution": comps.get("attribution"),
                "competitor_error": comps.get("error"), "error": None}
    except Exception as e:
        return {"sites": [], "error": str(e)[:200]}


def greenfield_scorecard(mode: str = "network",
                         org_cd: Optional[str] = None,
                         root: Optional[str] = None) -> Dict[str, Any]:
    """What a new site should carry, derived from this chain's own trading.

    Replaces the shipped-CSV scorecard, which held another retailer's per-SKU
    revenue and supplier terms and could never ship. See
    ``oasis.logic.scorecard_builder``.
    """
    try:
        from oasis.logic.scorecard_builder import build_from_adapter, summarise
        proj = root or project_root()
        orgs = ([org_cd] if (mode == "store" and org_cd)
                else [s["org_cd"] for s in list_stores(proj)])
        if not orgs:
            return {"recs": [], "skus": 0, "stores": 0, "summary": {},
                    "error": "No stores in the active database."}
        res = build_from_adapter(get_adapter(proj), orgs, mode=mode)
        res["summary"] = summarise(res)
        return res
    except Exception as e:
        return {"recs": [], "skus": 0, "stores": 0, "summary": {},
                "error": str(e)[:200]}


def run_greenfield(budget: float, mode: str = "network",
                   org_cd: Optional[str] = None,
                   root: Optional[str] = None) -> Dict[str, Any]:
    """Spend an opening budget across the recommended range.

    Two-pass budget-constrained allocation with the engine's efficiency guards
    — the Allocation Engine tab, minus the CSV.
    """
    blank = {"rows": [], "cash_spend": 0.0, "consignment_value": 0.0,
             "budget": budget, "skus": 0, "summary": {}, "error": None}
    try:
        from oasis.logic.order_engine import OrderEngine
        from oasis.logic.greenfield_runner import run_greenfield_allocation

        card = greenfield_scorecard(mode=mode, org_cd=org_cd, root=root)
        if card.get("error"):
            return dict(blank, error=card["error"])
        if not card["recs"]:
            return dict(blank, error="No line has enough demand signal to "
                                     "allocate an opening budget against.")

        proj = root or project_root()
        engine = OrderEngine(os.path.join(proj, "oasis", "data"))
        engine.load_local_databases()
        res = run_greenfield_allocation(engine, card["recs"], budget=float(budget))

        # GreenfieldResult.basket is a DataFrame. `df or fallback` raises
        # "The truth value of a DataFrame is ambiguous" — convert first, and
        # never put a frame on either side of a boolean operator.
        basket = getattr(res, "basket", None)
        if hasattr(basket, "to_dict"):
            rows = basket.to_dict("records")
        else:
            rows = list(basket or [])
        cash = float(getattr(res, "cash_spend", 0.0) or 0.0)
        consign = float(getattr(res, "consignment_value", 0.0) or 0.0)
        return {"rows": rows, "cash_spend": round(cash, 2),
                "consignment_value": round(consign, 2),
                "budget": float(budget), "skus": len(rows),
                "utilisation": round(cash / float(budget) * 100, 1) if budget else 0.0,
                "summary": dict(getattr(res, "summary", {}) or {}),
                "scorecard": card["summary"], "error": None}
    except Exception as e:
        return dict(blank, error=str(e)[:200])


#: budget bands that pick a simulation tier, as the console picks one
_SIM_TIERS = ((500_000, "Small_200k"), (5_000_000, "Medium_1M"),
              (float("inf"), "Large_10M"))


def simulation_tiers() -> Dict[str, Any]:
    """Store archetypes the simulator ships with — pure config, no client data."""
    try:
        from oasis.simulation.retail_simulator import STORE_UNIVERSES
        return {"tiers": [
            {"key": k, "budget": v.get("budget"), "max_skus": v.get("max_skus"),
             "safety_days": v.get("safety_days"),
             "description": v.get("description", k)}
            for k, v in STORE_UNIVERSES.items()], "error": None}
    except Exception as e:
        return {"tiers": [], "error": str(e)[:200]}


def run_simulation_comparison(org_cd: str, days: int = 30,
                              tier: Optional[str] = None,
                              root: Optional[str] = None) -> Dict[str, Any]:
    """Heuristic vs risk-adjusted replenishment over the SAME store, same seed.

    Built from the store's own enriched products, never from the allocation
    scorecard: that file is one retailer's per-SKU revenue and supplier terms
    and is not part of any install.

    Both runs share a seed and a starting SKU set, so the only difference is
    whether the store's risk score is fed to the ordering bridge — which is the
    whole point of the comparison. (The console once computed that risk and
    never passed it, making both runs identical.)
    """
    blank = {"days": days, "tier": None, "skus": 0, "risk": 0.0,
             "heuristic": None, "adjusted": None, "error": None}
    try:
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        from oasis.simulation.retail_simulator import (RetailSimulator, SKUState,
                                                       STORE_UNIVERSES)
        from oasis.logic import gnn_service

        proj = root or project_root()
        data_dir = os.path.join(proj, "oasis", "data")
        products = get_adapter(proj).fetch_enriched_products(org_cd) or []
        if not products:
            return dict(blank, error="No products found for this store.")

        bridge = SimulationOrderUtil(data_dir)
        enriched = bridge.prepare_sku_data(products)

        skus = []
        for p in enriched:
            try:
                s = SKUState(
                    product_name=p.get("product_name", "Unknown"),
                    supplier=p.get("supplier_name", "Unknown"),
                    department=p.get("department", "UNKNOWN"),
                    unit_price=float(p.get("selling_price", p.get("sell_price", 100)) or 100),
                    cost_price=float(p.get("wac", p.get("cost_price", 50)) or 50),
                    avg_daily_sales=float(p.get("avg_daily_sales", 0) or 0),
                    demand_cv=float(p.get("demand_cv", 0.5) or 0.5),
                    lead_time_days=int(p.get("lead_time_days", 3) or 3),
                    current_stock=float(p.get("current_stocks", 0) or 0),
                    is_fresh=bool(p.get("is_fresh", False)),
                )
                if s.avg_daily_sales > 0:
                    skus.append(s)
            except Exception:
                continue
        if not skus:
            return dict(blank, error="No SKU in this store has a demand signal "
                                     "to simulate against.")

        budget = sum(s.cost_price * s.current_stock for s in skus)
        if not tier:
            tier = next(k for cap, k in _SIM_TIERS if budget < cap)
        config = dict(STORE_UNIVERSES.get(tier)
                      or STORE_UNIVERSES.get("Medium_1M") or {})
        config["budget"] = budget

        risk = float(network_risk(root).get("stores") and
                     next((s["risk"] for s in network_risk(root)["stores"]
                           if s["org_cd"] == org_cd), 0.0) or 0.0)

        def _shape(r):
            return {"fill_rate": round(getattr(r, "avg_fill_rate", 0.0), 2),
                    "stockout_rate": round(getattr(r, "stockout_rate", 0.0), 2),
                    "revenue": round(getattr(r, "total_revenue", 0.0), 2),
                    "turnover": round(getattr(r, "inventory_turnover", 0.0), 2),
                    "capital_efficiency": round(
                        getattr(r, "capital_efficiency", 0.0), 2)}

        base = RetailSimulator("Heuristic Baseline", config, seed=42,
                               bridge=bridge, initial_skus=skus).run(days)
        adj = RetailSimulator("Risk-Adjusted", config, seed=42, bridge=bridge,
                              initial_skus=skus,
                              gnn_risk_score=risk).run(days)

        return {"days": days, "tier": tier, "skus": len(skus),
                "budget": round(budget, 2), "risk": round(risk, 3),
                "gnn_status": gnn_service.model_status(),
                "heuristic": _shape(base), "adjusted": _shape(adj),
                "error": None}
    except Exception as e:
        return dict(blank, error=str(e)[:200])


#: file types the batch processor accepts (the console's file_uploader list)
PROCESSOR_EXTENSIONS = ("xlsx", "xls", "csv")


def _run_coroutine(asyncio_mod, coro):
    """Run one coroutine without disturbing the caller's event loop.

    ``asyncio.run`` closes the loop it creates AND leaves the thread with no
    current loop, so anything later in the same process that reaches for
    ``get_event_loop().run_until_complete(...)`` — which the ordering-logic
    suite does — dies with "There is no current event loop". The desktop app is
    one long-lived process; a batch run must not sabotage what comes after it.
    """
    try:
        previous = asyncio_mod.get_event_loop_policy().get_event_loop()
    except Exception:
        previous = None
    loop = asyncio_mod.new_event_loop()
    try:
        asyncio_mod.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        # Put back whatever was current before, even if that was nothing.
        try:
            asyncio_mod.set_event_loop(previous)
        except Exception:
            pass


def process_inventory_files(paths: List[str], username: str, org_cd: str,
                            out_dir: Optional[str] = None,
                            root: Optional[str] = None) -> Dict[str, Any]:
    """Batch-process picking lists / GRN files into order recommendations.

    The console's pipeline exactly — parse → enrich → RuleBasedLLM decision →
    Excel report — one entry per file so a bad sheet cannot sink the batch.

    Unlike the browser console this writes the report to a real folder and hands
    back the path: a desktop app has a filesystem, so a download button would be
    a worse answer than a file the operator can open.
    """
    results: List[Dict[str, Any]] = []
    try:
        import asyncio
        import tempfile
        from oasis.logic.order_engine import OrderEngine
        from oasis.llm.inference import RuleBasedLLM
        from oasis.logic.audit_logger import (log_action, ACTION_FILE_PROCESSED,
                                              ENTITY_FILE)
        proj = root or project_root()
        data_dir = os.path.join(proj, "oasis", "data")
        out_dir = out_dir or os.path.join(tempfile.gettempdir(), "oasis_processed")
        os.makedirs(out_dir, exist_ok=True)
        engine = OrderEngine(data_dir)
        engine.load_local_databases()
        llm = RuleBasedLLM()
    except Exception as e:
        return {"results": [], "processed": 0, "failed": 0,
                "out_dir": None, "error": str(e)[:200]}

    for path in paths:
        name = os.path.basename(path)
        try:
            products = engine.parse_inventory_file(path)
            products = engine.enrich_product_data(products)
            recs = _run_coroutine(asyncio, llm.analyze(products))
            out_path = os.path.join(out_dir, f"processed_{name}")
            engine.generate_excel_report(path, recs, out_path)
            results.append({"file": name, "products": len(products or []),
                            "recommendations": len(recs or []),
                            "output": out_path, "error": None})
            try:
                log_action(store_db_path(proj), username, ACTION_FILE_PROCESSED,
                           ENTITY_FILE, name, org_cd,
                           {"products": len(products or []),
                            "recommendations": len(recs or [])})
            except Exception:
                pass
        except Exception as e:
            results.append({"file": name, "products": 0, "recommendations": 0,
                            "output": None, "error": str(e)[:200]})

    return {"results": results,
            "processed": sum(1 for r in results if not r["error"]),
            "failed": sum(1 for r in results if r["error"]),
            "out_dir": out_dir, "error": None}


#: Supplier failure modes, and what each does to a line's replenishment maths.
#: Verbatim from the console's Apply Supplier Disruption branch.
FAILURE_MODES = ("Complete (No Supply)", "Partial (50% Capacity)",
                 "Delayed (2x Lead Time)")
#: The day the console evaluates a competitive event's ramp at, and a price war's.
COMPETITOR_EVAL_DAY = 15
PRICE_WAR_EVAL_DAY = 7


def critical_suppliers(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Suppliers whose failure would actually hurt — the console's shortlist.

    Runs ``SupplierRiskAnalyzer.identify_critical_suppliers`` (>30% share of a
    department, or >100K revenue potential) over the store's live catalogue.
    """
    try:
        from oasis.simulation.black_swan_events import SupplierRiskAnalyzer
        products = get_adapter(root).fetch_enriched_products(org_cd) or []
        inventory = {
            p.get("product_name", "Unknown"): {
                "department": p.get("department", "UNKNOWN"),
                "supplier": p.get("supplier_name", "UNKNOWN"),
                "avg_daily_sales": p.get("avg_daily_sales", 0),
                "price": p.get("selling_price", p.get("sell_price", 0)),
            } for p in products
        }
        return {"suppliers": SupplierRiskAnalyzer().identify_critical_suppliers(
            inventory) or [], "error": None}
    except Exception as e:
        return {"suppliers": [], "error": str(e)[:200]}


def competitive_scenarios() -> Dict[str, Any]:
    """The shipped competitive-event templates, as pickable options."""
    try:
        from oasis.simulation.black_swan_events import SCENARIO_TEMPLATES
        out = []
        for key, ev in SCENARIO_TEMPLATES.items():
            out.append({
                "key": key,
                "name": getattr(ev, "competitor_name", key),
                "impact_pct": getattr(ev, "impact_pct", 0.0),
                "ramp_up_days": getattr(ev, "ramp_up_days", 0),
                "distance_meters": getattr(ev, "distance_meters", None),
            })
        return {"scenarios": out, "error": None}
    except Exception as e:
        return {"scenarios": [], "error": str(e)[:200]}


def _apply_disruption(products: List[dict], supplier: str, mode: str,
                      duration_days: int) -> tuple:
    """Copy the catalogue with one supplier's lines degraded. Pure."""
    out, affected = [], 0
    target = (supplier or "").upper().strip()
    for p in products:
        c = dict(p)
        if target and target in str(c.get("supplier_name", "")).upper().strip():
            affected += 1
            lead = c.get("lead_time_days", 3) or 3
            cv = c.get("demand_cv", 0.5) or 0.5
            if "Complete" in mode:
                c["lead_time_days"] = lead + duration_days
                c["demand_cv"] = min(2.0, cv * 2.0)
            elif "Partial" in mode:
                c["lead_time_days"] = lead + int(duration_days * 0.5)
                c["demand_cv"] = min(1.5, cv * 1.5)
            else:                                    # Delayed
                c["lead_time_days"] = lead * 2
        out.append(c)
    return out, affected


def simulate_ordering_scenario(org_cd: str, kind: str,
                               supplier: Optional[str] = None,
                               mode: str = FAILURE_MODES[0],
                               duration_days: int = 14,
                               template: Optional[str] = None,
                               root: Optional[str] = None) -> Dict[str, Any]:
    """Re-run the ordering engine under a shock, against an unshocked baseline.

    ``kind`` is ``"supplier"``, ``"competitor"`` or ``"price_war"``. Both runs
    use the SAME engine and the same gate-compliant risk as a normal
    generation, so the delta is attributable to the shock and nothing else.
    """
    blank = {"baseline_qty": 0.0, "adjusted_qty": 0.0, "delta": 0.0,
             "pct_change": 0.0, "affected": 0, "label": "", "recs": [],
             "multiplier": None, "error": None}
    try:
        from oasis.logic.order_engine import OrderEngine
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        from oasis.logic import gnn_service

        proj = root or project_root()
        data_dir = os.path.join(proj, "oasis", "data")
        products = get_adapter(proj).fetch_enriched_products(org_cd) or []
        if not products:
            return dict(blank, error="No product data for this store.")

        engine = OrderEngine(data_dir)
        engine.load_local_databases()
        util = SimulationOrderUtil(data_dir, engine=engine)
        enriched = util.prepare_sku_data(products)
        risk = gnn_service.ordering_risk(products, gnn_risk_score=0.0)

        def _total(recs):
            return sum(float(r.get("recommended_quantity", 0) or 0)
                       for r in recs
                       if float(r.get("recommended_quantity", 0) or 0) > 0)

        baseline = util.finalize_orders(
            util.calculate_order_quantity(list(enriched), gnn_risk_score=risk))

        affected, multiplier, label = 0, None, ""
        if kind == "supplier":
            shocked, affected = _apply_disruption(enriched, supplier or "",
                                                  mode, duration_days)
            label = f"{supplier} — {mode}, {duration_days} days"
        elif kind in ("competitor", "price_war"):
            from oasis.simulation.black_swan_events import SCENARIO_TEMPLATES
            key = template or ("price_war_aggressive" if kind == "price_war"
                               else next(iter(SCENARIO_TEMPLATES)))
            ev = SCENARIO_TEMPLATES[key]
            day = (PRICE_WAR_EVAL_DAY if kind == "price_war"
                   else COMPETITOR_EVAL_DAY)
            multiplier = round(ev.get_multiplier_for_day(day), 3)
            shocked = []
            for p in enriched:
                c = dict(p)
                dept = str(c.get("department", "")).upper()
                c["avg_daily_sales"] = (float(c.get("avg_daily_sales", 0) or 0)
                                        * ev.get_multiplier_for_day(day, dept))
                shocked.append(c)
            affected = len(shocked)
            label = (f"{getattr(ev, 'competitor_name', key)} at day {day} "
                     f"({getattr(ev, 'impact_pct', 0):+.1f}% YoY)")
        else:
            return dict(blank, error=f"unknown scenario: {kind}")

        adjusted = util.finalize_orders(
            util.calculate_order_quantity(shocked, gnn_risk_score=risk))

        b, a = _total(baseline), _total(adjusted)
        return {"baseline_qty": b, "adjusted_qty": a, "delta": a - b,
                "pct_change": round((a - b) / max(1.0, b) * 100, 1),
                "affected": affected, "label": label, "multiplier": multiplier,
                "recs": [r for r in adjusted
                         if float(r.get("recommended_quantity", 0) or 0) > 0],
                "error": None}
    except Exception as e:
        return dict(blank, error=str(e)[:200])


def push_purchase_order(org_cd: str, username: str, po_recs: List[Dict[str, Any]], root: Optional[str] = None) -> Dict[str, Any]:
    try:
        import time
        from oasis.logic.audit_logger import log_action, ACTION_PO_GENERATED, ENTITY_PO
        proj_root = root or project_root()
        adapter = get_adapter(proj_root)
        pushed = adapter.push_purchase_order(org_cd, po_recs)
        if pushed:
            db = store_db_path(proj_root)
            log_action(db, username, ACTION_PO_GENERATED, ENTITY_PO, f"PO_{org_cd}_{int(time.time())}", org_cd, {"items": pushed})
            return {"success": True, "pushed_count": pushed, "error": None}
        return {"success": False, "pushed_count": 0, "error": "No items pushed."}
    except Exception as e:
        return {"success": False, "pushed_count": 0, "error": str(e)[:200]}


def update_po_status(po_id: int, status: str, username: str, org_cd: str, new_qty: Optional[float] = None, reason: Optional[str] = None, root: Optional[str] = None) -> Dict[str, Any]:
    try:
        from oasis.logic.audit_logger import log_action, ENTITY_PO
        proj_root = root or project_root()
        adapter = get_adapter(proj_root)
        if adapter.update_po_status(po_id, status, username, new_qty, reason):
            db = store_db_path(proj_root)
            action = f"PO_{status}"
            details = {}
            if new_qty is not None: details["new_qty"] = new_qty
            if reason is not None: details["reason"] = reason
            log_action(db, username, action, ENTITY_PO, f"PO_ID_{po_id}", org_cd, details)
            return {"success": True, "error": None}
        return {"success": False, "error": "Update failed."}
    except Exception as e:
        return {"success": False, "error": str(e)[:200]}


def executive_roi(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Executive ROI overview statistics for one store."""
    try:
        rows = get_adapter(root).fetch_enriched_products(org_cd) or []
    except Exception as e:
        return {"total_skus": 0, "dead_pct": 0.0, "so_pct": 0.0, "trapped": 0.0,
                "stockout": 0, "avail": 0.0, "error": str(e)[:200]}

    total_skus = len(rows)
    dead = 0
    trapped = 0.0
    stockout = 0
    
    for r in rows:
        ads = float(r.get('avg_daily_sales', 0) or 0)
        soh = float(r.get('current_stocks', r.get('current_stock', 0)) or 0)
        cost = float(r.get('cost_price', r.get('wac', 0)) or 0) or \
               float(r.get('selling_price', 0) or 0) * 0.75
        if ads < 0.2 and soh > 15:
            dead += 1
            trapped += soh * cost
        if ads > 0 and soh < 1:
            stockout += 1
            
    dead_pct = round(dead / total_skus * 100, 1) if total_skus else 0.0
    so_pct = round(stockout / total_skus * 100, 1) if total_skus else 0.0
    avail = round(100.0 - so_pct, 1)
    
    return {
        "total_skus": total_skus,
        "dead_pct": dead_pct,
        "so_pct": so_pct,
        "trapped": round(trapped, 2),
        "stockout": stockout,
        "avail": avail,
        "error": None
    }


def eod_stock_heuristic(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Items with < 3 days cover."""
    try:
        rows = get_adapter(root).fetch_enriched_products(org_cd) or []
    except Exception as e:
        return {"items": [], "error": str(e)[:200]}
    
    items = []
    for r in rows:
        name = r.get("product_name", "Unknown")
        qty = float(r.get("current_stocks", 0) or 0)
        ads = float(r.get("avg_daily_sales", 0) or 0)
        uom = str(r.get("uom", "EA")).upper()
        
        if ads > 0:
            days_cover = qty / ads
            if days_cover < 3.0:
                if qty < 1.0: severity = "⛔ DEPLETED"
                elif days_cover < 0.5: severity = "🔴 CRITICAL (<½ day)"
                elif days_cover < 1.0: severity = "🟠 URGENT (<1 day)"
                else: severity = "🟡 LOW (<3 days)"
                
                items.append({
                    "Severity": severity,
                    "Product": name,
                    "Dept": str(r.get("department") or r.get("category") or ""),
                    "Stock": round(qty, 1) if uom == "KG" else int(round(qty)),
                    "ADS": round(ads, 1),
                    "Cover": f"{days_cover:.1f} days"
                })
    return {"items": items, "error": None}


#: Why the graph-dependent surfaces are dark on a client install. The store
#: graph and its trained checkpoint are development assets: neither
#: network_simulation.py nor models/ is on the release whitelist, and the
#: store-GNN has not yet beaten baseline on a real-outcome backtest, so it is
#: deliberately not shipped. Surfaces that need it must say so plainly rather
#: than report a generic failure the client cannot act on.
_NO_GRAPH = ("Network graph model is not part of this install — "
             "store-level risk is shown from inventory instead.")
_NO_GRAPH_CLUSTER = ("Network graph model is not part of this install, so "
                     "stores cannot be clustered by learned similarity.")


def transfer_intelligence(root: Optional[str] = None) -> Dict[str, Any]:
    """Transfer intelligence using GNN and CTS."""
    try:
        from oasis.logic.gnn_service import get_gnn_resources, store_risk
        
        gnn_model, gnn_sim = get_gnn_resources()
        if not gnn_model or not gnn_sim:
            # Honest, and specific about WHY. The ST-GAT proposal layer needs
            # the store graph and the trained checkpoint, neither of which is
            # part of a client release. Store-level risk does NOT need them, so
            # still hand back the inventory-led risks rather than going dark:
            # losing the proposals should not cost the client the risk view.
            fallback = network_risk(root)
            return {"risks": [{"store_id": s["org_cd"], "risk": s["risk"]}
                              for s in fallback.get("stores", [])],
                    "recs": [], "error": _NO_GRAPH}
            
        import torch
        x_t = gnn_sim.get_feature_matrix()
        with torch.no_grad():
            gnn_out = gnn_model(x_t, gnn_sim.edge_index)
            gnn_out['transfer'] = gnn_model.get_all_transfer_scores(gnn_out['embeddings']).unsqueeze(0)
            
        traffic_mat = gnn_sim.get_traffic_matrix()
        gnn_stores = gnn_sim.stores_data
        gnn_ids = [s['store_id'] for s in gnn_stores]
        
        # Build stock_by_org for store_risk() — use enriched products for real ADS
        _adapter = get_adapter(root)
        _stock_by_org = {}
        for sid in gnn_ids:
            org_cd = sid if sid.startswith("ORG") else sid.replace("CFP-", "ORG")
            try:
                _stock_by_org[org_cd] = _adapter.fetch_enriched_products(org_cd) or []
            except Exception:
                _stock_by_org[org_cd] = []
        risk_scores_map = store_risk(_stock_by_org)
        # Map back to gnn_ids (CFP-style keys if needed)
        for sid in gnn_ids:
            org_cd = sid if sid.startswith("ORG") else sid.replace("CFP-", "ORG")
            if sid not in risk_scores_map and org_cd in risk_scores_map:
                risk_scores_map[sid] = risk_scores_map[org_cd]
        risk_scores = [risk_scores_map.get(sid, gnn_out['risk'][i].item()) for i, sid in enumerate(gnn_ids)]
        
        risks = [{"store_id": sid, "risk": r} for sid, r in zip(gnn_ids, risk_scores)]
        
        transfer_mat = gnn_out['transfer'][0]
        traffic_sq = traffic_mat.squeeze(-1)
        gnn_recs = []
        for si, src in enumerate(gnn_stores):
            for dj, dst in enumerate(gnn_stores):
                if si == dj: continue
                score = transfer_mat[si, dj].item()
                fric = traffic_sq[si, dj].item()
                if score > 0.25:
                    profit_pulse = score * 1000
                    friction_pen = fric * 400
                    net_gain = profit_pulse - friction_pen
                    gnn_recs.append({
                        "From": src['store_id'], "To": dst['store_id'],
                        "Score": f"{score:.2f}", "Priority Index": f"{net_gain:,.0f}",
                        "_net_gain": net_gain
                    })
        gnn_recs.sort(key=lambda x: -x["_net_gain"])
        return {"risks": risks, "recs": gnn_recs, "error": None}
    except Exception as e:
        return {"risks": [], "recs": [], "error": str(e)[:200]}


def store_intelligence(org_cd: str, root: Optional[str] = None,
                       days: int = 90) -> Dict[str, Any]:
    """Top movers, revenue drivers and category mix from the store's OWN sales.

    ``root`` stays the SECOND positional argument: market_view calls this as
    ``store_intelligence(org, project_root)``, so slipping a new parameter in
    front of it passes a path where a day count is expected and the tab dies
    with "unsupported type for timedelta days component: str".

    This used to read ``sku.total_sales`` off the GNN NetworkSimulator, which
    was wrong twice over. It sourced a client's store numbers from a
    *simulation* rather than from their POS; and the simulator's dependencies
    (``network_simulation``, ``models/``) are not on the release whitelist, so
    on every client install the tab could only ever say "GNN resources
    unavailable". Store Intelligence is a sales question and is answered from
    sales.
    """
    try:
        adapter = get_adapter(root)
        orgs = ([s["org_cd"] for s in list_stores(root)] if org_cd == "ALL"
                else [org_cd])
        rows: List[Dict[str, Any]] = []
        for o in orgs:
            raw = adapter.fetch_sales_history(o, days=days)
            recs = (raw.to_dict("records") if hasattr(raw, "to_dict")
                    else list(raw or []))
            rows.extend({str(k).lower(): v for k, v in r.items()} for r in recs)
    except Exception as e:
        return {"top_qty": [], "top_rev": [], "categories": [],
                "error": str(e)[:200]}

    if not rows:
        return {"top_qty": [], "top_rev": [], "categories": [], "error": None}

    # Department comes from the catalogue, not the sales feed. The enriched
    # product rows key the SKU as `item_code`; the sales rows call it `itm_cd`.
    # Accept both — keying on the wrong one silently files every product under
    # "Uncategorised" and the category mix becomes a single meaningless bar.
    dept: Dict[Any, str] = {}
    on_hand: Dict[Any, float] = {}
    try:
        for o in orgs:
            for p in (adapter.fetch_enriched_products(o) or []):
                code = (p.get("item_code") or p.get("itm_cd")
                        or p.get("ITEM_CODE") or p.get("ITM_CD"))
                if code is None:
                    continue
                if code not in dept:
                    dept[code] = str(p.get("department")
                                     or p.get("category") or "Uncategorised")
                try:
                    on_hand[code] = on_hand.get(code, 0.0) + float(
                        p.get("current_stocks", p.get("current_stock", 0)) or 0)
                except (TypeError, ValueError):
                    pass
    except Exception:
        pass

    def _f(r, k):
        try:
            return float(r.get(k, 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    agg: Dict[Any, Dict[str, Any]] = {}
    for r in rows:
        code = r.get("itm_cd")
        if code is None:
            continue
        e = agg.setdefault(code, {
            "Product": r.get("item_name") or str(code),
            "Category": dept.get(code, "Uncategorised"),
            # What is on the shelf right now. The old simulator-backed version
            # reported ``sku.stockout_days`` — a count of stockout days inside a
            # simulation, which said nothing about the client's actual store.
            "OnHand": on_hand.get(code, 0.0),
            "Units": 0.0, "Revenue": 0.0})
        e["Units"] += _f(r, "qty")
        e["Revenue"] += _f(r, "net_amt")

    items = list(agg.values())
    cats: Dict[str, Dict[str, Any]] = {}
    for e in items:
        c = cats.setdefault(e["Category"],
                            {"Category": e["Category"], "Revenue": 0.0, "Units": 0.0})
        c["Revenue"] += e["Revenue"]
        c["Units"] += e["Units"]

    return {
        "top_qty": sorted(items, key=lambda e: -e["Units"])[:15],
        "top_rev": sorted(items, key=lambda e: -e["Revenue"])[:15],
        "categories": sorted(cats.values(), key=lambda c: -c["Revenue"]),
        "error": None,
    }

def cluster_analysis(root: Optional[str] = None) -> Dict[str, Any]:
    try:
        from oasis.logic.gnn_service import get_gnn_resources, store_risk
        
        gnn_model, gnn_sim = get_gnn_resources()
        if not gnn_model or not gnn_sim:
            return {"clusters": [], "error": _NO_GRAPH_CLUSTER}
            
        import torch
        from sklearn.decomposition import PCA
        from sklearn.cluster import KMeans
        
        x_t = gnn_sim.get_feature_matrix()
        X_np = x_t.cpu().numpy()
        
        pca = PCA(n_components=2)
        components = pca.fit_transform(X_np)
        
        kmeans = KMeans(n_clusters=4, random_state=42)
        clusters = kmeans.fit_predict(X_np)
        
        stores = gnn_sim.stores_data
        # Build stock_by_org for store_risk()
        _adapter = get_adapter(root)
        _stock_by_org = {}
        for s in stores:
            sid = s['store_id']
            org_cd = sid if sid.startswith("ORG") else sid.replace("CFP-", "ORG")
            try:
                _stock_by_org[org_cd] = _adapter.fetch_enriched_products(org_cd) or []
            except Exception:
                _stock_by_org[org_cd] = []
        risk_scores_map = store_risk(_stock_by_org)
        # Map back to gnn_ids (CFP-style keys if needed)
        for s in stores:
            sid = s['store_id']
            org_cd = sid if sid.startswith("ORG") else sid.replace("CFP-", "ORG")
            if sid not in risk_scores_map and org_cd in risk_scores_map:
                risk_scores_map[sid] = risk_scores_map[org_cd]
        
        res = []
        for i, s in enumerate(stores):
            sid = s['store_id']
            risk = risk_scores_map.get(sid, 0.0)
            res.append({
                "Store": sid,
                "Region": s.get('region', 'Unknown'),
                "Cluster": f"Group {clusters[i]}",
                "Risk": round(risk, 2)
            })
            
        return {"clusters": res, "error": None}
    except Exception as e:
        return {"error": str(e)[:200]}


# ── Command Center accessors ─────────────────────────────────────────────
# The Command Center tabs are presentation only. Anything that decides what a
# number MEANS lives here, next to the accessor the Operations view uses, so
# the two native surfaces can never disagree about the same store.

#: severity bands for unit-based days-of-cover, worst first. Shared by the
#: network scan and the per-store health split so one definition of "critical"
#: serves both.
_COVER_BANDS = (
    (0.5, "CRITICAL"),
    (1.0, "URGENT"),
    (3.0, "LOW"),
)


def _cover_severity(qty: float, days_cover: float) -> str:
    if qty < 1.0:
        return "DEPLETED"
    for limit, label in _COVER_BANDS:
        if days_cover < limit:
            return label
    return "HEALTHY"


def network_risk(root: Optional[str] = None) -> Dict[str, Any]:
    """Per-store risk across the whole network — inventory-led, GNN-gated.

    Unlike :func:`transfer_intelligence` this does NOT require a trained GNN:
    ``store_risk`` degrades to the interpretable inventory signal, which is the
    posture the risk-methodology gate demands until the model beats baseline.
    """
    try:
        from oasis.logic.gnn_service import store_risk, model_status
        adapter = get_adapter(root)
        stores = list_stores(root)
        stock_by_org: Dict[str, Any] = {}
        for s in stores:
            try:
                stock_by_org[s["org_cd"]] = adapter.fetch_enriched_products(s["org_cd"]) or []
            except Exception:
                stock_by_org[s["org_cd"]] = []
        risk_map = store_risk(stock_by_org)
        return {"status": model_status(),
                "stores": [{"org_cd": s["org_cd"], "name": s["name"],
                            "risk": float(risk_map.get(s["org_cd"], 0.0) or 0.0)}
                           for s in stores],
                "error": None}
    except Exception as e:
        return {"status": "unavailable", "stores": [], "error": str(e)[:200]}


def network_stockout_risk(root: Optional[str] = None) -> Dict[str, Any]:
    """Item-level items under 3 days of cover, across every store.

    The network-wide twin of :func:`eod_stock_heuristic` (which is one store).
    Returns ``{items, counts, error}`` sorted worst-first.
    """
    try:
        adapter = get_adapter(root)
        stores = list_stores(root)
    except Exception as e:
        return {"items": [], "counts": {}, "error": str(e)[:200]}

    items: List[Dict[str, Any]] = []
    for s in stores:
        try:
            rows = adapter.fetch_enriched_products(s["org_cd"]) or []
        except Exception:
            rows = []
        for r in rows:
            qty = float(r.get("current_stocks", r.get("current_stock", 0)) or 0)
            ads = float(r.get("avg_daily_sales", 0) or 0)
            if ads <= 0:
                continue
            # Round FIRST, then filter: a line at 2.96 days rounds to "3.0"
            # and would otherwise be listed under a heading that says
            # "under 3 days" while displaying 3.0.
            days_cover = round(qty / ads, 1)
            if days_cover >= 3.0:
                continue
            items.append({
                "severity": _cover_severity(qty, days_cover),
                "name": r.get("product_name", "Unknown"),
                "store": s["name"],
                "stock": round(qty, 1),
                "ads": round(ads, 2),
                "days_cover": days_cover,
            })

    order = {"DEPLETED": 0, "CRITICAL": 1, "URGENT": 2, "LOW": 3}
    items.sort(key=lambda x: (order.get(x["severity"], 99), x["days_cover"]))
    counts = {k: sum(1 for i in items if i["severity"] == k) for k in order}
    return {"items": items, "counts": counts, "error": None}


def network_transfer_scan(root: Optional[str] = None) -> Dict[str, Any]:
    """Cross-store transfer opportunities — the console's Live Network scan.

    Runs ``ConsolidatedTransferService.scan_network_opportunities``, the single
    shared implementation the Command Center and Smart Ordering both use. Two
    inputs are what make it honest rather than theoretical:

      * MOQ failures become pull triggers — a line the ordering engine could
        not buy because of a minimum-order quantity is exactly a line worth
        moving stock for instead.
      * REQUESTED / IN_TRANSIT transfers are committed supply. Without them the
        scan happily re-recommends stock that is already on a truck.
    """
    empty = {"store_health": [], "opportunities": [], "totals": {},
             "error": None}
    try:
        from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
        from oasis.logic.moq_failure_store import load_moq_failures

        proj = root or project_root()
        adapter = get_adapter(proj)
        stores = list_stores(proj)
        if len(stores) < 2:
            return dict(empty, error="A single-store install has nothing to "
                                     "transfer between.")

        org_names = {s["org_cd"]: s["name"] for s in stores}
        net_stock = {s["org_cd"]: (adapter.fetch_enriched_products(s["org_cd"]) or [])
                     for s in stores}

        data_dir = os.path.join(proj, "oasis", "data")
        try:
            moq_failures = load_moq_failures(os.path.join(data_dir,
                                                          "moq_failures.json"))
        except Exception:
            moq_failures = {}

        pending: List[dict] = []
        try:
            df = adapter.fetch_transfers(None)
            if hasattr(df, "empty") and not df.empty:
                pending = df.to_dict("records")
        except Exception:
            pass

        distance_map = {}
        coords = os.path.join(proj, "store_coords.json")
        if os.path.exists(coords):
            try:
                import json
                with open(coords, "r", encoding="utf-8") as fh:
                    distance_map = json.load(fh)
            except Exception:
                pass

        cts = ConsolidatedTransferService(
            org_names=org_names,
            stock_data=net_stock,
            registry_path=os.path.join(data_dir, "network_registry.json"),
            distance_map=distance_map,
            cold_node_days=60,
            hot_node_days=14,
        )
        scan = cts.scan_network_opportunities(moq_failures=moq_failures,
                                              pending_transfers=pending)
    except Exception as e:
        return dict(empty, error=str(e)[:200])

    risk = {s["org_cd"]: s["risk"] for s in network_risk(root).get("stores", [])}
    store_health = []
    for s in stores:
        oc = s["org_cd"]
        st_ = scan.store_stats.get(oc, {})
        r = float(risk.get(oc, 0.0))
        store_health.append({
            "store": s["name"], "org_cd": oc,
            "total_skus": st_.get("total_skus", 0),
            "overstock": st_.get("overstock", 0),
            "deficits": st_.get("deficits", 0),
            "push_from": st_.get("push_from", 0),
            "risk": round(r, 3),
            "status": ("High Risk" if r > 0.5
                       else "Moderate" if r > 0.25 else "Stable"),
        })
    store_health.sort(key=lambda x: -x["deficits"])

    opps = [{
        "type": o.type,
        "itm_cd": o.itm_cd,
        "product": o.product_name,
        "from_org": o.from_org, "from": org_names.get(o.from_org, o.from_org),
        "to_org": o.to_org, "to": org_names.get(o.to_org, o.to_org),
        "qty": o.transfer_qty,
        "donor_cover": o.donor_days_cover,
        "recipient_cover": o.recipient_days_cover,
        "donor_excess": o.donor_excess,
        "value": o.value_kes,
        "department": o.department,
        "supplier": o.supplier,
        # Fresh lines are surfaced but never auto-queued: a perishable move is
        # a judgement call about shelf life, not a number.
        "manual_only": bool(o.manual_only),
    } for o in scan.opportunities]

    return {
        "store_health": store_health,
        "opportunities": opps,
        "totals": {
            "stores": len(stores),
            "overstock_skus": sum(s["overstock"] for s in store_health),
            "deficit_skus": sum(s["deficits"] for s in store_health),
            "push_opps": sum(1 for o in opps if o["type"] == "PUSH"),
            "pending_outbound_units": getattr(scan, "pending_outbound_units", 0.0),
            "total_value": sum(o["value"] for o in opps),
            "unique_skus": len({o["itm_cd"] for o in opps}),
            "store_pairs": len({(o["from_org"], o["to_org"]) for o in opps}),
            "manual_only": sum(1 for o in opps if o["manual_only"]),
        },
        "error": None,
    }


def queue_transfers(opportunities: List[Dict[str, Any]], username: str,
                    org_cd: str, limit: int = 50,
                    root: Optional[str] = None) -> Dict[str, Any]:
    """Write transfer requests to the store so managers can dispatch them.

    Fresh lines are skipped, matching the console: ``manual_only`` opportunities
    are shown so a human can judge them, never queued automatically.
    """
    try:
        import time
        from oasis.logic.audit_logger import (log_action, ACTION_TRANSFER_EXECUTED,
                                              ENTITY_TRANSFER)
        proj = root or project_root()
        adapter = get_adapter(proj)
    except Exception as e:
        return {"queued": 0, "skipped": 0, "error": str(e)[:200]}

    queued = skipped = 0
    for o in opportunities[:max(0, int(limit))]:
        if o.get("manual_only"):
            skipped += 1
            continue
        try:
            payload = [{
                "item_code": o["itm_cd"],
                "product_name": o["product"],
                "transfer_qty": o["qty"],
                "transfer_value": o["value"],
                "urgency": "HIGH" if o.get("recipient_cover", 99) <= 1 else "MEDIUM",
            }]
            if adapter.push_transfer_request(o["from_org"], o["to_org"], payload):
                queued += 1
        except Exception:
            skipped += 1

    if queued:
        try:
            log_action(store_db_path(proj), username, ACTION_TRANSFER_EXECUTED,
                       ENTITY_TRANSFER, f"BATCH_{int(time.time())}", org_cd,
                       {"count": queued})
        except Exception:
            pass
    return {"queued": queued, "skipped": skipped,
            "error": None if queued else "No transfers were queued."}


#: statuses a queued transfer can be moved to, in order
TRANSFER_STATUSES = ("IN_TRANSIT", "RECEIVED")


def set_transfer_status(transfer_id: int, status: str, username: str,
                        org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Advance one transfer (REQUESTED → IN_TRANSIT → RECEIVED), audited."""
    if status not in TRANSFER_STATUSES:
        return {"success": False, "error": f"unknown status: {status}"}
    try:
        from oasis.logic.audit_logger import log_action, ENTITY_TRANSFER
        proj = root or project_root()
        adapter = get_adapter(proj)
        if not adapter.update_transfer_status(int(transfer_id), status):
            return {"success": False,
                    "error": "Transfer not found, or the store rejected it."}
        try:
            log_action(store_db_path(proj), username, "TRANSFER_EXECUTED",
                       ENTITY_TRANSFER, f"TX_{transfer_id}", org_cd,
                       {"status": status})
        except Exception:
            pass
        return {"success": True, "error": None}
    except Exception as e:
        return {"success": False, "error": str(e)[:200]}


def transfer_status(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Transfer records for a store: ``{rows, error}`` (rows are plain dicts)."""
    try:
        df = get_adapter(root).fetch_transfers(org_cd)
        rows = df.to_dict("records") if hasattr(df, "to_dict") else list(df or [])
        return {"rows": rows[:200], "error": None}
    except Exception as e:
        return {"rows": [], "error": str(e)[:200]}


#: Stock health bands. These DELIBERATELY differ from ops_dashboard.get_health,
#: which calls a line a stockout under half a day of cover and critical under
#: two. A shelf with stock on it is not a stockout — it is about to be one, and
#: conflating the two costs the operator the distinction they act on. So:
#: STOCKOUT means nothing left, CRITICAL means under a day of cover.
#: The overstock horizons DO match the console.
#: Reviewed and chosen 2026-08-08; the parity test pins the divergence so it is
#: not silently "corrected" back to the console's ladder.
CRITICAL_COVER_DAYS = 1.0
#: A month of cover on fresh milk is spoilage, not depth.
OVERSTOCK_COVER_DAYS_FRESH = 14.0
OVERSTOCK_COVER_DAYS_AMBIENT = 30.0
_FRESH_KEYS = ("MILK", "DAIRY", "FRESH", "MEAT", "BREAD", "BAKERY")

#: what a store with no demand signal looks like — used where cover is infinite
_NO_DEMAND_COVER = 999.0


def is_fresh_line(row: Dict[str, Any]) -> bool:
    """Perishable? Flag first, department keyword second (the console's rule)."""
    dept = str(row.get("department") or row.get("category") or "")
    return bool(row.get("is_fresh", False)) or any(
        k in dept.upper() for k in _FRESH_KEYS)


def classify_health(on_hand: float, days_cover: Optional[float],
                    is_fresh: bool) -> str:
    """STOCKOUT | CRITICAL | HEALTHY | OVERSTOCK for one line (pure).

    ``days_cover`` is ``None`` when the line has no demand signal, in which case
    cover is unbounded rather than zero — a never-sold line is not a stockout.
    """
    if on_hand <= 0:
        return "STOCKOUT"
    if days_cover is None:
        return "HEALTHY"
    limit = (OVERSTOCK_COVER_DAYS_FRESH if is_fresh
             else OVERSTOCK_COVER_DAYS_AMBIENT)
    if days_cover < CRITICAL_COVER_DAYS:
        return "CRITICAL"
    if days_cover > limit:
        return "OVERSTOCK"
    return "HEALTHY"


def stock_health(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Four-way health split for one store, plus the product detail behind it.

    ``items`` carries everything both the table and the stock-vs-demand scatter
    need, so neither surface recomputes cover for itself.
    """
    try:
        rows = get_adapter(root).fetch_enriched_products(org_cd) or []
    except Exception as e:
        return {"counts": {}, "items": [], "error": str(e)[:200]}

    counts = {"HEALTHY": 0, "CRITICAL": 0, "STOCKOUT": 0, "OVERSTOCK": 0}
    items: List[Dict[str, Any]] = []

    for r in rows:
        qty = float(r.get("current_stocks", r.get("current_stock", 0)) or 0)
        ads = float(r.get("avg_daily_sales", 0) or 0)
        days_cover = round(qty / ads, 1) if ads > 0 else None
        fresh = is_fresh_line(r)

        health = classify_health(qty, days_cover, fresh)
        counts[health] += 1
        items.append({
            "health": health,
            "name": r.get("product_name", "Unknown"),
            "dept": str(r.get("department") or r.get("category") or ""),
            "stock": round(qty, 1),
            "ads": round(ads, 2),
            "days_cover": days_cover,
            "has_demand": ads > 0,
            "is_fresh": fresh,
        })

    urgency = {"STOCKOUT": 0, "CRITICAL": 1, "OVERSTOCK": 2, "HEALTHY": 3}
    items.sort(key=lambda x: (urgency[x["health"]],
                              x["days_cover"] if x["days_cover"] is not None
                              else _NO_DEMAND_COVER))
    return {"counts": counts, "items": items, "error": None}


def live_sales(org_cd: str, days: int = 90,
               root: Optional[str] = None) -> Dict[str, Any]:
    """Latest trading day's sales for one store.

    ``fetch_sales_history`` returns LINE ITEMS, not baskets. The line count is
    reported as what it is; basket value is only computed when the feed carries
    a bill identifier we can group on, and is ``None`` otherwise. An earlier
    draft multiplied the line count by a hardcoded 5 to guess a basket size —
    that is a fabricated KPI and must not reach a client.
    """
    try:
        # fetch_sales_history returns a DataFrame — `df or []` is ambiguous and
        # raises, so convert before any truth test touches it.
        raw = get_adapter(root).fetch_sales_history(org_cd, days=days)
        rows = raw.to_dict("records") if hasattr(raw, "to_dict") else list(raw or [])
    except Exception as e:
        return {"error": str(e)[:200], "lines": 0, "revenue": 0.0,
                "units": 0.0, "skus": 0, "baskets": None,
                "basket_value": None, "top": [],
                "trend": [], "alerts": [], "trading_day": None}

    norm = [{str(k).lower(): v for k, v in r.items()} for r in rows]
    if not norm:
        return {"error": None, "lines": 0, "revenue": 0.0, "units": 0.0,
                "skus": 0, "baskets": None, "basket_value": None,
                "top": [], "trend": [], "alerts": [], "trading_day": None}
    if "bill_dt" not in norm[0]:
        return {"error": "sales feed has no bill_dt column", "lines": 0,
                "revenue": 0.0, "units": 0.0, "skus": 0, "baskets": None,
                "basket_value": None,
                "top": [], "trend": [], "alerts": [], "trading_day": None}

    dates = [r["bill_dt"] for r in norm if r.get("bill_dt") is not None]
    if not dates:
        return {"error": None, "lines": 0, "revenue": 0.0, "units": 0.0,
                "skus": 0, "baskets": None, "basket_value": None, "top": [],
                "trend": [], "alerts": [], "trading_day": None}
    latest = max(dates)
    today = [r for r in norm if r.get("bill_dt") == latest]

    def _f(r, k):
        try:
            return float(r.get(k, 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    revenue = sum(_f(r, "net_amt") for r in today)
    units = sum(_f(r, "qty") for r in today)
    skus = len({r.get("itm_cd") for r in today if r.get("itm_cd") is not None})

    bill_key = next((k for k in ("bill_no", "bill_id", "invoice_no", "txn_id")
                     if k in today[0]), None)
    if bill_key:
        baskets = len({r.get(bill_key) for r in today})
        basket_value = (revenue / baskets) if baskets else None
    else:
        baskets, basket_value = None, None

    agg: Dict[Any, Dict[str, Any]] = {}
    for r in today:
        code = r.get("itm_cd")
        if code is None:
            continue
        e = agg.setdefault(code, {"code": code,
                                  "name": r.get("item_name") or str(code),
                                  "units": 0.0, "revenue": 0.0})
        e["units"] += _f(r, "qty")
        e["revenue"] += _f(r, "net_amt")

    # Velocity needs the demand baseline, which lives on the catalogue.
    ads_by_code: Dict[Any, float] = {}
    try:
        for p in (get_adapter(root).fetch_enriched_products(org_cd) or []):
            code = (p.get("item_code") or p.get("itm_cd")
                    or p.get("ITEM_CODE") or p.get("ITM_CD"))
            if code is not None:
                ads_by_code[code] = float(p.get("avg_daily_sales", 0) or 0)
    except Exception:
        pass

    for e in agg.values():
        # Round FIRST, then divide: the ratio must be reproducible from the
        # units and ADS actually shown, or an operator checking the arithmetic
        # on screen gets a different answer than the column claims.
        ads = round(ads_by_code.get(e["code"], 0.0), 2)
        e["ads"] = ads
        # The console computes units / (ads * elapsed_hours/14) against a
        # synthetic intra-day clock. For a COMPLETED trading day elapsed is the
        # whole 14 hours, so the term cancels and the ratio is simply today's
        # units over the daily average — the same number, with nothing invented.
        e["velocity_ratio"] = round(e["units"] / ads, 1) if ads > 0 else None

    top = sorted(agg.values(), key=lambda e: -e["units"])[:20]

    # Day-over-day series (real dates, real totals — no simulated clock).
    per_day: Dict[Any, Dict[str, float]] = {}
    for r in norm:
        d = r.get("bill_dt")
        if d is None:
            continue
        b = per_day.setdefault(d, {"day": d, "revenue": 0.0, "units": 0.0,
                                   "lines": 0})
        b["revenue"] += _f(r, "net_amt")
        b["units"] += _f(r, "qty")
        b["lines"] += 1
    trend = sorted(per_day.values(), key=lambda b: b["day"])

    return {"error": None, "trading_day": latest, "lines": len(today),
            "revenue": revenue, "units": units, "skus": skus,
            "baskets": baskets, "basket_value": basket_value, "top": top,
            "trend": trend,
            "alerts": velocity_alerts(list(agg.values()))}


def _hhi(shares: List[float]) -> float:
    """Herfindahl-Hirschman Index over percentage shares (pure)."""
    return round(sum(s * s for s in shares), 1)


#: HHI bands, the competition-authority convention the console also uses.
HHI_HIGH = 2500.0
HHI_MODERATE = 1500.0


def supplier_concentration(org_cd: str, department: Optional[str] = None,
                           root: Optional[str] = None) -> Dict[str, Any]:
    """Supplier concentration for the store's OWN catalogue.

    The console answers this from ``supplier_analytics.load_scorecard_data``,
    which reads a scorecard CSV that is NOT in the release whitelist — so on a
    client install that tab could only ever raise FileNotFoundError. This reads
    the catalogue the store actually carries, which every install has.

    Share is by revenue potential (on-hand valued at selling price) where a
    price is available, falling back to SKU count.
    """
    try:
        rows = get_adapter(root).fetch_enriched_products(org_cd) or []
    except Exception as e:
        return {"suppliers": [], "hhi": 0.0, "band": "unknown",
                "departments": [], "error": str(e)[:200]}

    depts = sorted({str(r.get("department") or r.get("category") or "").strip()
                    for r in rows} - {""})
    if department:
        rows = [r for r in rows
                if str(r.get("department") or r.get("category") or "").strip()
                == department]

    agg: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        name = (r.get("supplier_name") or r.get("SUPPLIER_NAME")
                or r.get("vendor") or "").strip()
        if not name:
            continue
        e = agg.setdefault(name, {"supplier": name, "skus": 0,
                                  "revenue_potential": 0.0, "on_hand": 0.0})
        e["skus"] += 1
        try:
            qty = float(r.get("current_stocks", r.get("current_stock", 0)) or 0)
            price = float(r.get("selling_price", 0) or 0) or \
                float(r.get("cost_price", r.get("wac", 0)) or 0)
            e["on_hand"] += qty
            e["revenue_potential"] += qty * price
        except (TypeError, ValueError):
            pass

    if not agg:
        return {"suppliers": [], "hhi": 0.0, "band": "unknown",
                "departments": depts, "error": None}

    total_rev = sum(e["revenue_potential"] for e in agg.values())
    total_sku = sum(e["skus"] for e in agg.values())
    by_revenue = total_rev > 0
    for e in agg.values():
        e["share_pct"] = round(
            (e["revenue_potential"] / total_rev * 100) if by_revenue
            else (e["skus"] / total_sku * 100), 1)

    suppliers = sorted(agg.values(), key=lambda e: -e["share_pct"])
    hhi = _hhi([e["share_pct"] for e in suppliers])
    band = ("Highly Concentrated" if hhi > HHI_HIGH
            else "Moderately Concentrated" if hhi > HHI_MODERATE
            else "Unconcentrated (Healthy)")
    return {"suppliers": suppliers, "hhi": hhi, "band": band,
            "basis": "revenue potential" if by_revenue else "SKU count",
            "departments": depts, "error": None}


def supplier_failure_impact(org_cd: str, supplier: str,
                            department: Optional[str] = None,
                            root: Optional[str] = None) -> Dict[str, Any]:
    """What losing one supplier costs this store — from its own catalogue."""
    conc = supplier_concentration(org_cd, department, root)
    if conc.get("error"):
        return {"error": conc["error"], "supplier": supplier}
    match = next((s for s in conc["suppliers"] if s["supplier"] == supplier), None)
    if not match:
        return {"error": f"{supplier} carries nothing in this scope",
                "supplier": supplier}
    share = match["share_pct"]
    severity = ("CRITICAL" if share >= 40 else "HIGH" if share >= 25
                else "MEDIUM" if share >= 10 else "LOW")
    return {"supplier": supplier, "severity": severity,
            "affected_skus": match["skus"],
            "revenue_at_risk": round(match["revenue_potential"], 2),
            "share_pct": share, "error": None}


#: Playbook targets the Executive ROI verdict is judged against.
ROI_DEAD_STOCK_TARGET_PCT = 5.0
ROI_STOCKOUT_TARGET_PCT = 2.0


def weekly_revenue(org_cd: str, days: int = 90,
                   root: Optional[str] = None) -> Dict[str, Any]:
    """Revenue and units bucketed by ISO week — real dates, no simulated clock.

    Shared by the Executive ROI trend and the Analytics tab so the two cannot
    report different weeks for the same store. ``org_cd="ALL"`` aggregates the
    whole network.
    """
    try:
        adapter = get_adapter(root)
        orgs = ([s["org_cd"] for s in list_stores(root)] if org_cd == "ALL"
                else [org_cd])
        rows: List[Dict[str, Any]] = []
        for o in orgs:
            raw = adapter.fetch_sales_history(o, days=days)
            recs = (raw.to_dict("records") if hasattr(raw, "to_dict")
                    else list(raw or []))
            rows.extend({str(k).lower(): v for k, v in r.items()} for r in recs)
    except Exception as e:
        return {"weeks": [], "latest": None, "wow_pct": None, "avg": 0.0,
                "error": str(e)[:200]}

    from datetime import datetime
    buckets: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        raw_d = r.get("bill_dt")
        if not raw_d:
            continue
        try:
            d = datetime.strptime(str(raw_d)[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        iso = d.isocalendar()
        key = f"{iso[0]}-W{iso[1]:02d}"
        b = buckets.setdefault(key, {"week": key, "start": str(d),
                                     "revenue": 0.0, "units": 0.0, "lines": 0})
        b["start"] = min(b["start"], str(d))
        try:
            b["revenue"] += float(r.get("net_amt", 0) or 0)
            b["units"] += float(r.get("qty", 0) or 0)
        except (TypeError, ValueError):
            pass
        b["lines"] += 1

    weeks = sorted(buckets.values(), key=lambda b: b["week"])
    if not weeks:
        return {"weeks": [], "latest": None, "wow_pct": None, "avg": 0.0,
                "error": None}
    latest = weeks[-1]
    wow = None
    if len(weeks) > 1 and weeks[-2]["revenue"]:
        wow = round((latest["revenue"] - weeks[-2]["revenue"])
                    / weeks[-2]["revenue"] * 100, 1)
    avg = sum(w["revenue"] for w in weeks) / len(weeks)
    return {"weeks": weeks, "latest": latest, "wow_pct": wow,
            "avg": round(avg, 2), "error": None}


#: The console's trading day: 06:00–20:00, used as the denominator in its
#: intra-day velocity maths. Kept here so the day-end simplification above is
#: traceable to the number it came from.
TRADING_DAY_HOURS = 14.0
#: A line selling at more than this multiple of its daily average is a spike.
#: Matches ops_dashboard's AlertMonitor(spike_threshold_pct=200.0).
VELOCITY_SPIKE_PCT = 200.0

#: Re-exported from the shared engine so both front doors mean the same thing
#: by construction. See oasis.logic.alert_monitor for why the floors exist.
from oasis.logic.alert_monitor import (VELOCITY_MIN_ADS,  # noqa: E402
                                       VELOCITY_MIN_UNITS, is_alertable)


def velocity_alerts(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Velocity spikes for one completed trading day.

    Runs the SAME engine the console runs — ``AlertMonitor.check_velocity_spikes``
    — so an alert means the same thing in both windows. ``elapsed_hours`` is the
    full trading day because these are settled bills, not a live stream: the
    console's synthetic clock has no counterpart here and is not imitated.
    """
    try:
        from oasis.logic.alert_monitor import AlertMonitor
    except Exception:
        return []

    batch, stats = [], {}
    for e in items:
        if not e.get("ads"):
            continue
        # Slow movers cannot produce a meaningful ratio — see is_alertable.
        if not is_alertable(e["ads"], e["units"]):
            continue
        batch.append({"sku": e["code"], "qty": e["units"]})
        stats[e["code"]] = {"avg_daily_sales": e["ads"],
                            "product_name": e["name"]}
    if not batch:
        return []
    try:
        alerts = AlertMonitor(spike_threshold_pct=VELOCITY_SPIKE_PCT
                              ).check_velocity_spikes(
            batch, stats, elapsed_hours=TRADING_DAY_HOURS)
    except Exception:
        return []
    by_code = {e["code"]: e for e in items}
    for a in alerts:
        src = by_code.get(a.get("product_id"), {})
        a["units"] = src.get("units")
        a["ads"] = src.get("ads")
        a["velocity_ratio"] = src.get("velocity_ratio")
    return sorted(alerts, key=lambda a: -(a.get("velocity_ratio") or 0))
