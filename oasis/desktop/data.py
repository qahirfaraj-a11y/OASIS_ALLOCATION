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
            days_cover = qty / ads
            if days_cover >= 3.0:
                continue
            items.append({
                "severity": _cover_severity(qty, days_cover),
                "name": r.get("product_name", "Unknown"),
                "store": s["name"],
                "stock": round(qty, 1),
                "ads": round(ads, 2),
                "days_cover": round(days_cover, 1),
            })

    order = {"DEPLETED": 0, "CRITICAL": 1, "URGENT": 2, "LOW": 3}
    items.sort(key=lambda x: (order.get(x["severity"], 99), x["days_cover"]))
    counts = {k: sum(1 for i in items if i["severity"] == k) for k in order}
    return {"items": items, "counts": counts, "error": None}


def transfer_status(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Transfer records for a store: ``{rows, error}`` (rows are plain dicts)."""
    try:
        df = get_adapter(root).fetch_transfers(org_cd)
        rows = df.to_dict("records") if hasattr(df, "to_dict") else list(df or [])
        return {"rows": rows[:200], "error": None}
    except Exception as e:
        return {"rows": [], "error": str(e)[:200]}


def stock_health(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    """Four-way health split for one store, plus the product detail behind it.

    Overstock is judged against a shorter horizon for perishables — 14 days
    against 30 — because a month of cover on fresh milk is spoilage, not depth.
    """
    try:
        rows = get_adapter(root).fetch_enriched_products(org_cd) or []
    except Exception as e:
        return {"counts": {}, "items": [], "error": str(e)[:200]}

    fresh_keys = ("MILK", "DAIRY", "FRESH", "MEAT", "BREAD", "BAKERY")
    counts = {"HEALTHY": 0, "CRITICAL": 0, "STOCKOUT": 0, "OVERSTOCK": 0}
    items: List[Dict[str, Any]] = []

    for r in rows:
        name = r.get("product_name", "Unknown")
        dept = str(r.get("department") or r.get("category") or "")
        qty = float(r.get("current_stocks", r.get("current_stock", 0)) or 0)
        ads = float(r.get("avg_daily_sales", 0) or 0)
        days_cover = (qty / ads) if ads > 0 else float("inf")

        is_fresh = bool(r.get("is_fresh", False)) or any(
            k in dept.upper() for k in fresh_keys)
        overstock_limit = 14.0 if is_fresh else 30.0

        if qty <= 0:
            health = "STOCKOUT"
        elif ads > 0 and days_cover < 1.0:
            health = "CRITICAL"
        elif ads > 0 and days_cover > overstock_limit:
            health = "OVERSTOCK"
        else:
            health = "HEALTHY"

        counts[health] += 1
        items.append({
            "health": health, "name": name, "dept": dept,
            "stock": round(qty, 1), "ads": round(ads, 2),
            "days_cover": (None if days_cover == float("inf")
                           else round(days_cover, 1)),
            "is_fresh": is_fresh,
        })

    urgency = {"STOCKOUT": 0, "CRITICAL": 1, "OVERSTOCK": 2, "HEALTHY": 3}
    items.sort(key=lambda x: (urgency[x["health"]],
                              x["days_cover"] if x["days_cover"] is not None else 1e9))
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
                "trading_day": None}

    norm = [{str(k).lower(): v for k, v in r.items()} for r in rows]
    if not norm:
        return {"error": None, "lines": 0, "revenue": 0.0, "units": 0.0,
                "skus": 0, "baskets": None, "basket_value": None,
                "top": [], "trading_day": None}
    if "bill_dt" not in norm[0]:
        return {"error": "sales feed has no bill_dt column", "lines": 0,
                "revenue": 0.0, "units": 0.0, "skus": 0, "baskets": None,
                "basket_value": None,
                "top": [], "trading_day": None}

    dates = [r["bill_dt"] for r in norm if r.get("bill_dt") is not None]
    if not dates:
        return {"error": None, "lines": 0, "revenue": 0.0, "units": 0.0,
                "skus": 0, "baskets": None, "basket_value": None, "top": [],
                "trading_day": None}
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
        e = agg.setdefault(code, {"name": r.get("item_name") or str(code),
                                  "units": 0.0, "revenue": 0.0})
        e["units"] += _f(r, "qty")
        e["revenue"] += _f(r, "net_amt")
    top = sorted(agg.values(), key=lambda e: -e["units"])[:20]

    return {"error": None, "trading_day": latest, "lines": len(today),
            "revenue": revenue, "units": units, "skus": skus,
            "baskets": baskets, "basket_value": basket_value, "top": top}
