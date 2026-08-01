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
