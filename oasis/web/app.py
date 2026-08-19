"""OASIS Web Console — orders and transfers in a browser, nothing to install.

WHY THIS EXISTS
---------------
OASIS's two strongest outputs — the ordering engine and transfer intelligence —
were reachable only by launching a desktop app or a Streamlit console on the
machine where OASIS is installed. Every insight therefore required someone to
decide to come to it. This puts both behind a URL instead.

It is deliberately NOT the REST bridge (``oasis/api/bridge.py``). That is a
machine-to-machine surface, gated behind the licensed Integrations module, with
``/orders/review`` returning an in-memory list and ``approve`` carrying a TODO
where the ERP write should be. This reads LIVE through the same accessors the
Command Center uses, so there is one implementation of the intelligence and two
front doors — not two implementations.

BINDING, AND WHY IT DEFAULTS TO LOOPBACK
----------------------------------------
This console can push purchase orders into the client's ERP. It has no login,
matching the desktop app it mirrors. Those two facts together mean it must not
be casually exposed: it binds 127.0.0.1 unless ``OASIS_WEB_HOST`` is set
deliberately, so a default start cannot serve a client's order book to their
whole LAN. Same posture as the Tally adapter's remote-host refusal.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from oasis.web import jobs

logger = logging.getLogger("OasisWeb")

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

app = FastAPI(title="OASIS Web Console", docs_url=None, redoc_url=None)


def _root() -> Optional[str]:
    return os.getenv("OASIS_ROOT") or None


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


@app.get("/api/health")
def health() -> Dict[str, Any]:
    """Which backend is answering, and can it be reached at all.

    Reported before anything else because "no recommendations" has four
    completely different causes — dead connection, empty catalogue, no demand,
    no costs — and the page should never make the operator guess which.
    """
    from oasis.desktop import data as D
    from oasis.logic.erp_contract import Unsupported
    backend = (os.getenv("OASIS_ERP") or "pos").strip().lower()
    try:
        adapter = D.get_adapter(_root())
    except Exception as e:
        return {"backend": backend, "connected": False, "error": str(e)[:200]}

    try:
        h = adapter.health_check()
        return {"backend": backend, "connected": bool(h.get("connected", True)),
                "latency_ms": h.get("latency_ms"), "error": h.get("error")}
    except Unsupported:
        # Not every backend can self-report — PosErpAdapter has no health_check
        # and inherits the base stub, which RAISES. Treating that as "not
        # connected" paints the status light red on a perfectly working
        # install, which is the misleading signal this endpoint exists to
        # avoid. Probe with the lightest real read instead.
        pass
    except Exception as e:
        return {"backend": backend, "connected": False, "error": str(e)[:200]}

    try:
        adapter.fetch_all_organizations()
        return {"backend": backend, "connected": True, "latency_ms": None,
                "error": None, "probed": True}
    except Exception as e:
        return {"backend": backend, "connected": False, "error": str(e)[:200],
                "probed": True}


@app.get("/api/sites")
def sites() -> Dict[str, Any]:
    """The stores OASIS can see, and WHY it can see none if that is the case.

    ``list_stores`` swallows every exception and returns [], so an unreachable
    backend and a genuinely empty install are indistinguishable from the
    caller's side. Rendering that as "No stores found" sends the operator to
    check their configuration when the real answer is that the ERP is down.
    An empty result therefore asks health() what happened before answering.
    """
    from oasis.desktop import data as D
    try:
        found = D.list_stores(_root())
    except Exception as e:
        return {"sites": [], "error": str(e)[:200], "reachable": False}
    if found:
        return {"sites": found, "error": None, "reachable": True}
    h = health()
    if not h.get("connected"):
        return {"sites": [], "reachable": False,
                "error": f"Cannot reach the {h.get('backend')} backend: "
                         f"{h.get('error') or 'connection refused'}"}
    return {"sites": [], "reachable": True,
            "error": "Connected, but the backend reports no stores. Check that "
                     "at least one warehouse/location exists and is active."}


def _build_orders(org_cd: str) -> Dict[str, Any]:
    """The real engine run. Called on a job thread, never in a request."""
    from oasis.desktop import data as D
    res = D.generate_smart_orders(org_cd, root=_root())
    if res.get("error"):
        raise RuntimeError(res["error"])
    recs = res.get("po_recs") or res.get("recs") or []
    rows = []
    for r in recs:
        qty = float(r.get("recommended_quantity") or 0)
        cost = float(r.get("cost_price") or 0)
        rows.append({
            "item_code": r.get("item_code"),
            "product": r.get("product_name"),
            "supplier": r.get("supplier_name") or "Unknown",
            "supplier_cd": r.get("supplier_cd"),
            "qty": qty,
            "cost": cost,
            "value": round(qty * cost, 2),
            "stock": float(r.get("current_stocks") or 0),
            "ads": float(r.get("avg_daily_sales") or 0),
            "department": r.get("department"),
            "reasoning": r.get("reasoning") or "",
        })
    rows.sort(key=lambda x: -x["value"])
    by_supplier: Dict[str, float] = {}
    for r in rows:
        by_supplier[r["supplier"]] = by_supplier.get(r["supplier"], 0.0) + r["value"]

    # THE METHODOLOGY, MADE VISIBLE.
    #
    # generate_smart_orders is engine -> network -> MOQ gate, and NetworkPlan
    # documents its own output as "Per-store adjusted orders (original -
    # transfer fulfillments)". So the lines above are already NET of everything
    # a transfer could satisfy: transfers take precedence over buying, which is
    # the whole point of the product.
    #
    # This console was discarding the entire plan and keeping only po_recs, so
    # the one number that proves the pitch — what was moved instead of bought —
    # never reached the screen. Worse, the Transfers tab ran a SEPARATE
    # scan_network_opportunities, so the two tabs were showing different
    # computations of the same idea.
    plan = res.get("network_plan")
    network: Dict[str, Any] = {}
    if plan is not None:
        # TransferRecord (oasis/logic/transfer_state.py) uses qty and cost_kes.
        # scan_network_opportunities emits a DIFFERENT shape with transfer_qty
        # and value_kes, and reading the wrong pair here silently produced a
        # list of moves all showing qty 0 while the plan reported 531 units
        # transferred — the two numbers on screen contradicting each other with
        # no error anywhere.
        moves = []
        for t in (getattr(plan, "transfers", None) or []):
            moves.append({
                "item_code": getattr(t, "itm_cd", ""),
                "product": getattr(t, "product_name", ""),
                "from_org": getattr(t, "from_org", ""),
                "to_org": getattr(t, "to_org", ""),
                "qty": float(getattr(t, "qty", 0) or 0),
                "cost": round(float(getattr(t, "cost_kes", 0) or 0), 2),
                "urgency": getattr(t, "urgency", ""),
                "department": getattr(t, "department", ""),
                # A store cannot transfer to itself. Flagged rather than hidden:
                # see the note in the console's methodology banner.
                "self_transfer": (getattr(t, "from_org", "")
                                  == getattr(t, "to_org", "")),
            })
        donor_adds = sum(len(v) for v in
                         (getattr(plan, "donor_additions", None) or {}).values())
        network = {
            "transfers": moves,
            "items_transferred": int(getattr(plan, "total_items_transferred", 0) or 0),
            "units_transferred": float(getattr(plan, "total_units_transferred", 0) or 0),
            "orders_reduced": int(getattr(plan, "total_orders_reduced", 0) or 0),
            "savings_kes": round(float(getattr(plan, "estimated_savings_kes", 0) or 0), 2),
            "donor_additions": donor_adds,
        }

    return {
        "org_cd": org_cd,
        "rows": rows,
        "network": network,
        "funnel": res.get("funnel") or {},
        "totals": {
            "lines": len(rows),
            "value": round(sum(r["value"] for r in rows), 2),
            "suppliers": len(by_supplier),
            "top_suppliers": sorted(
                ({"name": k, "value": round(v, 2)} for k, v in by_supplier.items()),
                key=lambda x: -x["value"])[:6],
        },
        "error": None,
    }


def _build_transfers() -> Dict[str, Any]:
    """Cross-store transfer opportunities for the whole network.

    Network-wide by nature: a transfer is a relationship between two sites, so
    unlike orders it is not scoped to one.
    """
    from oasis.desktop import data as D
    scan = D.network_transfer_scan(_root())
    if scan.get("error"):
        # A single-store install legitimately has nothing to transfer. That is
        # an explanation, not a failure, so it is returned as data.
        return {"opportunities": [], "store_health": scan.get("store_health") or [],
                "totals": scan.get("totals") or {}, "error": scan["error"]}
    ops = scan.get("opportunities") or []

    # Ask the ADAPTER whether each route can actually be executed, rather than
    # showing a recommendation the writer will refuse. Odoo's two demo
    # warehouses sit in different companies, so the scan proposed four
    # perfectly sensible moves that push_transfer_request correctly rejects —
    # the imbalance was real, the route was not. Asked once per store PAIR,
    # not per line: a hundred lines between the same two stores is one
    # question.
    adapter = D.get_adapter(_root())
    verdicts: Dict[tuple, Dict[str, Any]] = {}
    for o in ops:
        pair = (o.get("from_org"), o.get("to_org"))
        if pair not in verdicts:
            try:
                verdicts[pair] = adapter.can_transfer(*pair)
            except Exception as e:
                verdicts[pair] = {"ok": False, "reason": str(e)[:160]}
        v = verdicts[pair]
        o["executable"] = bool(v.get("ok"))
        o["blocked_reason"] = "" if v.get("ok") else v.get("reason", "")

    blocked = sum(1 for o in ops if not o["executable"])
    totals = dict(scan.get("totals") or {})
    totals["blocked"] = blocked
    totals["executable_value"] = round(
        sum(o.get("value") or 0 for o in ops if o["executable"]), 2)
    return {"opportunities": ops, "store_health": scan.get("store_health") or [],
            "totals": totals, "error": None}


# ── jobs ─────────────────────────────────────────────────────────────────
@app.post("/api/jobs/orders/{org_cd}")
def start_orders(org_cd: str) -> Dict[str, Any]:
    """Kick off an engine run and hand back a job id to poll.

    Single-flight per store, so a reload or a second open tab joins the run in
    progress instead of launching another one.
    """
    job = jobs.submit("orders", org_cd, lambda: _build_orders(org_cd))
    return job.public(with_result=False)


@app.post("/api/jobs/transfers")
def start_transfers() -> Dict[str, Any]:
    job = jobs.submit("transfers", "network", _build_transfers)
    return job.public(with_result=False)


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str) -> Dict[str, Any]:
    job = jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Unknown job — it may have "
                                                    "expired. Run it again.")
    return job.public()


@app.post("/api/orders/{org_cd}/push")
def push(org_cd: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """Write the selected lines to the ERP as DRAFTS.

    The invariant the whole system is built on holds here too: OASIS proposes,
    a human approves inside the ERP. Nothing this endpoint writes commits money.
    """
    from oasis.desktop import data as D
    rows = body.get("rows") or []
    if not rows:
        raise HTTPException(status_code=400, detail="No lines selected.")
    who = str(body.get("username") or "web-console")
    res = D.push_purchase_order(org_cd, who, rows, root=_root())
    if not res.get("success"):
        raise HTTPException(status_code=502,
                            detail=res.get("error") or "Push failed.")
    return {"pushed": res.get("pushed_count", 0), "error": None}


@app.exception_handler(Exception)
async def _unhandled(request, exc):
    # Never leak a traceback to the browser: an adapter error can carry a
    # connection URI with credentials in it.
    logger.error("unhandled %s on %s", type(exc).__name__, request.url.path)
    return JSONResponse(status_code=500,
                        content={"detail": f"{type(exc).__name__} — see server log."})
