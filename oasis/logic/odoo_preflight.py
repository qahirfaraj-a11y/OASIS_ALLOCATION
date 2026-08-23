"""Is this Odoo safe to run OASIS Transfers against? Answered WITHOUT writing.

`erp-status` answers "what can OASIS see in the catalogue". This answers a
different question, and the one that decides whether a pilot is safe: does this
instance's SHAPE and SCALE fit the assumptions the connector was built on?

Every finding here is one that would otherwise be discovered by a customer:

  * **Caps.** Every read is capped. Under the cap the numbers are right; over
    it they are quietly wrong, and the two that matter answer "what is already
    coming" — truncate those and the scan re-proposes stock already in flight,
    so approving both ships twice. The depot has 14 stores; a real chain does
    not.
  * **Companies.** Odoo cannot confirm an internal transfer between two
    companies — that movement is a sale and a purchase. Approval refuses it,
    but finding that out at the pilot is late.
  * **Warehouse codes.** OASIS keys stores on `code`. A warehouse without one
    falls back to its database id, which no operator recognises.
  * **Read time.** The scan reads sites one after another while tills sell, so
    the plan is a composite of N instants. Fine for a human-reviewed
    suggestion, and the span is what decides whether it stays fine.
  * **Receipt attribution.** Per-store delivery cadence needs receipts that
    name a supplier. Without it sigma stays supplier-level.

STRICTLY READ-ONLY. Only `search_count` and `search_read` are used — no create,
no write, no unlink, and nothing is posted to any queue. Safe to run against
production.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger("OdooPreflight")

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"

#: A read this close to its cap is treated as already a problem: the customer's
#: data grows, and the failure is silent when it arrives.
_NEAR_CAP = 0.80


def _check(results: List[dict], level: str, label: str, detail: str = "") -> None:
    results.append({"level": level, "label": label, "detail": detail})


def _count(adapter, model: str, domain: list) -> int:
    try:
        return int(adapter._ex(model, "search_count", [domain]) or 0)
    except Exception as e:                       # a missing model is a finding
        logger.debug("count failed on %s: %s", model, e)
        return -1


def _site_root(adapter, warehouse: dict):
    """The warehouse's view location — what the adapter scopes its reads to."""
    try:
        rec = adapter._ex("stock.warehouse", "read",
                          [[warehouse["id"]], ["view_location_id"]]) or []
        v = rec[0].get("view_location_id") if rec else None
        return v[0] if isinstance(v, (list, tuple)) and v else None
    except Exception:
        return None

def run_preflight(adapter=None, org_cd: str = None) -> Dict[str, Any]:
    """Read-only readiness report for running Transfers against this Odoo."""
    from .odoo_adapter import OdooAdapter

    a = adapter or OdooAdapter()
    out: Dict[str, Any] = {"checks": [], "scale": {}, "url": a.url, "db": a.db}
    checks = out["checks"]

    # ── connection ───────────────────────────────────────────────────────
    health = a.health_check()
    out["connected"] = bool(health.get("connected"))
    if not out["connected"]:
        _check(checks, FAIL, "connection", str(health.get("error"))[:160])
        out["overall"] = FAIL
        return out
    _check(checks, PASS, "connection",
           f"{a.url} db={a.db} ({health.get('latency_ms')}ms)")

    # ── companies ────────────────────────────────────────────────────────
    companies = a._ex("res.company", "search_read", [[]], {"fields": ["name"]}) or []
    out["scale"]["companies"] = len(companies)
    if len(companies) > 1:
        _check(checks, WARN, "companies",
               f"{len(companies)} companies. Odoo cannot confirm an internal "
               f"transfer between two of them — approval refuses those routes, "
               f"so stores in different companies never exchange stock.")
    else:
        _check(checks, PASS, "companies", "single company — every route is legal")

    # ── warehouses and their codes ───────────────────────────────────────
    whs = a._ex("stock.warehouse", "search_read", [[]],
                {"fields": ["name", "code", "company_id"]}) or []
    out["scale"]["warehouses"] = len(whs)
    missing = [w["name"] for w in whs if not (w.get("code") or "").strip()]
    codes = [(w.get("code") or "").strip() for w in whs if (w.get("code") or "").strip()]
    dupes = sorted({c for c in codes if codes.count(c) > 1})
    if len(whs) < 2:
        _check(checks, FAIL, "warehouses",
               f"{len(whs)} warehouse — transfers need at least two sites")
    elif missing:
        _check(checks, WARN, "warehouse codes",
               f"{len(missing)} without a code, so OASIS keys them on database "
               f"id: {', '.join(missing[:3])}")
    elif dupes:
        _check(checks, FAIL, "warehouse codes",
               f"duplicate codes {dupes} — two sites would collapse into one org")
    else:
        _check(checks, PASS, "warehouses",
               f"{len(whs)} sites, all coded: {', '.join(sorted(codes)[:6])}"
               + (" …" if len(codes) > 6 else ""))

    # ── scale against every cap ──────────────────────────────────────────
    #
    # COUNT THE WAY THE ADAPTER ACTUALLY READS. Two of these reads are scoped
    # to ONE SITE (`location_dest_id child_of` the warehouse), so counting them
    # company-wide compares a whole chain's rows against a per-site cap and
    # cries FAIL on a perfectly healthy instance. Measured on the depot: 28,125
    # receipts company-wide against a 20,000 cap looks like a breach, while the
    # busiest single site holds ~2,600. A preflight that fails a healthy
    # instance is worse than no preflight — people learn to ignore it.
    #
    # So the site-scoped reads are counted per warehouse and judged on the
    # WORST site, which is the one that would truncate first.
    scopes = [(w, _site_root(a, w)) for w in whs]

    def _worst_site(model, base_dom, field):
        worst, where = 0, None
        for w, root in scopes:
            if not root:
                continue
            n = _count(a, model, base_dom + [[field, "child_of", root]])
            if n > worst:
                worst, where = n, (w.get("code") or w.get("name"))
        return worst, where

    caps = [
        ("product catalogue", "product.product",
         [["active", "=", True], ["type", "in", ["product", "consu"]]],
         a.PRODUCT_READ_LIMIT,
         "products past the cap are invisible to ordering AND transfers"),
        ("incoming moves (busiest site)", None,
         ("stock.move",
          [["state", "=", "done"],
           ["location_id.usage", "in", ["supplier", "inventory", "production"]]],
          "location_dest_id"),
         a.RECEIPT_READ_LIMIT,
         "receipt ages fall back to a lower bound, weakening the dead-stock guard"),
        # PAGED reads carry no cap to breach — see OdooAdapter._read_paged.
        # They are still counted, because volume drives scan TIME even when it
        # no longer threatens accuracy, but reporting them against a limit that
        # no longer binds would be a warning about nothing. A preflight that
        # cries wolf is worse than one that stays quiet.
        ("customer moves (busiest site)", None,
         ("stock.move",
          [["state", "=", "done"], ["location_dest_id.usage", "=", "customer"]],
          "location_id"),
         None,
         "ADS derives from this; it is paged, so volume costs time not accuracy."),
        ("supplier info", "product.supplierinfo", [],
         a.SUPPLIERINFO_READ_LIMIT,
         "products fall back to a default lead time instead of LATA's rhythm"),
        ("open internal transfers", "stock.picking",
         [["picking_type_id.code", "=", "internal"],
          ["state", "not in", ["done", "cancel"]]],
         None,
         "paged — stock in flight is read in full."),
        ("open purchase order lines", "purchase.order.line",
         [["order_id.state", "in", ["purchase", "done"]]],
         None,
         "paged — inbound stock is read in full."),
    ]
    for label, model, dom, cap, consequence in caps:
        if model is None:                       # site-scoped: judge the worst
            n, where = _worst_site(*dom)
            label = f"{label} [{where}]" if where else label
        else:
            n = _count(a, model, dom)
        out["scale"][label] = n
        if n < 0:
            _check(checks, WARN, label, "could not be counted on this instance")
        elif cap is None:
            _check(checks, PASS, f"{label} (paged)",
                   f"{n:,} rows, read in pages — no cap. {consequence}")
        elif n >= cap:
            _check(checks, FAIL, f"{label} EXCEEDS its cap",
                   f"{n:,} rows vs a {cap:,} cap — {consequence}")
        elif n >= cap * _NEAR_CAP:
            _check(checks, WARN, f"{label} near its cap",
                   f"{n:,} of {cap:,} ({n / cap:.0%}) — {consequence}")
        else:
            _check(checks, PASS, label, f"{n:,} rows, cap {cap:,}")

    # ── how long a full scan would read for ──────────────────────────────
    sites = [(w.get("code") or "").strip() or str(w["id"]) for w in whs]
    probe = org_cd or (sites[0] if sites else None)
    if probe:
        t0 = datetime.now()
        try:
            rows = a.fetch_enriched_products(probe)
            per_site = (datetime.now() - t0).total_seconds()
            out["scale"]["rows_per_site"] = len(rows)
            projected = per_site * max(1, len(sites))
            out["scale"]["projected_scan_seconds"] = round(projected, 1)
            level = PASS if projected < 120 else WARN
            _check(checks, level, "projected scan time (upper bound)",
                   f"{per_site:.1f}s for {probe} ({len(rows):,} rows) -> at "
                   f"most {projected:.0f}s for {len(sites)} sites. The first "
                   f"read pays one-off costs a real scan amortises, so the "
                   f"true figure is lower"
                   + ("" if level == PASS else
                      ". Over two minutes the first site read and the last "
                      "describe meaningfully different stock"))
        except Exception as e:
            _check(checks, FAIL, "site read", str(e)[:160])

    # ── can per-store cadence be derived at all? ─────────────────────────
    linked = _count(a, "stock.picking",
                    [["picking_type_id.code", "=", "incoming"],
                     ["partner_id", "!=", False]])
    out["scale"]["receipts_with_supplier"] = linked
    if linked <= 0:
        _check(checks, WARN, "receipt attribution",
               "no incoming pickings name a supplier, so delivery cadence "
               "stays supplier-level — sigma cannot be derived per store")
    else:
        _check(checks, PASS, "receipt attribution",
               f"{linked:,} incoming pickings name a supplier — per-store "
               f"cadence is derivable from this instance")

    levels = [c["level"] for c in checks]
    out["overall"] = FAIL if FAIL in levels else (WARN if WARN in levels else PASS)
    return out


def format_report(r: Dict[str, Any]) -> str:
    """ASCII deliberately — this prints to a customer's Windows console."""
    w = []
    w.append("")
    w.append("O.A.S.I.S. - Odoo transfer readiness (READ ONLY, nothing written)")
    w.append("=" * 66)
    w.append(f"  endpoint  {r.get('url')}  db={r.get('db')}")
    w.append("-" * 66)
    icon = {PASS: "[ ok ]", WARN: "[warn]", FAIL: "[FAIL]"}
    for c in r.get("checks", []):
        w.append(f"  {icon[c['level']]} {c['label']}")
        if c["detail"]:
            for line in _wrap(c["detail"], 58):
                w.append(f"         {line}")
    w.append("-" * 66)
    verdict = {
        PASS: "READY - a supervised pilot can proceed.",
        WARN: "READY WITH CAVEATS - read the warnings before piloting.",
        FAIL: "NOT READY - the failures above would produce wrong numbers "
              "or refused transfers.",
    }[r.get("overall", FAIL)]
    w.append(f"  {r.get('overall')}: {verdict}")
    w.append("")
    return "\n".join(w)


def _wrap(text: str, width: int) -> List[str]:
    words, line, out = text.split(), "", []
    for word in words:
        if len(line) + len(word) + 1 > width:
            out.append(line)
            line = word
        else:
            line = f"{line} {word}".strip()
    if line:
        out.append(line)
    return out
