"""How is the pilot actually going? Answered from the queue, not from opinion.

A pilot that cannot be measured is a demo. The interesting question is never
"did it produce suggestions" — it always does — but whether the people reading
them ACT on them, and where they consistently decide OASIS is wrong.

Three things this reports, in order of how much they should change your mind:

  ACCEPTANCE. Of the lines an operator actually decided, what share did they
  approve? This is the number that says whether the review is a formality or a
  fight. High acceptance means the engine agrees with people who know the shop.
  Low acceptance is not failure — it is the pilot doing its job, provided you
  read the breakdown and find out WHERE.

  WHERE IT IS OVERRULED. Rejections grouped by decision kind, by perishability,
  by store and by category. A uniform rejection rate is a threshold problem. A
  rate concentrated in one store or one category is a data problem in that
  store or category, and those have completely different fixes.

  WHAT MOVED, AND WHETHER IT HURT. Value completed, plus the donors that ended
  up short afterwards — the one outcome that would make a chain stop trusting
  the queue, and the reason the safety floor exists.

READ-ONLY. Reports on what happened; changes nothing.
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger("OASIS.PilotReport")

M = "oasis.transfer.suggestion"

#: Below this, the review is doing real work and the thresholds want another
#: look before anyone talks about automating anything.
HEALTHY_ACCEPTANCE = 0.60


def _dt(v):
    try:
        return datetime.strptime(str(v)[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def collect(adapter=None, days: int = 30) -> Dict[str, Any]:
    from .odoo_adapter import OdooAdapter
    a = adapter or OdooAdapter()

    rows = a._ex(M, "search_read", [[]],
                 {"fields": ["state", "kind", "is_fresh", "value_kes",
                             "quantity", "from_warehouse_id",
                             "to_warehouse_id", "categ_id", "create_date",
                             "write_date", "computed_on", "product_id"],
                  "limit": 100000}) or []

    out: Dict[str, Any] = {"total": len(rows), "by_state": defaultdict(int)}
    for r in rows:
        out["by_state"][r["state"]] += 1
    out["by_state"] = dict(out["by_state"])

    decided = [r for r in rows if r["state"] in ("approved", "done", "rejected")]
    accepted = [r for r in decided if r["state"] in ("approved", "done")]
    rejected = [r for r in decided if r["state"] == "rejected"]
    out["decided"] = len(decided)
    out["accepted"] = len(accepted)
    out["rejected"] = len(rejected)
    out["acceptance"] = (len(accepted) / len(decided)) if decided else None

    # still waiting: the queue nobody has read is its own signal
    out["awaiting"] = sum(1 for r in rows if r["state"] == "new")

    # decision latency — posted to decided
    lat = []
    for r in decided:
        a0, b0 = _dt(r.get("create_date")), _dt(r.get("write_date"))
        if a0 and b0 and b0 >= a0:
            lat.append((b0 - a0).total_seconds() / 3600.0)
    out["latency_hours_median"] = statistics.median(lat) if lat else None

    # where the engine gets overruled
    def rate(key_fn):
        tot, rej = defaultdict(int), defaultdict(int)
        for r in decided:
            k = key_fn(r)
            tot[k] += 1
            if r["state"] == "rejected":
                rej[k] += 1
        return sorted(((k, rej[k], tot[k], rej[k] / tot[k]) for k in tot
                       if tot[k] >= 3), key=lambda x: -x[3])

    out["by_kind"] = rate(lambda r: r["kind"] or "?")
    out["by_fresh"] = rate(lambda r: "perishable" if r["is_fresh"] else "dry")
    out["by_store"] = rate(lambda r: (r["to_warehouse_id"] or [0, "?"])[1][:28])
    out["by_categ"] = rate(lambda r: (r["categ_id"] or [0, "?"])[1][:28])

    done = [r for r in rows if r["state"] == "done"]
    out["completed"] = len(done)
    out["value_completed"] = sum(r.get("value_kes") or 0 for r in done)
    out["units_completed"] = sum(r.get("quantity") or 0 for r in done)
    out["value_awaiting"] = sum(r.get("value_kes") or 0
                                for r in rows if r["state"] == "new")

    # DID IT HURT? donors that ended up short after giving
    out["donors_short"] = _donors_left_short(a, done)
    return out


def _donors_left_short(a, done_rows) -> List[dict]:
    """Donors now below their own safety floor, having shipped.

    The one outcome that would make a chain stop trusting the queue. It should
    be empty: the release cap exists to prevent exactly this, and the invariant
    harness measures zero on the depot. Measuring it against the customer's own
    stock is how you find out whether that holds on real data.
    """
    from .consolidated_transfer_service import ConsolidatedTransferService as CTS
    if not done_rows:
        return []
    # RESOLVE THE WAREHOUSE CODE, do not parse it out of a display label.
    #
    # The first version took from_warehouse_id's label, truncated it to 28
    # characters for the report, and then split on "(" to recover the code —
    # so "Chandarana Diamond Plaza (CFP-009)" became "CF", the site lookup
    # missed, and the adapter fell back to COMPANY-WIDE stock. The floor was
    # then compared against the wrong scope and the scorecard raised a donor
    # alarm that was not real. A false alarm here is worse than none: it is
    # the one number that would make a chain stop trusting the queue.
    wh_ids = {r["from_warehouse_id"][0] for r in done_rows
              if r.get("from_warehouse_id")}
    code_of = {}
    if wh_ids:
        for w in (a._ex("stock.warehouse", "read",
                        [list(wh_ids), ["code", "name"]]) or []):
            code_of[w["id"]] = ((w.get("code") or "").strip(), w.get("name") or "")

    codes = {}
    for r in done_rows:
        wh = r.get("from_warehouse_id")
        if wh and wh[0] in code_of and code_of[wh[0]][0]:
            codes.setdefault(code_of[wh[0]], []).append(r)
    short = []
    try:
        svc = CTS(org_names={}, stock_data={})
        for (org, label), rs in codes.items():
            try:
                prods = {p["item_code"]: p for p in a.fetch_enriched_products(org)}
            except Exception:
                continue
            for r in rs:
                code = (r.get("product_id") or [0, ""])[1]
                code = code.split("] ")[0].lstrip("[") if "] " in code else code
                p = prods.get(code)
                if not p:
                    continue
                ads = float(p.get("avg_daily_sales") or 0)
                oh = float(p.get("current_stocks") or 0)
                if ads <= 0:
                    continue
                floor = ads * svc._safety_days(org, p)
                if oh < floor:
                    short.append({"store": f"{label[:22]} ({org})", "sku": code,
                                  "on_hand": oh, "floor": round(floor, 1)})
    except Exception as e:
        logger.debug("donor check unavailable: %s", e)
    return short


def format_report(r: Dict[str, Any]) -> str:
    """ASCII only — this prints to a customer's Windows console."""
    w = ["", "O.A.S.I.S. - transfer pilot scorecard", "=" * 66]
    st = r.get("by_state", {})
    w.append(f"  suggestions posted        {r.get('total', 0):>8,}")
    w.append(f"    awaiting review         {st.get('new', 0):>8,}")
    w.append(f"    approved (in flight)    {st.get('approved', 0):>8,}")
    w.append(f"    completed (received)    {st.get('done', 0):>8,}")
    w.append(f"    rejected                {st.get('rejected', 0):>8,}")
    w.append("-" * 66)

    acc = r.get("acceptance")
    if acc is None:
        w.append("  ACCEPTANCE   no decisions yet — nobody has worked the queue.")
        w.append("               That is the first thing to fix; a queue nobody")
        w.append("               reads measures nothing.")
    else:
        verdict = ("the engine agrees with the people who know the shop"
                   if acc >= HEALTHY_ACCEPTANCE else
                   "the review is doing real work — read the breakdown below")
        w.append(f"  ACCEPTANCE   {acc:.0%}  of {r['decided']:,} decided "
                 f"({r['accepted']:,} approved, {r['rejected']:,} rejected)")
        w.append(f"               {verdict}")
    lat = r.get("latency_hours_median")
    if lat is not None:
        w.append(f"  DECIDED IN   {lat:.1f} h median from posting")
    w.append("-" * 66)

    w.append(f"  MOVED        {r.get('units_completed', 0):,.0f} units, "
             f"KES {r.get('value_completed', 0):,.0f} across "
             f"{r.get('completed', 0):,} completed transfers")
    w.append(f"  PENDING      KES {r.get('value_awaiting', 0):,.0f} still "
             f"sitting in the queue")

    shorts = r.get("donors_short") or []
    if shorts:
        w.append("")
        w.append(f"  !! {len(shorts)} DONOR(S) NOW BELOW THEIR OWN SAFETY FLOOR")
        w.append("     This is the outcome the release cap exists to prevent.")
        for s in shorts[:5]:
            w.append(f"     {s['store'][:26]:<26} {s['sku'][:22]:<22} "
                     f"on hand {s['on_hand']:.0f} vs floor {s['floor']}")
    else:
        w.append("  DONORS       none left below their own safety floor")

    for label, rows in (("decision", r.get("by_kind")),
                        ("perishability", r.get("by_fresh")),
                        ("receiving store", r.get("by_store")),
                        ("category", r.get("by_categ"))):
        rows = [x for x in (rows or []) if x[1]]
        if not rows:
            continue
        w.append("")
        w.append(f"  WHERE IT IS OVERRULED - by {label}")
        for k, rej, tot, pct in rows[:5]:
            w.append(f"     {str(k)[:30]:<30} {rej:>4} of {tot:<4} rejected "
                     f"({pct:.0%})")
    w.append("")
    return "\n".join(w)
