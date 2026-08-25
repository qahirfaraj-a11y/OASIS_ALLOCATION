"""Was each delivery sized for the interval it actually had to serve?

WHY THIS EXISTS
---------------
Every horizon in the ordering engine is an estimate of "how long until the next
delivery", and every attempt to check one estimate against another has been
circular: the observed order interval is contaminated by our own past ordering,
so validating a horizon against it proves nothing. The only measurement that
cannot be contaminated is the one taken AFTERWARDS, against what happened.

So: for each delivery of a line, how many days of cover did it carry, and how
many days did it actually have to last before the next delivery arrived?

    residual = cover_delivered - interval_served

Consistently positive and the horizon is too deep — stock was still on the
shelf when the next lorry came, and the difference is working capital doing
nothing. Consistently negative and the horizon is too short — the line was
leaning on whatever happened to be in the back already.

WHAT THIS DELIBERATELY DOES NOT CLAIM
-------------------------------------
It ignores opening stock, because receipt history does not carry it. So a
negative residual does NOT prove a stockout — it proves the delivery alone did
not span the interval. That is still exactly what is needed to judge a
HORIZON: if deliveries are consistently sized for forty days and arrive every
fifteen, the horizon is too deep whatever was on the shelf beforehand.

Naming it a stockout ledger would be a lie; `hub_stock_movement` is where a
real one belongs.

READ-ONLY. Computes; changes nothing.
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("OASIS.ResidualCover")

#: Below this a line does not sell enough for days-of-cover to mean anything.
#: One unit a fortnight. Under it, a single pack is months of "cover" and the
#: arithmetic describes the pack size, not the buying decision.
MIN_ADS_FOR_COVER = 0.07

#: An interval longer than this is a delisting or a data gap, not a rhythm —
#: the same ceiling the rhythm derivation applies, for the same reason.
MAX_INTERVAL_DAYS = 180

#: Over-cover beyond this is worth reporting as capital, not noise.
OVER_COVER_DAYS = 14.0


def residual_days(qty: float, ads: float, interval_days: float) -> Optional[float]:
    """Days of cover left over when the next delivery landed.

    None where the line does not sell enough for cover to be meaningful —
    never 0.0, which would read as "ran out exactly on time" and poison every
    average it touched. The same discipline the 999 sentinel taught.
    """
    if ads is None or ads < MIN_ADS_FOR_COVER:
        return None
    if interval_days is None or interval_days <= 0:
        return None
    return (float(qty) / float(ads)) - float(interval_days)


def score_intervals(events: Sequence[Tuple[Any, float]],
                    ads: float) -> List[Dict[str, Any]]:
    """One row per interval between consecutive deliveries of a line.

    The final delivery is skipped: it has no successor yet, so nothing is known
    about how long it had to last. Counting it as a zero-length interval would
    report every line as massively over-delivered.
    """
    rows: List[Dict[str, Any]] = []
    ordered = sorted(events, key=lambda e: e[0])
    for i in range(len(ordered) - 1):
        (d0, qty), (d1, _) = ordered[i], ordered[i + 1]
        interval = (d1 - d0).days
        if interval <= 0 or interval > MAX_INTERVAL_DAYS:
            continue
        r = residual_days(qty, ads, interval)
        if r is None:
            continue
        rows.append({
            "from": d0, "to": d1, "interval_days": interval,
            "qty": float(qty), "ads": float(ads),
            "cover_delivered": float(qty) / float(ads),
            "residual_days": r,
            "ratio": (float(qty) / float(ads)) / interval,
        })
    return rows


def summarise(rows: Sequence[Dict[str, Any]],
              unit_cost: float = 0.0) -> Dict[str, Any]:
    """What the intervals say, in aggregate."""
    if not rows:
        return {"intervals": 0}
    res = [r["residual_days"] for r in rows]
    ratios = [r["ratio"] for r in rows]
    over = [r for r in rows if r["residual_days"] > OVER_COVER_DAYS]
    short = [r for r in rows if r["residual_days"] < 0]
    out = {
        "intervals": len(rows),
        "median_residual_days": statistics.median(res),
        "median_ratio": statistics.median(ratios),
        "over_covered": len(over),
        "ran_short": len(short),
        "median_interval_days": statistics.median([r["interval_days"] for r in rows]),
    }
    if unit_cost:
        # THE TYPICAL leftover, not the sum of every leftover.
        #
        # Summing across intervals prices the same shelf over and over: a line
        # resupplied twenty times in a year reported twenty times its idle
        # stock, so the headline scaled with how OFTEN a line is delivered
        # rather than how much sits still. On the real book that inflated the
        # figure to KES 72M. What an operator wants is the stock typically
        # sitting there at the moment of resupply, which is the median.
        leftovers = [max(0.0, r["residual_days"]) * r["ads"] * unit_cost
                     for r in rows]
        out["idle_capital"] = statistics.median(leftovers)
    return out


#: A ceiling has to be judged against the gap it is covering. Fifteen days of
#: leftover is catastrophic on a 4-day cycle and unremarkable on a 60-day one,
#: so the verdict is a RATIO of cover delivered to interval served.
RATIO_ABOUT_RIGHT = 1.35
RATIO_TOO_DEEP = 1.75
RATIO_TOO_SHORT = 0.9


def verdict(median_residual: Optional[float],
            median_interval: Optional[float]) -> str:
    """One line an operator can act on.

    Judged on the RATIO, not on absolute days. An absolute threshold called a
    13-day residual on a 15-day interval "about right" — when it means every
    delivery carried nearly twice the gap it had to span.
    """
    if median_residual is None or median_interval is None or not median_interval:
        return "not enough movement to judge"
    ratio = (median_residual + median_interval) / median_interval
    if ratio >= 2.0:
        return (f"every delivery carries {ratio:.1f}x the gap it has to span — "
                f"the horizon is far too deep")
    if ratio >= RATIO_TOO_DEEP:
        return (f"deliveries carry {ratio:.1f}x the interval they serve, so "
                f"stock is still on the shelf when the next one lands — the "
                f"horizon is too deep")
    if ratio < RATIO_TOO_SHORT:
        return (f"deliveries carry only {ratio:.1f}x the interval — they lean "
                f"on stock already in the back, or the line runs dry")
    return (f"deliveries carry {ratio:.1f}x the interval they serve — "
            f"about right")


def collect_from_receipts(receipts: Dict[str, List[Tuple[Any, float]]],
                          ads_of: Dict[str, float],
                          cost_of: Optional[Dict[str, float]] = None,
                          supplier_of: Optional[Dict[str, str]] = None
                          ) -> Dict[str, Any]:
    """Score every line, then roll up by supplier.

    `receipts` maps a line's code to its (date, quantity) deliveries. Keeping
    the reader outside this function is deliberate: the same scoring has to
    serve a customer's Odoo and an offline book extract without either being
    the "real" one.
    """
    cost_of = cost_of or {}
    supplier_of = supplier_of or {}
    per_line, by_supplier = {}, defaultdict(list)
    skipped_no_ads = skipped_thin = 0

    for code, events in receipts.items():
        ads = ads_of.get(code)
        if ads is None:
            skipped_no_ads += 1
            continue
        rows = score_intervals(events, ads)
        if not rows:
            skipped_thin += 1
            continue
        s = summarise(rows, cost_of.get(code, 0.0))
        s["code"] = code
        s["supplier"] = supplier_of.get(code, "")
        per_line[code] = s
        by_supplier[s["supplier"]].append(s)

    sup = {}
    for name, lines in by_supplier.items():
        if not lines:
            continue
        sup[name] = {
            "lines": len(lines),
            "median_residual_days": statistics.median(
                [x["median_residual_days"] for x in lines]),
            "median_interval_days": statistics.median(
                [x["median_interval_days"] for x in lines]),
            "idle_capital": sum(x.get("idle_capital", 0.0) for x in lines),
        }

    all_res = [x["median_residual_days"] for x in per_line.values()]
    return {
        "lines_scored": len(per_line),
        "skipped_no_sales_rate": skipped_no_ads,
        "skipped_too_few_deliveries": skipped_thin,
        "median_residual_days": statistics.median(all_res) if all_res else None,
        "median_interval_days": statistics.median(
            [x["median_interval_days"] for x in per_line.values()]) if per_line else None,
        "idle_capital": sum(x.get("idle_capital", 0.0) for x in per_line.values()),
        "per_line": per_line,
        "by_supplier": sup,
    }


def read_from_odoo(adapter=None, org_cd: str = None, days: int = 730
                   ) -> Dict[str, Any]:
    """Receipt history per line, and each line's sales rate, from Odoo."""
    from .odoo_adapter import OdooAdapter
    from datetime import datetime, timedelta

    a = adapter or OdooAdapter()
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    dom = [["picking_id.picking_type_id.code", "=", "incoming"],
           ["state", "=", "done"], ["date", ">=", since]]
    moves = a._ex("stock.move", "search_read", [dom],
                  {"fields": ["product_id", "quantity_done", "date"],
                   "order": "date asc", "limit": 200000}) or []
    logger.info("read %s completed incoming move lines", f"{len(moves):,}")

    prods = a.fetch_enriched_products(org_cd) or []
    ads_of = {p.get("item_code"): float(p.get("avg_daily_sales") or 0)
              for p in prods}
    cost_of = {p.get("item_code"): float(p.get("cost_price") or 0) for p in prods}
    supplier_of = {p.get("item_code"): (p.get("supplier_name") or "")
                   for p in prods}

    # product id -> the code the engine knows it by
    ids = list({(m.get("product_id") or [0])[0] for m in moves if m.get("product_id")})
    code_of = {}
    for i in range(0, len(ids), 2000):
        for r in (a._ex("product.product", "read",
                        [ids[i:i + 2000], ["default_code"]]) or []):
            if r.get("default_code"):
                code_of[r["id"]] = r["default_code"]

    receipts = defaultdict(list)
    for m in moves:
        pid = (m.get("product_id") or [0])[0]
        code = code_of.get(pid)
        qty = float(m.get("quantity_done") or 0)
        if not code or qty <= 0:
            continue
        try:
            d = datetime.strptime(str(m["date"])[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue
        receipts[code].append((d, qty))

    return collect_from_receipts(receipts, ads_of, cost_of, supplier_of)


def format_report(r: Dict[str, Any], top: int = 10) -> str:
    """ASCII only — this prints to a customer's Windows console."""
    w = ["", "O.A.S.I.S. - was each delivery sized for the gap it had to cover?",
         "=" * 70]
    w.append(f"  lines scored              {r.get('lines_scored', 0):>8,}")
    w.append(f"    skipped, no sales rate  {r.get('skipped_no_sales_rate', 0):>8,}")
    w.append(f"    skipped, <2 deliveries  {r.get('skipped_too_few_deliveries', 0):>8,}")
    if not r.get("lines_scored"):
        w.append("")
        w.append("  Nothing to judge. This needs at least two completed receipts")
        w.append("  of the same line, and a measured sales rate for it.")
        return "\n".join(w)

    mr, mi = r.get("median_residual_days"), r.get("median_interval_days")
    w.append("-" * 70)
    w.append(f"  MEDIAN INTERVAL SERVED    {mi:>8.0f} days between deliveries")
    w.append(f"  MEDIAN RESIDUAL           {mr:>+8.0f} days of cover left when the")
    w.append("                                     next delivery landed")
    w.append(f"  VERDICT   {verdict(mr, mi)}")
    if r.get("idle_capital"):
        w.append(f"  IDLE CAPITAL  KES {r['idle_capital']:>14,.0f} sitting unsold at the")
        w.append("                                   moment of resupply")
    w.append("")

    sups = sorted(r.get("by_supplier", {}).items(),
                  key=lambda kv: -(kv[1].get("idle_capital") or 0))
    real = [(k, v) for k, v in sups if v["lines"] >= 3]
    if real:
        w.append("  WORST BY IDLE CAPITAL (suppliers with 3+ scored lines)")
        w.append(f"     {'supplier':<34}{'lines':>6}{'interval':>10}{'residual':>10}{'idle capital':>16}")
        for name, v in real[:top]:
            w.append(f"     {str(name)[:32]:<34}{v['lines']:>6}"
                     f"{v['median_interval_days']:>9.0f}d{v['median_residual_days']:>+9.0f}d"
                     f"   KES {v['idle_capital']:>11,.0f}")
        w.append("")
        tight = sorted(real, key=lambda kv: kv[1]["median_residual_days"])[:5]
        w.append("  RUNNING CLOSEST (deliveries barely spanning the interval)")
        for name, v in tight:
            w.append(f"     {str(name)[:32]:<34}{v['lines']:>6}"
                     f"{v['median_interval_days']:>9.0f}d{v['median_residual_days']:>+9.0f}d")
    w.append("")
    return "\n".join(w)
