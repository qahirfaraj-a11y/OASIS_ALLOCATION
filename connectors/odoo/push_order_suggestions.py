"""Compute the replenishment plan and post it into Odoo's OASIS review queue.

The ordering counterpart of push_transfer_suggestions.py. OASIS reads the Odoo
instance store by store, works out what should be bought and why, and posts
SUGGESTIONS into ``oasis.order.suggestion``. The buyer reviews them in Odoo —
OASIS → Replenishment → Suggestions — and approving them creates the draft
purchase order.

Nothing here writes a purchase order. The queue is OASIS's own model; the only
thing that ever creates a buying document is a buyer pressing Approve.

    python connectors/odoo/push_order_suggestions.py --dry-run
    python connectors/odoo/push_order_suggestions.py --limit 200
    python connectors/odoo/push_order_suggestions.py --stores C001,C002
    python connectors/odoo/push_order_suggestions.py --clear
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

# The reasoning text uses real punctuation because it is read by buyers inside
# Odoo, where UTF-8 is fine. This script also PRINTS it, and a Windows console
# is cp1252 — which cannot encode an em dash and takes the whole run down with a
# UnicodeEncodeError.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, REPO)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "devkit"))

try:
    from dotenv import load_dotenv                                    # noqa: E402
    load_dotenv(os.path.join(REPO, ".env"))
except ImportError:                       # optional; the scan still runs
    pass

# The ordering path is ERP-agnostic and picks its adapter from OASIS_ERP. This
# script only ever talks to Odoo, so it says so rather than depending on what
# happens to be in the environment — running the Odoo scan against a POS
# database would produce a plan for the wrong system and post it anyway.
os.environ["OASIS_ERP"] = "odoo"

from oasis.logic.odoo_adapter import OdooAdapter                      # noqa: E402
from push_transfer_suggestions import store_universe                  # noqa: E402

M = "oasis.order.suggestion"


def adapter():
    return OdooAdapter(url=os.getenv("ODOO_URL", "http://localhost:8069"),
                       db=os.getenv("ODOO_DB", "oasis"),
                       user=os.getenv("ODOO_USER", "admin"),
                       password=os.getenv("ODOO_PASSWORD", "admin"))


def reason_for(rec, store_name):
    """The argument, in the buyer's language — not the engine's.

    This text is what the customer is actually buying. A draft PO already tells
    them WHAT to order; only this tells them why it is worth doing and what it
    costs to ignore. So it names the position, the horizon and the consequence
    — never 'z_score' or 'target_coverage_days'.
    """
    qty = float(rec.get("recommended_quantity") or 0)
    stock = float(rec.get("current_stock") or rec.get("current_stocks") or 0)
    ads = float(rec.get("avg_daily_sales") or 0)
    lead = float(rec.get("lead_time_days") or 0)
    on_order = float(rec.get("on_order_qty") or 0)
    fresh = bool(rec.get("is_fresh"))

    if ads > 0:
        cover = stock / ads
        if stock <= 0:
            head = f"{store_name} has none of this line left"
        else:
            head = (f"{store_name} holds {stock:.0f}, about {cover:.0f} day"
                    f"{'s' if round(cover) != 1 else ''} at {ads:.2f} a day")
        if lead > 0:
            head += (f", and {rec.get('supplier_name') or 'this supplier'} "
                     f"takes about {lead:.0f} days to deliver")
    else:
        # No measured demand. Say so plainly rather than dressing a
        # presentation-stock or new-line order up as a demand forecast.
        head = (f"{store_name} has no measured sales history for this line, so "
                f"the quantity covers presentation stock rather than demand")

    body = f". Ordering {qty:.0f}"
    if on_order > 0:
        eta = float(rec.get("on_order_eta_days") or 0)
        body += (f" on top of {on_order:.0f} already on order"
                 + (f" due in about {eta:.0f} days" if 0 < eta < 900 else ""))

    # STATE THE COVER THIS ACTUALLY DELIVERS, not the target it was aiming for.
    #
    # These are not the same number and the gap is not small: the ordering
    # stage re-floors the coverage target at gap + lead + safety, overriding
    # the ceiling the enrichment stage applied, on 61% of ordered lines
    # (median 1.36x, worst 6.2x — a 7-day UHT ceiling delivering 43 days).
    # Quoting the target made the queue claim a position it does not reach,
    # which is the fastest way to lose a buyer's trust: they can count the
    # cartons.
    delivered = ((stock + qty) / ads) if ads > 0 else 0.0
    if delivered > 0:
        body += f" takes it to about {delivered:.0f} days of cover"
        target = float(rec.get("target_coverage_days") or 0)
        if target > 0 and delivered > target * 1.25:
            body += (f", well past the {target:.0f} days this line is meant to "
                     f"hold — the supplier's own rhythm is what forces the depth")
    body += "."

    tail = ""
    shelf = float(rec.get("shelf_life_days") or 0)
    if shelf > 0 and delivered > shelf:
        # The sharpest failure there is: buying more than the item can survive.
        # Say it first, before anything else in the tail.
        tail = (f" WARNING: that is more than its {shelf:.0f}-day shelf life. "
                f"Some of this will expire before it sells.")
    elif fresh:
        tail = (" Perishable — check the shelf before approving; an over-order "
                "here becomes waste rather than stock.")
    elif ads > 0 and stock <= ads * max(lead, 1):
        tail = (" It runs out before the delivery lands, so anything not "
                "ordered now is a gap on the shelf.")
    return head + body + tail


def _build(limit=0, stores=None, log=print):
    """Read every store, compute its order plan, and shape it for the queue."""
    from oasis.desktop.data import generate_smart_orders

    log("-> reading through OdooAdapter…")
    a = adapter()
    codes, names, _coords = store_universe(a, log=log)
    if stores:
        wanted = {s.strip() for s in stores if s.strip()}
        missing = wanted - set(codes)
        codes = [c for c in codes if c in wanted]
        if missing:
            log(f"   ! --stores names {len(missing)} warehouse(s) this Odoo "
                f"does not have: {', '.join(sorted(missing)[:5])}")
    if not codes:
        raise SystemExit(
            "No stores to scan. Check OASIS_ODOO_STORES, --stores, or that the "
            "Odoo user can see the warehouses.")

    # WHEN each site was read, not when the write finishes — the same reasoning
    # as the transfer scan. The plan is a composite of N instants and can be no
    # fresher than the oldest reading it was built from.
    read_started = datetime.utcnow()

    rows, funnels = [], {}
    for code in codes:
        store_name = names.get(code, code)
        res = generate_smart_orders(code)
        if res.get("error"):
            log(f"   ! {code}: {res['error']}")
            continue
        funnel = res.get("funnel") or {}
        funnels[code] = funnel
        recs = res.get("po_recs") or []
        log(f"   {code:<10} {funnel.get('products_read', 0):>6,} read  ->  "
            f"{len(recs):>4} to order  "
            f"({funnel.get('below_moq', 0):,} under MOQ, "
            f"{funnel.get('no_order_needed', 0):,} no order needed)")

        # The supplier gate the engine actually applied, carried onto every row
        # so the Odoo module can check a part-approved basket without calling
        # back here.
        min_units = float(funnel.get("min_order_units") or 0)
        min_value = float(funnel.get("min_order_value_kes") or 0)

        for r in recs:
            qty = float(r.get("recommended_quantity") or 0)
            if qty <= 0:
                continue
            cost = float(r.get("cost_price") or 0)
            ads = float(r.get("avg_daily_sales") or 0)
            stock = float(r.get("current_stock") or r.get("current_stocks") or 0)
            rows.append({
                "item_code": r.get("item_code"),
                "store_code": code,
                "supplier_code": r.get("supplier_cd"),
                "supplier_name": r.get("supplier_name"),
                "quantity": qty,
                "unit_cost": cost,
                "value": qty * cost,
                "current_stock": stock,
                "avg_daily_sales": ads,
                "days_cover": (stock / ads) if ads > 0 else 0.0,
                "lead_time_days": float(r.get("lead_time_days") or 0),
                "target_cover_days": float(r.get("target_coverage_days") or 0),
                "on_order_qty": float(r.get("on_order_qty") or 0),
                "on_order_eta_days": float(r.get("on_order_eta_days") or 0),
                "is_fresh": bool(r.get("is_fresh")),
                "pack_size": float(r.get("pack_size") or 0),
                "supplier_min_units": min_units,
                "supplier_min_value": min_value,
                "reason": reason_for(r, store_name),
            })

    rows.sort(key=lambda x: -x["value"])
    kept = rows[:limit] if limit > 0 else rows
    fresh = sum(1 for r in kept if r["is_fresh"])
    log(f"   plan: {len(rows):,} lines across {len(funnels)} store(s); "
        f"posting {len(kept):,} ({fresh:,} perishable)")
    return kept, read_started


def run_scan(limit=0, stores=None, log=print):
    """Scan every store and repost the order queue. Returns what was written.

    The single implementation, shared by the CLI and by scan_service.py, so the
    button in Odoo and the command line can never compute different plans.
    """
    rows, read_started = _build(limit, stores=stores, log=log)
    a = adapter()
    # UTC, because Odoo stores every datetime in UTC and renders it in the
    # user's timezone. Local time here would write a computed_on in the FUTURE,
    # so the staleness window could never fire — the failure mode where the
    # safety feature is present, green, and doing nothing.
    res = a._ex(M, "oasis_replace_queue",
                [rows, read_started.strftime("%Y-%m-%d %H:%M:%S")])
    out = {"created": res.get("created", 0), "skipped": res.get("skipped", 0),
           "considered": len(rows)}
    detail = res.get("skipped_detail") or {}
    if detail:
        out["skipped_detail"] = detail
        log(f"   skipped: {detail}")
    return out


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=0,
                   help="post only the N most valuable suggestions (0 = all)")
    p.add_argument("--stores", default="",
                   help="comma-separated warehouse codes (default: all)")
    p.add_argument("--dry-run", action="store_true",
                   help="show what would be posted; write nothing")
    p.add_argument("--clear", action="store_true",
                   help="empty the pending queue and stop")
    args = p.parse_args(argv)

    a = adapter()
    if not a.health_check().get("connected"):
        raise SystemExit("Odoo unreachable — is the stack up?")

    if args.clear:
        res = a._ex(M, "oasis_replace_queue", [[]])
        print(f"queue cleared ({res})")
        return 0

    stores = [s for s in args.stores.split(",") if s.strip()] or None
    rows, read_started = _build(args.limit, stores=stores)

    if args.dry_run:
        print("\n--dry-run — nothing written.\n")
        for r in rows[:5]:
            fresh = "  [PERISHABLE]" if r["is_fresh"] else ""
            print(f"  {r['store_code']}  {r['quantity']:.0f} x "
                  f"{str(r['item_code'])[:40]}   "
                  f"{r['supplier_name']}{fresh}")
            print(f"      {r['reason']}\n")
        return 0

    res = a._ex(M, "oasis_replace_queue",
                [rows, read_started.strftime("%Y-%m-%d %H:%M:%S")])
    print(f"-> posted {res.get('created', 0):,} suggestions "
          f"({res.get('skipped', 0):,} skipped: {res.get('skipped_detail') or {}})")
    print("   Review in Odoo:  OASIS → Replenishment → Suggestions")
    print("   Approving creates a DRAFT purchase order; nothing is sent until "
          "your buyer confirms it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
