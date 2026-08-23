"""Compute the transfer plan and post it into Odoo's OASIS review queue.

This is the seam between the two halves of the product. OASIS reads the Odoo
depot, works out what should move and why, and posts SUGGESTIONS into
``oasis.transfer.suggestion``. The operator reviews them in Odoo — OASIS →
Transfers → Suggestions — and approving one creates the draft internal
transfer.

Nothing here writes a stock document. The queue is OASIS's own model; the only
thing that ever touches Odoo's inventory is an operator pressing Approve.

    python connectors/odoo/push_transfer_suggestions.py --dry-run
    python connectors/odoo/push_transfer_suggestions.py --limit 200
    python connectors/odoo/push_transfer_suggestions.py --clear
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
from datetime import datetime

# The reasoning text uses real punctuation because it is read by operators
# inside Odoo, where UTF-8 is fine. This script also PRINTS it, and a Windows
# console is cp1252 — which cannot encode an em dash and takes the whole run
# down with a UnicodeEncodeError. Make the console tolerant rather than
# flattening the text that ships to the customer.
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

# Honour the same .env the rest of OASIS reads. This path was the exception:
# oasis/api/server.py has always called load_dotenv, but the scan — the thing a
# customer actually schedules — read only the raw process environment, so
# settings put in .env silently did nothing here. load_dotenv does NOT override
# an already-set variable, so an explicit environment still wins.
try:
    from dotenv import load_dotenv                                    # noqa: E402
    load_dotenv(os.path.join(REPO, ".env"))
except ImportError:                       # optional; the scan still runs
    pass

from oasis.logic.odoo_adapter import OdooAdapter                      # noqa: E402
from oasis.logic.consolidated_transfer_service import (               # noqa: E402
    ConsolidatedTransferService as CTS)


def reason_for(o, relief=None):
    """The argument, in the operator's language — not the engine's.

    This text is what the customer is actually buying. A draft picking already
    tells them WHAT to move; only this tells them why it is worth doing, and
    what it costs to ignore. So it names the horizon, both positions, and the
    consequence — never 'risk_kes' or 'RELEASE_FRACTION'.
    """
    # Cover is None where nothing sells — the engine no longer emits a 999
    # sentinel, so there is no magic number left to guard against here. An
    # operator must never be shown either form as if it were a measurement.
    def position(days):
        return ("is not selling it at all" if days is None
                else f"has {days:.0f} days of cover")

    if o.type == "PULL":
        cover = o.recipient_days_cover
        if cover is None:
            head = f"{o.to_org} has none of this line and no measured demand"
        else:
            head = (f"{o.to_org} runs out in about {cover:.0f} day"
                    f"{'s' if round(cover) != 1 else ''}")
        if relief:
            head += f", before its next delivery is due in {relief:.0f}"
        body = (f". {o.from_org} is holding {o.donor_excess:.0f} spare and "
                f"{position(o.donor_days_cover)}, so moving "
                f"{o.transfer_qty:.0f} covers the gap without putting the donor short.")
        tail = (f" Leaving it costs roughly KES {o.value_kes:,.0f} of sales "
                f"on this line.")
        return head + body + tail
    if o.donor_days_cover is None:
        head = (f"{o.from_org} is not selling this line at all and is sitting on "
                f"{o.donor_excess:.0f} units of it")
    else:
        head = (f"{o.from_org} has {o.donor_days_cover:.0f} days of cover on this "
                f"line and it is barely moving")
    return (f"{head} — capital on a shelf rather than in the till. "
            f"{o.to_org} does sell it, and can take {o.transfer_qty:.0f} before "
            f"the line would go stale there too. About KES {o.value_kes:,.0f} "
            f"back in circulation once it does.")


def store_universe(a, log=print):
    """Which warehouses take part in the scan, and where they are.

    THIS USED TO READ A DEPOT FIXTURE. `store_network_seed.json` is built by
    build_store_network_seed.py from an 82MB Rhapta network file, it is
    gitignored, and it exists on exactly one machine — so the script behind the
    Refresh button could not run against a customer's Odoo at all. It did not
    degrade; it raised FileNotFoundError on its first line.

    Odoo already knows its own warehouses, so ask it. The seed is still
    honoured where it exists, because it carries surveyed coordinates for the
    depot and Odoo's are un-geocoded zeroes, but it is no longer required.

    Precedence, most specific first:

      1. OASIS_ODOO_STORES — an explicit comma-separated list of warehouse
         codes. Needed wherever not every warehouse is a shop: a central DC, a
         returns location, a scrap bin. Transfers between those are not
         movements anyone wants proposed.
      2. the depot seed, when present.
      3. every warehouse Odoo reports — the right default for a customer,
         since it is their own list rather than one we invented.

    Coordinates only sharpen the donor ranking; where they are missing the
    engine falls back to a neutral distance, so a customer who has not
    geocoded their sites still gets a plan.
    """
    seed_path = os.path.join(HERE, "store_network_seed.json")
    coords, names, codes = {}, {}, []

    if os.path.exists(seed_path):
        seed = json.load(open(seed_path, encoding="utf-8"))
        codes = [s["code"] for s in seed["stores"]]
        names = {s["code"]: s["name"] for s in seed["stores"]}
        coords = {s["code"]: {"lat": s["latitude"], "lon": s["longitude"]}
                  for s in seed["stores"] if s.get("latitude") is not None}
        log(f"   stores: {len(codes)} from the depot seed")
    else:
        orgs = a.fetch_all_organizations() or []
        codes = [o["ORG_CD"] for o in orgs]
        names = {o["ORG_CD"]: o["ORG_NAME"] for o in orgs}
        coords = _coords_from_odoo(a)
        log(f"   stores: {len(codes)} read from Odoo's own warehouses"
            + (f", {len(coords)} geocoded" if coords else ", none geocoded"))

    only = [c.strip() for c in os.getenv("OASIS_ODOO_STORES", "").split(",")
            if c.strip()]
    if only:
        missing = [c for c in only if c not in codes]
        codes = [c for c in codes if c in only]
        log(f"   stores: narrowed to {len(codes)} by OASIS_ODOO_STORES")
        if missing:
            log(f"   ! OASIS_ODOO_STORES names {len(missing)} warehouse(s) this "
                f"Odoo does not have: {', '.join(missing[:5])}")
    if len(codes) < 2:
        raise SystemExit(
            "A transfer needs at least two warehouses; this instance offers "
            f"{len(codes)}. Check OASIS_ODOO_STORES, or that the Odoo user can "
            "see the warehouses.")
    return codes, names, coords


def _coords_from_odoo(a):
    """Warehouse coordinates via its partner, where the customer has geocoded.

    Odoo hangs an address off every warehouse, and res.partner carries
    partner_latitude/partner_longitude. Un-geocoded partners read 0.0/0.0,
    which is a real place in the Gulf of Guinea rather than a missing value —
    so those are dropped rather than passed to a distance calculation.
    """
    out = {}
    try:
        whs = a._ex("stock.warehouse", "search_read", [[]],
                    {"fields": ["code", "partner_id"]}) or []
        pids = [w["partner_id"][0] for w in whs
                if isinstance(w.get("partner_id"), (list, tuple)) and w["partner_id"]]
        if not pids:
            return out
        geo = {g["id"]: g for g in (a._ex(
            "res.partner", "read",
            [pids, ["partner_latitude", "partner_longitude"]]) or [])}
        for w in whs:
            pid = (w["partner_id"][0]
                   if isinstance(w.get("partner_id"), (list, tuple)) and w["partner_id"]
                   else None)
            g = geo.get(pid) or {}
            lat, lon = g.get("partner_latitude") or 0, g.get("partner_longitude") or 0
            code = (w.get("code") or "").strip()
            if code and (lat or lon):
                out[code] = {"lat": float(lat), "lon": float(lon)}
    except Exception as e:
        logger_msg = str(e)[:100]
        print(f"   ! warehouse coordinates unreadable ({logger_msg}); donor "
              f"ranking falls back to a neutral distance")
    return out


def _build(limit=0, log=print):
    """Read the depot, compute the plan, and shape it for the queue."""
    from verify_store_network import fetch_with_retry
    from oasis.desktop.data import store_db_path
    log("-> reading through OdooAdapter…")
    a = adapter()
    codes, names, coords = store_universe(a, log=log)
    # WHEN each site was read, not when the write finishes.
    #
    # The 14 sites are read one after another while tills are selling, so the
    # plan is a composite of 14 instants, not one snapshot: the first store's
    # stock is already the oldest fact in it by the time the last is read.
    # Stamping the queue at write time therefore dated every suggestion from
    # the freshest possible moment and let the staleness window run from the
    # wrong end — a plan built on a two-minute read looked brand new.
    #
    # The oldest read is the honest age of the plan. Recording the span makes
    # the inconsistency visible instead of merely known.
    read_started = datetime.utcnow()
    data = {}
    for c in codes:
        data[c] = fetch_with_retry(a, c)
    read_span = (datetime.utcnow() - read_started).total_seconds()
    log(f"   snapshot: {len(codes)} sites read over {read_span:.1f}s "
        f"(plan ages from {read_started.strftime('%H:%M:%S')} UTC, the oldest "
        f"reading, not from now)")
    if read_span > 120:
        log(f"   ! the read took {read_span / 60:.1f} minutes. Suggestions at "
            f"the far ends of it describe stock at meaningfully different "
            f"times; review before approving in bulk.")

    # WHAT IS ALREADY ON ITS WAY. Without these the scan sees stock and demand
    # and nothing else, so it re-proposes movements that are already queued —
    # approve twice and you ship twice. The Command Center path has always
    # passed them; this one did not, so the two entry points were not scanning
    # the same network.
    pending = []
    try:
        df = a.fetch_transfers(None)
        if hasattr(df, "empty") and not df.empty:
            pending = df.to_dict("records")
    except Exception as e:
        log(f"   ! open transfers unreadable ({str(e)[:70]}) — the scan may "
            f"re-propose movements already in flight")

    # A line the buying engine could not order because it fell under the
    # supplier's minimum is the strongest case there is for moving stock
    # instead. It is a pull trigger, and was being dropped here.
    moq = {}
    try:
        from oasis.logic.moq_failure_store import load_moq_failures
        moq = load_moq_failures(
            os.path.join(REPO, "oasis", "data", "moq_failures.json")) or {}
    except Exception:
        pass

    # THE SAFETY FLOOR IS DERIVED, so nothing is passed here.
    #
    # The seed does carry a safety_days per store, and wiring it in is what
    # made the field live — but every store seeds to the same literal 10,
    # because build_store_network_seed falls back to one and
    # stores_network.json has no per-store value. Passing it would override the
    # derivation with a fixture default and re-declare the constant that
    # deriving sigma exists to remove.
    #
    # safety_days_by_org stays supported for a client whose ERP carries a real
    # per-site service level. A depot fixture is not that.
    log("   safety floor: derived per store-SKU from LATA's supplier rhythm "
        "(no per-store override passed)")

    with contextlib.redirect_stderr(io.StringIO()):
        svc = CTS(org_names=names, stock_data=data, distance_map=coords,
                  data_dir=os.path.join(REPO, "oasis", "data"),
                  settings_db=store_db_path(REPO))
        opps = svc.scan_network_opportunities(
            pending_transfers=pending, moq_failures=moq).opportunities
    log(f"   inputs: {len(pending):,} open transfer line(s), "
        f"{sum(len(v) for v in moq.values()):,} MOQ-blocked line(s)")

    # Velocity at both ends, and the recipient's own relief horizon. The
    # opportunity carries days-of-cover but not the rate behind it, and an
    # operator cannot judge "31 units" without knowing whether the receiving
    # store sells 30 a day or 0.3.
    ads = {}
    supplier_of = {}
    for org, prods in data.items():
        for p in prods:
            code = p.get("item_code")
            ads[(org, code)] = float(p.get("avg_daily_sales") or 0)
            supplier_of.setdefault(code, (p.get("supplier_name") or "",
                                          float(p.get("estimated_delivery_days") or 0),
                                          p.get("department") or ""))

    def relief_for(itm):
        sup, lead, dept = supplier_of.get(itm, ("", 0.0, ""))
        return svc._relief_days(sup, lead, dept) or svc.target_cover_days

    opps.sort(key=lambda o: -o.value_kes)
    kept = opps[:limit] if limit > 0 else opps
    rows = [{
        "item_code": o.itm_cd, "from_code": o.from_org, "to_code": o.to_org,
        "quantity": o.transfer_qty, "kind": o.type.lower(),
        "value": o.value_kes, "donor_cover": o.donor_days_cover,
        "recipient_cover": o.recipient_days_cover, "is_fresh": o.manual_only,
        "donor_ads": ads.get((o.from_org, o.itm_cd), 0.0),
        "recipient_ads": ads.get((o.to_org, o.itm_cd), 0.0),
        "relief_days": relief_for(o.itm_cd),
        "reason": reason_for(o, relief_for(o.itm_cd)),
    } for o in kept]

    pull = sum(1 for r in rows if r["kind"] == "pull")
    fresh = sum(1 for r in rows if r["is_fresh"])
    log(f"   plan: {len(opps):,} lines; posting {len(rows):,} "
          f"({pull:,} plug-a-gap, {len(rows) - pull:,} clear-idle, "
          f"{fresh:,} perishable)")
    return rows, opps, read_started


def adapter():
    return OdooAdapter(url=os.getenv("ODOO_URL", "http://localhost:8069"),
                       db=os.getenv("ODOO_DB", "oasis"),
                       user=os.getenv("ODOO_USER", "admin"),
                       password=os.getenv("ODOO_PASSWORD", "admin"))


def run_scan(limit=0, log=print):
    """Scan the depot and repost the queue. Returns what was written.

    The single implementation, shared by the CLI and by scan_service.py, so the
    button in Odoo and the command line can never compute different plans.
    """
    rows, _, read_started = _build(limit, log=log)
    a = adapter()
    # UTC, because Odoo stores every datetime in UTC and renders it in the
    # user's timezone. Sending local time here wrote a computed_on up to hours
    # in the FUTURE, which meant the staleness window could never fire and a
    # day-old plan looked freshly computed — the failure mode where the safety
    # feature is present, green, and doing nothing.
    #
    # And it is the START of the depot read, not now: the plan can be no
    # fresher than the oldest reading it was built from, so ageing it from the
    # write would restart the staleness clock on data already minutes old.
    res = a._ex("oasis.transfer.suggestion", "oasis_replace_queue",
                [rows, read_started.strftime("%Y-%m-%d %H:%M:%S")])
    return {"created": res.get("created", 0), "skipped": res.get("skipped", 0),
            "considered": len(rows)}


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=0,
                   help="post only the N most valuable suggestions (0 = all)")
    p.add_argument("--dry-run", action="store_true",
                   help="show what would be posted; write nothing")
    p.add_argument("--clear", action="store_true",
                   help="empty the pending queue and stop")
    args = p.parse_args(argv)

    a = adapter()
    if not a.health_check().get("connected"):
        raise SystemExit("Odoo unreachable — is the stack up?")

    if args.clear:
        res = a._ex("oasis.transfer.suggestion", "oasis_replace_queue", [[]])
        print(f"queue cleared ({res})")
        return 0

    rows, opps, read_started = _build(args.limit)

    if args.dry_run:
        # Show BOTH jobs. The list is value-ranked and gap-plugging dominates
        # by value, so sampling the top alone would never show what a
        # clear-idle suggestion reads like — which is half the product.
        print("\n--dry-run — nothing written.\n")
        for kind, label in (("pull", "PLUG A GAP"), ("push", "CLEAR IDLE STOCK")):
            sample = [r for r in rows if r["kind"] == kind][:2]
            if not sample:
                continue
            print(f"  ── {label} ──")
            for r in sample:
                fresh = "  [PERISHABLE]" if r["is_fresh"] else ""
                print(f"  {r['from_code']} -> {r['to_code']}   "
                      f"{r['quantity']:.0f} x {r['item_code'][:40]}{fresh}")
                print(f"      {r['reason']}\n")
        return 0

    # UTC, because Odoo stores every datetime in UTC and renders it in the
    # user's timezone. Sending local time here wrote a computed_on up to hours
    # in the FUTURE, which meant the staleness window could never fire and a
    # day-old plan looked freshly computed — the failure mode where the safety
    # feature is present, green, and doing nothing.
    #
    # And it is the START of the depot read — see run_scan.
    res = a._ex("oasis.transfer.suggestion", "oasis_replace_queue",
                [rows, read_started.strftime("%Y-%m-%d %H:%M:%S")])
    print(f"-> posted {res.get('created', 0):,} suggestions "
          f"({res.get('skipped', 0):,} skipped: unknown product or warehouse)")
    print("   Review in Odoo:  OASIS → Transfers → Suggestions")
    print("   Approving one creates a DRAFT internal transfer; nothing moves "
          "until your team confirms it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
