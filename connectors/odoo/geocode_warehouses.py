"""Write warehouse coordinates into Odoo, where OASIS actually reads them.

WHY THIS EXISTS
---------------
Donor ranking is distance-aware: given two stores that can both spare a line,
the nearer one should send it, because the lorry is the cost. OASIS reads those
coordinates from ``stock.warehouse -> partner_id -> partner_latitude /
partner_longitude``, which is where Odoo keeps them.

Most instances have never geocoded anything, so those fields read 0.0/0.0 —
a real point in the Gulf of Guinea, not a missing value. OASIS drops zeroes
rather than feeding them to a distance calculation, so ranking silently falls
back to a neutral distance and every donor looks equally far away. Nothing is
wrong with the plan; it is simply blind to geography.

This writes real coordinates onto the warehouse partners. Two sources, in order:

  1. ``--from-seed`` — the depot's surveyed Chandarana coordinates.
  2. ``--set CODE=lat,lon`` — for a customer, given by them or read off a map.

There is deliberately NO geocoding service call here. Sending a customer's site
addresses to a third-party API is a data-protection decision that belongs to
them, not a side effect of running a setup script.

    python connectors/odoo/geocode_warehouses.py --from-seed
    python connectors/odoo/geocode_warehouses.py --set C001=-1.31,36.82 --set C002=-1.29,36.78
    python connectors/odoo/geocode_warehouses.py --show
"""

from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, REPO)

from oasis.logic.odoo_adapter import OdooAdapter          # noqa: E402

SEED = os.path.join(HERE, "store_network_seed.json")


def adapter():
    return OdooAdapter(url=os.getenv("ODOO_URL", "http://localhost:8069"),
                       db=os.getenv("ODOO_DB", "oasis"),
                       user=os.getenv("ODOO_USER", "admin"),
                       password=os.getenv("ODOO_PASSWORD", "admin"))


def _warehouses(a):
    """code -> {id, name, partner_id} for every warehouse that has a code."""
    out = {}
    for w in a._ex("stock.warehouse", "search_read", [[]],
                   {"fields": ["code", "name", "partner_id", "company_id"]}) or []:
        code = (w.get("code") or "").strip()
        if code:
            out[code] = w
    return out


def show(a):
    whs = _warehouses(a)
    pids = [w["partner_id"][0] for w in whs.values()
            if isinstance(w.get("partner_id"), (list, tuple)) and w["partner_id"]]
    geo = {g["id"]: g for g in (a._ex(
        "res.partner", "read",
        [pids, ["partner_latitude", "partner_longitude", "city"]]) or [])}
    print(f"\n{'code':<8}{'lat':>12}{'lon':>12}  {'city':<18}{'name'}")
    print("-" * 74)
    placed = 0
    for code in sorted(whs):
        w = whs[code]
        pid = (w["partner_id"][0]
               if isinstance(w.get("partner_id"), (list, tuple)) and w["partner_id"]
               else None)
        g = geo.get(pid) or {}
        lat = g.get("partner_latitude") or 0.0
        lon = g.get("partner_longitude") or 0.0
        mark = "" if (lat or lon) else "   <- not geocoded"
        if lat or lon:
            placed += 1
        print(f"{code:<8}{lat:>12.4f}{lon:>12.4f}  "
              f"{str(g.get('city') or '')[:18]:<18}{w['name'][:26]}{mark}")
    print("-" * 74)
    print(f"{placed} of {len(whs)} warehouses carry coordinates. OASIS ranks "
          f"donors by distance only for those;\nthe rest fall back to a neutral "
          f"distance, which is a plan blind to geography rather than a wrong one.\n")
    return placed, len(whs)


def _shared_partners(whs):
    """partner_id -> [warehouse codes], for partners used by more than one."""
    by_partner = {}
    for code, w in whs.items():
        pid = (w["partner_id"][0]
               if isinstance(w.get("partner_id"), (list, tuple)) and w["partner_id"]
               else None)
        if pid:
            by_partner.setdefault(pid, []).append(code)
    return {pid: codes for pid, codes in by_partner.items() if len(codes) > 1}


def _own_address(a, code, w, dry_run=False):
    """Give this warehouse its own address partner, as a child of the company.

    Odoo's default is that a warehouse uses the COMPANY's address. That is
    fine until you want per-site coordinates: sixteen of the depot's seventeen
    warehouses shared one partner, so writing a coordinate for one store
    silently moved every other store to the same spot — the last write won and
    the whole chain collapsed onto one point.

    A multi-site business generally wants distinct addresses anyway; without
    them Odoo cannot print a delivery address per site either.
    """
    name = f"{w['name']} (site address)"
    if dry_run:
        return None
    pid = a._ex("res.partner", "create", [{
        "name": name,
        "type": "delivery",
        "parent_id": 1 if _company_partner_exists(a) else False,
        "company_id": w.get("company_id") and w["company_id"][0] or False,
    }])
    a._ex("stock.warehouse", "write", [[w["id"]], {"partner_id": pid}])
    return pid


def _company_partner_exists(a):
    try:
        return bool(a._ex("res.partner", "search_count", [[["id", "=", 1]]]))
    except Exception:
        return False


def apply(a, coords: dict, dry_run=False, separate_addresses=False):
    """coords: {warehouse_code: (lat, lon)} -> written onto its own partner."""
    whs = _warehouses(a)
    shared = _shared_partners(whs)
    targeted = set(coords)

    # REFUSE TO COLLAPSE A CHAIN ONTO ONE POINT.
    clashes = {pid: [c for c in codes if c in targeted]
               for pid, codes in shared.items()}
    clashes = {pid: codes for pid, codes in clashes.items() if len(codes) > 1}
    if clashes and not separate_addresses:
        print("")
        print("  REFUSED — these warehouses share one address partner, so a")
        print("  coordinate written for one would move all of them:")
        for pid, codes in clashes.items():
            print(f"    partner {pid} shared by: {', '.join(sorted(codes))}")
        print("")
        print("  Re-run with --separate-addresses to give each warehouse its")
        print("  own address first. That is the correct Odoo shape for a")
        print("  multi-site business, and it is what per-site coordinates need.")
        return 0

    written, skipped = 0, []
    for code, (lat, lon) in sorted(coords.items()):
        w = whs.get(code)
        if not w:
            skipped.append(f"{code} (no such warehouse)")
            continue
        pid = (w["partner_id"][0]
               if isinstance(w.get("partner_id"), (list, tuple)) and w["partner_id"]
               else None)
        made_own = False
        if separate_addresses and (pid is None or pid in shared):
            new_pid = _own_address(a, code, w, dry_run=dry_run)
            if new_pid:
                pid, made_own = new_pid, True
            elif dry_run:
                made_own = True
        if pid is None and not dry_run:
            skipped.append(f"{code} (warehouse has no address partner)")
            continue
        print(f"  {code:<8} {lat:>10.4f} {lon:>10.4f}   {w['name'][:32]}"
              + ("   [own address created]" if made_own else ""))
        if not dry_run and pid:
            a._ex("res.partner", "write",
                  [[pid], {"partner_latitude": lat, "partner_longitude": lon}])
        written += 1
    for s in skipped:
        print(f"  ! skipped {s}")
    return written


def _from_seed():
    if not os.path.exists(SEED):
        raise SystemExit(
            "No store_network_seed.json here — that file is the depot fixture. "
            "For a customer, pass coordinates with --set CODE=lat,lon.")
    seed = json.load(open(SEED, encoding="utf-8"))
    return {s["code"]: (float(s["latitude"]), float(s["longitude"]))
            for s in seed["stores"]
            if s.get("latitude") is not None and s.get("longitude") is not None}


def _parse_set(values):
    out = {}
    for v in values or []:
        try:
            code, pair = v.split("=", 1)
            lat, lon = pair.split(",", 1)
            out[code.strip()] = (float(lat), float(lon))
        except ValueError:
            raise SystemExit(f"--set expects CODE=lat,lon — got {v!r}")
    return out


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--from-seed", action="store_true",
                   help="use the depot's surveyed coordinates")
    p.add_argument("--set", action="append", metavar="CODE=lat,lon",
                   help="set one warehouse; repeatable")
    p.add_argument("--show", action="store_true",
                   help="print what Odoo currently holds and stop")
    p.add_argument("--separate-addresses", action="store_true",
                   help="give each warehouse its own address partner first — "
                        "required when they share the company address")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    a = adapter()
    if args.show or not (args.from_seed or args.set):
        show(a)
        return 0

    coords = {}
    if args.from_seed:
        coords.update(_from_seed())
    coords.update(_parse_set(args.set))      # explicit --set wins

    print(f"\nwriting coordinates for {len(coords)} warehouse(s)"
          + ("  [DRY RUN]" if args.dry_run else "") + ":")
    n = apply(a, coords, dry_run=args.dry_run,
              separate_addresses=args.separate_addresses)
    print(f"\n{n} warehouse(s) {'would be' if args.dry_run else ''} geocoded.")
    if n and not args.dry_run:
        show(a)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
