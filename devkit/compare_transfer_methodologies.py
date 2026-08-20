"""Network (Transfers) as the Command Center runs it, vs the derived methodology.

DEV TOOLING - devkit/, never ships.

Both are ``ConsolidatedTransferService.scan_network_opportunities``. They differ
only in what the constructor is handed:

  NETWORK   what oasis/desktop/data.py:network_transfer_scan() actually passes
            today - a supplier-calendar `next_delivery_days`, cold/hot windows
            of 60/14, and NOTHING else. No LATA rhythm and no AMIT tiers, so
            the variance term is unreachable and every category shares one
            45-day threshold.

  DERIVED   the same call with `data_dir`, so relief horizons come from LATA's
            measured GRN history and dead-stock thresholds from AMIT's
            per-category tiers.

Run across several store subsets, because the two do NOT diverge uniformly:
assortment breadth in this network scales with floor area, so a set of large
stores and a set of small ones stress different parts of the maths.

Reported per scenario:
  * volume      lines and units, split PULL/PUSH and fresh/dry
  * service     how much of the deficit population each one actually serves
  * clearance   share of dead stock moved
  * AGREEMENT   for each (SKU, recipient) both would serve, do they pick the
                SAME donor, and how far apart are the quantities? Two plans of
                equal size that disagree on every line are not equivalent, and
                totals alone cannot show that.

Usage:
    python devkit/compare_transfer_methodologies.py
    python devkit/compare_transfer_methodologies.py --scenario full
"""

from __future__ import annotations

import argparse
import contextlib
import io
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "connectors", "odoo"))

from oasis.logic.consolidated_transfer_service import (   # noqa: E402
    ConsolidatedTransferService as CTS)

DATA_DIR = os.path.join(REPO, "oasis", "data")


def scenarios(stores):
    """Store subsets that stress different parts of the network."""
    by_area = sorted(stores, key=lambda s: -float(s.get("floor_area_sqft") or 0))
    codes = [s["code"] for s in stores]
    return {
        "pair":       codes[:2],
        "small-4":    [s["code"] for s in by_area[-4:]],
        "large-4":    [s["code"] for s in by_area[:4]],
        "extremes-4": [s["code"] for s in by_area[:2]] +
                      [s["code"] for s in by_area[-2:]],
        "half-7":     codes[:7],
        "full-14":    codes,
    }


def run(config, names, data, coords, codes, ndd):
    subset = {c: data[c] for c in codes}
    kw = dict(org_names=names, stock_data=subset, distance_map=coords,
              cold_node_days=60, hot_node_days=14)
    if config == "network":
        kw["next_delivery_days"] = ndd          # exactly what the console passes
    else:
        kw["data_dir"] = DATA_DIR               # LATA + AMIT
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf):
        svc = CTS(**kw)
        opps = svc.scan_network_opportunities().opportunities
    return svc, opps


def summarise(opps):
    pull = [o for o in opps if o.type == "PULL"]
    push = [o for o in opps if o.type == "PUSH"]
    fresh = [o for o in opps if o.is_fresh]
    return {
        "pull_n": len(pull), "pull_u": sum(o.transfer_qty for o in pull),
        "push_n": len(push), "push_u": sum(o.transfer_qty for o in push),
        "fresh_u": sum(o.transfer_qty for o in fresh),
        "dry_u": sum(o.transfer_qty for o in opps if not o.is_fresh),
        "value": sum(o.value_kes for o in opps),
        "lines": len(opps),
        "units": sum(o.transfer_qty for o in opps),
    }


def dead_units(data, codes):
    return sum(float(p["current_stocks"]) for c in codes for p in data[c]
               if float(p["avg_daily_sales"] or 0) <= 0
               and float(p["current_stocks"] or 0) > 0)


def dead_moved(opps, data, codes):
    dead = {}
    for c in codes:
        for p in data[c]:
            if float(p["avg_daily_sales"] or 0) <= 0 and float(p["current_stocks"] or 0) > 0:
                dead.setdefault(p["item_code"], set()).add(c)
    return sum(o.transfer_qty for o in opps if o.from_org in dead.get(o.itm_cd, ()))


def agreement(a_opps, b_opps):
    """Do the two plans route the same (SKU, recipient) through the same donor?"""
    def index(opps):
        out = {}
        for o in opps:
            out.setdefault((o.itm_cd, o.to_org), []).append(o)
        return out

    A, B = index(a_opps), index(b_opps)
    shared = set(A) & set(B)
    if not shared:
        return {"shared": 0, "same_donor": 0.0, "qty_gap": 0.0,
                "only_a": len(set(A) - set(B)), "only_b": len(set(B) - set(A))}
    same = 0
    gaps = []
    for k in shared:
        da = max(A[k], key=lambda o: o.transfer_qty)
        db = max(B[k], key=lambda o: o.transfer_qty)
        if da.from_org == db.from_org:
            same += 1
        qa = sum(o.transfer_qty for o in A[k])
        qb = sum(o.transfer_qty for o in B[k])
        gaps.append(abs(qa - qb) / max(qa, qb, 1.0))
    return {"shared": len(shared),
            "same_donor": 100.0 * same / len(shared),
            "qty_gap": 100.0 * statistics.median(gaps),
            "only_a": len(set(A) - set(B)), "only_b": len(set(B) - set(A))}


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", default=None, help="run just one scenario")
    args = p.parse_args(argv)

    from analyse_transfer_funnel import load_from_seed
    import oasis.desktop.data as D

    seed, data = load_from_seed()
    stores = seed["stores"]
    names = {s["code"]: s["name"] for s in stores}
    coords = {s["code"]: {"lat": s["latitude"], "lon": s["longitude"]}
              for s in stores}
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        ndd = D._next_delivery_days(DATA_DIR, data)

    scen = scenarios(stores)
    if args.scenario:
        scen = {args.scenario: scen[args.scenario]}

    print("=" * 78)
    print("NETWORK (Transfers, as the Command Center wires it)  vs  DERIVED "
          "(LATA + AMIT)")
    print(f"supplier coverage: calendar {len(ndd)} | LATA 599")
    print("=" * 78)

    for label, codes in scen.items():
        _, a = run("network", names, data, coords, codes, ndd)
        svc_b, b = run("derived", names, data, coords, codes, ndd)
        sa, sb = summarise(a), summarise(b)
        dead = dead_units(data, codes)
        da, db = dead_moved(a, data, codes), dead_moved(b, data, codes)
        ag = agreement(a, b)

        print(f"\n{label}  ({len(codes)} stores)")
        print(f"  {'':<14}{'lines':>8}{'units':>10}{'PULL u':>10}{'PUSH u':>10}"
              f"{'fresh u':>10}{'dry u':>10}{'dead cleared':>14}")
        for nm, s, dd in (("NETWORK", sa, da), ("DERIVED", sb, db)):
            pct = (100.0 * dd / dead) if dead else 0.0
            print(f"  {nm:<14}{s['lines']:>8,}{s['units']:>10,.0f}"
                  f"{s['pull_u']:>10,.0f}{s['push_u']:>10,.0f}"
                  f"{s['fresh_u']:>10,.0f}{s['dry_u']:>10,.0f}"
                  f"{pct:>13.1f}%")
        fu = sa["fresh_u"]
        print(f"  fresh share of volume: NETWORK "
              f"{100.0 * fu / max(sa['units'], 1):.0f}%   DERIVED "
              f"{100.0 * sb['fresh_u'] / max(sb['units'], 1):.0f}%")
        print(f"  agreement: {ag['shared']:,} (SKU,recipient) pairs in both | "
              f"same donor {ag['same_donor']:.0f}% | median qty gap "
              f"{ag['qty_gap']:.0f}%")
        print(f"             {ag['only_a']:,} served only by NETWORK, "
              f"{ag['only_b']:,} only by DERIVED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
