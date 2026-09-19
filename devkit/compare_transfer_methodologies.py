"""The transfer scan as the product ships it, vs the derived methodology.

DEV TOOLING - devkit/, never ships. PROBE: probe.transfer-methodology-compare,
testing claim.transfer.network-vs-derived-diverge. Emits one JSON verdict line
(the probe harness contract); everything else on stdout is the readable report.

All arms are ``ConsolidatedTransferService.scan_network_opportunities``. They
differ only in how the service is built:

  SHIPPED   oasis.desktop.data.build_transfer_service - the one builder every
            surface (desktop, web, Command Center, Operations Console) has used
            since 9dca72cf. Built BY CALLING IT, so this probe cannot drift
            from the product the way its first version did: that version
            hard-coded the calendar-only wiring as "what the console passes",
            and kept measuring it after the console stopped passing it.
  DERIVED   the methodology: `data_dir` only, so relief horizons come from
            LATA's measured GRN history and dead-stock thresholds from AMIT's
            per-category tiers.
  LEGACY    what the surfaces passed before 9dca72cf - a supplier-calendar
            `next_delivery_days`, cold/hot 60/14 and NOTHING else (no LATA, no
            AMIT, one 45-day threshold). Reported for context; the verdict is
            SHIPPED vs DERIVED, because that is what the claim is about.

Run across several store subsets, because plans do NOT diverge uniformly:
assortment breadth in this network scales with floor area, so a set of large
stores and a set of small ones stress different parts of the maths.

Reported per scenario:
  * volume      lines and units, split PULL/PUSH and fresh/dry
  * clearance   share of dead stock moved
  * AGREEMENT   for each (SKU, recipient) both would serve, do they pick the
                SAME donor, and how far apart are the quantities? Two plans of
                equal size that disagree on every line are not equivalent, and
                totals alone cannot show that.

Usage:
    python devkit/compare_transfer_methodologies.py
    python devkit/compare_transfer_methodologies.py --scenario full-14
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "connectors", "odoo"))

from oasis.logic.consolidated_transfer_service import (   # noqa: E402
    ConsolidatedTransferService as CTS)

DATA_DIR = os.path.join(REPO, "oasis", "data")

#: The plans DIVERGE if any of these is exceeded (percent).
DIVERGE = {"lines_pct": 5.0, "units_pct": 5.0, "donor_disagree_pct": 5.0,
           "qty_gap_pct": 5.0, "unshared_pct": 5.0}


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
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf), contextlib.redirect_stdout(buf):
        if config == "shipped":
            import oasis.desktop.data as D
            # the SAME coordinates every arm gets: the install's store_coords.json
            # describes the live estate, not this network, and without these the
            # shipped arm chose donors blind to distance (a probe artefact that
            # read as 11-14% donor disagreement)
            svc = D.build_transfer_service({c: names[c] for c in codes}, subset, root=REPO,
                                           distance_map=coords)
        else:
            kw = dict(org_names=names, stock_data=subset, distance_map=coords,
                      cold_node_days=60, hot_node_days=14)
            if config == "legacy":
                kw["next_delivery_days"] = ndd      # what the surfaces passed pre-9dca72cf
            else:
                kw["data_dir"] = DATA_DIR           # LATA + AMIT
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


def divergence(sa, sb, ag):
    """How far two plans are apart, each as a percentage."""
    union = ag["shared"] + ag["only_a"] + ag["only_b"]
    return {
        "lines_pct": 100.0 * abs(sa["lines"] - sb["lines"]) / max(sa["lines"], sb["lines"], 1),
        "units_pct": 100.0 * abs(sa["units"] - sb["units"]) / max(sa["units"], sb["units"], 1.0),
        "donor_disagree_pct": (100.0 - ag["same_donor"]) if ag["shared"] else 0.0,
        "qty_gap_pct": ag["qty_gap"],
        "unshared_pct": 100.0 * (ag["only_a"] + ag["only_b"]) / max(union, 1),
    }


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", default=None, help="run just one scenario")
    p.add_argument("--source", choices=("seed", "odoo"), default="seed",
                   help="'odoo' reads the live depot through OdooAdapter — the "
                        "same path the product uses; 'seed' reconstructs it "
                        "offline and needs no container")
    args = p.parse_args(argv)

    from analyse_transfer_funnel import load_from_seed, load_from_odoo
    import oasis.desktop.data as D
    from devkit.methodology.traps import like_for_like, regime, run_all

    seed, data = load_from_odoo() if args.source == "odoo" else load_from_seed()
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
    print("SHIPPED (build_transfer_service)  vs  DERIVED (LATA + AMIT)  [+ legacy]")
    print(f"source: {args.source}   supplier coverage: calendar {len(ndd)} | LATA 599")
    print("=" * 78)

    per, diverged, traps = {}, [], []
    for label, codes in scen.items():
        _, a = run("shipped", names, data, coords, codes, ndd)
        _, b = run("derived", names, data, coords, codes, ndd)
        _, lg = run("legacy", names, data, coords, codes, ndd)
        sa, sb, sl = summarise(a), summarise(b), summarise(lg)
        dead = dead_units(data, codes)
        ag, ag_legacy = agreement(a, b), agreement(lg, b)

        # like for like: every arm scans the SAME stock rows of the same stores
        base = float(sum(len(data[c]) for c in codes))
        traps += run_all([like_for_like(("shipped rows scanned", base),
                                        ("derived rows scanned", base),
                                        f"{label}: deficit population")], strict=True)

        print(f"\n{label}  ({len(codes)} stores)")
        print(f"  {'':<14}{'lines':>8}{'units':>10}{'PULL u':>10}{'PUSH u':>10}"
              f"{'fresh u':>10}{'dry u':>10}{'dead cleared':>14}")
        for nm, s, opps in (("SHIPPED", sa, a), ("DERIVED", sb, b), ("legacy", sl, lg)):
            dd = dead_moved(opps, data, codes)
            pct = (100.0 * dd / dead) if dead else 0.0
            print(f"  {nm:<14}{s['lines']:>8,}{s['units']:>10,.0f}"
                  f"{s['pull_u']:>10,.0f}{s['push_u']:>10,.0f}"
                  f"{s['fresh_u']:>10,.0f}{s['dry_u']:>10,.0f}"
                  f"{pct:>13.1f}%")
        print(f"  shipped vs derived: {ag['shared']:,} shared (SKU,recipient) | same donor "
              f"{ag['same_donor']:.0f}% | median qty gap {ag['qty_gap']:.0f}% | "
              f"only shipped {ag['only_a']:,}, only derived {ag['only_b']:,}")
        print(f"  legacy  vs derived: same donor {ag_legacy['same_donor']:.0f}% | median qty gap "
              f"{ag_legacy['qty_gap']:.0f}% | only legacy {ag_legacy['only_a']:,}, "
              f"only derived {ag_legacy['only_b']:,}")
        dv, dv_legacy = divergence(sa, sb, ag), divergence(sl, sb, ag_legacy)
        over = [k for k, v in dv.items() if v > DIVERGE[k]]
        if over:
            diverged.append(label)
        per[label] = {"stores": len(codes),
                      "shipped_vs_derived": {k: round(v, 2) for k, v in dv.items()},
                      "legacy_vs_derived": {k: round(v, 2) for k, v in dv_legacy.items()},
                      "over_threshold": over}

    traps += run_all([regime("transfer plan agreement", "transfer", "transfer")], strict=True)
    print(json.dumps({
        "claim": "claim.transfer.network-vs-derived-diverge",
        "verdict": "supports" if diverged else "contradicts",
        "metric": {"thresholds_pct": DIVERGE, "scenarios": per, "diverged_in": diverged},
        "baseline": "derived methodology (data_dir: LATA horizons + AMIT tiers)",
        "beat_baseline": None,
        "held_out": False,
        "traps": sorted({r.trap for r in traps}),
        "notes": ("shipped = oasis.desktop.data.build_transfer_service as every surface calls it "
                  "since 9dca72cf; the pre-fix wiring is reported as legacy_vs_derived"),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
