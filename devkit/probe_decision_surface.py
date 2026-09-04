"""Probe: does the ordering engine's decision surface behave like an order policy?

Not "is the quantity right" — nothing here knows what will sell. A different and
answerable question: across every SKU, store and stock position the procurement
agents visited, do the decisions obey the properties an order policy MUST obey?

    P1  monotone in stock      more on hand can never mean a larger order
    P2  non-negative           an order is never a return
    P3  decomposable           S = cycle + safety, unless a clamp says otherwise
    P4  on-order credited      stock on the water counts, one for one
    P5  covers the interval    an empty shelf is ordered at least P days of cover
    P6  sigma_L bites          the term the derivation says is missing must move
                               the answer once supplied, or supplying it is theatre

A property violated by one line in twenty thousand is worth more than an average
over all of them: the average is a summary, the violation is a bug with an
address.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from oasis.logic.decision_ledger import DecisionLedger      # noqa: E402
from devkit.methodology.traps import ratio, run_all         # noqa: E402

TOL = 1e-6


def group(rows):
    """(store, sku, on_order_mult) -> rows sorted by on-hand."""
    g = defaultdict(list)
    for r in rows:
        i = r["inputs"]
        g[(r["org"], r["entity"], i["on_order_mult"])].append(r)
    for k in g:
        g[k].sort(key=lambda r: r["inputs"]["on_hand_mult"])
    return g


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--skus", default="400")
    ap.add_argument("--stores", type=int, default=5)
    ap.add_argument("--xlsx", default=None)
    ap.add_argument("--no-sweep", action="store_true")
    a = ap.parse_args(argv)

    # Run the team twice — once with sigma_L measured per vendor, once with the
    # single chain constant — so P6 has something to compare.
    if not a.no_sweep:
        for src in ("observed", "default"):
            subprocess.run(
                [sys.executable, str(ROOT / "devkit" / "procurement_sweep.py"),
                 "--skus", str(a.skus), "--stores", str(a.stores),
                 "--sigma-source", src]
                + (["--xlsx", a.xlsx] if a.xlsx else [])
                + (["--db", a.db] if a.db else []),
                cwd=str(ROOT), capture_output=True, text=True)

    led = DecisionLedger(a.db)
    rows = led.decisions(kind="order", limit=500_000)
    by_cfg = defaultdict(list)
    for r in rows:
        by_cfg[r["config_hash"]].append(r)
    print(f"  {len(rows):,} decisions across {len(by_cfg)} configuration(s)")

    cfgs = {}
    for h, rs in by_cfg.items():
        src = (rs[0].get("params") or {}).get("sigma_source", "?")
        cfgs[src] = rs
        print(f"    {src:<9} {h}  {len(rs):,} decisions")

    main_rows = cfgs.get("observed") or next(iter(by_cfg.values()))
    groups = group(main_rows)

    # ---- P1 monotone, P2 non-negative, P3 decomposable, P4 on-order --------
    v1 = v2 = v3 = v4 = v5 = 0
    ex1 = ex3 = ex5 = None
    for key, rs in groups.items():
        prev = None
        for r in rs:
            q = float(r["quantity"] or 0)
            i = r["inputs"]
            if q < -TOL:
                v2 += 1
            if prev is not None and q > prev + 1e-6:
                v1 += 1
                ex1 = ex1 or {"store": key[0], "sku": key[1],
                              "on_hand_mult": i["on_hand_mult"],
                              "q_prev": prev, "q": q}
            prev = q
            if not i.get("clamped"):
                lhs, rhs = i["S"], i["cycle_stock"] + i["safety_stock"]
                if rhs > 0 and abs(lhs - rhs) / rhs > 1e-6:
                    v3 += 1
                    ex3 = ex3 or {"sku": key[1], "S": lhs, "cycle+safety": rhs}
            # P5: an empty shelf must be ordered at least the protection interval
            if i["on_hand_mult"] == 0 and i["on_order_mult"] == 0:
                if i["d"] > 0 and (q / i["d"]) < i["P"] - 1e-6:
                    v5 += 1
                    ex5 = ex5 or {"sku": key[1], "cover_ordered": q / i["d"],
                                  "P": i["P"]}

    # P4: on-order is credited one for one
    pairs = defaultdict(dict)
    for r in main_rows:
        i = r["inputs"]
        pairs[(r["org"], r["entity"], i["on_hand_mult"])][i["on_order_mult"]] = r
    checked4 = 0
    for k, d in pairs.items():
        if 0.0 in d and 0.5 in d:
            a0, a5 = d[0.0], d[0.5]
            q0, q5 = float(a0["quantity"]), float(a5["quantity"])
            oo = a5["inputs"]["on_order"]
            expect = max(0.0, q0 - oo)
            checked4 += 1
            if abs(q5 - expect) > max(1.0, 0.001 * max(q0, 1)):
                v4 += 1

    n = len(main_rows)
    print(f"  P1 monotone in stock      violations {v1:,} / {n:,}"
          + (f"   e.g. {ex1}" if ex1 else ""))
    print(f"  P2 non-negative           violations {v2:,}")
    print(f"  P3 S = cycle + safety     violations {v3:,}"
          + (f"   e.g. {ex3}" if ex3 else ""))
    print(f"  P4 on-order credited      violations {v4:,} / {checked4:,} pairs")
    print(f"  P5 empty shelf gets P     violations {v5:,}"
          + (f"   e.g. {ex5}" if ex5 else ""))

    # ---- P6 does sigma_L actually move the answer? -------------------------
    p6 = {}
    if "observed" in cfgs and "default" in cfgs:
        def keyed(rs):
            return {(r["org"], r["entity"], r["inputs"]["on_hand_mult"],
                     r["inputs"]["on_order_mult"]): r for r in rs}
        A, B = keyed(cfgs["observed"]), keyed(cfgs["default"])
        shared = sorted(set(A) & set(B))
        diffs, safety_ratio, moved = [], [], 0
        for k in shared:
            qa, qb = float(A[k]["quantity"]), float(B[k]["quantity"])
            sa = A[k]["inputs"]["safety_stock"]
            sb = B[k]["inputs"]["safety_stock"]
            if qb > 0:
                diffs.append((qa - qb) / qb)
            if sb > 0:
                safety_ratio.append(sa / sb)
            if abs(qa - qb) > 1e-6:
                moved += 1
        p6 = {"compared": len(shared), "quantity_changed": moved,
              "share_changed": round(moved / max(len(shared), 1), 4),
              "median_safety_ratio": (round(st.median(safety_ratio), 4)
                                      if safety_ratio else None),
              "median_qty_delta_pct": (round(100 * st.median(diffs), 3)
                                       if diffs else None)}
        print(f"  P6 sigma_L bites          {moved:,}/{len(shared):,} decisions "
              f"changed · median safety ratio "
              f"{p6['median_safety_ratio']} · median qty delta "
              f"{p6['median_qty_delta_pct']}%")
        run_all([ratio(p6["median_safety_ratio"] or 1, 1.0,
                       "safety stock: observed sigma_L vs chain default",
                       lo=0.2, hi=5.0)])

    common = {"held_out": False, "provenance": "observed",
              "sources": ["source.fulfilment-detail", "source.corrected-ads",
                          "source.supplier-weekly-schedule"],
              "traps": ["T3"]}
    total_v = v1 + v2 + v3 + v4 + v5
    print(json.dumps({
        "claim": "claim.ordering.decision-surface-is-well-behaved",
        "verdict": "supports" if total_v == 0 else "contradicts",
        "metric": {"decisions": n, "groups": len(groups),
                   "P1_monotone": v1, "P2_non_negative": v2,
                   "P3_decomposable": v3, "P4_on_order_credited": v4,
                   "P4_pairs_checked": checked4, "P5_covers_interval": v5,
                   "examples": {"P1": ex1, "P3": ex3, "P5": ex5}},
        "baseline": "the properties an order policy must obey",
        "beat_baseline": None, **common,
        "notes": (f"{n:,} decisions across {len(groups):,} (store, SKU, "
                  "on-order) groups obey all five structural properties."
                  if total_v == 0 else
                  f"{total_v:,} property violations across {n:,} decisions: "
                  f"monotonicity {v1}, non-negativity {v2}, decomposition {v3}, "
                  f"on-order credit {v4}, interval cover {v5}.")}))

    if p6:
        # Incidence alone is not materiality. A term that moves a tenth of
        # decisions by nothing is bookkeeping; the median ratio has to move too.
        sr = p6["median_safety_ratio"] or 1.0
        bites = (p6["share_changed"] or 0) > 0.05 and abs(sr - 1.0) > 0.02
        print(json.dumps({
            "claim": "claim.ordering.sigma-L-changes-the-order",
            "verdict": "supports" if bites else "contradicts",
            "metric": p6,
            "baseline": "chain-wide sigma_L = 2.22d for every supplier",
            "beat_baseline": None, **common,
            "notes": (
                f"Measuring sigma_L per vendor changes {p6['share_changed']:.1%} "
                f"of decisions; median safety stock moves to "
                f"{p6['median_safety_ratio']}x of the chain-default figure and "
                f"the median order by {p6['median_qty_delta_pct']}%. The term is "
                "not cosmetic."
                if bites else
                f"Per-vendor sigma_L changes only {p6['share_changed']:.1%} of "
                "decisions. Supplying it is defensible bookkeeping, not a "
                "material change to what gets bought.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
