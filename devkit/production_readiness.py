"""Is the ordering engine fit to run a store? Full book, full stock range.

WHAT THIS ANSWERS THAT THE EARLIER RUNS DID NOT
    order_state_census walks the book at ONE stock position and says what
    decided each line. multistore_run compares configurations. Neither asks
    the operational question: does the engine hold up across the whole range
    of shelf positions a real store passes through in a month -- cold after a
    stockout week, drawn down, normal, and overbought after a buyer ran hot?

    A policy can look excellent at the position it was tuned on and fall over
    at 0.25x. So this sweeps the position and reports service, capital and
    waste at each, per store size, on the full 15,037-line book.

THE STORES
    Fifteen, spanning 0.3x to 2.5x of the observed book. One store's demand
    scaled, not fifteen independent histories -- there is only one branch of
    real data. That is a real limit on what this can claim (a chain's stores
    differ in mix, not only in size) and it is the honest treatment of what
    exists. What it DOES test is the thing the methodology claims: that the
    same per-store logic holds at every store size, which is exactly what
    scaling probes.

WHY S IS COMPUTED ONCE PER STORE AND NOT PER STOCK POSITION
    S = d(R+L) + z*sigma_P. It is a property of the supply cycle and does not
    move when the shelf does; only Q = S - position moves. Recomputing it per
    position would cost 105 enrichment passes to re-derive identical numbers.

COMMON RANDOM NUMBERS
    Demand and lead times are drawn once per store and REUSED across every
    stock position, so a difference between positions is the position and not
    the draw.

ASSERTED, NOT MEASURED
    shelf life by department; GEN_CV 0.40 for the generator; the opening
    position at each multiplier; the store-size spread.

USAGE
    python devkit/production_readiness.py [--days 28] [--stores 15]
"""
from __future__ import annotations

import argparse
import copy
import math
import os
import statistics
import sys
import zlib
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit.order_model_compare import build_book, DATA_DIR, band_of  # noqa: E402
from devkit.multistore_run import GEN_CV, HOLDING_RATE               # noqa: E402
from oasis.logic import order_up_to as ou                            # noqa: E402

#: Fifteen store sizes across a plausible chain spread.
STORES = (0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4,
          1.6, 1.8, 2.0, 2.2, 2.5)

#: Shelf positions a real store actually passes through.
STOCK_POINTS = (0.0, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0)


def levels(enriched, scale):
    """S, R, L per SKU for one store, from the real production call."""
    from oasis.logic.simulation_bridge import SimulationOrderUtil
    util = SimulationOrderUtil(DATA_DIR)
    arm = copy.deepcopy(enriched)
    for p in arm:
        p["avg_daily_sales"] = float(p.get("avg_daily_sales") or 0) * scale
    os.environ["OASIS_ORDER_MODEL"] = "order_up_to"
    recs = util.finalize_orders(util.calculate_order_quantity(arm, use_real_date=True))
    os.environ.pop("OASIS_ORDER_MODEL", None)
    out = {}
    for r in recs:
        k = r.get("sku") or r.get("item_code")
        t = r.get("order_up_to_terms") or {}
        out[k] = {
            "S": float(t.get("S") or 0), "R": float(t.get("R") or 7),
            "L": float(t.get("L") or 2), "d": float(r.get("avg_daily_sales") or 0),
            "suppressed": bool(r.get("auto_order_suppressed")),
            "feasible": t.get("feasible", True),
        }
    return out


def draw(book, scale, days, seed):
    """Demand and lead streams for one store, reused at every stock point."""
    rs = np.random.default_rng(zlib.crc32(f"store-{seed}".encode()) % (2**32))
    n = len(book)
    d = np.array([p["avg_daily_sales"] * scale for p in book])
    sig = math.sqrt(math.log(1 + GEN_CV ** 2))
    dem = rs.poisson(np.maximum(d, 1e-9)[:, None] * np.ones((n, days))) \
        * rs.lognormal(-0.5 * sig * sig, sig, (n, days))
    return dem, rs


def simulate(book, lv, shelf, dem, leads, mult, days, scale):
    """One store at one stock position. FIFO expiry, periodic (R, S) review."""
    sold = want_tot = waste = 0.0
    stock_days = gp_earned = cogs = 0.0
    lost_by_dept = defaultdict(float)
    never_reviewed = 0

    for i, p in enumerate(book):
        k = p["sku"]
        L_ = lv.get(k)
        if not L_ or L_["d"] <= 0:
            continue
        S, R, Lt = L_["S"], L_["R"], L_["L"]
        d = L_["d"]
        cost, gp = p["unit_cost"], p["gross_profit_per_unit"]
        sl = shelf.get(p["department"]) or 0.0
        if R >= days:
            never_reviewed += 1

        # Scale the OPENING shelf by the store size as well as the position
        # multiplier. Without the store scale a 0.3x shop opened with a
        # full-size shelf, so held stock came out nearly flat across store
        # sizes (52.3m at 0.3x against 49.9m at 0.5x) and the carrying charge
        # buried the small stores' EP.
        batches = [[sl if sl > 0 else float("inf"),
                    p["current_stock"] * mult * scale]]
        pipeline = []
        for t in range(days):
            for arr, q in [x for x in pipeline if x[0] <= t]:
                batches.append([t + sl if sl > 0 else float("inf"), q])
            pipeline = [x for x in pipeline if x[0] > t]

            keep = []
            for b in batches:
                if b[0] <= t:
                    waste += b[1] * cost
                elif b[1] > 1e-12:
                    keep.append(b)
            batches = sorted(keep)

            w = float(dem[i, t])
            took = 0.0
            for b in batches:
                if took >= w:
                    break
                use = min(b[1], w - took)
                b[1] -= use
                took += use
            batches = [b for b in batches if b[1] > 1e-12]
            on_hand = sum(b[1] for b in batches)
            sold += took
            want_tot += w
            if w > took:
                lost_by_dept[p["department"]] += (w - took) * gp
            gp_earned += took * gp
            cogs += took * cost
            stock_days += on_hand * cost

            if not L_["suppressed"] and t % max(1, int(round(R))) == 0:
                pos = on_hand + sum(q for _, q in pipeline)
                if S - pos > 0:
                    pipeline.append((t + leads[i, t], S - pos))

    avg_stock = stock_days / days
    yr = days / 365.0
    return {
        "service": sold / want_tot if want_tot > 0 else 0.0,
        "stock": avg_stock, "gp": gp_earned / yr, "waste": waste / yr,
        "ep": gp_earned / yr - HOLDING_RATE * avg_stock - waste / yr,
        "gmroi": (gp_earned / yr) / avg_stock if avg_stock > 0 else 0.0,
        "lost": lost_by_dept, "never_reviewed": never_reviewed,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=28)
    ap.add_argument("--stores", type=int, default=len(STORES))
    a = ap.parse_args(argv)

    book = build_book()
    shelf = {}
    for p in book:
        if p["department"] not in shelf:
            shelf[p["department"]] = ou.shelf_life_for(p["department"]) or 0.0

    from oasis.logic.simulation_bridge import SimulationOrderUtil
    util = SimulationOrderUtil(DATA_DIR)
    enriched = util.prepare_sku_data(copy.deepcopy(book))
    print(f"{len(book):,} SKUs | {a.stores} stores | "
          f"{len(STOCK_POINTS)} stock points | {a.days} days\n")

    mix = STORES[:a.stores]
    grid = defaultdict(list)
    diag = {"suppressed": 0, "infeasible": 0, "never_reviewed": 0, "n": 0}
    lost_all = defaultdict(float)

    for si, scale in enumerate(mix):
        lv = levels(enriched, scale)
        diag["suppressed"] += sum(1 for v in lv.values() if v["suppressed"])
        diag["infeasible"] += sum(1 for v in lv.values() if not v["feasible"])
        diag["n"] += len(lv)
        dem, rs = draw(book, scale, a.days, si)
        leads = np.maximum(0.5, rs.normal(
            np.array([lv.get(p["sku"], {"L": 2})["L"] for p in book])[:, None],
            1.0, (len(book), a.days))).astype(int)
        for mult in STOCK_POINTS:
            r = simulate(book, lv, shelf, dem, leads, mult, a.days, scale)
            grid[mult].append(r)
            diag["never_reviewed"] = r["never_reviewed"]
            for dpt, v in r["lost"].items():
                lost_all[dpt] += v
        print(f"  store {si+1:>2}/{len(mix)} (x{scale})  done")

    print("\n" + "=" * 84)
    print("ACROSS THE STOCK RANGE — chain totals over %d stores" % len(mix))
    print("=" * 84)
    print(f"  {'opening stock':>14}{'service':>10}{'stock KES':>15}"
          f"{'GP/yr':>15}{'waste/yr':>14}{'EP/yr':>15}")
    for mult in STOCK_POINTS:
        rs_ = grid[mult]
        print(f"  {mult:>13.2f}x{100*statistics.mean(r['service'] for r in rs_):>9.2f}%"
              f"{sum(r['stock'] for r in rs_):>15,.0f}"
              f"{sum(r['gp'] for r in rs_):>15,.0f}"
              f"{sum(r['waste'] for r in rs_):>14,.0f}"
              f"{sum(r['ep'] for r in rs_):>15,.0f}")

    print("\n" + "=" * 84)
    print("SERVICE BY STORE SIZE (at 1.00x opening stock)")
    print("=" * 84)
    at1 = grid[1.0]
    print(f"  {'store':>8}{'scale':>8}{'service':>10}{'stock KES':>14}{'EP/yr':>15}")
    for i, (scale, r) in enumerate(zip(mix, at1)):
        print(f"  {i+1:>8}{scale:>8.1f}{100*r['service']:>9.2f}%"
              f"{r['stock']:>14,.0f}{r['ep']:>15,.0f}")

    print("\n" + "=" * 84)
    print("WHERE THE LOST MARGIN IS (all stores, all stock points)")
    print("=" * 84)
    tot = sum(lost_all.values()) or 1.0
    for dpt, v in sorted(lost_all.items(), key=lambda x: -x[1])[:12]:
        print(f"  {dpt[:30]:<30}{v:>16,.0f}{100*v/tot:>8.1f}%")

    n = max(1, diag["n"])
    print("\n" + "=" * 84)
    print("STRUCTURAL DIAGNOSTICS")
    print("=" * 84)
    print(f"  dead-stock suppressions   {diag['suppressed']/len(mix):>10,.0f} lines/store"
          f"  ({100*diag['suppressed']/n:.1f}%)")
    print(f"  infeasible (shelf < R+L)  {diag['infeasible']/len(mix):>10,.0f} lines/store"
          f"  ({100*diag['infeasible']/n:.1f}%)")
    print(f"  never reviewed in {a.days}d     {diag['never_reviewed']:>10,} lines"
          f"   <-- R exceeds the window; this run cannot speak for them")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
