"""Multi-store outcome simulation, driven by the PRODUCTION order path.

WHY NOT engine_ablation
    That harness calls ou.recommend() directly, so it can only ever exercise
    the derived model -- and it builds its inputs the way the devkit sweep
    does, with default_patterns and a 2-day lead-time fallback. That
    construction is what reports 666 infeasible lines where the production
    path, using the measured lead-time cache, reports 147. A harness that
    disagrees with the engine about L cannot settle a question about service.

    So this takes S and R from SimulationOrderUtil.calculate_order_quantity --
    the same call the console makes, including the is_enabled() fork -- and
    simulates the inventory process those levels imply.

WHY SIMULATING THE POLICY IS THE RIGHT SCOPE
    An (R, S) policy IS S and R. The engine's whole job is to choose them; the
    rest is arithmetic on realised demand. Re-invoking the engine every
    simulated review period would cost hundreds of enrichment passes to
    re-derive numbers that do not change, so the levels are taken once per
    store per arm and the process is then simulated honestly against them.

NOT CIRCULAR, DELIBERATELY
    The demand LAW that generates sales is fixed and identical for every arm,
    and is NOT the law any arm plans with. An earlier version of this
    comparison generated demand from the same cv the planner assumed, which
    compares two worlds rather than two policies and inflated whichever policy
    shared the generator's assumption. Common random numbers: every arm sees
    the byte-identical demand and lead-time streams, so a difference is the
    policy and never the noise.

WHAT IS ASSERTED RATHER THAN MEASURED
    shelf life   by department -- no code-date data exists
    GEN_CV       the generating overdispersion, 0.40, same for all arms
    store mix    five stores at 0.4x-2.0x of the observed book
    opening stock  seeded; there is no historical on-hand series to replay

USAGE
    python devkit/multistore_run.py [--stores 5] [--seeds 3] [--days 364]
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

from devkit.order_model_compare import build_book, DATA_DIR  # noqa: E402
from oasis.logic import order_up_to as ou                    # noqa: E402

#: Store size multipliers. A chain is not five copies of one shop, and the
#: slow tail behaves differently at 0.4x than at 2.0x.
STORE_MIX = (0.4, 0.7, 1.0, 1.4, 2.0)

#: Overdispersion of the GENERATOR. Fixed, shared by every arm, and not read
#: from the planner's phi -- that identity is what made the earlier result
#: circular.
GEN_CV = 0.40

#: Cost of capital, annual. Used only to price held stock in EP.
HOLDING_RATE = 0.1438


def levels_for(book, scale, static_demand, model):
    """S, R, L per SKU from the real production call, for one store/arm."""
    from oasis.logic.simulation_bridge import SimulationOrderUtil

    products = copy.deepcopy(book)
    for p in products:
        p["avg_daily_sales"] = p["avg_daily_sales"] * scale
        p["current_stock"] = p["current_stock"] * scale
        if static_demand:
            p["avg_daily_sales"] = 0.0      # force the forecast-file fallback
            p.pop("ads_source", None)

    util = SimulationOrderUtil(DATA_DIR)
    enriched = util.prepare_sku_data(products)
    os.environ["OASIS_ORDER_MODEL"] = "order_up_to" if model == "derived" else "classic"
    recs = util.finalize_orders(util.calculate_order_quantity(
        enriched, use_real_date=True))
    os.environ.pop("OASIS_ORDER_MODEL", None)

    out = {}
    for r in recs:
        k = r.get("sku") or r.get("item_code")
        t = r.get("order_up_to_terms") or {}
        if t:
            # Derived: a PERIODIC (R, S) policy. Review every R days, order up
            # to S. No reorder point -- exposure is R + L by construction.
            S = float(t.get("S") or 0)
            R = float(t.get("R") or 7)
            L = float(t.get("L") or 2)
            rop = None
        else:
            # Classic is NOT periodic, and simulating it as though it were is
            # how you hand the derived model a win it did not earn. It carries
            # a reorder_point and a target_stock -- an (s, S) policy reviewed
            # continuously. Take the engine's own two numbers rather than
            # rebuilding them from a coverage figure, and review it daily.
            S = float(r.get("target_stock") or 0)
            if S <= 0:
                S = float(r.get("avg_daily_sales") or 0) * float(
                    r.get("target_coverage_days") or 7)
            rop = float(r.get("reorder_point") or 0)
            R = 1.0
            L = float(r.get("lead_time_days")
                      or r.get("estimated_delivery_days") or 2)
        out[k] = (max(0.0, S), max(1.0, R), max(0.5, L),
                  bool(r.get("auto_order_suppressed")), rop)
    return out


def simulate(book, levels, scale, shelf, rng, days):
    """One store, one arm, one seed. Returns the outcome totals."""
    sold = demand_tot = waste_u = 0.0
    stock_days = 0.0
    gp_earned = cogs = 0.0

    for p in book:
        k = p["sku"]
        lv = levels.get(k)
        if not lv:
            continue
        S, R, L, suppressed, rop = lv
        d = p["avg_daily_sales"] * scale
        if d <= 0:
            continue
        cost, gp = p["unit_cost"], p["gross_profit_per_unit"]
        sl = shelf.get(p["department"]) or 0.0

        # FIFO batches, each with the day it dies. Truncating "anything above
        # d * shelf_life" once a day instead looks like an expiry model and is
        # not one: under a daily-review policy holding 7 days of cover on a
        # 2-day product it re-kills the same excess every single day, which
        # charged 680m of waste against 425m of gross profit and made the
        # classic arm look catastrophic for a reason that was mine, not its.
        # Stock ages; it does not teleport past its date.
        batches = []                       # [expiry_day, qty]
        if sl > 0:
            batches.append([sl, p["current_stock"] * scale])
        else:
            batches.append([float("inf"), p["current_stock"] * scale])
        pipeline = []                      # (arrival_day, qty)
        # Common random numbers: one stream per SKU, drawn identically for
        # every arm. crc32, not hash() -- Python randomises string hashing per
        # process, so hash() would give the same run different answers on
        # different days and silently break reproducibility.
        rs = np.random.default_rng(
            (zlib.crc32(k.encode("utf-8")) ^ (int(rng) * 0x9E3779B1)) % (2**32))
        dem = rs.poisson(d, days) * rs.lognormal(
            -0.5 * math.log(1 + GEN_CV ** 2),
            math.sqrt(math.log(1 + GEN_CV ** 2)), days)
        lead = np.maximum(0.5, rs.normal(L, max(0.1, 0.3 * L), days))

        for t in range(days):
            for arr, q in [x for x in pipeline if x[0] <= t]:
                batches.append([t + sl if sl > 0 else float("inf"), q])
            pipeline = [x for x in pipeline if x[0] > t]

            # Expire first: what died overnight cannot serve today's demand.
            still = []
            for b in batches:
                if b[0] <= t:
                    waste_u += b[1] * cost
                elif b[1] > 1e-12:
                    still.append(b)
            batches = sorted(still)

            want = float(dem[t])
            take = 0.0
            for b in batches:              # FIFO: oldest sells first
                if take >= want:
                    break
                use = min(b[1], want - take)
                b[1] -= use
                take += use
            batches = [b for b in batches if b[1] > 1e-12]
            on_hand = sum(b[1] for b in batches)
            sold += take
            demand_tot += want
            gp_earned += take * gp
            cogs += take * cost

            stock_days += on_hand * cost

            if not suppressed and t % max(1, int(round(R))) == 0:
                pos = on_hand + sum(q for _, q in pipeline)
                # (s, S) for classic: only order once the position has fallen
                # through the reorder point. (R, S) for derived: every review
                # tops up, which is what the R + L protection interval assumes.
                if rop is None or pos <= rop:
                    q = S - pos
                    if q > 0:
                        pipeline.append((t + lead[t], q))

    avg_stock = stock_days / days
    years = days / 365.0
    gp_yr = gp_earned / years
    waste_yr = waste_u / years
    return {
        "service": (sold / demand_tot) if demand_tot > 0 else 0.0,
        "stock": avg_stock,
        "gp": gp_yr,
        "waste": waste_yr,
        "ep": gp_yr - HOLDING_RATE * avg_stock - waste_yr,
        "gmroi": (gp_yr / avg_stock) if avg_stock > 0 else 0.0,
        "turns": ((cogs / years) / avg_stock) if avg_stock > 0 else 0.0,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stores", type=int, default=len(STORE_MIX))
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--days", type=int, default=364)
    ap.add_argument("--skus", type=int, default=0, help="0 = the whole book")
    a = ap.parse_args(argv)

    # The pipeline's own ADS. See order_model_compare.pipeline_ads: the
    # snapshot file measures a different window of the same shop and the two
    # agree on 3.9% of lines, so a multi-store run against it answers for a
    # book nothing that ships reads.
    book = build_book("pipeline")
    print("demand     : the pipeline's (POS-weighted, as the shipped run "
          "measures it)")
    if a.skus:
        book = sorted(book, key=lambda p: -p["avg_daily_sales"])[:a.skus]
    shelf = {}
    for p in book:
        dept = p["department"]
        if dept not in shelf:
            shelf[dept] = ou.shelf_life_for(dept) or 0.0
    mix = STORE_MIX[:a.stores]
    print(f"{len(book):,} SKUs x {len(mix)} stores x {a.seeds} seeds x "
          f"{a.days} days\n  store mix: {mix}\n")

    arms = [("static classic", True, "classic"),
            ("static derived", True, "derived"),
            ("measured classic", False, "classic"),
            ("measured derived", False, "derived")]

    results = {}
    for name, static, model in arms:
        agg = defaultdict(list)
        for scale in mix:
            lv = levels_for(book, scale, static, model)
            for seed in range(a.seeds):
                r = simulate(book, lv, scale, shelf, seed, a.days)
                for kk, vv in r.items():
                    agg[kk].append(vv)
        results[name] = {kk: statistics.mean(vv) for kk, vv in agg.items()}
        # Sums, not means, for the money: five stores earn five stores' profit.
        for kk in ("stock", "gp", "waste", "ep"):
            results[name][kk] = sum(agg[kk]) / a.seeds
        print(f"  {name:<18} done")

    print("\n" + "=" * 92)
    print("MULTI-STORE OUTCOMES — chain totals, common random numbers")
    print("=" * 92)
    print(f"  {'arm':<18}{'service':>9}{'stock KES':>14}{'GP/yr':>15}"
          f"{'waste/yr':>13}{'EP/yr':>15}{'GMROI':>8}{'turns':>7}")
    for name, _, _ in arms:
        r = results[name]
        print(f"  {name:<18}{100*r['service']:>8.2f}%{r['stock']:>14,.0f}"
              f"{r['gp']:>15,.0f}{r['waste']:>13,.0f}{r['ep']:>15,.0f}"
              f"{r['gmroi']:>8.2f}{r['turns']:>7.2f}")

    sc, md = results["static classic"], results["measured derived"]
    print(f"\n  where we were -> where we are:")
    print(f"    service {100*sc['service']:.2f}% -> {100*md['service']:.2f}%"
          f"  ({100*(md['service']-sc['service']):+.2f} pp)")
    print(f"    EP/yr   {sc['ep']:,.0f} -> {md['ep']:,.0f}"
          f"  ({md['ep']-sc['ep']:+,.0f})")
    print(f"    stock   {sc['stock']:,.0f} -> {md['stock']:,.0f}"
          f"  ({md['stock']-sc['stock']:+,.0f})")
    print(f"    waste   {sc['waste']:,.0f} -> {md['waste']:,.0f}"
          f"  ({md['waste']-sc['waste']:+,.0f})")
    best = max(results.items(), key=lambda x: x[1]["ep"])
    print(f"\n  ranked on EP, the winner is: {best[0]}  ({best[1]['ep']:,.0f}/yr)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
