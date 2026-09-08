"""Is z calibrated? Two service measures, because they are not the same one.

THE TRAP THIS EXISTS TO AVOID
    z = 1.28 sets a CYCLE SERVICE LEVEL: P(demand over R+L <= S) = Phi(1.28)
    = 0.90. It is a probability of surviving a cycle without running out.

    What every run so far reported is a FILL RATE: units served / units
    demanded. Those are different quantities, and fill rate is structurally
    HIGHER, because a cycle that stocks out on its last day still served
    almost all of its units. Reading 93.64% fill against a 90% target and
    concluding the model over-serves compares a rate to a probability.

    So this measures both, plus the theoretical Phi(z), and sweeps z. If cycle
    service tracks Phi(z), z is doing its job and the fill rate is simply what
    that implies. If it runs far above, z is genuinely too high.

WHAT IS SWEPT
    z through the tabulated service levels. S is recomputed per z per store
    from the real production call, so the whole chain moves with it -- not
    just the safety term in isolation.

    Common random numbers across every z, so a difference is the policy.

USAGE
    python devkit/z_calibration.py [--stores 5] [--days 180]
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

from devkit.order_model_compare import build_book, DATA_DIR   # noqa: E402
from devkit.multistore_run import GEN_CV, HOLDING_RATE        # noqa: E402
from oasis.logic import order_up_to as ou                     # noqa: E402

STORES = (0.4, 0.7, 1.0, 1.5, 2.2)
SERVICE_LEVELS = (0.80, 0.85, 0.90, 0.95, 0.99)


def levels(enriched, scale):
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
        t = r.get("order_up_to_terms") or {}
        out[r.get("sku") or r.get("item_code")] = {
            "S": float(t.get("S") or 0), "R": float(t.get("R") or 7),
            "L": float(t.get("L") or 2), "d": float(r.get("avg_daily_sales") or 0),
            "suppressed": bool(r.get("auto_order_suppressed")),
        }
    return out


def simulate(book, lv, shelf, scale, days, seed):
    """Fill rate AND cycle service, on the same run."""
    sold = want = waste = 0.0
    stock_days = gp_earned = 0.0
    cycles = cycles_ok = 0

    rs = np.random.default_rng(zlib.crc32(f"z-{seed}".encode()) % (2**32))
    n = len(book)
    dvec = np.array([p["avg_daily_sales"] * scale for p in book])
    sig = math.sqrt(math.log(1 + GEN_CV ** 2))
    dem = rs.poisson(np.maximum(dvec, 1e-9)[:, None] * np.ones((n, days))) \
        * rs.lognormal(-0.5 * sig * sig, sig, (n, days))
    leadraw = np.maximum(1, rs.normal(3.0, 1.0, (n, days))).astype(int)

    for i, p in enumerate(book):
        L_ = lv.get(p["sku"])
        if not L_ or L_["d"] <= 0:
            continue
        S, R = L_["S"], max(1, int(round(L_["R"])))
        cost, gp = p["unit_cost"], p["gross_profit_per_unit"]
        sl = shelf.get(p["department"]) or 0.0
        batches = [[sl if sl > 0 else float("inf"), p["current_stock"] * scale]]
        pipeline = []
        cyc_short = False

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
            want += w
            if w - took > 1e-9:
                cyc_short = True          # this cycle failed
            gp_earned += took * gp
            stock_days += on_hand * cost

            if t % R == 0:
                if t > 0:
                    cycles += 1
                    cycles_ok += 0 if cyc_short else 1
                    cyc_short = False
                if not L_["suppressed"]:
                    pos = on_hand + sum(q for _, q in pipeline)
                    if S - pos > 0:
                        pipeline.append((t + leadraw[i, t], S - pos))

    avg_stock = stock_days / days
    yr = days / 365.0
    return {
        "fill": sold / want if want > 0 else 0.0,
        "cycle": cycles_ok / cycles if cycles else 0.0,
        "stock": avg_stock, "gp": gp_earned / yr, "waste": waste / yr,
        "ep": gp_earned / yr - HOLDING_RATE * avg_stock - waste / yr,
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stores", type=int, default=len(STORES))
    ap.add_argument("--days", type=int, default=180)
    a = ap.parse_args(argv)

    book = build_book()
    shelf = {}
    for p in book:
        if p["department"] not in shelf:
            shelf[p["department"]] = ou.shelf_life_for(p["department"]) or 0.0

    from oasis.logic.simulation_bridge import SimulationOrderUtil
    util = SimulationOrderUtil(DATA_DIR)
    enriched = util.prepare_sku_data(copy.deepcopy(book))
    mix = STORES[:a.stores]
    print(f"{len(book):,} SKUs | {len(mix)} stores | {a.days} days\n")

    print(f"  {'service lvl':>12}{'z':>7}{'Phi(z)':>9}{'fill rate':>12}"
          f"{'cycle svc':>12}{'stock KES':>15}{'EP/yr':>15}")
    for sl_ in SERVICE_LEVELS:
        os.environ["OASIS_SERVICE_LEVEL"] = str(sl_)
        import importlib
        importlib.reload(ou)
        agg = defaultdict(list)
        for si, scale in enumerate(mix):
            lv = levels(enriched, scale)
            r = simulate(book, lv, shelf, scale, a.days, si)
            for k, v in r.items():
                agg[k].append(v)
        os.environ.pop("OASIS_SERVICE_LEVEL", None)
        importlib.reload(ou)
        z = ou.z_score(sl_)
        phi = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        print(f"  {sl_:>12.2f}{z:>7.2f}{phi:>9.3f}"
              f"{100*statistics.mean(agg['fill']):>11.2f}%"
              f"{100*statistics.mean(agg['cycle']):>11.2f}%"
              f"{sum(agg['stock']):>15,.0f}{sum(agg['ep']):>15,.0f}")

    print("\n  If CYCLE SVC tracks Phi(z), z is doing its job and the fill")
    print("  rate is simply what that implies -- fill is structurally higher")
    print("  because a cycle that runs out on its last day still served")
    print("  almost all of its units. Compare like with like before moving z.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
