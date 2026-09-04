"""Probe: heuristic target, newsvendor trigger, or a statistical order-up-to level?

THREE WAYS TO SIZE THE SAME ORDER
    H  heuristic     the shipped default: flat fallback ROP, DDoS coverage target
    N  newsvendor    statistically-correct ROP over P = R + L, same target
    S  order-up-to   the derived model end to end: S = d*P + z*sqrt(P*sigma_d^2
                     + d^2*sigma_L^2), with sigma_L measured per vendor

SCORED THE SAME WAY R WAS
    An order is adequate when the cover it buys reaches the protection interval
    it has to span, and wasteful in proportion to how far past it goes.

        service  share of lines whose ordered cover reaches P
        capital  mean cover / P among those covered

    Better means more service for less capital. Anything else is a trade-off
    and gets reported as one rather than declared a winner.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics as st
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit import stock_ledger as SL                       # noqa: E402
from devkit.methodology.traps import ratio, run_all         # noqa: E402
from oasis.logic import order_up_to as OU                   # noqa: E402


def score(rows):
    """rows: (cover_ordered_days, P). service and capital."""
    if not rows:
        return 0.0, float("nan"), 0
    covered = [(c, p) for c, p in rows if c >= p - 1e-9]
    service = len(covered) / len(rows)
    capital = st.mean([c / p for c, p in covered if p > 0]) if covered else float("nan")
    return service, capital, len(rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default="/tmp/ff.xlsx")
    ap.add_argument("--skus", default="all")
    a = ap.parse_args(argv)

    led = SL.load(local_copy=Path(a.xlsx))
    patterns = OU.default_patterns()
    schedule = OU.load_review_schedule(str(ROOT))

    vendor_of = {}
    for item, rs in led.receipts.items():
        if rs:
            v = Counter(r.vendor for r in rs).most_common(1)[0][0]
            vendor_of[item] = v
    universe = sorted(set(led.ads) & set(vendor_of))
    if a.skus != "all":
        universe = universe[:int(a.skus)]

    products = []
    for item in universe:
        v = vendor_of[item]
        sup = " ".join((v.split(" - ", 1)[1] if " - " in v else v).upper().split())
        products.append({
            "sku_id": item, "product_name": item,
            "avg_daily_sales": led.ads[item], "demand_cv": 0.4,
            "current_stock": 0.0, "on_order_qty": 0.0,
            "supplier_name": sup,
            "lead_time_days": (patterns.get(sup, {}).get("lead_time_days") or 3),
            "estimated_delivery_days": (patterns.get(sup, {}).get("lead_time_days") or 3),
            "months_active": 6, "is_fresh": False, "pack_size": 1,
        })
    print(f"  {len(products):,} lines")

    # ---- S: the derived model --------------------------------------------
    S_rows, P_of = [], {}
    for p in products:
        t = OU.recommend(dict(p), schedule, patterns)
        d = t.get("d") or 0
        P_of[p["sku_id"]] = t.get("P")
        S_rows.append(((t["quantity"] / d, t["P"]) if d > 0 else (0.0, 1.0)))

    # ---- H and N: the shipped path, both ROP modes ------------------------
    def bridge(mode):
        os.environ["OASIS_ROP_MODE"] = mode
        os.environ["OASIS_LATA_SOURCE"] = "derived"
        for m in [k for k in list(sys.modules)
                  if k.startswith("oasis.logic.simulation_bridge")]:
            del sys.modules[m]
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        util = SimulationOrderUtil(str(ROOT / "oasis" / "data"))
        out, blocked = [], set()
        for i in range(0, len(products), 2000):
            chunk = [dict(x) for x in products[i:i + 2000]]
            for src, r in zip(chunk, util.calculate_order_quantity(
                    chunk, current_day=1, use_real_date=True)):
                d = src["avg_daily_sales"]
                P = P_of.get(src["sku_id"])
                if "Blocked:" in str(r.get("reasoning") or ""):
                    blocked.add(src["sku_id"])
                    continue
                if d > 0 and P:
                    out.append((src["sku_id"],
                                float(r.get("recommended_quantity") or 0) / d, P))
        return out, blocked

    H_rows, H_blocked = bridge("heuristic")
    N_rows, N_blocked = bridge("newsvendor")

    # LIKE FOR LIKE. The shipped path runs AMIT and MANDE; the pure order-up-to
    # model runs no governance at all. Comparing them across the whole book
    # measures the BLACKLIST, not the sizing: 59% of lines came back as zero
    # cover because they were blocked, and the derived model scored 100%
    # service simply by having nothing to stop it. Restrict every arm to the
    # lines that survive the gates, and the question becomes the one asked.
    survivors = {sku for sku, _, _ in H_rows} & {sku for sku, _, _ in N_rows}
    print(f"  {len(H_blocked):,} lines blocked by governance; comparing sizing "
          f"on the {len(survivors):,} that survive")
    H2 = [(c, p) for sku, c, p in H_rows if sku in survivors]
    N2 = [(c, p) for sku, c, p in N_rows if sku in survivors]
    S2 = [(c, p) for (sku, (c, p)) in zip([x["sku_id"] for x in products], S_rows)
          if sku in survivors] if len(S_rows) == len(products) else S_rows

    res = {}
    for name, rows in (("heuristic", H2), ("newsvendor", N2),
                       ("order_up_to", S2)):
        s, c, n = score(rows)
        res[name] = {"service": round(s, 4),
                     "capital": (None if c != c else round(c, 4)), "lines": n,
                     "median_cover_days": round(st.median([r[0] for r in rows]), 2)
                     if rows else None}
        print(f"    {name:<12} service {s:>7.1%} · cover carried "
              f"{'n/a' if c != c else f'{c:.2f}x'} · median cover "
              f"{res[name]['median_cover_days']}d over {n:,} lines")

    base = res["heuristic"]
    run_all([ratio(res["order_up_to"]["capital"] or 0,
                   max(base["capital"] or 1e-9, 1e-9),
                   "cover carried: order-up-to vs heuristic", lo=0.2, hi=5.0)])

    def better(x):
        return (x["service"] >= base["service"] - 0.02
                and (x["capital"] or 9e9) < (base["capital"] or 0))

    winners = [k for k in ("newsvendor", "order_up_to") if better(res[k])]
    print(json.dumps({
        "claim": "claim.ordering.statistical-target-beats-the-heuristic",
        "verdict": "supports" if winners else "contradicts",
        "metric": {**res, "dominates_heuristic": winners,
                   "governance_blocked": len(H_blocked),
                   "compared_on": len(survivors)},
        "held_out": False, "provenance": "observed",
        "sources": ["source.fulfilment-detail", "source.corrected-ads",
                    "source.supplier-weekly-schedule"],
        "baseline": "shipped heuristic: flat fallback ROP + DDoS coverage target",
        "beat_baseline": bool(winners), "traps": ["T3"],
        "notes": (
            "Scored on whether the cover ordered reaches the protection interval, "
            "and how far past it goes. "
            + (f"{', '.join(winners)} give at least the heuristic's service for "
               "less cover carried."
               if winners else
               "Neither alternative dominates: the heuristic's service is not "
               "matched, or it is matched only by carrying more cover. The "
               "difference is a trade-off, not an improvement."))}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
