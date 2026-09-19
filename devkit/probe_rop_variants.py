"""Probe: does the statistical order-up-to level order better than the heuristic?

PROBE: probe.rop-variants, testing claim.ordering.statistical-target-beats-the-heuristic.

THREE WAYS TO SIZE THE SAME ORDER, ALL THROUGH THE SHIPPED BRIDGE
    H  classic heuristic   the old shipped default: flat fallback ROP and the
                           DDoS coverage target (OASIS_ORDER_MODEL=classic)
    N  classic newsvendor  the same path with the statistically-correct ROP
                           over P = R + L (OASIS_ROP_MODE=newsvendor)
    S  order-up-to         the model the product ships TODAY
                           (global_settings.order_model = order_up_to):
                           S = d*P + z*sqrt(P*sigma_d^2 + d^2*sigma_L^2), with
                           sigma_L measured per vendor, the selling-day horizon,
                           the fresh cycle and the presence rule

WHY THIS VERSION EXISTS
    The first version (2026-09-04) labelled the bridge "the shipped heuristic"
    and ran the order-up-to arm as a bare order_up_to.recommend() call. Once
    order_up_to became the configured default, BOTH bridge arms silently ran
    order-up-to too, because the bridge reads the config when
    OASIS_ORDER_MODEL is unset. Re-run unchanged it would have compared the
    model with itself under the old labels. Now every arm names its model
    explicitly and all three pass through the same bridge, so governance
    (AMIT, MANDE) is identical and the comparison is like for like.

SCORED THE SAME WAY R WAS
    An order is adequate when the cover it buys reaches the protection interval
    it has to span, and wasteful in proportion to how far past it goes.

        service  share of lines whose ordered cover reaches P
        capital  mean cover / P among those covered

    P is order_up_to.recommend's protection interval for every arm, so all
    three are scored against one yardstick. Better means at least the
    heuristic's service for less capital; anything else is a trade-off.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import statistics as st
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit import stock_ledger as SL                       # noqa: E402
from devkit.methodology.traps import like_for_like, ratio, regime, run_all   # noqa: E402
from oasis.logic import order_up_to as OU                   # noqa: E402


def score(rows):
    """rows: (cover_ordered_days, P). service and capital."""
    if not rows:
        return 0.0, float("nan"), 0
    covered = [(c, p) for c, p in rows if c >= p - 1e-9]
    service = len(covered) / len(rows)
    capital = st.mean([c / p for c, p in covered if p > 0]) if covered else float("nan")
    return service, capital, len(rows)


def bridge(products, P_of, model, rop_mode):
    """The shipped bridge with the model named explicitly: (sku, cover, P) rows."""
    saved = {k: os.environ.get(k) for k in ("OASIS_ORDER_MODEL", "OASIS_ROP_MODE", "OASIS_LATA_SOURCE")}
    os.environ["OASIS_ORDER_MODEL"] = model
    os.environ["OASIS_ROP_MODE"] = rop_mode
    os.environ["OASIS_LATA_SOURCE"] = "derived"
    try:
        for m in [k for k in list(sys.modules) if k.startswith("oasis.logic.simulation_bridge")]:
            del sys.modules[m]
        OU.reset_fresh_cycle()
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        with contextlib.redirect_stdout(io.StringIO()):
            util = SimulationOrderUtil(str(ROOT / "oasis" / "data"))
        out, blocked = [], set()
        for i in range(0, len(products), 2000):
            chunk = [dict(x) for x in products[i:i + 2000]]
            with contextlib.redirect_stdout(io.StringIO()):
                recs = util.calculate_order_quantity(chunk, current_day=1, use_real_date=True)
            for src, r in zip(chunk, recs):
                d = src["avg_daily_sales"]
                P = P_of.get(src["sku_id"])
                if "Blocked:" in str(r.get("reasoning") or ""):
                    blocked.add(src["sku_id"])
                    continue
                if d > 0 and P:
                    out.append((src["sku_id"], float(r.get("recommended_quantity") or 0) / d, P))
        return out, blocked
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=None, help="a local copy of the fulfilment detail")
    ap.add_argument("--skus", default="all")
    a = ap.parse_args(argv)

    led = SL.load(local_copy=Path(a.xlsx) if a.xlsx else None)
    patterns = OU.default_patterns()
    schedule = OU.load_review_schedule(str(ROOT))

    vendor_of = {}
    for item, rs in led.receipts.items():
        if rs:
            vendor_of[item] = Counter(r.vendor for r in rs).most_common(1)[0][0]
    universe = sorted(set(led.ads) & set(vendor_of))
    if a.skus != "all":
        universe = universe[:int(a.skus)]

    products = []
    for item in universe:
        v = vendor_of[item]
        sup = " ".join((v.split(" - ", 1)[1] if " - " in v else v).upper().split())
        lead = patterns.get(OU.supplier_key(sup), {}).get("lead_time_days") or 3
        products.append({
            "sku_id": item, "product_name": item,
            "avg_daily_sales": led.ads[item], "demand_cv": 0.4,
            "current_stock": 0.0, "on_order_qty": 0.0,
            "supplier_name": sup,
            "lead_time_days": lead, "estimated_delivery_days": lead,
            "months_active": 6, "is_fresh": False, "pack_size": 1,
        })
    print(f"  {len(products):,} lines")

    # one yardstick: the protection interval P each line has to span
    P_of = {}
    for p in products:
        t = OU.recommend(dict(p), schedule, patterns)
        P_of[p["sku_id"]] = t.get("P")

    H_rows, H_blocked = bridge(products, P_of, "classic", "heuristic")
    N_rows, _ = bridge(products, P_of, "classic", "newsvendor")
    S_rows, S_blocked = bridge(products, P_of, "order_up_to", "heuristic")

    # LIKE FOR LIKE: governance runs in every arm; compare sizing on the lines
    # that survive it in all three, so the blacklist is not what gets measured
    survivors = ({s for s, _, _ in H_rows} & {s for s, _, _ in N_rows} & {s for s, _, _ in S_rows})
    print(f"  blocked by governance: classic {len(H_blocked):,}, order-up-to {len(S_blocked):,}; "
          f"comparing sizing on the {len(survivors):,} lines that survive every arm")
    arms = {name: [(c, p) for s, c, p in rows if s in survivors]
            for name, rows in (("heuristic", H_rows), ("newsvendor", N_rows), ("order_up_to", S_rows))}

    res = {}
    for name, rows in arms.items():
        s, c, n = score(rows)
        res[name] = {"service": round(s, 4),
                     "capital": (None if c != c else round(c, 4)), "lines": n,
                     "median_cover_days": round(st.median([r[0] for r in rows]), 2) if rows else None}
        print(f"    {name:<12} service {s:>7.1%} · cover carried "
              f"{'n/a' if c != c else f'{c:.2f}x'} · median cover "
              f"{res[name]['median_cover_days']}d over {n:,} lines")

    base = res["heuristic"]
    traps = run_all([
        like_for_like(("heuristic lines", float(res["heuristic"]["lines"])),
                      ("order-up-to lines", float(res["order_up_to"]["lines"])),
                      "sizing compared on one survivor set"),
        ratio(res["order_up_to"]["capital"] or 0, max(base["capital"] or 1e-9, 1e-9),
              "cover carried: order-up-to vs heuristic", lo=0.2, hi=5.0),
        regime("cover vs protection interval", "replenishment", "replenishment"),
    ], strict=True)

    def better(x):
        return (x["service"] >= base["service"] - 0.02
                and (x["capital"] or 9e9) < (base["capital"] or 0))

    winners = [k for k in ("newsvendor", "order_up_to") if better(res[k])]
    ou_better = "order_up_to" in winners
    print(json.dumps({
        "claim": "claim.ordering.statistical-target-beats-the-heuristic",
        "verdict": "supports" if ou_better else "contradicts",
        "metric": {**res, "dominates_heuristic": winners,
                   "governance_blocked": {"classic": len(H_blocked), "order_up_to": len(S_blocked)},
                   "compared_on": len(survivors)},
        "held_out": False, "provenance": "observed",
        "sources": ["source.fulfilment-detail", "source.corrected-ads",
                    "source.supplier-weekly-schedule"],
        "baseline": "classic heuristic: flat fallback ROP + DDoS coverage target",
        "beat_baseline": ou_better,
        "traps": sorted({r.trap for r in traps}),
        "notes": (
            "All arms through the shipped bridge with the model named explicitly; order_up_to is "
            "the configured default. Scored on whether ordered cover reaches the protection "
            "interval and how far past it goes. At an empty shelf every sizing reaches P, so "
            "service is a weak discriminator and capital carries the verdict. "
            + ("order-up-to gives at least the heuristic's service for less cover carried."
               if ou_better else
               "order-up-to does not dominate: it matches the heuristic's service only by "
               "carrying more cover, or falls short of it — a trade-off, not an improvement."))}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
