"""Probe: do AMIT and MANDE block the cold tail, or the hot nodes?

A GATE'S SHARE OF SKUs SAYS NOTHING ABOUT ITS AIM
    AMIT blacklists 9,296 of 19,492 lines and MANDE flags 300 of ~586
    suppliers. Read as percentages of the catalogue those look alarming, and
    read as percentages of the catalogue they are meaningless: a supermarket's
    line count is dominated by a long cold tail, so a gate that removes half the
    LINES may remove almost none of the TRADE.

    The question that separates a working gate from an over-reaching one is
    what share of velocity and value it takes with it, and whether any of the
    hot nodes are caught in the net.

    A gate blocking 48% of SKUs and 2% of value is doing exactly its job.
    The same gate blocking 48% of SKUs and 40% of value is making the
    assortment decision rather than refining it.

WHAT IS OBSERVED
    velocity   ADS from six months of POS
    value      ADS * avg_cost from the GRN intelligence cache
    Both real. The blacklist and purge lists are the engines' own output.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit import stock_ledger as SL                              # noqa: E402
from devkit.methodology.traps import join_match_rate, ratio, run_all  # noqa: E402

AMIT = ROOT / "oasis" / "data" / "amit_enforcement.json"
MANDE = ROOT / "oasis" / "data" / "mande_purge_report.json"
GRN = ROOT / "oasis" / "data" / "grn_intelligence_cache.json"
DEPT = ROOT / "oasis" / "data" / "product_department_map.json"


def norm(x):
    return " ".join(str(x).upper().split())


def share(part, whole):
    return (part / whole) if whole else 0.0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default="/tmp/ff.xlsx")
    ap.add_argument("--top", type=int, default=10)
    a = ap.parse_args(argv)

    led = SL.load(local_copy=Path(a.xlsx))
    cost = {norm(k): float(v.get("avg_cost") or 0)
            for k, v in json.loads(GRN.read_text(encoding="utf-8")).items()
            if isinstance(v, dict)}
    depts = {norm(k): v for k, v in json.loads(DEPT.read_text(encoding="utf-8")).items()}
    amit = json.loads(AMIT.read_text(encoding="utf-8"))
    black = {norm(s) for s in (amit.get("blacklist") or [])}
    details = {norm(d.get("sku")): d for d in (amit.get("blacklist_details") or [])}
    mande = json.loads(MANDE.read_text(encoding="utf-8"))
    purged = {norm(c.get("supplier")): c for c in (mande.get("purge_candidates") or [])}

    vendor_of = {}
    from collections import Counter
    for item, rs in led.receipts.items():
        if rs:
            v = Counter(r.vendor for r in rs).most_common(1)[0][0]
            vendor_of[item] = norm(v.split(" - ", 1)[1] if " - " in v else v)

    universe = sorted(set(led.ads) & set(vendor_of))
    run_all([
        join_match_rate(sorted(black), list(led.ads), "AMIT blacklist -> ADS", floor=0.30),
        join_match_rate(sorted(purged), list(vendor_of.values()),
                        "MANDE purge -> vendors in receipts", floor=0.20),
    ])

    rows = []
    for item in universe:
        d = led.ads[item]
        c = cost.get(item, 0.0)
        rows.append({"sku": item, "d": d, "units": d * 365.0,
                     "value": d * 365.0 * c,
                     "dept": depts.get(item, "UNMAPPED"),
                     "amit": item in black,
                     "mande": vendor_of[item] in purged})
    tot_sku = len(rows)
    tot_units = sum(r["units"] for r in rows)
    tot_value = sum(r["value"] for r in rows)

    # Hot nodes: the lines that actually carry the trade.
    by_value = sorted(rows, key=lambda r: -r["value"])
    decile = by_value[:max(1, tot_sku // 10)]
    top100 = by_value[:100]

    print(f"  universe {tot_sku:,} SKUs · {tot_units:,.0f} units/yr · "
          f"KES {tot_value:,.0f}/yr")
    print(f"  the trade is concentrated: the top decile carries "
          f"{share(sum(r['value'] for r in decile), tot_value):.1%} of value")

    out = {}
    for gate in ("amit", "mande"):
        hit = [r for r in rows if r[gate]]
        hv = sum(r["value"] for r in hit)
        hu = sum(r["units"] for r in hit)
        dec_hit = sum(1 for r in decile if r[gate])
        t100 = sum(1 for r in top100 if r[gate])
        out[gate] = {
            "skus_blocked": len(hit), "sku_share": round(share(len(hit), tot_sku), 4),
            "unit_share": round(share(hu, tot_units), 5),
            "value_share": round(share(hv, tot_value), 5),
            "top_decile_blocked": dec_hit,
            "top_decile_share": round(share(dec_hit, len(decile)), 4),
            "top100_blocked": t100,
            "median_ads_blocked": round(st.median([r["d"] for r in hit]), 5) if hit else None,
            "median_ads_allowed": round(
                st.median([r["d"] for r in rows if not r[gate]]), 5),
        }
        print(f"\n  {gate.upper()}: blocks {len(hit):,} SKUs "
              f"({share(len(hit), tot_sku):.1%}) carrying "
              f"{share(hu, tot_units):.2%} of units and "
              f"{share(hv, tot_value):.2%} of value")
        print(f"      of the top value decile ({len(decile):,} lines) it blocks "
              f"{dec_hit:,} ({share(dec_hit, len(decile)):.1%}); "
              f"of the top 100, {t100}")
        print(f"      median ADS blocked {out[gate]['median_ads_blocked']} vs "
              f"allowed {out[gate]['median_ads_allowed']}")

    # GMROI of what AMIT took, from its own reasoning.
    gm = [float(d.get("gmroi") or 0) for d in details.values()]
    zero_gm = sum(1 for g in gm if g <= 0)
    if gm:
        print(f"\n  AMIT's own reason: {len(gm):,} blacklisted lines carry a "
              f"recorded GMROI · {zero_gm:,} ({share(zero_gm, len(gm)):.1%}) are "
              f"exactly zero · median {st.median(gm):.3f}")

    msum = mande.get("summary") or {}
    print(f"  MANDE's own reason: {msum.get('purge_candidates_high', 0)} high + "
          f"{msum.get('purge_candidates_medium', 0)} medium of "
          f"{msum.get('total_suppliers_analyzed', 0)} suppliers · capital release "
          f"KES {msum.get('total_capital_release_potential', 0):,.0f}")

    run_all([ratio(out["amit"]["value_share"], max(out["amit"]["sku_share"], 1e-9),
                   "AMIT value share vs SKU share", lo=0.0, hi=1.0)])

    # Well-aimed: takes far less value than lines, and spares the hot nodes.
    def aimed(g):
        return (g["value_share"] < g["sku_share"] / 3.0
                and g["top_decile_share"] < 0.10)

    a_ok, m_ok = aimed(out["amit"]), aimed(out["mande"])
    print(json.dumps({
        "claim": "claim.ordering.purge-gates-spare-the-hot-nodes",
        "verdict": "supports" if (a_ok and m_ok) else "contradicts",
        "metric": {"universe": tot_sku, "annual_units": round(tot_units),
                   "annual_value_kes": round(tot_value),
                   "top_decile_value_share": round(
                       share(sum(r["value"] for r in decile), tot_value), 4),
                   "amit": out["amit"], "mande": out["mande"],
                   "amit_zero_gmroi_share": round(share(zero_gm, len(gm)), 4) if gm else None,
                   "mande_capital_release_kes": msum.get("total_capital_release_potential"),
                   "amit_well_aimed": a_ok, "mande_well_aimed": m_ok},
        "held_out": False, "provenance": "observed",
        "sources": ["source.corrected-ads", "source.fulfilment-detail"],
        "baseline": "a gate should take far less value than it takes lines",
        "beat_baseline": None, "traps": ["T1", "T3"],
        "notes": (
            f"AMIT blocks {out['amit']['sku_share']:.1%} of lines but "
            f"{out['amit']['value_share']:.2%} of value, and "
            f"{out['amit']['top_decile_blocked']:,} of the top value decile. "
            f"MANDE blocks {out['mande']['sku_share']:.1%} of lines and "
            f"{out['mande']['value_share']:.2%} of value. "
            + ("Both are aimed at the cold tail, which is what they are for: the "
               "share-of-catalogue figure was never the right measure."
               if (a_ok and m_ok) else
               "At least one gate is taking trade with it, not just tail."))}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
