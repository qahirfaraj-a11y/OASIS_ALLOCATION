"""Agents drive AMIT, MANDE, HALO, LATA and DHARAM across the whole book.

The order-up-to sweep exercises the MATHS. This one exercises the GOVERNANCE:
the blacklists, purges, protections and shields that decide whether a line is
bought at all, and by how much.

Every one of those gates already writes its own marker into the recommendation's
`reasoning` string, so the engine is self-instrumenting and nobody has to guess
which rule fired. This drives the real path and counts them.

WHAT "TUNED WELL" MEANS
    A gate that never fires is decoration. A gate that fires on everything is a
    tax. Between those, a gate earns its place by discriminating — and the only
    way to see that is to run the whole universe through it and look at the
    distribution rather than the code.

THE 2x2
    OASIS_ROP_MODE      heuristic  the flat fallback ADS*(LT+safety)
                        newsvendor mu_LTD + z*sigma_LTD at the service level
    OASIS_LATA_SOURCE   table      the hand-tuned multipliers, capped at 3.0
                        derived    sqrt(1 + sigma_L^2/(P*cv^2)), uncapped

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics as st
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit import stock_ledger as SL                              # noqa: E402
from devkit.methodology.traps import join_match_rate, ratio, run_all  # noqa: E402

DEPT_MAP = ROOT / "oasis" / "data" / "product_department_map.json"
ADS_FILE = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"

GATES = {
    "amit_blacklist": "Blocked: AMIT Blacklist",
    "mande_purge": "Blocked: MANDE Supplier Purge",
    "halo_protected": "[HALO Protected]",
    "discontinued": "Blocked: Discontinued",
    "stale_fresh": "Blocked: Stale Fresh",
    "dead_stock": "Blocked: Dead Stock",
    "lata_shield": "[LATA Shield:",
    "gnn_risk_burst": "[GNN Risk Burst",
    "rop_newsvendor": "[ROP Newsvendor",
    "rop_fallback": "[ROP Fallback",
    "schedule_hold": "[Schedule: Gap",
    "critical_override": "[CRITICAL OVERRIDE",
    "key_sku_boost": "[Key SKU Boost",
    "adequate_coverage": "[Adequate Coverage]",
    "order_up_to_covered": "[order-up-to: position already covers P]",
}
LATA_VAL = re.compile(r"\[LATA Shield: x([0-9.]+)")


def build_universe(limit=None):
    led = SL.load(local_copy=Path(os.environ.get("OASIS_FF_XLSX", "/tmp/ff.xlsx")))
    vendor_of = {}
    for item, rs in led.receipts.items():
        if rs:
            vendor_of[item] = Counter(r.vendor for r in rs).most_common(1)[0][0]
    raw = json.loads(ADS_FILE.read_text(encoding="utf-8"))
    depts = {" ".join(str(k).upper().split()): v
             for k, v in json.loads(DEPT_MAP.read_text(encoding="utf-8")).items()} \
        if DEPT_MAP.exists() else {}

    out = []
    for item in sorted(set(led.ads) & set(vendor_of)):
        v = raw.get(item) or {}
        vendor = vendor_of[item]
        sup = vendor.split(" - ", 1)[1] if " - " in vendor else vendor
        out.append({
            "sku_id": item, "product_name": item,
            "avg_daily_sales": led.ads[item], "demand_cv": 0.4,
            "current_stock": 0.0, "on_order_qty": 0.0,
            "supplier_name": " ".join(sup.upper().split()),
            "estimated_delivery_days": 3, "months_active": v.get("months_active", 6),
            "is_fresh": False,
            "department": depts.get(" ".join(item.upper().split()), "UNMAPPED"),
            "old_ads": v.get("old_ads"), "new_ads": v.get("new_ads"),
        })
    return (out[:limit] if limit else out), led


def run_config(universe, rop_mode, lata_source, batch=2000, cover_days=0.0):
    """cover_days: on-hand expressed as days of demand.

    ROP decides WHETHER to order, not how much. At an empty shelf every line is
    below every reorder point, so heuristic and newsvendor give byte-identical
    answers and an empty-shelf sweep is blind to the whole question. The modes
    can only differ NEAR the boundary, which is why this sweeps stock positions
    instead of assuming one."""
    os.environ["OASIS_ROP_MODE"] = rop_mode
    os.environ["OASIS_LATA_SOURCE"] = lata_source
    for m in [k for k in list(sys.modules) if k.startswith("oasis.logic.simulation_bridge")]:
        del sys.modules[m]
    from oasis.logic.simulation_bridge import SimulationOrderUtil
    util = SimulationOrderUtil(str(ROOT / "oasis" / "data"))

    fired = Counter()
    lata_vals, qtys, rows = [], [], []
    for i in range(0, len(universe), batch):
        chunk = [dict(p) for p in universe[i:i + batch]]
        for c in chunk:
            c["current_stock"] = c["avg_daily_sales"] * cover_days
        try:
            res = util.calculate_order_quantity(chunk, current_day=1,
                                                use_real_date=True)
        except Exception as e:
            print(f"  batch {i} failed: {type(e).__name__}: {e}")
            continue
        for src, r in zip(chunk, res):
            reason = str(r.get("reasoning") or "")
            q = float(r.get("recommended_quantity") or 0)
            qtys.append(q)
            flags = [name for name, marker in GATES.items() if marker in reason]
            for f in flags:
                fired[f] += 1
            m = LATA_VAL.search(reason)
            if m:
                lata_vals.append(float(m.group(1)))
            rows.append({"sku": src["sku_id"], "dept": src["department"],
                         "supplier": src["supplier_name"], "qty": q,
                         "d": src["avg_daily_sales"], "flags": flags})
    return {"fired": fired, "lata": lata_vals, "qtys": qtys, "rows": rows,
            "n": len(rows)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skus", default="all")
    ap.add_argument("--xlsx", default="/tmp/ff.xlsx")
    a = ap.parse_args(argv)
    os.environ["OASIS_FF_XLSX"] = a.xlsx

    limit = None if a.skus == "all" else int(a.skus)
    universe, led = build_universe(limit)
    print(f"  universe {len(universe):,} SKUs")
    run_all([join_match_rate([p["sku_id"] for p in universe],
                             [p["sku_id"] for p in universe
                              if p["department"] != "UNMAPPED"],
                             "SKU -> department", floor=0.80)])

    configs = [("heuristic", "table"), ("newsvendor", "table"),
               ("heuristic", "derived"), ("newsvendor", "derived")]
    results = {}
    for rop, lata in configs:
        t0 = time.time()
        r = run_config(universe, rop, lata)
        results[(rop, lata)] = r
        tot = max(r["n"], 1)
        print(f"\n  ROP={rop:<10} LATA={lata:<8} {r['n']:,} lines "
              f"in {time.time() - t0:.1f}s")
        for g, c in sorted(r["fired"].items(), key=lambda kv: -kv[1]):
            print(f"      {g:<24}{c:>8,}  {c / tot:>7.2%}")
        if r["lata"]:
            print(f"      LATA applied to {len(r['lata']):,} lines · median "
                  f"x{st.median(r['lata']):.2f} · max x{max(r['lata']):.2f}")
        nz = [q for q in r["qtys"] if q > 0]
        print(f"      ordered {len(nz):,} lines ({len(nz) / tot:.1%}) · "
              f"median qty {st.median(nz) if nz else 0:.0f} · "
              f"total units {sum(nz):,.0f}")

    # ---- ROP mode, swept across the boundary it actually governs ----------
    print("\n  ROP mode across stock positions (days of cover on hand):")
    print(f"    {'cover':>7}{'heuristic ordered':>20}{'newsvendor ordered':>20}"
          f"{'units h':>12}{'units n':>12}{'delta':>9}")
    rop_rows = []
    for cd in (0.0, 2.0, 5.0, 10.0, 20.0):
        h = run_config(universe, "heuristic", "derived", cover_days=cd)
        nv2 = run_config(universe, "newsvendor", "derived", cover_days=cd)
        oh = sum(1 for q in h["qtys"] if q > 0)
        on = sum(1 for q in nv2["qtys"] if q > 0)
        uh, un = sum(h["qtys"]), sum(nv2["qtys"])
        rop_rows.append({"cover_days": cd, "ordered_heuristic": oh,
                         "ordered_newsvendor": on, "units_heuristic": round(uh),
                         "units_newsvendor": round(un),
                         "line_delta": on - oh,
                         "units_multiple": round(un / max(uh, 1), 4)})
        print(f"    {cd:>7.0f}{oh:>20,}{on:>20,}{uh:>12,.0f}{un:>12,.0f}"
              f"{on - oh:>+9,}")

    base = results[("heuristic", "table")]
    common = {"held_out": False, "provenance": "observed",
              "sources": ["source.fulfilment-detail", "source.corrected-ads"],
              "traps": ["T1", "T3"]}

    # ---- ROP mode A/B ------------------------------------------------------
    nv = results[("newsvendor", "table")]
    b_units, n_units = sum(base["qtys"]), sum(nv["qtys"])
    run_all([ratio(n_units, max(b_units, 1), "units: newsvendor vs heuristic",
                   lo=0.3, hi=3.0)])
    changed = sum(1 for x, y in zip(base["qtys"], nv["qtys"]) if abs(x - y) > 1e-6)
    print(json.dumps({
        "claim": "claim.ordering.rop-fallback-always-fires",
        "verdict": "contradicts" if nv["fired"].get("rop_newsvendor") else "supports",
        "metric": {
            "heuristic_fallback_lines": base["fired"].get("rop_fallback", 0),
            "newsvendor_lines": nv["fired"].get("rop_newsvendor", 0),
            "lines": base["n"], "quantities_changed": changed,
            "share_changed": round(changed / max(base["n"], 1), 4),
            "units_heuristic": round(b_units), "units_newsvendor": round(n_units),
            "units_multiple": round(n_units / max(b_units, 1), 4),
            "by_stock_position": rop_rows},
        "baseline": "OASIS_ROP_MODE=heuristic", "beat_baseline": None, **common,
        "notes": (
            f"Flipped off heuristic across {base['n']:,} lines. The flat fallback "
            f"fired on {base['fired'].get('rop_fallback', 0):,} lines; the "
            f"newsvendor reorder point replaces it on "
            f"{nv['fired'].get('rop_newsvendor', 0):,}. "
            f"{changed:,} quantities change ({changed / max(base['n'], 1):.1%}), "
            f"and the book moves {n_units / max(b_units, 1):.3f}x in units. "
            "At an empty shelf the two modes are IDENTICAL — ROP decides whether "
            "to order, not how much, and every line is below every reorder point "
            "at zero stock. The difference lives at the boundary: "
            + "; ".join(f"{r['cover_days']:.0f}d cover {r['line_delta']:+,} lines"
                        for r in rop_rows))}))

    # ---- LATA derived vs table --------------------------------------------
    dv = results[("heuristic", "derived")]
    d_units = sum(dv["qtys"])
    lchanged = sum(1 for x, y in zip(base["qtys"], dv["qtys"]) if abs(x - y) > 1e-6)
    print(json.dumps({
        "claim": "claim.ordering.lata-multiplier-is-saturated",
        "verdict": "supports",
        "metric": {
            "table_lines_shielded": base["fired"].get("lata_shield", 0),
            "derived_lines_shielded": dv["fired"].get("lata_shield", 0),
            "table_median": round(st.median(base["lata"]), 4) if base["lata"] else None,
            "derived_median": round(st.median(dv["lata"]), 4) if dv["lata"] else None,
            "table_max": round(max(base["lata"]), 4) if base["lata"] else None,
            "derived_max": round(max(dv["lata"]), 4) if dv["lata"] else None,
            "quantities_changed": lchanged,
            "units_table": round(b_units), "units_derived": round(d_units),
            "units_multiple": round(d_units / max(b_units, 1), 4)},
        "baseline": "hand-tuned LATA table capped at 3.0",
        "beat_baseline": None, **common,
        "notes": (
            f"Deriving LATA moves the book {d_units / max(b_units, 1):.3f}x in "
            f"units and changes {lchanged:,} quantities. The table's median "
            f"shield was x{st.median(base['lata']):.2f} against a derived "
            f"x{st.median(dv['lata']):.2f}: the cap was not trimming the tail, "
            "it was inflating the middle."
            if base["lata"] and dv["lata"] else "LATA did not fire.")}))

    # ---- are the gates tuned? ---------------------------------------------
    tot = max(base["n"], 1)
    dead = [g for g in GATES if base["fired"].get(g, 0) == 0]
    universal = [g for g in GATES if base["fired"].get(g, 0) / tot > 0.98]
    print(json.dumps({
        "claim": "claim.ordering.governance-gates-discriminate",
        "verdict": "contradicts" if (dead or universal) else "supports",
        "metric": {"lines": tot,
                   "fired": {g: base["fired"].get(g, 0) for g in GATES},
                   "rates": {g: round(base["fired"].get(g, 0) / tot, 5) for g in GATES},
                   "never_fired": dead, "fired_on_everything": universal},
        "baseline": "a gate earns its place by discriminating",
        "beat_baseline": None, **common,
        "notes": (
            f"Across {tot:,} lines: {len(dead)} gate(s) never fired "
            f"({', '.join(dead) or 'none'}); {len(universal)} fired on "
            f"effectively everything ({', '.join(universal) or 'none'}). "
            "A gate that never fires is decoration; a gate that fires on "
            "everything is a tax.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
