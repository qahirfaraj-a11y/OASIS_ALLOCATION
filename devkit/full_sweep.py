"""End to end, every SKU, all engines, with the money metrics side by side.

THREE RETURN MEASURES, BECAUSE THEY DISAGREE
    GMROI  = annual gross profit / average inventory at cost
             a RATE. Scale-free, ignores how much profit there is.
    ROI    = (gross profit - carrying cost - waste) / average inventory
             what the shelf actually earns after the cost of holding it and
             the stock that dies on it. The number a finance director means.
    turns  = annual COGS / average inventory
             the operational cousin: how often the shelf empties.

    A configuration can raise GMROI by starving the shelf -- fewer shillings of
    stock under a smaller numerator -- and that is what every blocking engine
    in this system does. ROI catches it because lost sales cut the numerator
    faster than the blocked stock cuts the denominator.
"""
from __future__ import annotations
import json, sys, csv
from collections import defaultdict
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.engine_ablation import build, simulate, CONFIGS   # noqa: E402
from devkit.amit_return import norm                           # noqa: E402

H = 0.1438      # capital only: the chain does not charge handling into ordering
OUT = ROOT / "oasis" / "data" / "sweep_per_sku.csv"

keys, f = build()
dept = {}
try:
    snap = json.loads((ROOT / "oasis" / "data" / "stock_snapshot_dept.json").read_text(encoding="utf-8"))
    dept = {norm(k): " ".join(str(v.get("dept") or "").upper().split()) for k, v in snap.items()}
except (OSError, ValueError):
    pass

print(f"  {len(keys):,} SKUs · 12 seeded stores · demand = Poisson counts x lognormal basket")
print(f"  carrying rate h = {H:.2%} (capital only)\n")
hdr = (f"  {'configuration':<28}{'service':>9}{'avg stock':>13}{'GMROI':>8}"
       f"{'ROI':>8}{'turns':>7}{'waste/yr':>12}{'GP/yr':>14}")
print(hdr); print("  " + "-" * (len(hdr) - 2))
rows = {}
sku_out = None
for name, cfg in CONFIGS.items():
    want = name.startswith("ALL ON (MANDE reporting)")
    m = simulate(f, cfg, 12, real_open=True, warm=0, cv_mode="poisson",
                 demand_law="poisson", per_sku=want)
    I = m["avg_stock_kes"]; gp = m["gp_realised_year"]; w = m["waste_kes_year"]
    cogs = gp / 0.25 * 0.75 if gp else 0.0
    roi = (gp - I * H - w) / max(I, 1)
    rows[name] = dict(service=m["service"], stock=I, gmroi=m["gmroi"], roi=roi,
                      turns=cogs / max(I, 1), waste=w, gp=gp)
    print(f"  {name:<28}{m['service']:>8.2%}{I:>13,.0f}{m['gmroi']:>8.2f}"
          f"{roi:>8.2f}{cogs/max(I,1):>7.1f}{w:>12,.0f}{gp:>14,.0f}", flush=True)
    if want and m.get("per_sku"):
        sku_out = m["per_sku"]

if sku_out is not None:
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w_ = csv.writer(fh)
        w_.writerow(["sku", "dept", "d", "cost", "gp_unit", "R", "sigma_L", "cv",
                     "S", "demand_day", "sold_day", "avg_stock_kes", "waste_kes",
                     "fill", "gp_year", "gmroi", "roi", "blocked"])
        for i, k in enumerate(keys):
            dem = sku_out["dem"][i]; sold = sku_out["sold"][i]
            st_ = sku_out["stock"][i]; wa = sku_out["waste"][i]
            gpy = sold * f["gp"][i] / 315.0 * 365.0
            fill = sold / dem if dem > 0 else 1.0
            w_.writerow([k, dept.get(k, ""), round(f["d"][i], 4), round(f["cost"][i], 4),
                         round(f["gp"][i], 4), round(sku_out["R"][i], 2),
                         round(sku_out["sigma_L"][i], 3), round(sku_out["cv"][i], 3),
                         round(sku_out["S"][i], 2), round(dem / 315.0, 4),
                         round(sold / 315.0, 4), round(st_, 2), round(wa, 2),
                         round(fill, 4), round(gpy, 2),
                         round(gpy / max(st_, 1e-9), 3),
                         round((gpy - st_ * H - wa) / max(st_, 1e-9), 3),
                         int(sku_out["blocked"][i])])
    print(f"\n  wrote {OUT.relative_to(ROOT)}")

    by = defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0.0, 0.0])
    for i, k in enumerate(keys):
        b = by[dept.get(k, "?")]
        dem = sku_out["dem"][i]; sold = sku_out["sold"][i]
        b[0] += 1; b[1] += sku_out["stock"][i]
        b[2] += sold * f["gp"][i] / 315.0 * 365.0
        b[3] += sku_out["waste"][i]; b[4] += dem; b[5] += sold
    print(f"\n  BY DEPARTMENT, all engines on")
    print(f"  {'department':<26}{'skus':>6}{'avg stock':>13}{'GMROI':>8}{'ROI':>8}"
          f"{'service':>9}{'waste':>12}")
    for d0, b in sorted(by.items(), key=lambda kv: -kv[1][1])[:18]:
        roi = (b[2] - b[1] * H - b[3]) / max(b[1], 1)
        print(f"  {d0[:25]:<26}{b[0]:>6,}{b[1]:>13,.0f}{b[2]/max(b[1],1):>8.2f}"
              f"{roi:>8.2f}{b[5]/max(b[4],1):>8.1%}{b[3]:>12,.0f}")
    print(f"\n  WORST ROI departments -- where the shelf does not pay for itself")
    for d0, b in sorted(by.items(), key=lambda kv: (b_[2]-b_[1]*H-b_[3])/max(b_[1],1)
                        if False else ((kv[1][2]-kv[1][1]*H-kv[1][3])/max(kv[1][1],1)))[:8]:
        roi = (b[2] - b[1] * H - b[3]) / max(b[1], 1) if False else (
            (by[d0][2] - by[d0][1] * H - by[d0][3]) / max(by[d0][1], 1))
        print(f"    {d0[:28]:<30}stock {by[d0][1]:>11,.0f}  ROI {roi:>7.2f}  "
              f"waste {by[d0][3]:>11,.0f}")
json.dump({k: {kk: (float(vv) if isinstance(vv, (int, float, np.floating)) else vv)
               for kk, vv in v.items()} for k, v in rows.items()},
          open(ROOT / "devkit" / "full_sweep_result.json", "w"), indent=1)
