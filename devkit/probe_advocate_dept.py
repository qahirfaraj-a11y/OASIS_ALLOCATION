from __future__ import annotations
import json, sys, csv
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.engine_ablation import build, simulate
from devkit.amit_return import norm

H = 0.1438
keys, f = build()
snap = {}
try:
    snap = json.loads((ROOT / "oasis" / "data" / "stock_snapshot_dept.json").read_text(encoding="utf-8"))
except Exception:
    pass
dept = {norm(k): " ".join(str(v.get("dept") or "").upper().split()) for k, v in snap.items()}

cfg = dict(amit=0, lata=1, dharam=0, mande=0, shelf=1)
m = simulate(f, cfg, 12, real_open=True, warm=0, cv_mode="poisson",
             demand_law="poisson", per_sku=True)
sku_out = m["per_sku"]
by = defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0.0, 0.0])
for i, k in enumerate(keys):
    d0 = dept.get(k, "?")
    b = by[d0]
    dem = sku_out["dem"][i]; sold = sku_out["sold"][i]
    b[0] += 1; b[1] += sku_out["stock"][i]
    b[2] += sold * f["gp"][i] / 252.0 * 365.0
    b[3] += sku_out["waste"][i]; b[4] += dem; b[5] += sold

rows = []
for d0, b in by.items():
    roi = (b[2] - b[1] * H - b[3]) / max(b[1], 1)
    svc = b[5] / max(b[4], 1e-9)
    rows.append((d0, b[0], b[1], b[2], b[3], svc, roi))
rows.sort(key=lambda r: r[5])
out_path = ROOT / "devkit" / "probe_advocate_persku_dept.csv"
with out_path.open("w", newline="") as fh:
    w_ = csv.writer(fh)
    w_.writerow(["dept", "skus", "avg_stock_kes", "gp_year", "waste_year", "service", "roi"])
    for r in rows:
        w_.writerow(r)
print(f"wrote {out_path}  ({len(rows)} departments, {sum(r[1] for r in rows)} skus)")
print("  worst 15 by service, RECOMMENDED (LATA+shelf, no block):")
for r in rows[:15]:
    print(f"    {r[0][:28]:<30}skus={r[1]:>5}  stock={r[2]:>12,.0f}  service={r[5]:>7.2%}  roi={r[6]:>7.2f}  gp={r[3]:>13,.0f}")
print("  overall:")
tot = [sum(r[1] for r in rows), sum(r[2] for r in rows), sum(r[3] for r in rows), sum(r[4] for r in rows)]
print(f"    skus={tot[0]} stock={tot[1]:,.0f} gp={tot[2]:,.0f} waste={tot[3]:,.0f}")
n_lt70 = sum(1 for r in rows if r[5] < 0.70)
skus_lt70 = sum(r[1] for r in rows if r[5] < 0.70)
gp_lt70 = sum(r[3] for r in rows if r[5] < 0.70)
print(f"    departments with service<70%: {n_lt70} covering {skus_lt70} skus, GP {gp_lt70:,.0f}")
