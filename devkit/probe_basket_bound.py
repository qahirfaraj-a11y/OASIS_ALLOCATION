from __future__ import annotations
import sys, json
from pathlib import Path
from collections import defaultdict
import numpy as np
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.amit_return import norm

monthly = json.loads((ROOT/"devkit"/"cash_monthly_by_sku.json").read_text())
months = list(monthly.keys())
snap = json.loads((ROOT/"oasis"/"data"/"stock_snapshot_dept.json").read_text())
dept = {norm(k): " ".join(str(v.get("dept") or "").upper().split()) for k, v in snap.items()}

all_names = set()
for d in monthly.values():
    all_names |= set(d.keys())

# SKUs present in all 6 months, with a department
series = {}
for name in all_names:
    vals = np.array([monthly[m].get(name, 0.0) for m in months], dtype=float)
    if (vals > 0).sum() == 6 and name in dept and dept[name]:
        series[name] = vals

by_dept = defaultdict(list)
for name, vals in series.items():
    by_dept[dept[name]].append(vals)

print(f"skus usable: {len(series)}  departments: {len(by_dept)}")
rows = []
for d0, lst in by_dept.items():
    if len(lst) < 8:
        continue
    arr = np.array(lst)  # skus x 6
    sku_var_sum = arr.var(axis=1, ddof=1).sum()
    dept_total = arr.sum(axis=0)  # 6 months
    dept_var = dept_total.var(ddof=1)
    ratio = dept_var / sku_var_sum if sku_var_sum > 0 else np.nan
    rows.append((d0, len(lst), dept_total.mean(), sku_var_sum, dept_var, ratio))

rows.sort(key=lambda r: -r[2])
print(f"{'dept':<26}{'nsku':>6}{'dept_mean':>12}{'sum(var_i)':>14}{'var(sum)':>14}{'ratio':>8}")
for r in rows[:25]:
    print(f"{r[0][:25]:<26}{r[1]:>6}{r[2]:>12,.0f}{r[3]:>14,.0f}{r[4]:>14,.0f}{r[5]:>8.2f}")
ratios = np.array([r[5] for r in rows if not np.isnan(r[5])])
weights = np.array([r[1] for r in rows if not np.isnan(r[5])])
print(f"\nmedian ratio (unweighted, {len(ratios)} depts): {np.median(ratios):.3f}")
print(f"mean ratio weighted by n_sku: {np.average(ratios, weights=weights):.3f}")
print(f"share of depts with ratio<1 (net-negative co-movement, consistent w/ substitution): {(ratios<1).mean():.1%}")
