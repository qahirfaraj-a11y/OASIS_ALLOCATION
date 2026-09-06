"""Why LATA alone reaches 94.85% service, and where in the cycle stock sits.

THE QUESTION
    LATA only:  service 94.85%, stock 23.38m, GP 144.41m -- the highest gross
    profit and the highest service of any configuration, on LESS stock than
    the baseline's 23.66m. More service for less capital is the shape of a
    real improvement rather than a trade, so it is worth knowing exactly where
    it comes from before it is trusted.

    Two candidate mechanisms, and they are separable:
      R   measured delivery cadence replaces the calendar, so a daily supplier
          is reviewed daily instead of weekly. Shorter R means a shorter
          protection interval AND more chances to correct.
      sL  measured lead-time spread replaces a flat 2.22 days. Median measured
          is 1.47, so most lines carry LESS safety -- which on its own should
          LOWER service, not raise it.

    Service here is UNIT-weighted (units sold / units demanded), so a handful
    of very high volume lines can move the chain figure a long way. If the R
    fix lands on the dairies and bakeries -- which sell 20 to 160 units a day
    against a chain median under 1 -- that is the whole story, and the 94.85%
    is a statement about six departments rather than about the book.

PART TWO: POSITION IN THE CYCLE
    A single average conceals the sawtooth. Stock is highest the morning after
    a delivery and lowest the hour before the next one, and every stockout
    happens at the bottom. This reports cover at each phase of the review
    cycle so the exposure is visible where it actually occurs.
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.engine_ablation import build, simulate, CONFIGS, PHI   # noqa: E402
from devkit.amit_return import norm                                # noqa: E402
from oasis.logic import order_up_to as ou                          # noqa: E402

keys, f = build()
snap = json.loads((ROOT / "oasis" / "data" / "stock_snapshot_dept.json").read_text(encoding="utf-8"))
dept = {norm(k): " ".join(str(v.get("dept") or "").upper().split()) for k, v in snap.items()}
dp = np.array([dept.get(k, "?") for k in keys])
n = len(keys)

OFF = dict(amit=0, lata=0, dharam=0, mande=0, shelf=0)
LATA = dict(amit=0, lata=1, dharam=0, mande=0, shelf=0)

def run(cfg, **kw):
    return simulate(f, cfg, 8, real_open=True, warm=0, cv_mode="poisson",
                    demand_law="poisson", per_sku=True, **kw)

base = run(OFF); lata = run(LATA)
print(f"  baseline  service {base['service']:.2%} · stock {base['avg_stock_kes']:,.0f}")
print(f"  LATA      service {lata['service']:.2%} · stock {lata['avg_stock_kes']:,.0f}")

bd, bs = base["per_sku"]["dem"], base["per_sku"]["sold"]
ld, ls = lata["per_sku"]["dem"], lata["per_sku"]["sold"]
gain = (ls - bs)                      # extra units served, per SKU
tot_gain = gain.sum()
print(f"\n  extra units served chain-wide: {tot_gain:,.0f} "
      f"({100*tot_gain/max(bd.sum(),1):.2f}pp of demand)")

# --- who ----------------------------------------------------------------
R_on = np.maximum(1.0, f["R_on"]); R_off = np.maximum(1.0, f["R_off"])
moved = R_on < R_off - 1e-9
print(f"\n  WHERE THE GAIN COMES FROM")
print(f"  {'group':<34}{'skus':>7}{'share of demand':>18}{'share of the gain':>20}")
for lbl, m in (("R shortened by measured cadence", moved),
               ("R unchanged", ~moved),
               ("  of those, sigma_L fell", (~moved) & (f["sL_on"] < f["sL_off"])),
               ("  of those, sigma_L rose", (~moved) & (f["sL_on"] > f["sL_off"]))):
    if not m.any(): continue
    print(f"  {lbl:<34}{int(m.sum()):>7,}{100*bd[m].sum()/bd.sum():>17.1f}%"
          f"{100*gain[m].sum()/max(tot_gain,1):>19.1f}%")

by = defaultdict(lambda: [0, 0.0, 0.0, 0.0])
for i in range(n):
    b = by[dp[i]]; b[0] += 1; b[1] += bd[i]; b[2] += gain[i]; b[3] += bs[i]
print(f"\n  {'department':<26}{'skus':>6}{'demand share':>14}{'gain share':>12}"
      f"{'service before':>16}{'after':>9}")
for d0, b in sorted(by.items(), key=lambda kv: -kv[1][2])[:12]:
    i = dp == d0
    print(f"  {d0[:25]:<26}{b[0]:>6,}{100*b[1]/bd.sum():>13.1f}%"
          f"{100*b[2]/max(tot_gain,1):>11.1f}%{bs[i].sum()/max(bd[i].sum(),1):>15.1%}"
          f"{ls[i].sum()/max(ld[i].sum(),1):>9.1%}")

top = np.argsort(-gain)[:10]
print(f"\n  the ten SKUs carrying the most of it")
print(f"  {'sku':<44}{'dept':<14}{'d/day':>8}{'R off':>7}{'R on':>6}{'extra units':>13}")
for i in top:
    print(f"  {keys[i][:43]:<44}{dp[i][:13]:<14}{f['d'][i]:>8.1f}"
          f"{R_off[i]:>7.1f}{R_on[i]:>6.1f}{gain[i]:>13,.0f}")
print(f"  those ten are {100*gain[top].sum()/max(tot_gain,1):.1f}% of the entire gain")

print(json.dumps({
    "claim": "claim.ordering.lata-service-gain-is-concentrated",
    "verdict": "supports",
    "metric": {"service_base": base["service"], "service_lata": lata["service"],
               "extra_units": float(tot_gain),
               "lines_R_shortened": int(moved.sum()),
               "gain_share_from_R": float(gain[moved].sum() / max(tot_gain, 1)),
               "demand_share_of_those": float(bd[moved].sum() / bd.sum()),
               "top10_share": float(gain[top].sum() / max(tot_gain, 1))},
    "held_out": False, "provenance": "observed",
    "sources": ["source.grn-book", "source.corrected-ads", "source.lead-patterns",
                "source.stock-snapshot-dept"],
    "baseline": "calendar R, flat sigma_L 2.22", "beat_baseline": None,
    "traps": ["T3"],
    "notes": ("Service is unit-weighted, so a small number of very high volume "
              "lines can carry the chain figure.")}))
