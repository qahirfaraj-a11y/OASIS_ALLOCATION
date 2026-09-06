"""The velocity multiplier as mathematics, and what the return book knows about fresh.

PART ONE -- THE VELOCITY MULTIPLIER
    intelligence_mixin.py:296-308 scales the whole target:

        target_days *= v(d),   v = 1.4 (d>10), 1.3 (d>5), 1.2 (d>2), 1.0 (d>1), 0.8

    This is the spike/bulk-event preparation, and the INSTINCT is right: fast
    movers have fatter demand tails, so a bulk-shopping day hurts them more.
    The IMPLEMENTATION puts it in the wrong term.

        S = d*P + z*sigma_P          sigma_P = d*sqrt(P*cv^2 + sigma_L^2)

    Multiplying target_days scales BOTH terms. But cycle stock d*P is not a
    risk quantity -- it is the demand that will certainly arrive over the
    protection interval, and it is already proportional to d. Scaling it says
    a fast mover needs more than its own demand, which is not a statement
    about variance, it is just more stock.

    Meanwhile the safety term ALREADY scales with d: sigma_P is proportional to
    d, so a SKU selling ten times as much already carries ten times the safety
    stock in units. The multiplier is applied on top of a term that has already
    handled velocity, and applied equally to a term that must not be scaled at
    all.

    The variance is where the tail belongs. A step in v(d) is exactly
    equivalent to a step in cv, and this reports the cv each band implies -- so
    the same intent survives, in the term the derivation can defend, and the
    cliff between 9.99/day and 10.01/day disappears.

PART TWO -- WHAT THE RETURN BOOK SAYS ABOUT FRESH
    Receipt-to-write-off is contaminated three ways (see derive_shelf_life),
    but one class of return is NOT: a write-off within days of receipt is not
    a shelf-life measurement at all. It is short-dated stock arriving. For a
    daily-delivery dairy or bakery that is a supplier-performance fact with a
    price on it, and it is measured, not asserted.
"""
from __future__ import annotations

import argparse, glob, json, math, sys, datetime as dt, statistics as st
from collections import defaultdict
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, run_all      # noqa: E402
from devkit import build_margin                                    # noqa: E402
from devkit.probe_order_optimum import load, Phi, G, inv_Phi       # noqa: E402
from devkit.derive_shelf_life import norm, parse_date              # noqa: E402
from oasis.logic import order_up_to as ou                          # noqa: E402

BANDS = ((10.0, 1.4), (5.0, 1.3), (2.0, 1.2), (1.0, 1.0), (0.0, 0.8))


def v_of(d):
    out = np.full_like(d, 0.8)
    for lo, m in sorted(BANDS):
        out = np.where(d > lo, m, out)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(); ap.add_argument("--cv", type=float, default=0.40)
    a = ap.parse_args(argv)
    f = load(); n = f["d"].size
    d, c, gp, R, L, sL = f["d"], f["cost"], f["gp_unit"], f["R"], f["L"], f["sigma_L"]
    P = R + L; z = f["z"]; cv = a.cv
    sP = d * np.sqrt(P * cv ** 2 + sL ** 2)
    S = d * P + z * sP
    v = v_of(d)

    print("  PART ONE -- THE VELOCITY MULTIPLIER\n")
    print(f"  {'band':<18}{'skus':>7}{'v':>6}{'stock added KES':>18}{'implied cv':>13}")
    tot_add = 0.0
    for lo, m in sorted(BANDS, reverse=True):
        hi = next((x for x, _ in sorted(BANDS) if x > lo), None)
        mask = (d > lo) & ((d <= hi) if hi else True)
        if not mask.any(): continue
        add = float(np.sum((v[mask] - 1.0) * S[mask] * c[mask])); tot_add += add
        # the cv that would produce the same S through the SAFETY term alone
        need = (m * S[mask] - d[mask] * P[mask]) / np.maximum(z[mask], 1e-9)
        inside = (need / np.maximum(d[mask], 1e-9)) ** 2 - sL[mask] ** 2
        cv_eq = np.sqrt(np.maximum(inside, 0)) / np.sqrt(np.maximum(P[mask], 1e-9))
        lbl = f"d > {lo:g}" if lo else "d <= 1"
        print(f"  {lbl:<18}{int(mask.sum()):>7,}{m:>6.1f}{add:>18,.0f}{np.median(cv_eq):>13.2f}")
    print(f"  {'TOTAL':<18}{n:>7,}{'':>6}{tot_add:>18,.0f}")
    print(f"\n  The multiplier adds KES {tot_add:,.0f} of order-up-to stock, and it does it")
    print(f"  by scaling CYCLE stock as well as safety. The right-hand column is the cv")
    print(f"  that reaches the same level through the safety term alone -- the same")
    print(f"  intent, in the term the derivation can defend, with no cliff at d = 10.")
    below = float(np.sum((1.0 - v[d <= 1.0]) * S[d <= 1.0] * c[d <= 1.0]))
    print(f"  Note the bottom band cuts, it does not add: v = 0.8 removes KES {below:,.0f}")
    print(f"  from slow movers, which is a MARGIN decision wearing a variance costume.")

    # -------- PART TWO: the fresh return book -------------------------
    import openpyxl
    recv = defaultdict(list); vend = {}
    for fp in build_margin.workbooks():
        wb = openpyxl.load_workbook(fp, read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]; it = ws.iter_rows(values_only=True)
        h = [str(x).strip() if x else "" for x in next(it)]
        try:
            ni, di, vi = h.index("Item Name"), h.index("GRN Date"), h.index("Vendor Code - Name")
        except ValueError:
            wb.close(); continue
        for r in it:
            if len(r) <= max(ni, di, vi) or not r[ni]: continue
            dd = parse_date(r[di])
            if dd: k = norm(r[ni]); recv[k].append(dd); vend.setdefault(k, norm(r[vi]))
        wb.close()
    for k in recv: recv[k].sort()

    stock = {norm(k): v for k, v in json.loads(
        (ROOT / "oasis" / "data" / "stock_snapshot_dept.json").read_text(encoding="utf-8")).items()}
    mar = {norm(k): v for k, v in build_margin.load_or_derive()[0].items()}
    FRESH = {"FRESH MILK", "BREAD", "YOGHURT", "CHEESE", "CAKES", "EGGS",
             "FRESH GOURMET", "FRESH CREAM", "BUTTER", "ICE-CREAM",
             "FROZEN GOURMET", "PLANT BASED"}
    short = defaultdict(lambda: [0, 0.0, 0])   # vendor -> [returns, value, skus]
    seen = defaultdict(set)
    tot_ret = tot_short = 0; short_val = 0.0
    for fp in sorted(glob.glob(str(ROOT / "oasis" / "data" / "prts_*.xlsx"))):
        wb = openpyxl.load_workbook(fp, read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]; it = ws.iter_rows(values_only=True)
        h = [str(x).strip() if x else "" for x in next(it)]
        try:
            ni, di, ri, qi, ai = (h.index("Item Name"), h.index("Doc Date"),
                                  h.index("Reason"), h.index("Rejc Qty"), h.index("Net Amt"))
        except ValueError:
            wb.close(); continue
        for r in it:
            if len(r) <= max(ni, di, ri, qi, ai) or not r[ni]: continue
            if "EXPIR" not in norm(r[ri]): continue
            k = norm(r[ni]); dept = norm((stock.get(k) or {}).get("dept") or "")
            if dept not in FRESH: continue
            tot_ret += 1
            dd = parse_date(r[di])
            prior = [x for x in recv.get(k, []) if x <= dd] if dd else []
            if not prior: continue
            days = (dd - prior[-1]).days
            if days <= 3:
                tot_short += 1
                val = float(r[ai] or 0); short_val += val
                vn = vend.get(k, "?")
                short[vn][0] += 1; short[vn][1] += val; seen[vn].add(k)
        wb.close()
    for vn in short: short[vn][2] = len(seen[vn])

    print(f"\n  PART TWO -- FRESH, FROM THE RETURN BOOK")
    print(f"  {tot_ret:,} expiry returns on fresh lines · {tot_short:,} of them written off")
    print(f"  within 3 days of the delivery that brought them ({100*tot_short/max(tot_ret,1):.0f}%),")
    print(f"  worth KES {short_val:,.0f}. That is not shelf life, it is short-dated stock")
    print(f"  arriving -- a supplier-performance fact, measured.")
    print(f"\n  {'vendor':<44}{'skus':>6}{'returns':>9}{'value KES':>13}")
    for vn, b in sorted(short.items(), key=lambda kv: -kv[1][1])[:12]:
        print(f"  {vn[:43]:<44}{b[2]:>6}{b[0]:>9,}{b[1]:>13,.0f}")

    run_all([join_match_rate(list(short), list(vend.values()),
                             "short-dating vendors -> GRN vendors", floor=0.50)])
    print(json.dumps({
        "claim": "claim.ordering.velocity-belongs-in-the-variance",
        "verdict": "contradicts",
        "metric": {"skus": n, "velocity_stock_added_kes": round(tot_add),
                   "slow_band_stock_removed_kes": round(below),
                   "fresh_expiry_returns": tot_ret,
                   "short_dated_returns": tot_short,
                   "short_dated_value_kes": round(short_val),
                   "short_dating_vendors": len(short)},
        "held_out": False, "provenance": "observed",
        "sources": ["source.purchase-returns", "source.grn-book", "source.order-book"],
        "baseline": "target_days *= v(d)", "beat_baseline": None,
        "traps": ["T1", "T3"],
        "notes": ("The velocity multiplier scales cycle stock, which is not a "
                  "risk quantity, and scales safety stock a second time when "
                  "sigma_P is already proportional to d. The equivalent cv per "
                  "band puts the same intent in the variance term.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
