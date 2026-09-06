"""CONFIRMATION PASS 3 -- held out.

Everything in PASS 1/2 is fitted (d from corrected_ads_from_pos.json) and
evaluated (the engine_ablation / full_sweep numbers) using the SAME period.
Six real monthly extracts exist for org 027 -- jan/mar/may/jun/sep/oct_cash.xlsx
(2025 calendar days: 31,31,31,30,30,31; July excluded, POS outage) -- and they
are real POS, not synthetic, so testing against them is not circular the way a
simulator built from the same law would be (T5).

CAVEAT, stated up front and not worked around: these extracts have NO date or
time column (see devkit/probe_empirical_cv.py's loader; verified directly --
no header in {jan,mar,may,jun,sep,oct}_cash.xlsx contains a date field). They
are per-SKU MONTHLY totals for store 027 only. There is therefore no way to
re-run the day-level, 12-seed, 315-day engine_ablation simulation on real
held-out data -- that simulation's entire mechanism (daily review, daily
stockout, daily expiry) needs daily timestamps this install does not have for
any real POS extract. A held-out re-run of the FULL simulated service/waste/EP
ranking is NOT POSSIBLE with the data that exists, and no weaker substitute is
offered for the pass I cannot run.

What IS run instead, honestly labelled as coarser:
  A. d GENERALISATION. Fit d on {jan,mar,may} (93 days), predict {jun,sep,oct}
     (91 days), by stratum of fitted d. Both the classic and order_up_to paths
     stand on this one number more than on anything else (PASS 2, item 1:
     elasticity of R and L together dwarf cv) -- if d does not generalise,
     nothing downstream can.
  B. cv GENERALISATION -- the actual test of phi=0.40. All 6 months (not just
     the split) give one empirical monthly cv per SKU; compare it against the
     value the production law (cv(d)=sqrt(1/d+0.4^2), aggregated to a month)
     predicts, by decile of d. This is what the config's own footnote claims
     ("cv falls 0.541->0.268 across volume deciles") -- reproduced here from
     the source files rather than taken on trust.
  C. A held-out newsvendor check AT MONTHLY GRANULARITY: using d and the
     empirical monthly sigma fitted on {jan,mar,may} only, would the resulting
     S have covered the ACTUAL {jun,sep,oct} demand realisations? This is the
     coarsest available real stand-in for "does the ranking survive out of
     sample" -- coarser than daily, real rather than synthetic.
"""
from __future__ import annotations
import csv, json, math, sys
from pathlib import Path
from collections import defaultdict
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from oasis.logic import order_up_to as ou              # noqa: E402
from devkit.methodology.traps import ratio, run_all, non_circular  # noqa: E402

BOOK = ROOT / "oasis" / "data" / "order_book.csv"
CASH = ROOT / "devkit" / "cash_monthly_by_sku.json"

TRAIN = {"jan_cash.xlsx": 31, "mar_cash.xlsx": 31, "may_cash.xlsx": 31}
TEST = {"jun_cash.xlsx": 30, "sep_cash.xlsx": 30, "oct_cash.xlsx": 31}
ALL_MONTHS = {**TRAIN, **TEST}
PHI = 0.40


def load():
    rows = list(csv.DictReader(BOOK.open(encoding="utf-8")))
    cash = json.loads(CASH.read_text(encoding="utf-8"))
    return rows, cash


def main():
    rows, cash = load()
    print(non_circular("PASS3.d_and_cv_fit",
                       inputs=["jan_cash.xlsx", "mar_cash.xlsx", "may_cash.xlsx"],
                       downstream_of_behaviour=["order_up_to.recommend output",
                                                "engine_ablation simulated service/waste"]))
    n_train_days = sum(TRAIN.values()); n_test_days = sum(TEST.values())

    # -------- A. d generalisation ---------------------------------------
    recs = []
    for r in rows:
        sku = r["sku"]
        if not all(sku in cash[m] for m in ALL_MONTHS):
            continue
        q_train = sum(cash[m][sku] for m in TRAIN)
        q_test = sum(cash[m][sku] for m in TEST)
        d_train = q_train / n_train_days
        d_test = q_test / n_test_days
        recs.append((sku, float(r["d"]), d_train, d_test))
    print(f"\nA. d GENERALISATION -- {len(recs):,} SKUs present all 6 real months "
          f"AND in order_book.csv")
    d_train_arr = np.array([x[2] for x in recs])
    order = np.argsort(d_train_arr)
    n = len(recs)
    print(f"  {'decile':>7}{'n':>7}{'d_train range':>20}{'mean bias':>12}"
          f"{'median APE':>12}{'corr(train,test)':>18}")
    for dec in range(10):
        idx = order[dec*n//10:(dec+1)*n//10]
        if len(idx) < 5:
            continue
        dt = d_train_arr[idx]
        de = np.array([recs[i][3] for i in idx])
        bias = float(np.mean(de - dt))
        ape = np.abs(de - dt) / np.maximum(dt, 1e-6)
        med_ape = float(np.median(ape))
        corr = float(np.corrcoef(dt, de)[0, 1]) if len(idx) > 2 else float("nan")
        print(f"  {dec:>7}{len(idx):>7}{dt.min():>9.3f}-{dt.max():<10.3f}"
              f"{bias:>+12.4f}{med_ape:>12.1%}{corr:>18.3f}")
    all_dt = d_train_arr; all_de = np.array([x[3] for x in recs])
    overall_bias = float(np.mean(all_de - all_dt))
    overall_corr = float(np.corrcoef(all_dt, all_de)[0, 1])
    print(f"  OVERALL bias(test-train) = {overall_bias:+.4f} units/day "
          f"({100*overall_bias/max(np.mean(all_dt),1e-9):+.1f}% of mean d) "
          f"· corr = {overall_corr:.3f}")
    run_all([ratio(float(np.mean(all_de)), float(np.mean(all_dt)),
                   "mean d_test / mean d_train (chain-wide seasonal drift)",
                   lo=0.5, hi=2.0)])

    # -------- B. cv generalisation: production law vs real dispersion ---
    print(f"\nB. cv GENERALISATION -- empirical monthly cv (all 6 real months) vs "
          f"the production law cv(d)=sqrt(1/d+phi^2), phi={PHI}, aggregated to a month")
    cvrecs = []
    for r in rows:
        sku = r["sku"]
        if not all(sku in cash[m] for m in ALL_MONTHS):
            continue
        rates = np.array([cash[m][sku] / days for m, days in ALL_MONTHS.items()])
        tot = sum(cash[m][sku] for m in ALL_MONTHS)
        if tot < 30:            # T3: need enough volume that a variance estimate means something
            continue
        mean_rate = rates.mean()
        if mean_rate <= 0:
            continue
        cv_emp_daily = rates.std(ddof=1) / mean_rate     # month-to-month cv of the DAILY rate
        cvrecs.append((sku, mean_rate, cv_emp_daily))
    cvrecs.sort(key=lambda x: x[1])
    m = len(cvrecs)
    print(f"  {m:,} SKUs with >=30 units across the 6 real months (enough to estimate dispersion)")
    AVG_MONTH_DAYS = sum(ALL_MONTHS.values()) / len(ALL_MONTHS)
    print(f"  cv_law is the PRODUCTION daily law AGGREGATED to a "
          f"{AVG_MONTH_DAYS:.1f}-day month: sqrt(1/(days*d) + phi^2/days) -- "
          f"the earlier unscaled sqrt(1/d+phi^2) would compare a single-day cv "
          f"to a monthly-total cv, which is exactly the T4 like-for-like trap.")
    print(f"  {'decile':>7}{'n':>7}{'d range':>18}{'cv_empirical(median)':>22}"
          f"{'cv_law(phi=.40,monthly)':>24}{'cv_law(phi=.20,monthly)':>24}")
    decile_summary = []
    for dec in range(10):
        seg = cvrecs[dec*m//10:(dec+1)*m//10]
        if len(seg) < 5:
            continue
        ds = np.array([x[1] for x in seg])
        cve = np.array([x[2] for x in seg])
        cv_law = np.sqrt(1.0/(AVG_MONTH_DAYS*ds) + (PHI*PHI)/AVG_MONTH_DAYS)
        cv_law2 = np.sqrt(1.0/(AVG_MONTH_DAYS*ds) + (0.20*0.20)/AVG_MONTH_DAYS)
        med_emp = float(np.median(cve))
        print(f"  {dec:>7}{len(seg):>7}{ds.min():>8.3f}-{ds.max():<9.3f}"
              f"{med_emp:>22.3f}{float(np.median(cv_law)):>24.3f}"
              f"{float(np.median(cv_law2)):>24.3f}")
        decile_summary.append((dec, med_emp, float(np.median(cv_law))))
    resid = [(d, e, l, e/l if l>0 else float('nan')) for d,e,l in decile_summary]
    print(f"  empirical / model(phi=.40,monthly) ratio by decile: "
          + ", ".join(f"{r[3]:.2f}" for r in resid))
    lo_cv = decile_summary[0][1]; hi_cv = decile_summary[-1][1]
    print(f"  empirical cv falls {lo_cv:.3f} -> {hi_cv:.3f} across deciles "
          f"(config footnote claims 0.541 -> 0.268)")
    print(f"  NOTE: this is MONTH-TO-MONTH dispersion of a monthly rate, not "
          f"day-to-day dispersion of a daily count -- it is the best real signal "
          f"available (no daily POS timestamps exist) but is not the same quantity "
          f"phi parameterises. Treat as directional corroboration, not a fit.")

    # -------- C. held-out newsvendor at monthly granularity --------------
    print(f"\nC. HELD-OUT NEWSVENDOR (monthly granularity, real data)")
    print(f"  Fit d, sigma_month on {{jan,mar,may}} only; would S have covered "
          f"the ACTUAL {{jun,sep,oct}} realisation, one month at a time?")
    hit = 0; short_months = 0; tot_months = 0
    z = ou.z_score()
    short_examples = []
    for r in rows:
        sku = r["sku"]
        if not all(sku in cash[m] for m in ALL_MONTHS):
            continue
        train_rates = np.array([cash[m][sku] / days for m, days in TRAIN.items()])
        d_fit = train_rates.mean()
        if d_fit <= 0:
            continue
        sig_fit = train_rates.std(ddof=1) if len(train_rates) > 1 else 0.0
        R = float(r["R"]); L = float(r["L"])
        for m, days in TEST.items():
            # sig_fit is already a month-to-month std of the DAILY RATE (n=3 obs);
            # projecting to a month's TOTAL multiplies by days directly -- do not
            # also scale by sqrt(days) as if this were a within-month daily std,
            # that double-discounts the same uncertainty (the earlier version did).
            S_month = d_fit * days + z * sig_fit * days
            actual = cash[m][sku]
            tot_months += 1
            if actual <= S_month:
                hit += 1
            else:
                short_months += 1
                if actual - S_month > 20:
                    short_examples.append((sku, m, d_fit, S_month, actual))
    print(f"  {tot_months:,} SKU-months evaluated; S (fit on train) would have "
          f"covered {hit:,} ({hit/tot_months:.1%}) of the actual test-month totals")
    print(f"  {short_months:,} SKU-months ({short_months/tot_months:.1%}) the real "
          f"holdout month exceeded the train-fitted S -- this is the empirical, "
          f"out-of-sample analogue of 'service level', at monthly not daily grain")
    short_examples.sort(key=lambda x: -(x[4]-x[3]))
    print(f"  worst 8 by absolute overshoot:")
    for s in short_examples[:8]:
        print(f"    {s[0][:36]:<37}{s[1]:<14}d_fit={s[2]:>7.2f} S={s[3]:>8.1f} actual={s[4]:>8.0f}")

    print(json.dumps({
        "claim": "claim.ordering.demand-model-generalises-out-of-sample",
        "verdict": "supports" if abs(overall_bias) < 0.15*np.mean(all_dt) else "contradicts",
        "metric": {"skus_d_test": len(recs), "d_bias_pct": round(100*overall_bias/max(np.mean(all_dt),1e-9), 2),
                  "d_corr": round(overall_corr, 3), "cv_lo_decile": round(lo_cv, 3),
                  "cv_hi_decile": round(hi_cv, 3), "monthly_coverage_rate": round(hit/tot_months, 4)},
        "held_out": True, "provenance": "observed",
        "sources": ["source.cash-monthly-extracts-real-pos", "source.order-book"],
        "baseline": None, "beat_baseline": None, "traps": ["T3", "T5"],
        "notes": "Full daily-granularity re-simulation of service/waste on held-out "
                 "real data is NOT POSSIBLE: no real POS extract in this install "
                 "carries a date or time column (verified directly against all 6 "
                 "files' headers). This is the monthly-grain substitute, explicitly "
                 "coarser, not a silent swap-in for the daily test."}))

if __name__ == "__main__":
    raise SystemExit(main())
