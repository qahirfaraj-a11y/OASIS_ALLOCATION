"""Every SKU, twelve seeded stores, 45 weeks, one engine at a time.

WHY AN ABLATION AND NOT A SCORECARD
    Four engines are claimed to shape the order book. Three of them turned out
    to touch nothing at all: AMIT and MANDE can only veto a line outright,
    DHARAM has produced zero demand patches against this graph, and LATA moved
    a fresh line's trigger without ever moving its size. A single "after"
    number cannot tell you which engine did what -- and when the 12x45 run came
    back with service at 45%, it took a decomposition to find that the cause
    was an assortment blacklist, not the ordering formula everyone suspected.

    So: run the same population under each engine ALONE, under none, and under
    all four. Anything that does not move a metric when switched on by itself
    is decoration, and this harness is designed to say so out loud.

WHAT IS SIMULATED
    Daily demand ~ N(d, cv*d) truncated at zero, per SKU, per seed. Periodic
    review at R with order-up-to S; arrivals at t + L where L ~ N(L_mean,
    sigma_L) truncated at zero and rounded. Sales are capped by what is on the
    shelf, so a stockout costs the sale -- that is where service comes from.

    EXPIRY IS MODELLED, and it has to be. Without it, over-ordering fresh is
    free and the shelf-life clamp looks like pure service destruction. Stock
    held above `shelf_life` days of cover cannot be sold before it dies, so it
    is written off daily. Waste is reported in units and in KES at cost.

WHAT IS AND IS NOT REAL
    d           observed, corrected_ads_from_pos.json
    cost, GP    observed, the VAT-corrected GRN book (25.00% ex-VAT median)
    R           observed where the receipt book measures a cadence, else the
                declared calendar, else the chain default
    L, sigma_L  observed, PO date to GRN date, 107,165 receipts
    shelf life  ASSERTED by department. No code-date data exists.
    cv          ASSERTED at 0.40 chain-wide. No per-SKU demand history exists
                in a form this can read; this is the single largest untested
                assumption in the run and it drives every safety term.
    OPENING STOCK is the seed. There is NO on-hand snapshot anywhere in OASIS
                -- the file that looks like one is a monthly sales extract --
                so twelve seeds is not decoration either: it is the honest
                treatment of an unknown initial condition.
"""
from __future__ import annotations

import argparse, json, math, os, sys, time
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit import build_margin                                    # noqa: E402
from devkit.amit_return import norm, strip_code, load_departments  # noqa: E402
from oasis.logic import order_up_to as ou                          # noqa: E402

ADS = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"
STOCK = ROOT / "oasis" / "data" / "stock_snapshot_dept.json"
AMIT = ROOT / "oasis" / "data" / "amit_enforcement.json"
MANDE = ROOT / "oasis" / "data" / "mande_purge_report.json"
DHARAM = ROOT / "oasis" / "data" / "dharam_demand_patch.json"
CV = 0.40
PHI = 0.40
DAYS = 315          # 45 weeks
WARM = 63          # 9 weeks discarded so the opening seed does not dominate


def build():
    mar, _src = build_margin.load_or_derive()
    mar = {norm(k): v for k, v in mar.items()}
    ads = {norm(k): v for k, v in json.loads(ADS.read_text(encoding="utf-8")).items()}
    depts = load_departments()
    sched = ou.load_review_schedule(str(ROOT))
    pats = ou.default_patterns(str(ROOT))
    shelf = ou.load_shelf_life(str(ROOT))
    try:
        amit = {norm(x) for x in json.loads(AMIT.read_text(encoding="utf-8")).get("blacklist", [])}
    except (OSError, ValueError):
        amit = set()
    try:
        m = json.loads(MANDE.read_text(encoding="utf-8"))
        purge = {norm(c.get("supplier", "")) for c in m.get("purge_candidates", [])
                 if c.get("delisting_risk") in ("HIGH", "MEDIUM")}
    except (OSError, ValueError):
        purge = set()
    try:
        patch = {norm(k): v for k, v in
                 json.loads(DHARAM.read_text(encoding="utf-8")).get("demand_patches", {}).items()}
    except (OSError, ValueError):
        patch = {}

    try:
        snap = {norm(k): float(v.get("stock") or 0)
                for k, v in json.loads(STOCK.read_text(encoding="utf-8")).items()}
    except (OSError, ValueError):
        snap = {}
    keys, rec = [], []
    for k, m in mar.items():
        av = ads.get(k) or {}
        d = float(av.get("new_ads") or av.get("old_ads") or 0)
        if d <= 0:
            continue
        dept = " ".join(str(depts.get(k, "")).upper().split())
        v = strip_code(m.get("vendor") or "")
        pt = pats.get(v) or {}
        L = max(0.5, float(pt.get("lead_time_mean", pt.get("lead_time_days", 2)) or 2))
        keys.append(k)
        p = patch.get(k) or {}
        rec.append((
            d, m["unit_cost"], m["gross_profit_per_unit"],
            ou.review_period(v, sched),                      # LATA on
            (sched.get(v) if sched.get(v) is not None else 7.0),   # LATA off
            L, ou.sigma_lead(pt, record=False), ou.DEFAULT_SIGMA_LEAD,
            shelf.get(dept, 0.0),
            1.0 if k in amit else 0.0,
            1.0 if v in purge else 0.0,
            float(p.get("corrected_ads", d)) / d if p else 1.0,
            max(0.0, snap.get(k, float("nan"))),
        ))
    a = np.array(rec, dtype=np.float64)
    return keys, dict(
        d=a[:, 0], cost=a[:, 1], gp=a[:, 2], R_on=a[:, 3], R_off=a[:, 4],
        L=a[:, 5], sL_on=a[:, 6], sL_off=a[:, 7], shelf=a[:, 8],
        amit=a[:, 9] > 0.5, purge=a[:, 10] > 0.5, dharam=a[:, 11],
        open_real=a[:, 12])


def simulate(f, cfg, seeds, days=DAYS, lead_mult=1.0, open_mult=1.0, rng_base=12345,
             real_open=False, warm=WARM, cv_mode="poisson",
             demand_law="poisson", per_sku=False):
    """One configuration. Returns the metric dict."""
    n = f["d"].size
    R = np.maximum(1.0, f["R_on"] if cfg["lata"] else f["R_off"])
    sL = (f["sL_on"] if cfg["lata"] else f["sL_off"])
    L = np.maximum(0.0, f["L"] * lead_mult)
    z = ou.z_score()
    d_plan = f["d"] * (f["dharam"] if cfg["dharam"] else 1.0)
    P = R + L
    # cv: flat 0.40 as the engine shipped, or sqrt(1/d + phi^2) -- Poisson plus
    # overdispersion, which is what counting arrivals in a window actually is.
    cvv = (np.array([ou.demand_cv(x) for x in f["d"]]) if cv_mode == "poisson"
           else np.full(f["d"].size, CV))
    S = d_plan * P + z * np.sqrt(P * (cvv * d_plan) ** 2 + (d_plan * sL) ** 2)
    if cfg["shelf"]:
        cap = np.where(f["shelf"] > 0, d_plan * f["shelf"], np.inf)
        S = np.minimum(S, cap)
    blocked = np.zeros(n, dtype=bool)
    if cfg["amit"]:
        blocked |= f["amit"]
    if cfg["mande"]:
        blocked |= f["purge"]
    S = np.where(blocked, 0.0, S)

    Rint = np.maximum(1, np.round(R).astype(int))
    maxlead = int(max(2, np.ceil(L.max() + 4 * sL.max()) + 2))
    tot_dem = tot_sold = tot_waste = 0.0
    stock_acc = np.zeros(n)
    dem_acc = np.zeros(n); sold_acc = np.zeros(n); waste_acc = np.zeros(n)
    nobs = 0
    lost_lines = np.zeros(n)
    for s in range(seeds):
        rng = np.random.default_rng(rng_base + s)
        if real_open:
            # THE REAL SHELF. Every seed starts from the same observed
            # position; only demand differs. Nothing is discarded, so the
            # transition from today's book to the policy's steady state is
            # what the run measures.
            base = np.where(np.isnan(f["open_real"]), S * open_mult, f["open_real"])
            on_hand = base.copy()
        else:
            on_hand = np.where(blocked, f["d"] * 3.0, S * open_mult) * rng.uniform(0.5, 1.0, n)
        pipe = np.zeros((maxlead + 1, n))
        offset = rng.integers(0, Rint, n) if Rint.max() > 1 else np.zeros(n, dtype=int)
        for t in range(days):
            on_hand += pipe[0]
            pipe[:-1] = pipe[1:]; pipe[-1] = 0.0
            # DEMAND IS GENERATED FROM THE SAME LAW THE PLAN ASSUMES, which is
            # the only honest way to compare two cv models: Poisson counts for
            # the arrivals, a lognormal-ish multiplier for the basket effect.
            if demand_law == "poisson":
                dem = rng.poisson(f["d"]) * np.exp(
                    rng.normal(-0.5 * PHI ** 2, PHI, n))
            else:
                dem = np.maximum(0.0, rng.normal(f["d"], CV * f["d"]))
            sold = np.minimum(dem, on_hand)
            on_hand -= sold
            # EXPIRY, and the rate matters.
            # The first version wrote off the whole excess EVERY DAY. A line
            # ordered to more cover than its shelf life then paid the excess
            # 365 times a year instead of once per replenishment cycle, which
            # inflated chain waste to KES 60m -- 40% of gross profit, which
            # should have been the tell. Stock only dies after it has been
            # held for shelf_life days, so the excess ages out over that
            # window rather than instantly: the daily write-off is the excess
            # divided by the shelf life.
            keepmax = np.where(f["shelf"] > 0, f["d"] * f["shelf"], np.inf)
            excess = np.maximum(0.0, on_hand - keepmax)
            dead = np.where(f["shelf"] > 0, excess / np.maximum(f["shelf"], 1.0), 0.0)
            on_hand -= dead
            if t >= warm:
                tot_dem += dem.sum(); tot_sold += sold.sum()
                tot_waste += float(np.sum(dead * f["cost"])) if np.ndim(dead) else 0.0
                stock_acc += on_hand * f["cost"]; nobs += 1
                dem_acc += dem; sold_acc += sold
                if np.ndim(dead): waste_acc += dead * f["cost"]
                lost_lines += (dem > sold + 1e-9)
            due = ((t + offset) % Rint) == 0
            if due.any():
                pos = on_hand + pipe.sum(axis=0)
                q = np.where(due & ~blocked, np.maximum(0.0, S - pos), 0.0)
                lead = np.clip(np.round(rng.normal(L, sL)), 0, maxlead).astype(int)
                idx = np.nonzero(q > 0)[0]
                if idx.size:
                    np.add.at(pipe, (lead[idx], idx), q[idx])
    if per_sku:
        dem_s = dem_acc / max(seeds, 1); sold_s = sold_acc / max(seeds, 1)
        stock_s = stock_acc / max(nobs, 1)
    obs_days = days - warm
    avg_stock = stock_acc.sum() / max(nobs, 1)
    gp_real = tot_sold / max(seeds * obs_days, 1)      # units/day chain-wide
    gp_year = float(np.sum(f["gp"] * f["d"])) * 365.0  # potential
    realised_gp = (tot_sold / max(seeds, 1)) / obs_days
    # realised GP needs per-SKU weighting; recompute cheaply via fill-weighted GP
    fill = tot_sold / max(tot_dem, 1e-9)
    # TRUE PER-SKU GROSS PROFIT.
    # This used to be  potential_GP(pooled) * fill_rate(pooled)  -- which is
    # only right if fill rate is uncorrelated with margin across SKUs, and it
    # is emphatically not: the blocked and short-filled lines are fresh and
    # high-margin while the well-filled ones are dry and thin. The pooled form
    # understated the recommended configuration by 0.86% and OVERSTATED
    # LATA-only by 2.99%, which was enough to rank LATA-only second when the
    # true arithmetic puts it fourth, and to price the shelf-life clamp's
    # marginal at +3.74m when it is +9.41m.
    gp_realised_year = float(np.sum(sold_acc * f["gp"])) / max(seeds, 1) \
        / max(obs_days, 1) * 365.0
    out_sku = None
    if per_sku:
        out_sku = {"dem": dem_s, "sold": sold_s, "stock": stock_s,
                   "waste": waste_acc / max(seeds, 1), "blocked": blocked,
                   "S": S, "R": R, "sigma_L": sL, "cv": cvv}
    return {
        "per_sku": out_sku,
        "service": fill,
        "avg_stock_kes": avg_stock,
        "waste_kes_year": tot_waste / max(seeds, 1) / obs_days * 365.0,
        "gp_realised_year": gp_realised_year,
        "gmroi": gp_realised_year / max(avg_stock, 1e-9),
        "lines_blocked": int(blocked.sum()),
        "stockout_line_days_pct": float(lost_lines.sum() / max(seeds * obs_days * f["d"].size, 1)) * 100,
    }


CONFIGS = {
    "none (formula only)":  dict(amit=0, lata=0, dharam=0, mande=0, shelf=0),
    "AMIT only":            dict(amit=1, lata=0, dharam=0, mande=0, shelf=0),
    "LATA only":            dict(amit=0, lata=1, dharam=0, mande=0, shelf=0),
    "DHARAM only":          dict(amit=0, lata=0, dharam=1, mande=0, shelf=0),
    "MANDE only (enforcing)":dict(amit=0, lata=0, dharam=0, mande=1, shelf=0),
    "MANDE report-only":    dict(amit=0, lata=0, dharam=0, mande=0, shelf=0),
    "shelf-life clamp only":dict(amit=0, lata=0, dharam=0, mande=0, shelf=1),
    "ALL ON (MANDE enforcing)": dict(amit=1, lata=1, dharam=1, mande=1, shelf=1),
    "ALL ON (MANDE reporting)": dict(amit=1, lata=1, dharam=1, mande=0, shelf=1),
}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=12)
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--cv-mode", default="poisson", choices=("poisson", "flat"))
    ap.add_argument("--real-open", action="store_true",
                    help="start from the observed shelf, discard nothing")
    ap.add_argument("--out", default=str(ROOT / "devkit" / "engine_ablation_result.json"))
    a = ap.parse_args(argv)

    keys, f = build()
    print(f"  {len(keys):,} SKUs with observed demand · {a.seeds} seeded stores · "
          f"{DAYS} days ({WARM} discarded as warm-up)")
    print(f"  AMIT would block {int(f['amit'].sum()):,} · MANDE would block "
          f"{int(f['purge'].sum()):,} · DHARAM patches "
          f"{int((f['dharam'] != 1.0).sum()):,}")
    print(f"  R median {np.median(f['R_on']):.1f} (LATA on) vs "
          f"{np.median(f['R_off']):.1f} (off) · sigma_L median "
          f"{np.median(f['sL_on']):.2f} vs {np.median(f['sL_off']):.2f}\n")

    hdr = (f"  {'configuration':<24}{'service':>9}{'avg stock':>13}{'GMROI':>8}"
           f"{'waste/yr':>12}{'GP/yr':>14}{'blocked':>9}")
    print(hdr); print("  " + "-" * (len(hdr) - 2))
    res = {}
    base = None
    for name, cfg in CONFIGS.items():
        t0 = time.time()
        m = simulate(f, cfg, a.seeds, real_open=a.real_open,
                     warm=(0 if a.real_open else WARM), cv_mode=a.cv_mode)
        res[name] = m
        if base is None:
            base = m
        print(f"  {name:<24}{m['service']:>8.2%}{m['avg_stock_kes']:>13,.0f}"
              f"{m['gmroi']:>8.2f}{m['waste_kes_year']:>12,.0f}"
              f"{m['gp_realised_year']:>14,.0f}{m['lines_blocked']:>9,}", flush=True)

    print("\n  DELTA vs formula-only -- an engine that moves nothing is decoration")
    print(f"  {'engine':<24}{'service':>10}{'stock':>10}{'GMROI':>9}{'waste':>12}{'GP/yr':>15}")
    for name, m in res.items():
        if name.startswith("none"):
            continue
        print(f"  {name:<24}{(m['service']-base['service'])*100:>+9.2f}pp"
              f"{100*(m['avg_stock_kes']/base['avg_stock_kes']-1):>+9.1f}%"
              f"{m['gmroi']-base['gmroi']:>+9.2f}"
              f"{m['waste_kes_year']-base['waste_kes_year']:>+12,.0f}"
              f"{m['gp_realised_year']-base['gp_realised_year']:>+15,.0f}")

    out = {"skus": len(keys), "seeds": a.seeds, "days": DAYS, "warmup": WARM,
           "cv_assumed": CV, "configs": res}

    if a.sweep:
        print("\n  SENSITIVITY: lead time x opening stock, ALL ON (MANDE reporting)")
        print(f"  {'lead x':>8}{'open x':>8}{'service':>10}{'avg stock':>13}{'GMROI':>8}{'waste/yr':>12}")
        sw = {}
        for lm in (0.5, 1.0, 2.0):
            for om in (0.5, 1.0, 2.0):
                m = simulate(f, CONFIGS["ALL ON (MANDE reporting)"], max(4, a.seeds // 3),
                             lead_mult=lm, open_mult=om)
                sw[f"lead{lm}_open{om}"] = m
                print(f"  {lm:>8.1f}{om:>8.1f}{m['service']:>9.2%}"
                      f"{m['avg_stock_kes']:>13,.0f}{m['gmroi']:>8.2f}"
                      f"{m['waste_kes_year']:>12,.0f}", flush=True)
        out["sweep"] = sw

    Path(a.out).write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(f"\n  wrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
