"""Bread backtest: replay 1 Apr - 15 Sep one day at a time, suppliers vs engine.

WHAT IT ANSWERS
    Which bread ordering tunings would have sold more and wasted less than
    what the suppliers actually dropped, scored in one shared world.

THE WORLD (identical for every arm)
    demand      each SKU's measured sales for that month (bread-department monthly
                files; the period total for cakes, cookies and wraps), SHAPED
                across days by the SKU's own delivery rhythm -- each GRN's units
                spread evenly over the days until the next GRN -- then drawn
                Poisson per day, the same seeded draws for every arm.
                Why shaped: with a flat daily rate the replay of the suppliers'
                real deliveries needed a 9-10 day shelf life to match observed
                expiry, because demand and deliveries were out of step. Real
                drops follow real selling days (a Saturday drop covers Sunday),
                so the rhythm is the best available daily shape. No arm sees
                future demand: the engine still orders from past sales only.
    deliveries  the engine orders at the END of the day, after that day's sales,
                and the order arrives the next morning, every day of the week.
                Bread IS delivered on Sundays; the store posts Sunday's GRNs on
                Monday (no GRN or return is ever dated Sunday, while 681 PO
                lines are raised on Sundays). So a Monday GRN whose PO was raised
                on Saturday is moved back to Sunday, and Monday GRNs with no PO
                in the PO report are split evenly across Sunday and Monday.
                LATA measures 0-1 days for all four bakeries and the GRNs show
                1 day on 82% of POs.
                Ordering before the day's sales was tried first and is wrong for
                bread: position then includes stock that the same day's demand
                consumes, and a 2-day ceiling saw-toothed into lost sales
    shape       --shape rhythm (default) or flat; every conclusion is checked
                under both
    shelf       FIFO batches that expire at a fixed age
    calibration two unknowns per family -- shelf life, and how far recorded
                sales understate demand (sales can never exceed what was on the
                shelf) -- fitted jointly so that replaying the suppliers' REAL
                GRN deliveries reproduces BOTH the expiry they returned and the
                share of deliveries they sold. Shelf life alone could not: it
                ran to the edge of any plausible range and still missed
    burn-in     every arm starts with one day of stock; the first 7 days are
                not scored

THE ARMS
    actual      the suppliers' real deliveries (GRN quantity on GRN date)
    engine      the shipped path, daily: SimulationOrderUtil.
                calculate_order_quantity -> finalize_orders ->
                apply_minimum_order_gate, on inputs captured once from
                prepare_sku_data, with stock, on-order and the demand rate
                updated each day. The demand rate is what the POS adapter would
                measure from this arm's own (censored) sales, through
                demand_rate.weighted_daily_rate
    tunings     the engine arm plus one change each, then all together:
      measured_life    drop the enrichment's hardcoded shelf_life_days=7
      velocity_cap     fresh S capped at d*(1 + (P-1)*h), h = 1.25 / 1.15 / 1.0
                       for 10+, 2-10, under 2 a day
      weekday          (kept for completeness; the premise -- no Sunday
                       deliveries -- turned out to be a posting artefact)
      cakes_fresh      cakes ordered as fresh: 1-day lead, measured shelf life
      slow_presence    lines whose d x shelf life < 1: hold one unit, no more
      service_bands    z by velocity and tone: 95% at 10+, 90% at 2-10,
                       85% under 2, one band lower for brown/wholemeal
    structural arms, added after the first grid showed every order-shrinking
    tuning collapsing availability for reasons that were not the tuning:
      gate_off         skip apply_minimum_order_gate. Its KES 200 per fresh
                       line and 10 units / KES 5,000 per supplier floors drop
                       exactly the small daily bread lines a tighter cap creates
      censor_fix       the demand rate from IN-STOCK days only. Measured from
                       raw sales, a line the engine stops ordering sells nothing,
                       its rate decays to zero, and it is never reordered: 13% of
                       SKU-days on the shipped arm, 27% once orders shrink
    selling-day horizon, added once the label data was known (bread is baked
    and delivered the same day, best-before 5 days after, pulled a day early:
    4 selling days on the shelf):
      overnight        for fresh lines the horizon counts SELLING days: an
                       order placed after close is on the shelf before the
                       next opening, so the lead adds none and P = R (1 day
                       for daily bread) instead of R + L (2). S is the same
                       order-up-to level on that P, capped at d x sellable
                       life; a line whose d x life is under one unit holds one
      sigmaL0          lead-time spread set to 0. LATA's bakery sigma_L is
                       measured PO-to-GRN, and Sunday deliveries posted on
                       Monday inflate it
    --life Loaves=4,Buns, rolls, scones=4 fixes a family's shelf life (selling
    days from delivery); only the hidden-demand uplift is then calibrated.
    Arms combine with "+", e.g. gate_off+censor_fix+velocity_cap.

NOTHING HERE WRITES TO oasis/data. The source files are read-only inputs.

    python devkit/bread_backtest.py --out <dir> [--seeds 5] [--data <downloads>]
"""
from __future__ import annotations

import argparse
import copy
import json
import math
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import math                 # noqa: E402
import numpy as np          # noqa: E402
import pandas as pd         # noqa: E402

START, END = datetime(2026, 4, 1), datetime(2026, 9, 15)
NDAYS = (END - START).days + 1
BURN_IN = 7
SUPPLIER_NAMES = {"SD0029": "DPL FESTIVE LIMITED", "SM0202": "MINI BAKERIES NBI LTD",
                  "SK0044": "KENAFRIC BAKERY LTD", "SB0030": "BROADWAYS BAKERY LTD"}
SHORT = {"DPL FESTIVE LIMITED": "DPL Festive", "MINI BAKERIES NBI LTD": "Mini Bakeries",
         "KENAFRIC BAKERY LTD": "Kenafric", "BROADWAYS BAKERY LTD": "Broadways"}
LONG_LIFE = {"Cookies": 30, "Crumbs": 30, "Wraps": 30}
TUNINGS = {"measured_life", "velocity_cap", "weekday", "cakes_fresh", "slow_presence", "service_bands", "overnight", "sigmaL0"}
PHI_ARM = "phi"          # e.g. "engine+phi0.20": the engine plans on cv = sqrt(1/d + 0.20^2)
ALL_TUNINGS = {"measured_life", "cakes_fresh", "slow_presence", "service_bands"}
STRUCTURAL = {"gate_off", "censor_fix"}
FIXED = "gate_off+censor_fix"
ARMS = (["actual", "engine", "gate_off", "censor_fix", FIXED]
        + [f"{FIXED}+{x}" for x in ["measured_life", "velocity_cap", "cakes_fresh", "slow_presence", "service_bands"]]
        + [f"{FIXED}+measured_life+cakes_fresh+slow_presence+service_bands",
           f"{FIXED}+velocity_cap+cakes_fresh+slow_presence+service_bands"])

norm = lambda s: re.sub(r"\s+", " ", str(s).upper()).strip()


# ── inputs ────────────────────────────────────────────────────────────────
def load_inputs(data_dir: str, report_json: str):
    """SKUs, monthly rates, prices and GRN deliveries, keyed by SKU name."""
    deep = json.load(open(report_json, encoding="utf-8"))
    skus = {s["name"]: s for s in deep["skus"]}
    bc = {b: s["name"] for s in deep["skus"] for b in s["barcodes"]}
    gr = pd.read_excel(os.path.join(data_dir, "GRNDS_4_NOW_BRD.xlsx"))
    gr = gr[gr["GRN No"] != "Total"].copy()
    gr["date"] = pd.to_datetime(gr["GRN Date"], format="%d-%b-%Y")
    gr = gr[(gr.date >= START) & (gr.date <= END)]
    gr["name"] = [bc.get(str(b)) or (norm(n) if norm(n) in skus else None)
                  for b, n in zip(gr["Bar Code"].astype(str), gr["Item Name"])]
    gr = gr[gr.name.notna()]
    gr["supplier"] = gr["Vendor Code - Name"].str[:6].map(SUPPLIER_NAMES)
    # Sunday deliveries are posted on Monday: put them back on Sunday
    po = pd.read_excel(os.path.join(data_dir, "BREAD_PO_DTS.xlsx"))
    po = po[po["PO NO"] != "Total"]
    po_date = dict(zip(po["PO NO"].astype(str), pd.to_datetime(po["PO DATE"], format="%d-%b-%Y")))
    deliveries = defaultdict(lambda: defaultdict(float))
    for n, d, q, pn in zip(gr.name, gr.date, gr["GRN Qty"], gr["PO No"].astype(str)):
        day = (d.to_pydatetime() - START).days
        if d.weekday() == 0 and day > 0:
            pd_ = po_date.get(pn)
            if pd_ is not None:
                if (d - pd_).days >= 2:          # raised Saturday or earlier -> delivered Sunday
                    day -= 1
                deliveries[n][day] += float(q)
            else:
                deliveries[n][day - 1] += float(q) / 2
                deliveries[n][day] += float(q) / 2
        else:
            deliveries[n][day] += float(q)
    deliveries = {n: dict(v) for n, v in deliveries.items()}
    supplier_of = gr.groupby("name")["supplier"].first().to_dict()
    months = [4, 5, 6, 7, 8, 9]
    rate = {}
    for n, s in skus.items():
        m = s.get("sold_per_day_m")
        rate[n] = {mm: (m[i] if m else s["ads"]) for i, mm in enumerate(months)}
    return skus, rate, deliveries, supplier_of


def observed_waste(skus, deliveries):
    out = defaultdict(lambda: [0.0, 0.0])
    for n, s in skus.items():
        rec = sum(deliveries.get(n, {}).values())
        if rec:
            out[s["family"]][0] += s["expired"]; out[s["family"]][1] += rec
    return {f: e / r for f, (e, r) in out.items() if r}


# ── the shelf ─────────────────────────────────────────────────────────────
class Shelf:
    """FIFO batches of (arrival_day, qty); expired at a fixed age."""
    __slots__ = ("batches", "life")

    def __init__(self, life: int, opening: float):
        self.life = life
        self.batches = [[-1, float(opening)]] if opening > 0 else []

    def expire(self, day: int) -> float:
        gone = 0.0
        keep = []
        for b in self.batches:
            if day - b[0] >= self.life:
                gone += b[1]
            else:
                keep.append(b)
        self.batches = keep
        return gone

    def receive(self, day: int, qty: float):
        if qty > 0:
            self.batches.append([day, float(qty)])

    def sell(self, want: float) -> float:
        got = 0.0
        for b in self.batches:
            take = min(b[1], want - got)
            b[1] -= take; got += take
            if got >= want - 1e-9:
                break
        self.batches = [b for b in self.batches if b[1] > 1e-9]
        return got

    @property
    def stock(self) -> float:
        return sum(b[1] for b in self.batches)


def arrival_day(day: int, lead: int = 1) -> int:
    return day + max(1, lead)


def demand_shape(skus, rate, deliveries, mode: str = "rhythm"):
    """Mean daily demand per SKU: monthly sales shaped by the delivery rhythm (or flat)."""
    shape = {}
    for n, s in skus.items():
        if mode == "flat":
            months = np.array([(START + timedelta(days=d)).month for d in range(NDAYS)])
            if s.get("sold_per_day_m"):
                shape[n] = np.array([rate[n][int(m)] for m in months], dtype=float)
            else:
                shape[n] = np.full(NDAYS, float(s["sold"]) / NDAYS)
            continue
        dl = deliveries.get(n, {})
        w = np.zeros(NDAYS)
        days = sorted(dl)
        for i, d0 in enumerate(days):
            d1 = days[i + 1] if i + 1 < len(days) else min(NDAYS, d0 + max(1, int(round(np.median(np.diff(days)))) if len(days) > 1 else 1))
            span = max(1, d1 - d0)
            w[d0:d0 + span] += dl[d0] / span
        if w.sum() <= 0:
            w[:] = 1.0
        lam = np.zeros(NDAYS)
        months = np.array([(START + timedelta(days=d)).month for d in range(NDAYS)])
        if s.get("sold_per_day_m"):
            for m in np.unique(months):
                idx = months == m
                total = rate[n][int(m)] * idx.sum()
                ws = w[idx].sum()
                lam[idx] = (w[idx] / ws * total) if ws > 0 else total / idx.sum()
        else:
            lam = w / w.sum() * float(s["sold"])
        shape[n] = lam
    return shape


WORLD_PHI = 0.0          # set from --world-phi; the dispersion of the simulated world


def demand_draws(shape, seed: int, phi: float = None):
    """Daily demand draws. phi=0 is Poisson; phi>0 adds overdispersion so that
    Var = lam + phi^2 lam^2 -- the same law the engine's cv assumes, which is
    what makes an engine-phi and a world-phi comparable. Measured on the
    bakeries' own daily drops (scratchpad bread_phi.py), bread runs phi ~ 0.20
    against the engine's global 0.40."""
    phi = WORLD_PHI if phi is None else float(phi)
    rng = np.random.default_rng(seed)
    out = {}
    for n in sorted(shape):
        lam = np.maximum(shape[n], 0.0)
        if phi <= 0:
            out[n] = rng.poisson(lam).astype(float)
            continue
        r = 1.0 / (phi * phi)                       # NB: mean lam, var lam + lam^2/r
        with np.errstate(divide="ignore", invalid="ignore"):
            pr = np.where(lam > 0, r / (r + lam), 1.0)
        out[n] = np.where(lam > 0, rng.negative_binomial(r, pr), 0).astype(float)
    return out


# ── calibration ───────────────────────────────────────────────────────────
def life_for(fam: str, lives: dict) -> int:
    return LONG_LIFE.get(fam) or lives.get(fam, 3)


def replay_actual(skus, rate, deliveries, draws, lives):
    res = {}
    for n, s in skus.items():
        dl = deliveries.get(n, {})
        if not dl:
            continue
        sh = Shelf(life_for(s["family"], lives), rate[n][4])
        acc = np.zeros(5)                          # demand, sold, expired, delivered, morning stock
        for day in range(NDAYS):
            ex = sh.expire(day)
            q = dl.get(day, 0.0); sh.receive(day, q)
            morning = sh.stock
            want = draws[n][day]; got = sh.sell(want)
            if day >= BURN_IN:
                acc += (want, got, ex, q, morning)
        res[n] = acc
    return res


GRID = {"Loaves": [3, 4, 5, 6, 7], "Buns, rolls, scones": [3, 4, 5, 6, 7], "Cakes": [5, 7, 10, 14, 21, 28]}
UPLIFTS = [1.00, 1.03, 1.06, 1.10, 1.15, 1.20]


def calibrate(skus, rate, deliveries, shape, seeds=(11, 12), fixed_lives=None):
    """Joint fit of (shelf life, demand uplift) per family to observed waste AND sell-through."""
    lives, uplift, fit = {}, {}, {}
    fixed_lives = fixed_lives or {}
    for fam, grid in GRID.items():
        if fam in fixed_lives:
            grid = [int(fixed_lives[fam])]
        fs = {n: s for n, s in skus.items() if s["family"] == fam}
        recv = sum(sum(deliveries[n].values()) for n in fs)
        t_w = sum(s["expired"] for s in fs.values()) / recv
        t_s = sum(s["sold"] for s in fs.values()) / recv
        best = None
        for u in UPLIFTS:
            draws_all = [demand_draws({n: shape[n] * u for n in fs}, sd) for sd in seeds]
            for L in grid:
                w = s_ = 0.0
                for draws in draws_all:
                    r = replay_actual(fs, rate, deliveries, draws, {fam: L})
                    dq = sum(v[3] for v in r.values())
                    w += sum(v[2] for v in r.values()) / dq
                    s_ += sum(v[1] for v in r.values()) / dq
                w /= len(seeds); s_ /= len(seeds)
                err = (w - t_w) ** 2 + (s_ - t_s) ** 2
                if best is None or err < best[0]:
                    best = (err, L, u, w, s_)
        _, L, u, w, s_ = best
        lives[fam], uplift[fam] = L, u
        fit[fam] = {"life_days": L, "demand_uplift": u, "sim_waste": w, "observed_waste": t_w,
                    "sim_sold_of_delivered": s_, "observed_sold_of_delivered": t_s}
    return lives, uplift, fit


# ── the engine arm ────────────────────────────────────────────────────────
def build_engine(skus, supplier_of, lives):
    """Enrich the bread book once through the shipped path; return util + base rows."""
    os.environ["OASIS_ORDER_MODEL"] = "order_up_to"
    from oasis.logic.simulation_bridge import SimulationOrderUtil
    util = SimulationOrderUtil(os.path.join(ROOT, "oasis", "data"))
    book = []
    for n, s in skus.items():
        sup = supplier_of.get(n)
        if not sup:
            continue
        d = float(s["ads"])
        book.append({"sku": n, "item_code": n, "itm_cd": n, "product_name": n,
                     "department": s["dept"], "supplier_name": sup, "avg_daily_sales": d,
                     "current_stock": d, "current_stocks": d, "on_order_qty": 0.0,
                     "unit_cost": s["unit_cost"], "cost_price": s["unit_cost"],
                     "gross_profit_per_unit": s["gp_per_unit"], "selling_price": s["unit_cost"] + s["gp_per_unit"]})
    enriched = util.prepare_sku_data(copy.deepcopy(book))
    base = {}
    for p in enriched:
        d = float(p.get("avg_daily_sales") or 0) or 1e-9
        p["_rop_days"] = float(p.get("reorder_point") or 0) / d
        base[p["sku"]] = p
    return util, base


def make_policy(arm: str):
    parts = set(arm.split("+")) - {"engine"}
    if "all_tunings" in parts:
        parts = (parts - {"all_tunings"}) | ALL_TUNINGS
    unknown = {x for x in parts - TUNINGS - STRUCTURAL if not x.startswith(PHI_ARM)}
    assert not unknown, f"unknown arm parts: {unknown}"
    return parts


def patch_recommend(policy: set, meta: dict):
    """Wrap order_up_to.recommend as the bridge sees it, applying arm policy."""
    from oasis.logic import order_up_to as ou
    from oasis.logic import simulation_bridge as sb
    original = ou.recommend

    def wrapped(product, schedule=None, patterns=None, z=None, mode=None):
        n = product.get("sku")
        m = meta.get(n, {})
        p = dict(product)
        d = float(p.get("avg_daily_sales") or 0)
        _phi = [x for x in policy if x.startswith(PHI_ARM) and x != PHI_ARM]
        if _phi and d > 0:
            f = float(_phi[0][len(PHI_ARM):])
            p["demand_cv_daily"] = math.sqrt(1.0 / d + f * f)
        if "service_bands" in policy and d > 0:
            sl = 0.95 if d >= 10 else 0.90 if d >= 2 else 0.85
            if m.get("brown"):
                sl -= 0.05
            z = ou.z_score(sl)
        terms = original(p, schedule=schedule, patterns=patterns, z=z, mode=mode)
        if "overnight" in policy and m.get("fresh") and d > 0 and "S" in terms:
            sL = 0.0 if "sigmaL0" in policy else terms["sigma_lead"]
            S = ou.order_up_to_level(d, terms["sigma_d"], 0.0, terms["R"], sL, terms["z"])
            life = float(m.get("life", 4))
            S = min(S, d * life) if d * life >= 1.0 else 1.0
            terms = dict(terms)
            terms["S"] = S
            terms["quantity"] = ou.order_quantity(S, terms["on_hand"], terms["on_order"], float(p.get("pack_size") or 1))
        if "velocity_cap" in policy and m.get("fresh") and d > 0 and "S" in terms:
            h = 1.25 if d >= 10 else 1.15 if d >= 2 else 1.0
            cap = d * (1.0 + (terms["P"] - 1.0) * h)
            if terms["S"] > cap:
                terms = dict(terms)
                terms["S"] = cap
                terms["quantity"] = ou.order_quantity(cap, terms["on_hand"], terms["on_order"], float(p.get("pack_size") or 1))
        if "slow_presence" in policy and d > 0 and d * m.get("life", 3) < 1.0 and "S" in terms:
            terms = dict(terms)
            terms["S"] = 1.0
            terms["quantity"] = ou.order_quantity(1.0, terms["on_hand"], terms["on_order"], 1.0)
        return terms

    sb._ou.recommend = wrapped
    return lambda: setattr(sb._ou, "recommend", original)


def run_engine_arm(arm, util, base, skus, rate, draws, lives, supplier_of):
    from oasis.logic import demand_rate as dr
    from oasis.logic.order_up_to import shelf_life_for
    policy = make_policy(arm)
    meta = {}
    for n, p in base.items():
        s = skus[n]
        fam = s["family"]
        meta[n] = {"fresh": bool(p.get("is_fresh")) or ("cakes_fresh" in policy and fam == "Cakes"),
                   "brown": s.get("tone") == "Brown/wholemeal", "life": life_for(fam, lives)}
    restore = patch_recommend(policy, meta)
    shelves = {n: Shelf(life_for(skus[n]["family"], lives), rate[n][4]) for n in base}
    pipeline = defaultdict(lambda: defaultdict(float))     # arrival day -> sku -> qty
    hist = {n: [rate[n][4]] * 90 for n in base}             # pre-period at April's rate
    instock = {n: [True] * 90 for n in base}
    acc = {n: np.zeros(5) for n in base}
    blocked = defaultdict(int)
    reasons = defaultdict(float)
    try:
        for day in range(NDAYS):
            date = START + timedelta(days=day)
            os.environ["OASIS_AS_OF"] = date.strftime("%Y-%m-%d")
            ex = {n: sh.expire(day) for n, sh in shelves.items()}
            arrivals = pipeline.pop(day, {})
            for n, q in arrivals.items():
                shelves[n].receive(day, q)
            # the day's trading, then the end-of-day order
            for n, sh in shelves.items():
                want = draws[n][day]
                morning = sh.stock
                had = morning > 1e-9
                got = sh.sell(want)
                hist[n].append(got); hist[n] = hist[n][-90:]
                instock[n].append(had and got >= want - 1e-9); instock[n] = instock[n][-90:]
                if day >= BURN_IN:
                    acc[n] += (want, got, ex[n], arrivals.get(n, 0.0), morning)
            on_order = defaultdict(float)
            for a_day, lines in pipeline.items():
                for n, q in lines.items():
                    on_order[n] += q
            rows = []
            for n, p0 in base.items():
                h = hist[n]
                if "censor_fix" in policy:
                    ins = instock[n]
                    qs = []
                    for lo, hi in ((-30, None), (-60, -30), (-90, -60)):
                        hs, ks = h[lo:hi], ins[lo:hi]
                        days_in = sum(ks)
                        qs.append(sum(x for x, k in zip(hs, ks) if k) / days_in * 30 if days_in else 0.0)
                    d = dr.weighted_daily_rate(qs[0], qs[1], qs[2], 90)
                else:
                    d = dr.weighted_daily_rate(sum(h[-30:]), sum(h[-60:-30]), sum(h[-90:-60]), 90)
                p = dict(p0)
                st = shelves[n].stock
                p.update({"avg_daily_sales": d, "sales_velocity": d, "current_stock": st, "current_stocks": st,
                          "on_order_qty": on_order[n], "reorder_point": p0["_rop_days"] * d,
                          "days_since_delivery": 1, "last_days_since_last_delivery": 1, "total_units_sold_last_90d": sum(h)})
                if "measured_life" in policy:
                    p["shelf_life_days"] = 0
                if "cakes_fresh" in policy and skus[n]["family"] == "Cakes":
                    p["is_fresh"] = True
                    p["lead_time_days"] = 1
                    p["shelf_life_days"] = shelf_life_for("CAKES", sku=n) or life_for("Cakes", lives)
                lead = 1
                if "weekday" in policy and arrival_day(day, 1) - day > 1:
                    p["lead_time_days"] = arrival_day(day, 1) - day
                rows.append(p)
            recs = util.calculate_order_quantity(rows, use_real_date=True)
            if day >= BURN_IN:
                for r in recs:
                    why = str(r.get("reasoning") or "")
                    q = float(r.get("recommended_quantity") or 0)
                    k = ("order" if q > 0 else "suppressed" if r.get("auto_order_suppressed") else
                         "above reorder point" if "Above ROP" in why else "position covers P" if "covers P" in why else
                         "not an ordering day" if ("Schedule" in why or "Not an ordering" in why) else
                         "no demand" if r.get("ads_missing") else "blocked" if "Blocked" in why else "other: " + why[:40])
                    reasons[k] += 1
            recs = util.finalize_orders(recs)
            if "gate_off" in policy:
                gate = {"po_recs": [r for r in recs if float(r.get("recommended_quantity") or 0) > 0], "transfer_recs": []}
            else:
                gate = util.apply_minimum_order_gate(recs)
            for r in gate["transfer_recs"]:
                blocked[r.get("sku")] += 1
                if day >= BURN_IN:
                    reasons["dropped by minimum-order gate"] += 1
            arr = arrival_day(day, 1)
            for r in gate["po_recs"]:
                q = float(r.get("recommended_quantity") or 0)
                if q > 0:
                    pipeline[arr][r.get("sku")] += q
    finally:
        restore()
    return acc, blocked, reasons


# ── scoring ───────────────────────────────────────────────────────────────
def score(acc, skus, ndays):
    year = 365.0 / ndays
    rows = []
    for n, (dem, sold, exp, dlv, morning) in acc.items():
        s = skus[n]
        rows.append({"name": n, "supplier": s["supplier"], "family": s["family"], "ads": s["ads"],
                     "demand": dem, "sold": sold, "lost": dem - sold, "expired": exp, "delivered": dlv, "morning_stock": morning,
                     "lost_sales_kes": (dem - sold) * s["unit_price"] * year, "expiry_kes": exp * s["unit_cost"] * year,
                     "gp_kes": sold * s["gp_per_unit"] * year})
    return pd.DataFrame(rows)


def summarise(df):
    t = df[["demand", "sold", "lost", "expired", "delivered", "morning_stock", "lost_sales_kes", "expiry_kes", "gp_kes"]].sum()
    return {"fill": t.sold / t.demand, "waste_of_delivered": t.expired / t.delivered if t.delivered else None,
            "morning_cover_days": t.morning_stock / t.demand if t.demand else None,
            "lost_units_day": t.lost / (NDAYS - BURN_IN), "expired_units_day": t.expired / (NDAYS - BURN_IN),
            "delivered_units_day": t.delivered / (NDAYS - BURN_IN),
            "lost_sales_kes_year": t.lost_sales_kes, "expiry_kes_year": t.expiry_kes, "gp_kes_year": t.gp_kes}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(os.path.expanduser("~"), "Downloads"))
    ap.add_argument("--report-json", required=True, help="bread_deep.json from the bread report")
    ap.add_argument("--out", required=True)
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--arms", default=",".join(ARMS))
    ap.add_argument("--shape", default="rhythm", choices=["rhythm", "flat"])
    ap.add_argument("--world-phi", type=float, default=0.0,
                    help="overdispersion of the SIMULATED demand (0 = Poisson; bread measures ~0.20)")
    ap.add_argument("--life", default="", help='fix shelf life per family, e.g. "Loaves=4;Buns, rolls, scones=4"')
    a = ap.parse_args(argv)
    os.makedirs(a.out, exist_ok=True)
    import logging
    logging.disable(logging.WARNING)

    skus, rate, deliveries, supplier_of = load_inputs(a.data, a.report_json)
    skus = {n: s for n, s in skus.items() if n in supplier_of}
    shape = demand_shape(skus, rate, deliveries, a.shape)
    globals()["WORLD_PHI"] = a.world_phi
    fixed = {k.strip(): int(v) for k, v in (x.split("=") for x in a.life.split(";") if "=" in x)}
    lives, uplift, fit = calibrate(skus, rate, deliveries, shape, fixed_lives=fixed)
    shape = {n: v * uplift.get(skus[n]["family"], 1.0) for n, v in shape.items()}
    print("[calibrate] shelf life by family:", json.dumps(fit), flush=True)
    util, base = build_engine(skus, supplier_of, lives)
    print(f"[engine] {len(base)} SKUs enriched through the shipped path", flush=True)

    arms = [x for x in a.arms.split(",") if x]
    per_arm = defaultdict(list)
    per_sku = defaultdict(list)
    per_reason = defaultdict(list)
    for seed in range(a.seeds):
        draws = demand_draws(shape, 1000 + seed)
        for arm in arms:
            t0 = datetime.now()
            if arm == "actual":
                acc = replay_actual(skus, rate, deliveries, draws, lives); blocked = {}; reasons = {}
            else:
                acc, blocked, reasons = run_engine_arm(arm, util, base, skus, rate, draws, lives, supplier_of)
            df = score(acc, skus, NDAYS - BURN_IN)
            summ = summarise(df); summ["moq_gate_drops"] = int(sum(blocked.values()))
            per_reason[arm].append(dict(reasons))
            per_arm[arm].append(summ)
            per_sku[arm].append(df.assign(seed=seed))
            print(f"[seed {seed}] {arm:<14} fill {summ['fill']:.2%} | waste {summ['waste_of_delivered'] or 0:.2%} | "
                  f"lost KES {summ['lost_sales_kes_year']:,.0f}/yr | expiry KES {summ['expiry_kes_year']:,.0f}/yr | "
                  f"delivered {summ['delivered_units_day']:.0f}/day | gate drops {summ['moq_gate_drops']} "
                  f"({(datetime.now()-t0).seconds}s)", flush=True)

    summary = {arm: {k: float(np.mean([r[k] for r in rs if r[k] is not None])) for k in rs[0]} for arm, rs in per_arm.items()}
    breakdown = {}
    for arm, dfs in per_sku.items():
        full = pd.concat(dfs)
        g = full.groupby(["name", "supplier", "family", "ads"], as_index=False)[
            ["demand", "sold", "lost", "expired", "delivered", "morning_stock", "lost_sales_kes", "expiry_kes", "gp_kes"]].mean()
        g.to_csv(os.path.join(a.out, f"sku_{arm}.csv"), index=False)
        g["vel"] = pd.cut(g.ads, [0, 1, 2, 5, 10, 30, 1e9], labels=["under 1", "1–2", "2–5", "5–10", "10–30", "30+"], right=False).astype(str)
        breakdown[arm] = {by: json.loads(g.groupby(by)[["demand", "sold", "lost", "expired", "delivered", "morning_stock", "lost_sales_kes", "expiry_kes", "gp_kes"]].apply(lambda x: pd.Series(summarise(x))).reset_index().to_json(orient="records"))
                          for by in ("supplier", "family", "vel")}
    reasons_mean = {arm: {k: float(np.mean([r.get(k, 0) for r in rs])) for k in set().union(*rs)}
                    for arm, rs in per_reason.items() if rs and rs[0]}
    json.dump({"shape": a.shape, "lives": lives, "uplift": uplift, "calibration": fit, "reasons": reasons_mean, "seeds": a.seeds, "burn_in": BURN_IN, "days_scored": NDAYS - BURN_IN,
               "summary": summary, "breakdown": breakdown}, open(os.path.join(a.out, "bread_backtest.json"), "w", encoding="utf-8"), indent=1)
    print("\nWHY THE ENGINE DID OR DID NOT ORDER (SKU-days, mean over seeds)")
    for arm, rs in reasons_mean.items():
        tot = sum(v for k, v in rs.items() if k != "dropped by minimum-order gate") or 1
        print(f"  {arm:<14} " + " | ".join(f"{k} {100*v/tot:.1f}%" for k, v in sorted(rs.items(), key=lambda x: -x[1])[:7]))
    print("\nMEAN OVER SEEDS")
    print(pd.DataFrame(summary).T[["fill", "waste_of_delivered", "morning_cover_days", "lost_units_day", "expired_units_day", "delivered_units_day",
                                   "lost_sales_kes_year", "expiry_kes_year", "gp_kes_year", "moq_gate_drops"]].round(4).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
