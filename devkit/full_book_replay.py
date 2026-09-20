"""Full-book outcome replay: does order-up-to's extra stock pay for itself?

DEV TOOLING - devkit/, never ships. PROBE: probe.full-book-outcome-replay,
testing claim.ordering.statistical-target-beats-the-heuristic on OUTCOMES.

THE QUESTION
    probe.rop-variants found the order-up-to model (the configured default)
    carries 55% more cover than the classic heuristic for 3.5 points more
    reach, scored at an empty shelf with no demand variance. The extra cover is
    safety stock, and safety stock only pays when demand and lead time vary.
    This asks the question that settles it: replayed day by day over the whole
    book, under variance, what does each model EARN - gross profit on what
    sold, minus what expired, minus the cost of carrying the stock?

WHAT IS REAL (the decisions)
    Every order in both arms is made by the SHIPPED four-stage path -
    prepare_sku_data -> calculate_order_quantity -> finalize_orders ->
    apply_minimum_order_gate - through devkit/retail_sandbox/engine_bridge.py,
    called once per simulated day at close on the real closing position, with
    the supplier calendar deciding which suppliers take orders that day. The
    arms differ ONLY in OASIS_ORDER_MODEL (classic | order_up_to).

WHAT IS MEASURED (the inputs)
    d            corrected_ads_from_pos.json, per line
    cost, GP     the VAT-corrected GRN book (devkit/build_margin)
    L, sigma_L   PO date to GRN date per supplier (LATA, 107k receipts)
    order days   the supplier calendar and weekly schedule

WHAT IS ASSUMED - and reported as such
    demand law   Poisson arrivals x a lognormal basket multiplier of spread
                 phi, the law the engine itself plans with. phi is NOT
                 observed for the book (no per-line daily sales exist in the
                 repo: rhapta_pos.db holds 3 billing dates). It is swept:
                 0.2 (measured on the bread shelf), 0.4 (the chain setting),
                 0.8 (a volatile book). A verdict that flips across phi rests
                 on phi, and says so.
    shelf life   the WORLD's physical lives (WORLD_LIFE, by department), used
                 by neither arm - never the engine's own, which would be
                 circular; --world engine runs that variant for comparison
    holding      a share of average stock at cost per year: 25% central,
                 15% / 35% band
    opening      d x 7 on every line in both arms; the first 28 days are
                 discarded as warm-up

COMMON RANDOM NUMBERS
    Both arms see the identical demand path and the identical lead-time draw
    for an order placed on a given day, so a difference between them is the
    ordering decision and nothing else. Seeds are paired; differences are
    reported with a paired t.

EMITS one JSON verdict line (probe harness contract).

Usage:
    python devkit/full_book_replay.py                      # full book, 3 phi x 3 seeds
    python devkit/full_book_replay.py --lines 2000 --seeds 1 --days 42 --phi 0.4   # smoke
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "devkit" / "retail_sandbox"))

from devkit import engine_ablation as EA                    # noqa: E402

#: THE WORLD'S SHELF LIVES - physical days a unit stays sellable, ASSERTED and
#: used by NEITHER arm. It cannot be the engine's own table: order-up-to plans
#: with those lives (the clamp), so a world built from them rewards it by
#: construction (trap T5, circularity). And the engine's per-line lives are not
#: physical: they are write-off timings from the returns book, which gives
#: salted butter 3 days, 18.9L bottled water 7 and basmati rice 8.
#:
#: THE OPERATOR'S RULE (stated 2026-09-20): only FRESH and FROZEN produce carry
#: a life worth modelling. Everything else in this book is 90+ days, which is
#: negligible in an FMCG shop - stock turns long before a code date matters -
#: so an unlisted department simply does not expire inside the replay window.
#: The store is building a real expiry book; when it exists, pass it with
#: --life-file (JSON: {"DEPARTMENT": days}) and it replaces this table.
#: Matched on the department name, first hit wins. Bread is the store-checked
#: label (5 selling days), cakes the calibrated 21 from the bread backtest.
WORLD_LIFE = (
    ("FRESH MILK", 7), ("UHT", 180), ("BREAD", 5), ("CAKES", 21), ("BAKERY", 3),
    ("YOGHURT", 21), ("CREAM", 14), ("ICE", 180), ("FROZEN", 180), ("CHEESE", 45),
    ("BUTTER", 60), ("MARGARINE", 90), ("EGGS", 21), ("GOURMET", 7), ("DELI", 7),
    ("SALAD", 3), ("FRUIT", 5), ("VEGETABLE", 5), ("FRESH JUICE", 7), ("SAUSAGE", 14),
    ("POULTRY", 5), ("MEAT", 5), ("BUTCHERY", 5), ("FISH", 3), ("SEAFOOD", 3),
)


def world_life(dept: str, table=None) -> float:
    d = " ".join(str(dept or "").upper().split())
    for key, days in (table or WORLD_LIFE):
        if key in d:
            return float(days)
    return 0.0          # not fresh, not frozen: no expiry inside the window


def load_life_table(path=None):
    """The expiry book, when the store has one; else the asserted table."""
    if not path:
        return WORLD_LIFE
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return tuple((" ".join(str(k).upper().split()), float(v)) for k, v in data.items())


START_YDAY = 61             # 2 March: an ordinary trading stretch
WARM = 28
HOLDING = {"low": 0.15, "central": 0.25, "high": 0.35}
MAX_AGE = 400               # stock older than this is pooled (no expiry beyond)


def quiet():
    return contextlib.redirect_stdout(io.StringIO())


def seed_data_dir():
    """A throwaway copy of oasis/data (+ calendar files) so nothing shared is written."""
    tmp = tempfile.mkdtemp(prefix="oasis_replay_")
    data = os.path.join(tmp, "parent", "data")
    os.makedirs(data)
    src = ROOT / "oasis" / "data"
    for n in os.listdir(src):
        p = src / n
        if p.is_file() and p.stat().st_size < 20_000_000:
            shutil.copy2(p, os.path.join(data, n))
    for n in ("supplier_rhythm_analysis.json", "supplier_weekly_schedule.json",
              "Supplier_Order_Calendar_2026.xlsx"):
        if (ROOT / n).exists():
            shutil.copy2(ROOT / n, os.path.join(tmp, "parent", n))
    return tmp, data


class Book:
    """The measured universe, as arrays, plus the record template per line."""

    def __init__(self, n_lines=None, sample_seed=7, world="physical", life_table=None):
        with quiet():
            keys, f = EA.build()
        idx = np.arange(len(keys))
        if n_lines and n_lines < len(keys):
            idx = np.sort(np.random.default_rng(sample_seed).choice(len(keys), n_lines, replace=False))
        self.keys = [keys[i] for i in idx]
        self.d = f["d"][idx]
        self.cost = f["cost"][idx]
        self.gp = f["gp"][idx]
        self.L = np.maximum(0.5, f["L"][idx])
        self.sL = f["sL_on"][idx]
        # the world's lives: physical (default; see WORLD_LIFE) or, for
        # comparison only, the engine's own belief (circular by construction)
        from oasis.logic import order_up_to as ou
        self.engine_life = np.array([ou.shelf_life_for(str(f["dept_raw"][i]), sku=keys[i]) or 0.0 for i in idx])
        if world == "engine":
            self.shelf = np.where(self.engine_life > 0, self.engine_life, f["shelf"][idx])
        else:
            self.shelf = np.array([world_life(f["dept_raw"][i], life_table) for i in idx])
        self.world = world
        self.vendor = f["vendor_raw"][idx]
        self.dept = f["dept_raw"][idx]
        self.n = len(self.keys)
        self.price = self.cost + self.gp
        shelf = np.where(self.shelf > 0, np.minimum(self.shelf, MAX_AGE), MAX_AGE + 1)
        self.K = int(min(MAX_AGE, max(1, np.nanmax(np.where(self.shelf > 0, self.shelf, 1)))) + 1)
        ages = np.arange(self.K)[None, :]
        # units of this age are dead: age >= shelf life (never for lines with no life)
        self.dead_mask = ages >= shelf[:, None]
        self.template = [{
            "product_name": self.keys[i],
            "supplier_name": str(self.vendor[i]),
            "department": str(self.dept[i]), "product_category": str(self.dept[i]),
            "avg_daily_sales": float(self.d[i]), "avg_daily_sales_last_30d": float(self.d[i]),
            "total_units_sold_last_90d": round(float(self.d[i]) * 90),
            "estimated_delivery_days": max(1, int(round(float(self.L[i])))),
            "lead_time_days": float(self.L[i]),
            "selling_price": float(self.price[i]), "cost_price": float(self.cost[i]),
            "margin_pct": (100.0 * float(self.gp[i]) / float(self.price[i])) if self.price[i] else 0.0,
            "pack_size": 1,
        } for i in range(self.n)]


def demand_paths(book, phi, seed, days):
    rng = np.random.default_rng(10_000 + seed)
    arrivals = rng.poisson(book.d[None, :], size=(days, book.n))
    mult = np.exp(rng.normal(-0.5 * phi * phi, phi, size=(days, book.n)))
    dem = arrivals * mult
    # the lead time an order placed on day t would take (>= 1: ordered after close)
    lead = np.clip(np.round(rng.normal(book.L[None, :], book.sL[None, :], size=(days, book.n))),
                   1, 60).astype(int)
    return dem, lead


def run_arm(bridge, book, model, dem, lead, days):
    """One arm, one seed. Returns totals over the measured days and per-line sold."""
    n, K = book.n, book.K
    stock = np.zeros((n, K))
    stock[:, 0] = book.d * 7.0
    pipe = np.zeros((days + 62, n))
    since = np.zeros(n)                       # days since last delivery
    acc = dict(demand=0.0, sold=0.0, lost_gp=0.0, gp=0.0, expired_units=0.0,
               expired_cost=0.0, stock_cost_days=0.0, orders=0, ordered_units=0.0, obs=0)
    sold_line = np.zeros(n)
    dem_line = np.zeros(n)
    exp_line = np.zeros(n)
    stock_line = np.zeros(n)
    os.environ["OASIS_ORDER_MODEL"] = model
    for t in range(days):
        # morning: deliveries land, fresh
        arr = pipe[t]
        got = arr > 0
        stock[:, 0] += arr
        since = np.where(got, 0.0, since + 1.0)
        # the day's trade, oldest first
        want = dem[t].copy()
        sold = np.zeros(n)
        for age in range(K - 1, -1, -1):
            take = np.minimum(stock[:, age], want)
            stock[:, age] -= take
            want -= take
            sold += take
        # end of day: stock at the end of its life is written off, the rest ages
        expired = (stock * book.dead_mask).sum(axis=1)
        stock[book.dead_mask] = 0.0
        last = stock[:, K - 1].copy()
        stock[:, 1:] = stock[:, :-1]
        stock[:, 0] = 0.0
        stock[:, K - 1] += last                 # the oldest bucket pools
        on_hand = stock.sum(axis=1)
        on_order = pipe[t + 1:].sum(axis=0)

        if t >= WARM:
            acc["demand"] += float(dem[t].sum()); acc["sold"] += float(sold.sum())
            acc["gp"] += float((sold * book.gp).sum())
            acc["lost_gp"] += float(((dem[t] - sold) * book.gp).sum())
            acc["expired_units"] += float(expired.sum())
            acc["expired_cost"] += float((expired * book.cost).sum())
            acc["stock_cost_days"] += float((on_hand * book.cost).sum())
            acc["obs"] += 1
            sold_line += sold; dem_line += dem[t]; exp_line += expired; stock_line += on_hand * book.cost

        # close: the shipped four-stage decision on the real closing position
        skus = []
        for i, tpl in enumerate(book.template):
            r = dict(tpl)
            r["current_stocks"] = float(on_hand[i])
            r["on_order_qty"] = float(on_order[i])
            r["last_days_since_last_delivery"] = int(since[i])
            skus.append(r)
        os.environ["OASIS_AS_OF"] = f"2026-{1 + (START_YDAY + t - 1) // 31:02d}-01"
        with quiet():
            res = bridge.op_order({"skus": skus, "current_day": START_YDAY + t})
        q = np.array([float(x.get("ordered_quantity") or 0.0) for x in res["results"]])
        q = np.maximum(q, 0.0)
        idx = np.nonzero(q > 0)[0]
        if idx.size:
            np.add.at(pipe, (t + lead[t, idx], idx), q[idx])
            if t >= WARM:
                acc["orders"] += int(idx.size); acc["ordered_units"] += float(q[idx].sum())
    return acc, dict(sold=sold_line, demand=dem_line, expired=exp_line, stock=stock_line)


def summarise(acc):
    obs = max(acc["obs"], 1)
    per_year = 365.0 / obs
    avg_stock = acc["stock_cost_days"] / obs
    out = {
        "fill": acc["sold"] / max(acc["demand"], 1e-9),
        "gp_year": acc["gp"] * per_year,
        "lost_gp_year": acc["lost_gp"] * per_year,
        "expiry_cost_year": acc["expired_cost"] * per_year,
        "avg_stock_cost": avg_stock,
        "orders_per_day": acc["orders"] / obs,
    }
    for band, rate in HOLDING.items():
        out[f"net_year_{band}"] = out["gp_year"] - out["expiry_cost_year"] - rate * avg_stock
    out["gmroi"] = out["gp_year"] / max(avg_stock, 1e-9)
    return out


def paired_t(xs):
    xs = np.asarray(xs, float)
    if xs.size < 2 or xs.std(ddof=1) == 0:
        return float("nan")
    return float(xs.mean() / (xs.std(ddof=1) / math.sqrt(xs.size)))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", type=int, default=0, help="0 = the whole book")
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--days", type=int, default=112, help="including the 28-day warm-up")
    ap.add_argument("--phi", default="0.2,0.4,0.8")
    ap.add_argument("--life-file", default=None,
                    help="the store's expiry book as JSON {DEPARTMENT: days}; replaces WORLD_LIFE")
    ap.add_argument("--world", default="physical", choices=("physical", "engine"),
                    help="the world's shelf lives: physical (independent) or the engine's own (circular)")
    ap.add_argument("--out", default=str(ROOT / "devkit" / "full_book_replay_result.json"))
    a = ap.parse_args(argv)
    phis = [float(x) for x in a.phi.split(",")]

    t0 = time.time()
    book = Book(a.lines or None, world=a.world, life_table=load_life_table(a.life_file))
    tmp, data = seed_data_dir()
    import engine_bridge as EB
    with quiet():
        bridge = EB.Bridge(data)
    print(f"  {book.n:,} lines · {a.seeds} seeds · {a.days} days ({WARM} warm-up) · phi {phis}")
    print(f"  world '{book.world}': {int((book.shelf > 0).sum()):,} lines can expire; the engine plans "
          f"with a life on {int((book.engine_life > 0).sum()):,}")

    # WHERE THE ENGINE BELIEVES A LIFE THE WORLD DOES NOT HAVE.
    # shelf_life_for falls back to write-off timing when a department asserts
    # nothing, so ambient lines inherit a "life" from how long a write-off took
    # to be processed - and order-up-to's clamp then caps their cover at that.
    tight = (book.engine_life > 0) & (book.shelf <= 0)
    gp_day = book.d * book.gp
    print(f"  the engine plans a shelf life on {int(tight.sum()):,} lines the world says cannot "
          f"expire ({100 * gp_day[tight].sum() / max(gp_day.sum(), 1e-9):.1f}% of daily GP; "
          f"median life {np.median(book.engine_life[tight]) if tight.any() else 0:.0f}d)")

    results, diffs = {}, {}
    try:
        for phi in phis:
            per_arm = {"classic": [], "order_up_to": []}
            lines_acc = {"classic": None, "order_up_to": None}
            for s in range(a.seeds):
                dem, lead = demand_paths(book, phi, s, a.days)
                for model in ("classic", "order_up_to"):
                    acc, per_line = run_arm(bridge, book, model, dem, lead, a.days)
                    per_arm[model].append(summarise(acc))
                    if lines_acc[model] is None:
                        lines_acc[model] = {k: v.copy() for k, v in per_line.items()}
                    else:
                        for k in per_line:
                            lines_acc[model][k] += per_line[k]
                    m = per_arm[model][-1]
                    print(f"  phi {phi} seed {s} {model:<12} fill {m['fill']:.2%} · GP/yr {m['gp_year']:,.0f} · "
                          f"expiry/yr {m['expiry_cost_year']:,.0f} · avg stock {m['avg_stock_cost']:,.0f} · "
                          f"net(25%) {m['net_year_central']:,.0f}  [{time.time() - t0:.0f}s]", flush=True)
            mean = {mdl: {k: float(np.mean([r[k] for r in rs])) for k in rs[0]} for mdl, rs in per_arm.items()}
            d_net = [u["net_year_central"] - c["net_year_central"]
                     for u, c in zip(per_arm["order_up_to"], per_arm["classic"])]
            d_fill = [u["fill"] - c["fill"] for u, c in zip(per_arm["order_up_to"], per_arm["classic"])]
            # where the difference comes from: fresh (a shelf life) vs ambient
            fresh = book.shelf > 0
            split = {}
            for mdl in ("classic", "order_up_to"):
                la = lines_acc[mdl]
                for name, mask in (("fresh", fresh), ("ambient", ~fresh)):
                    split.setdefault(name, {})[mdl] = {
                        "fill": float(la["sold"][mask].sum() / max(la["demand"][mask].sum(), 1e-9)),
                        "expired_cost": float((la["expired"][mask] * book.cost[mask]).sum() / a.seeds),
                    }
            results[str(phi)] = {"classic": mean["classic"], "order_up_to": mean["order_up_to"],
                                 "split": split}
            diffs[str(phi)] = {
                "net_year_central": float(np.mean(d_net)), "t_net": paired_t(d_net),
                "fill_pp": 100 * float(np.mean(d_fill)), "t_fill": paired_t(d_fill),
                "stock_pct": 100 * (mean["order_up_to"]["avg_stock_cost"] / max(mean["classic"]["avg_stock_cost"], 1e-9) - 1),
                "net_low": mean["order_up_to"]["net_year_low"] - mean["classic"]["net_year_low"],
                "net_high": mean["order_up_to"]["net_year_high"] - mean["classic"]["net_year_high"],
            }
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print("\n  ORDER-UP-TO minus CLASSIC (per year, paired over seeds)")
    print(f"  {'phi':>5}{'fill pp':>10}{'stock':>9}{'net @15%':>14}{'net @25%':>14}{'t':>7}{'net @35%':>14}")
    for phi, dd in diffs.items():
        print(f"  {phi:>5}{dd['fill_pp']:>+10.2f}{dd['stock_pct']:>+8.1f}%{dd['net_low']:>+14,.0f}"
              f"{dd['net_year_central']:>+14,.0f}{dd['t_net']:>7.1f}{dd['net_high']:>+14,.0f}")

    # the verdict: does order-up-to EARN more? only where it wins net at the
    # central holding rate in every phi regime (paired t >= 2) is it "better"
    wins = [p for p, dd in diffs.items() if dd["net_year_central"] > 0 and (a.seeds < 2 or dd["t_net"] >= 2)]
    losses = [p for p, dd in diffs.items() if dd["net_year_central"] < 0 and (a.seeds < 2 or dd["t_net"] <= -2)]
    verdict = ("supports" if len(wins) == len(diffs) else
               "contradicts" if len(losses) == len(diffs) else "inconclusive")

    from devkit.methodology.traps import regime, run_all, like_for_like
    traps = run_all([
        regime("net contribution under replenishment", "replenishment", "replenishment"),
        like_for_like(("classic line-days", float(book.n * (a.days - WARM))),
                      ("order-up-to line-days", float(book.n * (a.days - WARM))),
                      "both arms scored on the same lines, days and demand paths"),
    ], strict=True)
    out = {"lines": book.n, "world": book.world, "seeds": a.seeds,
           "engine_life_on_non_expiring_lines": {
               "lines": int(tight.sum()),
               "gp_share_pct": float(100 * gp_day[tight].sum() / max(gp_day.sum(), 1e-9)),
               "median_days": float(np.median(book.engine_life[tight])) if tight.any() else 0.0}, "days": a.days, "warmup": WARM, "phi": phis,
           "holding": HOLDING, "results": results, "diffs": diffs}
    Path(a.out).write_text(json.dumps(out, indent=1), encoding="utf-8")
    print(json.dumps({
        "claim": "claim.ordering.statistical-target-beats-the-heuristic",
        "verdict": verdict,
        "metric": {"diffs_order_up_to_minus_classic": diffs, "wins_in_phi": wins, "losses_in_phi": losses,
                   "lines": book.n, "seeds": a.seeds, "measured_days": a.days - WARM,
                   "world_shelf_life": book.world},
        "baseline": "classic heuristic, same shipped four-stage path (OASIS_ORDER_MODEL=classic)",
        "beat_baseline": verdict == "supports",
        "held_out": False,
        "provenance": "simulated demand calibrated to observed ADS; decisions from the shipped engine",
        "traps": sorted({r.trap for r in traps}),
        "notes": ("Net = realised GP - expiry at cost - holding (25%/yr of average stock at cost). "
                  "Demand variability phi is assumed and swept; shelf life department-asserted. "
                  "held_out is false: demand is simulated, not replayed from observed daily sales, "
                  "which the repo does not hold."),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
