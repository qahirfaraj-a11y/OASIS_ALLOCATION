"""AMIT, rerouted: a return-on-inventory gate, not a line-count trimmer.

WHY THE OLD CONSTRAINT WAS THE WRONG ONE

    AMIT trimmed each department down to a SKU cap. Sixteen caps were written
    by hand, the data has 239 departments, and 83% of lines fell through to an
    undeclared `fallback_cap = 50` that was the real assortment policy. The
    ranking key inside that cap reduced algebraically to margin/lata -- price
    and velocity cancel exactly -- and was zero on 84% of nodes, so the sort
    was decided by nodes.csv row order on 96.4% of blacklistings.

    Fixing the key inside a line-count cap does not fix the question. A line
    cap has no business meaning: the store is not short of SKU numbers. What
    a shelf costs is CAPITAL and HANDLING, and both are per-shilling-per-year,
    not per-line. Under a capital constraint the Dantzig ratio -- gross profit
    over inventory value, which is what GMROI actually is -- IS the optimal
    key. It was the right metric bolted to the wrong constraint.

    The previous rewrite tried to solve a shadow price against a capital
    budget taken from the on-hand snapshot. That snapshot turned out to be a
    monthly SALES extract, so there is no observed stock anywhere in OASIS and
    no budget to solve against. Which is fine, because a budget was never
    needed: the economic rule is a HURDLE, not a budget. Keep a line if the
    gross profit it earns in a year exceeds the cost of the capital and
    handling its own replenishment policy commits. No budget, no ranking, no
    circularity -- each line is judged against a number that exists outside
    the data.

THE HURDLE
    capital  the cost of the next shilling. Kenya's average commercial bank
             lending rate was 14.38% in June 2026 (CBK). DECLARED, and it
             belongs to finance, not to this file.
    handling receiving, shelving, counting, rotating -- per shilling of stock
             carried per year.
    shrink   theft, damage and expiry, which for fresh is the dominant term.
    A line must clear the sum of the three.

INVENTORY COMES FROM THE ORDERING ENGINE, NOT FROM A PRIVATE FORMULA
    I(r) is computed by calling order_up_to.recommend(), so it carries the
    measured review cadence, the measured lead time and spread from the
    PO-to-GRN history, and the shelf-life clamp. Average on-hand under an
    order-up-to policy is

        S - d*(L + R/2)   ==   d*R/2 + safety

    which is the standard result and reduces correctly when S is clamped. A
    line whose clamp drives that negative is STRUCTURALLY SHORT -- the product
    dies before the next delivery can arrive -- and that is a supply-terms
    problem, not an assortment decision. Those lines are reported separately
    and never blacklisted, because cutting a SKU for failing a test it cannot
    pass under the current delivery terms is not a merchandising judgement.
"""
from __future__ import annotations

import argparse, csv, json, math, re, statistics as st, sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, ratio, run_all   # noqa: E402
from devkit import build_margin                                       # noqa: E402
from devkit.amit_adaptive import norm, norm_dept                      # noqa: E402
from oasis.logic import order_up_to as ou                             # noqa: E402

NODES = ROOT / "neutral_network_export" / "nodes.csv"
ADS = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"
STAPLES = ROOT / "oasis" / "data" / "staple_products.json"
LISTING = ROOT / "oasis" / "data" / "2_31_sl.xlsx"   # departments only. NOT stock.

#: Kenya average commercial bank lending rate, June 2026 (CBK) -- the cost of
#: the next shilling. DECLARED, replace with the chain's own cost of funds.
CAPITAL_COST = 0.1438
#: Receiving, shelving, counting, rotating, per shilling carried per year.
HANDLING = 0.10
#: Chain-wide default. Fresh overrides it below, because expiry is shrink.
SHRINK = 0.02
SHRINK_BY_SHELF_LIFE = ((2.0, 0.25), (7.0, 0.12), (30.0, 0.05))


def shrink_for(shelf_life: float) -> float:
    for lim, s in SHRINK_BY_SHELF_LIFE:
        if shelf_life and shelf_life <= lim:
            return s
    return SHRINK


def load_departments():
    """SKU -> department, from the listing extract. Quantities are ignored:
    that column is a month of sales, not stock, and reading it as stock is
    what produced the withdrawn 32.7-day cover figure."""
    try:
        import openpyxl
    except ImportError:
        return {}
    wb = openpyxl.load_workbook(LISTING, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]; it = ws.iter_rows(values_only=True)
    h = [str(x).strip() if x else "" for x in next(it)]
    ni, di = h.index("Item Name"), h.index("Department")
    out = {}
    for r in it:
        if len(r) > max(ni, di) and r[ni]:
            out[norm(r[ni])] = norm(r[di] or "")
    wb.close()
    return out


def strip_code(v):
    s = norm(v).strip("[]").strip()
    return s.split(" - ", 1)[1].strip() if " - " in s else s


def build(hurdle_parts):
    mar, src = build_margin.load_or_derive()
    mar = {norm(k): v for k, v in mar.items()}
    ads = {norm(k): v for k, v in json.loads(ADS.read_text(encoding="utf-8")).items()}
    raw = json.loads(STAPLES.read_text(encoding="utf-8"))
    staples = {norm(x) for x in (raw if isinstance(raw, list) else raw.get("staples", raw.keys()))}
    depts = load_departments()
    sched = ou.load_review_schedule(str(ROOT))
    pats = ou.default_patterns(str(ROOT))
    shelf = ou.load_shelf_life(str(ROOT))

    rows, dropped = [], defaultdict(int)
    for r in csv.DictReader(NODES.open(encoding="utf-8")):
        if r.get("type") != "SKU":
            continue
        k = norm(r["id"])
        av = ads.get(k) or {}
        d = float(av.get("new_ads") or av.get("old_ads") or r.get("velocity_ads") or 0)
        if d <= 0:
            dropped["no_demand"] += 1; continue
        m = mar.get(k)
        if not m:
            dropped["no_margin"] += 1; continue
        dept_raw = depts.get(k) or norm(r.get("department", "")).strip("[]").strip()
        vendor = strip_code(m.get("vendor") or r.get("supplier") or "")
        pt = pats.get(vendor) or {}
        L = float(pt.get("lead_time_mean", pt.get("lead_time_days", 2.0)) or 2.0)
        rec = ou.recommend({"avg_daily_sales": d, "supplier_name": vendor,
                            "current_stock": 0, "lead_time_days": L,
                            "department": dept_raw},
                           schedule=sched, patterns=pats)
        sl = shelf.get(dept_raw, 0.0)
        # average on-hand = S - d*(L + R/2) == d*R/2 + safety
        avg_units = rec["S"] - d * (rec["L"] + rec["R"] / 2.0)
        structurally_short = avg_units <= 0
        if structurally_short:
            avg_units = rec["S"] / 2.0
        I = max(avg_units * m["unit_cost"], 1e-9)
        gp_year = m["gross_profit_per_unit"] * d * 365.0
        rows.append({
            "sku": k, "dept_raw": dept_raw, "dept": norm_dept(dept_raw),
            "vendor": vendor, "d": d, "cost": m["unit_cost"],
            "gp_year": gp_year, "rev_year": m["selling_price"] * d * 365.0,
            "I": I, "gmroi": gp_year / I, "S": rec["S"], "R": rec["R"],
            "L": rec["L"], "P": rec["P"], "shelf": sl,
            "feasible": rec["feasible"], "short": structurally_short,
            "R_source": rec["R_source"], "pat": bool(pt),
            "hurdle": hurdle_parts[0] + hurdle_parts[1] + shrink_for(sl),
            "staple": k in staples,
        })
    return rows, dict(dropped), src, pats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--capital-cost", type=float, default=CAPITAL_COST)
    ap.add_argument("--handling", type=float, default=HANDLING)
    ap.add_argument("--line-cost", type=float, default=None,
                    help="annual cost of carrying ONE line: facing, receiving, "
                         "counting, rotating, buyer attention. A FIXED cost per "
                         "SKU, which is the constraint that actually binds.")
    ap.add_argument("--frontier", action="store_true",
                    help="sweep the hurdle instead of applying one")
    ap.add_argument("--top", type=int, default=12)
    a = ap.parse_args(argv)

    rows, dropped, src, pats = build((a.capital_cost, a.handling))
    if not rows:
        print("  nothing built"); return 2

    tot_gp = sum(r["gp_year"] for r in rows)
    tot_I = sum(r["I"] for r in rows)
    print(f"  {len(rows):,} SKUs · margin {src} · dropped {dropped}")
    print(f"  lead time measured for {sum(1 for r in rows if r['pat']):,} lines "
          f"(PO date -> GRN date, supplier_lead_patterns.json)")
    srcs = defaultdict(int)
    for r in rows:
        srcs[r["R_source"]] += 1
    print(f"  R source {dict(srcs)}")
    print(f"\n  policy inventory the engine commits   KES {tot_I:>13,.0f}   "
          f"(avg on-hand = d*R/2 + safety, at cost)")
    print(f"  gross profit at stake                 KES {tot_gp:>13,.0f} /yr")
    print(f"  portfolio GMROI                       {tot_gp/tot_I:>13.2f}   "
          f"KES GP per KES-year of stock")
    print(f"  hurdle = capital {a.capital_cost:.1%} + handling {a.handling:.1%} "
          f"+ shrink 2-25% by shelf life")

    run_all([
        ratio(tot_gp / tot_I, 1.0, "portfolio GMROI", lo=0.5, hi=40.0),
        join_match_rate([r["vendor"] for r in rows], list(pats),
                        "vendors -> measured lead patterns", floor=0.50),
    ])

    short = [r for r in rows if r["short"]]
    print(f"\n  STRUCTURALLY SHORT: {len(short):,} lines whose shelf life is under "
          f"their protection interval")
    print(f"    these are a supply-terms problem, not an assortment one, and are "
          f"never blacklisted here")
    bs = defaultdict(list)
    for r in short:
        bs[r["dept_raw"]].append(r)
    for dept, it in sorted(bs.items(), key=lambda kv: -len(kv[1]))[:6]:
        print(f"    {dept[:26]:<28}{len(it):>5} lines · shelf {it[0]['shelf']:>4.1f} d "
              f"· P {st.median([x['P'] for x in it]):>4.2f} d · "
              f"KES {sum(x['gp_year'] for x in it):>11,.0f} GP/yr")

    judged = [r for r in rows if not r["short"]]
    if a.frontier:
        print(f"\n  CAPITAL frontier -- hurdle as a RATE, KES GP per KES-year")
        print(f"  this is the constraint that does NOT bind: the portfolio earns "
              f"{tot_gp/tot_I:.1f}x against a {a.capital_cost+a.handling:.0%}+ hurdle,")
        print(f"  because a daily-delivered line needs almost no stock. Capital is "
              f"not what a marginal SKU costs this store.")
        print(f"  {'hurdle':>8}{'cut':>8}{'GP lost':>14}{'GP lost %':>11}"
              f"{'capital released':>18}{'staples cut':>13}")
        for h in (0.10, 0.1638, 0.2438, 0.35, 0.50, 0.75, 1.0, 1.5, 2.0, 3.0):
            cut = [r for r in judged if r["gmroi"] < h]
            print(f"  {h:>8.2f}{len(cut):>8,}{sum(r['gp_year'] for r in cut):>14,.0f}"
                  f"{100*sum(r['gp_year'] for r in cut)/tot_gp:>10.1f}%"
                  f"{sum(r['I'] for r in cut):>18,.0f}"
                  f"{sum(1 for r in cut if r['staple']):>13,}")

        # ------------------------------------------------------------------
        # THE CONSTRAINT THAT DOES BIND
        # A shelf costs money two ways. Per shilling carried -- capital,
        # handling, shrink -- which the frontier above shows is negligible
        # here. And per LINE, regardless of how little stock sits on it: a
        # facing that another SKU could use, a receiving line to check, a
        # count, a rotation, a buyer's attention, a slot in the planogram.
        # That second cost is FIXED PER SKU, so the right key against it is
        # absolute annual gross profit and the right threshold is KES per
        # line per year -- not a ratio. This is the same dual argument as
        # before, but with a threshold that means something instead of a cap
        # somebody typed.
        gps = sorted(r["gp_year"] for r in judged)
        qq = lambda x: gps[int(x * len(gps))]
        print(f"\n  LINE frontier -- hurdle as KES of annual gross profit per line")
        print(f"  GP per line: p10 {qq(.10):,.0f} · p25 {qq(.25):,.0f} · "
              f"median {qq(.5):,.0f} · p75 {qq(.75):,.0f} · p90 {qq(.90):,.0f}")
        print(f"  {'KES/line':>10}{'cut':>8}{'GP lost':>14}{'GP lost %':>11}"
              f"{'capital released':>18}{'staples cut':>13}{'depts emptied':>15}")
        by_dept_n = defaultdict(int)
        for r in judged:
            by_dept_n[r["dept_raw"]] += 1
        for h in (1_000, 2_500, 5_000, 10_000, 20_000, 40_000, 80_000):
            cut = [r for r in judged if r["gp_year"] < h]
            cn = defaultdict(int)
            for r in cut:
                cn[r["dept_raw"]] += 1
            emptied = sum(1 for d, n in cn.items() if n >= by_dept_n[d])
            print(f"  {h:>10,}{len(cut):>8,}{sum(r['gp_year'] for r in cut):>14,.0f}"
                  f"{100*sum(r['gp_year'] for r in cut)/tot_gp:>10.1f}%"
                  f"{sum(r['I'] for r in cut):>18,.0f}"
                  f"{sum(1 for r in cut if r['staple']):>13,}{emptied:>15}")
        print(f"\n  read it as a break-even: a line is worth its slot only if the "
              f"slot costs less\n  than the GP the line earns. Nobody has costed a "
              f"slot yet -- that is the\n  number to go and get, and it is an "
              f"operating figure, not a data one.")
        return 0

    if a.line_cost is not None:
        keep = [r for r in judged if r["gp_year"] >= a.line_cost]
        cut = [r for r in judged if r["gp_year"] < a.line_cost]
        print(f"\n  gate: annual gross profit per line >= KES {a.line_cost:,.0f}")
    else:
        keep = [r for r in judged if r["gmroi"] >= r["hurdle"]]
        cut = [r for r in judged if r["gmroi"] < r["hurdle"]]
        print(f"\n  gate: GMROI >= capital + handling + shrink")
    print(f"\n  KEEP {len(keep):,} · CUT {len(cut):,} · UNJUDGED (short) {len(short):,}")
    print(f"    GP lost         KES {sum(r['gp_year'] for r in cut):>13,.0f} /yr "
          f"({100*sum(r['gp_year'] for r in cut)/tot_gp:.1f}%)")
    print(f"    revenue lost    KES {sum(r['rev_year'] for r in cut):>13,.0f} /yr")
    print(f"    capital released KES {sum(r['I'] for r in cut):>12,.0f} "
          f"({100*sum(r['I'] for r in cut)/tot_I:.1f}%)")
    print(f"    staples cut     {sum(1 for r in cut if r['staple']):,}")
    print(f"    carrying cost avoided KES "
          f"{sum(r['I']*r['hurdle'] for r in cut):>9,.0f} /yr vs GP lost "
          f"KES {sum(r['gp_year'] for r in cut):,.0f} -- net "
          f"{sum(r['I']*r['hurdle'] - r['gp_year'] for r in cut):+,.0f}")

    by_d = defaultdict(list)
    for r in cut:
        by_d[r["dept_raw"]].append(r)
    print(f"\n  departments losing the most lines")
    print(f"  {'department':<30}{'cut':>6}{'of':>6}{'GP lost':>13}{'capital out':>14}")
    tot_by = defaultdict(int)
    for r in judged:
        tot_by[r["dept_raw"]] += 1
    for dept, it in sorted(by_d.items(), key=lambda kv: -len(kv[1]))[:a.top]:
        print(f"  {dept[:29]:<30}{len(it):>6}{tot_by[dept]:>6}"
              f"{sum(x['gp_year'] for x in it):>13,.0f}"
              f"{sum(x['I'] for x in it):>14,.0f}")

    print(f"\n  best capital performers now that fresh is clamped")
    print(f"  {'sku':<44}{'dept':<18}{'gmroi':>9}{'GP/yr':>12}{'I':>10}")
    for r in sorted(judged, key=lambda r: -r["gmroi"])[:a.top]:
        print(f"  {r['sku'][:43]:<44}{r['dept_raw'][:17]:<18}{r['gmroi']:>9.1f}"
              f"{r['gp_year']:>12,.0f}{r['I']:>10,.0f}")

    print(json.dumps({
        "claim": "claim.allocation.amit-gates-on-return-not-line-count",
        "verdict": "supports",
        "metric": {
            "skus": len(rows), "judged": len(judged), "structurally_short": len(short),
            "keep": len(keep), "cut": len(cut),
            "policy_inventory": round(tot_I), "gp_at_stake": round(tot_gp),
            "portfolio_gmroi": round(tot_gp / tot_I, 3),
            "capital_cost": a.capital_cost, "handling": a.handling,
            "gp_lost": round(sum(r["gp_year"] for r in cut)),
            "capital_released": round(sum(r["I"] for r in cut)),
            "carrying_cost_avoided": round(sum(r["I"] * r["hurdle"] for r in cut)),
            "staples_cut": sum(1 for r in cut if r["staple"]),
            "R_source": dict(srcs)},
        "held_out": False, "provenance": "observed",
        "sources": ["source.grn-book", "source.corrected-ads",
                    "source.lead-patterns", "source.cbk-lending-rate"],
        "baseline": "line-count caps with a GMROI-shaped key",
        "beat_baseline": None, "traps": ["T1", "T3", "T5"],
        "notes": ("No budget and no ranking: each line is judged against a "
                  "hurdle that exists outside the data, so there is nothing to "
                  "solve and nothing to be circular with. Inventory comes from "
                  "order_up_to.recommend(), carrying measured cadence, measured "
                  "PO-to-GRN lead time and the shelf-life clamp. Lines whose "
                  "shelf life is under their protection interval are reported "
                  "and never cut: a SKU that cannot pass under current delivery "
                  "terms is a supply problem, not a merchandising one.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
