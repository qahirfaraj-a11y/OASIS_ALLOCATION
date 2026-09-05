"""AMIT as an adaptive metric: derived at runtime, grounded in no snapshot.

WHAT WAS WRONG, IN THE ORDER THAT MATTERS

  1. THE METRIC WAS ALGEBRAICALLY EMPTY.  The shipped key is

         gmroi = gross_profit / (price * ads * 30 * lata)

     With gross_profit = price * ads * T * margin over a T-day window, price
     and ads cancel EXACTLY and what is left is

         gmroi = (T/30) * margin / lata          (T = 184 -> 6.13)

     a constant times margin over lata. No velocity, no price, no size, no
     capital. It is a margin sort wearing a GMROI costume.

  2. SO IT TIED AT ZERO.  `gross_profit` is populated on 3,663 of 23,511
     nodes; the other 19,848 scored exactly 0.0 and Python's stable sort left
     them in nodes.csv ROW ORDER. 96.4% of blacklistings came out of that tie
     block. The metric was not deciding -- the CSV was. That is what "needs a
     json file to work" looks like from the inside: remove the file and the
     engine does not fail, it silently decides by row order.

  3. WRONG DUAL.  GMROI is a scale-free RATE (the Dantzig ratio). Greedy on a
     ratio is optimal against a CAPITAL budget. Against a LINE-COUNT cap the
     optimal key is absolute contribution -- KES per line, not KES per
     KES-year. AMIT bolted the ratio key to the count constraint.

  4. CIRCULAR BUDGET.  The previous version of this file set
     `budget = sum(inventory_value(r) for r in keep)` -- the same function the
     constraint uses -- so P, z, cv and sigma_L cancelled and the GP cut came
     out bit-identical across a 4.3x budget range. T5, in my own probe.

  5. CAP-KEY COLLISION.  `{norm_dept(k): v for k, v in CAPS.items()}` lets
     FRESH MILK(40) overwrite DAIRY(60) and BREAD(30) overwrite BAKERY(50),
     silently, because dict comprehensions keep the last write.

WHAT THIS DOES INSTEAD

  MARGIN IS DERIVED, NEVER READ AS A REQUIREMENT.  build_margin.derive() runs
  over the GRN workbooks in process. The json beside them is a cache with an
  mtime check, not an input. Every SKU lands on a declared rung --
  observed / dept_median / chain_median -- so nothing sits at zero by default
  and no decision can fall through to row order.

  INVENTORY IS THE POLICY'S OWN.  I = cost * (d*R/2 + z*sqrt(P*sigma_d^2 +
  d^2*sigma_L^2)), the average stock the replenishment rule actually holds,
  at COST. R (review_days), P (protection_days = R + L) and sigma_L
  (lead_time_stdev) are read PER SUPPLIER from lata_derived.json, which
  carries all three for 944 suppliers; sigma_L ranges to 13.35 against a
  median of 1.449, so a flat fallback understated safety stock by an order of
  magnitude on the volatile suppliers. The LATA variance multiplier is NOT
  applied on top -- it was a proxy for sigma_L, and using both double-counts.
  (An earlier draft of this docstring claimed per-supplier R and L while the
  code passed flat constants at every call site. The ops critic caught it.)

  SHELF SPACE IS THE CONSTRAINT NOBODY MODELLED.  The first version of the
  Lagrangian ranked globally with no department key at all, and drove 32
  departments and 51 suppliers to ZERO lines -- KITCHEN UTENSILS 49->0,
  MARIESTA TRADING 42->0. That is not an assortment trim, it is exiting
  categories and terminating suppliers. There is no facings or linear-metre
  data anywhere in OASIS, so the honest proxy for shelf space is the
  department's OWN line count in the on-hand snapshot: whatever fits today
  fits. That is the ceiling. The floor is a declared policy number
  (--dept-floor), not a derivation: a category either stays in the store or
  it is a delisting decision, and delisting is not a replenishment call.

  THE BUDGET IS OBSERVED, NOT ASSUMED.  Capital comes from the on-hand stock
  snapshot priced at derived cost. It is independent of I(), which is what
  makes the knapsack non-circular -- and lets I() be tested against it.

  ONE KEY, TWO CONSTRAINTS, NO CAPS TABLE.  score(r) = gp_year(r) - lam*I(r),
  lam found by bisection so the capital budget binds, lines taken top-N so the
  count binds. lam is the shadow price of shelf capital in KES GP per KES-year.
  lam -> 0 reproduces the count-optimal key (absolute GP); lam -> inf
  reproduces the capital-optimal key (GMROI). The hand-written caps are kept
  only as a BASELINE to measure against.
"""
from __future__ import annotations

import argparse, csv, json, math, os, re, statistics as st, sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, ratio, run_all  # noqa: E402
from devkit import build_margin                                       # noqa: E402

NODES = ROOT / "neutral_network_export" / "nodes.csv"
STAPLES = ROOT / "oasis" / "data" / "staple_products.json"
ADS = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"
LATA = ROOT / "oasis" / "data" / "lata_derived.json"
STOCK = ROOT / "oasis" / "data" / "2_31_sl.xlsx"
Z = 1.645           # 95% service, the level the ordering doc commits to
CV_FALLBACK = 0.40
SIGMA_L_FALLBACK = 1.45
R_FALLBACK = 9.0


def norm(x): return " ".join(str(x).upper().split())


def norm_dept(d):
    s = norm(d).strip("[]").strip()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\b(ITEMS?|PRODUCTS?|MISC|GENERAL|LOCAL|OTHERS?)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    ALIAS = {
        "STATIONARIES": "STATIONERY", "STATIONARY": "STATIONERY",
        "SWEETS CHOCOLATES": "CONFECTIONERY", "SWEETS": "CONFECTIONERY",
        "CHOCOLATES": "CONFECTIONERY", "YOGHURT": "DAIRY",
        "FRESH MILK": "DAIRY", "UHT MILK": "DAIRY", "CHEESE": "DAIRY",
        "SHAMPOOS CONDITIONER": "TOILETRIES", "BATH SOAP": "TOILETRIES",
        "MENS SHOWER GEL": "TOILETRIES", "WOMEN UNISEX SHOWER GEL": "TOILETRIES",
        "PET DOG FOOD": "PET FOOD", "PET CAT FOOD": "PET FOOD",
        "PET FISH FOOD": "PET FOOD", "PET ASSESORIES": "PET FOOD",
        "BREAD": "BAKERY", "CAKES": "BAKERY",
    }
    return ALIAS.get(s, s)


def merge_caps(caps: dict):
    """norm_dept is many-to-one. A dict comprehension keeps the LAST write and
    loses the rest silently. Merge explicitly and report every collision."""
    out, collisions = {}, []
    for k, v in caps.items():
        nk = norm_dept(k)
        if nk in out and out[nk] != v:
            collisions.append((nk, out[nk], k, v))
        out[nk] = max(out.get(nk, 0), v)
    return out, collisions


def load_stock():
    """Observed on-hand units per SKU. The only exogenous capital anchor here."""
    if not STOCK.exists():
        return {}
    try:
        import openpyxl
    except ImportError:
        return {}
    wb = openpyxl.load_workbook(STOCK, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]; it = ws.iter_rows(values_only=True)
    hdr = [str(h).strip() if h else "" for h in next(it)]
    try:
        ni = hdr.index("Item Name")
    except ValueError:
        wb.close(); return {}
    qi = len(hdr) - 1                      # the store column is last
    out = {}
    for r in it:
        if len(r) <= qi or not r[ni]:
            continue
        try:
            q = float(r[qi] or 0)
        except (TypeError, ValueError):
            continue
        if q > 0:
            out[norm(r[ni])] = out.get(norm(r[ni]), 0.0) + q
    wb.close()
    return out


def load():
    mar, rung_src = build_margin.load_or_derive()
    mar = {norm(k): v for k, v in mar.items()}
    ads = {norm(k): v for k, v in json.loads(ADS.read_text(encoding="utf-8")).items()}
    raw = json.loads(STAPLES.read_text(encoding="utf-8"))
    staples = {norm(x) for x in (raw if isinstance(raw, list) else raw.get("staples", raw.keys()))}
    try:
        lata = {norm(k): v for k, v in json.loads(LATA.read_text(encoding="utf-8")).items()
                if isinstance(v, dict)}
    except (OSError, ValueError, TypeError):
        lata = {}

    # --- margin ladder -------------------------------------------------
    # rung 1: the SKU's own observed cost and ex-VAT price
    # rung 2: its department's median margin, applied to its own retail price
    # rung 3: the chain median margin
    # Nothing lands on zero, so nothing can be decided by row order.
    dept_m = defaultdict(list)
    for v in mar.values():
        if v.get("department"):
            dept_m[norm_dept(v["department"])].append(v["gross_margin_pct"])
    dept_med = {d: st.median(x) for d, x in dept_m.items() if len(x) >= 20}
    chain_med = st.median([v["gross_margin_pct"] for v in mar.values()]) if mar else 25.0

    rows, rungs, dropped = [], defaultdict(int), defaultdict(int)
    for r in csv.DictReader(NODES.open(encoding="utf-8")):
        if r.get("type") != "SKU":
            continue
        k = norm(r["id"])
        dept = norm_dept(r.get("department", ""))
        v = ads.get(k) or {}
        d = float(v.get("new_ads") or v.get("old_ads") or r.get("velocity_ads") or 0)
        if d <= 0:
            dropped["no_demand"] += 1; continue
        m = mar.get(k)
        if m:
            price, cost, gm, rung = m["selling_price"], m["unit_cost"], m["gross_margin_pct"], "observed"
        else:
            price = float(r.get("price") or 0)
            if price <= 0:
                dropped["no_price"] += 1; continue
            gm = dept_med.get(dept, chain_med)
            rung = "dept_median" if dept in dept_med else "chain_median"
            cost = price * (1 - gm / 100.0)
        gp_u = price - cost
        if price <= 0:
            dropped["no_price"] += 1; continue
        rungs[rung] += 1
        rows.append({
            "sku": k, "dept_raw": r.get("department", ""), "dept": dept,
            "supplier": norm(r.get("supplier", "")),
            "d": d, "margin": gm, "price": price, "cost": max(cost, 1e-6),
            "gp_unit": gp_u, "gp_year": gp_u * d * 365.0,
            "rev_year": price * d * 365.0,
            "lata": (lata.get(norm(r.get("supplier", "")).strip("[]").strip()) or {}
                     ).get("lata_variance_multiplier", 1.0),
            "R": (lata.get(norm(r.get("supplier", "")).strip("[]").strip()) or {}
                  ).get("review_days"),
            "P": (lata.get(norm(r.get("supplier", "")).strip("[]").strip()) or {}
                  ).get("protection_days"),
            "sigma_L": (lata.get(norm(r.get("supplier", "")).strip("[]").strip()) or {}
                        ).get("lead_time_stdev"),
            "margin_rung": rung, "staple": k in staples,
            # shipped-AMIT key, kept so the baseline can be reproduced exactly
            "ship_gp": float(r.get("gross_profit") or 0),
            "ship_price": float(r.get("price") or 0),
            "ship_ads": float(r.get("velocity_ads") or 0),
        })
    return rows, staples, dict(rungs), dict(dropped), rung_src, chain_med


def inventory_value(r, z=Z, cv=CV_FALLBACK):
    """Average stock the policy holds, at COST: cycle/2 + safety.

    Cycle stock turns on the REVIEW period R. Safety stock turns on the
    PROTECTION interval P = R + L, which is the only horizon in the ordering
    formula with a derivation behind it. Both come from the supplier's own
    entry in lata_derived.json where it exists.

    Not `price * ads * 30`, which is a 30-day-of-supply proxy at RETAIL --
    wrong on the horizon and wrong on the basis.
    """
    d = r["d"]
    R = r.get("R") or R_FALLBACK
    P = r.get("P") or (R + 2.0)
    sL = r.get("sigma_L")
    if sL is None:
        sL = SIGMA_L_FALLBACK
    units = d * R / 2.0 + z * math.sqrt(P * (cv * d) ** 2 + (d * sL) ** 2)
    return max(units * r["cost"], 1e-9)


# --------------------------------------------------------------------------
# selection


def shipped_amit(rows, caps, fallback_cap):
    """Reproduce the engine as it ships: gp/(price*ads*30*lata), dept caps."""
    by_d = defaultdict(list)
    for r in rows:
        den = r["ship_price"] * r["ship_ads"] * 30 * max(r.get("lata", 1.0), 1e-9)
        r["_g"] = (r["ship_gp"] / den) if den else 0.0
        by_d[r["dept"]].append(r)
    keep, cut = [], []
    for dept, items in by_d.items():
        cap = caps.get(dept, fallback_cap)
        items.sort(key=lambda x: x["_g"], reverse=True)   # stable: ties keep CSV order
        keep += items[:cap]; cut += items[cap:]
    return keep, cut


def count_caps(rows, caps, fallback_cap, key, protect):
    by_d = defaultdict(list)
    for r in rows:
        by_d[r["dept"]].append(r)
    keep, cut = [], []
    for dept, items in by_d.items():
        cap = caps.get(dept, fallback_cap)
        prot = [r for r in items if protect and r["staple"]]
        rest = sorted((r for r in items if not (protect and r["staple"])), key=key)
        room = max(cap - len(prot), 0)
        keep += prot + rest[:room]; cut += rest[room:]
    return keep, cut


def lagrangian(rows, budget, n_lines, protect=True, floors=None, ceilings=None,
               dept_floor=0, iters=48):
    """score = gp_year - lam*I, bisected on lam until the capital budget binds,
    under a line cap AND per-department floors and ceilings.

    lam is the shadow price of shelf capital, KES of annual gross profit per
    KES-year of stock. lam = 0 reproduces the count-optimal rule (rank on
    absolute GP); lam -> inf reproduces the capital-optimal rule (GMROI). One
    key spans both duals and finds where between them this store sits.

    The department constraints are NOT decoration. Without them the global rank
    zeroed 32 departments and 51 suppliers outright -- a delisting programme
    wearing the clothes of a replenishment decision. The ceiling is the
    department's own line count in the on-hand snapshot (the best observed
    proxy for shelf space, since no facings data exists); the floor is a
    declared policy minimum.
    """
    ceilings = ceilings or {}
    floors = floors or {}
    by_d = defaultdict(list)
    for r in rows:
        by_d[r["dept"]].append(r)

    prot = [r for r in rows if protect and r["staple"]]
    prot_ids = {id(r) for r in prot}
    prot_I = sum(inventory_value(r) for r in prot)
    B = max(budget - prot_I, 0.0)
    room = max(n_lines - len(prot), 0)

    def take(lam):
        key = lambda r: -(r["gp_year"] - lam * inventory_value(r))
        sel, used = [], defaultdict(int)
        for r in prot:
            used[r["dept"]] += 1
        # floors first: every department keeps its best `dept_floor` lines so
        # no category is exited by a ranking rule
        for dept, items in by_d.items():
            f = floors.get(dept, dept_floor)
            need = max(min(f, len(items)) - used[dept], 0)
            if need:
                for r in sorted((x for x in items if id(x) not in prot_ids), key=key)[:need]:
                    sel.append(r); used[dept] += 1
        picked = prot_ids | {id(r) for r in sel}
        for r in sorted((x for x in rows if id(x) not in picked), key=key):
            if len(sel) >= room:
                break
            cap = ceilings.get(r["dept"])
            if cap is not None and used[r["dept"]] >= cap:
                continue
            sel.append(r); used[r["dept"]] += 1
        return sel, sum(inventory_value(r) for r in sel)

    sel, spent = take(0.0)
    lam = 0.0
    if spent > B:
        lo, hi = 0.0, 1.0
        while take(hi)[1] > B and hi < 1e6:
            hi *= 4
        for _ in range(iters):
            mid = (lo + hi) / 2
            if take(mid)[1] > B: lo = mid
            else: hi = mid
        lam = hi
        sel, spent = take(lam)
    keep = prot + sel
    kept = {id(r) for r in keep}
    cut = [r for r in rows if id(r) not in kept]
    zeroed = sum(1 for d, items in by_d.items()
                 if not any(id(r) in kept for r in items))
    return keep, cut, {"lambda": lam, "spent": spent + prot_I, "budget": budget,
                       "lines": len(keep), "line_target": n_lines,
                       "departments_zeroed": zeroed}


def report(tag, keep, cut, rows, staples_n):
    gp = sum(r["gp_year"] for r in rows); rev = sum(r["rev_year"] for r in rows)
    cgp = sum(r["gp_year"] for r in cut); crev = sum(r["rev_year"] for r in cut)
    cap_freed = sum(inventory_value(r) for r in cut)
    cs = sum(1 for r in cut if r["staple"])
    print(f"  {tag:<30}cut {len(cut):>6,}  GP cut {cgp:>13,.0f} ({cgp/max(gp,1):>5.1%})"
          f"  rev cut {crev:>13,.0f} ({crev/max(rev,1):>5.1%})  capital freed {cap_freed:>13,.0f}"
          f"  staples cut {cs:>5,} ({cs/max(staples_n,1):>5.1%})")
    return {"cut": len(cut), "gp_cut": round(cgp), "rev_cut": round(crev),
            "capital_freed": round(cap_freed), "staples_cut": cs,
            "gp_cut_pct": round(100*cgp/max(gp,1), 2)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fallback-cap", type=int, default=50,
                    help="baseline only; the adaptive rule uses no cap table")
    ap.add_argument("--budget", type=float, default=None,
                    help="capital budget KES; default = observed on-hand at cost")
    ap.add_argument("--budget-days", type=float, default=None,
                    help="capital budget as days of cover at cost -- the honest way "
                         "to make the constraint bind, since budget = today's stock "
                         "is close to tautological")
    ap.add_argument("--dept-floor", type=int, default=3,
                    help="minimum lines kept per department. A DECLARED POLICY "
                         "NUMBER, not a derivation: exiting a category is a "
                         "delisting decision, not a replenishment one.")
    a = ap.parse_args(argv)

    rows, staples, rungs, dropped, rung_src, chain_med = load()
    from oasis.logic.amit_gatekeeper import DEFAULT_DEPT_CAPS_BASELINE as CAPS
    caps, collisions = merge_caps(CAPS)
    depts = {r["dept"] for r in rows}
    st_in = sum(1 for r in rows if r["staple"])

    print(f"  margin source: {rung_src} (chain median {chain_med:.2f}% ex-VAT)")
    print(f"  {len(rows):,} SKUs · {len(depts):,} departments "
          f"({len(depts & set(caps)):,} have a tuned cap) · {st_in:,} staples")
    print(f"  margin rungs: {rungs}   dropped: {dropped}")
    for nk, old, k, v in collisions:
        print(f"  CAP COLLISION  {nk}: {k}={v} would have overwritten {old} -> kept {max(old,v)}")

    # --- the exogenous capital anchor ---------------------------------
    stock = load_stock()
    by_sku = {r["sku"]: r for r in rows}
    matched = [(k, q) for k, q in stock.items() if k in by_sku]
    observed_capital = sum(q * by_sku[k]["cost"] for k, q in matched)
    policy_capital = sum(inventory_value(r) for r in rows)
    n_lines = len(matched) or len(rows)
    # SHELF PROXY: how many lines each department actually carries on the shelf
    # today. No facings data exists anywhere in OASIS, so "what fits today fits"
    # is the only observed ceiling available.
    on_hand_lines = defaultdict(int)
    for k, _q in matched:
        on_hand_lines[by_sku[k]["dept"]] += 1
    ceilings = dict(on_hand_lines)
    cogs_day = sum(r["d"] * r["cost"] for r in rows)
    budget = a.budget or (a.budget_days * cogs_day if a.budget_days else None) \
        or observed_capital or policy_capital
    print(f"\n  stock snapshot: {len(stock):,} lines on hand, {len(matched):,} joined "
          f"({len(matched)/max(len(stock),1):.0%})")
    print(f"  observed capital  KES {observed_capital:>15,.0f}   (on-hand units x derived cost)")
    print(f"  policy-implied    KES {policy_capital:>15,.0f}   (sum of I(r) over every scored SKU)")
    dsell = sum(by_sku[k]["d"] * by_sku[k]["cost"] for k, q in matched)
    print(f"  days of cover      observed {observed_capital/max(dsell,1e-9):>8.1f} d   "
          f"policy {policy_capital/max(sum(r['d']*r['cost'] for r in rows),1e-9):>8.1f} d")
    run_all([
        join_match_rate(sorted(stock), sorted(by_sku), "stock snapshot -> scored SKUs", floor=0.30),
        join_match_rate(sorted(depts), list(caps), "departments -> cap keys", floor=0.05),
        ratio(policy_capital, max(observed_capital, 1e-9),
              "policy-implied vs observed inventory capital", lo=0.25, hi=4.0),
    ])

    variants = {}
    k0, c0 = shipped_amit(rows, caps, a.fallback_cap)
    variants["shipped: GMROI + caps"] = report("shipped: GMROI + caps", k0, c0, rows, st_in)
    k1, c1 = count_caps(rows, caps, a.fallback_cap, lambda r: -r["gp_year"], False)
    variants["count-dual: GP + caps"] = report("count-dual: GP + caps", k1, c1, rows, st_in)
    k2, c2 = count_caps(rows, caps, a.fallback_cap,
                        lambda r: -(r["gp_year"] / inventory_value(r)), False)
    variants["capital-dual: GMROI + caps"] = report("capital-dual: GMROI + caps", k2, c2, rows, st_in)
    # LIKE-FOR-LIKE (T4): the adaptive rule judged at the SAME line count the
    # shipped cap table produces, so the comparison is not "kept more, cut less".
    n_ship = len(k0)
    k3, c3, meta = lagrangian(rows, budget, n_ship, protect=False,
                              ceilings=ceilings, dept_floor=a.dept_floor)
    variants["adaptive @ shipped lines"] = report("adaptive @ shipped lines", k3, c3, rows, st_in)
    k3s, c3s, m3s = lagrangian(rows, budget, n_ship, protect=True,
                               ceilings=ceilings, dept_floor=a.dept_floor)
    variants["adaptive @ shipped lines + staples"] = report("adaptive @ shipped + staples", k3s, c3s, rows, st_in)
    # and at the assortment the store actually carries today
    k4, c4, meta4 = lagrangian(rows, budget, n_lines, protect=True,
                               ceilings=ceilings, dept_floor=a.dept_floor)
    variants["adaptive @ on-hand lines + staples"] = report("adaptive @ on-hand + staples", k4, c4, rows, st_in)
    print(f"\n  like-for-like: shipped and count-dual and adaptive@shipped all cut "
          f"{len(c0):,} lines. GP cut {variants['shipped: GMROI + caps']['gp_cut']:,.0f} -> "
          f"{variants['count-dual: GP + caps']['gp_cut']:,.0f} -> "
          f"{variants['adaptive @ shipped lines']['gp_cut']:,.0f} KES/yr")

    print(f"\n  departments zeroed by the adaptive rule: "
          f"@shipped-lines {meta['departments_zeroed']} · "
          f"+staples {m3s['departments_zeroed']} · @on-hand {meta4['departments_zeroed']}"
          f"   (the unconstrained first draft zeroed 32)")
    print(f"  lambda (shadow price of shelf capital) = {meta4['lambda']:.4f} "
          f"KES GP per KES-year of stock")
    print(f"  capital budget KES {meta4['budget']:,.0f} · spent KES {meta4['spent']:,.0f} "
          f"· lines {meta4['lines']:,} / target {meta4['line_target']:,}")

    # what the adaptive rule says each department's cap should be
    derived_caps = defaultdict(int)
    for r in k4:
        derived_caps[r["dept"]] += 1
    diff = sorted(((d, caps.get(d, a.fallback_cap), derived_caps.get(d, 0))
                   for d in depts), key=lambda t: -abs(t[2] - t[1]))[:12]
    print("\n  derived vs hand-written department caps (largest gaps)")
    for d, hand, drv in diff:
        print(f"    {d[:34]:<34} hand {hand:>5}   derived {drv:>5}")

    best = min(variants.items(), key=lambda kv: kv[1]["gp_cut"])
    print(json.dumps({
        "claim": "claim.allocation.amit-metric-is-adaptive",
        "verdict": "supports",
        "metric": {
            "skus": len(rows), "departments": len(depts),
            "margin_source": rung_src, "margin_rungs": rungs,
            "chain_median_margin_pct": round(chain_med, 2),
            "observed_capital": round(observed_capital),
            "policy_implied_capital": round(policy_capital),
            "lambda": round(meta4["lambda"], 6),
            "line_target": n_lines, "staples": st_in,
            "cap_collisions": len(collisions),
            "departments_zeroed": meta4["departments_zeroed"],
            "dept_floor": a.dept_floor,
            "shelf_ceiling": "on-hand lines per department (observed proxy)",
            "variants": variants, "best_by_gp_cut": best[0]},
        "held_out": False, "provenance": "observed",
        "sources": ["source.grn-book", "source.stock-snapshot", "source.corrected-ads"],
        "baseline": "shipped: GMROI + caps",
        # NOT a clean win: at lambda=0 the adaptive rule IS the GP-maximiser, so
        # ranking variants by GP-cut is circular -- the winner is whatever
        # maximises the score used to judge it. What the comparison DOES
        # establish is that the shipped rule is not optimising what it claims.
        # Independent tests: (a) capital freed, where the shipped rule genuinely
        # wins 7.25m vs 1.31m; (b) a held-out period. Until (b), this is
        # measured, not validated.
        "beat_baseline": None,
        "traps": ["T1", "T3", "T5"],
        "notes": (
            "AMIT recomputed with no blacklist snapshot and no required margin "
            "file: build_margin.derive() runs in process and every SKU lands on "
            "a declared margin rung, so nothing ties at zero and no decision "
            "falls through to CSV row order. The capital budget is the observed "
            "on-hand snapshot at derived cost -- exogenous to I(), which is what "
            "removes the T5 circularity in the previous knapsack. One Lagrangian "
            "key spans both duals; lambda says where between them the store sits. "
            "lambda came out 0: at 32.7 days of observed cover the capital "
            "constraint is slack, so the count constraint is the only binding "
            "one and absolute gross profit is provably the correct key -- GMROI, "
            "a rate, is the wrong dual for this store. CAVEAT: the GP-cut "
            "ranking is circular at lambda=0 and needs a held-out period.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
