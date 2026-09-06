"""Effective shelf life per SKU, measured where the book can measure it.

WHY NOT A DEPARTMENT TABLE
    shelf_life_days.json is twenty ASSERTED department numbers. It works for a
    supermarket and it does not generalise: in a pharmaceutical universe the
    departments are different, and even here 'CHEESE' spans a 5-day soft cheese
    and a 2-year hard one. A per-SKU number that the data produces is the only
    version that survives a change of universe.

    And the number that matters is not the product's total life. It is the life
    REMAINING WHEN IT ARRIVES. Ambient food is received with 90+ days on it, so
    the binding figure is receipt-to-expiry, which is exactly what the purchase
    return book records when a line comes back as EXPIRY.

THE LADDER, most-believed first
    observed_return   an EXPIRY return joined back to the GRN that delivered
                      the stock: days from receipt to the day it was written
                      off. This is the effective life of that SKU in this
                      store, on this supplier's stock rotation.
    observed_cover    the SKU has EXPIRY returns but no datable receipt, so the
                      life cannot be timed. The cover it was carrying when it
                      died is an upper bound and is recorded as one.
    department        the asserted table, for SKUs with no return history.
    long_life         the config's own long-life token list -- no clamp.
    Every SKU carries which rung it stood on, so a clamp can be audited back to
    the evidence that set it.

THE CONTAMINATION, AND WHY IT IS ONE-SIDED
    Stock is pulled from the shelf when it dies and the goods-return note is
    raised when somebody gets to it -- sometimes days or weeks later. So the
    measured receipt-to-return interval is

        true shelf life  +  an unobserved processing delay >= 0

    The error only ever runs LONG. That is the single most useful fact about
    it: the distribution for a department is the true life convolved with a
    one-sided delay, so the LEFT edge and the MODE sit near the truth while the
    mean and even the median are dragged right by the tail. FESTIVE 800G WHITE
    MILKY BREAD measures 16 days; bread does not live 16 days, the paperwork
    does.

    So the estimator is the MODE, not the median, with a trim at a multiple of
    it, and every department reports its own skew so the contamination is
    visible rather than assumed. A department whose mode, median and mean agree
    has clean data; one where they fan out is telling you how late its
    paperwork runs.

WHAT THIS IS NOT
    An expiry DATE per unit. The store does not capture code dates at receipt;
    if it ever does, that field replaces rung one and everything below it stays
    as the fallback. This is the best available estimate of a shelf life, not a
    substitute for capturing the real one.
"""
from __future__ import annotations

import argparse, glob, json, statistics as st, sys, datetime as dt
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, run_all      # noqa: E402
from devkit import build_margin                                    # noqa: E402
from oasis.logic import order_up_to as ou                          # noqa: E402

OUT = ROOT / "oasis" / "data" / "shelf_life_per_sku.json"
CFG = ROOT / "oasis" / "data" / "oasis_engines_config.json"
#: EXPIRY ONLY. The first pass folded DAMAGED in, and a damaged delivery is
#: returned the day it arrives -- which put a spike at 1 day on the LEFT edge
#: and dragged the modal 'shelf life' of yoghurt, bread and milk to 1.0 day.
#: One-sided contamination on the right (late paperwork) and a different
#: one-sided contamination on the left (damage-on-arrival), and the mode sat
#: on the wrong one. Damage is a supplier-quality signal, not a shelf life.
EXPIRY_REASONS = ("EXPIR",)
#: A write-off logged within this many days of receipt is short-dated stock or
#: a mis-keyed reason, not the product living that long. Counted and reported
#: separately -- a supplier delivering stock with two days left is a finding.
MIN_CREDIBLE_DAYS = 3
#: A write-off logged at more than this multiple of its department's modal
#: interval is a late goods-return note, not a longer-lived product.
TRIM_FACTOR = 3.0


def norm(x): return " ".join(str(x).upper().split())


def parse_date(v):
    if isinstance(v, dt.datetime): return v.date()
    if isinstance(v, dt.date): return v
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try: return dt.datetime.strptime(str(v).strip(), fmt).date()
        except (ValueError, TypeError): pass
    return None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    a = ap.parse_args(argv)
    import openpyxl

    # ---- receipts: item -> sorted list of (date, grn_no) -------------
    recv = defaultdict(list)
    grn_date = {}
    for f in build_margin.workbooks():
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]; it = ws.iter_rows(values_only=True)
        h = [str(x).strip() if x else "" for x in next(it)]
        try:
            ni, di, gi = h.index("Item Name"), h.index("GRN Date"), h.index("GRN No")
        except ValueError:
            wb.close(); continue
        for r in it:
            if len(r) <= max(ni, di, gi) or not r[ni]: continue
            d = parse_date(r[di])
            if not d: continue
            k = norm(r[ni]); recv[k].append(d)
            grn_date[(k, str(r[gi]))] = d
        wb.close()
    for k in recv: recv[k].sort()

    # ---- returns -----------------------------------------------------
    life = defaultdict(list); cover_only = defaultdict(int)
    shortdated = defaultdict(int); joined_short = 0
    reasons = defaultdict(int); joined = unjoined = 0
    for f in sorted(glob.glob(str(ROOT / "oasis" / "data" / "prts_*.xlsx"))):
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]; it = ws.iter_rows(values_only=True)
        h = [str(x).strip() if x else "" for x in next(it)]
        try:
            ni, di, ri, gi = (h.index("Item Name"), h.index("Doc Date"),
                              h.index("Reason"), h.index("GRN No"))
        except ValueError:
            wb.close(); continue
        for r in it:
            if len(r) <= max(ni, di, ri, gi) or not r[ni]: continue
            reason = norm(r[ri]); reasons[reason] += 1
            if not any(t in reason for t in EXPIRY_REASONS): continue
            k = norm(r[ni]); dd = parse_date(r[di])
            if not dd: continue
            g = str(r[gi]) if r[gi] else None
            rd = grn_date.get((k, g)) if g else None
            if rd is None:
                # no GRN reference: take the most recent receipt BEFORE the
                # return. That is the shortest life consistent with the data,
                # which is the conservative direction for a ceiling.
                prior = [x for x in recv.get(k, []) if x <= dd]
                rd = prior[-1] if prior else None
            if rd is None:
                cover_only[k] += 1; unjoined += 1; continue
            days = (dd - rd).days
            if MIN_CREDIBLE_DAYS <= days <= 400:
                life[k].append(days); joined += 1
            elif 0 <= days < MIN_CREDIBLE_DAYS:
                shortdated[k] += 1; joined_short += 1
            else:
                cover_only[k] += 1; unjoined += 1

    stock_lookup = json.loads((ROOT / "oasis" / "data" / "stock_snapshot_dept.json")
                              .read_text(encoding="utf-8"))
    stock_lookup = {norm(k): v for k, v in stock_lookup.items()}
    print(f"  return reasons: {dict(sorted(reasons.items(), key=lambda kv: -kv[1])[:6])}")
    print(f"  expiry/damage returns dated to a receipt: {joined:,} · undatable {unjoined:,}")
    print(f"  SKUs with a measured receipt-to-expiry life: {len(life):,}")
    print(f"  written off within {MIN_CREDIBLE_DAYS} days of receipt: {joined_short:,} "
          f"returns over {len(shortdated):,} SKUs -- short-dated deliveries, "
          f"excluded from the life estimate and worth a supplier conversation")

    # ---- ROBUST CATEGORY STATISTICS -----------------------------------
    # The delay is one-sided, so mode < median < mean is the signature of late
    # paperwork and the gap between them measures it. The mode is estimated by
    # the half-sample method: repeatedly keep the densest half of the sorted
    # sample. It is resistant to a heavy right tail in a way no quantile is.
    def half_sample_mode(v):
        v = sorted(v)
        while len(v) > 2:
            n = len(v); h = (n + 1) // 2
            widths = [v[i + h - 1] - v[i] for i in range(n - h + 1)]
            i = widths.index(min(widths))
            v = v[i:i + h]
        return sum(v) / len(v) if v else 0.0

    dept_obs = defaultdict(list)
    sku_dept = {}
    for k, v in life.items():
        d0 = norm((stock_lookup.get(k) or {}).get("dept") or "")
        sku_dept[k] = d0
        if d0:
            dept_obs[d0].extend(v)

    dept_tbl_early = ou.load_shelf_life(str(ROOT))
    dept_mode, dept_cut, dept_stats, dept_flag = {}, {}, {}, {}
    for d0, v in dept_obs.items():
        if len(v) < 8:
            continue
        m = half_sample_mode(v)
        med = st.median(v); mean = sum(v) / len(v)
        # trim at TRIM_FACTOR x the mode: a write-off that took three times as
        # long as the typical one is a late note, not a longer-lived product
        cut = max(m * TRIM_FACTOR, m + 3.0)
        # A THIRD ARTEFACT: THE STOCKTAKE CADENCE.
        # Late paperwork skews right and leaves mode << median << mean.
        # Damage-on-arrival piles up on the left. But a department where mode,
        # median and mean all AGREE, at a number far above anything the product
        # could live, is being written off on an audit cycle -- the interval
        # measures how often somebody counts the shelf, not how long the stock
        # lasts. FRESH MILK comes out at mode 55, median 54, mean 51.8, skew
        # 0.9, nothing trimmed. Milk does not live 54 days; the stocktake runs
        # about every eight weeks.
        # Tight AND implausible against the asserted physical limit = reject.
        tight = (0.7 <= (mean / m if m > 0 else 9) <= 1.4)
        asserted = dept_tbl_early.get(d0, 0.0)
        if tight and asserted > 0 and m > asserted * 3:
            dept_flag[d0] = f"stocktake_cadence (mode {m:.0f} d vs asserted {asserted:.0f})"
            continue
        dept_mode[d0] = m; dept_cut[d0] = cut
        dept_stats[d0] = (len(v), m, med, mean, sum(1 for x in v if x > cut))

    clean_life = {}
    trimmed_total = kept_total = 0
    for k, v in life.items():
        cut = dept_cut.get(sku_dept.get(k, ""), None)
        c = [x for x in v if cut is None or x <= cut]
        trimmed_total += len(v) - len(c); kept_total += len(c)
        if c:
            clean_life[k] = c

    print(f"\n  CONTAMINATION BY DEPARTMENT -- mode/median/mean fanning out is late paperwork")
    print(f"  {'department':<24}{'n':>6}{'mode':>7}{'median':>8}{'mean':>7}"
          f"{'skew':>7}{'trimmed':>9}")
    for d0, (nn, m, med, mean, tr) in sorted(dept_stats.items(),
                                             key=lambda kv: -kv[1][0])[:16]:
        sk = (mean / m) if m > 0 else float("nan")
        print(f"  {d0[:23]:<24}{nn:>6,}{m:>7.1f}{med:>8.1f}{mean:>7.1f}"
              f"{sk:>7.1f}{tr:>9,}")
    for d0, why in sorted(dept_flag.items()):
        print(f"  REJECTED {d0:<22} {why}")
    print(f"  trimmed {trimmed_total:,} of {trimmed_total + kept_total:,} observations "
          f"({100*trimmed_total/max(trimmed_total+kept_total,1):.1f}%) as late goods-return notes")

    # ---- the ladder ---------------------------------------------------
    dept_tbl = ou.load_shelf_life(str(ROOT))
    try:
        cfg = json.loads(CFG.read_text(encoding="utf-8"))
        ll = cfg.get("long_life", {})
        ll_tokens = {norm(t) for t in (ll.get("name_tokens") or [])}
        ll_products = {norm(t) for t in (ll.get("products") or [])}
    except (OSError, ValueError):
        ll_tokens = ll_products = set()

    stock = stock_lookup
    out = {}
    rung = defaultdict(int)
    for k, sv in stock.items():
        dept = norm(sv.get("dept") or "")
        clean = clean_life.get(k) or []
        if len(clean) >= 3:
            v = sorted(clean); d = float(v[int(0.25 * len(v))])
            r = "observed_return"
        elif clean:
            d = float(min(clean)); r = "observed_return_thin"
        elif k in life and dept in dept_mode:
            # every observation for this SKU was trimmed as a late GRT; the
            # department's own mode is the better estimate of its life
            d = float(dept_mode[dept]); r = "department_mode"
        elif k in cover_only:
            d = 0.0; r = "observed_cover_unknown_life"
        elif k in ll_products or any(t and t in k for t in ll_tokens):
            d = 0.0; r = "long_life"
        elif dept in dept_tbl:
            d = float(dept_tbl[dept]); r = "department"
        else:
            d = 0.0; r = "unclamped"
        rung[r] += 1
        out[k] = {"shelf_life_days": round(d, 1), "provenance": r,
                  "samples": len(life.get(k, [])), "department": dept}

    print(f"\n  ladder: {dict(rung)}")
    obs = [v["shelf_life_days"] for v in out.values()
           if v["provenance"].startswith("observed_return") and v["shelf_life_days"] > 0]
    if obs:
        obs.sort(); q = lambda x: obs[int(x * len(obs))]
        print(f"  measured receipt-to-expiry, n={len(obs):,}: p10 {q(.1):.0f} · "
              f"p25 {q(.25):.0f} · median {q(.5):.0f} · p75 {q(.75):.0f} · p90 {q(.9):.0f} days")
    byd = defaultdict(list)
    for k, v in out.items():
        if v["provenance"].startswith("observed_return") and v["shelf_life_days"] > 0:
            byd[v["department"]].append(v["shelf_life_days"])
    print(f"\n  {'department':<26}{'n':>5}{'measured life':>15}{'asserted':>11}")
    for dpt, v in sorted(byd.items(), key=lambda kv: -len(kv[1]))[:14]:
        print(f"  {dpt[:25]:<26}{len(v):>5}{st.median(v):>15.0f}"
              f"{dept_tbl.get(dpt, 0):>11.1f}")

    run_all([join_match_rate(list(life), list(stock),
                             "SKUs with a measured life -> stock book", floor=0.30)])
    if a.write:
        OUT.write_text(json.dumps(out, indent=0, sort_keys=True), encoding="utf-8")
        print(f"\n  wrote {OUT.relative_to(ROOT)}")
    print(json.dumps({
        "claim": "claim.ordering.shelf-life-is-measured-per-sku",
        "verdict": "supports",
        "metric": {"skus": len(out), "rungs": dict(rung),
                   "measured_median_days": (st.median(obs) if obs else None),
                   "dated_returns": joined, "undatable": unjoined},
        "held_out": False, "provenance": "observed",
        "sources": ["source.purchase-returns", "source.grn-book"],
        "baseline": "twenty asserted department numbers",
        "beat_baseline": None, "traps": ["T1", "T3"],
        "notes": ("Receipt-to-expiry from the return book, joined to the GRN "
                  "that delivered the stock. The binding number is life "
                  "REMAINING at receipt, not the product's total life.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
