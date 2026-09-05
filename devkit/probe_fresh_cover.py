"""Fresh cohort: what the shelf holds, what the engine would order, and why.

THE QUESTION
    Pouch milk from a daily-delivery dairy is sitting at 25-30 days of cover.
    Either the stock snapshot is not counting sellable eaches, or the demand
    feed understates velocity, or the ordering policy is genuinely asking for
    a month of a product with a week of life. Those are three different
    tickets and they must be separated before anything is "optimised".

THE EVIDENCE THAT SETTLES R
    The review period R is how long until the buyer next gets a chance to
    order. lata_derived.json carries review_days = 7.0 for ALL 944 suppliers,
    which is not a measurement, it is a default wearing a measurement's
    clothes. The GRN book knows better: every receipt carries a vendor and a
    date, so the DISTINCT DELIVERY DATES per vendor measure the cadence the
    supplier actually runs. A dairy that appears on 300 distinct dates in 15
    months is not a weekly supplier.

WHAT THIS DOES NOT DO
    It does not change anything. It reports the decomposition:
        observed cover  =  what the shelf holds / daily demand
        policy cover    =  S / d, from order_up_to.recommend as it ships
        cycle vs safety, and the share of policy cover that is R alone
    so the next step edits the term that is actually wrong.
"""
from __future__ import annotations

import argparse, glob, json, math, os, statistics as st, sys, datetime as dt
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, ratio, run_all   # noqa: E402
from devkit import build_margin                                        # noqa: E402
from oasis.logic import order_up_to as ou                              # noqa: E402

CADENCE = ROOT / "oasis" / "data" / "supplier_cadence_from_grn.json"
STOCK = ROOT / "oasis" / "data" / "2_31_sl.xlsx"
ADS = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"

# The fresh cohort, by the department names the data actually uses.
FRESH = {
    "FRESH MILK": 1.2, "UHT MILK": 90.0, "YOGHURT": 14.0, "CHEESE": 21.0,
    "BREAD": 1.2, "CAKES": 3.0, "BAKERY": 2.0, "DELI": 5.0,
    "FRESH GOURMET": 5.0, "BUTTER AND MARGARINE": 30.0, "CREAM": 10.0,
    "EGGS": 14.0, "FRUITS": 4.0, "VEGETABLES": 4.0, "FRESH JUICE": 5.0,
    "SAUSAGES": 7.0, "FROZEN GOURMET": 60.0, "ICE CREAM": 90.0,
}


def norm(x): return " ".join(str(x).upper().split())


def strip_code(v):
    """'SA0238 - ABONY DAIRIES LIMITED' -> 'ABONY DAIRIES LIMITED'."""
    s = norm(v).strip("[]").strip()
    if " - " in s:
        s = s.split(" - ", 1)[1]
    return s.strip()


def derive_cadence(rebuild=False):
    """Distinct delivery dates per vendor, straight out of the GRN book.

    Gap statistics are computed on the SORTED distinct dates, so a vendor
    delivering every weekday shows a median gap of 1 and a p90 of 3 (the
    weekend), not an average of 1.4 that hides both facts.
    """
    if CADENCE.exists() and not rebuild:
        try:
            return json.loads(CADENCE.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass
    import openpyxl
    dates = defaultdict(set)
    for f in build_margin.workbooks():
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]; it = ws.iter_rows(values_only=True)
        hdr = [str(h).strip() if h else "" for h in next(it)]
        try:
            vi, di = hdr.index("Vendor Code - Name"), hdr.index("GRN Date")
        except ValueError:
            wb.close(); continue
        for r in it:
            if len(r) <= max(vi, di) or not r[vi]:
                continue
            d = r[di]
            try:
                dd = d.date() if isinstance(d, dt.datetime) else dt.datetime.strptime(str(d), "%d-%b-%Y").date()
            except Exception:
                continue
            dates[strip_code(r[vi])].add(dd)
        wb.close()
    out = {}
    for v, ds in dates.items():
        ds = sorted(ds)
        if len(ds) < 2:
            out[v] = {"deliveries": len(ds), "span_days": 0,
                      "median_gap": None, "p90_gap": None, "provenance": "observed"}
            continue
        gaps = [(ds[i + 1] - ds[i]).days for i in range(len(ds) - 1)]
        gaps.sort()
        out[v] = {"deliveries": len(ds), "span_days": (ds[-1] - ds[0]).days,
                  "median_gap": st.median(gaps),
                  "p90_gap": gaps[min(int(0.9 * len(gaps)), len(gaps) - 1)],
                  "first": ds[0].isoformat(), "last": ds[-1].isoformat(),
                  "provenance": "observed"}
    CADENCE.write_text(json.dumps(out, indent=1, sort_keys=True), encoding="utf-8")
    return out


def load_stock():
    import openpyxl
    wb = openpyxl.load_workbook(STOCK, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]; it = ws.iter_rows(values_only=True)
    hdr = [str(h).strip() if h else "" for h in next(it)]
    ni, di, qi = hdr.index("Item Name"), hdr.index("Department"), len(hdr) - 1
    out = {}
    for r in it:
        if len(r) <= qi or not r[ni]:
            continue
        try:
            q = float(r[qi] or 0)
        except (TypeError, ValueError):
            continue
        out[norm(r[ni])] = {"qty": q, "dept": norm(r[di] or "")}
    wb.close()
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild-cadence", action="store_true")
    ap.add_argument("--mode", default=None, help="scheduled | on_demand")
    ap.add_argument("--top", type=int, default=18)
    a = ap.parse_args(argv)

    cad = derive_cadence(a.rebuild_cadence)
    mar, src = build_margin.load_or_derive()
    mar = {norm(k): v for k, v in mar.items()}
    ads = {norm(k): v for k, v in json.loads(ADS.read_text(encoding="utf-8")).items()}
    stock = load_stock()
    sched = ou.load_review_schedule(str(ROOT))
    pats = ou.default_patterns(str(ROOT))

    print(f"  cadence from GRN book: {len(cad):,} vendors "
          f"({sum(1 for v in cad.values() if v['deliveries']>=10):,} with >=10 deliveries)")
    print(f"  margin source {src} · review calendar {len(sched):,} suppliers")

    rows = []
    for k, sv in stock.items():
        dept = sv["dept"]
        if dept not in FRESH:
            continue
        m = mar.get(k)
        av = ads.get(k) or {}
        d = float(av.get("new_ads") or av.get("old_ads") or 0)
        if d <= 0 or not m:
            continue
        vendor = strip_code(m.get("vendor") or "")
        c = cad.get(vendor) or {}
        rec = ou.recommend({
            "avg_daily_sales": d, "supplier_name": vendor,
            "current_stock": sv["qty"], "lead_time_days":
                (pats.get(vendor) or {}).get("lead_time_mean", (pats.get(vendor) or {}).get("lead_time_days", 2.0)),
        }, schedule=sched, patterns=pats, mode=a.mode)
        rows.append({
            "sku": k, "dept": dept, "vendor": vendor, "d": d,
            "on_hand": sv["qty"], "cost": m["unit_cost"],
            "obs_cover": sv["qty"] / d,
            "policy_cover": rec["S"] / d, "S": rec["S"],
            "R": rec["R"], "L": rec["L"], "P": rec["P"],
            "cycle_cover": rec["cycle_stock"] / d,
            "safety_cover": rec["safety_stock"] / d,
            "scheduled": rec["scheduled"],
            "deliveries": c.get("deliveries", 0),
            "median_gap": c.get("median_gap"),
            "shelf_life": FRESH[dept],
        })
    if not rows:
        print("  no fresh SKUs joined"); return 2

    run_all([
        join_match_rate([r["vendor"] for r in rows], list(cad),
                        "fresh vendors -> GRN cadence", floor=0.60),
        join_match_rate([r["vendor"] for r in rows], list(sched),
                        "fresh vendors -> review calendar", floor=0.05),
    ])

    print(f"\n  {len(rows):,} fresh SKUs on hand, "
          f"KES {sum(r['on_hand']*r['cost'] for r in rows):,.0f} of stock")
    hdr = (f"  {'department':<22}{'skus':>5}{'obs cov':>9}{'policy':>8}"
           f"{'shelf':>7}{'R':>6}{'gap':>6}{'capital':>13}{'over shelf':>12}")
    print("\n" + hdr); print("  " + "-" * (len(hdr) - 2))
    by_d = defaultdict(list)
    for r in rows:
        by_d[r["dept"]].append(r)
    tot_excess = 0.0
    for dept, items in sorted(by_d.items(), key=lambda kv: -sum(
            x["on_hand"] * x["cost"] for x in kv[1])):
        cap = sum(x["on_hand"] * x["cost"] for x in items)
        gaps = [x["median_gap"] for x in items if x["median_gap"] is not None]
        sl = FRESH[dept]
        excess = sum(max(x["on_hand"] - x["d"] * sl, 0) * x["cost"] for x in items)
        tot_excess += excess
        print(f"  {dept:<22}{len(items):>5}"
              f"{st.median([x['obs_cover'] for x in items]):>9.1f}"
              f"{st.median([x['policy_cover'] for x in items]):>8.1f}"
              f"{sl:>7.1f}"
              f"{st.median([x['R'] for x in items]):>6.1f}"
              f"{(st.median(gaps) if gaps else float('nan')):>6.1f}"
              f"{cap:>13,.0f}{excess:>12,.0f}")
    print(f"  {'TOTAL':<22}{len(rows):>5}{'':>9}{'':>8}{'':>7}{'':>6}{'':>6}"
          f"{sum(r['on_hand']*r['cost'] for r in rows):>13,.0f}{tot_excess:>12,.0f}")

    print(f"\n  worst {a.top} fresh lines by capital held beyond shelf life")
    print(f"  {'sku':<44}{'dept':<14}{'obs':>7}{'pol':>6}{'shelf':>6}{'R':>5}{'gap':>5}{'excess KES':>12}")
    worst = sorted(rows, key=lambda r: -max(r["on_hand"] - r["d"] * r["shelf_life"], 0) * r["cost"])
    for r in worst[:a.top]:
        ex = max(r["on_hand"] - r["d"] * r["shelf_life"], 0) * r["cost"]
        g = r["median_gap"]
        print(f"  {r['sku'][:43]:<44}{r['dept'][:13]:<14}{r['obs_cover']:>7.1f}"
              f"{r['policy_cover']:>6.1f}{r['shelf_life']:>6.1f}{r['R']:>5.0f}"
              f"{(g if g is not None else -1):>5.0f}{ex:>12,.0f}")

    # how much of the policy's own cover is R alone?
    r_share = st.median([r["R"] / r["P"] for r in rows])
    gap_vs_R = [(r["median_gap"], r["R"]) for r in rows if r["median_gap"] is not None]
    agree = sum(1 for g, R in gap_vs_R if abs(g - R) <= 1)
    print(f"\n  R is {r_share:.0%} of the protection interval on the median fresh line")
    print(f"  observed delivery gap agrees with assumed R (+-1 day) on "
          f"{agree:,}/{len(gap_vs_R):,} lines ({agree/max(len(gap_vs_R),1):.1%})")
    print(f"  median observed gap {st.median([g for g,_ in gap_vs_R]):.1f} d "
          f"vs median assumed R {st.median([R for _,R in gap_vs_R]):.1f} d")

    print(json.dumps({
        "claim": "claim.ordering.fresh-cover-matches-shelf-life",
        "verdict": "contradicts",
        "metric": {
            "fresh_skus": len(rows),
            "fresh_capital": round(sum(r["on_hand"] * r["cost"] for r in rows)),
            "capital_beyond_shelf_life": round(tot_excess),
            "median_observed_cover": round(st.median([r["obs_cover"] for r in rows]), 2),
            "median_policy_cover": round(st.median([r["policy_cover"] for r in rows]), 2),
            "R_share_of_P": round(r_share, 3),
            "cadence_agrees_with_R_pct": round(100 * agree / max(len(gap_vs_R), 1), 1),
            "median_observed_gap_days": st.median([g for g, _ in gap_vs_R]),
            "mode": a.mode or ou.ordering_mode()},
        "held_out": False, "provenance": "observed",
        "sources": ["source.grn-book", "source.stock-snapshot", "source.corrected-ads"],
        "baseline": "policy cover as shipped", "beat_baseline": None,
        "traps": ["T1", "T3"],
        "notes": ("Delivery cadence measured from distinct GRN dates per vendor "
                  "rather than assumed. shelf_life_days is a parameter "
                  "clamp_level() already accepts and nothing populates.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
