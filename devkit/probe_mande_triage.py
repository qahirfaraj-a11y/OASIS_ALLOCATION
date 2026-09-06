"""
MANDE triage probe -- independent re-derivation of the ablation-loss vs
capital-release trade, the purge-list revenue reality, the substitution-edge
artifact, and the returns-vs-SEI overlap.

Run from repo root:  python3 devkit/probe_mande_triage.py
Reads only; writes nothing. No engine files touched.
"""
import csv, json, glob, os
from collections import defaultdict, Counter

DATA = "oasis/data"
NN = "neutral_network_export"

def load_mande():
    return json.load(open(os.path.join(DATA, "mande_purge_report.json")))

def load_nodes():
    sku_supplier, sku_dept, sku_rev, sku_gp = {}, {}, {}, {}
    with open(os.path.join(NN, "nodes.csv")) as f:
        for row in csv.DictReader(f):
            if row["type"] != "SKU":
                continue
            sid = row["id"]
            sku_supplier[sid] = row.get("supplier", "Unknown").strip("[]").strip().upper()
            sku_dept[sid] = row.get("department", "").strip("[]").strip().upper()
            sku_rev[sid] = float(row.get("revenue", 0) or 0)
            sku_gp[sid] = float(row.get("gross_profit", 0) or 0)
    return sku_supplier, sku_dept, sku_rev, sku_gp

def section(title):
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)

def q_a_trade(mande):
    section("Q A -- Trade defensibility (capital release vs GP loss)")
    reported_capital = mande["summary"]["total_capital_release_potential"]
    ablation_gp_loss = 141_815_198 - 121_536_658
    print(f"reported one-off capital release   : {reported_capital:,.0f} KES")
    print(f"measured ANNUAL GP loss (ablation)  : {ablation_gp_loss:,.0f} KES/yr")
    print(f"multiple (annual loss / one-off gain): {ablation_gp_loss/reported_capital:.2f}x")
    print("Net position Y1  = capital - 1yr loss =", f"{reported_capital - ablation_gp_loss:,.0f} KES")
    print("Net position Y2  = capital - 2yr loss =", f"{reported_capital - 2*ablation_gp_loss:,.0f} KES")
    print("There is no payback period: the release is one-off, the loss recurs every year.")

def q_b_c_purge_reality(mande, sku_supplier, sku_rev, sku_gp):
    section("Q B/C -- What's actually in the purge list (nodes.csv basis)")
    pc = mande["purge_candidates"]
    zero = [s for s in pc if s["total_revenue"] == 0]
    near_zero = [s for s in pc if 0 < s["total_revenue"] < 1000]
    live = [s for s in pc if s["total_revenue"] >= 1000]
    print(f"purge candidates: {len(pc)}  zero-rev: {len(zero)} ({len(zero)/len(pc):.1%})  "
          f"near-zero(<1000): {len(near_zero)} ({len(near_zero)/len(pc):.1%})  "
          f"live(>=1000): {len(live)} ({len(live)/len(pc):.1%})")
    days = [s["net_capital_position_improvement_days"] for s in pc]
    days_nonzero = [s["net_capital_position_improvement_days"] for s in pc if s["total_revenue"] > 0]
    print(f"avg 'days improvement' reported (all 300)     : {sum(days)/len(days):,.1f}")
    print(f"avg 'days improvement' (nonzero-revenue only) : {sum(days_nonzero)/len(days_nonzero):,.1f}")
    print("net_capital_position_improvement_days = trapped_capital / max(revenue/365, 1.0)")
    print("-> for revenue==0, this floors to trapped_capital itself, mislabelled as 'days' (T3).")

    section("Q B -- Purge-listed SKU revenue/GP: nodes.csv vs POS-corrected + VAT-corrected margin")
    purge_set = {s["supplier"].strip().upper() for s in pc}
    purge_skus = [sid for sid, sup in sku_supplier.items() if sup in purge_set]
    ads = json.load(open(os.path.join(DATA, "corrected_ads_from_pos.json")))
    margin = json.load(open(os.path.join(DATA, "margin_from_grn.json")))
    in_ads = sum(1 for s in purge_skus if s in ads)
    in_margin = sum(1 for s in purge_skus if s in margin)
    print(f"purge-listed live SKUs (by supplier membership): {len(purge_skus)}")
    print(f"join rate to corrected_ads_from_pos.json : {in_ads}/{len(purge_skus)} ({in_ads/len(purge_skus):.1%})")
    print(f"join rate to margin_from_grn.json        : {in_margin}/{len(purge_skus)} ({in_margin/len(purge_skus):.1%})")

    rev_c, gp_c, n = 0.0, 0.0, 0
    rev_n = sum(sku_rev[s] for s in purge_skus)
    gp_n = sum(sku_gp[s] for s in purge_skus)
    for sid in purge_skus:
        a, m = ads.get(sid), margin.get(sid)
        if a and m:
            units = (a.get("new_ads", 0) or 0) * 365
            rev_c += units * (m.get("selling_price", 0) or 0)
            gp_c += units * (m.get("gross_profit_per_unit", 0) or 0)
            n += 1
    print(f"nodes.csv-basis   : revenue={rev_n:,.0f}  GP={gp_n:,.0f}  (what MANDE 'sees')")
    print(f"POS+GRN-corrected : revenue={rev_c:,.0f}  GP={gp_c:,.0f}  (n={n} joined SKUs)  (what's real)")
    print(f"understatement factor: revenue {rev_c/max(rev_n,1):.1f}x, GP {gp_c/max(gp_n,1):.1f}x")

    # sanity check: same method applied chain-wide should reconcile with ablation baseline GP/yr
    all_sku_ids = set(sku_supplier)
    tot_gp_all, n_all = 0.0, 0
    for sid in all_sku_ids:
        a, m = ads.get(sid), margin.get(sid)
        if a and m:
            units = (a.get("new_ads", 0) or 0) * 365
            tot_gp_all += units * (m.get("gross_profit_per_unit", 0) or 0)
            n_all += 1
    print(f"\nsanity check: same corrected method, WHOLE assortment -> GP/yr={tot_gp_all:,.0f} "
          f"(n={n_all}); ablation baseline GP/yr=141,815,198; ratio={tot_gp_all/141_815_198:.3f}")

def q_d_substitution(sku_supplier, sku_dept):
    section("Q D -- Substitution edges: real signal or category-fanout artifact?")
    sub_count = defaultdict(int)
    with open(os.path.join(NN, "edges.csv")) as f:
        for row in csv.DictReader(f):
            if row["relation"] == "substitution":
                sub_count[row["source"]] += 1
    dist = Counter(sub_count.values())
    print("distribution of substitution-edge count per SKU:", sorted(dist.items()))
    exactly5 = dist.get(5, 0)
    print(f"SKUs with EXACTLY 5 substitution edges: {exactly5}/{sum(dist.values())} "
          f"({exactly5/sum(dist.values()):.1%})")
    dept_size = Counter(sku_dept.values())
    for sid in ["SANTA DIGNA 750ML SAUV BLANC RESERVA", "CHAINKWO 632ML MUSHROOM SOY SAUCE"]:
        if sid in sku_dept:
            print(f"  {sid}: dept={sku_dept[sid]} (size={dept_size[sku_dept[sid]]}), "
                  f"n_subs={sub_count.get(sid)}")
    print("-> a 53-SKU department and a 915-SKU department both produce exactly 5 edges/SKU:")
    print("   the count is a near-constant, not a measure of category size or interchangeability.")

def q_e_returns(sku_supplier):
    section("Q E -- What the prts_*.xlsx returns data actually says")
    agg = defaultdict(lambda: {"n": 0, "net_amt": 0.0, "reasons": Counter()})
    dates = []
    files = sorted(glob.glob(os.path.join(DATA, "prts_*.xlsx")))
    import openpyxl
    for fp in files:
        wb = openpyxl.load_workbook(fp, read_only=True, data_only=True)
        ws = wb.active
        header = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
        hmap = {str(h).strip().lower(): i for i, h in enumerate(header) if h}
        c_ven, c_date = hmap.get("ven code / name"), hmap.get("doc date")
        c_amt, c_reason = hmap.get("net amt"), hmap.get("reason")
        for row in ws.iter_rows(min_row=2, values_only=True):
            v = row[c_ven] if c_ven is not None else None
            if not v or str(v).strip().upper() == "TOTAL":
                continue
            v = str(v)
            supplier = v.split(" - ", 1)[1].upper().strip() if " - " in v else v.upper().strip()
            reason = str(row[c_reason]).upper() if c_reason is not None and row[c_reason] else "UNKNOWN"
            agg[supplier]["n"] += 1
            agg[supplier]["net_amt"] += float(row[c_amt] or 0.0)
            agg[supplier]["reasons"][reason] += 1
            if row[c_date]:
                dates.append(row[c_date])
        wb.close()
    print(f"files: {len(files)}  rows: {sum(v['n'] for v in agg.values())}  suppliers: {len(agg)}")
    print(f"date range: {min(dates)} .. {max(dates)}")
    all_reasons = Counter()
    for v in agg.values():
        all_reasons.update(v["reasons"])
    print("reason counts:", dict(all_reasons.most_common()))

    mande = load_mande()
    purge_set = {s["supplier"].strip().upper() for s in mande["purge_candidates"]}
    universe = {s["supplier"].strip().upper() for s in mande["all_suppliers"]}
    matched = sum(1 for k in agg if k in universe)
    print(f"join rate: prts suppliers found in nodes.csv supplier universe: "
          f"{matched}/{len(agg)} ({matched/len(agg):.1%})")

    by_expiry = sorted(agg.items(), key=lambda kv: -kv[1]["reasons"].get("EXPIRY ITEM", 0))
    print("\nTop 5 suppliers by EXPIRY-return count (the honest quality-risk signal):")
    for k, v in by_expiry[:5]:
        print(f"  {k:40s} expiry={v['reasons'].get('EXPIRY ITEM',0):5d}  "
              f"net_amt={v['net_amt']:>12,.0f}  IN_MANDE_PURGE={'YES' if k in purge_set else 'no'}")

    for topk in (20, 30, 50, 100):
        top = {k for k, v in by_expiry[:topk]}
        ov = len(top & purge_set)
        print(f"overlap: top-{topk} by expiry-count vs MANDE purge list: {ov}/{topk} ({ov/topk:.1%})")
    print("-> a returns-based ranking would flag substantially DIFFERENT suppliers than SEI does.")

if __name__ == "__main__":
    mande = load_mande()
    sku_supplier, sku_dept, sku_rev, sku_gp = load_nodes()
    q_a_trade(mande)
    q_b_c_purge_reality(mande, sku_supplier, sku_rev, sku_gp)
    q_d_substitution(sku_supplier, sku_dept)
    q_e_returns(sku_supplier)
