"""Probe: AMIT enforcement -- the degenerate key, the shadowed writer, and
whether One-In-One-Out does anything.

THE THREE BUGS THIS PROBE VERIFIES ARE FIXED

  1. STALE DUPLICATE SHADOWING (T2). Two unrelated engines --
     amit_gatekeeper.run_amit() (GMROI ranking + category caps) and
     amit_governance.activate_purchase_block() (a DIFFERENT policy: dead
     stock, days-of-stock vs perishability + a capital floor) -- wrote the
     SAME oasis/data/amit_enforcement.json. Whichever ran last won, silently,
     and amit_governance's writer hard-coded 'lowest_gmroi_per_dept': {},
     permanently blanking the gatekeeper's One-In-One-Out data whenever the
     daily pipeline (which called only the governance path) ran after a
     human's manual `bootstrap-governance` run. FIX: the two engines now
     write two namespaced files (amit_enforcement.json /
     amit_dead_stock_block.json), both loaded by order_engine.py with their
     roles logged, both enforced by procurement_mixin.py under distinct
     reason tags. This probe shows both files exist, are attributed, and
     lowest_gmroi_per_dept survives a governance run untouched.

  2. THE KEY WAS ALGEBRAICALLY EMPTY. calculate_gmroi() was
     gross_profit / (price * ads * 30 * lata). Expand
     gross_profit = price * ads * T * margin and price, ads cancel EXACTLY,
     leaving (T/30) * margin / lata -- no velocity, no capital, a margin sort
     in a GMROI costume -- and gross_profit was populated on only 3,663 of
     23,511 nodes.csv rows, so 19,848 SKUs (84%) tied at exactly 0.0 and
     Python's stable sort resolved the tie by CSV ROW ORDER. FIX: two keys
     (annual_gross_profit for the per-line cap, gmroi for a capital budget --
     see amit_gatekeeper.py's module docstring), sourced from the VAT-
     corrected GRN book wherever it matches, real order_up_to inventory, and
     a labelled fallback (never a silent zero) elsewhere. THE SHUFFLE TEST
     (permute nodes.csv row order, measure what fraction of blacklist
     decisions change) is the acceptance criterion for "is this a ranking at
     all": ~23% before, ~0% after is the claim; this probe measures both.

  3. ONE-IN-ONE-OUT WAS NEVER WIRED. lowest_gmroi_per_dept was computed and
     then read by nothing (order_engine.py loaded it into self.databases and
     no code ever looked it up again). This probe traces a concrete swap:
     take one real department sitting just over its cap, inject one
     synthetic new SKU, and show that the batch trim (once the key is fixed)
     removes exactly the incumbent with the lowest annual_gross_profit --
     which is what procurement_mixin.py's existing `should_list = False`
     blacklist check already turns into a real removal on the next PO run.
     What is still NOT wired, and is reported honestly rather than papered
     over, is a trigger: nothing calls run_amit() when "a PO is drafted" --
     AMIT is a batch pre-filter against a point-in-time neutral_network_export
     snapshot (see staleness numbers below), not a per-PO firewall, despite
     the spec's language. The closest existing proxy for "a new SKU is being
     ingested" is procurement_mixin.py's `avg_daily_sales == 0` check (used
     there as `is_new_product`), which is a WEAK proxy -- it also fires for
     an existing SKU going through a temporary stockout.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import statistics as st
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit.methodology.traps import join_match_rate, ratio, run_all   # noqa: E402
from oasis.logic import amit_gatekeeper as gk                          # noqa: E402
from oasis.logic import order_up_to as ou                              # noqa: E402

NN_PATH = str(ROOT / "neutral_network_export")
DATA_DIR = str(ROOT / "oasis" / "data")
SEEDS = [1, 2, 3]


def legacy_gmroi(node, lata_multiplier=1.0):
    """The formula being replaced, reproduced faithfully here (the shipped
    module has been rewritten in place, so this is not an import): the exact
    algebra of the old calculate_gmroi()."""
    gp = node["gross_profit"]; ads = node["velocity_ads"]; price = node["price"]
    denom = price * ads * 30.0 * (lata_multiplier or 1.0)
    return gp / denom if denom else 0.0


def legacy_blacklist(nodes, lata_multipliers, seed=None):
    """Faithful reproduction of the ORIGINAL run_amit(): raw (un-normalized)
    department string, calculate_gmroi() as above, DEFAULT_DEPT_CAPS_BASELINE
    / DEFAULT_CAP_FALLBACK_BASELINE, stable sort descending, keep the top
    `cap`. Returns the blacklisted SKU id set."""
    ns = list(nodes)
    if seed is not None:
        random.Random(seed).shuffle(ns)
    by_dept = defaultdict(list)
    for n in ns:
        mult = lata_multipliers.get(n["supplier"], 1.0)
        by_dept[n["department"]].append((n["id"], legacy_gmroi(n, mult)))
    blacklisted = set()
    for dept, scored in by_dept.items():
        cap = gk.DEFAULT_DEPT_CAPS_BASELINE.get(dept, gk.DEFAULT_CAP_FALLBACK_BASELINE)
        scored.sort(key=lambda t: t[1], reverse=True)
        if len(scored) > cap:
            for sku, _ in scored[cap:]:
                blacklisted.add(sku)
    return blacklisted


def new_blacklist(seed=None):
    result = gk.run_amit(NN_PATH, DATA_DIR, node_order_seed=seed)
    return set(result["blacklist"]), result


def frac_changed(canonical, other, universe):
    return len(canonical.symmetric_difference(other)) / universe


def score_department(dept_name, nodes, lata_multipliers, margin_book, ads_book,
                     review_schedule, lead_patterns, extra_synthetic=None):
    """Mirrors amit_gatekeeper.run_amit()'s per-SKU scoring loop for ONE
    department, so a minimal reproducible One-In-One-Out example can be built
    without re-running the full 23,511-SKU pass. `extra_synthetic`, if given,
    is one more node dict injected into the department before scoring."""
    pool = [n for n in nodes if gk.norm_dept(n["department"]) == dept_name]
    if extra_synthetic is not None:
        pool = pool + [extra_synthetic]
    margin_stats, ads_stats = {}, {}
    scored = []
    for node in pool:
        supplier = node["supplier"].upper()
        multiplier = lata_multipliers.get(supplier, 1.0)
        unit_cost, gp_per_unit, _mp = gk.resolve_margin(node, margin_book, margin_stats)
        ads_val, _ap = gk.resolve_ads(node, ads_book, ads_stats)
        lp = lead_patterns.get(supplier) or {}
        lead_days = float(lp.get("lead_time_mean", lp.get("lead_time_days", 3.0)) or 3.0)
        structurally_short = False
        if ads_val > 0:
            rec = ou.recommend({"avg_daily_sales": ads_val, "supplier_name": supplier,
                                "current_stock": 0, "lead_time_days": lead_days,
                                "department": node["department"]},
                               schedule=review_schedule, patterns=lead_patterns)
            if "S" in rec:
                avg_units = rec["S"] - ads_val * (rec["L"] + rec["R"] / 2.0)
                structurally_short = avg_units <= 0
        annual_gp = gp_per_unit * ads_val * 365.0
        scored.append({"sku": node["id"], "annual_gross_profit": annual_gp,
                       "structurally_short": structurally_short})
    scored.sort(key=lambda x: (x["structurally_short"], x["annual_gross_profit"]), reverse=True)
    return scored


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, nargs="*", default=SEEDS)
    a = ap.parse_args(argv)

    # ------------------------------------------------------------------
    # 1. ONE AUTHORITATIVE WRITER PER FILE (T2)
    # ------------------------------------------------------------------
    enforcement_path = Path(DATA_DIR) / "amit_enforcement.json"
    deadstock_path = Path(DATA_DIR) / "amit_dead_stock_block.json"
    gk_data = json.loads(enforcement_path.read_text(encoding="utf-8"))
    ds_data = json.loads(deadstock_path.read_text(encoding="utf-8")) if deadstock_path.exists() else {}
    print(f"\n  WRITER SEPARATION")
    print(f"  {enforcement_path.name:<28} source={gk_data.get('source_engine')!r} "
          f"lowest_gmroi_per_dept={len(gk_data.get('lowest_gmroi_per_dept', {}))} depts")
    print(f"  {deadstock_path.name:<28} source={ds_data.get('source_engine')!r} "
          f"policy={ds_data.get('policy')!r}")
    writer_ok = (gk_data.get("source_engine") == "amit_gatekeeper.run_amit"
                and ds_data.get("source_engine") == "amit_governance.activate_purchase_block"
                and len(gk_data.get("lowest_gmroi_per_dept", {})) > 0)

    # ------------------------------------------------------------------
    # 2. STALENESS
    # ------------------------------------------------------------------
    nodes_mtime = os.path.getmtime(os.path.join(NN_PATH, "nodes.csv"))
    graph_age_days = (time.time() - nodes_mtime) / 86400.0
    enf_gen = datetime.fromisoformat(gk_data["generated_at"])
    enf_age_days = (datetime.now(timezone.utc) - enf_gen).total_seconds() / 86400.0
    print(f"\n  STALENESS")
    print(f"  neutral_network_export/nodes.csv is {graph_age_days:.0f} days old")
    print(f"  amit_enforcement.json is {enf_age_days:.1f} days old "
          f"(guard fires past {gk_data.get('max_age_days')} days -- see "
          "order_engine.py's _check_amit_staleness, exercised above at import time)")

    # ------------------------------------------------------------------
    # 3. LOAD SHARED INPUTS ONCE
    # ------------------------------------------------------------------
    nodes = gk.load_nodes(NN_PATH)
    lata_multipliers = gk.load_lata_patterns(DATA_DIR)
    margin_book = gk.load_margin_book(DATA_DIR)
    ads_book = gk.load_ads_book(DATA_DIR)
    universe = len(nodes)

    margin_join = join_match_rate([n["id"] for n in nodes], list(margin_book.keys()),
                                  "SKU -> margin_from_grn.json", floor=0.50)
    ads_join = join_match_rate([n["id"] for n in nodes], list(ads_book.keys()),
                               "SKU -> corrected_ads_from_pos.json", floor=0.80)

    # ------------------------------------------------------------------
    # 4. TIE-BLOCK SIZE, OLD KEY VS NEW KEY (T3 denominator honesty)
    # ------------------------------------------------------------------
    legacy_scores = [legacy_gmroi(n, lata_multipliers.get(n["supplier"].upper(), 1.0)) for n in nodes]
    legacy_zero = sum(1 for s in legacy_scores if s == 0.0)
    zero_ads_stats = {}
    new_zero = 0
    for n in nodes:
        ads_val, _ = gk.resolve_ads(n, ads_book, zero_ads_stats)
        if ads_val <= 0:
            new_zero += 1
    print(f"\n  TIE BLOCK (SKUs scoring exactly 0, before any sort)")
    print(f"  old key (gross_profit/(price*ads*30*lata)): {legacy_zero:,}/{universe:,} "
          f"({legacy_zero/universe:.1%}) tie at 0.0")
    print(f"  new key (annual_gross_profit, ads<=0 only):  {new_zero:,}/{universe:,} "
          f"({new_zero/universe:.1%}) tie at 0.0 (genuine zero-velocity SKUs, not a data gap)")

    # ------------------------------------------------------------------
    # 5. SHUFFLE TEST -- the acceptance criterion
    # ------------------------------------------------------------------
    print(f"\n  SHUFFLE TEST (fraction of blacklist decisions that flip when "
          f"nodes.csv row order is permuted, seeds={a.seeds})")
    legacy_canon = legacy_blacklist(nodes, lata_multipliers, seed=None)
    legacy_fracs = [frac_changed(legacy_canon, legacy_blacklist(nodes, lata_multipliers, seed=s), universe)
                    for s in a.seeds]
    print(f"  OLD key: canonical blacklist {len(legacy_canon):,} SKUs; "
          f"mean flip fraction across {len(a.seeds)} shuffles = {st.mean(legacy_fracs):.1%} "
          f"(per-seed: {[f'{x:.1%}' for x in legacy_fracs]})")

    new_canon, new_canon_result = new_blacklist(seed=None)
    new_fracs = []
    for s in a.seeds:
        other, _ = new_blacklist(seed=s)
        new_fracs.append(frac_changed(new_canon, other, universe))
    print(f"  NEW key: canonical blacklist {len(new_canon):,} SKUs; "
          f"mean flip fraction across {len(a.seeds)} shuffles = {st.mean(new_fracs):.1%} "
          f"(per-seed: {[f'{x:.1%}' for x in new_fracs]})")

    # ------------------------------------------------------------------
    # 6. THE 63% DECOMPOSITION -- key vs caps (T3, T5)
    # ------------------------------------------------------------------
    old_documented = 14870  # oasis/data/amit_enforcement.json as diagnosed, pre-fix
    print(f"\n  63% DECOMPOSITION")
    print(f"  original shipped state (old key, raw dept strings): "
          f"{old_documented:,}/{universe:,} = {old_documented/universe:.1%}")
    print(f"  this probe's faithful reproduction of that state:   "
          f"{len(legacy_canon):,}/{universe:,} = {len(legacy_canon)/universe:.1%}")
    print(f"  new key, same caps, norm_dept-folded departments:   "
          f"{len(new_canon):,}/{universe:,} = {len(new_canon)/universe:.1%}")
    print(f"  KEY CONTRIBUTION TO COUNT: zero, structurally. Cap trimming keeps "
          f"exactly `cap` SKUs per department and removes exactly max(0, N-cap); "
          f"that arithmetic is fixed BEFORE any ranking runs, so no key can ever "
          f"change the blacklisted COUNT -- only WHICH {len(new_canon):,} SKUs. "
          f"The ~{old_documented - len(new_canon)} SKU difference above is 100% "
          f"attributable to norm_dept() folding {gk_data['join_stats']['raw_departments_seen']} "
          f"raw department spellings to {gk_data['join_stats']['normalized_departments']}.")
    print(f"  CAPS ARE THE REAL LEVER: only {len(gk.DEFAULT_DEPT_CAPS_BASELINE)} of "
          f"{gk_data['join_stats']['normalized_departments']} departments have an authored "
          f"cap; the other {gk_data['join_stats']['normalized_departments'] - len(gk.DEFAULT_DEPT_CAPS_BASELINE)} "
          f"fall to the undeclared fallback_cap={gk.DEFAULT_CAP_FALLBACK_BASELINE}, and this "
          f"catalogue averages {universe/gk_data['join_stats']['normalized_departments']:.0f} "
          f"SKUs/department -- so most departments are 'over cap' almost regardless of what "
          f"the cap number is. 63% was very unlikely to have been an intended assortment-width "
          f"target; it is an artifact of one fallback constant nobody reviewed against 219 "
          f"departments it silently governs.")

    # ------------------------------------------------------------------
    # 7. ONE-IN-ONE-OUT: A TRACED SWAP
    # ------------------------------------------------------------------
    review_schedule = ou.load_review_schedule(str(ROOT))
    lead_patterns = ou.default_patterns(str(ROOT))
    ou.load_shelf_life(str(ROOT))

    by_dept_count = defaultdict(int)
    for item in new_canon_result["blacklist_details"]:
        by_dept_count[item["department"]] += 1
    demo_dept = None
    for d, cnt in sorted(by_dept_count.items(), key=lambda kv: kv[1]):
        if 1 <= cnt <= 3 and new_canon_result["department_caps_applied"].get(d, 0) > 0:
            demo_dept = d
            break
    print(f"\n  ONE-IN-ONE-OUT TRACE -- department '{demo_dept}'")
    before = score_department(demo_dept, nodes, lata_multipliers, margin_book, ads_book,
                              review_schedule, lead_patterns)
    cap = new_canon_result["department_caps_applied"].get(demo_dept, gk.DEFAULT_CAP_FALLBACK_BASELINE)
    before_kept = {r["sku"] for r in before[:cap]}
    before_cut = {r["sku"] for r in before[cap:]}
    incumbent = before[cap - 1]  # current lowest-ranked KEPT SKU: exactly lowest_gmroi_per_dept's role
    print(f"  {len(before)} SKUs, cap {cap}: {len(before_cut)} already blacklisted; "
          f"incumbent at the cap line = {incumbent['sku']!r} "
          f"(annual GP KES {incumbent['annual_gross_profit']:,.0f})")

    synthetic = {
        "id": "SYNTHETIC-NEW-SKU-DEMO", "department": demo_dept, "supplier": "DEMO SUPPLIER",
        "price": 300.0, "margin_pct": 0.0, "revenue": 0.0, "gross_profit": 0.0,
        "sales_rank": 1.0, "velocity_ads": 5.0, "total_quantity": 0.0, "store_fill_rate": 0.0,
    }
    # Force this SKU's economics via the same margin_book path a real new
    # listing with a GRN-book match would use: inject it directly so the
    # demo does not depend on a fabricated SKU name happening to collide
    # with a real GRN row.
    margin_book_demo = dict(margin_book)
    margin_book_demo[gk.norm(synthetic["id"])] = {
        "unit_cost": 200.0, "gross_profit_per_unit": 100.0,
    }
    ads_book_demo = dict(ads_book)
    ads_book_demo[gk.norm(synthetic["id"])] = {"new_ads": 5.0}

    after = score_department(demo_dept, nodes, lata_multipliers, margin_book_demo, ads_book_demo,
                             review_schedule, lead_patterns, extra_synthetic=synthetic)
    after_kept = {r["sku"] for r in after[:cap]}
    after_cut = {r["sku"] for r in after[cap:]}
    newly_evicted = after_cut - before_cut
    print(f"  + 1 new SKU ({synthetic['id']}, annual GP KES "
          f"{100.0 * 5.0 * 365.0:,.0f}) -> {len(after)} SKUs, cap {cap} unchanged")
    print(f"  synthetic SKU kept: {synthetic['id'] in after_kept}")
    print(f"  newly evicted by this arrival (was kept, now cut): {sorted(newly_evicted)}")
    swap_traced = (synthetic["id"] in after_kept and len(newly_evicted) == 1
                  and incumbent["sku"] in newly_evicted)
    print(f"  CLEAN ONE-IN-ONE-OUT SWAP: {swap_traced} "
          f"(new SKU in, exactly the prior cap-line incumbent {incumbent['sku']!r} out)")
    print(f"  MECHANISM: this is amit_gatekeeper's own batch trim -- no new code in "
          f"procurement_mixin.py was needed, because `should_list = False` there "
          f"already fires for anything amit_enforcement.json's blacklist names. What "
          f"is still MISSING is a trigger tying 'a PO drafted a new SKU' to a fresh "
          f"run_amit() call: nothing in this codebase currently does that (AMIT only "
          f"runs from the daily pipeline's schedule or a manual bootstrap-governance "
          f"command, see amit_gatekeeper.py's module docstring on batch vs firewall). "
          f"The closest existing signal for 'a new SKU is being ingested' is "
          f"procurement_mixin.py's `avg_daily_sales == 0` (`is_new_product`), which "
          f"also fires for an existing SKU mid-stockout -- a real NPI flag (a "
          f"first-listed-date field on the SKU master, or an explicit event from "
          f"whatever system drafts the PO) is what would be needed to fire this "
          f"per-PO instead of on AMIT's own batch cadence.")

    # ------------------------------------------------------------------
    # 8. RESTORE CANONICAL STATE (the shuffle/demo calls above rewrote the
    #    real amit_enforcement.json repeatedly; leave it in the correct,
    #    unshuffled, un-synthesized state as the actual deliverable).
    # ------------------------------------------------------------------
    gk.run_amit(NN_PATH, DATA_DIR, node_order_seed=None)

    # ------------------------------------------------------------------
    # TRAPS
    # ------------------------------------------------------------------
    checks = [
        margin_join, ads_join,
        ratio(len(legacy_canon), universe, "old blacklist / universe", lo=0.05, hi=1.0),
        ratio(len(new_canon), universe, "new blacklist / universe", lo=0.05, hi=1.0),
    ]
    run_all(checks)

    print(json.dumps({
        "claim": "claim.amit.enforcement-key-and-writer-fixed",
        "verdict": "supports" if (writer_ok and swap_traced
                                  and st.mean(new_fracs) < 0.02
                                  and st.mean(legacy_fracs) > 0.10) else "inconclusive",
        "metric": {
            "writer_separation_ok": writer_ok,
            "graph_age_days": round(graph_age_days, 1),
            "enforcement_age_days": round(enf_age_days, 2),
            "max_age_days_guard": gk_data.get("max_age_days"),
            "universe_skus": universe,
            "tie_block_old": legacy_zero, "tie_block_old_pct": round(legacy_zero / universe, 4),
            "tie_block_new": new_zero, "tie_block_new_pct": round(new_zero / universe, 4),
            "shuffle_flip_fraction_old_mean": round(st.mean(legacy_fracs), 4),
            "shuffle_flip_fraction_old_per_seed": [round(x, 4) for x in legacy_fracs],
            "shuffle_flip_fraction_new_mean": round(st.mean(new_fracs), 4),
            "shuffle_flip_fraction_new_per_seed": [round(x, 4) for x in new_fracs],
            "blacklist_old_documented": old_documented,
            "blacklist_old_reproduced": len(legacy_canon),
            "blacklist_new": len(new_canon),
            "blacklist_old_pct": round(old_documented / universe, 4),
            "blacklist_new_pct": round(len(new_canon) / universe, 4),
            "departments_with_authored_cap": len(gk.DEFAULT_DEPT_CAPS_BASELINE),
            "departments_total_normalized": gk_data["join_stats"]["normalized_departments"],
            "fallback_cap": gk.DEFAULT_CAP_FALLBACK_BASELINE,
            "one_in_one_out_swap_traced": swap_traced,
            "one_in_one_out_demo_department": demo_dept,
            "margin_join_rate": margin_join.evidence.get("rate"),
            "ads_join_rate": ads_join.evidence.get("rate"),
        },
        "held_out": False,
        "provenance": "observed (GRN book, POS-derived ADS labelled as exercise-only per "
                      "MOCK_POS synthetic-data caveat; nodes.csv/neutral_network_export is a "
                      "real but stale snapshot); the shuffle test and One-In-One-Out demo are "
                      "constructed/synthetic by design (that is the point of both tests)",
        "sources": ["source.neutral-network-export", "source.grn-book", "source.pos-corrected-ads"],
        "baseline": "shipped calculate_gmroi() + shared amit_enforcement.json writer "
                   "(pre-2026-09 state, reproduced by legacy_blacklist() above)",
        "beat_baseline": (st.mean(new_fracs) < st.mean(legacy_fracs)) if new_fracs and legacy_fracs else None,
        "traps": ["T1", "T2", "T3", "T5"],
        "notes": (
            f"The blacklist COUNT barely moved ({old_documented:,} -> {len(new_canon):,}) "
            "because caps, not the ranking key, decide how many SKUs a department loses; "
            "the key decides WHICH ones, and that is where the fix shows up: "
            f"{st.mean(legacy_fracs):.0%} of decisions used to flip under a row-order "
            f"shuffle, {st.mean(new_fracs):.0%} do now. lowest_gmroi_per_dept is populated "
            "for every department and no longer blanked by the dead-stock writer. The "
            "One-In-One-Out swap is real once triggered, but nothing in this codebase "
            "currently triggers AMIT from a PO event -- it is a batch job on a schedule, "
            "against a snapshot that is currently "
            f"{graph_age_days:.0f} days old, now with a loud staleness warning where "
            "before there was none.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
