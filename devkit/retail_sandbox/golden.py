"""Golden vectors: pin what the REAL engine answers, so drift cannot be silent.

Phase 3 of the extraction.

    python golden.py            # regenerate contract/golden_orders.json
    python golden.py --check    # exit 2 if production's answers have moved

The problem this solves
-----------------------
The sandbox carries a JavaScript reimplementation of the ordering rules. It was
written from the book, not ported from the engine, so the two can disagree - and
until now nothing would have said so. Worse, the engine has **zero required
fields** (contract/CONTRACT.md): feed it a thin record and it returns a
confident number through a fallback branch rather than an error. A wrong answer
and a right one look identical from the outside.

So the check cannot be "does it still run". It has to be "does it still return
the same number, for the same reason". That is what this file pins:

    record in  ->  target_days, qty at each of the three stages, fulfilment,
                   and the `reasoning` string

`reasoning` is load-bearing, not decoration. It is the only place the engine
says which branch it took. Two configurations can agree on 1,500 units where
one arrived via the DDoS target and the other via the ROP fallback; asserting
on quantity alone would call that a match.

What a failure means
--------------------
Not necessarily a bug. `--check` fails whenever production's answer moves, and
production is allowed to move. It means: someone changed an ordering rule, and
every sandbox conclusion drawn under the old vectors needs re-deriving before
it is quoted again. Re-run without `--check` to re-baseline, and say in the
commit what moved and why.

The vectors
-----------
The 58-line board from dataset.json - real SKUs, real costs, real suppliers -
crossed with five stock positions expressed in days of cover (0, 1, 3, 7, 21),
so every line is exercised on both sides of its reorder point. Plus the edge
cases from pin_contract.py's matrix, which is where the classification rules
(fresh vs long-life, dead stock, zero velocity) actually get decided.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
OUT = os.path.join(HERE, "contract")
GOLDEN = os.path.join(OUT, "golden_orders.json")
sys.path.insert(0, REPO)

#: Days of cover on hand. Spans both sides of every reorder point the engine
#: can compute, so the trigger itself is under test and not just the quantity.
COVER_POSITIONS = (0.0, 1.0, 3.0, 7.0, 21.0)

#: Fixed. current_day feeds the supplier order-day check, so a golden file
#: generated on a different day would differ for reasons that are not drift.
CURRENT_DAY = 40


def load_board():
    """The 58-line board, as engine records. Mirrors engine_bridge.mjs."""
    with open(os.path.join(HERE, "dataset.json"), encoding="utf-8") as f:
        data = json.load(f)
    skus = data["skus"]
    sup_by_name = {s["name"]: s for s in data["suppliers"]}
    rank = {s["id"]: i + 1
            for i, s in enumerate(sorted(skus, key=lambda x: -x["ads"]))}

    rows = []
    for s in skus:
        sup = sup_by_name.get(s["supplier"]) or {}
        lead = sup.get("leadMean", 2.3)
        for cover in COVER_POSITIONS:
            rows.append({
                "case": f"{s['id']} @ {cover:g}d",
                "sku_id": s["id"],
                "cover_days": cover,
                "record": {
                    "product_name": s["name"].upper(),
                    "avg_daily_sales": s["ads"],
                    "avg_daily_sales_last_30d": s["ads"],
                    "current_stocks": round(s["ads"] * cover, 2),
                    "on_order_qty": 0,
                    "estimated_delivery_days": max(1, round(lead)),
                    "supplier_name": s["supplier"].upper(),
                    "is_fresh": s["shelf"] <= 30,
                    "median_gap_days": 1.0,
                    "pack_size": s["pack"],
                    "department": s["dept"].upper(),
                    "product_category": s["dept"].upper(),
                    "selling_price": s["price"],
                    "cost_price": s["cost"],
                    "margin_pct": s["margin"],
                    "last_days_since_last_delivery": 0,
                    "total_units_sold_last_90d": round(s["ads"] * 90),
                    "sales_rank": rank[s["id"]],
                },
            })
    return rows


def edge_cases():
    """The classification edge cases, reused from the contract pin."""
    from pin_contract import MATRIX
    return [{"case": f"edge: {label}", "sku_id": None, "cover_days": None,
             "why": why, "record": dict(fields)}
            for label, why, fields in MATRIX]


def temp_data_dir():
    tmp = tempfile.mkdtemp(prefix="oasis_golden_")
    parent = os.path.join(tmp, "parent")
    data_dir = os.path.join(parent, "data")
    os.makedirs(data_dir, exist_ok=True)
    # data_dir/.. is the first place the calendar is looked for; without it the
    # calendar loads zero suppliers and every order-day check degrades.
    for f in ("supplier_rhythm_analysis.json", "supplier_weekly_schedule.json",
              "Supplier_Order_Calendar_2026.xlsx"):
        src = os.path.join(REPO, f)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(parent, f))
    return tmp, data_dir


def config_sha():
    p = os.path.join(REPO, "oasis", "data", "oasis_engines_config.json")
    return hashlib.sha256(open(p, "rb").read()).hexdigest() if os.path.exists(p) else None


def generate():
    """Run every vector through the whole live pipeline, one at a time.

    One at a time on purpose. The MOQ gate's second stage aggregates by
    supplier, so a line's fate would depend on which other lines happened to be
    in the same batch - the vector would stop being a property of the record.
    Batch behaviour is measured separately, in the harness.
    """
    from oasis.logic.order_engine import OrderEngine
    from oasis.logic.simulation_bridge import SimulationOrderUtil

    tmp, data_dir = temp_data_dir()
    eng = OrderEngine(data_dir)
    util = SimulationOrderUtil(data_dir, engine=eng)

    vectors = []
    for row in load_board() + edge_cases():
        rec = dict(row["record"])
        out = {"case": row["case"], "sku_id": row["sku_id"],
               "cover_days": row["cover_days"],
               "product_name": rec.get("product_name")}
        try:
            out["target_days"] = round(
                float(eng.calculate_replenishment_target_stock(dict(rec), {})), 4)
        except Exception as e:
            out["target_days"] = f"ERROR: {type(e).__name__}: {e}"

        try:
            enriched = util.prepare_sku_data([dict(rec)])
            raw = util.calculate_order_quantity(
                enriched, store_config={}, current_day=CURRENT_DAY)
            out["reorder_point"] = round(float(raw[0].get("reorder_point") or 0), 4)
            out["qty_raw"] = round(float(raw[0].get("recommended_quantity") or 0), 4)
            fin = util.finalize_orders(raw)
            out["qty_finalized"] = round(float(fin[0].get("recommended_quantity") or 0), 4)
            gate = util.apply_minimum_order_gate(list(fin))
            if gate.get("po_recs"):
                out["ordered_quantity"] = round(
                    float(gate["po_recs"][0].get("recommended_quantity") or 0), 4)
                out["fulfillment"] = "PO"
            elif gate.get("transfer_recs"):
                out["ordered_quantity"], out["fulfillment"] = 0.0, "TRANSFER_FIRST"
            else:
                out["ordered_quantity"], out["fulfillment"] = 0.0, "NONE"
            # the branch the engine took, which the quantity alone does not say
            out["reasoning"] = " ".join((fin[0].get("reasoning") or "").split())
        except Exception as e:
            out["error"] = f"{type(e).__name__}: {e}"
        vectors.append(out)

    shutil.rmtree(tmp, ignore_errors=True)
    return {
        "engines_config_sha256": config_sha(),
        "current_day": CURRENT_DAY,
        "cover_positions": list(COVER_POSITIONS),
        "count": len(vectors),
        "vectors": vectors,
    }


# ── comparison ────────────────────────────────────────────────────────────
#: Numbers are compared with a tolerance; `reasoning` and `fulfillment` must
#: match exactly, because they name the branch rather than measure it.
NUMERIC = ("target_days", "reorder_point", "qty_raw", "qty_finalized",
           "ordered_quantity")
EXACT = ("fulfillment", "reasoning")
TOL = 1e-6


def diff(pinned, live):
    """Every vector whose answer moved, and what moved in it."""
    by_case = {v["case"]: v for v in pinned["vectors"]}
    out = []
    for v in live["vectors"]:
        old = by_case.pop(v["case"], None)
        if old is None:
            out.append({"case": v["case"], "kind": "new vector"})
            continue
        moved = []
        for k in NUMERIC:
            a, b = old.get(k), v.get(k)
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                if abs(a - b) > max(TOL, abs(a) * 1e-9):
                    moved.append((k, a, b))
            elif a != b:
                moved.append((k, a, b))
        for k in EXACT:
            if old.get(k) != v.get(k):
                moved.append((k, old.get(k), v.get(k)))
        if moved:
            out.append({"case": v["case"], "kind": "changed", "moved": moved})
    for case in by_case:
        out.append({"case": case, "kind": "vector no longer generated"})
    return out


def check():
    if not os.path.exists(GOLDEN):
        print("no golden file pinned yet - run without --check first")
        return 1
    with open(GOLDEN, encoding="utf-8") as f:
        pinned = json.load(f)
    live = generate()

    cfg_moved = pinned.get("engines_config_sha256") != live.get("engines_config_sha256")
    d = diff(pinned, live)
    if not d:
        print(f"OK  {live['count']} golden vectors reproduce exactly")
        if cfg_moved:
            print("    note: engines config hash changed but no answer moved")
        return 0

    print(f"DRIFT: {len(d)} of {live['count']} golden vectors no longer match.\n")
    if cfg_moved:
        print("  the engines config also changed - that is the likely cause")
        print(f"    pinned {pinned.get('engines_config_sha256', '?')[:16]}"
              f"  ->  live {live.get('engines_config_sha256', '?')[:16]}\n")
    for row in d[:25]:
        if row["kind"] != "changed":
            print(f"  {row['case']:<34} {row['kind']}")
            continue
        for k, a, b in row["moved"]:
            if k == "reasoning":
                print(f"  {row['case']:<34} reasoning:")
                print(f"      was {a}")
                print(f"      now {b}")
            else:
                print(f"  {row['case']:<34} {k}: {a} -> {b}")
    if len(d) > 25:
        print(f"  ... and {len(d) - 25} more")
    print("\n  Production's ordering answers have moved. That may be correct - but")
    print("  every sandbox conclusion drawn against the old vectors needs")
    print("  re-deriving before it is quoted. Re-run `python golden.py` to")
    print("  re-baseline, and record what moved and why.")
    return 2


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true",
                    help="fail if production's answers have moved from the pin")
    a = ap.parse_args()
    if a.check:
        return check()

    os.makedirs(OUT, exist_ok=True)
    data = generate()
    with open(GOLDEN, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1, default=str)

    ordered = sum(1 for v in data["vectors"] if v.get("fulfillment") == "PO")
    transfer = sum(1 for v in data["vectors"] if v.get("fulfillment") == "TRANSFER_FIRST")
    errs = sum(1 for v in data["vectors"] if v.get("error"))
    print(f"wrote {os.path.relpath(GOLDEN, REPO)}")
    print(f"  {data['count']} vectors   PO {ordered}   TRANSFER_FIRST {transfer}"
          f"   no order {data['count'] - ordered - transfer}"
          + (f"   ERRORS {errs}" if errs else ""))
    print(f"  config sha256 {(data['engines_config_sha256'] or '?')[:16]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
