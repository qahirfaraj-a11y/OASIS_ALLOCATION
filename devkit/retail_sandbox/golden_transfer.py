"""Golden vectors for the TRANSFER engine.

Phase 4 of the extraction. The companion to `golden.py`, which pins the
ordering answer; this pins the network answer.

    python golden_transfer.py            # regenerate contract/golden_transfers.json
    python golden_transfer.py --check    # exit 2 if the answers have moved

Why the transfer engine needs its own vectors
---------------------------------------------
In production the transfer engine is not a system that runs alongside ordering.
It runs INSIDE it, between `finalize_orders` and `apply_minimum_order_gate`
(`oasis/desktop/data.py:367-369`), and it subtracts what one store can send
another from the purchase order before the gate ever sees it. So its output is
two things at once - a set of stock movements, and a smaller PO - and both have
to be pinned or a change can move one while leaving the other looking right.

It is also a STOCKOUT RELIEF engine rather than a rebalancer.
`fulfillment_decider.decide` Rule 0 returns ORDER whenever `gap_days <= 0`,
which is to say whenever the store's stock lasts until its next delivery. On a
network of comfortable stores it correctly raises nothing. The scenarios below
are therefore built to include stores that genuinely run short, or nothing
would be under test.

What this caught when it was written
------------------------------------
`decide_batch` sorted its input by descending ADS and returned the decisions in
that order, while `optimize_network` zipped them against the UNSORTED list. Every
decision was attached to a different store's shortfall whenever the shortfalls
did not happen to arrive in descending-ADS order. Fixed in
`oasis/logic/fulfillment_decider.py`, guarded by `tests/test_decision_alignment.py`.
None of the 107 existing transfer tests caught it, because each of them passes
shortfalls that are already in ADS order.
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
GOLDEN = os.path.join(OUT, "golden_transfers.json")
sys.path.insert(0, REPO)

CURRENT_DAY = 40

#: How many of the board's lines each scenario uses. The whole board makes the
#: file large and the diffs unreadable without testing anything the top slice
#: does not; these are the highest-velocity lines, which is where the engine
#: actually has decisions to make.
N_LINES = 12


def board():
    with open(os.path.join(HERE, "dataset.json"), encoding="utf-8") as f:
        data = json.load(f)
    skus = sorted(data["skus"], key=lambda s: -s["ads"])[:N_LINES]
    sup = {s["name"]: s for s in data["suppliers"]}
    return skus, sup


def record(s, sup, cover, org):
    lead = (sup.get(s["supplier"]) or {}).get("leadMean", 2.3)
    return {
        "product_name": s["name"].upper(),
        "itm_cd": s["id"],
        "avg_daily_sales": s["ads"],
        "avg_daily_sales_last_30d": s["ads"],
        "current_stocks": round(s["ads"] * cover, 2),
        "on_order_qty": 0,
        "on_order_eta_days": 999,
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
    }


def scenarios():
    """Networks chosen so each exercises a different branch of the decider.

    Cover figures are days on hand per store. The engine's donor floor is 14
    days and Rule 0 declines any store whose stock outlives its lead time, so
    a scenario with no short store is a scenario that tests nothing - except
    "no transfers", which is itself worth pinning.
    """
    skus, sup = board()
    out = []

    def net(name, why, covers):
        stores = {org: [record(s, sup, cov, org) for s in skus]
                  for org, cov in covers.items()}
        out.append({"scenario": name, "why": why, "stores": stores})

    net("balanced", "every store comfortable - the engine should decline",
        {"ST1": 10.0, "ST2": 10.0, "ST3": 10.0})

    # THE DONOR THRESHOLD. Measured by sweeping the donor's cover with ST1 at
    # zero: nothing is offered until the donors hold about THIRTY days, even
    # to a branch that is completely out. That is much higher than the 14-day
    # safety floor suggests, because `_excess_units` subtracts a target cover
    # built from lead time plus the relief horizon, not just the floor.
    # These three bracket it, so a change to any of those terms shows up as a
    # scenario crossing the line rather than as a quantity drifting.
    net("empty vs 25d donors", "ST1 out, donors just BELOW the threshold",
        {"ST1": 0.0, "ST2": 25.0, "ST3": 25.0})
    net("empty vs 30d donors", "ST1 out, donors just AT the threshold",
        {"ST1": 0.0, "ST2": 30.0, "ST3": 30.0})
    net("empty vs 40d donors", "ST1 out, donors comfortably above it",
        {"ST1": 0.0, "ST2": 40.0, "ST3": 40.0})

    net("short not empty", "2 days left - tests gap_days, not stock-out",
        {"ST1": 2.0, "ST2": 40.0, "ST3": 40.0})
    net("no donor has excess", "ST1 out, nobody has anything to give",
        {"ST1": 0.0, "ST2": 6.0, "ST3": 6.0})
    net("one long, two short", "one donor, two claimants - tests the priority",
        {"ST1": 1.0, "ST2": 1.0, "ST3": 40.0})
    net("two stores", "smallest network that can transfer at all",
        {"ST1": 0.0, "ST2": 40.0})
    net("deep overstock", "everyone long - PUSH has no hot node to push to",
        {"ST1": 45.0, "ST2": 50.0, "ST3": 55.0})
    return out


def temp_data_dir():
    tmp = tempfile.mkdtemp(prefix="oasis_xfer_")
    parent = os.path.join(tmp, "parent")
    data_dir = os.path.join(parent, "data")
    os.makedirs(data_dir, exist_ok=True)
    for f in ("supplier_rhythm_analysis.json", "supplier_weekly_schedule.json",
              "Supplier_Order_Calendar_2026.xlsx"):
        src = os.path.join(REPO, f)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(parent, f))
    return tmp, data_dir


def config_sha():
    """Hash the configuration, not its line terminators. See golden.config_sha
    -- the raw-bytes version failed on every fresh Windows clone because
    `git archive` applies CRLF conversion, reporting drift where the values
    were identical."""
    p = os.path.join(REPO, "oasis", "data", "oasis_engines_config.json")
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        canonical = json.dumps(json.load(f), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def generate():
    from oasis.logic.order_engine import OrderEngine
    from oasis.logic.simulation_bridge import SimulationOrderUtil
    from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService

    tmp, data_dir = temp_data_dir()
    eng = OrderEngine(data_dir)
    util = SimulationOrderUtil(data_dir, engine=eng)

    vectors = []
    for sc in scenarios():
        row = {"scenario": sc["scenario"], "why": sc["why"]}
        try:
            prepared, finalized = {}, {}
            for org, skus in sc["stores"].items():
                p = util.prepare_sku_data([dict(x) for x in skus])
                prepared[org] = p
                recs = util.calculate_order_quantity(
                    [dict(x) for x in p], store_config={}, current_day=CURRENT_DAY)
                for i, r in enumerate(recs):
                    r["_idx"] = i
                    r.setdefault("itm_cd", str(r.get("product_name", "")))
                fin = util.finalize_orders(recs)
                fin.sort(key=lambda r: r.get("_idx", 0))
                finalized[org] = fin

            # a throwaway registry per run, or accepted transfers would carry
            # across scenarios and the vectors would depend on their order
            reg = os.path.join(data_dir, f"reg_{sc['scenario'].replace(' ', '_')}.json")
            svc = ConsolidatedTransferService(
                org_names={o: o for o in sc["stores"]},
                stock_data={o: [dict(x) for x in prepared[o]] for o in prepared},
                registry_path=reg, data_dir=data_dir, settings={})
            plan = svc.optimize_network(
                {o: [dict(r) for r in finalized[o]] for o in finalized})

            row["transfers"] = sorted(
                [{"from": t.from_org, "to": t.to_org, "itm": t.itm_cd,
                  "qty": round(float(t.qty), 4), "urgency": t.urgency}
                 for t in plan.transfers],
                key=lambda t: (t["from"], t["to"], t["itm"]))
            row["transfer_count"] = len(plan.transfers)
            row["units_transferred"] = round(float(plan.total_units_transferred), 4)
            row["orders_reduced"] = int(plan.total_orders_reduced)
            # the other half of the answer: what the PO became
            po_before, po_after = {}, {}
            for org in finalized:
                po_before[org] = round(sum(
                    float(r.get("recommended_quantity") or 0) for r in finalized[org]), 4)
                po_after[org] = round(sum(
                    float(r.get("recommended_quantity") or 0)
                    for r in plan.adjusted_orders.get(org, finalized[org])), 4)
            row["po_before"] = po_before
            row["po_after"] = po_after
            # decisions, because they name the branch the way `reasoning` does
            row["decisions"] = sorted(
                [f"{d.recipient_org}/{d.itm_cd}:{d.decision}" for d in plan.decisions])
            # the invariant that the alignment bug violated
            row["self_transfers"] = sum(
                1 for t in plan.transfers if t.from_org == t.to_org)
        except Exception as e:
            row["error"] = f"{type(e).__name__}: {e}"
        vectors.append(row)

    shutil.rmtree(tmp, ignore_errors=True)
    return {"engines_config_sha256": config_sha(), "current_day": CURRENT_DAY,
            "n_lines": N_LINES, "count": len(vectors), "vectors": vectors}


def diff(pinned, live):
    by = {v["scenario"]: v for v in pinned["vectors"]}
    out = []
    for v in live["vectors"]:
        old = by.pop(v["scenario"], None)
        if old is None:
            out.append((v["scenario"], "new scenario", None, None))
            continue
        for k in ("transfer_count", "units_transferred", "orders_reduced",
                  "self_transfers", "transfers", "decisions",
                  "po_before", "po_after", "error"):
            a, b = old.get(k), v.get(k)
            if isinstance(a, float) and isinstance(b, float):
                if abs(a - b) > 1e-6:
                    out.append((v["scenario"], k, a, b))
            elif a != b:
                out.append((v["scenario"], k, a, b))
    for s in by:
        out.append((s, "scenario no longer generated", None, None))
    return out


def check():
    if not os.path.exists(GOLDEN):
        print("no golden transfer file yet - run without --check first")
        return 1
    pinned = json.load(open(GOLDEN, encoding="utf-8"))
    live = generate()
    d = diff(pinned, live)
    if not d:
        print(f"OK  {live['count']} transfer scenarios reproduce exactly")
        return 0
    print(f"DRIFT: {len(d)} differences across {live['count']} scenarios.\n")
    for scen, key, a, b in d[:20]:
        print(f"  {scen} :: {key}")
        print(f"      was {a}")
        print(f"      now {b}")
    if len(d) > 20:
        print(f"  ... and {len(d) - 20} more")
    print("\n  The transfer engine's answers have moved. Re-run without --check to")
    print("  re-baseline, and say in the commit what moved and why.")
    return 2


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true")
    a = ap.parse_args()
    if a.check:
        return check()

    os.makedirs(OUT, exist_ok=True)
    data = generate()
    with open(GOLDEN, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1, default=str)
    print(f"wrote {os.path.relpath(GOLDEN, REPO)}")
    for v in data["vectors"]:
        if v.get("error"):
            print(f"  {v['scenario']:<22} ERROR {v['error']}")
            continue
        print(f"  {v['scenario']:<22} {v['transfer_count']:>2} transfers, "
              f"{v['units_transferred']:>9,.0f} units, "
              f"{v['orders_reduced']} orders reduced, "
              f"{v['self_transfers']} self-transfers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
