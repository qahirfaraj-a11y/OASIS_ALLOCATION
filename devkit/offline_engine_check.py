"""Do ordering AND transfer run with no POS, on the local snapshot alone?

Walks the same stages the Command Center walks -- enrich, order, network
transfer, MOQ gate, buyer worklist -- outside Streamlit, so an offline failure
arrives as a stack trace in a second rather than as a lock screen or a blank
tab five minutes in. Use it before reaching for the console: if this passes,
anything still broken is the UI, not the engine.

WHAT IT DELIBERATELY DOES NOT PROVE
    A single-store network has no donors, so the transfer stage reports zero
    transfers however healthy it is. That is the arithmetic, not a failure.

    --multi adds a second node over-stocked to 120 days so that surplus
    exists and the decision path actually executes. Copying the shelf instead
    would NOT do this -- a mirror carries the same shortages, so every line
    short at one store is short at the other and the answer is still zero. The
    donor is therefore synthetic, and what it moves is a statement about the
    code, not about any real network.

USAGE
    python devkit/offline_engine_check.py [--multi]
"""
import copy
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from devkit.order_model_compare import build_book, DATA_DIR          # noqa: E402
from oasis.logic import buyer_worklist as bw                          # noqa: E402
from oasis.logic import order_up_to as ou                             # noqa: E402
from oasis.logic.consolidated_transfer_service import (               # noqa: E402
    ConsolidatedTransferService)
from oasis.logic.simulation_bridge import SimulationOrderUtil         # noqa: E402

ORG = "RHAPTA"


def main(argv) -> int:
    multi = "--multi" in argv

    print("=" * 70)
    # is_enabled() is the one to trust. model_name() reads ONLY the env var and
    # defaults to "classic", so it disagrees whenever the engines config is the
    # answer -- which it is, via global_settings.order_model. Nothing
    # user-facing calls model_name(); reading it cost me a wrong conclusion
    # once, hence this line rather than the shorter one.
    print("derived engine live :", ou.is_enabled(),
          "  (env OASIS_ORDER_MODEL:",
          (os.getenv("OASIS_ORDER_MODEL") or "unset") + ")")
    print("cover cap           :", ou.MAX_AUTO_ORDER_COVER_DAYS, "days")
    print("service level / z   :", ou.service_level(), "/", round(ou.z_score(), 3))
    print("=" * 70)

    book = build_book()
    util = SimulationOrderUtil(DATA_DIR)
    enriched = util.prepare_sku_data(copy.deepcopy(book))
    # prepare_sku_data zeroes current_stock, which silently makes every stock
    # position identical and every downstream number meaningless. Restore the
    # book's own positions.
    base = {p["sku"]: float(p.get("current_stock") or 0) for p in book}
    for p in enriched:
        p["current_stock"] = base.get(p.get("sku"), 0.0)

    recs = util.finalize_orders(util.calculate_order_quantity(
        copy.deepcopy(enriched), use_real_date=True))
    with_qty = sum(1 for r in recs
                   if float(r.get("recommended_quantity") or 0) > 0)
    print(f"ORDERING   : {len(recs):,} recommendations, {with_qty:,} with a quantity")

    orgs = {ORG: "Rhapta"}
    stock = {ORG: enriched}
    if multi:
        # A MIRROR IS NOT A DONOR. Copying the shelf gives the second node the
        # same shortages, so every line short here is short there too and the
        # transfer stage correctly reports zero -- which looks like a broken
        # flag rather than an honest answer. A donor needs EXCESS, so this node
        # is deliberately over-stocked to 120 days of cover.
        #
        # That is synthetic. It proves the transfer decision path executes
        # offline and what it decides given surplus; it says nothing about
        # whether a real network would move these units.
        donor = copy.deepcopy(enriched)
        for p in donor:
            d = float(p.get("avg_daily_sales") or 0)
            if d > 0:
                p["current_stock"] = d * 120.0
        orgs["RHAPTA2"] = "Rhapta (synthetic donor, 120d cover)"
        stock["RHAPTA2"] = donor

    # NEVER THE REAL REGISTRY. optimize_network does not just compute -- it
    # PERSISTS, via TransferState.save, replacing the file rather than
    # appending to it. An earlier run of this script pointed at
    # oasis/data/network_registry.json and wrote 1,522 synthetic PENDING
    # transfers from a store that does not exist over the operator's real
    # pending queue, unrecoverably: the registry is untracked machine state,
    # so there was no version to restore.
    #
    # A read-only-looking call that writes shared state is exactly the trap a
    # devkit probe must not fall into. Throwaway path, every run.
    registry = os.path.join(tempfile.mkdtemp(prefix="oasis_offline_check_"),
                            "network_registry.json")
    cts = ConsolidatedTransferService(
        org_names=orgs,
        stock_data=stock,
        registry_path=registry,
        distance_map={},
        cold_node_days=60,
        hot_node_days=14,
    )
    plan = cts.optimize_network({ORG: recs},
                                risk_scores={k: 0.0 for k in orgs})
    adjusted = plan.adjusted_orders.get(ORG, [])
    print(f"TRANSFER   : {len(plan.transfers):,} transfers, "
          f"{len(adjusted):,} adjusted orders, "
          f"{plan.total_units_transferred:,.0f} units moved"
          + ("" if multi else "   [single store: no donors exist, 0 is correct]"))

    gate = util.apply_minimum_order_gate(adjusted)
    # UNITS, not just lines. A transfer that fully covers a line zeroes its
    # quantity and the line leaves the PO; a partial one keeps the line and
    # only shrinks it. Line count alone cannot tell those apart, so it can
    # look identical with and without a donor while the order is much smaller.
    po_units = sum(float(r.get("recommended_quantity") or 0)
                   for r in gate["po_recs"])
    print(f"MOQ GATE   : {len(gate['po_recs']):,} PO lines "
          f"({po_units:,.0f} units), "
          f"{len(gate['transfer_recs']):,} rejects, "
          f"{len(gate['supplier_summary']):,} suppliers")

    s = bw.summarise(bw.build(adjusted))
    print(f"WORKLIST   : {s['lines']:,} lines ({s['out_of_stock']:,} at zero), "
          f"{s['worth_reviewing']:,} worth review, "
          f"KES {s['gp_per_year']:,.0f}/yr at stake")
    print("=" * 70)
    print("no POS connection was opened at any point in the above")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
