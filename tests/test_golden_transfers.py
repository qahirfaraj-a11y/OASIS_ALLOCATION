"""The transfer engine's network answers must not move without someone saying so.

`devkit/retail_sandbox/golden_transfer.py` pins what
`ConsolidatedTransferService.optimize_network` returns for nine networks - the
transfers raised, the decision each shortfall got, and the purchase order
before and after the network step.

Both halves matter. The transfer engine's output is two things at once: a set
of stock movements, and (in principle) a smaller purchase order. Pinning only
the movements would let a change to the order adjustment pass unnoticed.

Three of the scenarios bracket the DONOR THRESHOLD. Measured by sweeping the
donors' cover against a store holding nothing, the engine offers no relief at
all until the donors are sitting on roughly thirty days - much higher than the
14-day safety floor suggests, because `_excess_units` subtracts a target cover
built from lead time plus the relief horizon rather than the floor alone.
Scenarios at 25d / 30d / 40d sit either side of it, so a change to any of those
terms shows up as a scenario crossing the line rather than a number drifting.

The sandbox is dev-only and is not shipped, so this skips cleanly if devkit/
is absent.
"""
import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SANDBOX = os.path.join(REPO, "devkit", "retail_sandbox")
GOLDEN = os.path.join(SANDBOX, "contract", "golden_transfers.json")

pytestmark = pytest.mark.skipif(
    not os.path.exists(GOLDEN),
    reason="retail sandbox transfer vectors not present (devkit is dev-only)")


@pytest.fixture(scope="module")
def mod():
    if SANDBOX not in sys.path:
        sys.path.insert(0, SANDBOX)
    import golden_transfer
    return golden_transfer


@pytest.fixture(scope="module")
def pinned():
    with open(GOLDEN, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def live(mod):
    """Runs the real network optimiser once. Temp data dir, never oasis/data."""
    return mod.generate()


def test_no_scenario_raises(live):
    errs = [(v["scenario"], v["error"]) for v in live["vectors"] if v.get("error")]
    assert not errs, f"{len(errs)} scenarios now raise: {errs[:4]}"


def test_no_store_is_ever_its_own_donor(live):
    """The symptom of the decide_batch/optimize_network misalignment.

    `decide_batch` used to return decisions sorted by descending ADS while
    `optimize_network` zipped them against the unsorted list, so a decision
    computed for one store was attached to another's shortfall - and a store
    could end up listed as the donor for its own transfer. See
    tests/test_decision_alignment.py.
    """
    bad = [(v["scenario"], v["self_transfers"])
           for v in live["vectors"] if v.get("self_transfers")]
    assert not bad, f"transfers from a store to itself: {bad}"


def test_golden_transfers_reproduce(mod, pinned, live):
    moved = mod.diff(pinned, live)
    if not moved:
        return
    lines = [f"{len(moved)} differences across {live['count']} scenarios.", ""]
    for scen, key, was, now in moved[:12]:
        lines += [f"  {scen} :: {key}", f"      was {was}", f"      now {now}"]
    if len(moved) > 12:
        lines.append(f"  ... and {len(moved) - 12} more")
    lines += ["",
              "If intended, re-baseline with:",
              "    python devkit/retail_sandbox/golden_transfer.py",
              "and record in the commit what moved and why."]
    pytest.fail("\n".join(lines))


def test_the_donor_threshold_is_still_bracketed(live):
    """Guard the guard: the three threshold scenarios must still straddle it.

    If all three start raising transfers, or none do, the vectors have stopped
    testing the boundary and this file would keep passing while saying nothing.
    """
    by = {v["scenario"]: v for v in live["vectors"]}
    below = by["empty vs 25d donors"]["transfer_count"]
    at = by["empty vs 30d donors"]["transfer_count"]
    above = by["empty vs 40d donors"]["transfer_count"]
    assert below == 0, (
        f"donors on 25 days now donate ({below} transfers) - the threshold "
        "moved below the bracket, so these vectors no longer straddle it")
    assert above > 0, (
        "donors on 40 days no longer donate - the threshold moved above the "
        "bracket, so these vectors no longer straddle it")
    assert at <= above, "relief should not shrink as the donor gets longer"


def test_fresh_lines_are_never_auto_transferred(mod, live):
    """A standing rule of the engine, held independently of the pinned numbers.

    Fresh items are surfaced for a human to dispatch and never auto-queued
    (`fulfillment_decider`: "FRESH ITEM - NO AUTO-TRANSFER"). A regression here
    would move milk between branches on its own.
    """
    skus, _ = mod.board()
    fresh = {s["id"] for s in skus if s["shelf"] <= 30}
    assert fresh, "no fresh lines in the vector board - the test proves nothing"

    offenders = [(v["scenario"], t["itm"])
                 for v in live["vectors"]
                 for t in v.get("transfers", [])
                 if t["itm"] in fresh]
    assert not offenders, (
        f"fresh lines were auto-queued for transfer: {offenders[:6]}")


def test_the_network_step_reports_what_it_did_to_the_order(live):
    """`po_before`/`po_after` must be captured, whatever they say.

    As measured, `optimize_network` never reduces a purchase order: the
    decider's `transfer_target = min(gap_qty, shortfall_qty)` caps a transfer
    at the stockout gap, so donors never cover the whole shortfall, so every
    decision is BOTH ("order kept for buffer") rather than TRANSFER - and only
    the TRANSFER branch calls `_adjust_order(..., 0.0, ...)`. That is the
    engine's design, not a defect, but it means the reduction is 0 and the
    vectors must keep recording it so the day it changes is visible.
    """
    for v in live["vectors"]:
        assert "po_before" in v and "po_after" in v, (
            f"{v['scenario']}: the purchase-order effect was not captured")
        assert set(v["po_before"]) == set(v["po_after"]), (
            f"{v['scenario']}: a store disappeared between before and after")
