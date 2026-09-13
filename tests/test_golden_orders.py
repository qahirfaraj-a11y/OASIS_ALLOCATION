"""The ordering engine's answers must not move without someone saying so.

`devkit/retail_sandbox/golden.py` pins what the live pipeline returns for 297
vectors - the 58-line board at five stock positions, plus the classification
edge cases - down all four stages:

    prepare_sku_data -> calculate_order_quantity -> finalize_orders
                     -> apply_minimum_order_gate

Why this test exists rather than the usual unit tests: the engine has **zero
required fields**. Feed it a thin or wrong record and it returns a confident
number through a fallback branch instead of raising. So "it still runs" and
"it still ordered something" both pass while the answer is wrong. Only pinning
the actual numbers - and the `reasoning` string, which is the only place the
engine names the branch it took - can tell the difference.

A failure here is not automatically a bug. Ordering rules are allowed to
change. It means: an answer moved, and every sandbox result derived from the
old vectors has to be re-derived before it is quoted again. To accept the
change, run `python devkit/retail_sandbox/golden.py` and say in the commit
message what moved and why.

The sandbox is dev-only and is not shipped to clients, so this test skips
cleanly if devkit/ is absent.
"""
import json
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SANDBOX = os.path.join(REPO, "devkit", "retail_sandbox")
GOLDEN = os.path.join(SANDBOX, "contract", "golden_orders.json")

pytestmark = pytest.mark.skipif(
    not os.path.exists(GOLDEN),
    reason="retail sandbox golden vectors not present (devkit is dev-only)")


@pytest.fixture(scope="module")
def golden_mod():
    if SANDBOX not in sys.path:
        sys.path.insert(0, SANDBOX)
    import golden
    # The vectors are pinned against derived files that are regenerable and
    # deliberately gitignored, so a clean checkout does not have them. Without
    # them the review schedule loads zero suppliers, R falls back to its
    # default, and 221 of 297 vectors report as "moved" when nothing moved at
    # all -- the engine was answering a different question. Skip honestly
    # rather than fail loudly about the wrong thing.
    gone = golden.missing_inputs()
    if gone:
        pytest.skip("golden vectors need derived inputs this checkout does "
                    f"not have: {', '.join(gone)}. They are gitignored as "
                    "ephemeral; regenerate them before trusting a drift run.")
    return golden


@pytest.fixture(scope="module")
def pinned():
    with open(GOLDEN, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def live(golden_mod):
    """Runs the real pipeline once. Uses a temp data dir, never oasis/data."""
    return golden_mod.generate()


def test_config_has_not_drifted(golden_mod, pinned):
    """The vectors were produced under one engines config. Say so if it moved."""
    live_sha = golden_mod.config_sha()
    assert live_sha == pinned.get("engines_config_sha256"), (
        "oasis_engines_config.json has changed since the golden vectors were "
        "pinned. The vectors below may still pass, but they are no longer "
        "attributable to the pinned configuration. Re-run "
        "`python devkit/retail_sandbox/golden.py` to re-baseline.")


def test_every_vector_still_answers(live):
    """No vector may start raising. A thin record is answered, not refused."""
    errs = [v["case"] for v in live["vectors"] if v.get("error")]
    assert not errs, f"{len(errs)} vectors now raise: {errs[:8]}"


def test_golden_orders_reproduce(golden_mod, pinned, live):
    """Quantities, target days, fulfilment and the reasoning branch all match."""
    assert live["count"] == pinned["count"], (
        f"vector count changed ({pinned['count']} -> {live['count']}); "
        "the matrix itself moved, so nothing below is comparable")

    moved = golden_mod.diff(pinned, live)
    if not moved:
        return

    lines = [f"{len(moved)} of {live['count']} golden vectors no longer match.",
             ""]
    for row in moved[:12]:
        if row["kind"] != "changed":
            lines.append(f"  {row['case']}: {row['kind']}")
            continue
        for key, was, now in row["moved"]:
            lines.append(f"  {row['case']}  {key}:")
            lines.append(f"      was {was}")
            lines.append(f"      now {now}")
    if len(moved) > 12:
        lines.append(f"  ... and {len(moved) - 12} more")
    lines += [
        "",
        "If this change is intended, re-baseline with:",
        "    python devkit/retail_sandbox/golden.py",
        "and record in the commit what moved and why. Sandbox findings drawn",
        "against the old vectors need re-deriving before they are quoted.",
    ]
    pytest.fail("\n".join(lines))


def test_reasoning_is_pinned_not_just_quantity(pinned):
    """Guard the guard: a vector set with no reasoning strings proves nothing.

    Two engine configurations can agree on a quantity while reaching it by
    different branches - one via the DDoS target, one via the ROP fallback.
    The reasoning string is the only signal that separates them, so if it ever
    stops being captured this whole file quietly weakens.
    """
    with_reason = [v for v in pinned["vectors"] if (v.get("reasoning") or "").strip()]
    assert len(with_reason) > 0.8 * pinned["count"], (
        "most golden vectors carry no `reasoning` string, so the branch the "
        "engine took is not actually pinned - only the number is")
