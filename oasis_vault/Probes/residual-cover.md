---
id: probe.residual-cover
type: probe
status: measured
domain: ordering
title: Residual cover
entrypoint: devkit/probe_residual_cover.py --xlsx /tmp/ff.xlsx
guards: []
tests: [claim.ordering.R-is-observed-gap, claim.ordering.cadence-is-a-distribution, claim.ordering.p75-service-implicit, claim.ordering.receipt-history-is-complete]
---

**Entrypoint:** `devkit/probe_residual_cover.py`

Loop B's objective function, on observed data.

For each delivery, the cover it carried against the gap it actually had to
span — taken **afterwards**, from what happened, so the ordering habit cannot
contaminate its own score.

Compares sizing for `P = R + L` under two choices of R — the engine's 7-day
policy, and each item's own median receipt gap learned from the training block —
scored on gaps it has never seen. `L` is observed too: PO date to GRN date, per
vendor, from the same export.

**The held-out split is a gift from a defect.** The missing quarter separates
2025 into two contiguous blocks that never touch, which is exactly the shape a
train/test split wants. The hole that would have corrupted a naive mean is what
makes this measurement honest.
