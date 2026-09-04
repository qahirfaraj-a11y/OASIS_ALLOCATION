---
id: probe.governance-sweep
type: probe
status: measured
domain: ordering
title: Governance engine sweep
entrypoint: devkit/governance_sweep.py --skus all
guards: []
tests: [claim.ordering.governance-gates-discriminate, claim.ordering.newsvendor-rop-covers-the-review-period, claim.ordering.lata-multiplier-is-saturated, claim.ordering.rop-fallback-always-fires]
---

**Entrypoint:** `devkit/governance_sweep.py`

Agents drive the real bridge path — AMIT, MANDE, HALO, LATA, DHARAM, the
schedule and the ROP gate — across the whole universe, under a 2x2 of
`OASIS_ROP_MODE` and `OASIS_LATA_SOURCE`.

Every gate already writes its own marker into the recommendation's
`reasoning`, so the engine is self-instrumenting and nothing has to be
guessed. The probe counts them.

## It sweeps stock positions, and that is the whole design

ROP decides **whether** to order, not how much. At an empty shelf every
line is below every reorder point, so the two modes give byte-identical
answers and an empty-shelf sweep is blind to the question. The first run
reported '0 quantities changed' and would have retired the comparison.

Sweeping across days of cover on hand is what exposed that the newsvendor
ROP was missing the review period.
