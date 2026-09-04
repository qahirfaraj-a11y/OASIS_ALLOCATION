---
id: claim.ordering.sigma-L-changes-the-order
type: claim
status: measured
domain: ordering
title: Measuring sigma_L per vendor materially changes what gets bought
worth: median safety stock falls to 0.739x of the chain default
ttl_days: 90
last_evidence: 2026-09-04
source: devkit/probe_decision_surface.py
supports: [param.sigma_L]
tested_by: [probe.decision-surface]
guarded_by: [trap.denominator-sanity]
---

`sigma_L` being absent from the formula is one claim; `sigma_L` mattering
once supplied is another, and only the second justifies the work.

Run the whole procurement team twice over the same universe — once with
the chain-wide 2.22-day constant, once with each vendor's measured spread —
and compare decision for decision.

**Median safety stock falls to 0.739x.** The constant overstates the
typical line's safety stock by about a third, because the measured
per-vendor median is 1.45 days, not 2.22.

Incidence alone would not have settled this: only 12.6% of order
quantities change, and the median quantity delta is 0.0%, because cycle
stock dominates and packs round to integers. The safety term is where the
money is, and it moves.

## Status history

- 2026-09-04 — `asserted` → `measured` — probe.decision-surface → supports
