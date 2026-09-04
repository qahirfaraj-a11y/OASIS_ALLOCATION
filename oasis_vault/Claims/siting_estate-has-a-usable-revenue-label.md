---
id: claim.siting.estate-has-a-usable-revenue-label
type: claim
status: falsified
domain: siting
title: The existing estate has a defensible measure of realised store performance
worth: BLOCKING — no siting constant can be fitted without one
ttl_days: 60
source: devkit/probe_comparable_stores.py
supports: []
tested_by: [probe.comparable-store-correlation]
guarded_by: [trap.circularity, trap.silent-join-failure, trap.like-for-like]
---

Siting has no outcome labels: no store has ever been opened on this model's
recommendation, so there is nothing to attribute. The substitute is
comparable-store inference — score the stores that already exist and correlate
predicted capture against how they actually perform.

That substitute is only worth anything if "how they actually perform" is a
defensible number. This claim asserts that it is, and it is the **precondition**
for fitting `SIZE_EXPONENT`, `DISTANCE_DECAY`, `CATCHMENT_KM` or
`CANNIBALISATION_KM` against anything.

Two candidate labels exist:

* `stores_network.json` → `avg_monthly_revenue` — the file the GNN also trains on.
* POS till takings by `ORG_CD` in `oasis/data/variant_network.db` — observed.

If they disagree, at most one of them is the truth, and a fit against either is
a coin toss dressed as a measurement.

## Status history

- 2026-09-04 — `asserted` → `falsified` — probe.comparable-store-correlation → contradicts

## What the probe established (2026-09-04)

**There is no observed store-performance label on this install.** Not one.

* `stores_network.json` → `avg_monthly_revenue` sits on a fixed ladder of
  456,000 and tracks `footfall_rank` at rho = 0.996. It is a function of the
  model's own inputs.
* All **seven** POS databases fail the provenance tells. The richest,
  `mock_pos_erp.db`, has 136,102 rows, ids running 1..136102 with no gaps, and
  is named `mock`.
* The sources split into two mutually contradictory families: `declared`,
  `mock_pos_erp` and `mock_pos_erp_lite` agree at rho >= 0.996 — a common
  generator, not corroboration — while `variant_network.db` anti-correlates
  with all three at rho ~ -0.4. Spread across pairs: **1.433**.

So the constants would be fitted to whichever synthetic estate happened to be
loaded. The blocker is not the model and not the sweep; it is that no realised
outcome exists to fit against.

**Unblocked by:** real client POS with per-store takings over a window long
enough to rank the estate. Nothing else in the siting chain can be validated
until then.
