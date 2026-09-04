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
