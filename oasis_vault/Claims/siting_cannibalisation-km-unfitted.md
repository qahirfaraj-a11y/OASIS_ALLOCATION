---
id: claim.siting.cannibalisation-km-unfitted
type: claim
status: stale
domain: siting
title: CANNIBALISATION_KM = 3.0 is a convention that was never fitted
worth: UNMEASURED — drives the model's most defensible output
ttl_days: 120
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.cannibalisation-km]
depends_on: [claim.siting.estate-has-a-usable-revenue-label]
tested_by: [probe.comparable-store-correlation, probe.siting-robustness]
guarded_by: []
stale_from: asserted
---

The radius within which an own-store counts as cannibalised. It produces the Westlands-style downgrades — the outputs a buyer finds most convincing — and it rests on a constant nobody fitted.

Found by the graph itself: this parameter fed a live decision with **no supporting claim at all**, which is a worse position than an unfitted one.

## Status history

- 2026-09-04 — `asserted` → `stale` — downstream of falsified claim.siting.estate-has-a-usable-revenue-label
