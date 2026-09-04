---
id: claim.siting.recommendation-sensitive-to-catchment-km
type: claim
status: measured
domain: siting
title: The site recommendation depends on CATCHMENT_KM
worth: decides whether an unfitted constant is urgent
ttl_days: 90
last_evidence: 2026-09-04
source: devkit/probe_siting_robustness.py
informs: [param.catchment-km]
tested_by: [probe.siting-robustness]
guarded_by: [trap.denominator-sanity]
---

Distinct from "CATCHMENT_KM was never fitted", which is a statement about where the
number came from. This one asks whether the ANSWER moves when it changes.

Both matter, and confusing them is how every unfitted constant ends up looking
equally urgent. An unfitted constant the recommendation is insensitive to is a
different problem from one it turns on.

Perturbing the catchment radius loses top-1 at 5km and returns only 4 of the top 10 at 20km — the largest single destabiliser measured.

## Status history

- 2026-09-04 — `asserted` → `measured` — probe.siting-robustness → supports
