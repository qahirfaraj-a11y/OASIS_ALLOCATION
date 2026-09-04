---
id: claim.siting.recommendation-sensitive-to-distance-decay
type: claim
status: measured
domain: siting
title: The site recommendation depends on DISTANCE_DECAY
worth: decides whether an unfitted constant is urgent
ttl_days: 90
last_evidence: 2026-09-04
source: devkit/probe_siting_robustness.py
informs: [param.distance-decay]
tested_by: [probe.siting-robustness]
guarded_by: [trap.denominator-sanity]
---

Distinct from "DISTANCE_DECAY was never fitted", which is a statement about where the
number came from. This one asks whether the ANSWER moves when it changes.

Both matter, and confusing them is how every unfitted constant ends up looking
equally urgent. An unfitted constant the recommendation is insensitive to is a
different problem from one it turns on.

Perturbing beta across 1.5-3.0 loses top-1 twice and drifts ranks by up to 1.9 places.

## Status history

- 2026-09-04 — `asserted` → `measured` — probe.siting-robustness → supports
