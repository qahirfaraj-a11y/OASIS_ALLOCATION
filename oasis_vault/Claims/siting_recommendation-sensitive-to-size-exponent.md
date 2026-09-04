---
id: claim.siting.recommendation-sensitive-to-size-exponent
type: claim
status: falsified
domain: siting
title: The site recommendation depends on SIZE_EXPONENT
worth: decides whether an unfitted constant is urgent
ttl_days: 90
source: devkit/probe_siting_robustness.py
informs: [param.size-exponent]
tested_by: [probe.siting-robustness]
guarded_by: [trap.denominator-sanity]
---

Distinct from "SIZE_EXPONENT was never fitted", which is a statement about where the
number came from. This one asks whether the ANSWER moves when it changes.

Both matter, and confusing them is how every unfitted constant ends up looking
equally urgent. An unfitted constant the recommendation is insensitive to is a
different problem from one it turns on.

Perturbing alpha across 0.6-1.2 holds top-1 in every condition, with mean rank drift 0.03. The recommendation does not depend on it.

## Status history

- 2026-09-04 — `asserted` → `falsified` — probe.siting-robustness → contradicts
