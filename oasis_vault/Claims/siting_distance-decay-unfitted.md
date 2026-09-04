---
id: claim.siting.distance-decay-unfitted
type: claim
status: stale
domain: siting
title: DISTANCE_DECAY = 2.0 is a convention that was never fitted
worth: UNMEASURED — every site recommendation depends on it
ttl_days: 120
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.distance-decay]
depends_on: [claim.siting.estate-has-a-usable-revenue-label]
tested_by: [probe.comparable-store-correlation]
guarded_by: []
stale_from: asserted
---

The exponent in `u = A / d^beta`. `BETA_RANGE = (1.5, 2.0, 2.5, 3.0)` sweeps it; no fit has been performed against realised revenue.

## Status history

- 2026-09-04 — `asserted` → `stale` — downstream of falsified claim.siting.estate-has-a-usable-revenue-label
