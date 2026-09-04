---
id: claim.siting.size-exponent-unfitted
type: claim
status: stale
domain: siting
title: SIZE_EXPONENT = 1.0 is a convention that was never fitted
worth: UNMEASURED — every site recommendation depends on it
ttl_days: 120
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.size-exponent]
depends_on: [claim.siting.estate-has-a-usable-revenue-label]
tested_by: [probe.comparable-store-correlation]
guarded_by: []
stale_from: asserted
---

`ALPHA_RANGE = (0.6, 0.8, 1.0, 1.2)` exists precisely to sweep this, and nothing has ever chosen a value from data. Together with distance decay and catchment radius this is the largest unmeasured exposure in the system.

## Status history

- 2026-09-04 — `measured` → `stale` — TTL expired

- 2026-09-04 — `stale` → `falsified` — probe.siting-robustness → contradicts

- 2026-09-04 — `asserted` → `stale` — downstream of falsified claim.siting.estate-has-a-usable-revenue-label
