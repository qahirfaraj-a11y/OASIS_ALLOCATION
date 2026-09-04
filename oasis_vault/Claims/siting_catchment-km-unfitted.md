---
id: claim.siting.catchment-km-unfitted
type: claim
status: stale
domain: siting
title: CATCHMENT_KM = 10.0 is a convention that was never fitted
worth: UNMEASURED — conditions every capture figure reported
ttl_days: 120
last_evidence: 2026-09-04
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.catchment-km]
depends_on: [claim.siting.estate-has-a-usable-revenue-label]
tested_by: [probe.comparable-store-correlation, probe.siting-robustness]
guarded_by: []
stale_from: asserted
---

Every capture percentage the model reports is conditional on this radius, and the radius is a convention.

## Status history

- 2026-09-04 — `measured` → `stale` — downstream of falsified claim.siting.estate-has-a-usable-revenue-label

- 2026-09-04 — `stale` → `measured` — probe.siting-robustness → supports

- 2026-09-04 — `asserted` → `stale` — downstream of falsified claim.siting.estate-has-a-usable-revenue-label
