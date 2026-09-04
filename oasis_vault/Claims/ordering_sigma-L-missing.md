---
id: claim.ordering.sigma-L-missing
type: claim
status: measured
domain: ordering
title: sigma_L belongs in the safety term and is the larger variance contributor
worth: 2.22d spread on a 2.29d mean
ttl_days: 90
last_evidence: 2026-09-04
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.sigma_L]
tested_by: [probe.term-attribution, probe.lead-time-from-receipts]
guarded_by: [trap.denominator-sanity]
---

The formula is `S = d(R+L) + z*sqrt((R+L)*sigma_d^2 + d^2*sigma_L^2)`. The `d^2*sigma_L^2` term is absent from the implementation. Lead time is as variable as it is long, so for any line that moves this is the larger of the two variance terms.
