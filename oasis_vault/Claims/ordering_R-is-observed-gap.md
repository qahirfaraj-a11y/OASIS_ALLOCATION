---
id: claim.ordering.R-is-observed-gap
type: claim
status: measured
domain: ordering
title: R should be the observed order gap, not a policy review period
worth: 2.14x working capital
last_evidence: 2026-08-25
ttl_days: 90
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.R]
depends_on: [claim.ordering.review-schedule-is-clean]
tested_by: [probe.residual-cover]
guarded_by: [trap.circularity, trap.like-for-like]
---

`P = R + L` is the only horizon in the engine with a derivation. Safety grows with the **square root** of it and enters **additively**.

`supplier_weekly_schedule.json` declares an order weekday for **940 suppliers**. R was in the data all along, unused for months. Actual ordering ran **2.21x** less often than the declared schedule — a demand-driven gap, not a supply constraint.

The derived model lands cover-to-gap on 0.97x for 41% of capital and runs 49 lines dry: it sizes for a 7-day review while deliveries arrive every 15. **This cannot be promoted without an operations commitment** — halving working capital means ordering twice as often.

## Correction — 2026-09-04

The "940 suppliers" figure is wrong, and was measured wrong rather than quoted
wrong. `order_up_to.load_review_schedule` admitted anything longer than three
characters, so 262 display truncation markers (`...AND 123 MORE`, `(BI-WK)`)
counted as suppliers. **The real figure is 688** — 678 coded entries plus 12
comma-split names now repaired.

This does not overturn the claim; R is still the observed order gap and still
the largest lever. It does mean the denominator behind "2.21x less often than
the declared schedule" needs recomputing against 688, not 940 — see
`trap.denominator-sanity` and `claim.ordering.review-schedule-is-clean`.
