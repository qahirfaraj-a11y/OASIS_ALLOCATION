---
id: claim.transfer.network-vs-derived-diverge
type: claim
status: falsified
domain: ordering
title: The shipped transfer scan and the derived methodology give different answers
worth: unmeasured across store subsets
ttl_days: 120
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: []
tested_by: [probe.transfer-methodology-compare]
guarded_by: [trap.like-for-like]
---

The shipped path passes a supplier-calendar `next_delivery_days` and cold/hot windows of 60/14 and nothing else — no LATA rhythm, no AMIT tiers, so the variance term is unreachable and every category shares one 45-day threshold. The probe exists; no verdict has been recorded.

## Re-tested 2026-09-19 — true when written, no longer true

Since 9dca72cf every surface builds the service through
`oasis.desktop.data.build_transfer_service`, which passes `data_dir` (LATA +
AMIT), operator settings, the calendar, the registry and distances. The probe
now builds its SHIPPED arm by calling that function, so it cannot drift from
the product again (its first version hard-coded the old wiring as "what the
console passes"). On the 14-store depot seed, six store subsets:

* **shipped vs derived: 0.00% apart on every measure** — lines, units, donor
  choice, quantities, coverage — in all six subsets.
* legacy (pre-fix) vs derived: 10–24% of lines, 10–50% of units, up to 16%
  donor disagreement and 59% of (SKU, recipient) pairs served by only one.

The first run showed 11–14% donor disagreement at 7 and 14 stores. That was a
probe artefact: the shipped arm read the install's `store_coords.json`, which
holds the live estate, none of the depot's codes — so it chose donors blind
to distance. With the same coordinates in every arm it vanished. The artefact
exposed a real silent-join risk, now logged by `build_transfer_service`:
a network whose codes are missing from the distance map is warned about
instead of transferring distance-blind.

## Status history

- 2026-09-19 — `asserted` → `falsified` — probe.transfer-methodology-compare → contradicts
