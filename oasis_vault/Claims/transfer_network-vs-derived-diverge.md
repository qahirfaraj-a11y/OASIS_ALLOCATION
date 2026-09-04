---
id: claim.transfer.network-vs-derived-diverge
type: claim
status: asserted
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
