---
id: claim.gnn.risk-blind-to-stockouts
type: claim
status: falsified
domain: ordering
title: GNN store risk responds to live stockouts
worth: risk delta = -0.00001 on full stockout injection
last_evidence: 2026-06-18
ttl_days: 365
source: OASIS_GNN_Methodology_Review.md
supports: [param.gnn-ordering-weight]
tested_by: [probe.gnn-feature-injection]
guarded_by: []
---

**Falsified.** Injecting a full stockout into store 0 (cols 24-25 = 1.0) moved risk by -0.00001. The Command Center's injection of stockout ratios into the GNN is a no-op. Live risk reaches the blended score only through the separate `inventory_risk` term.

The gate holds: `OASIS_GNN_ORDERING_WEIGHT` is 0.
