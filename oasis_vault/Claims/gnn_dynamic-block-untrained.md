---
id: claim.gnn.dynamic-block-untrained
type: claim
status: falsified
domain: ordering
title: The GNN's dynamic feature block (cols 24-29) carries learned weight
worth: payday toggle delta = 0.00000
last_evidence: 2026-06-18
ttl_days: 365
source: OASIS_GNN_Methodology_Review.md
supports: [param.gnn-ordering-weight]
tested_by: [probe.gnn-feature-injection]
guarded_by: []
---

**Falsified.** `store_to_features` ends with `features.extend([0.0] * 6)`, so cols 24-29 are constant zero at training time. A constant-zero column contributes zero gradient; those weights never left initialisation. At inference the same columns are populated live.

The model is fed live signal on exactly the dimensions it learned nothing about.
