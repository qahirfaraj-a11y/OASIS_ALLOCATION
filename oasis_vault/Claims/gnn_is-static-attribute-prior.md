---
id: claim.gnn.is-static-attribute-prior
type: claim
status: measured
domain: ordering
title: GNN risk is a static, attribute-derived vulnerability prior
worth: spread 0.039 across 14 stores
last_evidence: 2026-06-18
ttl_days: 365
source: OASIS_GNN_Methodology_Review.md
supports: [param.gnn-risk-blend-ratio]
tested_by: [probe.gnn-feature-injection]
guarded_by: [trap.circularity]
---

Learned, graph-smoothed approximation of a 5-term linear heuristic over static store attributes. Near-circular (same attributes are inputs and the basis of the label), non-reproducible (unseeded), unvalidated (no held-out metric), fit in-sample on 14 nodes.

Safe to **show**, unsafe to **act** on. That distinction is why `surface.store-risk-display` exists as a separate node.
