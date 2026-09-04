---
id: claim.ordering.cadence-is-a-distribution
type: claim
status: measured
domain: ordering
title: Cadence is a distribution, not a point estimate
worth: gap CV median 1.27; 4% of lines have a rhythm
last_evidence: 2026-08-25
ttl_days: 180
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.R]
tested_by: [probe.residual-cover]
guarded_by: [trap.circularity]
---

A line's cadence is **1.93x** its supplier's visit cadence, and a visit brings a median of 1.6 lines. Supplier cadence measures how often the van arrives, never how often a line is restocked.

This is why every attempt to derive cadence more precisely felt like chasing something.
