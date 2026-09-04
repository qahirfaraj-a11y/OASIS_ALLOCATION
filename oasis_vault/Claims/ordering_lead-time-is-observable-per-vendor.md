---
id: claim.ordering.lead-time-is-observable-per-vendor
type: claim
status: measured
domain: ordering
title: Lead time and its spread are measurable per vendor, from receipts alone
worth: 472 of 575 vendors, no gap reasoning required
ttl_days: 90
last_evidence: 2026-09-04
source: devkit/probe_decision_surface.py
supports: [param.L, param.sigma_L]
tested_by: [probe.lead-time-from-receipts]
guarded_by: [trap.circularity]
---

The gap between consecutive receipts is a fact about ordering behaviour,
contaminated by the habit it is meant to measure and broken outright by
the missing quarter. **PO date to GRN date needs none of that.** It is a
fact about the supplier, measured inside one transaction, and no hole in
the calendar can fabricate it.

92,181 receipts carry a lead time. 472 of 575 vendors have eight or more,
which is enough to give each its own `L` and `sigma_L` instead of sharing
one constant. The remaining 103 keep the default — which is the correct
behaviour for a supplier nobody has measured, and not evidence that it
delivers on time.

## Status history

- 2026-09-04 — `asserted` → `measured` — probe.lead-time-from-receipts → supports
