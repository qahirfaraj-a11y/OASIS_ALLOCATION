---
id: claim.ordering.receipt-history-is-complete
type: claim
mitigated: true
mitigated_by: devkit/stock_ledger.py
status: falsified
domain: ordering
title: The receipt history covers the year it claims to
worth: every inter-receipt gap measured across the hole is fiction
ttl_days: 90
source: devkit/probe_residual_cover.py
supports: [param.R, param.L]
tested_by: [probe.residual-cover]
guarded_by: [trap.denominator-sanity, trap.stale-duplicate-shadowing]
---

`All_Suppliers_Fulfillment_Detail.xlsx` presents as a year of receipts,
2025-01-01 to 2025-12-09. It is not one.

* **2025-04, 2025-05 and 2025-06 are absent entirely** — from GRN dates and PO
  dates alike. An item received in March and again in July shows a ~120-day
  gap that never happened. 9,660 gaps span it.
* **16 subtotal rows carry 2,875,674 units**, exactly half the naive total.
  The file was rendered as a report before it was saved as data — the same
  family of defect as the supplier calendar.

Neither is fatal, and both are silent. The ledger rejects the subtotals and
refuses any span crossing the hole; the durable fix is a clean export.

## Status history

- 2026-09-04 — `asserted` → `falsified` — probe.residual-cover → contradicts

## Mitigated, not ignored

`devkit/stock_ledger.py` rejects the subtotal rows and refuses any span that
crosses the hole, so measurements built on it — `probe.residual-cover` above
all — are sound *despite* the defect. The claim therefore does not propagate:
the ordering surface is not in question over a defect that has already been
answered.

It stays falsified, and visible, because the durable fix is a clean export and
nobody should be able to forget that.
