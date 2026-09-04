---
id: claim.ordering.unscheduled-vendors-carry-phantom-cover
type: claim
status: measured
domain: ordering
title: Defaulting unscheduled vendors to a weekly review buys cover for a cycle that does not exist
worth: 1.66x cover on the 48% of vendors with no declared order day
ttl_days: 90
last_evidence: 2026-09-04
source: devkit/probe_order_outliers.py
supports: [param.R]
tested_by: [probe.order-outliers, probe.decision-surface]
guarded_by: [trap.denominator-sanity, trap.category-error]
---

`R` is the review period — how long until the buyer next gets a chance to
order. That is a property of the **ordering process**, not of the supplier.

A calendar day is needed when ordering is scheduled or autonomous. When a buyer
can raise an order today, there is no cycle to cover. But a supplier absent from
the calendar was being treated as if it could only be ordered weekly, when in
fact it had told us nothing at all.

**Only 301 of 582 vendors in the receipt history have a declared order day.** On
the other 48%, isolating just those lines:

| mode | median cover ordered |
|---|---|
| `scheduled` (R defaults to 7d) | 15.5 d |
| `on_demand` (R = 1d) | 9.3 d |

**1.66x.** That is a week of protection bought against a review cycle that does
not exist, on nearly half the book.

A supplier that IS on the calendar keeps its declared cadence in both modes —
that is a commitment somebody made, and it holds whether or not the engine could
have ordered sooner.

## Status history

- 2026-09-04 — `asserted` → `measured` — probe.order-outliers → supports
