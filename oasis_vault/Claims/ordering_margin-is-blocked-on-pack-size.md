---
id: claim.ordering.margin-is-blocked-on-pack-size
type: claim
status: asserted
domain: allocation
title: Gross margin cannot be assembled because GRN cost and retail price are in different units
worth: it is the only thing between AMIT and a real ranking signal
ttl_days: 90
source: devkit/derive_margin.py
supports: [param.department-scaling-ratios]
tested_by: [probe.derive-margin]
guarded_by: [trap.like-for-like, trap.denominator-sanity]
---

The cost half is there. `All_Suppliers_Fulfillment_Detail.xlsx` gives a unit
cost for **18,037 SKUs** — Net Amt over GRN Qty, observed, from what the
business actually paid. The price half is there too: the allocation
scorecard carries `Unit_Price` for 23,497 lines.

They are not in the same unit.

| cost / price | value |
|---|---|
| p5 | 0.48 |
| p25 | 1.10 |
| p50 | **2.54** |
| p75 | 5.55 |
| p95 | 18.99 |

The modal integer ratios are 1, 2, 3, 4, 5, 6, 7. **That distribution is a
pack size, not a margin.** GRN quantity is counted in cases; the scorecard
price is per each. Multiplying them out gives negative gross profit on
77.9% of lines, and wiring that into AMIT would blacklist most of the book
on an arithmetic artefact.

## What was ruled out on the way

The scorecard's own `Margin_Pct` looked like the answer and is not: it is
**exactly 5.0 on 23,092 of 23,511 lines (98.2%)**, a hardcoded default.
Ranking on it would replace AMIT's ties at zero with ties at five — the same
defect wearing a different number.

A revenue-route cross-check (`revenue * margin_pct`) disagreed with the
cost route on 14,961 of 14,973 lines by a consistent factor of about nine.
That is not two measurements disagreeing, it is one measurement in two
periods, and it is why the check is now done per unit where the period
cancels.

**Unblocks with:** a per-SKU pack size or UOM map joining goods-receipt
units to retail units. With that, gross profit is immediately computable
for ~15,400 lines against the 500 AMIT has today, and AMIT stops ranking
97% of its decisions on ties.

Nothing is wired in until then. `oasis/data/gross_margin_derived.json` is
written with a `_BLOCKED` header saying so.
