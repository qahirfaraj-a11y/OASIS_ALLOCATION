---
id: claim.ordering.margin-resolved-from-grn-detail
type: claim
status: falsified
mitigated: true
mitigated_by: GRN detail with Cost Price and SP, 2026-09-05
domain: allocation
title: Gross margin cannot be assembled because cost and price are on different bases
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

## CORRECTION — the pack-size explanation was wrong

Tested by conservation, which is the right test and I should have run it
first: over a long window, units received times pack equals units sold.
Two independent observed series, neither derived from price.

**93.9% of 14,937 SKUs have sold/received <= 1.15.** GRN quantity is already
in eaches. There is no pack factor. The modal integers in the cost/price
ratio were a coincidence of two unrelated distributions, and I read them as
a mechanism.

### What is actually wrong, in two layers

**Layer one — price is a placeholder.** The scorecard's `Unit_Price` takes
only 419 distinct values across 23,511 lines, and **13,552 of them (57.6%)
are exactly 108.20** — the same figure as the file's TOTAL row. Like
`Margin_Pct` at 5.0%, it is a fill-in. Real price exists only where
`revenue / total_qty_sold` can be computed: **500 lines**.

**Layer two — even there, the bases disagree.** On the 471 lines with both
real GRN cost and real revenue-derived price, **79.8% show negative margin**
at a median of -11.4%:

| line | cost | price | margin |
|---|---|---|---|
| RINSUN 5LT SUNFLOWER OIL | 1,448.29 | 1,231.16 | -17.6% |
| RINA 5LT VEGETABLE OIL | 1,235.60 | 1,074.78 | -15.0% |
| RINSUN 3LT SUNFLOWER OIL | 965.04 | 796.65 | -21.1% |

A supermarket does not sell cooking oil below cost as a rule. The two
figures are on different bases — tax-inclusive against tax-exclusive, or
landed cost against invoice cost — and the difference is consistent enough
to be a convention nobody has written down.

### The unblock, corrected

Not a pack-size map. **A price list on the same basis as the GRN cost** —
the POS selling-price master, with the tax and landed-cost convention
stated. Until then AMIT's coverage stays at ~500 lines however the
arithmetic is arranged, and 97% of its decisions remain ties.

*Two wrong explanations preceded this one. Both were arithmetic patterns I
took for mechanisms, and both fell to a test that used a series the
hypothesis had not touched.*

## RESOLVED — the client had the data all along

Sixteen GRN detail workbooks arrived carrying two columns the fulfilment
export did not have: **`Cost Price`** and **`SP`**, per line, plus `Taxable
Amt` and `Total tax Amt` separately so the tax basis is explicit.

That settles the basis question outright. `Net Amt` is tax-INCLUSIVE (16%
VAT); I had been dividing it by quantity and calling it unit cost, then
comparing it to a tax-exclusive price. Hence 79.8% negative margins and a
5-litre bottle of oil apparently selling below cost.

**Real gross margin, 17,249 SKUs:**

| | margin |
|---|---|
| p5 | 19.6% |
| p25 | 32.5% |
| **median** | **35.3%** |
| p75 | 37.3% |
| p95 | 45.4% |

Nine lines negative — 0.1%. BROOKSIDE 500ML milk, the number-one seller,
comes out at cost 59.00 against SP 60.00: a 1.67% anchor, which is exactly
what a milk line should look like.

Three wrong explanations preceded this: pack size, then a placeholder price,
then a vague 'basis mismatch'. The first two fell to tests using series the
hypothesis had not touched. The third was right in kind and useless in
practice — it named the problem without naming the column.

`oasis/data/margin_from_grn.json`.
