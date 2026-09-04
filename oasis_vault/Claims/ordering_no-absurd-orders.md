---
id: claim.ordering.no-absurd-orders
type: claim
status: falsified
domain: ordering
title: The engine never orders a quantity a buyer would refuse
worth: the difference between a shaped surface and a sane one
ttl_days: 60
last_evidence: 2026-09-04
source: devkit/probe_order_outliers.py
supports: [param.R, param.L, param.sigma_L]
tested_by: [probe.order-outliers]
guarded_by: [trap.denominator-sanity]
---

The property checks say the decision surface is SHAPED right — monotone,
decomposable, non-negative. They say nothing about whether any individual answer
is sane. An order can obey every structural rule and still be four months of
cover on a slow line.

**Zero engine defects across 5,000 empty-shelf decisions.**

## The condition that makes this claim mean anything

The first run flagged 154 orders of 185 days' cover as absurd. Every one was a
**single unit of a line selling five a year**. You cannot order less than one,
so the engine had no smaller answer available — the cover is a property of the
assortment, not of the ordering logic.

Check the denominator before raising the alarm. Those lines now have their own
flag (`O5_minimum_order_overhang`, 154 of 5,000) and are reported as a **range
question**, not an ordering defect. Calling them an ordering defect would send
somebody to fix the wrong thing.

`BAKE WARE ITEMS` carries 7.05x the chain median cover for exactly this reason:
twenty slow lines whose smallest possible order is months of stock.

## Status history

- 2026-09-04 — `measured` → `falsified` — probe.order-outliers → contradicts

- 2026-09-04 — `asserted` → `measured` — probe.order-outliers → supports

## The whole book, 2026-09-04

431,508 decisions - 15,411 SKUs across 14 stores in both ordering modes.

**140 engine defects (0.03%)**, and they are all one thing: lines behind a
single supplier whose measured lead-time spread is 13.35 days, where safety
stock reaches 81% of the order-up-to level. Not minimum-order artefacts -
real quantities, genuinely dominated by the variance term.

**44,044 lines (10.2%) are minimum-order overhang**, and the tail is severe:
`CH.PANCETTA TESA AFFUMICATA` orders one unit and carries **4,167 days** of
cover. Eleven years. Still not an ordering defect - one unit is the smallest
order that exists - but a range question with a number attached.

Twenty departments carry more than 3x the chain median cover, and they are
exactly the ones you would guess: WATER DISPENSERS and HOME AUDIO at 15.4x,
XMAS ITEMS and AIR/DEEP FRYERS at 9.7x, BABY ITEMS at 8.2x across 756 lines.
Durables and seasonal goods, where a single unit is months of stock.

The 500-SKU sample found none of this. It was not wrong, it was small.
