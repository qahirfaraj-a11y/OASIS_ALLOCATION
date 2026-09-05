---
id: claim.allocation.amit-cuts-half-the-staples
type: claim
status: asserted
domain: allocation
title: AMIT blacklists half the staples list, and does not consult it
worth: 1,323 of 2,681 staples blocked; milk, bread and flour among them
ttl_days: 30
source: devkit re-run of run_amit with real margin
supports: [param.department-scaling-ratios]
tested_by: [probe.assortment-health]
guarded_by: [trap.hierarchy-inversion]
---

Re-running `run_amit` with real gross profit from `margin_from_grn.json` is a
net gain and a named risk.

| | blocked | GP blocked | % of book GP | top decile |
|---|---|---|---|---|
| before (gross_profit mostly 0) | 9,194 | KES 131.2m | 53.0% | 816 |
| after (real margin) | 7,069 | **KES 110.7m** | **44.7%** | 756 |

**KES 20.5m of annual gross profit recovered.** 3,429 lines freed — RINA 5LT
VEGETABLE OIL at KES 1.68m GP and 42% margin among them — against 1,304 newly
blocked.

## The risk the recovery creates

Because GMROI reduces to margin (see `claim.allocation.gmroi-is-blind-to-revenue`),
correcting the data does not make the ranking revenue-aware. It makes it
*correctly* margin-ranked — which puts thin-margin anchors at the bottom of
their department on purpose:

| newly blocked | revenue/yr | margin |
|---|---|---|
| BROOKSIDE 1LT UHT WHOLE MILK | KES 2.71m | 3.2% |
| BROOKSIDE 500ML TR FRESH MILK | KES 1.98m | 3.1% |
| TUZO 500ML FRESH MILK (POUCH) | KES 1.92m | 3.4% |
| EXE 2KG ALL PURPOSE FLOUR | KES 1.71m | 8.6% |
| FESTIVE 600G FAMILY WHITE BREAD | KES 1.50m | 10.6% |

Milk, flour, bread. Before the fix they survived by tie-break luck; after it
they are identified as low-margin and cut deliberately.

## The protection already exists and is not wired in

`oasis/data/staple_products.json` holds **2,681 staples** and BudgetManager
loads it for the 60% rule. AMIT does not consult it. HALO protects **12
items**.

**AMIT currently blacklists 1,323 of the 2,681 staples — 49.3%.** Every anchor
named above is on that list.

Do not ship the margin fix without wiring the staples list into AMIT's
protection. The fix is right and it makes this failure sharper, not milder.
