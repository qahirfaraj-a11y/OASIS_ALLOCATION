---
id: claim.ordering.amit-blocks-profitable-lines
type: claim
status: asserted
domain: allocation
title: AMIT blocks a third of the book's gross profit, indiscriminately by margin
worth: KES 77.7m of annual gross profit blocked; KES 36.6m in the top decile alone
ttl_days: 30
source: devkit/probe_assortment_health.py
supports: [param.department-scaling-ratios]
tested_by: [probe.assortment-health]
guarded_by: [trap.denominator-sanity, trap.silent-join-failure]
---

With real margin available for the first time, AMIT's blacklist can be priced.

On 14,939 SKUs carrying **KES 814m revenue and KES 248m gross profit** a year
(blended 30.4%), AMIT blocks 5,594 lines — and with them **KES 77.7m of
annual gross profit, 31.4% of the book's total**.

## It is not selecting on profitability at all

| | blocked | allowed |
|---|---|---|
| median margin | **35.3%** | **35.3%** |
| median GP/yr | KES 5,719 | KES 6,307 |

Identical margins on both sides of the gate. Worse, the direction is
backwards: AMIT blocks **38.1% of lines with margin >= 30%** and only **17.9%
of lines with margin < 5%**. It blocks high-margin lines at more than twice
the rate of low-margin ones.

That is the mechanical consequence of ranking on a GMROI that was zero for
97% of the catalogue: within a department everything tied, and the
line-count cap trimmed by tie-break.

## What it is giving up

In the top revenue decile it blocks 453 lines carrying **KES 36.6m of gross
profit a year**, whose median margin (34.1%) is *higher* than the decile's
own (32.3%):

| line | GP/yr (KES) | margin | department |
|---|---|---|---|
| RINA 5LT VEGETABLE OIL | 1,675,451 | 42.0% | COOKING OIL |
| NESCAFE 200G CLASSIC JAR | 504,578 | 32.1% | COFFEE |
| JAMESON 1 LTR IRISH WHISKEY | 438,076 | 31.4% | SPIRITS |
| JOHNNIE WALKER BLACK LABEL 1LTR | 411,135 | 34.0% | SPIRITS |
| GIN GILBEYS 750ML | 381,460 | 26.2% | SPIRITS |
| NESCAFE 100G CLASSIC JAR | 302,378 | 37.5% | COFFEE |

Anchor lines in high-margin departments. Re-running AMIT with
`margin_from_grn.json` is now a one-input change, and it is the highest-value
action open on the ordering side.
