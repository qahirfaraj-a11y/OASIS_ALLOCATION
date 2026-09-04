---
id: claim.ordering.purge-gates-spare-the-hot-nodes
type: claim
status: falsified
domain: allocation
title: AMIT and MANDE take far less trade than they take lines
worth: the share-of-catalogue figure was never the right measure
ttl_days: 60
source: devkit/probe_assortment_health.py
supports: [param.department-scaling-ratios]
tested_by: [probe.assortment-health]
guarded_by: [trap.denominator-sanity, trap.like-for-like]
---

A supermarket's line count is dominated by a long cold tail, so a gate that
removes half the LINES may remove almost none of the TRADE. The question is
what share of velocity and value goes with them.

On 15,411 lines carrying 3.5m units and KES 562m a year, where the top decile
carries **64% of all value**:

| gate | SKUs | units | value | top decile | top 100 |
|---|---|---|---|---|---|
| AMIT | 36.9% | 18.9% | **27.4%** | 483 (31.3%) | 12 |
| MANDE | 39.4% | 13.4% | **23.4%** | 392 (25.4%) | 1 |

Neither is the clean cold-tail cut the headline percentages implied. AMIT
takes value at three quarters the rate it takes lines — nearly proportional,
which is what a gate looks like when it is **not** discriminating. MANDE is
better aimed: it takes value at 0.59x the rate it takes lines and touches
one line in the top hundred, and its own report attributes KES 6.06m of
trapped capital to the 300 suppliers it flags.

So MANDE is doing its job. AMIT is blocking the right KIND of line and too
many of them, for the reason in
`claim.ordering.amit-ranks-on-data-it-mostly-lacks`.

## Status history

- 2026-09-04 — `asserted` → `falsified` — probe.assortment-health → contradicts
