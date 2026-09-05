---
id: claim.allocation.amit-should-derive-not-read
type: claim
status: asserted
domain: allocation
title: AMIT should derive its ranking and its constraint, not read them from a snapshot
worth: same capital retains KES 36m more gross profit and 6,452 more lines
ttl_days: 60
source: devkit/amit_adaptive.py
supports: [param.department-scaling-ratios]
tested_by: [probe.amit-adaptive]
guarded_by: [trap.silent-join-failure, trap.hierarchy-inversion, trap.category-error]
---

AMIT read a blacklist snapshot, ranked on a `gross_profit` column that was
empty for 84% of lines, and capped by a department table that reached 16 of
256 departments. Every one of those is a stored artefact where a computation
belonged.

## The dual was wrong, and that is the expensive part

GMROI is a scale-free **rate** — gross profit per unit of inventory capital,
the Dantzig ratio. Greedy-on-ratio is LP-optimal against a **capital**
budget. Against a **line-count** cap the optimal key is absolute
contribution, because the shadow price of that constraint is KES per line and
KES/KES-year cannot be a per-line value.

AMIT used the ratio key under the count constraint. Right metric, wrong
constraint.

## Recomputed from the book, 14,262 lines, no snapshot

| selection | lines cut | GP cut | revenue cut | staples cut |
|---|---|---|---|---|
| count cap, sort by GP | 7,226 | KES 43.6m (18.2%) | 19.0% | 740 (32.6%) |
| capital knapsack, GMROI | 509 | KES 11.5m (4.8%) | 14.9% | 283 (12.5%) |
| count cap + staples | 6,681 | KES 37.9m (15.8%) | 14.4% | 0 |
| **capital knapsack + staples** | **229** | **KES 1.9m (0.8%)** | **1.7%** | **0** |

Against the shipped engine, which cuts KES 110.7m (44.7%) even after the
margin fix.

**At the identical capital budget of KES 9.24m**, treating the constraint as
capital rather than line count retains 6,452 more lines and roughly KES 36m
more gross profit. The line count was never the real constraint; money was.

## What stops being configuration

The capital rule needs no per-department caps and no `fallback_cap = 50`. It
needs a budget, which the business already has. Twenty-six hand-set caps and
one undocumented literal leave the config surface.

One global budget, not per-department splits: a split imposes extra
constraints on a problem whose optimum has a single shadow price, so it is
feasible-but-dominated — line caps wearing a currency sign.

## What no per-SKU key can see

Staple protection stays explicit. The loss from delisting milk is not milk's
gross profit, it is the margin of baskets that stop occurring, and no sort
key over per-SKU contribution — GMROI, absolute GP or knapsack — can observe
a cross-SKU basket externality. Every rule above silently assumes it is zero.
For the six staple departments that is the one assumption certain to be false.
