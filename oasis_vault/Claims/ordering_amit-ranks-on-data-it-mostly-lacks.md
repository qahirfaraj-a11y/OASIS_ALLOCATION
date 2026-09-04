---
id: claim.ordering.amit-ranks-on-data-it-mostly-lacks
type: claim
status: asserted
domain: allocation
title: AMIT trims departments by a GMROI that exists for 3% of the catalogue
worth: 5,683 lines blocked, 27% of book value, on a signal that is mostly absent
ttl_days: 60
source: devkit/probe_assortment_health.py
supports: [param.department-scaling-ratios]
tested_by: [probe.assortment-health]
guarded_by: [trap.silent-join-failure, trap.denominator-sanity]
---

AMIT is not a profitability threshold. It is a **per-department line-count
cap**: sort a department by GMROI, keep the top N, blacklist the rest.

GMROI is `gross_profit / (avg inventory value * LATA penalty)`. Of 17,249
lines in the profitability intelligence, **500 carry any gross profit at
all** — 479 positive, 21 negative. The other **16,749 (97%) are exactly
zero**, so their GMROI is zero, so within a department they all tie and the
cap trims by whatever the sort's tie-break happens to be.

The behaviour where it CAN measure is impeccable:

| population | lines | blacklisted |
|---|---|---|
| positive gross profit | 479 | **0 (0.0%)** |
| negative gross profit | 21 | 6 (28.6%) |
| zero / unmeasured | 16,749 | 6,013 (35.9%) |

It never blocks a line it can see is profitable, and it keeps every one of
the top twelve sellers including the negative-margin milk anchors. The
problem is not aim, it is **coverage**: 97% of the decisions are made
without the input the method is defined on.

That is what puts 483 of the top value decile on the blacklist. They are not
unprofitable lines; they are unmeasured ones that tied at zero.

The fix is upstream and cheap to state: populate gross profit, or rank the
unmeasured population on something observed (velocity, receipt frequency)
rather than letting them tie.
