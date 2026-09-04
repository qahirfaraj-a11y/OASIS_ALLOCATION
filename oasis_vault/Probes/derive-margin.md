---
id: probe.derive-margin
type: probe
status: measured
domain: allocation
title: Gross margin assembly
entrypoint: devkit/derive_margin.py --xlsx /tmp/ff.xlsx
guards: []
tests: [claim.ordering.margin-is-blocked-on-pack-size, claim.ordering.amit-ranks-on-data-it-mostly-lacks]
---

**Entrypoint:** `devkit/derive_margin.py`

Joins GRN unit cost to scorecard price and POS velocity to build the gross
profit AMIT ranks on, and cross-checks the stated margin per unit — where
the period cancels — rather than per year, where it does not.

It currently reports the join as BLOCKED. That is the useful output: it
names the one missing input, and it refuses to write a ranking signal that
would blacklist most of the catalogue.
