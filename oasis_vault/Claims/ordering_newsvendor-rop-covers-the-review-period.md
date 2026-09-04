---
id: claim.ordering.newsvendor-rop-covers-the-review-period
type: claim
status: asserted
domain: ordering
title: The newsvendor reorder point protects the full protection interval
worth: without it, flipping the mode stops replenishment on 6,313 lines
ttl_days: 60
source: devkit/governance_sweep.py
supports: [param.R, param.L]
tested_by: [probe.governance-sweep]
guarded_by: [trap.category-error, trap.denominator-sanity]
---

A reorder point covers demand until stock can next **arrive**. Under
periodic review that is the review period plus the lead time, `P = R + L`.

The newsvendor ROP built for F4 used `mu_LTD = ADS * lead_time` — lead time
alone. It dropped `R`, the one horizon in this engine with a derivation and
the term measured at 2.07x working capital.

**The omission did not look like an error. It looked like a tighter reorder
point.** On a 10/day line it halved ROP from 31 units to 16.6, and across
the book at 2 days of cover it took ordering from 6,313 lines to **zero**.
Flipping `OASIS_ROP_MODE` off `heuristic` would have quietly stopped
replenishing every line holding more than about 1.7 days of stock.

With `P = R + L` restored the direction reverses, which is the point: at 5
days of cover the newsvendor ROP reorders **5,154 lines the flat heuristic
misses**. It is not more conservative than the heuristic, it is better
aimed — and it was only ever going to be visible by flipping the switch
across the whole book and looking at the stock positions in between.
