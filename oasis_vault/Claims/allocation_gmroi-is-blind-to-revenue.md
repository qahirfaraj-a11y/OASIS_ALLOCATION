---
id: claim.allocation.gmroi-is-blind-to-revenue
type: claim
status: asserted
domain: allocation
title: AMIT's GMROI reduces to a margin ranking, so revenue is invisible to it
worth: it is why high-revenue thin-margin staples are cut
ttl_days: 60
source: oasis/logic/amit_gatekeeper.py
supports: [param.department-scaling-ratios]
tested_by: [probe.assortment-health]
guarded_by: [trap.hierarchy-inversion, trap.category-error]
---

`calculate_gmroi` uses a velocity proxy for inventory value:

    GMROI = gross_profit / (price * ads * 30 * lata)

and `gross_profit` is itself annual — margin x price x (ads x 365). Substitute:

    GMROI = (margin * price * ads * 365) / (price * ads * 30 * lata)
          = margin * 12.17 / lata

**Price and velocity cancel exactly.** Ranking a department by GMROI is
ranking it by margin over LATA and nothing else. How much trade a line
carries plays no part.

That is the whole answer to why AMIT blocks high-revenue items. A 42%-margin
trinket outranks a 26%-margin spirit that sells a thousand times more, and
the per-department line-count cap then cuts the spirit.

It is not a data defect and real margins do not fix it. It is the shape of
the metric. GMROI is a legitimate measure of return on inventory investment;
using it as the ONLY sort key for an assortment cap silently declares that
contribution does not matter.
