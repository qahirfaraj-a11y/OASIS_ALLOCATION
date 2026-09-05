---
id: claim.allocation.gmroi-is-blind-to-revenue
type: claim
status: falsified
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

## CORRECTED by methodology-challenger, 2026-09-05

Three quantitative errors and one wrong cause.

**The constant is 6.13, not 12.17.** `gross_profit` in nodes.csv is a
**184-day** figure, not annual: implied T = total_quantity / velocity_ads is
184 for 2,381 rows, 185 for 511, 183 for 349, and 365 for only 398. The
window is bimodal, so 398 SKUs carry a free **2x rank boost** purely for
having a full year of history. At staple margins a 2x artefact outranks the
entire real margin spread inside a department.

**Price does not cancel exactly.** Cancellation needs revenue = price x qty,
which holds to 1e-6 for 55% of rows; p90 deviation 21.9%, max 105%. `price`
is list, `revenue` is realised — discount, promo and pack mix live in the
gap. The identity holds for **8.6% of the catalogue**, not universally.

**And the cause I named was wrong for 96% of the output.** `gross_profit`
was populated on 3,663 of 23,511 nodes. The other 19,848 scored GMROI = 0.0
exactly, tied, and Python's stable sort left them in **nodes.csv row order**.
14,361 of 14,890 blacklistings (96.4%) came out of that tie block. Shuffling
the input flips ~23% of decisions with no input value changed.

So the metric's shape was a real defect and a minor one. The operative cause
was a missing join, and the second cause was that
`DEFAULT_DEPT_CAPS_BASELINE` names 16 departments against 256 in the data —
**83% of SKUs fall to an undocumented `fallback_cap = 50`** that is the real
assortment policy. HOUSEHOLD ITEMS: 1,008 lines, cap 50, 958 cut.

Superseded by `claim.allocation.amit-should-derive-not-read`.
