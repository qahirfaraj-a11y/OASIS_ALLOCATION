---
id: param.distance-decay
type: parameter
status: stale
domain: siting
title: DISTANCE_DECAY
value: 2.0 — a convention
defined_in: "`oasis/logic/site_scoring.py:162`"
feeds: [surface.site-recommendation]
stale_from: measured
---

**Value:** 2.0 — a convention
**Defined in:** `oasis/logic/site_scoring.py:162`

The distance exponent in `u = A / d^beta`. Never fitted. `BETA_RANGE = (1.5, 2.0, 2.5, 3.0)` exists to sweep it.

## Status history

- 2026-09-04 — `measured` → `stale` — downstream of falsified claim.siting.estate-has-a-usable-revenue-label
