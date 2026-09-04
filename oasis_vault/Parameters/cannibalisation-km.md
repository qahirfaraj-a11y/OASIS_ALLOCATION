---
id: param.cannibalisation-km
type: parameter
status: stale
domain: siting
title: CANNIBALISATION_KM
value: 3.0 — a convention
defined_in: "`oasis/logic/site_scoring.py:87`"
feeds: [surface.site-recommendation]
stale_from: measured
---

**Value:** 3.0 — a convention
**Defined in:** `oasis/logic/site_scoring.py:87`

The radius within which an own-store is counted as cannibalised. Drives the Westlands-style downgrades, which are the model's most defensible output — and rest on an unfitted constant.

## Status history

- 2026-09-04 — `measured` → `stale` — downstream of falsified claim.siting.estate-has-a-usable-revenue-label
