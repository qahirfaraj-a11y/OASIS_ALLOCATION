---
id: param.L
type: parameter
status: stale
domain: ordering
title: Lead time
value: 2.29d mean
defined_in: `oasis/data/grn_intelligence_cache.json`
feeds: [surface.purchase-order-quantity]
stale_from: measured
---

**Value:** 2.29d mean
**Defined in:** `oasis/data/grn_intelligence_cache.json`

Median error of the derivation against the client's book: **0.0 days**. The point estimate is sound; its variance is the problem — see `param.sigma_L`.

## Status history

- 2026-09-04 — `measured` → `stale` — downstream of expired claim.ordering.rop-fallback-always-fires
