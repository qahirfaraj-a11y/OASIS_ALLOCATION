---
id: claim.ordering.rop-fallback-always-fires
type: claim
status: stale
domain: ordering
title: Live enrichment supplies no ROP or coverage target, so the fallback always fires
worth: F4 — open
ttl_days: 60
last_evidence: 2026-06-19
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.L]
tested_by: [probe.pipeline-trace]
guarded_by: []
stale_from: measured
---

`fetch_enriched_products` sets ADS / cv / on_order but not `reorder_point` or `target_coverage_days`. The maths is sound, but this is a heuristic, not a forecasting layer — worth naming so it is not mistaken for one.

## Status history

- 2026-09-04 — `measured` → `stale` — TTL expired
