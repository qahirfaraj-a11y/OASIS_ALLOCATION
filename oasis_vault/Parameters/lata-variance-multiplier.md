---
id: param.lata-variance-multiplier
type: parameter
status: stale
domain: ordering
title: LATA variance multiplier
value: computed, not applied
defined_in: `amit_gatekeeper.load_lata_patterns`
feeds: [surface.allocation-priority]
stale_from: measured
---

**Value:** computed, not applied
**Defined in:** `amit_gatekeeper.load_lata_patterns`

LATA's stated purpose is Supplier Shield: inflate safety stock for unreliable suppliers. It does not appear in `calculate_order_quantity`. It feeds allocation priority instead. Finding F3, still open.

## Status history

- 2026-09-04 — `measured` → `stale` — downstream of expired claim.ordering.lata-not-in-safety-buffer
