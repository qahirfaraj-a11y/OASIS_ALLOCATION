---
id: claim.ordering.lata-not-in-safety-buffer
type: claim
status: stale
domain: ordering
title: LATA supplier toxicity never reaches the replenishment safety buffer
worth: F3 — open, no owner
ttl_days: 60
last_evidence: 2026-06-19
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.lata-variance-multiplier]
tested_by: [probe.pipeline-trace]
guarded_by: []
stale_from: measured
---

The safety buffer is `base*(1+vol*cv)*GNN_mult` with no LATA term. A toxic-supplier SKU gets no extra replenishment cover. LATA is computed-but-underused for ordering — exactly the shape of the old GNN issue.

Resolve as either: thread it into the buffer, or document explicitly that LATA is allocation-only.

## Status history

- 2026-09-04 — `measured` → `stale` — TTL expired
