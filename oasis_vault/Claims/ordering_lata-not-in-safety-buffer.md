---
id: claim.ordering.lata-not-in-safety-buffer
type: claim
mitigated: true
mitigated_by: F3 closed 2026-09-04
status: falsified
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

- 2026-09-04 — `stale` → `falsified` — probe.pipeline-trace → contradicts

- 2026-09-04 — `measured` → `stale` — TTL expired

## Closed, and badly named

F3 is resolved: the multiplier reaches the replenishment safety buffer, and
against lead-time variance measured independently from 92,181 receipts it
correlates at **rho = 0.93** on the coefficient of variation. LATA measures
what it says it measures.

This claim is phrased as a DEFECT, so falsifying it is good news - and the
graph propagated an alarm for it anyway, because falsified is falsified.
That is a naming fault, not a graph fault. It is marked `mitigated` so the
fall stops here, and the live finding moved to
`claim.ordering.lata-multiplier-is-saturated`.

**Name a claim for the state you want to be true.**
