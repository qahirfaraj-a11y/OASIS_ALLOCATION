---
id: claim.ordering.decision-surface-is-well-behaved
type: claim
status: measured
domain: ordering
title: The ordering engine's decisions obey the properties an order policy must obey
worth: structural soundness across the whole decision surface
ttl_days: 60
last_evidence: 2026-09-04
source: devkit/probe_decision_surface.py
supports: [param.R, param.L, param.sigma_L]
tested_by: [probe.decision-surface]
guarded_by: [trap.denominator-sanity]
---

Not "is the quantity right" — nothing here knows what will sell. The
answerable question: across every SKU, store and stock position the
procurement agents visit, do the decisions behave like an order policy?

| | property | why it must hold |
|---|---|---|
| P1 | monotone in stock | more on hand can never mean a larger order |
| P2 | non-negative | an order is never a return |
| P3 | decomposable | `S = cycle + safety`, unless a clamp says otherwise |
| P4 | on-order credited | stock on the water counts, one for one |
| P5 | covers the interval | an empty shelf is ordered at least `P` days |

A grid rather than a replay, because replaying history only visits the
positions the business happened to be in. The interesting failures live at
the edges, and a grid visits them on purpose.

## Status history

- 2026-09-04 — `asserted` → `measured` — probe.decision-surface → supports
