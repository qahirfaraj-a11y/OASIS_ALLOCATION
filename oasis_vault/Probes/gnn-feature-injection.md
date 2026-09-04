---
id: probe.gnn-feature-injection
type: probe
status: asserted
domain: ordering
title: GNN feature injection
guards: []
tests: [claim.gnn.risk-blind-to-stockouts, claim.gnn.dynamic-block-untrained]
---

**Entrypoint:** none yet — this probe is a placeholder.

Inject full stockout into a store (cols 24-25 = 1.0) and toggle payday (col 29) against the live checkpoint; measure the risk delta. Produced -0.00001 and 0.00000 respectively. Re-run on every retrain — this is the check that keeps the gate honest.
