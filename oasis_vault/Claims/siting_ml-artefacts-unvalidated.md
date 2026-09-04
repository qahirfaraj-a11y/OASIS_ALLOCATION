---
id: claim.siting.ml-artefacts-unvalidated
type: claim
status: falsified
domain: siting
title: The siting ML artefacts have predictive validity
worth: neither has seen an outcome
last_evidence: 2026-08-25
ttl_days: 365
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: []
tested_by: [probe.comparable-store-correlation]
guarded_by: [trap.circularity]
---

**Falsified.** The RandomForest is trained on 10,000 rows generated from hand-written formulas; the GCN on a weighted sum of its own input features. No validation split in either. The GCN is a *lossy* re-encoding of a formula that is exact in closed form — and it is a GCN, not a GAT: no attention, no temporal component.

**Permitted claim:** catchment analysis and comparable-store inference. That survives a technical buyer. "AI predicts store success" does not.
