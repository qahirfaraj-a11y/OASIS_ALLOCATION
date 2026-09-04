---
id: source.pos-fixtures
type: source
status: trusted
domain: data
title: POS fixture databases
provenance: synthetic
location: `oasis/data/*.db`
---

**Provenance: synthetic** · `oasis/data/*.db`

Seven generated POS databases. They split into two contradictory families: `declared`, `mock_pos_erp` and `mock_pos_erp_lite` agree at rho >= 0.996 (a common generator), while `variant_network` anti-correlates at rho ~ -0.4.\n\nUseful for exercising the pipeline. Fitting a constant against one of them fits the generator, not the geography.
