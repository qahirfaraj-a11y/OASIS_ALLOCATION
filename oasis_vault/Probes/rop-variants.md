---
id: probe.rop-variants
type: probe
status: measured
domain: ordering
title: Order sizing variants
entrypoint: devkit/probe_rop_variants.py
guards: []
tests: [claim.ordering.statistical-target-beats-the-heuristic]
---

**Entrypoint:** `devkit/probe_rop_variants.py`

Scores the shipped heuristic, the newsvendor reorder point and the derived
order-up-to level on service and capital, restricted to lines that survive
governance so the comparison is like for like.
