---
id: probe.siting-robustness
type: probe
status: measured
domain: siting
title: Siting robustness
guards: []
tests: [claim.siting.recommendation-sensitive-to-distance-decay, claim.siting.recommendation-sensitive-to-size-exponent, claim.siting.recommendation-sensitive-to-catchment-km]
entrypoint: devkit/probe_siting_robustness.py
---

**Entrypoint:** `devkit/siting_robustness.py`

Perturbs each uncertain input against a real shortlist and reports top-1 held, top-10 overlap, and rank drift. Already written — what it has never had is a **series**. Run every cycle; a recommendation whose stability is falling is a signal even with no outcome label.
