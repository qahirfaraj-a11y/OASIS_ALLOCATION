---
id: probe.siting-robustness
type: probe
status: measured
domain: siting
title: Siting robustness
guards: []
tests: [claim.siting.size-exponent-unfitted, claim.siting.distance-decay-unfitted, claim.siting.catchment-km-unfitted]
entrypoint: devkit/siting_robustness.py
---

**Entrypoint:** `devkit/siting_robustness.py`

Perturbs each uncertain input against a real shortlist and reports top-1 held, top-10 overlap, and rank drift. Already written — what it has never had is a **series**. Run every cycle; a recommendation whose stability is falling is a signal even with no outcome label.
