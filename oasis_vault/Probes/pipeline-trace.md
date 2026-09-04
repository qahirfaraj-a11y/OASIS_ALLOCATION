---
id: probe.pipeline-trace
type: probe
status: asserted
domain: ordering
title: Ordering pipeline trace
guards: []
tests: [claim.ordering.lata-not-in-safety-buffer, claim.ordering.rop-fallback-always-fires]
---

**Entrypoint:** none yet — this probe is a placeholder.

Static trace of which intelligence signals actually reach `calculate_order_quantity`. Produced findings F1-F6. Cheap to re-run and catches the whole class of computed-but-unused signals.
