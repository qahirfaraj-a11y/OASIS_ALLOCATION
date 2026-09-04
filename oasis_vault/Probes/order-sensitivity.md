---
id: probe.order-sensitivity
type: probe
status: measured
domain: allocation
title: Allocation order sensitivity
guards: []
tests: [claim.allocation.iteration-order-matters]
entrypoint: devkit/measure_order_sensitivity.py
---

**Entrypoint:** `devkit/measure_order_sensitivity.py`

Runs the identical scan with stores iterated in three different orders and compares what each recipient got. Already written.
