---
id: probe.backtest-allocation
type: probe
status: measured
domain: allocation
title: Allocation backtest
guards: []
tests: [claim.allocation.sixty-percent-rule-real]
entrypoint: backtest_allocation.py
---

**Entrypoint:** `backtest_allocation.py`

Allocate -> simulate -> measure fill rate -> repeat. Already written. Guard with `trap.category-error`: this is an INITIAL-LOAD allocator and must not be scored with replenishment metrics.
