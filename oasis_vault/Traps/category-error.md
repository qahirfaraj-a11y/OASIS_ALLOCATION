---
id: trap.category-error
type: trap
status: trusted
domain: method
title: Category error across regimes
code: T7
guards: [probe.backtest-allocation]
---

Greenfield allocation is **initial load**, not replenishment. Width first (one pack of everything, ~70% of budget), depth second, then consolidate. Fresh bypasses depth entirely at `Cycle + 0.5 days` to prevent spoilage.

Milk's wallet looked under-used at 11.8% — but fresh is JIT-capped by design, so it was never meant to be drained.

**Detector:** `traps.regime(metric, declared, expected)`. Every metric must declare its regime.
