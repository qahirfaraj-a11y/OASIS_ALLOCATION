---
id: trap.denominator-sanity
type: trap
status: trusted
domain: method
title: Denominator sanity
code: T3
guards: [probe.term-attribution, probe.backtest-allocation]
---

A **43,405% budget overrun** was 434x of one hundred and three shillings.

**Detector:** `traps.ratio(num, den, label)` — any ratio outside [0.1, 10] must print both terms, so the reader can see whether the number is large or the base is small.
