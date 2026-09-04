---
id: trap.circularity
type: trap
status: trusted
domain: method
title: Circular measurement
code: T5
guards: [probe.residual-cover, probe.comparable-store-correlation]
---

A measure fed by the behaviour it measures proves only that the behaviour exists. This caught us **twice** before it was named.

The pattern that works is `--mode residual-cover`: cover carried against the gap the delivery actually had to span, taken **afterwards**.

**Detector:** `traps.non_circular(measure, inputs, downstream_of_behaviour)`.
