---
id: claim.ordering.lata-multiplier-is-saturated
type: claim
status: asserted
domain: ordering
title: LATA discriminates across most of its range but saturates at the top
worth: 31% of suppliers share one multiplier, and it is 50% above the documented ceiling
ttl_days: 90
source: devkit/probe_pipeline_trace.py
supports: [param.lata-variance-multiplier]
tested_by: [probe.pipeline-trace]
guarded_by: [trap.denominator-sanity]
---

LATA is well-calibrated where it moves. Against the coefficient of variation
of lead time, measured independently from the receipt history on 437 shared
vendors, it correlates at **rho = 0.93**. That is corroboration from a source
LATA never saw, and it is the right comparison: LATA is a variance-RATIO
multiplier, and against absolute spread it only reaches 0.40.

Two things remain wrong at the top of the range:

* **187 of 599 suppliers (31%) sit exactly at 3.0.** Among the worst third
  of the supplier base the multiplier no longer distinguishes anything - a
  merely unreliable supplier and a catastrophic one buy identical cover.
* **The ceiling is undocumented.** The code comment says the shield tops out
  at 2.0x. The data goes to 3.0, and the median supplier sits at 2.59.

The median supplier's safety buffer is therefore multiplied by 2.59 on top of
`base_safety * (1 + 2*cv)`. That is defensible if the ceiling is right and
indefensible if it was raised without anyone writing it down.
