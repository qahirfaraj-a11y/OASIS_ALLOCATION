---
id: claim.ordering.newsvendor-rop-is-a-trigger-not-a-size
type: claim
status: asserted
domain: ordering
title: The newsvendor ROP changes whether to order, never how much
worth: explains why flipping the mode moves 0 units at an empty shelf
ttl_days: 120
source: devkit/governance_sweep.py
supports: [param.R, param.L]
tested_by: [probe.governance-sweep, probe.rop-variants]
guarded_by: [trap.category-error]
---

Heuristic and newsvendor give byte-identical orders at an empty shelf, and
that is not a bug — it is the design, stated in the code:

> *The TRIGGER above is deliberately shared, so a difference in the result
> is attributable to the quantity decision alone.*

`OASIS_ROP_MODE` sets the **reorder point**, which decides whether a line
crosses the threshold. The **quantity** comes from the coverage target
below it, and that target is unchanged by the mode. So:

| stock on hand | heuristic | newsvendor | delta |
|---|---|---|---|
| 0 d | 6,313 lines | 6,313 | 0 |
| 2 d | 6,313 | 6,313 | 0 |
| **5 d** | **0** | **5,154** | **+5,154** |
| 10 d | 0 | 0 | 0 |

It is not a decorated heuristic — it changes 5,154 decisions at five days of
cover, catching lines the flat rule misses. But it is a **trigger**, and
calling it 'the statistically-correct reorder point' oversells what it
reaches: the statistics never touch the order size.

The quantity statistic lives behind a different switch entirely,
`OASIS_ORDER_MODEL=order_up_to`. Testing them as one thing was the error;
the real design space is 2x2, trigger x quantity model, and only the
trigger half has been swept.
