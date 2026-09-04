---
description: Run the four promotion checks before a parameter change is allowed to ship
argument-hint: "<claim id>"
allowed-tools: Read, Bash
---

Run the promotion gate on `$1`.

```bash
python devkit/methodology/cli.py gate $1
```

## The four checks — all four, or the answer is no

1. **Held-out** — measured on a window the proposal did not touch.
2. **Baseline** — beats the incumbent, *including the trivial incumbent*. The GNN
   must beat `inventory_risk`; if it does not, it stays a monitoring aid.
3. **Rank check** — `traps.rank_diff` was run and the resulting hierarchy diffs
   clean against the operating docs. A fix that measures clean can still invert
   the design.
4. **Shadow** — ran in shadow mode for N cycles with divergence logged
   (`devkit/shadow_monitor.py`, `shadow_logs/`) before touching a live PO.

## Failing is not rejection

A proposal that fails the gate is `measured` — honest and useful. Only `validated`
and above may change a live weight. Say which check failed and what would clear it.

## The one that is not a coefficient

If the proposal touches `param.R`, stop. Halving working capital requires ordering
twice as often; that is an operations decision about buyer workload, not a number
to set. Classify it `REQUIRES_HUMAN_COMMITMENT` and name the human.
