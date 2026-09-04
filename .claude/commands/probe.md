---
description: Run a probe and record its verdicts as evidence
argument-hint: "<probe id> [extra args]"
allowed-tools: Read, Write, Edit, Bash, Task
---

Run probe `$1` and record what it returns.

```bash
python devkit/methodology/cli.py probe $ARGUMENTS
```

## If the probe has no entrypoint yet

Several probe nodes are placeholders — `probe.residual-cover`,
`probe.term-attribution`, `probe.comparable-store-correlation`. Write them under
`devkit/`, to this contract:

1. **Emit one JSON object per line on stdout** for each claim judged, with at
   least `{"claim": ..., "verdict": "supports|contradicts|inconclusive"}`. Add
   `metric`, `held_out`, `baseline`, `beat_baseline`, `traps`, `notes`. Anything
   else on stdout is ignored, so log freely.
2. **Guard it with the traps it needs**, and list their codes in `traps`:

```python
from devkit.methodology.traps import join_match_rate, ratio, non_circular, run_all
run_all([
    join_match_rate(derived, consumers, "supplier lookup"),
    non_circular("residual cover", inputs, downstream_of_behaviour=["order_gaps"]),
], strict=True)
```

3. **Set `held_out` honestly.** It is the difference between `measured` and
   `validated` — between a number and a licence to move money.
4. **Declare the regime** with `traps.regime(...)` where the metric could be
   mistaken across `initial-load` / `replenishment` / `transfer` / `siting`.
5. Read the source data from the decision ledger where the question is "what did
   the engine decide, and what happened" — `oasis/logic/decision_ledger.py`.

Then register the entrypoint in the probe's vault node and re-run.

Report the verdicts, the status moves, and any blast radius.
