---
id: probe.pipeline-trace
type: probe
status: measured
domain: ordering
title: Ordering pipeline trace
entrypoint: devkit/probe_pipeline_trace.py --xlsx /tmp/ff.xlsx
guards: []
tests: [claim.ordering.lata-not-in-safety-buffer, claim.ordering.rop-fallback-always-fires]
---

**Entrypoint:** `devkit/probe_pipeline_trace.py`

Static trace of which intelligence signals actually reach `calculate_order_quantity`. Produced findings F1-F6. Cheap to re-run and catches the whole class of computed-but-unused signals.

## What re-running it found

Both findings had been implemented in code and neither claim had been re-probed,
so both sat stale in the ledger while the code moved underneath them. That is
exactly what the TTL exists to catch.

**F3 is closed, and LATA is well-calibrated.** The multiplier reaches the
replenishment safety buffer. Checked against lead-time variance measured
independently from 92,181 receipts — a source LATA never saw — it correlates at
**rho = 0.93** against the coefficient of variation of lead time on 437 shared
vendors. That is the right comparison: LATA is a variance-ratio multiplier, and
against absolute spread it only reaches 0.40.

What remains is the top of the range: **187 of 599 suppliers (31%) sit exactly
at 3.0**, so among the worst third the multiplier no longer discriminates — and
the ceiling is undocumented, the code saying 2.0.

**F4 is partially closed.** A statistically-correct reorder point exists behind
`OASIS_ROP_MODE`, and the flat fallback announces itself per line instead of
passing silently. But the default is still `heuristic`, so on an unconfigured
install the flat heuristic supplies every reorder point. Mechanism built; not
enabled.
