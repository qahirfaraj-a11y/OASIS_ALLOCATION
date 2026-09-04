---
id: probe.decision-surface
type: probe
status: measured
domain: ordering
title: Decision surface properties
entrypoint: devkit/probe_decision_surface.py --xlsx /tmp/ff.xlsx --skus 300 --stores 5 --db /tmp/oasis_surface.db
guards: []
tests: [claim.ordering.decision-surface-is-well-behaved, claim.ordering.sigma-L-changes-the-order]
---

**Entrypoint:** `devkit/probe_decision_surface.py`

Runs the procurement agents twice over the universe — measured `sigma_L`
  against the chain default — and checks five structural properties over every
  decision, plus whether the term moves the answer.

  Reads the decision ledger, so the same rows Loop B will attribute against are
  the rows the properties are checked on.

## Two ledgers, on purpose

The probe sweeps into a **local** ledger so a cycle finishes inside a shell
timeout: 21,000 decisions take 0.5s against a local SQLite file and 95s against
the network-mounted one, and the difference is filesystem latency, not the
engine.

The **durable** ledger is written by running the sweep on its own —

    python devkit/procurement_sweep.py --skus all --stores 14

— which is what Loop B attributes against. Both write identical rows; only the
destination differs.
