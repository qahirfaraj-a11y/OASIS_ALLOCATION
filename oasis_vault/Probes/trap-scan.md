---
id: probe.trap-scan
type: probe
status: measured
domain: ordering
title: Loader determinism scan (T2)
entrypoint: devkit/probe_trap_scan.py
guards: []
tests: [claim.ordering.grn-cache-load-is-deterministic]
---

**Entrypoint:** `devkit/probe_trap_scan.py`

Static AST sweep for any `listdir`/`glob` whose result is used to **pick one
file** — indexed, or taken with `next(...)` — without an explicit sort or
mtime rule.

Deliberately distinguishes picking from iterating. Aggregating every GRN workbook
in a directory does not care what order they arrive in; choosing which file backs
a lead time does. An earlier version flagged both and produced 37 hits over
`oasis/logic`, which is how a register teaches people to ignore it.
