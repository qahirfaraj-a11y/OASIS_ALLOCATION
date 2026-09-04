---
id: claim.ordering.grn-cache-load-is-deterministic
type: claim
status: measured
domain: ordering
title: The GRN intelligence cache is loaded deterministically
worth: lead times feed gap + lead + safety on every line
ttl_days: 30
last_evidence: 2026-09-04
source: devkit/probe_trap_scan.py
supports: [param.L]
tested_by: [probe.trap-scan]
guarded_by: [trap.stale-duplicate-shadowing]
---

`param.L` — lead time — is read from `oasis/data/grn_intelligence_cache.json`.
Which file on disk actually backs that number decides every order quantity for
the supplier, because lead time enters `gap + lead + safety` directly.

`oasis/logic/order_engine.pick_intelligence_file` exists precisely to make that
choice deterministic. Its docstring names the failure it was written to kill: a
Windows Explorer copy leaves `supplier_patterns_2025 (3).json` beside the
canonical file, the space sorts before the dot, and the engine silently loads a
three-week-old duplicate. KAMILI PACKERS carried **21 days against a measured 3**.

This claim asserts that no live loader still bypasses it.

## Status history

- 2026-09-04 — `falsified` → `measured` — probe.trap-scan → supports

- 2026-09-03 — `asserted` → `falsified` — probe.trap-scan → contradicts
