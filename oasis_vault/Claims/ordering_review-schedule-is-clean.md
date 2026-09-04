---
id: claim.ordering.review-schedule-is-clean
type: claim
status: measured
domain: ordering
title: The review schedule the engine derives contains only real suppliers
worth: R is worth 2.14x working capital; its supplier count was 38% junk
ttl_days: 60
last_evidence: 2026-09-04
source: devkit/probe_supplier_calendar.py
supports: [param.R]
tested_by: [probe.supplier-calendar-integrity]
guarded_by: [trap.silent-join-failure, trap.denominator-sanity]
---

`param.R` comes from the client's declared order calendar. What the engine
actually derived from it was **940 suppliers with a declared order day** — the
figure the derivation quotes — of which **262 were display truncation markers**
like `...AND 123 MORE` and `(BI-WK)`, admitted by a `len(key) > 3` guard.

Separately, twelve real suppliers had names containing a comma and arrived split
across two entries (`SB0179 -` then `BRANDACTIV KENYA SR`). They keyed to
neither half of their own name, so each fell back to the default review period
with nothing said.

The file itself is not a data export — it was parsed out of a rendered report.
The consumer now rejects the artefacts and rejoins the split names; the durable
fix is upstream, at whatever produced the file.

## Status history

- 2026-09-04 — `asserted` → `measured` — probe.supplier-calendar-integrity → supports
