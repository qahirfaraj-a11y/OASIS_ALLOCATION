---
id: probe.supplier-calendar-integrity
type: probe
status: measured
domain: ordering
title: Supplier calendar integrity
entrypoint: devkit/probe_supplier_calendar.py
guards: []
tests: [claim.ordering.review-schedule-is-clean]
---

**Entrypoint:** `devkit/probe_supplier_calendar.py`

Asks the real consumer — `order_up_to.load_review_schedule` — what it admits,
rather than reimplementing its rule. A probe that reimplements what it measures
is the F1 pattern: two copies of one rule, drifting apart, then disagreeing
about what the engine does.

Guards with T1 (what fraction of admitted keys are real suppliers) and T3
(admitted vs real, with both terms printed).
