---
id: source.supplier-weekly-schedule
type: source
status: trusted
domain: data
title: Declared supplier order calendar
provenance: observed
location: `supplier_weekly_schedule.json`
---

**Provenance: observed** · `supplier_weekly_schedule.json`

The client's own declared order weekdays. **Observed**, and the input to `param.R` — the largest lever in the ordering engine.\n\nIt is not a data export: it was parsed out of a rendered report, and carries the marks — names split on commas, and 951 truncation markers of the form `...AND 123 MORE`. The consumer now rejects those; the durable fix is upstream, at whatever produced the file.
