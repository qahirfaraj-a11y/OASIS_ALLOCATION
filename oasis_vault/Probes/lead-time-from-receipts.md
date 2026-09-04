---
id: probe.lead-time-from-receipts
type: probe
status: measured
domain: ordering
title: Lead time from receipts
entrypoint: devkit/probe_lead_time.py --xlsx /tmp/ff.xlsx --write
guards: []
tests: [claim.ordering.sigma-L-missing, claim.ordering.lead-time-is-observable-per-vendor]
---

**Entrypoint:** `devkit/probe_lead_time.py`

PO date to GRN date, per vendor, across 92,181 receipts. No inter-receipt
  gaps, so neither the ordering habit nor the missing quarter can touch it.

  Writes `oasis/data/supplier_lead_patterns.json` in the shape
  `order_up_to.sigma_lead()` reads, keyed on the supplier name the engine
  actually looks up.
