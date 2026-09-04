---
id: probe.order-outliers
type: probe
status: measured
domain: ordering
title: Order outliers by SKU and department
entrypoint: devkit/probe_order_outliers.py --db /tmp/outl.db
guards: []
tests: [claim.ordering.no-absurd-orders, claim.ordering.unscheduled-vendors-carry-phantom-cover]
---

**Entrypoint:** `devkit/probe_order_outliers.py`

Sweeps every decision the procurement agents made for answers a buyer would
refuse, then rolls them up by department — an outlier that appears once is a
line, an outlier that appears across a department is a rule.

Separates **engine defects** (O1-O4) from **assortment facts** (O5 minimum-order
overhang, O6 wild supplier lead time). The distinction is the probe: a line whose
smallest possible order is six months of stock is real and expensive, and it
belongs to the range review, not the order engine.
