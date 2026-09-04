---
id: probe.order-outliers
type: probe
status: measured
domain: ordering
title: Order outliers by SKU and department
entrypoint: devkit/probe_order_outliers.py --db /tmp/full.db --top 10
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

## Running it over the whole book

    for m in scheduled on_demand; do
      python devkit/procurement_sweep.py --skus all --stores 14 \
        --mode $m --conditions empty-shelf --db /tmp/full.db
    done
    python devkit/probe_order_outliers.py --db /tmp/full.db

`--conditions empty-shelf` runs the one canonical order per SKU per store
rather than the full stock grid. 15,411 SKUs x 14 stores x 2 modes =
**431,508 decisions in 15 seconds**, which is what makes covering the whole
book routine instead of an expedition. The grid stays for property checking,
where the edges are the point.
