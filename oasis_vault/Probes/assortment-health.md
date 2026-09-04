---
id: probe.assortment-health
type: probe
status: measured
domain: ordering
title: Assortment health after the purges
entrypoint: devkit/probe_assortment_health.py
guards: []
tests: [claim.ordering.purge-gates-spare-the-hot-nodes, claim.ordering.amit-ranks-on-data-it-mostly-lacks]
---

**Entrypoint:** `devkit/probe_assortment_health.py`

Compares the blocked and allowed populations on observed velocity and value
rather than on line counts, and checks how much of the top value decile each
gate catches.

A gate's share of the catalogue says nothing about its aim. This measures
the aim.
