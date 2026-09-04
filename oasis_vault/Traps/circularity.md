---
id: trap.circularity
type: trap
status: trusted
domain: method
title: Circular measurement
code: T5
guards: [probe.residual-cover, probe.comparable-store-correlation]
---

A measure fed by the behaviour it measures proves only that the behaviour exists. This caught us **twice** before it was named.

The pattern that works is `--mode residual-cover`: cover carried against the gap the delivery actually had to span, taken **afterwards**.

**Detector:** `traps.non_circular(measure, inputs, downstream_of_behaviour)`.

## Second detector — is the measure OBSERVED at all?

Circularity asks whether a measure is fed by the behaviour it measures. The
sibling question is whether the behaviour happened. Both produce a number that
correlates with something and means nothing.

    traps.looks_generated(records, name, date_key=..., group_key=...,
                          id_key=..., low_cardinality={...})

Tells, any two of which condemn a source:

* identical record count per period
* perfectly regular spacing between periods
* a single value where reality has many — one till, one payment mode
* identifiers running 1..N with no gaps

**The worked example is on this install.** Every one of the seven POS databases
is a fixture. `variant_network.db` holds 17,244 sales headers — twelve dates
exactly five days apart, an identical bill count per store per day, one counter,
one payment mode, no customers. Row count reads as evidence and is not.

**And agreement between labels is not corroboration.** `declared`,
`mock_pos_erp.db` and `mock_pos_erp_lite.db` agree at rho >= 0.996 because they
came out of the same generator; `variant_network.db` contradicts all three at
rho ~ -0.4. Spread across pairs: 1.433. Two supposedly independent measures
agreeing perfectly is a tell, not a result.
