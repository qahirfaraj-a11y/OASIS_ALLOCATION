---
id: param.lata-variance-multiplier
type: parameter
status: measured
domain: ordering
title: LATA variance multiplier
value: computed, not applied — 944 suppliers loaded, multiplier pinned to 1.0
defined_in: `amit_gatekeeper.load_lata_patterns`
feeds: [surface.allocation-priority]
---

**Value:** computed, not applied — 944 suppliers loaded, multiplier pinned to 1.0
**Defined in:** `amit_gatekeeper.load_lata_patterns`

LATA's stated purpose is Supplier Shield: inflate safety stock for unreliable suppliers. It does not appear in `calculate_order_quantity`. It feeds allocation priority instead. Finding F3, still open.

## Re-checked 2026-09-13 — the claim holds

Unlike `param.R` and `param.sigma_L`, which had drifted, this note is accurate. Traced end to end:

- `SimulationOrderUtil.__init__` loads `self._lata_multipliers` from `lata_derived.json` — **944 suppliers** — and **never reads it again**. The only other mentions in `simulation_bridge.py` are comments describing what it used to do.
- `simulation_bridge.py` sets `lata_multiplier = 1.0` outright, commented `NEUTRALISED`.
- `amit_gatekeeper.py` does consume it: `load_lata_patterns()` reads `lata_variance_multiplier`, and the value lands on the allocation node as `node["lata_multiplier"]`.

So: computed, not applied in ordering; applied in allocation. F3 stands.

## But the reasoning string says otherwise, on 23% of the book

Ordering output carries `[LATA Shield: sigma_L=0.84d -> safety 2.20d of a 9.0d protection interval]` on **3,460 of 15,037 lines (23.0%)**. An operator reading that would conclude LATA is shielding them in ordering. It is not.

That `sigma_L` comes from `default_patterns()` — `supplier_lead_patterns.json` with `supplier_patterns_2025.json` as a gap-filler — and **not** from `lata_derived.json`. The mechanism is real and the number is measured; only the name is LATA's.

The overlap makes it harder to catch rather than easier: 3,382 of those 3,460 lines DO have a supplier present in `lata_derived.json`, so spot-checking a line would show a supplier LATA knows about, carrying a shield LATA did not compute.

This is worth separating in the label, because the two possible readings differ in what a buyer should do: "your unreliable-supplier shield is live" versus "a measured lead-time spread is inflating this line, and LATA's own multiplier is still parked".

## What closed by another route

LATA's *purpose* — more safety stock where the supplier is less reliable — is now served, through `d^2*sigma_L^2` in the quadrature (see `param.sigma_L`). The multiplier form was retired deliberately: multiplying by `sigma_L / lead_time` makes the lead-time contribution SHRINK as lead time grows, which is backwards. A supplier at L=1 +/-1 day and one at L=10 +/-1 day carry the same absolute exposure.

So F3 is open as written — the multiplier is still unapplied — while the outcome it was asking for arrived from elsewhere. Closing it should mean deciding whether `lata_variance_multiplier` has a job left at all in ordering, not wiring the old form back in.

The dead load is a small trap in its own right: 944 entries read into `SimulationOrderUtil` on every construction and never consulted, which reads as live wiring to anyone scanning the constructor.

## Status history

- 2026-09-04 — `measured` → `stale` — downstream of expired claim.ordering.lata-not-in-safety-buffer

- 2026-09-04 — `stale` → `measured` — upstream recovered

- 2026-09-13 — confirmed `measured` — re-traced end to end: loaded (944 suppliers), never read in ordering, consumed by `amit_gatekeeper` for allocation priority. Claim unchanged; the misleading `[LATA Shield]` label recorded above.
