---
id: param.lata-variance-multiplier
type: parameter
status: measured
domain: ordering
title: LATA variance multiplier
value: the MULTIPLIER is computed, not applied — the shield runs on lead_time_stdev instead
defined_in: `amit_gatekeeper.load_lata_patterns`
feeds: [surface.allocation-priority, surface.purchase-order-quantity]
---

**Value:** the MULTIPLIER is computed, not applied (944 suppliers loaded, pinned to 1.0) — the shield itself runs on `lead_time_stdev`
**Defined in:** `amit_gatekeeper.load_lata_patterns`

LATA's stated purpose is Supplier Shield: inflate safety stock for unreliable suppliers. **That purpose is served in ordering today** — but through LATA's measured `lead_time_stdev` entering `d^2*sigma_L^2`, not through `lata_variance_multiplier`, which is pinned to 1.0 and feeds allocation priority only.

Finding F3 is open only in its literal wording; see the correction below. The original sentence — "it does not appear in `calculate_order_quantity`" — is true of the MULTIPLIER and false of LATA, and reading it as the latter is a mistake this note has now caused twice.

## Re-checked 2026-09-13 — the claim holds

The MULTIPLIER half of this note is accurate, unlike `param.R` and `param.sigma_L`, which had drifted. Traced end to end:

- `SimulationOrderUtil.__init__` loads `self._lata_multipliers` from `lata_derived.json` — **944 suppliers** — and **never reads it again**. The only other mentions in `simulation_bridge.py` are comments describing what it used to do.
- `simulation_bridge.py` sets `lata_multiplier = 1.0` outright, commented `NEUTRALISED`.
- `amit_gatekeeper.py` does consume it: `load_lata_patterns()` reads `lata_variance_multiplier`, and the value lands on the allocation node as `node["lata_multiplier"]`.

So the MULTIPLIER is computed, not applied in ordering, and applied in allocation. What the next section corrects is the leap from that to "LATA is not in ordering".

## The shield IS live — a correction to this note

An earlier revision of this note (commit b6a00bd9, same day) claimed the `[LATA Shield: sigma_L=...]` label on 23% of ordering lines was misleading, because the `sigma_L` came from `supplier_lead_patterns.json` rather than `lata_derived.json`. **That was wrong.** The two files are two derivations of ONE source:

- `devkit/probe_lead_time.py` writes `supplier_lead_patterns.json` from PO-date-to-GRN-date receipt history
- `devkit/derive_lata.py` writes `lata_derived.json` from the same `stock_ledger` receipts, `MIN_SAMPLE=8`

Checked rather than assumed: **944 keys in each, 944/944 exact key overlap, and `lead_time_stdev` identical on all 400 sampled overlapping suppliers.** `supplier_lead_patterns.json` carries LATA's lead-time derivation without the `lata_variance_multiplier` / `protection_days` / `review_days` fields.

So the label is accurate and LATA's stated purpose — more safety stock where the supplier is less reliable — **is being served in ordering today**:

| | |
|---|---|
| lines carrying the LATA Shield | 3,460 of 15,037 (23.0%) |
| order lines resolving a LATA-measured sigma_L | **3,166 of 3,472 (91.2%)** |
| suppliers with a derived spread | 944 |

## What is parked, and why that is correct

Only the **multiplier** is unapplied. `lata_variance_multiplier` is a derived scalar; the shield runs on `lead_time_stdev` instead, in DAYS, through `d^2*sigma_L^2` in the quadrature.

That substitution was deliberate and is the better form. Dividing by lead time — the multiplier's shape — makes the lead-time contribution SHRINK as lead time grows, which is backwards: a supplier at L=1 +/-1 day and one at L=10 +/-1 day carry the same absolute exposure, one day of demand.

So F3 is open only in its literal wording. The outcome it asked for is delivered; what remains unapplied is a form that should stay unapplied. Closing it means deciding whether `lata_variance_multiplier` has any job left in ordering — the honest answer looks like "no, `lead_time_stdev` superseded it" — not wiring the old form back in.

One residue worth clearing: `SimulationOrderUtil.__init__` still loads 944 multiplier entries into `self._lata_multipliers` and never reads them. Harmless, but it reads as live wiring to anyone scanning the constructor, and it is the reason this note was mis-read twice.

## Status history

- 2026-09-04 — `measured` → `stale` — downstream of expired claim.ordering.lata-not-in-safety-buffer

- 2026-09-04 — `stale` → `measured` — upstream recovered

- 2026-09-13 — confirmed `measured` — re-traced end to end: the MULTIPLIER is loaded (944 suppliers), never read in ordering, and consumed by `amit_gatekeeper` for allocation priority. Claim unchanged.

- 2026-09-13 — correction — an earlier revision the same day claimed the `[LATA Shield]` label was misleading. It is not: `supplier_lead_patterns.json` and `lata_derived.json` are two derivations of one receipt history, identical on `lead_time_stdev` across all 944 suppliers. The shield is live on 91.2% of order lines.
