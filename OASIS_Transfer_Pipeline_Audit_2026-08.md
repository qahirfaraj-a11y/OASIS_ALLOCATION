# Transfer Pipeline — Pre-Test Audit

> **THE STATE OF THE PIPELINE** before testing the algorithm in Odoo. What is
> sound, what misfires, and what to watch during the test.
> Specification: `OASIS_Master_Transfer_Formulae.md` · Engine at `bd5fce38`.

**Date:** 2026-08-20 · audited against the live 14-store Odoo depot.

Findings are ranked by what they would cost you in a live test, and every one
is evidenced against the running instance rather than read off the code.

---

## Verdict

The **algorithm** is sound and testable: allocation is order-independent
(0.00%), the ledger holds, horizons and thresholds are derived rather than
guessed, and the two jobs are cleanly separated.

The **pipeline around it** has four real gaps, and three of them share one
shape: **the scan does not know what is already on its way.** It sees stock and
demand, but not inbound purchase orders, not transfers already approved, and
not the orders the buying engine could not place. In a static depot that is
invisible. In a live test it will produce transfers you do not need — and the
first thing an operator loses faith in is a system that tells them to move
stock that is already arriving.

Fix H1–H3 before the test. H4 and everything below can be watched.

---

## HIGH — fix before testing

> **H1, H2 and H3 are FIXED** in `e008b3d2`. H1's rule changed during the fix:
> an open order **shortens the horizon** rather than cancelling the transfer —
> a delivery in three days does not help a store that is empty today, it still
> loses three days of sales. The sections below are kept as the record of what
> was wrong and how it was proven. **H4 remains open.**

### H1. Inbound purchase orders are invisible to the transfer scan

**Evidence.** `on_order_qty` appears **0 times** in
`consolidated_transfer_service.py`. `OdooAdapter.fetch_enriched_products`
hardcodes `"on_order_qty": 0.0` (line 442). The adapter *has*
`fetch_pending_po_by_sku`, and the ordering path uses it — the transfer path
never calls it. There are 100 open PO lines in the depot right now.

**What misfires.** A store with 500 units landing tomorrow looks exactly as
short as one with nothing coming. The trigger is
`cover < relief`, and relief is the supplier's *typical* cadence from LATA —
not the actual open order. So the engine moves stock to a store whose
replenishment is already in transit, and the receiving store ends up
overstocked while the donor goes short.

This is the precise failure the adapter's own docstring says `on_order_qty`
exists to prevent. It is prevented for ordering and not for transfers.

**FIXED.** `on_order_qty` and `on_order_eta_days` now come from
`fetch_pending_po_by_sku`, and an open order **shortens the relief horizon** to
the delivery date instead of counting as present supply. Treating it as supply —
the first attempt — was wrong: it suppressed the transfer entirely and left the
store losing sales until the pallet arrived. Measured, recipient selling 4/day
on an 18-day horizon: empty with nothing coming moves 72 units; empty with 500
arriving in 3 days moves **12** — exactly the gap; 40 units held with a delivery
in 3 days moves nothing.

### H2. The Odoo path ignores transfers already in flight

**Evidence.** `push_transfer_suggestions.py:104` calls
`scan_network_opportunities()` with **no arguments**. The Command Center path
(`desktop/data.py:1390`) passes `moq_failures=` and `pending_transfers=`.
The two entry points do not scan the same way.

**What misfires.** `_pending_flows([])` yields empty inbound and outbound maps,
so a re-scan re-proposes movements that are already queued or approved as draft
pickings. Approve twice and you ship twice. The scan has explicit machinery for
this and the Odoo path bypasses it.

**FIXED.** The push script now passes both. On this depot: 2 open transfer
lines, 9 MOQ-blocked lines.

### H3. MOQ failures are not fed in on the Odoo path

**Evidence.** Same call site as H2.

**What misfires.** A line the buying engine could not order because it fell
under the supplier's minimum is *exactly* a line worth moving stock for
instead. That is one of the two documented reasons the scan takes inputs at
all. On the Odoo path it never happens, so the engine silently under-serves the
cases with the strongest argument for a transfer.

### H4. Dead-stock detection systematically misses the oldest stock

**Evidence.** `odoo_adapter.py:409` —
`days_since = (now - last_recv).days if last_recv else 0`. `_last_receipt`
reads at most 20,000 moves (line 269), most-recent first.

**What misfires.** A product with **no receipt in the read window** is reported
as `days_since_delivery = 0` — "delivered today". `is_dead` requires
`age >= 90`, so that line can never be classified dead. The rule is exactly
inverted: **the older and staler the stock, the more likely it is to be
missed**, because the older its last receipt, the more likely it fell outside
the 20,000 most recent moves.

Dormant in this depot (767 products at C003 report age 0, but none hold stock).
It will not be dormant on a client with years of history.

**Fix.** Distinguish "no receipt found" from "received today" — carry `None`
and let the dead test treat unknown age as *unknown*, not as fresh. Raising the
limit only moves the cliff.

---

## MEDIUM — know about these during the test

### M1. Sub-unit rounding breaches the donor release cap

PULL calls `_round_transfer_qty`, which **ceils**: given a releasable pool of
0.44 units it ships **1**. PUSH gates on `pool < 1` and ships nothing. Two
passes, opposite answers to the same question. On the uniform network *every*
PULL line was a sub-unit pool rounded up.

Small in absolute terms; it means `RELEASE_FRACTION` is advisory rather than
binding on small lines.

### M2. Read limits truncate silently

`_last_receipt` 20,000 · `_sales_by_product` 200,000 · `fetch_enriched_products`
100,000. None warn on truncation. A chain busy enough to exceed them gets
understated ADS and wrong receipt ages with **no signal at all** — the numbers
just quietly become wrong. Contrast the connector sync, which now logs when it
truncates.

### M3. The scan is not a consistent snapshot

Fourteen sites read over ~30 seconds while tills are selling. C003 is read at
second 2, C010 at second 28; a SKU can sell out in between. Fine for a
suggestion a human reviews — **not** fine as a basis for automation, and worth
remembering when a suggestion looks wrong by a few units.

### M4. Per-store `safety_days` is a dead input

The seed carries `safety_days` for all 14 stores. The transfer service reads it
**0 times** — the safety floor is a hardcoded `ADS x 14` for every store. A
forecourt and a 22,500 sqft anchor are protected identically.

### M5. Approving a stale suggestion fails confusingly

A draft picking reserves nothing, so if the stock has since sold the failure
surfaces at *confirmation*, as an Odoo reservation error with no reference to
OASIS. The 30-minute staleness window is the mitigation, but the operator
experience of the failure is poor.

---

## LOW — watch, do not fix yet

- **Same product and route can appear twice** — once as PULL, once as PUSH.
  Measured 326 of 4,956 routes across the full plan; none in the top 200, so it
  will not show at small limits. Deliberate (see the master spec), but on
  approval it produces two move lines for one product on one picking.
- **Cross-company stores.** The Odoo push path uses the 14-store seed, all in
  one company — safe. `data.py` uses `list_stores()`, which would admit `CHIC1`
  (a second company) if configured; `can_transfer` refuses it, but only at the
  write step.
- **`value_kes` basis differs** — PULL prices at the recipient, PUSH at the
  donor. Identical today because `list_price` is global, divergent the moment
  per-store pricelists are used.

---

## What is genuinely solid

Worth stating, so the test targets the right things:

- **Allocation is order-independent** — 0.00% divergence, reversed and sorted,
  verified on live Odoo data.
- **The donor ledger holds.** One book across all three claimants; a donor
  cannot promise more than it has. Was 1,568 units from a 600-unit donor.
- **Horizons and thresholds are derived**, not chosen — LATA GRN history and
  AMIT category tiers, with operator overrides that are logged as overrides.
- **POS sales count once.** Verified: 7 units in, 7 units read, where the old
  path reported 14.
- **Perishables are never auto-queued**, and approval into Odoo is always a
  draft a human confirms.

---

## Suggested test plan

1. **Fix H1–H3 first** — three small changes, all plumbing rather than
   algorithm. Testing without them measures the wrong thing: you will be
   judging the algorithm on inputs it was never given.
2. **Start read-only.** Run scans, read the queue, approve nothing. Check the
   reasoning against what a buyer would say about the same line.
3. **Then approve a handful on one route** and follow them through
   confirmation and validation in Inventory.
4. **Re-scan after approving** — with H2 fixed, the approved movement should
   NOT be re-proposed. That single check exercises the whole feedback loop.
5. **Watch for**: transfers into stores with an inbound PO (H1), lines whose
   donor "has 0 days cover" (H4), and any suggestion whose quantity is 1 where
   the pool was fractional (M1).
