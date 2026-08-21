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

The **pipeline around it** had four real gaps — **all four are now fixed**, and three of them share one
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
> was wrong and how it was proven. **H4 is fixed too** — see below.

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

**FIXED.** Absence from the receipt read is now treated as evidence of age,
not of freshness. Three cases in descending confidence:

1. a receipt was found — exact;
2. the read was **truncated** — the oldest record actually read is a sound
   LOWER BOUND, since nothing missing can be newer than it or it would have
   been in the result;
3. the read was complete and the product still has none — never received here,
   so the product's own creation date is the floor: stock cannot predate its
   record.

Products now carry `days_since_delivery_estimated` so a consumer can tell a
measured age from an inferred one. Truncation logs a warning naming the oldest
date read.

Measured at C003: the 767 products that reported "0 days — delivered today" now
report 1–35 days, and **nothing** reports 0. Forcing the truncated branch (a
200-move window) fires the warning and ages 3,145 products from the window
floor instead of from today.

---

## MEDIUM — know about these during the test

> **M1–M5 are ALL FIXED.** Measured on the 14-store depot, same cached read
> before and after, so the deltas below are the fixes and nothing else.
>
> | | before | after |
> |---|---|---|
> | opportunities | 5,120 | 4,207 |
> | units | 23,377 | 21,979 |
> | value | KES 5,814,652 | KES 4,375,752 |
> | donor/SKU pairs breaching the release cap | **1,321 of 1,725 (77%)** | **0 of 1,287** |
>
> Two results worth carrying forward:
>
> * **PUSH went UP**, 835 → 944, while PULL fell. PULL no longer overdraws, so
>   more excess survives to the PUSH pass — the shared donor ledger working as
>   designed, visible for the first time.
> * **M4 went further than the finding asked.** Wiring `safety_days` only
>   swapped a hardcoded 14 for the seed's hardcoded 10, so the floor is now
>   *derived*: σ = the relief horizon, per store-SKU, from LATA. Final plan
>   **3,590 lines / 22,378 units / KES 3,994,037** — fewer lines than either
>   literal, but more units than the 14-day floor gave. See M4.
>
> One check outside M1–M5 was fixed alongside them: `verify_store_network.py`
> asserted `on_hand == plan - ADS x days_since_delivery`, which holds only on
> the day the depot is seeded and then fails by exactly one ADS per day. It
> read 1,618 of 2,578 SKUs "breaching", every one in the same direction, one
> day after seeding. It now tests that every SKU implies the *same* seed date,
> which is the part of the invariant that survives the depot ageing.

### M1. Sub-unit rounding breaches the donor release cap

PULL calls `_round_transfer_qty`, which **ceils**: given a releasable pool of
0.44 units it ships **1**. PUSH gates on `pool < 1` and ships nothing. Two
passes, opposite answers to the same question. On the uniform network *every*
PULL line was a sub-unit pool rounded up.

Small in absolute terms; it means `RELEASE_FRACTION` is advisory rather than
binding on small lines.

**FIXED.** `_releasable_transfer_qty` is the missing primitive: ceiling is
right when sizing to a recipient's NEED (3.2 units of a boxed item ships as 4)
and wrong when the number is capped by a donor's POOL. Both PULL sites and the
PUSH `_give` now bound the rounded quantity by what is genuinely releasable.

Not as small as "small in absolute terms" suggested: **1,321 of 1,725
donor/SKU pairs were over the cap**, 77% of them, every breach under one unit.
The cap now holds at zero breaches under both a 14-day and a 10-day floor.
PUSH was breaching too, one unit higher up — its `pool < 1` gate stopped the
sub-unit case but nothing stopped a pool of 1.4 shipping 2.

### M2. Read limits truncate silently

`_last_receipt` 20,000 · `_sales_by_product` 200,000 · `fetch_enriched_products`
100,000. None warn on truncation. A chain busy enough to exceed them gets
understated ADS and wrong receipt ages with **no signal at all** — the numbers
just quietly become wrong. Contrast the connector sync, which now logs when it
truncates.

**FIXED.** Every cap is now a named class constant and every capped read is
checked by `_warn_if_truncated`, which logs the model, the site and the
*consequence* — "ADS is UNDERSTATED for this site", "products beyond the cap
are INVISIBLE to ordering and transfers". A fifth read had the same defect and
was not in this list: `_supplier_of` at 20,000, where truncation makes products
fall back to a default lead time instead of LATA's measured rhythm.

Unlike the receipt window these reads are **unordered**, so a truncated result
is an arbitrary subset with no lower bound to reason from — the reason the
warning says what breaks rather than just that it happened.

### M3. The scan is not a consistent snapshot

Fourteen sites read over ~30 seconds while tills are selling. C003 is read at
second 2, C010 at second 28; a SKU can sell out in between. Fine for a
suggestion a human reviews — **not** fine as a basis for automation, and worth
remembering when a suggestion looks wrong by a few units.

**FIXED as far as it can be.** The read cannot be made atomic over XML-RPC, so
the fix is to stop *misreporting* it. Two changes:

* `computed_on` is now the **start** of the depot read, not the moment the
  queue is written. A plan can be no fresher than the oldest reading it was
  built from; stamping it at write time restarted the staleness clock on data
  already minutes old, so the 30-minute window ran from the wrong end. Measured
  on this depot: 24.9 seconds of read, plus engine time, all of it previously
  invisible.
* The span is logged every scan, and a read over two minutes warns explicitly
  that suggestions at its far ends describe stock at meaningfully different
  times.

The dangerous consequence is separately handled by M5: approval now re-checks
availability against live stock, so an inconsistent snapshot can produce a
suboptimal suggestion but no longer an unfulfillable transfer.

### M4. Per-store `safety_days` is a dead input

The seed carries `safety_days` for all 14 stores. The transfer service reads it
**0 times** — the safety floor is a hardcoded `ADS x 14` for every store. A
forecourt and a 22,500 sqft anchor are protected identically.

**FIXED — but read this before trusting the result.** The service now takes
`safety_days_by_org` and applies it at both places that compute excess. Absent
a value the floor is still 14, so a caller that passes nothing gets exactly the
old behaviour; a zero or unparseable field falls back rather than dropping the
floor to nothing. `push_transfer_suggestions` and `verify_store_network` both
pass it, because two entry points scanning one depot with different protection
is the bug class the pending-transfer plumbing was already fixed for.

**Then the input turned out to be worthless, so σ IS NOW DERIVED.** Every one
of the 14 stores seeds the *same* `safety_days` of 10 —
`build_store_network_seed` falls back to a literal 10 and `stores_network.json`
carries no per-store value. Wiring it moved a hardcoded 14 to a hardcoded 10
and nothing else, and 10 is the wrong direction: LATA measures a median relief
of 23 days, so 14 was already below what the supplier book requires.

**σ is now the relief horizon**, `σ(s,i) = R(s,i)`, from the same LATA rhythm
under the same AMIT shelf-life ceiling. The Odoo scripts deliberately pass
**no** `safety_days_by_org` — passing the fixture's uniform 10 would override
the derivation and re-declare the constant it removes — and say so in the scan
log. An explicit per-store policy from a real client ERP still wins.

Measured over 46,830 store-SKUs: **147 distinct values**, min 2.2d, median
17.2d, p90 26d, max 45d. LATA knows the supplier on 39,718 of them; the rest
fall back to the network median. The floor tracks the goods — bread 3.5d,
fresh milk 3.7d, butter 3.7d at the short end; footwear 40.7d, pest control
45d at the long end.

Plan effect: **4,207 → 3,590 lines but 21,979 → 22,378 units**, median line
1 → 2. Deriving σ does not simply shrink the plan, it re-targets it: marginal
lines from donors barely above an arbitrary 14 days disappear, while donors
genuinely past their own replenishment horizon give more.

**The framing above was subtly wrong, and deriving σ shows why.** A forecourt
and a 22,500 sqft anchor *should* hold the same number of DAYS of milk; store
size enters through ADS, so they already hold different numbers of UNITS. σ
differentiates by line, not by building, and that is correct. The real
remaining gap is that `_relief_days` keys on supplier alone, so a site served
on a genuinely different cadence from its siblings is still not distinguished
— that needs per-store GRN history, not a per-store constant.

### M5. Approving a stale suggestion fails confusingly

A draft picking reserves nothing, so if the stock has since sold the failure
surfaces at *confirmation*, as an Odoo reservation error with no reference to
OASIS. The 30-minute staleness window is the mitigation, but the operator
experience of the failure is poor.

**FIXED.** `action_approve` now checks free stock at the donor location before
creating anything, and refuses with a message that names the product, the gap,
the snapshot it was computed from, and the fix (Refresh from OASIS). Quantities
are summed per product first, because one product can legitimately appear twice
on a route — once pulled, once pushed — against one pool of stock.

Live-proven against the depot: an inflated suggestion was refused, the message
named product, gap and remedy, and **no picking was created**. The staleness
window narrows how often this happens and did nothing about what it looked like
when it did; this is that half.

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
