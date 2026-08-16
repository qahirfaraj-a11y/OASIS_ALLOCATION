# OASIS Goes ERP-Agnostic — Odoo Adapter, August 2026

Companion to [[RXL_Integration_Log_2026-08]] and [[OASIS_Port_Method]].
Written 2026-08-13.

---

## The architectural finding that started this

OASIS's ordering intelligence was reachable through **exactly one door**:
`PosErpAdapter`, which requires DIRECT DATABASE ACCESS to the client's POS.

Verified, not assumed: nothing in `oasis/` reads `hub_stock_movement` — the hub
is consumed only by `oasis_hub/analytics.py`, `models.py`, `visibility.py`.

**Consequence: an Odoo client could push data to OASIS and get supplier
analytics, but never Smart Ordering.** That is why RXL was such a slog — it was
the only path that reached the engine, and walking it cost a canonical view
layer, a rewritten schema profile and six product-bug fixes.

## Why ordering was NOT routed through the Cloud Hub

The hub is deliberately supplier-facing:

- `hub_stock_movement` carries **no cost price** — "never raw GRN lines, cost
  prices, or credit terms (those stay in the store and are not modelled here at
  all, so they cannot leak by accident)"
- **no item master** among its 11 tables — no pack size, MOQ, UoM, fresh flag
- its only read route, `GET /portal/movements`, is gated by `require_supplier`
  and filtered through ownership ∩ consent

Feeding ordering from it would mean extending a schema whose entire design
premise is minimal supplier-safe data.

**Decision (operator): the hub stays purely supplier-facing. OASIS reads the
client's ERP directly.**

---

## What was built

### `oasis/logic/odoo_adapter.py`
`PosErpAdapter`'s contract over XML-RPC. Verified against a LIVE Odoo 16
instance throughout — never against documentation.

| Engine input | Odoo source |
|---|---|
| catalogue | `product.product` |
| **cost price** | **`standard_price`** |
| selling price | `list_price` |
| on-hand | `stock.quant` (INTERNAL locations only) |
| **receipt dates** | **incoming `stock.move`** |
| demand | `pos.order.line` **∪** outgoing `stock.move` to customer |
| supplier + lead time | `product.supplierinfo` |
| organisations | `stock.warehouse` |
| PO write-back | `purchase.order` — **DRAFT only** |

**Odoo supplies what neither alternative can**: cost price (hub excludes by
design) and genuine receipt dates (RXL has no `SM_LAST_RECV_DT` at all).

### Two-source demand — a deliberate design choice
Demand is read from POS lines **and** outgoing stock moves. Relying on POS alone
would report zero demand for any client selling via Sales or eCommerce; the
engine would then see a live catalogue with no velocity and behave as if nothing
moves. Same silent-zero shape as the `SM_LAST_RECV_DT` bug, in a new place.

### 3-level product hierarchy (adopting the iAnalytics pattern)
`split_hierarchy()` parses Odoo's category tree into
**Department → Category → Sub-Category**.

This fixed a real bug: `department` had been the ENTIRE path string
(`"All / Saleable / Office Furniture"`), so every leaf looked like a different
department and grouping/budget allocation fragmented. Odoo's synthetic `All`
root is dropped; levels fill forward so any consumer can group at any depth
without null-checking. Live result: **54 real departments** (FRESH VEG CS 27,
MINERAL WATER 26, SODA 21, BREAD 20).

### `--mode erp-status` — because an adapter is otherwise a black box
"No recommendations" could mean a dead connection, an empty catalogue, missing
demand, or unset costs — **each needs a completely different fix**.

```
OASIS_ERP=odoo python entrypoint.py --mode erp-status
```
Reports connection, products, stock, negative stock, demand coverage, cost/price
coverage, suppliers, receipt-date coverage, departments, open PO lines — and
WARNS on the conditions that break ordering silently: zero demand, negative
stock, missing costs, no receipt dates, everything in one department.

### `get_adapter()` is pluggable
`OASIS_ERP=odoo` switches the source. The default `PosErpAdapter` path is
unchanged — verified by the existing suite throughout.

### `generate_smart_orders()` now returns a `funnel`
`products_read / priced_and_enriched / after_network / ordered / below_moq /
no_order_needed / min_order_units`, plus a log line when it returns nothing.
An empty `po_recs` used to be ambiguous — "nothing needed" and "the pipeline
collapsed" looked identical, and that cost real debugging time. Counts only; no
change to what is recommended.

---

## Proven end-to-end

```
Odoo -> OdooAdapter (XML-RPC) -> enriched products (cost + real receipt dates)
     -> ordering engine -> recommendations
     -> DRAFT purchase.orders back INTO Odoo
```

**No schema bridge. No views. No database credentials.**

PO write-back verified by reading Odoo back, not by trusting a return value:
5 POs (P00009–P00013), one per supplier, all `draft`, correct quantities and
unit costs (`Kabras 2Kg` × 826 @ 239.25), totals computed by Odoo.

Draft by design: **OASIS proposes, a human approves in Odoo.** Writing confirmed
POs would commit a client's money without review.

---

## Test data seeded (all from real client data)

300 products chosen where the real catalogue (`oasis_store.db`: name,
department, supplier, sell price, cost, opening stock) overlaps real demand
(`*_cash.xlsx`, parsed with OASIS's own `load_monthly_demand`).
Final state: 345 products, 62 categories, 71 vendors, 160,262 demand moves.

### Seeding error worth remembering
First run reported **296 recommendations / KES 22.3M** — inflated ~10x. Cause:
160k stock-DEPLETING outgoing moves with opening stock set only once, driving
on-hand to **-11,700** on fast movers. The engine was correct; the DATA was
wrong. Fixed by resetting on-hand to the catalogue snapshot (stock is "as of
now", moves are "before now" — same model as the RXL bills).

**A negative on-hand does not error. It silently inflates every order quantity.**

---

## The guards: from structurally dead to firing

Before receipts existed, `days_since_delivery` was 0 for every product, so
`0 > 200` was never true — the dead-stock and stale-fresh guards were
STRUCTURALLY DEAD. The same fail-open state that caused ~KES 20M of phantom
ordering on RXL.

First receipt seed clustered at the extremes (fast 0-10d, dead 220-400d),
leaving the **121-200 band empty** — precisely where stale-fresh (>120d) bites.
Re-spread across the full range, weighted by demand but with deliberate overlap.

Note: the adapter takes `MAX(date)` per product, so ADDING older moves changes
nothing — the previous seed had to be DELETED first.

| | clustered | full-range spread |
|---|---|---|
| `days_since_delivery` 0 | 34 | **8** |
| 1–120 | 289 | 276 |
| **121–200** | **0** | **34** |
| >200 | 22 | 27 |
| guards fired | Dead Stock ×1 | **Stale Fresh ×19, Dead Stock ×6** |
| order lines | 97 | **86** |

### Why only 1 guard fired on the first attempt
Not a defect. The **supplier schedule gate runs BEFORE the guards** — 21 of 22
candidates showed `[Schedule: Gap 7d, Next: Day 231]` and exited early. Both
paths end in "not ordered"; the distinction is only in the reason given.

---

## Final measured state (Odoo)

```
products 345 | with stock 285 | negative stock 0 | with demand 306
with cost 325 | with price 344 | with supplier 322 | receipt dates 311
departments 54 | categories 55 | open PO lines 21
funnel: read=345, ordered=86, below_moq=8, no_order_needed=251
```

---

## What this means beyond Odoo

The adapter surface is **eight methods**:
```
fetch_enriched_products   fetch_all_organizations   fetch_sales_history
fetch_transfers           push_purchase_order       update_po_status
push_transfer_request     update_transfer_status
```
Zoho, Shopify or a CSV drop need only implement those eight. **There is no
schema to bridge** — which was the entire cost of the RXL path.

## Honest limits
- Seeded demand: per-SKU DISTRIBUTION is real; basket composition, timing and
  receipt cadence are modelled.
- Receipt cadence skewed (most SKUs ADS ≥ 1 landed "fast") — fine for exercising
  guards, not a faithful purchasing pattern.
- `update_po_status`, `fetch_transfers`, `push_transfer_request` are declared on
  the contract but NOT implemented for Odoo.
- 5 draft POs (P00009–P00013) were left in the `oasis` database by the test.


---

## MULTI-SITE [2026-08-13] Per-warehouse scoping — a silent correctness bug

**The bug:** `fetch_enriched_products(org_cd)` ACCEPTED an org and IGNORED it.
Stock, demand and receipts were all read company-wide, so every site returned
identical numbers:

```
org=WH      products=345  total_stock=63,232  total_ADS=3531.5
org=CHIC1   products=345  total_stock=63,232  total_ADS=3531.5
```

In a real chain each store would see the whole group's inventory and order as
though it held it — **systematic under-ordering at every site, with no error**.
Same failure family as the other findings this session: wrong answers, silently.

**The fix:** scope `_on_hand`, `_last_receipt` and `_sales_by_product` to the
warehouse's location TREE via a new `_warehouse_scope()`:

  * `child_of view_location_id` — the warehouse ROOT, which covers stock, input,
    quality and output sub-locations.
  * NOT `lot_stock_id` — that is only the default stock location and would miss
    goods sitting in receiving or quality control.
  * on-hand -> quants under the site; receipts -> `location_dest_id` under the
    site; demand -> outgoing moves whose `location_id` is under the site.
  * `org_cd=None` still reads company-wide (single-site installs unaffected).
  * unknown warehouse code logs a WARNING and falls back to company-wide rather
    than returning an empty store.

**Verified after the fix:**

| site | stock | ADS | with receipt date |
|---|---|---|---|
| WH | 63,032 | 3,516.9 | 337 |
| CHIC1 | 200 | 14.6 | 2 |

Genuinely different positions per site.

### Multi-site: what works and what does not
- **Multi-WAREHOUSE in one database — WORKS** (verified above).
- **Multi-COMPANY (`res.company` + record rules) — UNTESTED.** OASIS would need
  company context on the connection.
- **Multiple Odoo INSTANCES (one DB per site) — NOT SUPPORTED.** `get_adapter()`
  caches a single adapter keyed on URL+DB; a per-site registry would be needed.
- **`push_purchase_order` is NOT site-scoped.** It creates POs without setting a
  destination warehouse, so goods land at the default. **Fix this first** before
  any real multi-site deployment.

---

## MULTI-SITE [2026-08-14] The accept-and-ignore bug was in FIVE methods

Picking up item 1 below. `push_purchase_order` was not the only survivor —
grepping every method that takes `org_cd` found the same shape four more times:
the parameter is accepted, never read, and the whole company's answer comes
back with no error.

| method | what unscoped actually costs |
|---|---|
| `push_purchase_order` | Odoo applies the DEFAULT warehouse's receipt type, so a PO computed from one store's stock is **received at another**. Goods arrive at the wrong site. |
| `fetch_sales_history` | passed `days` to `_sales_by_product` but dropped `org_cd` — every site reports identical ADS |
| **`fetch_pending_po_by_sku`** | feeds `on_order_qty`: stock inbound to ONE store **suppresses ordering at EVERY store** in the chain |
| `fetch_pending_pos` | pending-approval counts were chain-wide |
| `_on_hand` / `_last_receipt` | already fixed 08-13; now has regression cover |

POs are scoped by the order's **picking type**, not by a location — that is what
decides where goods land. `_warehouse()` resolves the site once and caches it
(one `fetch_enriched_products` resolves the scope three times over).

**A failed warehouse lookup no longer propagates.** Its callers sit inside
`fetch_enriched_products`' try/except, so an unknown field would have blanked
the entire CATALOGUE and reported it as "no products" — a schema fault wearing
the costume of an empty store. Same silent-zero family as `ACTIVE_FLAG` and
`SM_LAST_RECV_DT`. It now logs "site scoping is NOT in effect" and reads
company-wide.

### VERIFIED against live Odoo 16 (16.0-20250909)

The fix was written while Docker was down, so it shipped on two ASSUMED field
names with a stub test that encoded the same assumption — a green suite proving
only that the code agreed with itself ([[oasis-assumed-api-trap]]). Docker came
up later the same day and both were checked with `fields_get`:

| assumed | real |
|---|---|
| `stock.warehouse.in_type_id` | ✔ many2one -> `stock.picking.type`, "In Type" |
| `purchase.order.picking_type_id` | ✔ many2one -> `stock.picking.type`, "Deliver To", **`required: True`** |

**`required: True` is the whole bug in one flag.** The field can never be
empty, so omitting it does not fail — Odoo fills in the default warehouse's
receipt type. Every PO in the database, P00001-P00013 included, carries
`[1, 'YourCompany: Receipts']`: yesterday's ordering run for any site landed
at WH, silently.

### Measured per-site, through OASIS's own adapter

| | WH | CHIC1 | company-wide |
|---|---|---|---|
| stock | 63,031.8 | 200.0 | — |
| ADS | 3,434.0 | 14.6 | — |
| `fetch_sales_history` | 302 SKUs / 309,056 u | 5 SKUs / 1,310 u | — |
| **`fetch_pending_po_by_sku`** | 16 SKUs / 7,571 | **0** | 16 / 7,571 |

The `on_order_qty` row is the costly one made concrete: unscoped, CHIC1 saw
7,571 units "already on order" that were physically inbound to WH, and would
have skipped replenishing them. `org_cd=None` still reads company-wide, so
single-site installs are untouched.

**Write test — the actual proof:** `push_purchase_order("CHIC1", ...)` created
**P00014, draft, `picking_type=[7, 'Chicago 1: Receipts']`**. Before the fix it
would have read `[1, 'YourCompany: Receipts']` like all thirteen before it.

`--mode erp-status`: 345 products, 285 with stock, 0 negative, 306 with demand,
343 with receipt dates, 54 departments, 22 open PO lines — **no issues detected**.

Unknown warehouse still returns None and warns, so a typo'd site code degrades
to company-wide rather than an empty store.

What the stub tests prove independently: `org_cd` reaches every domain instead
of being dropped. All five bugs fail them on the previous commit (verified by
stashing the fix).

Commits: `ab8e20ac` (the whole RXL + Odoo day, which had never been committed),
`62d9cb4f` (this).

---

## Next session — starting points
1. ~~Site-scope `push_purchase_order`~~ — **DONE and live-verified** (`62d9cb4f`,
   evidence above). Five methods, not one.
2. ~~`update_po_status`, `fetch_transfers`, `push_transfer_request`~~ —
   **ALL DONE and live-verified.** The eight-method contract is covered.
3. Multi-company: partly answered by the transfer work. Reads cross companies
   fine; internal transfers CANNOT and are now refused with a clear reason.
   Real inter-company movement (sale/purchase pair, or a transit location) is
   an unbuilt feature — decide whether any target deployment needs it.
   Note also: warehouse `WH` is NAMED "YourCompany", so its receipt type reads
   "YourCompany: Receipts" — fine as an id, confusing in any UI showing labels.
4. Optional, from the iAnalytics analysis: negative-stock + data-quality checks
   in preflight (`CHK_LIST_*` equivalent), and a price-change audit trail.
5. Housekeeping: draft POs left in the `oasis` database — P00009-P00013 from the
   08-13 write-back test, plus **P00014** from the 08-14 site-scoping proof.
   All draft, all commit nothing.
6. `oasis-odoo-odoo-db-1` had exited (255) while the Odoo container stayed up,
   which looks exactly like "Odoo is broken". Check the DB container first.

---

## GOTCHA [2026-08-14] An Odoo button that SUCCEEDS still raises over XML-RPC

Found while clearing the test POs. `purchase.order.button_cancel` returns
`None`, and Odoo's XML-RPC endpoint dumps responses with `allow_none=False`:

```
TypeError: cannot marshal None unless allow_none is enabled
```

**The cancel had already committed.** Setting `allow_none=True` on OUR
ServerProxy does not help — it is the SERVER serialising the reply. So any
Odoo action method returning `None` looks like a hard failure to the caller
while having fully succeeded, and a naive retry re-runs a write that already
happened.

This matters directly for `update_po_status` (next-session item 2), which will
call `button_confirm` / `button_cancel`. The rule: **never infer an Odoo write
from its return value or its exception — read the record back.** Same
discipline that proved the PO write-back in the first place.

Deletion order also matters: Odoo refuses to `unlink` a purchase order that is
not cancelled ("you must cancel it first"), so the sequence is
cancel -> verify state -> unlink.

## MILESTONE [2026-08-14] `update_po_status` implemented for Odoo

Next-session item 2, first of the three missing contract methods.

### The id-space trap, found before it bit
`po_id` on this contract is whatever `fetch_pending_pos` handed the console —
and on the Odoo path that returns `{"PO_ID": <purchase.order.LINE id>}`, while
PosErpAdapter's `PO_ID` is an `INTEGRATION_PURCHASE_ORDERS` key. **Same
parameter name, two different id spaces.** A "not found, fall back to the store
table" convenience would have hit an unrelated OASIS row with a coincidentally
equal id and reported success. A missing line now returns False and says so.

### The decision that needed making: what does APPROVED mean?
The obvious implementation calls `button_confirm`. **It must not.** The design
premise recorded throughout this document is *OASIS proposes, a human approves
in Odoo* — confirming from the desktop console would commit a client's money
without anyone opening the ERP, from a button labelled "approve" in a different
application. So:

| decision | effect in Odoo |
|---|---|
| APPROVED | applies any quantity override; posts the decision + who made it to the order's chatter; **leaves it draft** |
| REJECTED | removes the line; cancels the order if that emptied it (an empty draft PO is litter that still reads as real) |
| confirmed order | **refused** — returns False, changes nothing |
| unknown status | refused, not guessed — defaulting a typo to "approve" is not a failure mode worth having |

The chatter note is the useful part: whoever confirms in Odoo sees what OASIS
decided and who decided it, without opening OASIS.

### Live-verified against Odoo 16
| scenario | result |
|---|---|
| APPROVED + qty override | qty 10 -> 42, order state still `draft` |
| chatter | "OASIS: approved by qahir. Quantity 10 -> 42. Line: ... Left as draft for confirmation in Odoo." |
| REJECTED, 2 lines | line dropped, 1 left, order still `draft` |
| REJECTED, last line | 0 lines left, order `cancel` |
| unknown status / missing id | False, nothing written |
| REJECT a **confirmed** order | False, line count unchanged — committed spend untouched |

Test POs P00015-P00017 created by this verification were cancelled and deleted;
the database is back to the pre-existing P00001-P00008.

### A Windows detail worth keeping
The approval note first used `→` (U+2192). It goes to the LOGGER as well as to
Odoo, and OASIS logs to a Windows console whose cp1252 codec cannot encode it —
it crashed the verification script on the way past. Log strings stay ASCII.

## MILESTONE [2026-08-16] Transfers implemented — the contract is now covered

`fetch_transfers`, `push_transfer_request` and `update_transfer_status`
(the third came along because without it the console renders transfer rows it
cannot advance). All eight contract methods now exist for Odoo.

### Mapping
| OASIS | Odoo |
|---|---|
| a transfer | ONE internal `stock.picking`, source warehouse's `int_type_id`, `lot_stock_id` -> `lot_stock_id` |
| REQUESTED | picking `draft` |
| IN_TRANSIT | `waiting` / `confirmed` / `assigned` |
| RECEIVED | `done` |
| URGENCY HIGH | `priority = '1'` |
| TRANSFER_ID | **the picking id** — see below |

**One picking per request, not per line.** Odoo's unit of work is the picking;
splitting one shipment into N pickings makes the warehouse pick, pack and
validate N times for one van. Consequently `TRANSFER_ID` is shared by all the
item rows of a shipment, which DIFFERS from PosErpAdapter (one id per item
row). The console only uses the id to identify what to advance, and advancing a
whole shipment is the correct grain in Odoo.

`VALUE_KES` is derived at **cost** here (PosErpAdapter stores a value the caller
passed in) — cost is what actually moves off one store's books onto another's.

### THE FINDING: WH and CHIC1 are DIFFERENT COMPANIES
This instance is Odoo's demo multi-company setup:

- `WH` -> company 1, "My Company (San Francisco)"
- `CHIC1` -> company 2, "My Company (Chicago)"

Odoo **creates** a cross-company internal picking happily and then refuses to
CONFIRM it: *"Incompatible companies on records ... 'Destination Location'
belongs to another company."* Left alone, OASIS would strand a draft that reads
as REQUESTED in the console forever and fails every attempt to advance —
another silent-trap shape. `push_transfer_request` now checks `company_id` on
both ends and refuses up front, naming the reason. Moving stock between legal
entities is a sale/purchase pair in Odoo, not a transfer; implementing that is
a separate feature, not a bug fix.

**This also updates the multi-company entry above.** Reads across companies DO
work (the admin user carries both in `company_ids`), which is why the 08-14
site-scoping numbers for WH vs CHIC1 were real. Writes are the constrained
half.

### Live-verified against Odoo 16
A second warehouse `WH2` was created in company 1 to exercise the same-company
path, since the two that shipped are in different companies.

| step | result |
|---|---|
| cross-company WH -> CHIC1 | refused, **no orphan draft created** |
| push WH -> WH2 | one draft picking, 2 lines |
| fetch_transfers | 2 rows, one shared TRANSFER_ID, `WH`->`WH2`, REQUESTED/HIGH, matched from BOTH ends |
| -> IN_TRANSIT | Odoo `assigned`, stock unchanged |
| -> RECEIVED | Odoo `done`, **stock really moved: WH 70 -> 67, WH2 0 -> 3** |
| refusals | already-done, unknown status, missing id, unknown warehouse, unknown SKU, zero qty — all False |

Two implementation details found only by running it:

1. `_warehouse()` cached only the fields the PO path needed, so `lot_stock_id`
   and `int_type_id` came back empty and the first transfer attempt failed on
   its own guard. Cache the whole set once.
2. `product_uom` is **required** on `stock.move` and is NOT defaulted when the
   move is created through the picking's one2many.

Also: without setting `quantity_done`, `button_validate` answers with an
"Immediate Transfer" WIZARD (a dict) instead of validating — the picking stays
open while the call looks like it worked. Same read-back-the-state discipline.

### Left in the instance
Warehouse `WH2` ("OASIS Test Depot", company 1) and a validated picking that
really moved 3 units of two SKUs from WH. A validated move cannot be undone,
only reversed.

## FIXTURE [2026-08-16] The suite depended on the trial not having lapsed
`test_operations_still_shows_the_order_book_read_only` went red overnight with
nothing changed in the code. `.oasis_install_state.json` records
`first_run: 2026-08-02`, `OASIS_TRIAL_DAYS` defaults to 14, and 08-02 + 14 is
08-16 — the evaluation trial expired, the module gates closed, and the ops view
swapped its order book for "contact iLink to activate Network (Transfers)".

Same family as the `OASIS_POS_DB_URL` fix: ambient machine state leaking into
assertions. A new autouse `_trial_is_not_a_clock` fixture pins `_first_run` to
today, so the suite no longer depends on WHEN it runs relative to the
developer's install date. Tests that are about licensing set their own posture
afterwards and still win.

## HOUSEKEEPING [2026-08-14] Test POs cleared
P00009-P00013 (08-13 write-back proof) and P00014 (08-14 site-scoping proof)
cancelled and unlinked, confirmed by reading back: none remain. The database
holds only the pre-existing P00001-P00008, which were NOT touched — P00004 is
`sent` and P00008 is `purchase`, and confirmed orders are never in scope for
cleanup. Open PO lines 23 -> 17.
