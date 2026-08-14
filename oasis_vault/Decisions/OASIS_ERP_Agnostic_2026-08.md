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

## Next session — starting points
1. Site-scope `push_purchase_order` (destination warehouse / picking type).
2. Implement the three declared-but-missing contract methods for Odoo:
   `update_po_status`, `fetch_transfers`, `push_transfer_request`.
3. Multi-company support, if the target deployment needs it.
4. Optional, from the iAnalytics analysis: negative-stock + data-quality checks
   in preflight (`CHK_LIST_*` equivalent), and a price-change audit trail.
5. Housekeeping: 5 draft POs (P00009-P00013) left in the `oasis` database.
