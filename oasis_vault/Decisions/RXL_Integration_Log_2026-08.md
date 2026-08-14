# RXL / iRetail Integration Log — August 2026

Running log of milestones, key events and findings while porting OASIS onto a real
RXL (Polaris iRetail) POS. Companion to [[Log]].

---

## MILESTONE [2026-08-13] Console login achieved
iRetail Business Suite Console Manager loads with all modules reachable:
iBASE, iPOS, iFINANCE, iREPORT, iBARCODE, iANALYTICS, iCXO Dashboard,
iUTILITY, iL&P, and Admin Apps (iSUPPORT, iHERMES, iARCHIVER, iPMS).

Credentials: `ADMINISTRATOR` / `rdt` — Level `1`, Org `001`, Language `ENGLISH`.
Password is stored Base64 (`cmR0`) in **both** `NBT_USER_MST.PASSWD` and
`EMPLOYEE_MST.EMP_PWORD`. Do not edit one without the other.

Environment: SQL Server 2019 in Docker (`sql_retail`, port 1433), database
`TESTING11`, restored from `Base\Database Dump\RetailExcel.bak`.
Container now has `--restart unless-stopped` (it silently stayed down after a
Docker restart and made the whole stack look broken).

---

## FINDING [2026-08-13] RetailExcel.bak is an OLDER schema than the binaries
Not corrupt — behind. Each gap surfaced only when a code path touched it, and
killed the app with no useful message. Traced every one with an Extended Events
capture and fixed against the app's literal SQL.

| Missing | Kind | Fix |
|---|---|---|
| `LANGUAGE_MST` | table | created (empty is enough — see below) |
| `USER_INFO.UI_MODULE` | column | `varchar(10)` |
| `USER_INFO.LAST_UPDATED_DATE` | column | `datetime` |
| `USER_INFO.LAST_UPDATED_TIME` | column | `varchar(12)` |
| `SYSTEM_PROFILE.SP_SECURITY_PATTERN` | column | `varchar(1)` |
| `UDP_DESIGN_MST.LAST_UPDATED_TIME` | column | `varchar(12)` — found proactively |
| `ORG_LOGO_IMAGE_MST` | table | created |

**Language dropdown**: the app's query is
`SELECT 'ENGLISH' As LNG_CODE, 'ENGLISH' as LNG_DESC UNION ALL SELECT LNG_CODE, LNG_DESC FROM LANGUAGE_MST (NOLOCK)`.
It **hardcodes an ENGLISH row** — the table only has to *exist*. Because the
table was missing, the whole statement errored and the combo came back empty,
which cascaded into `SelectedIndex '0' is not valid` then
`Object reference not set`. One root cause, three different error dialogs.
An earlier attempt to insert English into `NBT_CURLAN_MST` was the wrong table.

**Proactive sweep**: 464 of 465 tables carry both `LAST_UPDATED_DATE` and
`LAST_UPDATED_TIME`. Exactly one was asymmetric (`UDP_DESIGN_MST`) and is fixed.
That asymmetry is the drift signature — worth re-running after any restore.

**COM**: `Base64.dll` (CLSID `{28656ABB-8E12-11D2-950F-000000000000}`) was
unregistered — the `80040154` error blocking the login crypto path. 155
components now registered via 32-bit `regsvr32` (`C:\Windows\SysWOW64`).
109 correctly skipped as non-COM/.NET. One timeout, `GridEX20.ocx`, verified
harmless: it lives in the `BackUpRXL` archive folder, neither `RXLPos.exe` nor
`iRetailConsole.exe` reference GridEX, and `GridEX16` is already registered.

---

## CORRECTION [2026-08-13] RXL date/time storage — do not trust the first reading

I initially reported that *"RXL stores dates as `'13/Aug/2026'` strings, so date
arithmetic on `BILL_DT` is unsafe."* **That was wrong** and was nearly logged as
fact. Verified against declared column types:

| Column | Actual type |
|---|---|
| `POS_SALES_DTL.BILL_DT` | **`datetime`** |
| `POS_SALES_HDR.BILL_DT` | **`datetime`** |
| `POS_SALES_HDR.BILL_TIME` | **`varchar(8)`** |
| `LAST_UPDATED_DATE` / `LAST_UPDATED_TIME` | `datetime` / `varchar(12)` |

`BILL_DT` is a real datetime — **date arithmetic is safe**. The `'13/Aug/2026'`
form is how the *application writes literals* (dd/MMM/yyyy), parsed by SQL Server
on the way in. That is a locale/`DATEFORMAT` concern for anything **we write**,
not a storage concern for what we read.

**Two real consequences survive the correction:**

1. **There is no time-of-day in `BILL_DT`.** The time lives in a separate
   `BILL_TIME varchar(8)` on the *header*. Our `POS_SALES_DTL` view does not map
   it, so any intraday or hourly analysis needs `BILL_TIME` joined from
   `POS_SALES_HDR` — otherwise every sale silently collapses to midnight.
   **Check this once the first real sale is rung.**
2. If OASIS ever *writes* to RXL, date literals must be locale-safe
   (`YYYYMMDD` or parameterised), never `dd/MMM/yyyy`.

**Lesson**: verify a column's declared type before recording a claim about it.
A captured SQL literal shows how the app *writes*, not how the column is *stored*.

---

## FINDING [2026-08-13] Login session tracking is per-module
The login path runs
`DELETE FROM USER_INFO WHERE ... AND UI_MODULE = 'M'` then re-INSERTs.
RXL tracks concurrent logins **per module** (`M` here; presumably `B`/`P` for
Base/POS) — related to `MULTI_USER_LOCK`. If OASIS ever holds a long-lived
connection it must stay out of `USER_INFO`, or it will look like a phantom
logged-in session and may block a real operator.

---

## OPEN [2026-08-13] Store hierarchy — blocks the POS
- `ORGANIZATION_MST` has **one** row: `001` "Polaris Retail Infotech Ltd." at
  `ORG_LEVEL_NUMBER = 1`. That is the **company**, not an outlet.
- `COUNTER_MST` = 0 rows. `RXLPos.exe` binds a session to a till and **cannot log
  in without a Counter**.
- `ITEM_MST` = 0, `STOCK_MASTER` = 0.

Dependency chain: **store org -> counter -> items -> stock -> sale.**

The OASIS views are currently generated for `OASIS_STORE_LEVEL=1`. Once a real
store exists at level 2+, **regenerate them for that level**.

---

## MILESTONE [2026-08-13] Virtual store seeded — OASIS reads live RXL data

Pivoted to Path B (seed via SQL) because `Rxlbase.exe` / `RXLPos.exe` need
`COMDLG32.OCX`, which is absent from the machine and not bundled. Rather than
block on obtaining a Microsoft redistributable, the store was written directly
into RXL in RXL's own column shapes.

Seeded from the OASIS stock snapshot (`oasis/data/oasis_store.db`):

| Table | Rows |
|---|---|
| ORGANIZATION_MST | +1 — `002` "OASIS Virtual Store" at **level 2** under company `001` |
| COUNTER_MST | 1 (Till 1) |
| DIVISION_MST | 247 (departments) |
| VENDOR_ADDRESS_MST | 823 (suppliers) |
| ITEM_MST | 39,728 |
| BASIC_SP_MST / BASIC_CP_MST | 39,728 each |
| STOCK_MASTER | 39,728 (18,118 with qty > 0) |

`SYSTEM_PROFILE.SP_ORG_MAX_LEVEL` raised 1 -> 2 with `SP_ORG_LEVEL2_NAME='STORE'`;
RXL shipped with a company-only hierarchy and would not have accepted a store.

**Schema constraints that shaped the mapping** (found in the live schema, not docs):
- `DIV_ID` and `VAM_CD` are `varchar(3)` — 247 departments and 823 suppliers had
  to compress to 3-char codes (`001`..`999`). Any client with >999 of either
  cannot be represented 1:1 and needs a different strategy.
- `*_LEVEL_NUMBER` is `numeric(1,0)` — the store tier must be a single digit.
- prices are `numeric(18,8)`, stock `numeric(13,3)`.
- `BCP_VEND_CD` on BASIC_CP_MST carries the item/vendor link. This is the exact
  thing the original profile got wrong by assuming `ITEM_MST.VENDOR_CD`.

Views regenerated with `OASIS_STORE_LEVEL=2`. **OASIS's real adapter join now
returns live RXL rows** — 18,118 items with stock, sell price, cost price and
supplier, e.g. `Tusker Can 500Ml Beer / BEER / qty 1,465 / 229.00 sell / 187.78 cost`.

**Fidelity caveat**: `VAM_NAME` currently holds the OASIS supplier *code*
(`SUP00585`) because the snapshot's ITEM_MST carries codes, not supplier names.
Cosmetic for ordering logic, but it should be backfilled from a supplier master
before anything customer-facing uses the name.

**Not proven by this**: no sale has been rung. The POS still needs `COMDLG32.OCX`
(Path A). Seeded rows also bypass RXL's own code-generation (`FINANCE_CTRL_SEQ`),
so RXL may not consider these items fully "its own" when transacting.

---

## MILESTONE [2026-08-13] 90 days of sales seeded; supplier names backfilled

**Supplier backfill**: `VAM_NAME` held the OASIS supplier *code*; matched exactly
against `SUPPLIER_MST` in the snapshot and replaced with real names. Zero rows
still hold a code. `KENYA BREWERIES LTD`, `POLYTHENE INDUSTRIES LIMITED` etc. now
resolve through `OASIS.SUPPLIER_MST`.

**Bills**: 42,272 bills / 120,437 lines / 1,970 distinct SKUs, spanning
`2026-05-15 .. 2026-08-12` — exactly 90 days, meeting preflight's
`MIN_SALES_HISTORY_DAYS = 90`. ADS now computes off live RXL data
(e.g. *Cartoons 30G Perfectly Salted* — 3,280 units, ADS 36.44).

Design decisions, deliberate:
- **Demand is not invented.** Per-SKU weights derive from the 30 days of real
  history in the snapshot, preserving the velocity curve (few fast movers, long
  tail). 1,970 of 39,728 SKUs move — realistic for a supermarket.
- **STOCK_MASTER is NOT decremented.** Stock is "as of now", bills are "before
  now". Retro-decrementing double-counts and drives fast movers negative.
- **Dates written `'20260515'`**, never `dd/MMM/yyyy` — applying the locale rule
  from the BILL_DT correction above.
- **VAT treated as inclusive** (16% extracted from shelf price), so
  `NET_VALUE + TAX_AMT = TOTAL_AMT` reconciles.
- Today left empty, as a live till would be at start of day.

---

## BLOCKER [2026-08-13] OASIS ships with NO SQL Server driver

`requirements.txt` carries `pyodbc` **commented out**:
`# SQL Server Integration (Optional - install only if using MSSQL)` /
`# pyodbc==5.2.0`. It is absent from `.oasis_venv`.

RXL — and most mid-market POS/ERP — run on SQL Server. So the **shipped OASIS
client cannot connect to a SQL Server POS out of the box**, despite the adapter,
schema profile and `--mode build-views` all being built for exactly that case.
The client-facing consequence: a client following the documented onboarding
gets as far as `build-views`, then cannot connect.

Two things gate the live test:
1. `pyodbc` must be installed (a real dependency decision, not an accident to
   paper over — it should probably move out of the commented block).
2. A SQL login with `DEFAULT_SCHEMA = OASIS` must exist, so the adapter's
   unqualified reads resolve to the views rather than raw `dbo` tables with the
   wrong column names.

---

## MILESTONE [2026-08-13] OASIS preflight PASSES against real RXL

`oasis_ro` login created with `DEFAULT_SCHEMA = OASIS`. Proven via `EXECUTE AS`
that **unqualified** queries resolve to the canonical views:
`FROM ITEM_MST` -> 39,728 rows, and `DEPARTMENT`/`ACTIVE_FLAG`/`SUPPLIER_CD`
resolve — none of which exist on `dbo.ITEM_MST`. The default-schema strategy
works with **zero adapter changes**.

Preflight result: all 8 required tables PASS with required columns.
Two WARNs, both correct: `GRN_HDR` has no RXL equivalent (recommended only),
and history reads 89 days because today is deliberately empty (a live till
closes that on its first sale). OVERALL: WARN.

---

## FOUR PRODUCT BUGS found only by running against a real POS

Every one of these was invisible to the SQLite mock, and each would have hit a
client on day one.

**1. Preflight could not see VIEWS.** It called only `get_table_names()`. On RXL
the canonical set is **8 views and 0 tables**, so preflight reported *every
required table missing* on exactly the installs we tell clients to build with
`--mode build-views`. OASIS's own documented porting strategy was invisible to
its own validation. Fixed: inspects views too, and degrades per-object instead
of aborting the whole check.

**2. Legacy ODBC driver broke schema inspection.** The in-box `SQL Server` driver
(often the only one on a client machine) fails SQLAlchemy's setinputsizes with
`HY104 Invalid precision value (0)` before reading a single table. Fixed:
`db_connector` detects that driver and disables setinputsizes, logging a
recommendation for ODBC Driver 17+.

**3. The adapter's SQL was SQLite-only.** `fetch_sales_intelligence` used
`SUBSTR(d.BILL_DT, 1, 7)` — doubly wrong on a real POS: SQL Server has no
`SUBSTR` at all, and `BILL_DT` there is a **DATETIME**, not the `'YYYY-MM-DD'`
TEXT the mock stores, so string-slicing is meaningless. Fixed with a dialect-aware
`_year_month_expr()` (mssql / postgres / mysql / sqlite).

**4. ACTIVE_FLAG was mapped by NAME but not by VALUE.** RXL marks an active item
`ITM_STATUS='O'` (open); the OASIS contract means `'Y'`. Mapping the column
straight through gave a view with 39,728 rows that the adapter then filtered to
**ZERO** on `WHERE ACTIVE_FLAG='Y'`. No error — just a silent empty result, the
worst failure mode there is. Fixed in the view:
`CASE WHEN i.ITM_STATUS='O' THEN 'Y' ELSE 'N' END`.

**The lesson from #4 is the important one**: mapping a schema means mapping the
**vocabulary**, not just the column names. A structurally perfect mapping can
still return nothing. Any future POS profile must be validated on VALUES —
"does the adapter's filter actually match rows?" — not merely on column presence.

---

## CRITICAL [2026-08-13] One NULL column silently disabled the dead-stock guard

**Chain**: RXL has no `SM_LAST_RECV_DT` on STOCK_MASTER -> the profile mapped it
to `NULL` -> the adapter set `days_since_delivery = 0` for all 39,728 SKUs ->
the guard `if days_since_delivery > 200` was NEVER true -> both the DEAD-STOCK
and STALE-FRESH blocks were inert.

Effect on a live order: **7,325 recommendations / KES 23.9M**, of which 86% of
value was SKUs with NO demand history, and **6,837 lines were for items with
zero stock AND zero sales**. No error, no warning; the reasoning strings looked
confident (`Key SKU Boost +20%`, `DDoS Target: 23.80d`).

**It fails OPEN.** A disabled guard here over-orders. On a client that is a
multi-million-shilling PO of dead stock, generated silently.

### CORRECTION to my earlier claim
I previously wrote that RXL "has no equivalent" for receipt dates. **That was
wrong.** Prompted by the iAnalytics report catalogue (GRN_REGISTER,
SP_TRACE_ITEM_GRN, LPP tracking), I searched the real schema and found:
- `ITEM_MST.ITM_LAST_PURCH_DT` (datetime) — per item
- **`ITEM_MOVEMENT_DATE`** — a per-store, per-item, per-DAY movement ledger:
  OPENING_QTY, RECEIVED_QTY, ISSUE_QTY, SOLD_QTY, RETURN_QTY, STK_ADJ_QTY,
  TRANSFER_IN/OUT_QTY, CURRENT_DAY_STOCK_QTY
- siblings: ITEM_MOVEMENT, ITEM_MOVEMENT_MONTH (+ _PERIODS)

They were empty here only because WE seeded this store and never populated them.
A trading client populates them via RXL's own processes.

### Fix
`STOCK_MASTER` view now derives the date:
`COALESCE(MAX(ITEM_MOVEMENT_DATE.TXN_DATE WHERE RECEIVED_QTY>0), ITEM_MST.ITM_LAST_PURCH_DT)`

With realistic last-purchase dates seeded, the guard fires and the order drops:

| | guard disabled | guard active |
|---|---|---|
| recommendations | 7,325 | **4,015** |
| order value | KES 23.9M | **KES 13.5M** |
| zero-stock + zero-demand lines | 6,837 | 3,588 |

**KES 10.4M of dead-stock ordering removed by one column mapping.**

### Still open
3,588 zero-stock/zero-demand lines survive the guard (they fall under the
200-day threshold, or are halo/fresh protected). The policy question stands:
*should a SKU with zero stock AND zero 90-day sales be replenished at all,
regardless of receipt date?* That is an ordering-economics decision, not a
mapping one.

### Lessons that generalise beyond RXL
1. **An unmappable field can disable a safety rule silently.** Preflight checks
   that a column EXISTS, never that it is POPULATED or that the rule it feeds
   still functions. A guard keyed on a NULL field is worse than no guard —
   it looks present.
2. **Fail-open vs fail-closed matters.** Every threshold of the form
   `if X > N` where X defaults to 0 is inert on any client that cannot supply X.
   Worth auditing every guard for this shape.
3. **"The client does not have it" deserves a second look.** RXL had the data
   in a table we had not thought to inspect. Search the schema for the
   CONCEPT (received / purchase / movement), not for our column name.

---

## MILESTONE [2026-08-13] Real Rhapta Road demand loaded; ordering on genuine data

Replaced the synthetic bills with demand derived from the client's own
`*_cash.xlsx` (7 files, 7 months, 2.18M units), parsed with OASIS's **own**
`load_monthly_demand` rather than a private reimplementation. Names matched to
the RXL catalogue on a normalised key: **82.3%** (18,865 of 22,916 SKUs).

| | synthetic | real demand |
|---|---|---|
| SKUs with demand | 1,970 | **18,836** |
| bills / lines | 42,272 / 120,437 | **168,987 / 425,627** |
| distinct SKUs sold | 1,970 | **15,524** |

Demand profile is now recognisably a Kenyan supermarket: *Ilara Fresh Milk 500Ml*
ADS 157, *Tuzo 500Ml* 84, carrier bags and fresh veg behind them.

### CORRECTION to the earlier "86% no demand" claim
That figure came from the synthetic seed, where only 5% of the catalogue had any
history. On real demand it is **53.2%**, and the demand-driven side went from
**42 lines / 24.2% of value** to **1,623 lines / 46.8%**. The composition claim
was an artifact of thin test data, not an engine defect — as flagged when it was
first reported, and now measured rather than assumed.

### Policy implemented as an OPT-IN threshold (no Golden Logic drift)
Operator policy: an item with zero stock and no 90-day sales is dead or
discontinued and must never be replenished.

Implemented as `thresholds['block_discontinued']`, **defaulting to False**, using
the engine's existing tuning mechanism (the same place `dry_dead_days` and
`dry_dead_min_sales` live) rather than a new hardcoded branch. A parity test
asserts the rule does NOT fire by default, so shipped behaviour is unchanged.

Rationale for keying on stock+sales rather than `days_since_delivery`: that field
is unavailable on some POS backends (RXL has no `SM_LAST_RECV_DT`), and when it
defaults to 0 the existing `>200d` guard is silently inert. Stock and sales are
data every POS can supply, so this rule cannot be disabled by a missing column.

### Caveats on the seeded volume
The cash files give monthly **units per SKU**, not transactions. So the per-SKU
demand DISTRIBUTION is real; bills/day (1,681), basket composition and intraday
timing are modelled. Fine for exercising the engine, not a substitute for real
transaction data.

### Available and not yet used: `sales_forecasting_2025 (1).json`
A 24,004-SKU ledger with `monthly_sales` (Jan-Oct), `total_10mo_sales`,
`months_active`, `avg_daily_sales`, `trend` and `trend_pct`. Two uses:
1. **Validation** — compare OASIS's computed ADS (read from RXL through our
   views) against this independently-derived ADS. Nothing so far has checked
   whether the numbers are RIGHT, only that they resolve and produce output.
   Correlation and rank matter; absolute values will differ (different windows).
2. **Seasonality** — regenerate bills weighted by real monthly variation instead
   of a flat rate + weekend uplift. Do this AFTER validation, since regenerating
   destroys the only independent yardstick.

---

## VALIDATION [2026-08-13] OASIS's ADS agrees with an independent ledger

First check of whether the numbers OASIS derives from RXL are **right**, not just
whether they resolve. Compared the ADS the ADAPTER produces (what the ordering
engine consumes, read from RXL through the canonical views) against
`sales_forecasting_2025 (1).json`, derived independently. Matched on OASIS's own
shipped `normalise_name`, not a private normaliser.

| metric | result |
|---|---|
| ledger SKUs (normalised) | 23,673 |
| matched to RXL by name | 20,355 |
| both non-zero (compared) | 15,516 |
| **Spearman rank correlation** | **0.883** |
| Pearson linear correlation | 0.981 |
| top-50 overlap | 47/50 (94%) |
| top-200 overlap | 185/200 (92%) |
| top-1000 overlap | 915/1000 (92%) |
| median ratio OASIS/ledger | 1.28x |

**Rank agreement is what matters here** — absolute values cannot match, because
the ledger spans Jan-Oct while our window is a 90-day slice, and the bills were
generated from the cash files rather than this ledger. On the question that
drives ordering — *which SKUs are the fast movers* — the two agree on 92-94% of
the top N.

Read plainly: the chain **RXL tables -> canonical views -> adapter SQL ->
aggregation -> ADS** preserves demand signal faithfully. That is the first
end-to-end correctness evidence in this integration; everything before it only
proved the pipeline ran.

### Caveat, honestly
The largest relative disagreements are all OASIS reading ~7x high on
low-volume items (ledger 0.058-0.200/day). At those magnitudes ratios are noisy,
and two other explanations are live: our name-normalised keys can collapse
distinct SKUs (we take max() on collision), and the seeded bills sample the cash
distribution rather than reproducing it exactly. Median across all 15,516 is
1.28x, so this is a tail effect, not a systematic bias. Worth revisiting if
low-volume accuracy ever matters.

### Next
Re-seed the bills using the ledger's `monthly_sales` for real seasonality
(currently a flat rate + weekend uplift). Deliberately sequenced AFTER this
validation — regenerating from the ledger would have destroyed the only
independent yardstick.

---

## MILESTONE [2026-08-13] OASIS reads Odoo directly — ERP-agnostic ordering

### The architectural finding that prompted this
OASIS's ordering intelligence was reachable ONLY through `PosErpAdapter`, which
needs DIRECT DATABASE ACCESS to the client's POS. Verified: nothing in `oasis/`
reads `hub_stock_movement` — the hub is consumed only by `oasis_hub/analytics.py`,
`models.py`, `visibility.py`.

So: **an Odoo client could push data to OASIS but could not get Smart Ordering.**
That is why RXL was such a slog — it was the only path that reached the engine,
and it cost a schema bridge, a view layer and six bug fixes.

### Why not route ordering through the hub
The hub is deliberately supplier-facing. `hub_stock_movement` carries no cost
price ("never raw GRN lines, cost prices, or credit terms — those stay in the
store") and there is no item master among its 11 tables. Its only read route,
`GET /portal/movements`, is gated by `require_supplier` and filtered through
ownership ∩ consent. Feeding ordering from it would have meant extending a
schema whose entire design premise is minimal supplier-safe data.

**Decision: the hub stays purely supplier-facing. OASIS reads the ERP directly.**

### `oasis/logic/odoo_adapter.py`
Implements PosErpAdapter's contract over XML-RPC. Verified against a LIVE
Odoo 16 instance (db `oasis`), not documentation:

| Engine input | Odoo source | Live result |
|---|---|---|
| catalogue | `product.product` | 45 active products |
| **cost_price** | **`standard_price`** | e.g. Coke 1L = 71.25 |
| selling price | `list_price` | ✓ |
| on-hand | `stock.quant` (internal locations only) | e.g. 450 units |
| **days_since_delivery** | **`stock.move` (done, incoming)** | e.g. 28 days |
| supplier + lead time | `product.supplierinfo` | e.g. "Ready Mat" |
| organisations | `stock.warehouse` | WH, CHIC1 |
| PO write-back | `purchase.order` (DRAFT only) | module installed this session |

**Odoo gives us MORE than either alternative**: cost price (the hub excludes it
by design) and genuine receipt dates (RXL has no `SM_LAST_RECV_DT` at all, which
is what silently disabled the dead-stock guard). No schema bridge, no views, no
database credentials.

`get_adapter()` is now pluggable via `OASIS_ERP=odoo`; the default PosErpAdapter
path is unchanged (51 tests pass).

### Proven end-to-end
`adapter -> prepare_sku_data -> calculate_order_quantity -> finalize_orders`
carries all 45 products and yields a recommendation. The engine is untouched —
only the source was swapped.

### Open
- `generate_smart_orders()` returns an empty list on the Odoo path while the raw
  pipeline returns 45 rows. The later stages (network optimisation / MOQ gate)
  drop them. Needs tracing.
- Only 1 recommendation, because `pos.order.line` is EMPTY — no demand signal in
  this instance. Same problem as RXL wore in a different costume.
- PO write-back is coded but UNTESTED (no run has created a purchase.order yet).
- Writes are DRAFT only, deliberately: OASIS proposes, a human approves in Odoo.

### Why this matters beyond Odoo
The eight-method adapter surface is small and the contract is now proven
swappable. Zoho, Shopify or a CSV drop need only implement the same eight
methods — no schema bridge, because there is no schema to bridge.

---

## MILESTONE [2026-08-13] Full ordering cycle on Odoo — no schema bridge

Seeded Odoo from real client data: 300 products chosen where the real catalogue
(`oasis_store.db`: name, department, supplier, sell price, **cost**, opening
stock) overlaps real demand (`*_cash.xlsx` via OASIS's own `load_monthly_demand`).
90 days of demand written as outgoing `stock.move` (internal -> customer).
Result: 345 products, 62 categories, 71 vendors, 160,262 demand moves.

**Adapter now reads demand from TWO sources unioned**: `pos.order.line` AND
outgoing `stock.move` to a customer location. POS alone would report zero demand
for any client selling via Sales or eCommerce — the engine would then see a live
catalogue with no velocity and behave as if nothing moves. Same silent-zero
failure shape as the `SM_LAST_RECV_DT` bug, in a different place.

### Result
| | |
|---|---|
| products / with demand | 345 / 306 |
| recommendations | **101** |
| order value | **KES 2,161,459** |
| funnel | ordered=101, below_moq=8, no_order=236 |

Top line: *Chandarana Carrier Bag*, stock 0, ADS 66.6 — correctly the most
urgent. Real suppliers throughout (Brookside Dairy, Capwell, West Kenya Sugar).

### A seeding error worth recording
The first run reported **296 recommendations / KES 22.3M** — inflated ~10x.
Cause: 160k stock-DEPLETING outgoing moves were seeded with opening stock set
only once, driving on-hand to **-11,700** on fast movers. The engine was correct
given the data; the DATA was wrong. Fixed by resetting on-hand to the
catalogue's real snapshot (stock is "as of now", moves are "before now" — the
same model used for the RXL bills).

Lesson: seeding demand as inventory-depleting movements requires either matching
receipts or a post-seed stock reset. A negative on-hand does not error — it
silently inflates every order quantity.

### What this proves
OASIS runs its full ordering cycle against a real ERP over XML-RPC with **no
schema bridge, no views, no database credentials** — and gets cost price and
genuine receipt dates natively, which neither the hub (excludes cost by design)
nor RXL (no `SM_LAST_RECV_DT` at all) can supply.

### Still untested
PO write-back to `purchase.order` (coded, DRAFT-only, never executed).

---

## MILESTONE [2026-08-13] PO write-back verified — the loop is CLOSED

`OdooAdapter.push_purchase_order` tested against live Odoo with 5 of the 101
recommendations. Verified by reading Odoo back, not by trusting the return value:

| PO | Vendor | State | Total |
|---|---|---|---|
| P00013 | WEST KENYA SUGAR COMPANY | draft | 227,264 |
| P00012 | GRAIN INDUSTRIES LIMITED | draft | 121,857 |
| P00011 | EXCEL CHEMICALS LTD | draft | 43,015 |
| P00010 | CAPWELL INDUSTRIES LTD | draft | 150,424 |
| P00009 | TEXPLAST INDUSTRIES | draft | 126,753 |

Correctly grouped one PO per supplier; quantities and unit costs carried through
(`Kabras 2Kg` x 826 @ 239.25); Odoo computed the totals.

**Written as DRAFT by design.** OASIS proposes, a human approves in Odoo.
Writing confirmed POs would commit a client's money without review.

### The complete cycle now proven on a real ERP
```
Odoo -> OdooAdapter (XML-RPC) -> enriched products (cost + real receipt dates)
     -> ordering engine -> 101 recommendations / KES 2.16M
     -> draft purchase.orders back INTO Odoo
```
No schema bridge. No views. No database credentials. Contrast RXL, which needed
a canonical view layer, a rewritten schema profile, and six product-bug fixes to
reach the same place.

### Housekeeping
5 draft POs (P00009-P00013) were created in the `oasis` Odoo database by this
test. Delete them if unwanted — they are drafts and commit nothing.

---

## MILESTONE [2026-08-13] Guards spread + full ERP-agnostic writeup

Receipt dates re-spread across all four bands so every guard is exercised:
Stale Fresh fired x19, Dead Stock x6 (previously 1). Order 97 -> 86 lines.

The complete Odoo / ERP-agnostic work is written up separately in
[[OASIS_ERP_Agnostic_2026-08]] — architecture, adapter contract, hierarchy fix,
`--mode erp-status`, seeding errors and honest limits.

---

## Next actions
1. Create store org under `001` (note the level it lands on).
2. Create a Counter for that store.
3. Create ~10–20 items across 2–3 departments with cost + sell price.
4. Post opening stock.
5. Ring a sale on `RXLPos.exe`.
6. Verify `BILL_DT` vs `BILL_TIME` behaviour (see CORRECTION above).
7. Regenerate OASIS views for the real store level; point `OASIS_POS_DB_URL` at it.

Instrumentation is live for all of the above:
- Extended Events capture `rxl_learn` -> `/var/opt/mssql/log/rxl_learn.xel`
  (statements + errors, `STARTUP_STATE=ON`)
- `dbo.OASIS_ROWCOUNT_BASELINE` — row counts for all 1,000 tables taken before
  any UI work (65,220 rows). Diff after creating master data to reveal **every**
  table RXL touches, including sequence/audit/hierarchy side-effects.
