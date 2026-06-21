# O.A.S.I.S. — RXL/iRetail Porting Reconciliation

> Schema reconciliation of OASIS's POS/ERP adapter against the **real RXL /
> iRetail ERP** (analyzed from the actual install + DB dump: `Retail Excel/Base/
> Database Dump/Upgrade/UpgradeDDL.sql`, `iAnalytics/iAnlayticsScript.sql`, RXL
> 7.0.0.0 / iRetail V8.15). Purpose: make the live port actually work.
> Generated 2026-06-19. Read-only analysis.

---

## 0. Headline

**The core schema is real and our table set is correct — but the adapter's SQL
uses abbreviated column/table names that do NOT match the real RXL database, so
several queries would fail live.** All deltas are either (a) name remaps that the
existing `--mode build-views` mechanism can bridge, or (b) attributes that aren't
in the ERP at all and must be OASIS-derived. None are blockers; all are now
known and mappable.

---

## 1. What's validated (matches reality) ✓

From `UpgradeDDL.sql` (the real RXL schema), our assumed tables all exist:
`ITEM_MST`, `STOCK_MASTER`, `POS_SALES_HDR`, `POS_SALES_DTL`, `BASIC_SP_MST`,
`BASIC_CP_MST`, `BASE_SYSTEM_PREFERENCES`, `GRN_HDR`, `GRN_DTL`,
`ORGANIZATION_MST`. And these columns match exactly:

- **ITEM_MST**: `ITM_CD`, `ITM_LONG_NAME`, `SCAN_ITM_CD`, `CATEGORY`,
  `DEPARTMENT`, `ACTIVE_FLAG` ✓
- **POS_SALES_DTL/HDR**: `ORG_CD`, `ITM_CD`, `QTY`, `BILL_DT`, `VOID_FLAG`,
  `NET_AMT`, `TOTAL_VALUE`, `SELL_PRICE` ✓ — the demand/sales read is sound.
- Keys `SM_ITM_CD`, `SM_ORG_CD`, `BSP_ITEM_CD/ORG_CD`, `BCP_ITEM_CD/ORG_CD` ✓

So the **demand side (POS sales) ports cleanly**; the issues are on stock, price,
and supplier.

## 2. Column / table remaps (adapter → real RXL) — fix via schema views

| OASIS adapter uses | Real RXL name | Used by |
|---|---|---|
| `STOCK_MASTER.SM_QTY` | **`SM_CURR_STK_QTY`** | stock snapshot, enrichment |
| `STOCK_MASTER.SM_WAC` | **`SM_WT_AVG_COST`** | cost fallback |
| `BASIC_SP_MST.BSP_SP` | **`BSP_SELL_PRICE`** | selling price |
| `BASIC_CP_MST.BCP_CP` | **`BCP_COST_PRICE`** | cost price |
| `ITEM_MST.SUPPLIER_CD` | **`ITEM_MST.VENDOR_CD`** | product→supplier join |
| `SUPPLIER_MST` (table) | **`VENDOR_ADDRESS_MST`** | supplier master |
| `SUPPLIER_NAME` / `SUPPLIER_CD` | **`VENDOR_NAME` / `VENDOR_CD`** | supplier display |
| `BI_SALES_REPORT` | **`BI_SALES`** (in the iAnalytics DB) | optional BI summary |

These are pure renames → handled by the **`--mode build-views` schema profile**
(canonical name → real column/table). See the profile sketch in §5.

## 3. Attributes the ERP does NOT have — must be OASIS-derived

`fetch_product_master` / `fetch_supplier_patterns` SELECT
`sup.LEAD_TIME_DAYS`, `sup.RELIABILITY_SCORE`, `sup.ORDER_FREQUENCY` and
`STOCK_MASTER.SM_LAST_RECV_DT`. **None of these exist in RXL** (0 occurrences in
the DDL). They are intelligence OASIS *computes*, not ERP fields:

- **lead time / order frequency / reliability** → already derived by
  `bootstrap-intel` + LATA from **GRN cadence** (`supplier_patterns`). The adapter
  should NOT read them from the ERP.
- **days-since-last-delivery** → derive from the latest `GRN_HDR` date per item,
  not a stock-master column.

**Action:** make the adapter tolerant of these being absent (default/omit), and
source them from `supplier_patterns` / GRN — not the ERP query. Otherwise the
product-master query throws "invalid column" against a real RXL DB.

## 4. Structural considerations

- **Multi-level org hierarchy.** Every master carries `LEVEL_NUMBER`
  (`SM_LEVEL_NUMBER`, `BSP_LEVEL_NUMBER`, …). RXL stores stock/price at multiple
  org levels; queries must filter to the **store level** (or the views must),
  else rows multiply. The canonical views must pin `LEVEL_NUMBER` to the store
  tier.
- **Consignment stock** is a first-class column (`SM_CONSIGN_STK_QTY`) — relevant
  to the consignment-budget logic; the stock view should expose it.
- **Multiple price tiers** (`BSP_PRICE_CATG`, `SM_SELLING_PRICE/SM_MRP` on stock
  too) — the view should pick the canonical retail price deterministically.
- **iAnalytics is a separate DB** (its own `iAnlayticsScript.sql` / `BI_SALES`).
  `OASIS_POS_DB_URL` points at the RXL transactional DB; BI summary is optional
  and would need its own connection — keep it out of the required contract.

## 5. Recommended porting path

1. **Author the RXL schema profile** (canonical → real), e.g.:
   ```json
   {
     "STOCK_MASTER": {"source": "STOCK_MASTER",
        "columns": {"SM_ITM_CD":"SM_ITM_CD","SM_ORG_CD":"SM_ORG_CD",
                    "SM_QTY":"SM_CURR_STK_QTY","SM_WAC":"SM_WT_AVG_COST"},
        "where": "SM_LEVEL_NUMBER = :store_level"},
     "BASIC_SP_MST": {"source":"BASIC_SP_MST",
        "columns":{"BSP_ITEM_CD":"BSP_ITEM_CD","BSP_ORG_CD":"BSP_ORG_CD",
                   "BSP_SP":"BSP_SELL_PRICE","BSP_MRP":"BSP_MRP"}},
     "BASIC_CP_MST": {"source":"BASIC_CP_MST",
        "columns":{"BCP_ITEM_CD":"BCP_ITEM_CD","BCP_ORG_CD":"BCP_ORG_CD",
                   "BCP_CP":"BCP_COST_PRICE"}},
     "SUPPLIER_MST": {"source":"VENDOR_ADDRESS_MST",
        "columns":{"SUPPLIER_CD":"VENDOR_CD","SUPPLIER_NAME":"VENDOR_NAME"}},
     "ITEM_MST": {"source":"ITEM_MST",
        "columns":{"...":"...","SUPPLIER_CD":"VENDOR_CD"}}
   }
   ```
2. **Two `build-views` generator enhancements** (small):
   - support a **`where`** clause per view (to pin `LEVEL_NUMBER`);
   - support **literal/default columns** (e.g. `NULL AS LEAD_TIME_DAYS`) so the
     canonical view satisfies the adapter's SELECT even when the ERP lacks the
     column.
3. **Refactor the adapter's supply attributes** to come from `supplier_patterns`
   (GRN-derived) rather than the ERP — drop `LEAD_TIME_DAYS`/`RELIABILITY_SCORE`/
   `ORDER_FREQUENCY`/`SM_LAST_RECV_DT` from the ERP SELECTs.
4. **Update the preflight contract** doc to note the RXL real names so a DBA can
   build the views, and add a `:store_level` parameter to the onboarding config.
5. **Re-validate** against a real RXL DB (or `RetailExcel.bak` restored) with
   `--mode preflight`.

---

## 6. Net

The demand/sales path ports as-is; the **stock, price, and supplier** paths need
name remaps (mechanical, via schema views) plus a clean separation of
**ERP-provided** vs **OASIS-derived** supply attributes. The mock
(`mock_pos_erp.db`) faithfully implements the *adapter's* idealized schema, which
is why everything passes locally — but a real RXL port needs the §5 profile +
two small `build-views` enhancements. With those, the adapter runs unchanged
against the canonical views over the real RXL database.
