# Bootstrap Data Ingestion Pipeline (v1.0)
This document defines the schema, mapping requirements, and ingestion logic for the O.A.S.I.S. **Day-0 Bootstrap** process. This pipeline is critical for initializing the "Retail Universe" for new clients during the Forensic Audit phase.

## 📁 Source Path: `C:\Oasis\inbound_drops\bootstrap`
The `ForensicOperationsIngestor` (`pitch_data_ingestor_v2.py`) monitors this directory for the following datasets. Files must be in `.csv` or `.xlsx` format.

---

## 1. Inventory Master (The SKU Universe)
**Standard Filename:** `stock.csv` or `{ORG}_stock.csv`
**Objective:** Establishes the SKU master, initial stock levels (SOH), and category hierarchy.

| Column | Mapping Target | Type | Logic / Requirement |
|---|---|---|---|
| `Item_Name` | `description` | String | **Required.** Primary lookup key. |
| `Barcode` | `barcode` | String | **Unique ID.** GS1/EAN-13 preferred. |
| `Department` | `category` | String | Used for AMIT category caps. |
| `Supplier` | `primary_vendor` | String | Used for LATA/MANDE reliability audits. |
| `SOH` | `current_stock` | Float | Physical units on shelf at time of snapshot. |
| `ADS` | `velocity_est` | Float | Client's manual estimate of daily sales. |
| `Unit_Cost` | `cost_price` | Float | Used for GMROI and Capital Trap calculation. |
| `Selling_Price` | `retail_price` | Float | Used for Revenue Leakage calculation. |
| `Pack_Size` | `case_size` | Integer | Default: 1. Used for rounding logic. |
| `Lead_Time` | `replenishment_lag`| Integer | Expected days from order to delivery. |

---

## 2. Sales History (The Demand Pulse)
**Standard Filename:** `sales.csv` or `{ORG}_sales.csv`
**Objective:** Builds the historical velocity profile used by DHARAM for demand correction.

| Column | Mapping Target | Type | Logic / Requirement |
|---|---|---|---|
| `Date` | `txn_date` | Date | **Required.** Format: YYYY-MM-DD. |
| `Item_Name` | `description` | String | Must match Inventory Master. |
| `Barcode` | `barcode` | String | Must match Inventory Master. |
| `Qty_Sold` | `units_sold` | Float | **Required.** Total units moved at the till. |
| `Unit_Price_KES`| `sale_value` | Float | Actual price realized per unit. |
| `Transaction_ID`| `receipt_id` | String | Used to detect Basket Affinities (HALO). |

---

## 3. Inbound Log (The Supply Chain Audit)
**Standard Filename:** `grn.csv` or `{ORG}_grn.csv`
**Objective:** Calibrates LATA (Logistics) and MANDE (Supplier Risk) metrics.

| Column | Mapping Target | Type | Logic / Requirement |
|---|---|---|---|
| `Order_Date` | `po_date` | Date | Date the Purchase Order was raised. |
| `Received_Date`| `grn_date` | Date | Date the stock physically arrived. |
| `Supplier_Name`| `vendor` | String | Must match Inventory Master. |
| `Item_Name` | `description` | String | Must match Inventory Master. |
| `Ordered_Qty` | `po_qty` | Float | **Required.** The "Promise" from the vendor. |
| `Received_Qty` | `grn_qty` | Float | **Required.** The "Reality" delivered. |
| `PO_Number` | `reference_id` | String | Links PO to GRN for variance analysis. |

---

## ⚙️ Pipeline Execution Logic
1. **Fuzzy Ingestion:** The ingestor utilizes a weighted string-matching algorithm to resolve minor name discrepancies between Sales, GRN, and Stock lists.
2. **Day-0 Normalization:** O.A.S.I.S. automatically converts all historical data into a unified SQLite schema (`oasis.db`) to enable sub-second forensic queries.
3. **Audit Trigger:** Once ingestion is 100% complete, the system automatically triggers the **Forensic Diagnostic Suite** (AMIT/LATA/DHARAM) to generate the Executive Summary.
4. **Archive:** Post-ingestion, raw files are moved to `C:\Oasis\inbound_drops\archive\bootstrap_{timestamp}\`.

---
*Document Class: Operational Schema | Author: O.A.S.I.S. Architecture Team | Version 1.0*
