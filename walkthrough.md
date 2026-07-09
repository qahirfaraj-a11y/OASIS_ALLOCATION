# O.A.S.I.S. — The Complete User Experience

> From first handshake to full autonomous procurement — what happens at every step, what code runs under the hood, and where the store universe gets built.

---

## The 7 Phases of an OASIS Deployment

```
Phase 0        Phase 1         Phase 2         Phase 3          Phase 4         Phase 5         Phase 6
THE PITCH  →  INSTALLATION  →  DATA FEED  →  UNIVERSE INIT  →  SHADOW MODE  →  TRANSITION  →  FULL AUTONOMY
(Your team)   (Client site)   (Client IT)    (OASIS engine)   (30-90 days)   (Approval)     (Engine runs solo)
```

---

## Phase 0: The Pitch (Pre-Sales)

**Who:** Your sales team + the client's operations manager.

**What happens:**
1. You run the **Pitch App** (`run_pitch_app.bat`) using the client's own sample data.
2. The Pitch Engine ingests a sample CSV (even 500 SKUs works) and runs a forensic diagnostic.
3. It produces: AMIT dead-stock report, DHARAM ghost-demand map, LATA supplier risk scores, and a projected ROI.
4. The client sees: *"You're wasting KES 2.3M/month on dead stock. OASIS would save 18%."*

**What code runs:**
- [integrated_app.py](file:///C:/Users/iLink/.gemini/antigravity/scratch/integrated_app.py) — Flet desktop app
- `OrderEngine.run_intelligent_analysis()` — The core AI

**Data needed from client at this stage:**
- A simple CSV export from their POS. Even a partial dump works for the pitch.

---

## Phase 1: Installation (Client Site)

**Who:** Your deployment engineer (remote or on-site, 30 minutes).

**What happens:**
1. Extract `OASIS_Protected_Release.zip` to `C:\OASIS\` on the client's server.
2. Double-click `install_oasis.bat`.
3. The installer:
   - Detects Python (or prompts to install it)
   - Creates `.oasis_venv` and installs all dependencies
   - Initializes the SQLite database (`oasis.db`)
   - Creates `inbound_drops/`, `shadow_logs/`, `logs/` directories
   - Places Desktop shortcuts

**What code runs:**
- [install_oasis.bat](file:///C:/Users/iLink/.gemini/antigravity/scratch/install_oasis.bat) — Lines 1-180
- `UniversalConnector.ensure_oasis_tables()` — Creates the DB schema

**Result:** The machine is ready. No data has been ingested yet — it's an empty universe.

---

## Phase 2: The Data Feed (Client IT Handoff)

**Who:** Client's IT department + your engineer.

**This is the critical step.** OASIS needs the client's ERP/POS data to build their unique retail universe. There are two pathways:

### Pathway A: Direct SQL Connection
Your engineer edits `oasis_client_config.json`:
```json
{
    "data_pathway": "sql",
    "sql_connection": {
        "enabled": true,
        "server": "RETAILSRV\\IRETAIL",
        "database": "iRetailDB",
        "trusted_connection": true
    }
}
```
OASIS will pull data directly from the client's SQL Server using `MssqlConnector`. The client IT team grants read-only access to the OASIS service account.

### Pathway B: Scheduled File Dumps (More Common)
Client IT exports CSVs from their POS system and drops them into `C:\OASIS\inbound_drops\`.

**The EXACT file schema OASIS requires:**

#### Required File: `Full_Product_Allocation_Scorecard.csv`
This is the master file. Every column maps to a specific engine:

| Column | Required? | Example | Used By |
|---|---|---|---|
| `Item_Name` | **YES** | `BROOKSIDE MILK 500ML` | All engines (primary key) |
| `SOH` | **YES** | `45` | OrderEngine (Stock on Hand) |
| `ADS` | **YES** | `12.5` | OrderEngine (Average Daily Sales = velocity) |
| `Unit_Cost` | **YES** | `85.00` | BudgetManager (capital allocation) |
| `Barcode` | Optional | `5012345678901` | Product matching, deduplication |
| `Department` | Optional | `DAIRY` | AMIT (category caps), BudgetManager |
| `Supplier` | Optional | `BROOKSIDE DAIRIES LTD` | LATA (supplier risk), PO routing |
| `Lead_Time` | Optional | `3` | OrderEngine (days to delivery) |
| `Pack_Size` | Optional | `12` | Rounding engine (order in multiples) |
| `Selling_Price` | Optional | `120.00` | DHARAM (margin analysis) |
| `Safety_Factor` | Optional | `1.3` | OrderEngine (buffer multiplier) |

> [!IMPORTANT]
> **Minimum viable data:** If the client can only give you `Item_Name`, `SOH`, `ADS`, and `Unit_Cost`, OASIS will still work. The `FileWatcher` auto-fills defaults for missing columns (`Lead_Time=7`, `Supplier='GENERAL'`, `Department='GENERAL'`, `Safety_Factor=1.3`).

#### Optional Historical Files (For Better Day-0 Calibration):
| File | Columns | Used By |
|---|---|---|
| `historical_sales.csv` | `Item_Name, TransDate, Qty, Revenue` | 90-day velocity calculation |
| `historical_grn.csv` | `GRNDate, Item_Name, ReceivedQty, SupplierName, CostPrice` | LATA supplier lead-time variance |

---

## Phase 3: Universe Initialization (Day-0 Bootstrap)

**Who:** Your engineer runs one command.

```bash
python oasis_bootstrap.py
```

**What happens under the hood (6 steps):**

```
Step 1: Load Config
    └── DataGateway reads oasis_client_config.json
        └── Determines pathway (SQL or File)
        └── Identifies configured stores

Step 2: Initialize Database
    └── UniversalConnector.ensure_oasis_tables()
        └── Creates: OASIS_SYSTEM_CONFIG, OASIS_PRODUCT_MASTER,
            OASIS_ORDER_HISTORY, OASIS_SUPPLIER_RISK tables

Step 3: Ingest Product Catalog (THE BIG PULL)
    ├── SQL Path: MssqlConnector.fetch_stock() pulls from StockOnHand table
    └── File Path: DataGateway reads Full_Product_Allocation_Scorecard.csv
        └── Normalizes column names (Item_Name → product_name, SOH → current_stocks)
        └── Validates data types (SOH must be int, ADS must be float)
        └── Result: The SKU Universe is now loaded (e.g., 12,847 products)

Step 4: Ingest Sales History (90-day window)
    ├── SQL Path: MssqlConnector.fetch_sales() → aggregated by item
    └── File Path: Reads historical_sales.csv OR derives from ADS column
        └── Result: Demand velocity baselines established

Step 5: Ingest GRN History (365-day window)
    ├── SQL Path: MssqlConnector.fetch_grn_history()
    └── File Path: Reads historical_grn.csv
        └── Result: Supplier delivery patterns available for LATA

Step 6: Engine Warm-Up
    ├── AMIT Governance → Scans all 12,847 products
    │   └── Identifies dead stock (SOH > 0 but ADS = 0 for 30+ days)
    │   └── Flags high-risk toxicity items
    │   └── Writes: amit_enforcement.json (blacklist)
    │
    ├── LATA Shield → Analyzes GRN history
    │   └── Calculates supplier lead-time mean + variance
    │   └── Calculates historical return/rejection rates
    │   └── Writes: supplier_patterns.json (risk scores)
    │
    └── DHARAM Revenue → Analyzes basket affinities
        └── Identifies "ghost demand" (items with hidden cross-sell lift)
        └── Writes: dharam_demand_patch.json (demand corrections)
```

**Result:** The retail universe is fully initialized. OASIS "knows" every product, every supplier's behavior, and every demand pattern. The intelligence caches (JSON files) are the engine's memory.

---

## Phase 4: Shadow Mode (30-90 Days)

**Who:** OASIS runs autonomously. Your team monitors remotely.

**What happens:**
Every day (or every 8 hours, depending on the configured cycle), the system runs the `DailyPipeline`:

```
06:00 — Scheduler triggers DailyPipeline.run_daily_cycle()
    │
    ├── 1. DATA PULL
    │   ├── SQL: Fresh stock snapshot from ERP
    │   └── File: FileWatcher detected new CSV in inbound_drops/
    │
    ├── 2. AMIT PRE-FLIGHT
    │   └── Re-scans for new dead stock. Updates blacklist.
    │
    ├── 3. LATA PRE-FLIGHT
    │   └── Re-calculates supplier risk with latest GRN data.
    │
    ├── 4. DHARAM PRE-FLIGHT
    │   └── Re-analyzes demand patterns. Updates ghost demand patches.
    │
    ├── 5. SHADOW PO GENERATION
    │   └── OrderEngine.run_intelligent_analysis()
    │       └── Loads all engine caches (AMIT blacklist, DHARAM patches)
    │       └── Calculates optimal order quantities for every SKU
    │       └── Applies budget constraints (BudgetManager)
    │       └── Applies safety guards (order_logic_guards.py)
    │       └── Filters out AMIT-blocked items
    │       └── Writes shadow PO to shadow_logs/
    │
    └── 6. COMPARISON
        └── ShadowModeEngine compares OASIS PO vs Human PO
        └── Writes shadow_comparison_YYYYMMDD.csv
        └── Calculates: Fill Rate Delta, Capital Efficiency Delta, Stockout Prevention
```

**What the client sees:**
- The **Ops Dashboard** shows daily KPIs and trends.
- The **Shadow Dashboard** shows "What OASIS would have ordered vs what you actually ordered."
- The **Approval Dashboard** is available but not yet controlling real orders.

**What you monitor:**
- The **Heartbeat Service** pings your Slack/Teams every 6 hours.
- Pipeline logs in `oasis/data/pipeline_logs/`.

---

## Phase 5: Transition (Client Approval)

**Who:** Your team presents the shadow results to client management.

**What happens:**
1. After 30-90 days, you pull the shadow comparison data.
2. You show the client: *"Over the last 60 days, OASIS would have saved you KES 1.8M in dead stock purchases while maintaining 97.2% fill rate vs your current 89.1%."*
3. The client approves the transition to live mode.

**Configuration change:**
```json
{
    "engines": {
        "shadow_mode": false    ← Changed from true to false
    }
}
```

---

## Phase 6: Full Autonomy

**Who:** OASIS runs alone. Client staff use the Approval Dashboard for oversight.

**What changes:**
- The `DailyPipeline` now generates **real POs** instead of shadow POs.
- If `data_pathway` is `"sql"`, the POs are pushed directly back to the ERP via `IRetailBridge.push_purchase_order()`.
- If `data_pathway` is `"file"`, the POs are saved as CSVs in `oasis/data/approved_pos/` for the client's procurement team to import manually.
- The **Approval Dashboard** becomes the control center: staff can review, approve, or reject individual PO lines before they're sent.
- The `auto_approve_below_kes` threshold in the config allows small orders to flow through without human review.

**The daily rhythm:**
```
06:00   Scheduler triggers DailyPipeline
06:01   Data pulled (SQL) or File detected (watcher)
06:02   AMIT/LATA/DHARAM pre-flights execute
06:05   OrderEngine generates POs for all stores
06:10   POs land in Approval Dashboard queue
08:00   Procurement manager reviews flagged POs
08:30   Approved POs dispatched to suppliers
14:00   (8_HOUR cycle) Mid-day refresh runs
22:00   Evening summary: today's KPIs logged
```

---

## What Currently Exists vs What's New

| Component | Status | File |
|---|---|---|
| OrderEngine (core AI) | ✅ Exists | `order_engine.py` (25.8 KB) |
| AMIT Governance | ✅ Exists | `amit_governance.py` + `amit_gatekeeper.py` |
| LATA Shield | ✅ Exists | `lata_shield.py` (14.5 KB) |
| DHARAM Revenue | ✅ Exists | `dharam_revenue.py` (12.6 KB) |
| Shadow Mode Engine | ✅ Exists | `shadow_mode.py` (18 KB) |
| Daily Pipeline | ✅ Exists | `daily_pipeline.py` (10.4 KB) |
| File Watcher | ✅ Exists | `file_watcher.py` (5.3 KB) |
| SQL Connectors | ✅ Exists | `mssql_connector.py` + `iretail_integration.py` |
| Scheduler | ✅ Exists | `scheduler_service.py` (20 KB) |
| Budget Manager | ✅ Exists | `budget_manager.py` (7.2 KB) |
| All Dashboards | ✅ Exists | 5 Streamlit apps + Flet desktop app |
| **DataGateway** | 🆕 New | `data_gateway.py` — Unified router |
| **Heartbeat** | 🆕 New | `heartbeat.py` — Remote monitoring |
| **Bootstrap CLI** | 🆕 New | `oasis_bootstrap.py` — Day-0 init |
| **Installer** | 🆕 New | `install_oasis.bat` — One-click setup |
| **Windows Service** | 🆕 New | `oasis_service.py` — Runs on boot |
| **Updater** | 🆕 New | `update_oasis.bat` — Self-update |
| **Cython Pipeline** | 🆕 New | `compile_release.bat` — IP protection |
| **Client Config** | 🆕 New | `oasis_client_config.template.json` |

**Total modules in `oasis/logic/`:** 44 files (the complete intelligence stack)
