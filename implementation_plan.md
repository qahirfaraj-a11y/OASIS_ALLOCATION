# O.A.S.I.S. Client Deployment Architecture

## Goal
Design an installable, self-contained O.A.S.I.S. package that can be deployed on any client machine, initialize a new retail universe from scratch (Day 0), and operate autonomously in live, 8-hour, or 24-hour cycles.

---

## Part 1: Day-0 Retail Universe Initialization Sequence

When installing OASIS at a new retailer, the system must ingest their historical data to build the "Retail Universe" (SKU master, store graph, supplier risk baselines, and demand history). This is the **Day 0 Bootstrap**.

### Step 1: Historical Data Ingestion (The Big Pull)
- **Pathway 1 (SQL):** The `IRetailBridge` uses BCP (Bulk Copy Program) or chunked SQL reads to extract 1-3 years of sales history, GRN (Goods Received Notes), and the complete SKU master.
- **Pathway 2 (File):** The client drops historical CSV dumps (`historical_sales.csv`, `historical_grn.csv`, `sku_master.csv`) into a designated `inbound_drops/bootstrap/` folder. The `FileWatcher` detects and ingests them.

### Step 2: Store Graph Initialization
- OASIS analyzes the POS locations to generate the initial Store Network.
- It identifies **Warehouse Hubs** vs **Retail Nodes**.
- It calculates geographical and operational friction between branches to build the initial `edge_index` for the GNN.

### Step 3: Engine Warm-up (Baseline Generation)
Before any PO can be generated, the intelligence caches must be built:
1. **LATA Shield Run:** Analyzes the historical GRN data to calculate supplier lead times, volatility, and historical return rates. Builds the initial `supplier_patterns.json`.
2. **DHARAM Run:** Analyzes historical basket affinities to build the initial `dharam_demand_patch.json` (Ghost Demand map).
3. **AMIT Run:** Establishes the initial category perishability tiers and department capital caps based on historical revenue contribution.

### Step 4: First Shadow Run
- The system executes a historical backtest (Shadow Mode) on the last 30 days of data to establish the **Legacy Baseline** (Fulfillment Rate, Stock Availability, Capital Efficiency).
- These metrics populate the "Before" state in the dashboards (replacing the hardcoded showcase metrics).

---

## Part 2: What Needs to Be Built (The 6 Components)

To support seamless installation and daily operation, we will build:

### 1. `DataGateway` (Unified Router)
- **Path:** `oasis/logic/data_gateway.py`
- **Purpose:** Abstracts the data source. When `OrderEngine` requests stock data, `DataGateway` reads `oasis_client_config.json` and routes the request to either `IRetailBridge` (SQL) or reads from `Full_Product_Allocation_Scorecard.csv` (File dump).
- **Features:** Supports bulk initialization calls for Day 0.

### 2. `install_oasis.bat` (Automated Installer)
- **Purpose:** A one-click bootstrap script for Windows.
- **Actions:** 
  1. Checks for Python 3.10+ (prompts download if missing).
  2. Creates `.oasis_venv` and installs `requirements.txt`.
  3. Triggers the interactive CLI or Flet setup wizard to generate `oasis_client_config.json`.
  4. Runs `ensure_oasis_tables()` to build the SQLite DB.
  5. Registers `oasis_service.py` as a Windows Background Service.

### 3. `oasis_client_config.json` (Per-Client Configuration)
- **Purpose:** The single source of truth for the deployment.
- **Contents:** Connection strings, active data pathway, ingestion cycle speed, store hierarchy, engine feature flags, and budget parameters.

### 4. Scheduler Cycle Presets
- **Path:** Update `scheduler_service.py`
- **Purpose:** Replace manual cron configuration with a single switch (`LIVE`, `8_HOUR`, `24_HOUR`).
- **Logic:** The scheduler reads the config and automatically binds the `DailyPipeline`, stock monitor, and summary tasks to the optimal intervals.

### 5. Heartbeat / Telemetry Service
- **Path:** `oasis/logic/heartbeat.py`
- **Purpose:** Operational visibility. 
- **Logic:** Every 6 hours, it posts a lightweight JSON payload (Client ID, pipeline status, stockout counts, DB size) to an external OASIS Mothership endpoint. Contains **zero** sensitive PII or raw pricing data.

### 6. Windows Service Wrapper
- **Path:** `oasis_service.py`
- **Purpose:** Resilience. Uses `pywin32` to wrap the `OasisScheduler` and `FileWatcher` so they launch when the server boots, crash-loop gracefully, and run invisibly in the background.

---

## Part 3: Strategic Recommendations (The 4 Open Questions)

Here is the proposed strategy for the architecture decisions:

> [!TIP]
> **1. Central Monitoring Mothership?**  
> **Recommendation: YES (Phase 1: Slack/Discord/Email integration).**  
> Instead of building a complex central dashboard immediately, the Heartbeat service should push alerts to a webhook (e.g., Slack or Teams). If a client's pipeline fails or file-dumps stop arriving, your team gets an immediate alert.

> [!TIP]
> **2. Auto-Update Mechanism?**  
> **Recommendation: Signed ZIP Downloads.**  
> Relying on Git on client machines is fragile. We will build an `update_oasis.bat` that pings a release URL, downloads a versioned ZIP of the `oasis/logic` folder, stops the Windows Service, extracts, and restarts.

> [!TIP]
> **3. Multi-store vs Single-store Installations?**  
> **Recommendation: SINGLE INSTALLATION per Retail Network.**  
> Install OASIS once on the retailer's central HQ server or cloud instance. `oasis_client_config.json` will contain a `stores: [...]` array. The `DailyPipeline` loops through all configured stores. This allows DHARAM and the Transfer Service to optimize stock *across* the network.

> [!TIP]
> **4. Flet Standalone `.exe` vs Venv-based?**  
> **Recommendation: Standalone `.exe` for the Client App, Venv for the Engine.**  
> We should compile the Flet PO generation UI into a single `Oasis_Terminal.exe` using PyInstaller. This allows warehouse managers to run the app on their local laptops without installing Python, while the heavy `OrderEngine` runs centrally on their server via the Venv.

---

## Next Steps for Execution
If you approve this architecture and the recommendations for the initialization sequence and the 4 strategic questions, we will begin execution by building the **DataGateway** and the **Day-0 Initialization Logic**.
