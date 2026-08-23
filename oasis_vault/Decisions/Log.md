# Decision Log
Tracking technical changes and the rationale behind them.

## [2026-03-17] Restoration of Golden State
- **Problem**: Logic degradation following refactoring.
- **Solution**: Unified `main.py` to use `run_intelligent_analysis` as the master orchestrator.
- **Outcome**: 100% parity achieved across report formatting and historical baseline anchoring.

## [2026-03-19] Final Logic Parity Sync (v10.0)
- **Problem**: Lingering logic ghosts in auxiliary mixins (Rounding, GRN Scan, etc.).
- **Solution**: Performed comprehensive audit and line-by-line sync with Golden v10.0. Unified the `OrderEngine` orchestrator with AI fallback and safety guards.
- **Outcome**: 100% logic parity achieved. Optimized for production-scale allocation across all store tiers.

## [2026-03-20] Order Engine Audit: Greenfield & Budget Calibration
- **Problem**: Budget overruns after safety guards and perceived "degradation" in small-store allocations.
- **Solution**:
    - Implemented **Post-Guard Budget Pruning** (Priority-based sorting).
    - Restored **Greenfield-Safe Intelligence** (is_greenfield=True mode).
    - Synchronized all target stock formulas with **Golden Logic v10.0** (Removed `profile_depth` caps).
- **Outcome**: 100% Logic Parity. Budget is now strictly enforced while maintaining historical allocation depth.

## [2026-03-21] Greenfield Optimization: Dynamic Scrutiny & Syncs
- **Problem**: Allocation app overriding all items to "1" unit; Departments showing only "general/fresh"; Bread/Fresh items absorbing 60 days of budget; New SKUs utilizing rigid `0.5` mathematically-naive fallbacks.
- **Solution**:
    - **Pack Size Unlocking**: Removed broad `pack_size` rounding in Greenfield formulas to allow precision integer targets to survive Pass 1.
    - **Department Sync**: Loaded `product_department_map.json` natively into `OrderEngine` loaders and preserved map in Phase 3 enrichment.
    - **Spoilage Hard Capping**: Explicitly blocked `is_fresh` items from Pass 4 "Mop-Up" limits to prevent them from soaking 60 days of buffer.
    - **Dynamic Scrutiny**: Stripped away all static fallback metrics (`0.1`, `0.5`, `3.0`, `6.0`) across Pass 1, 2B, 4, and CV calculations. Replaced them with intelligent, price-ceiling driven baseline velocities.
- **Outcome**: Target allocations are precise, natively intelligent even with zero historical forecast, and department structures correctly mirror database ground truths.

## [2026-08-13] Live RXL POS Integration
- **Problem**: The RXL schema profile had only ever been written against documentation; the live-port path had never been executed.
- **Solution**: Verified everything against a real RXL database (TESTING11, 999 tables). Six DDL/mapping blockers fixed; local RXL install brought up and Console login achieved.
- **Outcome**: See [[RXL_Integration_Log_2026-08]] for the full log, and [[OASIS_Port_Method]] for the repeatable procedure for future POS vendors.

## [2026-08-13] OASIS Made ERP-Agnostic (Odoo Adapter)
- **Problem**: Ordering intelligence was reachable ONLY via `PosErpAdapter` (direct POS database access). Verified nothing in `oasis/` reads `hub_stock_movement`, and the hub is deliberately supplier-facing (no cost price, no item master). So an Odoo/Zoho client could push data but never get Smart Ordering.
- **Solution**: Built `oasis/logic/odoo_adapter.py` — PosErpAdapter's contract over XML-RPC, verified against a live Odoo 16 throughout. Added a 3-level product hierarchy (fixing `department` being an entire path string), `--mode erp-status` for adapter observability, a `funnel` diagnostic on `generate_smart_orders`, and made `get_adapter()` pluggable via `OASIS_ERP=odoo`.
- **Outcome**: Full cycle proven — Odoo -> adapter -> engine -> DRAFT purchase.orders back into Odoo, with NO schema bridge, NO views and NO database credentials. Odoo natively supplies cost price and real receipt dates, which the hub excludes by design and RXL does not have at all. Dead-stock and stale-fresh guards went from structurally dead to firing (19 + 6). See [[OASIS_ERP_Agnostic_2026-08]].

## [2026-08-14] Site scoping finished; the RXL/Odoo day committed
- **Problem**: A full day of RXL + Odoo work existed ONLY in the working tree (51 files, `odoo_adapter.py` untracked), alongside an 86MB `.bak` and the hub admin key with no ignore rules. And `push_purchase_order` still accepted `org_cd` and ignored it.
- **Solution**: Ignored the dump and the key, restored the placeholder password in `create_oasis_login.sql`, committed the day as `ab8e20ac`. Then scoped the **five** methods with the accept-and-ignore shape, not the one that was flagged — `fetch_pending_po_by_sku` was the expensive one, since it feeds `on_order_qty` and unscoped it suppresses ordering chain-wide (`62d9cb4f`).
- **Also**: `OASIS_POS_DB_URL` persists as a USER environment variable after a port, so the suite pointed at a SQL Server that was no longer running; a five-store network asserted as zero stores with the traceback swallowed. Five test files already delenv'd it by hand — hoisted to one autouse fixture in `conftest`.
## [2026-08-16] Odoo transfer methods — the adapter contract is covered
- **Problem**: `fetch_transfers` / `push_transfer_request` (and `update_transfer_status`, without which the console renders transfer rows it cannot advance) were declared on the contract but missing for Odoo.
- **Solution**: One internal `stock.picking` per request (Odoo's unit of work is the picking, so `TRANSFER_ID` is the picking id and item rows share it — differs from PosErpAdapter deliberately); Odoo's six picking states collapsed onto the console's REQUESTED/IN_TRANSIT/RECEIVED ladder; `VALUE_KES` derived at cost.
- **The finding**: this instance's two warehouses are in **different `res.company` records**. Odoo CREATES a cross-company internal picking and then refuses to CONFIRM it, which would have stranded a draft reading REQUESTED forever. Now refused up front. Reads across companies do work — so the multi-company question is half-answered, not open.
- **Outcome**: Live-verified end to end including real stock movement (WH 70 -> 67, WH2 0 -> 3) and six refusal paths. Separately, the suite turned out to depend on the dev install's 14-day trial not having lapsed — it expired overnight (first_run 08-02 + 14 = 08-16) and reddened an unrelated test; pinned with an autouse fixture. See [[OASIS_ERP_Agnostic_2026-08]].

## [2026-08-14] Site scoping finished; the RXL/Odoo day committed (cont.)
- **Outcome**: Tree clean, both commits green. Written while Docker was down, so it shipped on two assumed Odoo field names; Docker came up later and **both were confirmed against live Odoo 16**. `purchase.order.picking_type_id` is `required: True` — which IS the bug: it can never be empty, so omitting it silently gets the default warehouse, and every PO through P00013 landed at WH. A PO pushed for CHIC1 now creates as `picking_type=[7, 'Chicago 1: Receipts']` (P00014). `on_order_qty` scoped: CHIC1 was seeing 7,571 units inbound to WH as its own. See [[OASIS_ERP_Agnostic_2026-08]].

## [2026-08-23] Transfers become an installable Odoo module (and the push blocker clears)
- **Problem**: 16 commits of transfer work could not be pushed (`.git` 2.26 GB, five files over GitHub's 100 MB limit); the addon assumed our own depot; and the safety floor was still a declared constant (14 in code, 10 in the seed) with nothing behind either number.
- **Solution**: `git filter-repo` took the 2 GB backup zip and the oversized DBs out of history — **`.git` 2.26 GB → 132 MB**, 216 commits pushed, `main` deliberately left where it was. **σ became R**: the safety floor a store keeps IS its relief horizon, derived from that store's own goods receipts where it has ≥6 of them, network rhythm otherwise. The addon split into `oasis_connector` / `oasis_transfers` / `oasis_telemetry`, each installable alone, with the separability asserted by tests rather than intended. Three read-only modes added: `odoo-preflight`, `odoo-rhythm`, `odoo-pilot`.
- **The finding**: `*.csv` in `.gitignore` had excluded **every `ir.model.access.csv`** since the connector was written, so a clean clone installed the modules with no access rules at all. Visible only from a fresh checkout — the working tree had always carried them untracked. Two smaller ones cost real time: `os.environ.setdefault` returns the *existing* value (CI seeded one password and signed in with another), and 16 of 17 warehouses shared one address partner, so per-store geocoding overwrote the same record 14 times.
- **Outcome**: Suite 1,340 passed / 47 skipped on a clean clone in the CI image; 59 addon tests in each POS configuration; e2e 12/0. On live depot data: order independence 0.0000%, zero donor-protection breaches, the 999 cover sentinel 1,431 → 0, pass-through 17 → 0. A six-step runbook takes a real multi-store Odoo from cold to a reviewed queue. See [[Odoo_Transfer_Module_2026-08]].
