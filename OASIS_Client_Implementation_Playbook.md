# O.A.S.I.S. Client Implementation Playbook
### From First Contact to Full Autonomous Replenishment

**Classification:** INTERNAL — iLink Operations  
**Version:** 1.0  
**Last Updated:** 05 April 2026

---

## How To Read This Document

This playbook defines the exact sequence of events, deliverables, and decision gates required to take a retail prospect from initial contact through to full autonomous replenishment under the O.A.S.I.S. engine. Each phase has:

- **Objective:** What we are trying to achieve.
- **Prerequisites:** What must be true before this phase begins.
- **Process:** The exact steps, in order.
- **Deliverables:** What the client receives at the end of the phase.
- **Decision Gate:** The condition that must be met to proceed to the next phase.
- **Timeline:** Expected duration.

---

## PHASE 0: PROSPECTING & FIRST CONTACT
**Timeline: Day 0**

### Objective
Identify a retail operation that is likely hemorrhaging working capital due to manual procurement, and secure agreement to receive a free forensic audit.

### Process
1. **Target Identification:** Identify retail chains, supermarkets, or distribution networks that:
   - Operate 1+ physical store locations.
   - Use a POS system that generates transactional logs (any format: CSV, Excel, JSON, SQL dump).
   - Have a procurement/buying team that manually decides what to order and how much.
   - Carry 500+ SKUs (sufficient complexity for O.A.S.I.S. to demonstrate value).

2. **The Hook (The Pitch):** Position O.A.S.I.S. as a **free diagnostic tool**, not a product pitch. The language is:
   > *"We would like to run a free, no-obligation operations audit on your store. We will take your raw sales and purchasing data, run it through our forensic engine, and show you exactly how much money your operation is losing to dead stock, supplier inconsistency, and logistics friction. The audit takes 24 hours. You lose nothing. You keep the report regardless."*

3. **Data Request:** Upon agreement, formally request the following raw data exports from their ERP/POS system. Emphasize that we accept **any format** (CSV, Excel, JSON, database dumps):

| # | Data Category | What To Ask For | Why We Need It |
|---|---|---|---|
| 1 | **POS / Sales Log** | Cash register transaction dump for 1-3 months. Must contain: Item Name or Barcode, Quantity Sold, Date. Price is preferred but not required. | To calculate true daily sales velocity per SKU and identify dead stock vs. fast movers. |
| 2 | **GRN / Inbound Log** | Goods Received Notes for the same period. Must contain: Supplier Name, Item Name, Quantity Ordered (PO), Quantity Received (GRN), Date Received. | To measure supplier fulfillment rates and lead time variance. |
| 3 | **Purchase Returns (PRTS/GRTS)** | Any supplier returns, write-offs, or shrinkage adjustments. Must contain: Item Name, Quantity Adjusted, Reason (if available), Net Amount. | To quantify the cost of spoilage, expiry, and over-ordering. |
| 4 | **Branch Transfers (STI/STO)** | If multi-branch: Stock Transfer In/Out logs. Must contain: From Branch, To Branch, Item Name, Quantity, Cost Value. | To measure allocation failure and lateral logistics cost. |
| 5 | **Stock Snapshot (Optional)** | A current stock-on-hand report. Item Name or Barcode, Current Quantity, Cost Price. | To identify items currently sitting at zero (stockouts) or excessively high levels (dead stock). |

### Deliverables
- Signed NDA (if required by prospect).
- Raw data files received and securely stored.

### Decision Gate
**Data received.** Proceed to Phase 1 once at least the POS log and GRN log are in hand.

---

## PHASE 1: THE FORENSIC AUDIT (The Free Diagnosis)
**Timeline: 24-48 Hours After Data Receipt**

### Objective
Ingest the prospect's raw operational data, run it through the O.A.S.I.S. forensic engine, and produce a consulting-grade diagnostic report that quantifies the total revenue bleed.

### Prerequisites
- At least POS and GRN logs received (Transfers and PRTS are bonuses).
- Data is in a parseable format (CSV, Excel, JSON).

### Process
1. **Data Ingestion:**
   - Load the prospect's files into the O.A.S.I.S. Pitch Dashboard (`pitch_app_v2.py`).
   - The `ForensicOperationsIngestor` automatically detects file format and parses columns.
   - If column names don't match our standard schema, manually map them in the ingestor (this is a one-time configuration per client ERP system).

2. **Engine Execution:**
   - **AMIT Scan:** Identifies all SKUs with Average Daily Sales (ADS) < 0.2 units/day AND Stock On Hand > 15 units. These are flagged as **Dead Stock** with trapped capital value calculated as `SOH x Unit Cost`.
   - **DHARAM Scan:** Identifies all SKUs with ADS > 2.0 units/day AND SOH = 0. These are flagged as **Ghost Demand** with lost revenue estimated over a 14-day stockout window as `ADS x 14 x Unit Selling Price`.
   - **LATA Scan:** Groups all GRN records by supplier. For each supplier, calculates:
     - `Fulfillment %` = Average of (GRN Qty / PO Qty) across all orders.
     - `Lead Time Variance` = Standard Deviation of (GRN Date - PO Date) in days.
     - Suppliers with Fulfillment < 85% OR Variance > 3 days are classified as **CRIMINAL/HOSTILE**.
   - **MANDE Scan:** Sums the absolute Net Amount of all PRTS (returns) and STI (transfer) documents to compute total **Network Entropy Cost**.

3. **Report Generation:**
   - Generate the **Executive Summary Word Document** (`OASIS_Executive_Diagnostic.docx`):
     - Cover page with confidential classification.
     - Full methodology explanation (what data was used, how each metric was calculated).
     - Item-level evidence tables (top 20 dead stock items, all stockout items, criminal supplier register).
     - Baseline benchmark scorecard (PASS/FAIL against O.A.S.I.S. healthy retail standards).
     - Consolidated financial impact summary.
     - Projected O.A.S.I.S. ROI (80% dead stock liquidation, 80% revenue recovery, 60% entropy reduction).
   - Generate the **Raw Forensic Data Excel** (`OASIS_Forensic_Audit_Data.xlsx`):
     - Sheet 1: Projected ROI summary.
     - Sheet 2: Full dead stock register (every dormant SKU with SOH, ADS, capital trapped).
     - Sheet 3: Ghost demand register.
     - Sheet 4: Full supplier variance log (every supplier with fulfillment % and lead variance).

4. **The Presentation:**
   - Schedule a 45-minute meeting with the prospect's executive team (CEO, COO, Head of Procurement).
   - Walk through the Streamlit dashboard live, tab by tab.
   - At the end, hand over the Word and Excel reports as the "take-home diagnosis."
   - **Critical framing:** *"This is what your business looks like under a microscope. These are not opinions — these are mathematical facts extracted from your own data. The question now is: do you want to keep bleeding, or do you want us to fix it?"*

### Deliverables
- `OASIS_Executive_Diagnostic.docx` — The consulting-grade Word report.
- `OASIS_Forensic_Audit_Data.xlsx` — The raw data proof.
- Live Streamlit dashboard presentation.

### Decision Gate
**Client agrees to proceed with O.A.S.I.S. deployment.** Contract is signed. Proceed to Phase 2.

---

## PHASE 2: API HOOK & SHADOW MODE
**Timeline: Weeks 1-2 After Contract Signing**

### Objective
Connect O.A.S.I.S. to the client's live ERP/POS system without disrupting their current operations. Run the engine in "Shadow Mode" to build trust and prove algorithmic superiority over human buyers.

### Prerequisites
- Contract signed. 
- Technical access to the client's ERP database or API (read-only access is sufficient for this phase).

### Process
1. **ERP Integration:**
   - Establish a daily automated data pull from the client's POS and inventory systems.
   - This replaces the manual CSV upload from Phase 1 with a scheduled pipeline.
   - Supported integrations: Direct SQL connection, REST API, SFTP file drop (automated CSV/Excel export from their ERP).

2. **Shadow Mode Activation:**
   - O.A.S.I.S. begins generating **daily Auto-Replenishment Purchase Orders** internally.
   - These POs are **NOT** sent to suppliers. They are stored in a shadow log.
   - The client's human buyers continue ordering normally. Their actual POs are also logged.

3. **Daily Shadow Comparison Report:**
   - Every morning, generate a comparison report:
     - What the human buyer ordered yesterday vs. what O.A.S.I.S. would have ordered.
     - Highlight specific divergences: items the buyer over-ordered (future dead stock), items the buyer missed (future stockouts), suppliers the buyer used that LATA has flagged as hostile.
   - Deliver this report to the client's procurement manager daily via email or dashboard.

4. **Week 2 Shadow Review Meeting:**
   - After 14 days, schedule a review meeting with the client's executive team.
   - Present the aggregated shadow comparison:
     - *"Over the past 14 days, your buyer ordered KES X of dead stock items. O.A.S.I.S. ordered KES 0."*
     - *"Your buyer missed restocking Y fast-moving items. O.A.S.I.S. caught all Y."*
     - *"Your buyer ordered from Z hostile suppliers without adjusting quantities. O.A.S.I.S. automatically inflated safety buffers."*
   - This is the **trust-building moment**. The client sees, with their own live data, that the algorithm is smarter than their buyer.

### Deliverables
- Automated data pipeline (daily ERP pull).
- 14 days of Shadow Comparison Reports.
- Week 2 Shadow Review presentation.

### Decision Gate
**Client approves transition from Shadow Mode to Active Mode.** Proceed to Phase 3.

---

## PHASE 3: THE AMIT FLUSH (Dead Stock Liquidation)
**Timeline: Week 3**

### Objective
Before O.A.S.I.S. starts buying new things, it must first stop the bleeding. Activate the AMIT engine to systematically liquidate trapped capital in dead stock.

### Prerequisites
- Shadow Mode review approved.
- Client procurement team briefed on the AMIT "Negative List" concept.

### Process
1. **Generate the AMIT Negative List:**
   - Extract the definitive list of all SKUs classified as dead stock from the latest O.A.S.I.S. scan.
   - Present the list to the client's procurement manager for sign-off. This is a governance step — O.A.S.I.S. recommends, the client approves.
   
2. **Activate the System-Level Block:**
   - Once approved, O.A.S.I.S. writes a hard block against these SKUs in the ordering logic.
   - Human buyers **physically cannot** re-order items on the Negative List through O.A.S.I.S.-generated POs.
   - If the client's ERP supports it, the block is also applied at the ERP level to prevent manual circumvention.

3. **Liquidation Strategy:**
   - For items with remaining shelf life: Apply promotional pricing to accelerate sell-through.
   - For expired or unsaleable items: Initiate formal write-off procedures with the client's finance team.
   - For items with supplier return clauses: Generate PRTS (Purchase Return to Supplier) documents to recover cost from the vendor.

4. **Capital Recovery Tracking:**
   - Track the KES value of capital released as dead stock items sell off or are returned.
   - Report this weekly to the client's finance team as "O.A.S.I.S. Capital Recovery."
   - This is the **first concrete ROI the client sees on their investment.**

### Deliverables
- Signed-off AMIT Negative List.
- System-level purchase blocks activated.
- Weekly Capital Recovery Reports.

### Decision Gate
**Dead stock capital begins freeing up.** Client authorizes O.A.S.I.S. to begin active purchasing for high-velocity items. Proceed to Phase 4.

---

## PHASE 4: HIGH-VELOCITY HYPER-FUNDING (DHARAM Activation)
**Timeline: Weeks 4-6**

### Objective
Redirect the freed capital (from the AMIT flush) into hyper-funding the store's fastest-moving items — the 20% of SKUs generating 80% of revenue. Ensure zero stockouts on these critical items.

### Prerequisites
- AMIT flush in progress. Capital is being recovered.
- Baseline daily sales velocity data (from Phase 1/2 POS analysis) is current.

### Process
1. **Identify the "Revenue Core" (Top 20% by Velocity):**
   - Rank all SKUs by Average Daily Sales (ADS) descending.
   - The top 20% (by velocity) are designated as the **Revenue Core**.
   - These items are the first batch whose ordering is fully controlled by O.A.S.I.S.

2. **Activate DHARAM Ordering:**
   - O.A.S.I.S. begins generating live, real Purchase Orders for Revenue Core items only.
   - For each item, the order quantity is calculated as:
     ```
     Order Qty = (ADS x Lead Time Days) + Safety Stock - Current SOH - In-Transit Qty
     ```
   - Safety Stock is dynamically calculated per item based on demand variance (standard deviation of daily sales).
   - POs are grouped by supplier and presented to the client's procurement manager for daily approval (one-click approval, not manual calculation).

3. **Stockout Monitoring:**
   - The dashboard tracks the stockout rate for Revenue Core items daily.
   - Target: **0% stockout rate** on the Revenue Core within 14 days of DHARAM activation.
   - Any stockout triggers an immediate root-cause alert (supplier delay? demand spike? warehouse error?).

4. **Revenue Impact Reporting:**
   - Weekly report comparing:
     - Revenue from Revenue Core items **before** O.A.S.I.S. (from Phase 1 baseline).
     - Revenue from Revenue Core items **after** DHARAM activation.
   - The delta is the **second concrete ROI metric**.

### Deliverables
- Revenue Core SKU list (the "Priority 1" items).
- Daily auto-generated Purchase Orders for Revenue Core items.
- Weekly Revenue Impact Reports.

### Decision Gate
**Revenue Core stockout rate hits 0%.** Revenue delta is positive. Client authorizes expansion of O.A.S.I.S. ordering to the full catalog. Proceed to Phase 5.

---

## PHASE 5: SUPPLIER SHIELD ACTIVATION (LATA)
**Timeline: Month 2**

### Objective
With the floor optimized (dead stock flushed, fast movers funded), fix the supply chain. Activate the LATA engine to dynamically manage supplier reliability and safety stock buffers.

### Prerequisites
- DHARAM is live on Revenue Core items.
- GRN data pipeline is actively ingesting new delivery data daily.

### Process
1. **Supplier Scorecard Generation:**
   - Using the continuously updated GRN data, generate a live Supplier Scorecard.
   - Each supplier is rated on:
     - Average Fulfillment % (GRN Qty / PO Qty).
     - Lead Time Variance (SD of delivery days).
     - Short-Ship Frequency (% of orders with Received < Ordered).
   - Suppliers are classified: **RELIABLE** (green), **WATCH** (yellow), or **HOSTILE** (red).

2. **Dynamic Safety Stock Adjustment:**
   - For **RELIABLE** suppliers: Safety stock is set to a lean `1.2x` multiplier. This minimizes tied-up capital.
   - For **WATCH** suppliers: Safety stock is inflated to `1.5x`. The retailer carries a modest buffer.
   - For **HOSTILE** suppliers: Safety stock is inflated to `2.0x` or higher. O.A.S.I.S. orders aggressively early to protect the shelf from inevitable short-ships and delays.
   - These multipliers are applied automatically to every PO generated by O.A.S.I.S. — no human intervention required.

3. **Supplier Accountability Reports:**
   - Monthly Supplier Performance Reports are generated and shared with the client's procurement team.
   - These reports serve as formal evidence for renegotiating supplier contracts, demanding credits for short-ships, or terminating hostile vendor relationships.

### Deliverables
- Live Supplier Scorecard (integrated into dashboard).
- Automated per-supplier safety stock multipliers.
- Monthly Supplier Performance Reports.

### Decision Gate
**Safety stock is dynamically managed per supplier.** Shelf availability improves without capital inflation. Proceed to Phase 6.

---

## PHASE 6: FULL AUTONOMOUS ORDERING (MANDE & Network Optimization)
**Timeline: Month 3**

### Objective
O.A.S.I.S. takes over 100% of the daily ordering cycle across all branches and all SKUs. The MANDE engine mathematically allocates inbound stock to the exact branch that needs it.

### Prerequisites
- AMIT, DHARAM, and LATA are all live and stable.
- Client has approved full catalog expansion (not just Revenue Core).
- Multi-branch allocation data (if applicable) is available.

### Process
1. **Full Catalog Activation:**
   - Expand O.A.S.I.S. ordering from the Revenue Core (top 20%) to the **full SKU catalog**.
   - The AMIT Negative List continues to block dead items. Everything else is ordered algorithmically.

2. **Branch-Level Allocation (Multi-Store Only):**
   - For clients with multiple branches, the MANDE engine calculates the **optimal distribution** of each inbound PO across branches.
   - Allocation is based on:
     - Branch-specific sales velocity (ADS per branch per item).
     - Branch-specific current SOH.
     - Branch-specific demand seasonality.
   - The goal: **eliminate lateral transfers entirely.** If O.A.S.I.S. places the right quantity at the right branch on Day 1, there is no need to move stock between branches on Day 7.

3. **Procurement Team Transition:**
   - The client's procurement/buying team transitions from **manual order creation** to **order approval**.
   - Their new daily workflow:
     1. Open the O.A.S.I.S. dashboard.
     2. Review the auto-generated Daily Master PO.
     3. Click "Approve" or flag specific items for manual override (with documented reason).
   - The team's role shifts from data entry to **exception management**.

4. **Continuous Optimization:**
   - O.A.S.I.S. re-calculates all engine parameters daily based on the latest POS and GRN data.
   - The demand model improves with each cycle. After 90 days, predictions are typically 15-25% more accurate than Day 1.
   - The AMIT scan runs weekly, automatically adding newly dormant items to the Negative List.
   - The LATA scan runs daily, automatically adjusting supplier risk scores as new GRNs arrive.

### Deliverables
- Full-catalog autonomous PO generation.
- Branch-level allocation (if multi-store).
- Procurement team fully transitioned to approval-only workflow.
- Ongoing daily/weekly/monthly automated reporting.

### Decision Gate
**Lateral transfers approach zero. Stockout rate < 2%. Dead stock < 5% of catalog. The operation is self-driving.**

---

## POST-IMPLEMENTATION: ONGOING VALUE

Once Phase 6 is complete, O.A.S.I.S. continues to run autonomously. The ongoing value proposition:

| Metric | Pre-O.A.S.I.S. | Post-O.A.S.I.S. Target |
|---|---|---|
| Dead Stock % | 30-50% of catalog | < 5% |
| Stockout Rate (Fast Movers) | 10-20% | < 2% |
| Capital Utilization | 50-70% | > 95% |
| Supplier Fulfillment (Avg) | Unmonitored | > 85% enforced |
| Lateral Transfers | High (weekly) | Near zero |
| Procurement Team Size | 5-10 manual buyers | 1-2 approval managers |
| Order Accuracy | Human gut instinct | Mathematical precision |

---

## TIMELINE SUMMARY

```
Week 0       : First Contact & Data Request
Day 1-2      : Phase 1 — Forensic Audit (The Free Diagnosis)
               ↳ Deliverable: Word Report + Excel Data
               ↳ DECISION: Client signs contract

Week 1-2     : Phase 2 — Shadow Mode (Prove The Algorithm)
               ↳ Deliverable: 14-day Shadow Comparison
               ↳ DECISION: Client approves active mode

Week 3       : Phase 3 — AMIT Flush (Stop The Bleeding)
               ↳ Deliverable: Capital Recovery Reports
               ↳ DECISION: Client authorizes active purchasing

Week 4-6     : Phase 4 — DHARAM Hyper-Funding (Fix The Revenue)
               ↳ Deliverable: Revenue Impact Reports
               ↳ DECISION: Client authorizes full catalog

Month 2      : Phase 5 — LATA Shield (Fix The Supply Chain)
               ↳ Deliverable: Supplier Scorecards
               ↳ DECISION: Client authorizes network optimization

Month 3      : Phase 6 — Full Autonomous (Self-Driving Retail)
               ↳ Deliverable: Zero-touch procurement
               ↳ OUTCOME: The operation runs itself.
```

---

*This playbook is a living document. Update after each client engagement with lessons learned.*
