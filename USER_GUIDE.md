# OASIS Retail Manager — User Guide

> **Version 4.0** | Operations · Allocation · Sales Intelligence · Simulation

---

## Quick Start

### 1. Launch the Dashboard
```bash
# Option A: Batch file
run_command_center.bat

# Option B: Direct command
streamlit run ops_dashboard.py --server.headless true

# Option C: Docker
docker compose up -d
# Open http://localhost:8501
```

### 2. Sign In

| Account | Username | Access Level |
|---------|----------|-------------|
| Full Access | `ops_admin` | All tabs + Settings |
| Regional | `regional_mgr` | Multi-store + Approvals |
| Branch | `branch_mgr` | Single-store view |
| Demo | `demo_user` | Single-store view |

Passwords are set by the `OASIS_SEED_PASSWORD` environment variable when the
database is first seeded (see `.env.example`). If it was not set, a one-time
password per account is printed to the application log on first seed — sign in
with it and rotate immediately. Contact your administrator for credentials.

---

## Dashboard Tabs

### 📊 Live Sales Feed
Real-time sales monitoring with hourly revenue chart, top movers, and velocity spike alerts.

- **Revenue metrics**: Running total for the simulated day
- **Velocity Ratio**: Current sales pace vs historical ADS. Red = spike, Green = normal
- **Alerts**: Automatic detection of abnormal selling patterns

### 🔄 Transfer Intelligence
Three-layer stockout prevention:
1. **Live Sim Transfers** — Real-time transfer opportunities from the intra-day simulator
2. **ST-GAT Neural** — GNN-powered risk scoring and network transfer recommendations
3. **ADS Heuristic** — Item-level stockout projection based on current depletion rates

### 📦 End-of-Day Stock
Stock health matrix showing days-cover, stock vs demand scatter analysis, and department breakdown.

### 🛒 Smart Ordering
Per-store purchase order generation with:
- SimulationBridge deterministic logic
- Supplier calendar integration
- GNN risk-adjusted safety stock
- Chaos & disruption scenario testing
- PO approval workflow (Generate → Review → Approve)

### 🚀 OASIS Processor
Batch file processing for picking lists and GRNs. Upload files → get enriched Excel reports with AI recommendations.

### 🧮 Allocation Engine
Budget-constrained order basket generation:
1. Set **capital budget** and **target month**
2. Click **Run Allocation** — two-pass allocation with efficiency guards
3. Review KPIs: ROI, utilization, days-to-ROI
4. Download the generated order basket as CSV

### 🧪 Simulation Lab
Monte Carlo simulation comparing heuristic vs GNN-adjusted ordering across multi-day scenarios.

### 📈 Analytics
Historical KPI tracking: weekly revenue trends, PO statistics, stockout heatmaps.

### ⚙️ Settings (Admin Only)
- **System Config**: Edit thresholds (spike %, safety stock days, MOV)
- **Scheduler**: Toggle automated background jobs (morning PO, hourly monitor, evening summary)
- **User Management**: View registered accounts
- **Audit Trail**: Full activity log with filters

---

## Common Workflows

### Morning Routine (Branch Manager)
1. Sign in → Check **Live Sales Feed** for overnight alerts
2. Open **End-of-Day Stock** → review items below safety threshold
3. Open **Smart Ordering** → generate PO recommendations
4. Export PO as CSV → forward to procurement

### PO Approval (Regional Manager)
1. Sign in → Check 🔔 notification bell for pending POs
2. Open **Smart Ordering** → review line items and quantities
3. Override quantities if needed (with reason capture)
4. Click **Approve & Push** to finalize

### Transfer Execution (Regional Manager)
1. Open **Transfer Intelligence** → review live sim transfer opportunities
2. Check GNN risk scores across stores
3. Click **Execute Live Sim Transfers** for urgent items
4. Track transfer status (REQUESTED → IN_TRANSIT → RECEIVED)

### Allocation Planning (Any Role)
1. Open **🧮 Allocation Engine** tab
2. Set budget slider and target month
3. Run allocation → review department spend pie chart
4. Download basket CSV for procurement

---

## Sidebar Controls

- **📅 Simulation Date**: Set the operating day
- **📍 Store**: Select which store to view (role-filtered)
- **🕐 Time of Day**: Simulate different hours (6AM–10PM)
- **♻️ Reset Simulator**: Restart the intra-day simulation engine
- **🔗 ERP Connection**: Database health status
- **📋 Recent Activity**: Quick-view audit log

---

## FAQ

**Q: How do I change thresholds (spike %, safety stock days)?**
Sign in as `ops_admin` → ⚙️ Settings → edit values → Save.

**Q: Can I run the allocation without the Command Center?**
Yes. The standalone `allocation_app.py` still works independently via `run_allocation_app.bat`.

**Q: How do I set up scheduled jobs?**
Sign in as `ops_admin` → ⚙️ Settings → Scheduler section → Start Scheduler → toggle individual jobs.

**Q: Does the Docker deployment persist my data?**
Yes. Database and uploads are stored in named Docker volumes that survive container restarts.
