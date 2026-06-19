# O.A.S.I.S. — Client Data Contract & Onboarding (Onsite Install)

> What a client must provide so we can run the OASIS engines against their live
> POS/ERP, and how an onsite install is wired. The data contract below is
> **enforced by the preflight check** (`python entrypoint.py --mode preflight`) —
> the table/column lists are the same constants used in
> `oasis/logic/preflight.py`, so this document and the code cannot drift.
> Generated 2026-06-18.

---

## 1. Deployment model — onsite silo

- **Direct installation on the client's machine** (or a server on their LAN). No
  cloud; the install is a self-contained silo.
- **Two database roles, kept separate** (enforced in code as of this release):
  - **POS/ERP source** — the client's existing system, accessed **read-only**.
    Configured via `OASIS_POS_DB_URL`.
  - **OASIS store** — OASIS's *own* database (users, audit, config, and the
    purchase-order / transfer queues OASIS manages). A local SQLite file by
    default, or the client's Postgres. Configured via `OASIS_DB_URL`.
  - OASIS **never writes into the client POS DB.** Recommendations land in the
    OASIS store's `INTEGRATION_*` tables, from which they can be exported or fed
    into the client's procurement.
- If only `OASIS_DB_URL` is set (no `OASIS_POS_DB_URL`), OASIS reads and writes
  one DB — the demo/single-store mode. Production sets both.

```
[ Client POS/ERP DB ]  --read-only-->  [ OASIS install ]  --writes-->  [ OASIS store ]
   ITEM_MST, STOCK_MASTER,                 3 consoles            OASIS_USERS, _AUDIT_LOG,
   POS_SALES_DTL, GRN, ...                 + engines             _SYSTEM_CONFIG,
   (OASIS_POS_DB_URL)                                            INTEGRATION_* (OASIS_DB_URL)
```

All three systems — **Operations Console** (`run_oasis.bat`), **Intelligence
Console** (`run_oasis_intel.bat`), and **Command Center** (`run_command_center.bat`)
— read through the same adapter, so this one contract serves all three.

---

## 2. Required data (POS/ERP source) — the hard contract

These tables/columns **must** be readable (directly or via views with these
names). Missing any of them fails the preflight.

| Table | Required columns | Purpose |
|---|---|---|
| `ITEM_MST` | `ITM_CD, ITM_LONG_NAME, DEPARTMENT, SUPPLIER_CD, ACTIVE_FLAG` | product master |
| `STOCK_MASTER` | `SM_ITM_CD, SM_ORG_CD, SM_QTY` | live on-hand per store |
| `POS_SALES_DTL` | `ORG_CD, ITM_CD, BILL_DT, QTY, VOID_FLAG` | transaction-level sales (demand, ADS, the stockout-validation outcome) |
| `ORGANIZATION_MST` | `ORG_CD, ACTIVE_FLAG` | store list |

**Sales-history depth:** at least **90 days** of `POS_SALES_DTL` (the recency-
weighted ADS and the risk model need it; more is better and unlocks the
real-outcome stockout validation).

## 3. Recommended data (improves intelligence; preflight warns if absent)

| Table | Key columns | Adds |
|---|---|---|
| `POS_SALES_HDR` | `ORG_CD, BILL_NO, BILL_DT` | basket/transaction linkage |
| `BASIC_SP_MST` | `BSP_ITEM_CD, BSP_ORG_CD, BSP_SP` | selling price / margin |
| `BASIC_CP_MST` | `BCP_ITEM_CD, BCP_ORG_CD, BCP_CP` | cost price (transfer/PO economics) |
| `SUPPLIER_MST` | `SUPPLIER_CD, SUPPLIER_NAME` (+ lead time / reliability if held) | supplier risk, lead-time σ |
| `GRN_HDR` | `SUPPLIER_CD, ORG_CD` | delivery cadence, fill rate, lead time |

> **Schema adaptation.** OASIS's queries use the names above. If the client's
> schema differs, the standard integration is a thin set of **read-only views**
> named as in the tables above (or a per-client query pack). The `SchemaMapper`
> handles column-name aliasing on the output side; the views handle table/column
> naming on the input side.

## 4. OASIS store (we create + own it)

OASIS auto-creates these in its **own** store (not the client POS):
`OASIS_USERS`, `OASIS_AUDIT_LOG`, `OASIS_SYSTEM_CONFIG`, `OASIS_SESSIONS`,
`INTEGRATION_PURCHASE_ORDERS`, `INTEGRATION_TRANSFER_ORDERS`. Default is a local
SQLite file; for multi-user installs use the client's Postgres via `OASIS_DB_URL`.

---

## 5. What we need from the client (checklist)

**Database access**
- [ ] A **read-only login** to the POS/ERP (or a nightly replica), reachable from
      the OASIS machine, exposing the §2/§3 tables or agreed views.
- [ ] Connection details: backend (SQL Server / Postgres / MySQL / Oracle / SQLite),
      host\instance, database name, port, username, password.
- [ ] For SQL Server: the ODBC driver installed on the OASIS machine (`pyodbc`).

**Host machine (onsite)**
- [ ] A Windows/Linux machine on the LAN with network access to the POS DB.
- [ ] Python runtime + the OASIS package (we install), plus disk for the local
      OASIS store and logs.

**Business config**
- [ ] Store/org codes in scope; default lead times / safety-stock days; the
      `gnn_risk_blend_ratio` and ordering thresholds (seeded into `OASIS_SYSTEM_CONFIG`).
- [ ] Where purchase orders / transfers should go (export from `INTEGRATION_*`,
      or a hand-off into their procurement process).

**Secrets (env only; never committed)**
- [ ] `OASIS_SEED_PASSWORD` — to seed the initial admin user deterministically.
- [ ] `OASIS_API_KEY`, `OASIS_LICENSE_SALT` — per the secrets model.

---

## 6. Install sequence

1. **Provision** the OASIS machine; install the runtime + package; install the
   POS DB driver (e.g. ODBC for SQL Server).
2. **Configure env:**
   ```
   OASIS_POS_DB_URL = mssql+pyodbc://readonly:***@SERVER/POSDB   # read-only source
   OASIS_DB_URL     = sqlite:///C:/oasis/data/oasis_store.db     # OASIS's own store
   OASIS_SEED_PASSWORD = ********
   OASIS_API_KEY = ...   OASIS_LICENSE_SALT = ...
   ```
   (Omit `OASIS_POS_DB_URL` only for a single-DB demo.)
3. **Migrate / bootstrap the OASIS store:** `python entrypoint.py --mode migrate`
   (creates OASIS tables + seeds the admin from `OASIS_SEED_PASSWORD`).
4. **Build schema views** *(only if the client schema differs from §2/§3)*:
   author a schema profile (canonical → client names) and emit the read-only views:
   ```
   OASIS_SCHEMA_PROFILE=client.json OASIS_DB_DIALECT=mssql \
       python entrypoint.py --mode build-views > views.sql
   ```
   The DBA runs `views.sql` once. (No profile = identity / RXL schema.)
5. **Preflight:** `python entrypoint.py --mode preflight` — must be `PASS` (or a
   `WARN` you accept, e.g. short history). Checks connectivity, the §2/§3
   contract, sales-history depth, and store writability. Exits non-zero on `FAIL`.
6. **Intelligence bootstrap** (one-time, then periodic — one command):
   ```
   python entrypoint.py --mode bootstrap-intel
   ```
   Generates `sales_forecasting`/`supplier_patterns` from the client's live POS
   (writes the `*_updated.json` files the engine prefers).
7. **Governance bootstrap** (one command): generate the governance artifacts by
   orchestrating the canonical engines in order (LATA → AMIT → MANDE → DHARAM):
   ```
   python entrypoint.py --mode bootstrap-governance
   ```
   Writes `amit_enforcement.json` (dead-stock blacklist), `mande_purge_report.json`
   (supplier delisting), `dharam_demand_patch.json`, and the LATA-enriched
   `supplier_patterns`. Needs the `neutral_network_export/` graph (built upstream
   by the forensic ingestion of the catalog/graph). Schedule steps 6–7 on a
   periodic refresh (e.g. monthly).
8. **Launch** the consoles (`run_oasis.bat` / `run_oasis_intel.bat` /
   `run_command_center.bat`) and log in with the seeded admin.

---

## 7. Status & open items (honest)

- **Live intake: ready.** All three consoles read live stock, transaction-level
  sales, recency-weighted ADS, on-order awareness, and write POs/transfers back —
  through one DB-agnostic adapter. Read/store separation is enforced. The
  preflight gates an install.
- **Per-client schema views**: now a command — `--mode build-views` emits the
  read-only view DDL from a schema profile (the one integration task that varies
  per client, only when their schema differs from §2/§3 names).
- **Intelligence bootstrap (demand + supplier)**: one command —
  `--mode bootstrap-intel` regenerates `sales_forecasting`/`supplier_patterns`
  from the live POS.
- **Governance bootstrap (AMIT/LATA/MANDE/DHARAM)**: one command —
  `--mode bootstrap-governance` orchestrates the canonical engines. The remaining
  upstream dependency is the `neutral_network_export/` graph, still built by the
  forensic ingestion of the catalog (the last non-command step).
- **Risk model**: the engines run on the interpretable inventory logic; the
  GNN/ML risk stays **monitoring-only** until validated against real daily
  stockout outcomes (see `OASIS_Risk_Scoring_Methodology_Redesign.md`). Not wired
  to ordering.
- **OASIS store on non-SQLite/Postgres**: the raw helper path supports SQLite +
  Postgres; SQL Server is fine as the **read-only source**, but use SQLite or
  Postgres for the **OASIS store**.
