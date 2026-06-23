# O.A.S.I.S. — Detailed Change Explanation (June 2026)

A file-level, before→after walkthrough of everything changed in this window. Pairs with the
high-level [handover](OASIS_Handover_Basket_and_LivePOS_2026-06.md). Share this with anyone who
needs to understand *exactly what changed and why*.

Conventions: **Files** = what was touched · **Before** = prior behaviour · **After** = new behaviour ·
**Why** = the reasoning · each block ends with the commit.

---

## 0. Command Center live clock — time of day auto-accrues with the POS  *(this change)*

**Files:** `ops_dashboard.py`, `run_command_center_live.bat`

**Before.** The Command Center had a manual sidebar **"🕐 Time of Day" slider** (06:00–22:00, default 14:00).
The operator dragged it to scrub the store's state at a chosen hour. Fine for a static demo, wrong for a
**live run** — you'd have to keep dragging it to "watch" the day progress, and it has no relationship to the
sales actually arriving from the mock POS.

**After.** A new **live mode** (`OASIS_LIVE_MODE=true`, set by `run_command_center_live.bat`):

- The slider is **replaced by an auto-accruing clock**. The trading hour is *derived from the sales rung up
  today* in the live snapshot:

  ```
  bills_today      = COUNT(POS_SALES_HDR WHERE BILL_DT = today)
  fraction_of_day  = min(1, bills_today / OASIS_LIVE_FULL_DAY_BILLS)   # default 1000 bills = a full day
  sim_hour         = 6 + round(fraction_of_day * 16)                   # 06:00 (open) → 22:00 (close)
  ```

  So as the mock POS streams more receipts, the clock advances by itself — **the day progresses in lockstep
  with POS activity**, not with a slider or wall-clock. A read-only sidebar card shows
  `LIVE — HH:00 · N sales today · X% of trading day`.
- The **IntraDaySimulator** (which powers the risk roll-up and hourly chart) is now built from the **live DB
  itself** in live mode, instead of the canned `mock_pos_erp_showcase.db`, so what you see reflects the real
  streamed sales.
- Outside live mode nothing changes — the manual slider remains for static exploration.

**Why.** The user is now doing *live runs*: the mock POS streams real sales into the static snapshot and the
consoles should show the day unfolding from those signals. A manual slider fights that; an auto clock tied to
accrued sales makes "how the system reacts to live sale signals" visible without touching a control.

**How to use.** Launch `run_command_center_live.bat` (sets `OASIS_LIVE_MODE=true` + points at
`rhapta_pos.db`), start `run_mock_pos.bat`, and refresh — the clock and KPIs accrue as receipts land. Tune the
day length with `OASIS_LIVE_FULL_DAY_BILLS` (e.g. `set OASIS_LIVE_FULL_DAY_BILLS=2000` for a longer day).

*(uncommitted at time of writing this section → see commit log)*

---

## 1. Mock POS: from "Live POS panel" (rejected) to a transactional, real-time stream

### 1a. The rejected artifact
**Files:** `oasis/ui/shell.py`, `oasis/ui/intel.py`, `ops_dashboard.py`, `oasis/logic/pos_activity.py` (deleted),
the `*_demo.bat` launchers (deleted).
**Before.** First attempt at "see injections live" added a bespoke **"Live POS" page/tab** to all three
consoles plus an `OASIS_DEMO_MODE` `st.fragment` auto-refresh.
**After.** All of it **removed**. `pos_activity.py`, the panels, the registry entries, the demo launchers, the
auto-refresh — gone.
**Why.** Design review: a POS feed is *purely transactional sales* and must not carry restock data; and the
consoles must react through their **real** views, not a demo artifact we'd never ship.
Commit `7d3d357` (and the superseded `460338c`/`495b7cb`/`41dddc8`).

### 1b. Sales-only injector
**Files:** `oasis/logic/pos_injector.py`.
**Before.** The injector both **sold and restocked** SKUs (it topped stock back up).
**After.** Restocking **removed**. It rings up bills and decrements on-hand only — a till sells, it never
receives goods. `build_bill` is pure; `inject_once`/`run_injector` are sales-only. Commit `7d3d357`.

### 1c. Real-time stream with stock integrity
**Files:** `oasis/logic/pos_simulator.py`, `entrypoint.py`, `run_mock_pos.bat`, `tests/test_pos_simulator.py`.
**Before.** Sales were generated in bulk (in-memory, one big write at the end) — nothing visible mid-run.
**After.**
- `ring_up(codes, stock, meta, rng)` — a **pure stock-integrity gate**: a receipt line is only created for an
  item with on-hand > 0, quantity never exceeds availability (no overselling, stock never negative), and
  out-of-stock items are dropped from the basket. Unit-tested.
- `stream_realtime(...)` (`--mode pos-stream`) — rings up baskets **one at a time, committing each receipt**,
  so WAL readers (the consoles) see every sale immediately. On-hand is **re-read from the DB per receipt**, so
  integrity is authoritative against the live snapshot. `--batches 0` streams until Ctrl-C; `--interval` paces it.
- `run_mock_pos.bat` repointed to the real-time streamer; auto-builds the snapshot + prior on first run.
Commit `d9aa854`.

---

## 2. Real Rhapta catalog → the static stock snapshot

**Files:** `oasis/logic/rhapta_catalog.py` (new), `oasis/logic/mock_pos_build.py` (new), `entrypoint.py`.

**Before.** The only mock DB was `mock_pos_erp.db` — synthetic, with randomly generated stock and 621 k
synthetic sales rows. No real structure.

**After.**
- `rhapta_catalog.py` loads and de-dupes the **six real `dept_*.xlsx`** exports (VENDOR_NAME, BARCODE,
  ITM_NAME, DEPARTMENT, SellPrice, STOCK) → 39,728 SKUs, 247 departments, 823 vendors. `vendor_departments()`
  maps each vendor to its primary department.
- `mock_pos_build.py` (`--mode build-pos-db`) builds the **static stock snapshot** `rhapta_pos.db`: it reuses
  the canonical RXL `SCHEMA_SQL` and the generator's auth/tax/counter/config seeds (so the consoles still log
  in), populates `ITEM_MST / STOCK_MASTER / BASIC_SP_MST / BASIC_CP_MST / SUPPLIER_MST` from the real catalog
  (cost estimated at 0.82 × price), one Rhapta store, and **empty sales**.
**Why.** A live-run demo needs an honest "real world" starting inventory. This snapshot *is* the static stock —
the fixed start-of-day state that sales draw down. Commits `f89b91e`, `afa27ac`.

---

## 3. Basket affinity engine (DHARAM's anchor→attachment layer)

### 3a. Market-basket mining
**Files:** `oasis/logic/basket_affinity.py` (new), `oasis/logic/dharam_revenue.py`, `entrypoint.py`.
**Before.** DHARAM's `link` (basket) relation was **empty/meaningless** — the graph's link edges pointed
SKU→department, not SKU↔SKU co-purchase, so anchor/attachment discovery found nothing.
**After.** `basket_affinity.py` mines co-purchase from `POS_SALES_DTL` (items on the same bill): co-occurrence
→ **support / confidence / lift**; only above-chance pairs (lift > 1) become `link` edges, weighted by
co-count. `--mode build-baskets`. `dharam_revenue.load_edges` made **weight-aware**.
**Why.** Lift (not raw frequency) is the correct gate — a popular item co-occurs with everything; frequency
alone invents affinities. Commit `e924f1a`.

### 3b. Directional affinity (the Bible Ch. 8.4 fix)
**Files:** `oasis/logic/basket_affinity.py`, `oasis/logic/dharam_revenue.py`.
**Before.** Edges were emitted **bidirectionally** (a→b *and* b→a, equal weight); DHARAM mirrored them too.
**After.** One **directed** edge per pair, anchor→attachment: the anchor is the higher-`velocity_ads` SKU
(the price-sensitive "destination"), with the association antecedent (higher-confidence direction) as the
tie-break. DHARAM consumes link edges directionally (no reverse mirror).
**Why.** Kenyan_Retail_Bible Ch. 8.4 "Broken Halo": affinity is *not* mutual — diapers pull wipes, not vice
versa; you only ever discount the Anchor. Commit `9e00678`.

### 3c. Vault coarse prior (cold-start)
**Files:** `oasis/logic/vault_prior.py` (new), `entrypoint.py`.
**Before.** Nothing seeded baskets before live co-purchase data existed.
**After.** `vault_prior.py` parses the vault's Supplier-node `[complimentary]:: [[X]] (Weight: N)` edges (538
suppliers) and **projects them to a department-level halo prior** via each vendor's primary department
(`--mode build-prior` → `basket_prior.json`). Output is intuitive: CHOCOLATES→SWEETS/BISCUITS, HERBAL
TEAS→TEA/CEREALS, OLIVES→PASTA.
**Why.** The vault is GRN/supply-derived and has **no SKU-level co-purchase**, but its supplier affinity is a
usable coarse prior to bootstrap baskets — superseded by real SKU confidence/lift once live data lands.
Commit `f89b91e`.

### 3d. Affinity-aware simulator
**Files:** `oasis/logic/pos_simulator.py`.
**Before.** Sales were random — co-purchase had no structure, so nothing was recoverable.
**After.** A trip starts at a seed department, picks a **Zipfian-popular Anchor**, then pulls **Attachments
from complementary departments** drawn from the halo prior. A `core_per_dept` cap concentrates sales on a
realistic fast-moving assortment so hero pairs recur. `--mode pos-sim` (bulk) and `--mode pos-stream`
(real-time). Commit `afa27ac`.
**Validated:** 40 k trips → 20,996 bills → mine → **201 above-chance pairs, 97 % department-affiliated**.
**Honest caveat:** this validates *pipeline wiring*, not ground-truth affinities (the prior is a coarse
supply-side stand-in); real validation needs live customer co-purchase.

---

## 4. Login fix on the catalog-built DB

**Files:** `oasis/logic/mock_pos_build.py`.
**Before.** `build-pos-db` seeded users via the canonical seeder, which — with no `OASIS_SEED_PASSWORD` set —
generates a **random one-time password per user**. So `oasis2026` failed on `rhapta_pos.db`.
**After.** The builder defaults `OASIS_SEED_PASSWORD=oasis2026` (override still honoured). All five users
(`ops_admin`, `regional_mgr`, `branch_mgr`, `branch_mgr2`, `demo_user`) log in with **`oasis2026`**.
Commit `a200cd4`.

---

## 5. Earlier in the window (condensed — see handover §3 for detail)

- **Risk methodology redesign** (`55bddbc`, `50d6c6a`): outcome-grounded stockout risk
  (`stockout_ledger`/`ledger_loader`/`risk_features`/`risk_baseline`/`backtest`/`risk_calibration`); proved the
  GNN risk head inert and the inventory backtest tautological; GNN gated **inventory-only** in ordering via
  `ordering_risk()` + `OASIS_GNN_ORDERING_WEIGHT` (`3a0c4b7`).
- **Native graph builds** (`173130c`, `8192cfb`): `--mode build-graph` / `build-store-graph`.
- **Client onboarding / RXL port / supplier intake** (`b20a5c5`…`28d4441`): headless modes, view literals +
  `rxl_schema_profile.json`, `suppliers.csv` intake, read-only POS vs OASIS-store DB separation.
- **Zero-ADS FulfillmentDecider** (`1ace323`): investigated — intentional and safe, no fix needed.

---

## 6. New `entrypoint.py` modes (full list)

`preflight · build-views · bootstrap-intel · bootstrap-governance · build-graph · build-store-graph ·
build-baskets · build-prior · build-pos-db · pos-sim · pos-stream · pos-inject`

## 7. Environment flags introduced

| Flag | Effect |
|---|---|
| `OASIS_LIVE_MODE` | Command Center: auto-accruing clock instead of the slider |
| `OASIS_LIVE_FULL_DAY_BILLS` | bills-today that map to a full trading day (default 1000) |
| `OASIS_DB_PATH` | which POS DB the console/stream uses (live run → `rhapta_pos.db`) |
| `OASIS_BASKET_PRIOR` | path to `basket_prior.json` |
| `OASIS_BASKET_MIN_COUNT` / `_LIFT` / `_ITEM_COUNT` | basket-mining thresholds |
| `OASIS_SEED_PASSWORD` | demo login password (default `oasis2026` on catalog build) |
| `OASIS_GNN_ORDERING_WEIGHT` | GNN blend weight in ordering (default 0 = inventory-only) |

---
*Generated 2026-06-23.*
