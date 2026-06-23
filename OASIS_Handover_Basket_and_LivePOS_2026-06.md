# O.A.S.I.S. — Session Handover

**Window:** June 2026 · **Scope:** GNN/risk interrogation → basket-affinity engine → real-catalog static stock + live POS simulation
**Audience:** the next engineer/session picking this up. Read §2 and §4 before changing anything — they capture decisions that should not be re-litigated.

---

## 1. Executive summary

This window moved O.A.S.I.S. from "synthetic mock with an inert GNN" toward an **honest, data-grounded demo of how the live system reacts to a real POS feed**. Three threads:

1. **Risk methodology** — proved the store-GNN risk head was an inert static prior, redesigned an outcome-grounded stockout-risk methodology, and gated the GNN out of ordering until it beats a baseline on real data.
2. **Basket affinity** — built the SKU↔SKU co-purchase layer DHARAM needs (anchor→attachment), made it **directional** per the retail doctrine, and added a vault-derived cold-start prior.
3. **Live POS simulation** — replaced the rejected "Live POS panel" artifact with a clean model: a **static real-world stock snapshot** (the real Rhapta catalog) into which an **affinity-aware POS streams sales in real time**, so the three consoles react exactly as they would against a live till.

Everything is committed, lint-clean, and test-covered (full suite green: **584 passed, 6 skipped, 13 xfailed**).

---

## 2. The operating model — static stock + live sales injection ⭐

This is the mental model for the whole demo and the thing to preserve:

```
  dept_*.xlsx (real Rhapta catalog)                         a simulated working day
            │  build-pos-db                                          │
            ▼                                                        ▼
  ┌───────────────────────┐      pos-stream (sales only)    ┌──────────────────────┐
  │  STATIC STOCK SNAPSHOT │  ───────────────────────────▶  │  three live consoles  │
  │  rhapta_pos.db         │   one receipt at a time,        │  react on refresh     │
  │  (real SKUs/price/qty, │   commit-per-receipt,           │  (stock ↓, at-risk ↑, │
  │   EMPTY sales)         │   strict stock integrity        │   reorder advice)     │
  └───────────────────────┘                                 └──────────────────────┘
            ▲                                                        │
            └──────────────  rebuild to reset to "start of day" ─────┘
```

- **Static stock = the real world.** `rhapta_pos.db` is built from the actual Chandarana Rhapta catalog snapshot (39,728 SKUs, 247 departments, 823 vendors, real prices and on-hand quantities) with **zero sales history**. It is the fixed "start of the working day" inventory.
- **Sales are injected, not invented.** The POS simulator rings up customer baskets and **draws stock down from that static snapshot** — purely transactional (it sells and decrements; it never restocks — restocking is OASIS's job, not the POS's).
- **The live systems react.** Because each receipt is committed immediately, the consoles (pointed at the same DB) reflect the day's activity: stock falls, SKUs go at-risk, Smart Ordering recommends replenishment.
- **Resetting the day** = rebuild the snapshot (`--mode build-pos-db`, which resets sales and restores the static stock).

> A "real working day" simulation = start from the static snapshot, stream the day's sales, watch the systems react. To run another day, rebuild.

---

## 3. What we built, in order

Commits are newest-last within each theme.

### A. Risk methodology redesign (outcome-grounded, beat-baseline-gated)
- Interrogated the store-GNN: its risk head was inert to live signal (a stockout injection moved risk by ~1e-5) — a **static dead-feature prior**.
- Built an outcome-grounded stockout-risk methodology from realized outcomes: `stockout_ledger.py`, `ledger_loader.py`, `risk_features.py` (μ/σ of lead-time demand), `risk_baseline.py` (newsvendor / z-score / Poisson-normal hybrid), `backtest.py` (PR-AUC, calibration, ablation), `risk_calibration.py` (isotonic PAV). Commits `55bddbc`, `50d6c6a`.
- **Self-validation ablation** (`50d6c6a`): proved the inventory backtest (0.93) was tautological — a naive days-of-cover baseline (0.948) beat it. Only the de-circularized supply temporal backtest (0.757) is a genuine signal.
- **Standing gate:** the GNN stays **inventory-only in ordering** (`OASIS_GNN_ORDERING_WEIGHT` default 0) until it beats the baseline on real daily POS outcomes. `ordering_risk()` enforces this across all three surfaces (`3a0c4b7`).

### B. Native graph builds
- `--mode build-graph` (`173130c`) — native product-graph export (`graph_export.py`): nodes + substitution + structural edges, no Obsidian dependency.
- `--mode build-store-graph` (`8192cfb`) — native store-graph (`store_graph_export.py`) + hardened the store-risk org join.

### C. Client onboarding / RXL porting / supplier intake
- Headless onboarding modes: `preflight`, `build-views`, `bootstrap-intel`, `bootstrap-governance` (`b20a5c5`, `2efd50a`, `d37f757`, `30119dc`).
- Real RXL/iRetail backend analysis → `rxl_schema_profile.json` + literal-NULL/WHERE view generation (`35f787f`, `4da039c`) — no adapter surgery.
- Supplier & ordering-calendar client intake (`suppliers.csv` → `supplier_schedule.json`) (`bc24e20`, `28d4441`).
- DB role separation: read-only POS source vs OASIS's own store DB (`bf2aeea`).

### D. Mock POS: the rejected artifact, then the right model
- First attempt: a "Live POS Feed" panel + `OASIS_DEMO_MODE` auto-refresh on all consoles (`460338c`, `495b7cb`, `41dddc8`).
- **Rejected by design review:** a POS feed is *purely transactional sales* and must not carry restock data, and the consoles shouldn't host a bespoke artifact. **Removed it all** and made the mock POS sales-only (`7d3d357`). See §4.

### E. Basket affinity — the core of this window
- `basket_affinity.py` (`e924f1a`) — market-basket mining: co-occurrence → support/confidence/**lift**; lift-gated so popular items don't fake affinities. `--mode build-baskets`. DHARAM's loader made weight-aware.
- **Honest finding:** on the old random mock, mining yielded ~0 real baskets (confidence ≈ 0.01) — the algorithm correctly refused to fabricate structure that wasn't there.
- **Vault analysis** drove the next two pieces (see §4): Kenyan_Retail_Bible Ch. 8 *is* the spec (Confidence/Lift/Anchor/Attachment); Ch. 8.4 says affinity is **directional**.
- `#1` Directional affinity (`9e00678`) — `link_edges` emits one directed anchor→attachment edge (anchor = higher velocity, confidence tie-break); DHARAM consumes directionally.
- `#2` Vault coarse prior (`f89b91e`) — `vault_prior.py` parses the Supplier nodes' weighted `[complimentary]` edges and projects them to a **department halo prior** (`build-prior` → `basket_prior.json`). Cold-start only.

### F. Real Rhapta catalog → static stock + affinity simulator
- `rhapta_catalog.py` (`f89b91e`) — load/dedupe the 6 `dept_*.xlsx`.
- `mock_pos_build.py` (`afa27ac`) — `--mode build-pos-db` builds the clean static snapshot (`rhapta_pos.db`): reuses the canonical RXL schema + auth/tax/config seeds, real masters/stock, **empty sales**.
- `pos_simulator.py` (`afa27ac`) — affinity-aware sales: seed dept → Zipfian anchor → attachments from the halo prior. In-memory + batched for bulk (`pos-sim`).

### G. Real-time streaming + stock integrity
- `pos_simulator.ring_up()` + `stream_realtime()` (`d9aa854`) — `--mode pos-stream`. Rings up baskets **one at a time, commit-per-receipt**, so consoles see each sale live. **Stock integrity is authoritative**: on-hand is re-read per receipt; a line is only created for in-stock items and never exceeds availability (no overselling, no negative stock); OOS items are dropped.
- `_live` launchers point the three consoles at the static snapshot.

### H. Login fix
- `a200cd4` — `build-pos-db` now defaults `OASIS_SEED_PASSWORD=oasis2026` (the catalog build was seeding random passwords). Users: `ops_admin`, `regional_mgr`, `branch_mgr`, `branch_mgr2`, `demo_user` — all password **`oasis2026`**.

---

## 4. Architecture decisions & rationale (do not re-litigate)

| Decision | Why |
|---|---|
| **POS = transactional sales only.** The simulator sells + decrements; it never restocks. | A real POS emits customer sales, not GRN/replenishment. Restocking is OASIS's recommendation, a separate flow. |
| **No "Live POS" artifact in the consoles.** | The consoles must react through their *real* views (Stock Review, Pulse, Ordering). A bespoke demo panel is an artifact we don't ship. |
| **Basket affinity comes from co-purchase, not the vault.** | The vault (`oasis_vault`) is GRN/supply-derived; it has substitution + supplier-level `[complimentary]` but **no SKU-level co-purchase**. Ch. 8.2's own formula needs transactions. |
| **Affinity is directional (anchor→attachment).** | Kenyan_Retail_Bible Ch. 8.4 "Broken Halo": diapers pull wipes, not vice versa; you only ever discount the Anchor. |
| **Lift gate, not raw frequency.** | A popular item co-occurs with everything; frequency alone invents affinities. Lift > 1 = above chance. |
| **GNN stays inventory-only in ordering until it beats baseline on real data.** | The risk head is unvalidated; self-validation ablation showed naive baselines win. `OASIS_GNN_ORDERING_WEIGHT` gates it. |
| **Static stock snapshot from the real catalog.** | Synthetic random stock has no real structure; the real Rhapta snapshot is the honest "real world" baseline for a working-day simulation. |
| **Simulator validates *wiring*, not ground-truth affinities.** | The planted baskets come from a coarse supply-side prior. Recovering them proves the pipeline extracts affinity *when it exists*; real validation needs live customer co-purchase. |

---

## 5. Component reference

### New `oasis/logic/` modules
| Module | Role |
|---|---|
| `stockout_ledger.py`, `ledger_loader.py`, `risk_features.py`, `risk_baseline.py`, `backtest.py`, `risk_calibration.py` | Outcome-grounded risk redesign |
| `graph_export.py`, `store_graph_export.py` | Native product / store graph builds |
| `basket_affinity.py` | Market-basket mining (directional, lift-gated) → DHARAM `link` layer |
| `vault_prior.py` | Vault `[complimentary]` → department halo prior (cold-start) |
| `rhapta_catalog.py` | Load/dedupe the real `dept_*.xlsx` catalog |
| `mock_pos_build.py` | Build the static `rhapta_pos.db` snapshot from the catalog |
| `pos_injector.py` | Simple random sales-only injector (`pos-inject`) |
| `pos_simulator.py` | Affinity-aware simulator: `ring_up` (integrity), `run_simulator` (bulk), `stream_realtime` (real-time) |

### `entrypoint.py --mode …`
`preflight · build-views · bootstrap-intel · bootstrap-governance · build-graph · build-store-graph · build-baskets · build-prior · build-pos-db · pos-sim · pos-stream · pos-inject`

Relevant env: `OASIS_DB_PATH`, `OASIS_BASKET_PRIOR`, `OASIS_VAULT_DIR`, `OASIS_DATA_DIR`, `OASIS_BASKET_MIN_COUNT/_LIFT/_ITEM_COUNT`, `OASIS_SEED_PASSWORD`, `OASIS_GNN_ORDERING_WEIGHT`.

### Launchers
| Launcher | Points at | Purpose |
|---|---|---|
| `run_command_center_live.bat` | `rhapta_pos.db` | Command Center on the static snapshot |
| `run_oasis_live.bat` | `rhapta_pos.db` | Operations Console on the static snapshot |
| `run_oasis_intel_live.bat` | `rhapta_pos.db` | Intelligence Console on the static snapshot |
| `run_mock_pos.bat` | `rhapta_pos.db` | Real-time sales stream (auto-builds snapshot + prior on first run) |
| `run_command_center.bat` / `run_oasis.bat` / `run_oasis_intel.bat` | default `mock_pos_erp.db` | the legacy synthetic DB (not the static Rhapta snapshot) |

---

## 6. Validation results & honest caveats

- **End-to-end recovery:** build static DB → simulate 40,000 trips (20,996 bills) → mine → **201 above-chance SKU pairs, 97 % department-affiliated**. The directional algorithm faithfully recovers the planted halo structure.
- **Stock integrity:** smoke run of 12 real-time receipts → **0 negative-stock rows, 0 oversold lines**; unit tests lock the invariant.
- **Tests:** full suite **584 passed, 6 skipped, 13 xfailed, 0 failed**. New: `test_basket_affinity.py`, `test_vault_prior.py`, `test_pos_simulator.py`.
- **Caveat (carried in code + commits + memory):** the simulator validates **pipeline wiring**, not ground-truth affinities — the prior is a coarse supply-side stand-in, so a few recovered pairs are supplier-affiliated rather than intuitively complementary. **Real validation requires live customer co-purchase data.**

---

## 7. How to run a simulated working day

Each `_live` launcher and `run_mock_pos.bat` auto-builds the static snapshot (`rhapta_pos.db`) and halo prior on first run. Open four PowerShell windows:

```powershell
& "C:\Users\iLink\.gemini\antigravity\scratch\run_command_center_live.bat"
& "C:\Users\iLink\.gemini\antigravity\scratch\run_oasis_live.bat"
& "C:\Users\iLink\.gemini\antigravity\scratch\run_oasis_intel_live.bat"
& "C:\Users\iLink\.gemini\antigravity\scratch\run_mock_pos.bat"   # streams 1 receipt / 2s
```

- **Log in:** any of `ops_admin` / `regional_mgr` / `branch_mgr` / `branch_mgr2`, password **`oasis2026`**.
- Watch the consoles' normal views react as the stream runs: **Stock Review / Pulse** (stock depleting, at-risk rising), **Live Sales**, **Smart Ordering** (reorder advice).
- Pace/volume: `run_mock_pos.bat --interval 1 --batches 200` (or `--batches 0` = until Ctrl-C).
- **Reset to start-of-day:** `python entrypoint.py --mode build-pos-db` (rebuilds the static snapshot with empty sales).

> The DB updates continuously (commit-per-receipt). Streamlit views repaint on **refresh / cache-TTL** — there is intentionally no auto-refresh artifact. If a self-ticking view is wanted later, add a *plain* timed refresh to one view (no DEMO_MODE flag, no separate panel).

---

## 8. Known limitations & next steps

- **F3** — thread LATA's `lata_variance_multiplier` into the replenishment safety buffer.
- **F4** — decide the live ROP/coverage source vs the fallback heuristic.
- **Risk-validation gate** — GNN stays inventory-only until it beats the baseline on the real daily POS outcomes (to be provided).
- **Basket affinity on live data** — re-run `build-baskets` against real bill-level co-purchase; it then supersedes the vault prior with real confidence/lift.
- **Console auto-refresh (optional)** — a plain timed repaint on a single view, if "hands-off real-time" viewing is desired.
- **Working-day realism (optional)** — overnight GRN/replenishment between days, time-of-day demand curve, multi-store.

---

## 9. Commit map (this window)

```
a200cd4  Fix login on catalog-built POS DB (OASIS_SEED_PASSWORD=oasis2026)
d9aa854  Real-time mock POS stream with stock integrity
afa27ac  #3 Mock POS from real Rhapta catalog + affinity-aware simulator
f89b91e  #2 Vault coarse-prior extractor + real Rhapta catalog loader
9e00678  #1 Directional basket affinity (Bible Ch.8.4)
e924f1a  Market-basket affinity algorithm (DHARAM link layer)
7d3d357  Remove Live POS artifact; mock POS = transactional sales-only
41dddc8 / 495b7cb / 460338c  (Live POS panel — superseded/removed)
9a32513  Mock POS injector (--mode pos-inject)
3a0c4b7  Ordering-risk unification (GNN gated, inventory-only default)
28d4441 / bc24e20  Supplier & ordering-calendar client intake
4da039c / 35f787f  RXL port: view literals/WHERE + schema profile
1ace323  Zero-ADS FulfillmentDecider: intentional + safe
980b632 / a3e7311 / 8192cfb  CI green / audit-log / native store-graph
9150300 / 173130c / 30119dc / d37f757 / 2efd50a / b20a5c5 / 0593f21  onboarding
bf2aeea  DB role separation (read-only POS vs OASIS store)
50d6c6a / 55bddbc  Risk redesign: ablation + calibration/de-circularized backtest
```

---
*Generated 2026-06-23. Companion docs at repo root: `OASIS_GNN_Methodology_Review.md`, `OASIS_Risk_Scoring_Methodology_Redesign.md`, `OASIS_Systems_Analysis_PostSHA.md`, `OASIS_Ship_Readiness_Assessment.md`, `OASIS_Intelligence_Ordering_Pipeline_Analysis.md`, `OASIS_RXL_Porting_Reconciliation.md`, `OASIS_Client_Onboarding_and_Data_Contract.md`, `OASIS_Universe_Initialization_Supplier_Calendar.md`.*
