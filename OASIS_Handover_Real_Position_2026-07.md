# O.A.S.I.S. — Real Position Handover · July 2026

**Purpose:** the unvarnished state of the platform before the online expansion
(license server / ERP connectors / portal) begins. Written for anyone — including
a future us — who needs to know exactly what is real, what is simulated, what is
proven, and what is still owed. **142 commits** this effort · **v2.3.0** ·
full suite **681 passed, 0 failed, 0 skipped, 0 quarantined**.

---

## 1. Executive position

OASIS today is a **sellable, license-gated, on-premise retail intelligence
suite** with a 35 MB installer, five module SKUs, and client-facing analytics
proven on real Chandarana Rhapta data. The *analytical* core is genuinely strong
and evidence-backed; the *commercial* wrapper (licensing, packaging, reports,
pricing collateral) now exists and is smoke-proven end to end. What does **not**
yet exist is any online machinery (no license server, no payments, no portal, no
cloud-ERP connectors) and any **production deployment** — no client install has
ever run outside this development machine. We are at "ready to sell the first
pilot," not "operating a product."

---

## 2. Maturity matrix — every subsystem, graded honestly

Grades: **A = proven on real data live** · **B = tested + smoke-proven locally**
· **C = works, demo-grade inputs** · **G = built but gated off pending validation**
· **✗ = does not exist**

| Subsystem | Grade | Evidence / caveat |
|---|---|---|
| Canonical schema + adapter (`UniversalConnector`/`SchemaMapper`/views) | **B** | RXL profile built from real ERP docs; never connected to a live MSSQL |
| Replenishment engine (ROP→target→net-req, AMIT/MANDE/HALO gates) | **B** | golden tests; LATA variance now in the safety buffer (F3) |
| Newsvendor ROP | **G** | built + tested; `OASIS_ROP_MODE=heuristic` default until A/B'd on live sim |
| Store-GNN risk | **G** | proven inert to live signal; `OASIS_GNN_ORDERING_WEIGHT=0`; beat-baseline gate stands |
| Supply-risk signal (temporal backtest 0.757) | **A** | the one outcome-validated model; powers the Supplier Scorecard |
| Basket affinity (directional, lift-gated) | **B/C** | algorithm matches the in-house doctrine (Bible Ch. 8); recovery validated on *planted* structure only — **no real co-purchase data has ever been mined** |
| Halo Pricing view | **C** | correct math; awaits real basket data to be trustworthy |
| Category / SKU deep-dive / Day-0 / Scorecard / Value reports | **A** | run live on real catalog + real sales + real GRN costs (KES 3.4M recoverable found in alcohol) |
| Real GRN cost injection (WAC) | **A** | 141,803 lines → 17,852 SKUs (44.9% of catalog) |
| Demand baseline (ADS) | **A/C split** | *reports* use real monthly sales; the *live-run DB* (`rhapta_pos.db`) carries a seeded reconstruction so ordering demos are normalized — see §3 |
| POS simulators (single + 5-store multi) | **B** | stock-integrity tested (no oversell); 5 concurrent tills verified |
| Licensing (5 SKUs, bundles, trial, feature gates, upsell stubs) | **B** | starter/enterprise walls smoke-proven; trial stamp is a deletable file (§6) |
| Release packaging + install.bat | **B/✗** | 35.3 MB zip, hygiene-audited (0 leaks) — **never installed on a clean machine** |
| Upgrade path (backup→stamp→migrate→preflight) | **B** | proven on a DB copy; migrations chain repaired (alembic was shadowed AND uninstalled) |
| REST bridge (12 authed endpoints; `/erp/sync` fixed) | **B** | fail-closed auth; api-module middleware; never load-tested |
| Consoles ×4 + Home launcher | **B** | live-run verified; repaint on refresh/TTL by design |
| Online: license server, portal, payments, Zoho/Sage/Odoo connectors | **✗** | blueprint agreed, nothing built |

---

## 3. Data truth table — what is real vs simulated

This distinction is the difference between honest selling and overselling.

**REAL (client-grade evidence):**
- Catalog snapshot: **39,728 SKUs**, 247 departments, 823 vendors, real prices + on-hand.
- Monthly sales: 10 files ≈ **9 complete months** (~3.1M units; July export truncated → auto-excluded from rates).
- GRN history: **141,803 receipt lines** → real weighted-average costs (real margins: beer 26–31%, spirits ~32%).
- Alcohol section snapshots (6–7 Jul 2026): 2,356 SKUs → the SKU deep-dive verdicts.
- Supplier delivery patterns (599 suppliers) → LATA multipliers, scorecard.

**SIMULATED (wiring demos — never present as client evidence):**
- All POS *streams* (single + 5-store) and the **seeded sales history inside
  `rhapta_pos.db` / `rhapta_multi_store.db`** — bills are synthetic even where
  the per-SKU ADS was reconstructed from real monthly totals.
- Basket co-purchase structure (planted from a *supply-side* vault prior; 97%
  recovery proves the pipeline, not customer behaviour).
- The 5 store personalities (real catalog, invented archetypes).

**Standing rule preserved:** GNN and newsvendor ROP stay gated until they beat
baselines on **real bill-level data** — which we still do not have (the ask to
the client remains: daily/bill-level POS export + Itm-Code↔barcode crosswalk;
that also closes the 16% demand-volume match gap).

---

## 4. Commercial state — how a sale works today

**Working now (manual fulfilment):**
1. `--mode issue-license --tenant X --bundle pro --expiry …` (vendor machine, salt set)
2. hand over `dist/OASIS_v2.3.0.zip` → client runs `install.bat`
3. key file dropped in → modules unlock; locked ones show activation notices
4. monthly `--mode value-report` = the renewal artifact; per-module usage included

**SKUs:** core (mandatory) · ordering · network · revenue · api.
**Bundles:** starter / pro / enterprise. **Trial:** 14 days, everything unlocked.
**Pre-sales funnel:** `--mode assess` (Day-0 X-ray from raw exports, vendor-run, ungated).
**Price sheet:** exists (`OASIS_Module_Price_Sheet.md` + PDF) — **prices are blank; only you can fill them.**

**Not yet possible:** online purchase, self-serve activation, subscriptions/renewal
automation, hosted tier, any cloud-ERP intake. (Agreed sequence: license server +
`--mode activate` → Odoo connector → portal → payments → hosted single-tenant
containers. Nothing started.)

---

## 5. Verification ledger

**Proven live in this effort:** license walls (starter vs enterprise); upgrade
chain on a real-size DB copy; backup (36 MB, WAL-safe) + restore round-trip;
all five client reports on real data; GRN injection; 5-store concurrent
streaming with stock integrity; adapter reads of all 5 orgs; release-zip hygiene
audit; PDF rendering.

**Never tested — treat as unverified until done:**
- `install.bat` cold-start on a clean machine/VM (the single most important gap before handing a zip to a stranger).
- Live connection to a real client ERP (MSSQL/RXL) — the views/profile are built from documentation, not a live socket.
- Multi-user concurrent console usage; long-running (days) stability; Streamlit exposed beyond localhost.
- Restore/upgrade against a *client's* (non-Rhapta-shaped) database.
- The API bridge under any real load.

---

## 6. Risks & debts, ranked

1. **`OASIS_LICENSE_SALT` is the business.** Anyone holding it mints licenses.
   No management story yet (should live only on the future license server).
2. **Trial stamp is a deletable JSON file** — trivially reset. Acceptable for
   supervised pilots; harden (registry/DB copy) before unsupervised distribution.
3. **Repo hygiene: 651 dirty entries** in the working tree — 425 untracked
   (sales collateral, docx/zips, old releases), 150 uncommitted deletions
   (root `analyze_*` scripts whose copies live in `scripts/archive/` — the move
   was half-committed), 76 modified incl. **51 .py of engine WIP** (the "BUG N
   FIX" series: models/, exchange/, analytics/…) that is **not in any commit and
   not in the release zip**. Decide: commit, stash, or discard — silent drift is
   the risk.
4. **Single-tenant reality** (`TENANT_ID` cosmetic). Fine on-prem; the hosted
   tier must be per-client containers, not a shared DB.
5. **Basket/Halo layer awaits real co-purchase data** — do not sell Halo Pricing
   numbers as findings yet; sell the *capability* with the demo caveat.
6. **fpdf reports are latin-1** (emoji/₹-class glyphs replaced); fine for now.
7. **Consoles repaint on refresh/TTL** — a deliberate choice; demo scripts
   should include the click.

---

## 7. Asset inventory

- **46 entrypoint modes** (onboarding: preflight/build-views/bootstrap-* · data:
  build-pos-db/seed-real-demand/inject-grn-costs · sim: pos-stream/multi-pos-stream
  · commerce: issue-license/license-status/package-release/upgrade/backup/restore/
  set-password · reports: assess/category-report/sku-deepdive/supplier-scorecard/
  value-report/metering-report · graphs/baskets: build-graph/build-store-graph/
  build-baskets/build-prior).
- **Launchers:** `run_oasis_home` (suite front door, :8490) + 3 consoles (+ `_live`
  variants on `rhapta_pos.db`) + `run_command_center_multi` + `run_mock_pos` /
  `run_multi_pos` + market-intel.
- **Artifacts:** `dist/OASIS_v2.3.0.zip` (35.3 MB) · `reports/` (alcohol category
  + SKU deep-dive + Day-0 + scorecard + value report + price sheet, MD/CSV/PDF)
  · Desktop\OASIS_Reports copies.
- **Docs:** 21 committed `OASIS_*.md` analyses at repo root (GNN review, risk
  redesign, ship-readiness, RXL port, onboarding contract, monetization,
  detailed-changes, prior handovers, this document).
- **Key env gates:** `OASIS_GNN_ORDERING_WEIGHT=0` · `OASIS_ROP_MODE=heuristic`
  · `OASIS_LIVE_MODE` · `OASIS_TRIAL_DAYS=14` · `OASIS_SEED_PASSWORD` ·
  `OASIS_DB_PATH` per launcher.
- Login (demo installs): `ops_admin` etc. / `oasis2026`.

---

## 8. Bottom line

The platform is **honestly demo-ready and pilot-sellable today** via manual
fulfilment: real catalog, real costs, real supplier history, defensible reports,
enforced module walls, and a clean installer. The three things standing between
this and a *distributed product* are (1) a cold-start install proof on a machine
that isn't this one, (2) the license server so sales don't route through a CLI
with the salt, and (3) one real client's bill-level data to graduate the
demand/basket layer from "wiring validated" to "outcomes validated." Everything
else is growth, not readiness.

*Generated 2026-07-08 · v2.3.0 · suite 681/0 · this file: repo root +
`reports/` PDF + `Desktop\OASIS_Reports`.*
