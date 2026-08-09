# O.A.S.I.S. — Handover

**Date:** 2026-08-09 · **Version:** 2.3.0 · **Branch:** `pre-mosaic-backup`
**Head:** `ce468142` · **Tests:** 1,099 passing · **Release:** 0.8 MB / 182 files

Everything below was verified against a **freshly built release zip, extracted
into a clean directory**, not against the working tree. Three times in this
period something worked in the tree and was broken in the artifact; that rule
is what caught all three, and it is the single most important habit to keep.

---

## 1. What this period delivered

The native Flet desktop app went from a **read-only viewer** to the operating
surface of the product. Command Center parity with the Streamlit console is
complete: all 11 console tabs are migrated, plus Site Selection which the
console never had (11 native tabs + Settings in its own view).

Along the way, **five defects were found that made the shipped product not work
on a client machine**. None were visible from a developer checkout.

### Commits

| Commit | Subject |
|---|---|
| `56c7c9de` | Phase 3: license the Command Center, and make it act |
| `b6cc33c6` | Desktop: fix the four defects behind the reported console errors |
| `3689550a` | sec: stop announcing seed passwords that were never stored |
| `7d5b577f` | Command Center parity: Live Sales, Stock Review, Transfer Intelligence |
| `5c2c60e1` | Command Center: the remaining six tabs, and the greenfield spine |
| `34dee3d5` | Sample data: de-identify the estate, stock it from the hot product set |
| `9d7ec8cd` | Housekeeping: import paths, container copy layout, connector fixes |
| `99532a5c` | Role gating, a greenfield SKU, the last of the whitewash, and ODbL |
| `e20e8c9b` | Whitewash: catch the own-brand lines, and hand over |
| `b25e4cb7` | Location pillar: interpretable site selection, no model, no client data |
| `ce468142` | Velocity floors in both front doors, ODbL notices, ignore machine state |

---

## 2. The Command Center (11 tabs)

Every tab passes **two independent gates**, and they fail differently on
purpose:

* **Module SKU** — "has this install bought the capability?" A locked tab shows
  an **upsell**, because it can be bought.
* **Role** — "may this person use it?" A forbidden tab is **not built at all**,
  because it cannot.

| Tab | Module | Notes |
|---|---|---|
| Executive ROI | core | Console's AMIT dead-stock rule verbatim |
| Live Sales | core | Real baskets; runs `AlertMonitor` itself |
| Transfers | network | Shared `scan_network_opportunities`; queue + status writes |
| Stock Review | core | **Deliberate** band divergence — see §5 |
| Ordering | ordering | Generate → push → approve, + 3 scenario levers |
| Processor | core | Same pipeline; native file picker |
| Site Selection | **greenfield** | Huff scoring; client's estate + their OSM fetch |
| Allocation | **greenfield** | Network-derived, no CSV |
| Simulation | greenfield | Store's own products |
| Analytics | core | Shares the weekly accessor with ROI |
| Suppliers | ordering | Client's own catalogue |

Role visibility: `ops_admin` 11 tabs · `executive` 6 · `branch_manager` 5 ·
`finance` 2.

**Guarded by 107 parity tests** (`tests/test_command_center_parity.py`) which
read `ops_dashboard.py`'s **source** and assert the native side agrees. The
console is never imported or modified — it stays the reference, and drift is
caught from either direction.

---

## 3. The five client-install defects

These are the reason to keep testing against the artifact.

1. **`oasis/llm` never shipped.** `ops_dashboard.py` imports `RuleBasedLLM` at
   *module* level, so the Streamlit Command Center — the reference architecture
   itself — **could not start on any client install**.
2. **`retail_simulator` never shipped.** `intraday_sim.py` imports it at module
   level; that script couldn't start either. Moved to `oasis/simulation/` with
   a root shim.
3. **Supplier Intelligence + Allocation** depended on a scorecard CSV that can
   never ship → `FileNotFoundError` forever. Both re-sourced.
4. **Store Intelligence** read a *simulator*, not the client's POS.
5. **Seed passwords announced but never stored** — `INSERT OR IGNORE` silently
   discarded them while the log told the operator to use them. This is what
   caused the lockout.

**Two permanent guards now exist** in `tests/test_release_zip.py`: every
`oasis.*` package **and** every sibling root module a shipped script imports
must be present in the zip.

---

## 4. Data protection — read this before shipping anything

### Never ships
`Full_Product_Allocation_Scorecard_*.csv` is **one retailer's book**: 23,511
rows of per-SKU revenue, margin, gross profit, GMROI and named supplier terms.
Competitors of that retailer are named in this product's own scenario
templates. It was previously excluded only by *accident* of default-deny; it is
now excluded **by rule**, with a test.

The Obsidian vault (`oasis_vault/`, 2.2 GB) is the same data in another form.

### Sample data is de-identified
The demo estate is a fictional chain — **Meridian Fresh**, branches Parkview /
Highgrove / Oakridge / Northgate Mall / Central Plaza. Store *archetypes* are
unchanged (flagship, upscale, family bulk, mall express, urban impulse); only
identity is invented, and it lives in one swappable module,
`oasis/logic/demo_identity.py`.

**Verified 2026-08-09:** a demo network built inside a clean extraction was
scanned across all 22 tables — **zero identifying tokens**.

### One thing the first pass missed
The early guards checked the catalogue's *fields* (no revenue/margin) and the
*store estate* (renamed) — and both passed while two own-brand product lines
still carried the retailer's name in the **product name**, which is the text a
client actually reads. The generator now filters them and a test checks the
words, not the schema.

**Lesson worth keeping: check the text a user sees, not the shape of the data.**

### Deliberate residue (9 files)
Each has a reason, and `test_the_customer_name_survives_only_where_it_must`
holds the allow-list:

* `demo_identity.py` — the guard's own vocabulary
* `amit_gatekeeper` / `dharam_revenue` / `graph_export` / `lata_shield` — a
  one-release back-compat alias (`rhapta_fill_rate` → `store_fill_rate`) for a
  field **we** write and read
* `onboarding.py` — `LEGACY_DB_NAMES`, so existing installs aren't orphaned
* `data_mixin.py`, `merge_additional_data.py` — **genuine client spreadsheet
  column headers**. Renaming these would stop OASIS reading the client's own
  files to hide a string no user ever sees. *Leave them.*
* `ops_dashboard.py` — the untouched reference

---

## 5. Decisions taken (don't silently reverse these)

**Stock health bands diverge from the console, deliberately.** The console
calls a line a stockout under half a day of cover; we say **STOCKOUT = nothing
on hand, CRITICAL = under one day**. A shelf with stock on it is not a
stockout — it's about to be one, and collapsing the two costs the operator the
distinction they act on. Overstock horizons (14d fresh / 30d ambient) *do*
match. A test pins the divergence so it can't be "corrected" without re-making
the call.

**The Hourly Revenue Pattern is not ported.** The POS schema has no time of day
— `BILL_DT` is a date, there is no `BILL_TIME` — so the console manufactures
the hour with `np.random.normal(14, 3)`. A real 14-day trend takes its place. A
test pins that `np.random` line: if the console ever gains a real clock, the
test fails and the chart becomes portable.

**Velocity alerts need a floor.** A line selling 0.02/day reads as a 50× spike
the one day it sells — true arithmetic, no information. 184 of 266 alerts on
the sample store were such lines. Both an ADS floor (≥1.0) and a unit floor
(≥3) must clear. **This defect is still live in the Streamlit console.**

**Greenfield is its own SKU.** Allocation used to ride `network`, so a chain
couldn't buy site planning without also buying inter-store transfers. Opening a
site is a different job from running one: different buyer, different cadence,
and no history exists yet by definition. New `expansion` bundle.

**Multi-store is the default shape** everywhere (resolved path, install
profile, wizard, menu). OASIS is a network product; on a single outlet,
Transfer Intelligence, Allocation and cluster analysis have nothing to say.

**OpenStreetMap is ODbL.** Redistributing an extract makes it a *Derivative
Database* and obliges ODbL on that database. A score computed **from** it is a
*Produced Work* and does not. So OASIS ships the scoring and the client fetches
their own region (`oasis/logic/geo_sources.py`) — which is also the better
product answer. `OSM_ATTRIBUTION` must appear wherever OSM-derived output is
shown. *This is an engineering summary of a licence, not legal advice — confirm
before a commercial release.*

---

## 6. Sample data

4,000 hot lines (velocity tiers A/B/C; the ~16,600 near-dead D-tier dropped),
175 departments, 318 suppliers, **112 KB gzipped**. Built by
`scripts/build_demo_catalog.py`, a **dev-only** script reading sources that
never ship.

It carries identity, department, supplier and shelf price (all publicly
observable) plus **synthesised** opening stock. **No revenue, margin, gross
profit or real per-SKU demand.**

`demo-multi` builds 5 stores in ~4s. `OASIS.bat → 9` is the menu.

Adding the richer catalogue exposed three defects the 34-SKU demo had been
hiding: the velocity-alert noise above, seeding that didn't scale (60 bills/day
was tuned for 34 lines and left 87% of 4,000 unsold), and a rounding boundary
that put "3.0 days" under a heading reading "under 3 days".

---

## 7. Test suite

**1,082 tests, ~13½ minutes** for the full run. Prefer running the file you're
touching; run everything once at the end.

Two `conftest.py` guards worth knowing about:

* **The real store is not a fixture.** `test_customer_flow` rendered the
  onboarding wizard with a bare `MagicMock` — whose `button()` is *truthy*, so
  every setup button read as clicked and the render actually executed
  `apply_multi_demo()` against the real project root. The suite deleted and
  rebuilt the developer's own store on every run, and passed the whole time. An
  autouse guard now fails any test that writes to the installed `oasis/data`
  (opt out with `@pytest.mark.real_store`).
* **Demo fixtures are capped.** `OASIS_DEMO_MAX_SKUS=150` and
  `OASIS_DEMO_HISTORY_DAYS=7` in tests; production keeps the full range.
  Without this the 4,000-SKU catalogue took the suite from ~5 to ~20 minutes.

---

## 8. Greenfield suite

| Pillar | Status |
|---|---|
| **Allocation** | **Done.** Network-derived via `oasis/logic/scorecard_builder.py` |
| **Simulation** | **Done.** Shipped; dev paths removed |
| **Location** | **Done.** `site_scoring` + `store_locations` + `geo_sources`, Site Selection tab |

`scorecard_builder` has two modes. In `network` mode demand is averaged over
the stores that **carry** a line — not summed, and not averaged over the whole
estate — because a new store behaves like an average store, and dividing by
outlets that never stocked a line would understate a regional product into
nonexistence. "Staple" is then **revealed by carriage** (≥4 of 5 outlets)
rather than asserted by a shipped column.

### Location pillar — built

`oasis/logic/site_scoring.py` — Huff gravity share, pure and interpretable, no
model. `store_locations.py` holds the client's estate (entered once per
install; `ORGANIZATION_MST` has an address but no coordinates and OASIS will
not geocode-and-guess). `geo_sources.py` fetches their region's competitors.
Site Selection is the 11th Command Center tab, gated on `greenfield`.

**A real bug was found porting the console's expansion engine.** It places the
demand point ON the candidate site (`{'S': ..., 'dist': 0.1}`), which hands the
site a 267x utility advantage by construction — an empty field scored 100% and
was recommended a "Hyper / Flagship". Demand is now sampled on rings *around*
the site, so competitor proximity actually bites. An isolated site reports
"Nothing within 10 km — no competition, but no evidence of demand either"
rather than implying an opportunity.

**What it deliberately cannot do:** OASIS has no population or footfall data,
so this ranks how CONTESTED a catchment is, not how big it is. That limitation
is in the module docstring, in the verdict text, and in the tab's own footnote.

### The audit that led there

* `competitor_network.csv` — 335 sites, 100% OpenStreetMap. Public, but ODbL
  (see §5). Fetched per-client, not shipped.
* `store_coords.json` — the customer's own estate. **Never ships.** A client
  install should generate theirs from their org master.
* `expansion_model.joblib` (13 MB) — **trained on `np.random`** against a
  hand-written target commented `# GROUND TRUTH LOGIC`. It leaks nothing but
  **predicts nothing**, and costs 20× the entire release. **Recommendation: do
  not ship it.** Extract the interpretable geographic scoring instead — Huff
  probability, competitor friction, cannibalisation distance, traffic friction
  — all already implemented in `network_simulation.py`. A real model becomes
  possible later from post-opening site performance, which is exactly what
  OASIS accumulates.
* `stores_network.json` (89 MB) — 110× the release. Built per-install.

---

## 9. Open items

**None outstanding.** The three that were open at the previous revision are
closed in `ce468142`:

* velocity-alert floors now live in `oasis/logic/alert_monitor.py` and are used
  by BOTH front doors (the console was still emitting the noise);
* the OpenStreetMap position is recorded in `THIRD_PARTY_NOTICES.md`, which
  ships with the release. **Three questions are marked there for legal
  sign-off** — attribution placement in exported work, Overpass fair-use at
  commercial volume, and what reattaches if a competitor set is ever bundled.
  Those are the only known blockers to a commercial release, and they are not
  engineering ones;
* machine state and client-owned data (`network_registry.json`,
  `store_locations.json`, `competitor_network.csv`) are gitignored with a test.

---

## 10. If you read one thing

**Build the zip, extract it clean, run it there.** The working tree is not the
product. Every serious defect in this period — a console that couldn't start, a
script that couldn't import, two tabs that could only ever throw, a password
that was never stored — was invisible from a developer checkout and obvious
from a fresh extraction.
