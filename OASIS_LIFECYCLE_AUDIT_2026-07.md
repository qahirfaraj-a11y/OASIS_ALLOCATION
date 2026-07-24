# OASIS — Whole-System Lifecycle Audit

**Initial contact → full reliance: shortfalls, inefficiencies, logic traps, UX debt**
July 2026. Grounded in code inspection + everything live-verified this cycle.
Companion to `OASIS_Handover_Online_Expansion_2026-07.md` and `SUPPLIER_PORTAL_PLAN.md`.

Method: walk the journey a real store (and a real supplier) takes, stage by
stage, and grade what actually happens — not what the architecture intends.
Every finding cites where it lives. Severity: **C**ritical / **H**igh /
**M**edium / **L**ow.

---

## 0. The journey map, graded

| Stage | Experience today | Grade |
|---|---|---|
| A. Discovery / first contact | No public site; placeholder domain in listings; Odoo listing ready but unsubmitted | **C** |
| B. Install | Interactive `install.bat` is good — but 3 installers and 39 launchers sit beside it | **B−** |
| C. Day 0 (onboarding) | The wizard (sample/empty/connect) is genuinely strong | **A−** |
| D. Trial (day 1–14) | Full access, but the clock starts before real data and ends in a cliff | **C+** |
| E. Conversion (day 15) | Hard lock + manual key-file surgery at the exact moment they'd pay | **C** |
| F. Daily operation | Consoles work and look right, but are fragmented and die on reboot | **B−** |
| G. Growth (multi-store, Odoo, portal) | Odoo funnel proven end-to-end; two structural gaps remain | **B** |
| H. Full reliance | Nothing survives a restart; no alerting; retailer runs the hub by curl | **D** |

The pattern: **the beginnings are now polished (install, Day 0) and the deep
capabilities are real, but the *connective tissue of ongoing life* — staying up,
being administered, converting, being trusted — is where the product still
behaves like a project.**

---

## A. Discovery & first contact — C

**What exists.** OASIS SYSTEMS v1.0 brand identity, a whitelabel config, a
tight 0.5 MB client zip, an Apps-store-ready Odoo listing.

**Findings.**
- **A1 (H).** `website` in the Odoo manifest and listing assets is a
  placeholder (`oasis-systems.example`). Until a real domain + one landing page
  exists, every channel (Odoo listing, install banner "Contact iLink", portal
  footer) dead-ends. This is the cheapest unfixed hole in the funnel.
- **A2 (M).** The Odoo submission (our biggest discovery channel) is packaged
  but not submitted — human steps in `connectors/odoo/SUBMISSION.md` pending.
- **A3 (L).** "Contact iLink" appears as the support path everywhere; no email
  address or form anywhere in the product.

**Fix.** One domain, one landing page, one support email; submit the Odoo
listing. Nothing else in stage A matters yet.

---

## B. Install — B−

**What exists.** `install.bat` asks 3 questions with defaults and does the rest;
cold-start proven on the 145-file zip.

**Findings.**
- **B1 (M) — three installers.** `install.bat`, `install_oasis.bat`,
  `install_embedded.bat` all sit at root. Two are legacy. A buyer running the
  wrong one gets a different (worse) experience. *Evidence: root listing.*
- **B2 (M) — 39 `run_*.bat` launchers.** The "191 scripts" problem survives in
  launcher form (dev tree; the client zip whitelists 8). `run_oasis.bat` vs
  `run_oasis_live.bat` vs `run_app.bat` vs `run_app_online.bat` is a quiz, not
  a product. *Evidence: root listing.*
- **B3 (M) — the multi-store branch bypasses onboarding.** `install.bat`'s
  `multi` path still eager-builds the demo topology (`--mode init --profile
  multi`), while single-store correctly defers to the wizard. Multi installs
  therefore get mock data without ever choosing it — the exact sin Day-0
  onboarding was built to end. *Evidence: install.bat:75-77.*

**Fix.** Delete/archive the two legacy installers; collapse launchers into one
`OASIS.bat` menu (client zip already close); give `multi` a wizard path
(topology question in the wizard, not a silent build).

---

## C. Day 0 — A−

**What exists.** First-run wizard (sample / start fresh / connect a POS),
persistent SAMPLE badge, `verify_pos_connection` with an honest
"needs a mapping profile" message. Live-verified; render crash fixed.

**Findings.**
- **C1 (H) — the SAMPLE badge only exists on Home.** `demo_badge` is imported
  by `oasis/ui/home.py` and nothing else. Open Operations, Intelligence, or
  Command Center directly and the sample store masquerades as real data — the
  precise failure the badge was written to prevent, present in the three
  surfaces where the user actually works. *Evidence: grep `demo_badge` → 2
  files (onboarding.py defines, home.py uses).*
- **C2 (M) — three overlapping state files.**
  `.oasis_install_profile.json`, `.oasis_install_state.json`,
  `.oasis_onboarding.json` each hold a piece of "what is this install"
  (topology / trial stamp / data source). Three sources of truth; the wizard
  doesn't read the profile, so a multi-profile install still sees the
  single-store-shaped wizard (interacts with B3). *Evidence: `ls oasis/data`.*
- **C3 (L) — "connect" path records the URL but doesn't set it.** After a
  successful test the user is told to set `OASIS_POS_DB_URL` themselves —
  a manual env-var step in an otherwise zero-touch flow.

**Fix.** Call `demo_badge` in every console header (one line each — highest
value-per-line change in this audit); merge install state into one module; have
apply_connect write the URL into client config so consoles pick it up without
env surgery.

---

## D. Trial — C+

**What exists.** 14-day trial unlocks everything; tamper-resistant dual-anchor
stamp; day-count banner.

**Findings.**
- **D1 (H) — the clock starts before their data does.** The trial stamp is
  written at first run. A cautious owner who explores the sample store for a
  week has burned half the evaluation before their own catalogue is in. The
  metric that converts ("OASIS found N thousand shillings in my stock") never
  gets a fair window. *Evidence: `_first_run()` stamps on first launch;
  onboarding does not touch it.*
- **D2 (M) — a countdown, not a story.** The only trial communication is a
  banner counting down. No day-3 "here's what OASIS found so far", no day-12
  "here is your Value Report — this is what locks on Friday". The Value Report
  exporter exists and is not wired into the trial arc.
- **D3 (L) — trial HMAC key ships in source.** `_TRIAL_HMAC_KEY` is a literal
  in `license_manager.py`; the stamp is obfuscation, not security. Accepted
  risk — but it should be a documented one.

**Fix.** Start (or reset) the trial clock at *real-data onboarding* — sample
exploration is free; the countdown begins when their store connects. Wire the
Value Report into day-10/13 messaging. Document D3 as accepted.

---

## E. Conversion — C

**What exists.** Feature-level gating with upsell stubs, bundles, price sheet,
online issuer on the hub.

**Findings.**
- **E1 (H) — day 15 locks users away from their own data.** `console_gate` →
  `st.stop()` on every console. A store that chose "Start fresh" and built its
  catalogue inside OASIS is locked out of *its own records* with only "contact
  iLink" on screen. That converts anger, not licenses. *Evidence:
  license_manager.py console_gate locked branch.*
- **E2 (H) — activation is file-system surgery.** The purchase moment requires
  placing `oasis_license.key` beside install.bat. No upload/paste in the UI,
  no "license accepted ✓" feedback loop. The hub can *issue* online but the
  client can't *receive* online.
- **E3 (M) — the cliff has no ramp.** Nothing escalates before day 15 (see
  D2); the lock screen doesn't show the price sheet or the Value Report it
  computed during trial — the two things that would sell.

**Fix.** Lock to **read-only + export**, never no-access; add "Activate
license" (paste/upload/fetch-from-hub) to Home; put the tenant's own Value
Report and the price sheet on the lock screen.

---

## F. Daily operation — B−

**What exists.** Five consoles, spec-sheet brand identity, universal deep-dive,
gated modules, backup/restore modes.

**Findings.**
- **F1 (H) — five apps, five ports, five logins.** Each console authenticates
  independently (`oasis/ui/auth.py` per app; no shared session). An operator
  moving Ops → Intelligence → Command logs in three times. Ports (8500/8501/
  8510…) leak into the Home cards and the user's mental model.
- **F2 (M) — port collisions are configured in.** `DEFAULT_PORTS`: allocation
  **8502** and stgat **8502**; integrated 8503 vs compose's approval usage —
  start both allocation and stgat and the second silently fails. *Evidence:
  entrypoint.py DEFAULT_PORTS.*
- **F3 (M) — offline consoles are homework.** Home shows "○ offline · start
  with run_market_intelligence_tool.bat" — the launcher tells the user to go
  run a batch file instead of offering a Start button (`subprocess` is one
  call away; entrypoint already knows every command).
- **F4 (M) — Streamlit chrome leaks.** "Deploy" button, hamburger menu,
  "Streamlit" tab titles during load, and in dev the sidebar exposes internal
  page names (`1_Phase_1_Pitch_Audit.py` → "Phase 1 Pitch Audit"). Client zip
  excludes `pages/`, but every demo you give from the dev tree shows internal
  scaffolding. *Evidence: pages/ listing; observed in every live session.*
- **F5 (L) — heavy reruns.** Streamlit's rerun-everything model + live DB reads
  per interaction; fine at demo scale, worth st.cache_data passes before a
  1,000-SKU fleet complains.

**Fix.** Shared session token across consoles (localhost cookie or signed
query-param handoff from Home); unique ports; Start buttons on Home cards;
`.streamlit/config.toml` to strip chrome; rename internal pages.

---

## G. Growth — B

**What exists.** Multi-store modes, Odoo connector (Apps-ready), OASIS-inside-
Odoo embeds, supplier portal P0–P4 complete, insight rail with the Flex.

**Findings.**
- **G1 (C) — the Odoo data illusion.** OASIS-inside-Odoo embeds the consoles in
  Odoo's UI, but the consoles analyze the *OASIS store DB*, not the customer's
  Odoo data — the canonical-schema adapter for Odoo's Postgres was explicitly
  deferred. An Odoo customer clicking "Intelligence" inside their ERP sees a
  store that isn't theirs, with no label saying so. This is the single most
  misleading surface in the product today.
- **G2 (C) — the retailer administers the hub by curl.** Consent, ownership
  rules, exposure toggles (the entire Negotiation Flex), the offer review
  queue, tiers, ingest tokens: REST-only. We built the weapon and gave the
  retailer no trigger. Until an admin UI exists, every Flex reveal and every
  offer response is a support call to you.
- **G3 (H) — insights don't reach absent suppliers.** The portal's pitch is
  "a reason to log in every morning", but a stockout alert only exists *inside*
  the portal. A supplier who doesn't log in never learns their SKU is 0.9 days
  from dry. No email/webhook digest exists.
- **G4 (M) — portal lifecycle gaps.** No supplier password reset; 8h session
  hard-expires into a silent sign-out (unsaved offer form lost); no offer
  withdraw route (status exists, endpoint doesn't); no counter-offer; the
  single-page layout now stacks seven panels (plan said tabs — due at next
  growth).
- **G5 (M) — hub scale ceilings.** `visible_movements` pulls ≤200k rows into
  Python per overview call; `supplier_store_summary` re-fetches 100k; store
  handle resolution is N+1. Correct, tested — and single-digit-fleet only.
  Needs SQL-side aggregation before a real fleet.
- **G6 (M) — insight push isn't in the loop.** `--mode push-insights` exists
  but no scheduler invokes it (the Odoo cron ships movements only). The
  "self-driving" insight rail is actually hand-cranked until it joins the
  engine cycle / a scheduled task.

**Fix (order matters).** G2 first (retailer admin page — Command Center tab or
hub-served admin app), then G3 (SMTP digest), then G1 (Odoo Postgres source
profile — the real engineering; until then, badge embedded consoles "showing
OASIS store data, not Odoo" honestly), then G4–G6.

---

## H. Full reliance — D

The stage the whole methodology aims at ("the autonomous cycle", Ch. 12) and
the weakest today.

- **H1 (C) — nothing survives a reboot.** Every console, the hub, and the
  compose stack are foreground processes started from terminals/bat files. No
  Windows service, no scheduled-task autostart, no watchdog, no auto-restart.
  Observed all cycle: every session began with everything down. A store that
  *relies* on OASIS loses it at every power cut until someone double-clicks
  batch files in the right order. Full reliance is impossible in this state —
  this is the #1 gap in the entire system.
- **H2 (H) — silence when it breaks.** No heartbeat, no alerting. The engine
  has a heartbeat *log*; nobody is notified when sync stops, a console dies, or
  the hub goes unreachable. The first detector of failure is the user's own
  confusion.
- **H3 (M) — backups are a mode, not a habit.** `--mode backup` works and is
  manual. No schedule, no retention, no restore drill.
- **H4 (M) — hub is demo-hardened, not fleet-hardened.** Single global admin
  key (no per-tenant identities), SQLite default, `create_all` (no
  migrations), no rate limiting, no login lockout on the portal, token
  revocation is a DB edit. All documented in the plan — restating here as a
  **gate before the first paying fleet**, not a someday list.

**Fix.** One supervised "OASIS service" (Task Scheduler/NSSM wrapping
`--mode full` + hub) with restart-on-failure and boot autostart; a watchdog
that emails/webhooks on missed heartbeat; scheduled backup with retention. This
wave alone moves stage H from D to B+.

---

## I. Cross-cutting patterns

1. **We polish what we demo and defer what runs at 3 a.m.** Install, wizard,
   portal — excellent. Persistence, alerting, admin — missing. Invert one
   build-wave to fix the imbalance.
2. **Every capability that lacks a UI becomes a support call to you.**
   Licensing (file placement), the Flex (curl), offers (curl), tokens (curl).
   The product's operating cost currently includes *you* as its UI.
3. **State fragmentation repeats.** Three install-state files; five console
   sessions; three installers; 39 launchers. Each was locally reasonable;
   together they are the fog a new user gets lost in.
4. **Honesty guards must travel with the data.** The SAMPLE badge (Home-only)
   and the Odoo embed (unlabeled foreign data) both break the rule the ST-GAT
   work established: *never let a surface imply data it doesn't have.*

---

## J. Prioritized roadmap

**Wave 1 — days, mostly one-liners, disproportionate trust gains**
1. `demo_badge` in every console header (C1).
2. Trial clock starts at real-data onboarding (D1).
3. Day-15 lock → read-only + export, price sheet + own Value Report on the lock
   screen (E1, E3).
4. "Activate license" upload/paste in Home (E2).
5. Unique DEFAULT_PORTS; Start buttons on Home cards (F2, F3).
6. One `OASIS.bat` menu; archive legacy installers/launchers (B1, B2).
7. Badge the in-Odoo consoles "OASIS store data" until the adapter lands (G1 interim).

**Wave 2 — the reliance wave (~a week)**
8. Supervised OASIS service + boot autostart + restart-on-failure + watchdog
   alerting (H1, H2). *The single most important item in this document.*
9. Retailer admin UI: consent, exposure toggles, offer queue, tokens, tiers (G2).
10. Supplier email digest: stockout risk + newly-shared insights (G3).
11. Shared console session (F1); scheduled backups (H3).
12. Multi-store wizard path; unify install state (B3, C2).

**Wave 3 — pre-fleet gate**
13. Hub hardening: per-tenant admin, Postgres, Alembic, rate limits, lockout,
    HTTPS runbook (H4).
14. Odoo Postgres source profile so embedded consoles analyze the customer's
    real data (G1 proper).
15. Portal tabs IA + session refresh + offer withdraw/counter (G4); SQL-side
    aggregation (G5); insight push into the scheduled cycle (G6).
16. Public site + Odoo listing submission (A1, A2).

---

## K. Where this is heading — the "retail NASDAQ" ledger

The ambition to run a NASDAQ-like ledger for retail products is not a new
system — **`hub_stock_movement` already is the ledger**: append-only in
practice, idempotent on `(store, source_ref)`, per-SKU, timestamped, with
volume and price, fed live by real ERPs, privacy-gated by consent. What
separates it from a market board is presentation and guarantees, not data:

- **Immutability as a rule, not a habit** — forbid UPDATE/DELETE on the
  movement table (trigger/permission), add a hash-chain column if audit-grade
  is wanted later.
- **Ticker aggregation API** — per-SKU OHLC-style bars (volume, velocity,
  price) per day/week; the portal's Overview already computes the primitives.
- **Indices** — category and department composites across consenting stores
  (anonymized), the way the doc's archetypes and habitats already slice the
  network.
- **The board** — a public/anonymous read surface ("Beverages index up 4% this
  week in Nairobi") is the marketing engine; named data stays consent-gated
  exactly as today.

Recommend treating this as **P5 of the portal plan** — a productization of the
ledger we already ship, sequenced *after* Wave 2 above (a market board on
infrastructure that dies at reboot would be theater).

---

## Bottom line

OASIS's capabilities are ahead of its connective tissue. The install and Day-0
story is genuinely excellent; the intelligence is real and tested; the
two-sided exchange exists end to end. What stands between "impressive demo"
and "full reliance" is unglamorous: processes that stay up, alerts when they
don't, an admin UI for the person holding the Flex, a trial that starts when
the user's data does, and a lock screen that sells instead of punishes. Wave 1
is days of work; Wave 2 is the product's adulthood.
