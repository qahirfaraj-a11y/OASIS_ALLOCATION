# O.A.S.I.S. — Monetization Readiness Analysis (June 2026)

Fresh-eyes audit of the whole platform with one question: **what stands between this
codebase and recurring revenue?** Three lenses: (A) monetization blockers,
(B) built-but-unutilized revenue potential, (C) engineering weaknesses that become
commercial risks. Every claim below was verified against the code this session.

---

## A. Monetization blockers (P0 — nothing ships paid without these)

### A1. The license manager is dead code ⛔
`oasis/logic/license_manager.py` has `OfflineLicenseManager.verify_license()` —
**and nothing calls it**. `grep verify_license` across the codebase: zero callers.
Additional defects inside it:
- **Expiry is never checked** (the code's own comment admits it) — a key issued once
  is valid forever.
- **No-key path fail-opens** core modules (`ops/stgat/shadow/approval` run free in
  "evaluation mode") — i.e. the flagship consoles are un-gated by design today.
- Default key path is `/data/oasis_license.key` — a **Unix path on a Windows
  deployment**; it will never exist on a client machine.
- **No issuer exists** — there is no tool that generates/signs a license key, so
  even if enforcement worked, we couldn't sell one.

**Fix:** (1) an issuer CLI (`--mode issue-license`, salt-signed, tenant + modules +
expiry); (2) expiry check; (3) Windows-appropriate key path; (4) enforcement at
console startup (each launcher checks its module) with a clear "contact iLink"
lock screen; (5) decide the fail-closed set (premium modules refuse, core runs
14-day trial keyed to first-run date).

### A2. No usage metering or telemetry
Nothing counts stores/SKUs/orders processed, so per-store or per-volume pricing is
unenforceable, and we'd fly blind on client health (no heartbeat "is the install
alive/failing" signal for support). The audit log (`OASIS_AUDIT_LOG`) already
captures operator actions — extend that pattern into a small metering table +
optional phone-home summary.

### A3. No update/release channel for on-prem installs
Installs are direct-on-client-machines (the chosen model). There is CI and a
Dockerfile/compose, but **no versioned release artifact, no updater, no migration
run-on-upgrade story**. Monetization = ongoing relationship = shipping fixes.
Minimum: tagged release zips + a `--mode upgrade` (git-pull/unzip + `alembic
upgrade` + preflight) and a documented rollback.

### A4. Seeded credentials are a liability in a paid install
Every install seeds 5 known users with `oasis2026`. Fine for demos; in production
it's a breach waiting to be blamed on us. Add: force-password-change on first
login + a `--mode set-admin-password` for installers. (API auth is healthy — it
fail-closes, generates an ephemeral key when unset.)

### A5. The value story isn't exported
`journey_state.value_recovered` + the Executive ROI tab exist and are live-data
driven — but the ROI evidence **never leaves the app**. Recurring revenue needs a
recurring artifact: a **monthly Value Report** (capital recovered, stockouts
prevented vs baseline, dead stock cleared, order accuracy) as PDF/XLSX the owner
can forward to their CFO. This is the retention engine; without it, renewals are
a vibe.

---

## B. Unutilized potential (P1 — revenue features already 80% built)

### B1. The Halo pricing engine — the biggest untapped monetizable asset 💰
We built directional anchor→attachment basket affinity (lift-gated, Bible-Ch.8
correct), and today its only consumer is DHARAM's ghost-demand patches — a *cost*
feature. But Bible Ch. 8.3 defines the *revenue* feature we never surfaced: the
**Halo Pricing Matrix** — "never discount an Attachment, never take a high margin
on an Anchor," with concrete per-pair pricing/placement recommendations. The data
already exists (`basket_affinity.csv`: anchor, attachment, confidence, lift).
A "Basket Intelligence" tab that says *"Milk anchors Bread (lift 4.0): protect
milk price, raise bread margin ~X%"* is a *makes-the-client-money* feature — the
easiest premium tier to sell. **Effort: small (the math is done; it's a view).**

### B2. Day-0 Demand Assessment as a paid pilot product
`real_demand.py` turns raw monthly sales exports into a normalized demand baseline
in ~15s, with honest coverage reporting. Package `build-pos-db + seed-real-demand +
stock review + dead-stock/basket reports` as a **fixed-price pre-sales assessment**
("give us your exports, get your store's X-ray in 48h"). It runs before any live
POS connection — zero integration risk, natural upsell into the subscription, and
we already proved it on Rhapta's own data.

### B3. Supplier scorecard from the validated risk signal
The **supply temporal backtest (0.757) is the one genuinely validated signal** in
the risk stack — supplier reliability/lead-time risk. Package it as a quarterly
**Supplier Scorecard** (reliability, fill-rate, risk trend per vendor) — retail
buyers pay for negotiation ammunition. The fresh/daily supplier calendar intake is
already productized; this completes the procurement story.

### B4. The REST bridge is an unsold integrations tier
`oasis/api/bridge.py` is a real, authenticated API (orders review/approve, sales
ingest, alerts, ERP sync/push-PO) with /health + /metrics. That's an
**Integrations tier** (webhooks/ERP write-back) at a higher price point — it needs
packaging and docs, not code.

### B5. The autonomous cycle is wired but not sold
`daily_pipeline` / `heartbeat` / `file_watcher` are integrated in `entrypoint.py`
(engine mode) — the Bible Ch. 12 "Autonomous Cycle" — but the product is currently
sold/demoed as consoles a human clicks. The premium story is inversion:
**OASIS runs the cycle; the human just approves POs** (approval dashboard exists).
Positioning + a supervised "autopilot" mode = the top tier.

### B6. Known unwired engine outputs (from prior analyses, still open)
- **F3:** LATA's `lata_variance_multiplier` never threads into the replenishment
  safety buffer (volatility signal computed, then ignored).
- **F4:** live ROP/coverage source vs the fallback heuristic — undecided.
- **DHARAM ghost demand** feeds order_engine but is not shown as "recovered
  revenue" in the ROI narrative — free value-story ammunition.

---

## C. Engineering weaknesses → commercial risks (P2)

| # | Weakness | Commercial consequence | Fix size |
|---|---|---|---|
| C1 | ADS divides by fixed 30/60/90-day windows; a client with <30 days of history gets silently understated demand (~25× at 2 days) | Wrong first orders at a new client = instant credibility loss | Small: divide by observed window (guard already designed) |
| C2 | 16% of Rhapta volume unmatched (assortment gap: Dairy Best/Maziwa Bora etc. absent from stock extract) | Under-ordering exactly the fastest movers | Onboarding contract: require Itm-Code↔barcode crosswalk; optionally auto-create missing SKUs at stock 0 so demand is captured and flagged |
| C3 | Live run validated single-store only (ORG001); transfer/allocation engines' value story needs multi-store | Can't demo the network features live | Medium: multi-org snapshot + streams |
| C4 | GNN honestly gated (inventory-only) — correct, but marketing may promise "AI" | Overselling → churn; undersell → weaker pitch | Positioning: sell the *statistical* engine + validation gate as a feature ("we don't ship unvalidated AI") |
| C5 | 13 xfail-quarantined tests + WIP "BUG N FIX" series in tree | Latent regressions land at a paying client | Schedule a quarantine-burn-down before first sale |
| C6 | No backup/restore for the client's OASIS store DB | Data-loss incident at a client = contract-ending | Small: `--mode backup/restore` + retention |
| C7 | Consoles repaint on refresh/TTL only (by design) | Demo optics; live-run wow-factor | Optional plain timed rerun in LIVE_MODE (already offered) |

---

## D. Recommended monetization model & sequence

**Model (fits the direct-install, on-prem reality):**
- **Core** (per store/month): Ops Console, Stock Review, Velocity Alerts, Live POS.
- **Ordering+** : Smart Ordering, supplier calendars, Supplier Scorecard.
- **Intelligence** : Basket/Halo pricing, Exec ROI, Value Report.
- **Integrations** : REST bridge, ERP write-back, API keys.
- **Entry funnel:** paid **Day-0 Demand Assessment** (B2) → subscription.

**Sequence (each step is small and unblocks selling):**
1. **License enforcement + issuer** (A1) — the literal paywall. ~1 day.
2. **Halo Pricing tab** (B1) — the premium-tier headline. ~1 day.
3. **Monthly Value Report export** (A5) — the renewal engine. ~1 day.
4. **ADS observed-window guard** (C1) + **first-login password change** (A4) — first-client safety. hours.
5. **Metering table + heartbeat summary** (A2), **release/upgrade path** (A3), **backup** (C6).
6. Then: multi-store demo (C3), autopilot positioning (B5), F3/F4.

**Honest bottom line:** the *analytical* product is further along than the
*commercial* wrapper. Nothing above requires new research — the highest-value work
(license gate, Halo pricing surface, value report) is packaging intelligence the
system already computes.

---
*Generated 2026-06-23. Verified against: license_manager.py (no callers),
api/security.py (fail-closed), api/bridge.py (12 authed endpoints), TENANT_ID
(schema-only, never filtered — single-tenant reality), entrypoint engine mode
(pipeline/heartbeat/watcher wired), basket_affinity consumers (DHARAM only).*
