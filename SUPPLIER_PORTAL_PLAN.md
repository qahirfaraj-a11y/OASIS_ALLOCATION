# Supplier Portal — Improvement Plan

**Retail Central Intelligence, done to the OASIS methodology**
Grounded in *The Algorithmic Retailer 2.0* (Ch. 6 "Weaponizing the Shelf",
Ch. 11.4 MANDE, Appendices D/E/Q) and the existing OASIS engine.
Status: PLAN (nothing built yet). Author handoff, July 2026.

---

## 1. The gap, stated plainly

Today's supplier portal (`oasis_hub/portal_web/index.html` + `/portal/*`) shows a
supplier **one thing**: a passive feed of their own stock movements — a table of
sales rows, a per-store rollup, four KPI tiles (units sold, SKUs, stores,
movements). It answers *"what moved?"* and nothing else.

Meanwhile the methodology defines an entire **supplier-intelligence engine**, and
**we already built most of it** — it just never leaves the store:

| Doc concept | Where it already lives in code |
|---|---|
| MANDE — SEI, trapped capital, purge report | `oasis/logic/mande_triage.py` (`calculate_supplier_efficiency_index`) |
| Supplier reliability class (RELIABLE/WATCH/HOSTILE), lead-time | `oasis/logic/supplier_scorecard.py` (`scorecard_rows`, `classify`) |
| Halo / attachment (Confidence, Lift) | `oasis/logic/basket_affinity.py` |
| Halo pricing / basket value | `oasis/logic/halo_pricing.py` |
| Supplier intake / classification | `oasis/logic/supplier_intake.py` |

So the portal is not under-built because the intelligence is missing. It is
under-built because **the hub only receives the raw movement stream**
(`hub_stock_movement`), and the rich, methodology-grade metrics — which need
GRN, credit terms, cost prices, and basket data the hub never sees — are
computed on-prem and stay there.

**The fix is a pipe, not a new brain.**

---

## 2. The reframe: a two-sided intelligence exchange

The doc's supplier metrics are explicitly the **retailer's negotiation weapon**:

> "The expert operator does not ask for 'better terms.' They present the MANDE
> Network Triage data to the supplier representative … 'You are trapping 500,000
> KES of my capital while generating zero incremental revenue.'" — Ch. 11.4.3

That single sentence is the product thesis for the portal. The portal is the
**delivery surface for the Flex** — the place where the retailer's algorithmic
leverage meets the supplier. And the same surface is genuinely *useful to the
supplier* (sell-through, stockout risk, forecast), which is what makes suppliers
show up and pay.

That duality is the business model:

```
   Supplier VALUE (carrot)            Retailer LEVERAGE (stick)
   ───────────────────────           ─────────────────────────
   • live sell-through, velocity      • SEI / trapped-capital scorecard
   • stockout & lost-sales alerts     • NCP (float) position
   • halo / attachment lift           • Quality score Q_s (ghost/expiry)
   • demand forecast for their SKUs   • Cannibalization / substitution
   • their own quality scorecard      • the Purge Report / ultimatum
        │                                     │
        └──────────► OASIS sits in the middle ◄┘
              monetizes BOTH sides (§7)
```

The retailer decides, per supplier and per metric, which side of that line is
shown — a **controlled Flex**, not a data dump (§6).

---

## 3. Metric catalog — the portal's real content

Each row is a card/section the enriched portal can render. "Compute" says where
the number is produced; "Gate" says who may see it.

| Metric (doc ref) | Formula (doc) | Data needed | Compute | Portal surface | Gate |
|---|---|---|---|---|---|
| **Velocity / ADS** (Ch.10) | units ÷ observed days | movement stream | **Hub** (have it) | "Velocity" trend per SKU/store | supplier value |
| **Days of cover / stockout risk** (LATA, Ch.12) | on_hand ÷ ADS | movement + `on_hand` | **Hub** (have it) | red "will stock out in N days @ store" alert | supplier value |
| **GMROI / capital efficiency** (Ch.1.5) | margin × turns | + cost price | on-prem | "your capital efficiency vs category" | supplier value |
| **Halo — Confidence & Lift** (Ch.3.5) | P(attach\|anchor); lift | basket data | on-prem `basket_affinity` | "your anchors pull these attachments" | supplier value |
| **Broken-halo / ghost demand** (DHARAM 11.3.2) | detect_broken_halo() | basket + sales | on-prem | "attachment sales dropped — halo broke" | supplier value |
| **SEI** (11.4.2) | Revenue ÷ trapped capital | rev + velocity + price | on-prem `mande_triage` | "Supplier Efficiency Index: X KES/unit-capital" | **retailer-gated** |
| **NCP / float** (Ch.6.2) | Credit_Days − DIO | credit terms + DIO | on-prem | "your terms cost me N float-days" | **retailer-gated** |
| **Quality score Q_s** (11.4.2) | 1 − (damaged+short)÷ordered | GRN checkpoint | on-prem | "fill/quality: 0.xx — Consignment threshold 0.85" | **retailer-gated** |
| **Cannibalization Rate** (Ch.9) | stolen ÷ new-SKU volume | substitution graph | on-prem | "this line extension is 100% cannibalizing" | **retailer-gated** |
| **Value Density V_d** (App.E) | COGS ÷ volume | cost + volume | on-prem | hoarding/priority signal | retailer-gated |
| **Reliability class** (LATA) | multiplier → class | lead-time history | on-prem `supplier_scorecard` | RELIABLE / WATCH / HOSTILE badge | either side |
| **Reorder forecast** (Ch.12) | target stock − on_hand | forecast + ROP | on-prem | "ship X to store Y by date Z" | supplier value |

The left column is not a wishlist — every "on-prem" row maps to a module that
exists (§1). The build is exposure, not invention.

---

## 4. Architecture: the "Insight Push" channel

The hub deliberately holds only the thin movement stream (privacy — it should not
hold every store's cost prices, credit terms, and GRN). So methodology-grade
metrics are **computed on-prem and pushed as derived "insight cards"** to the
hub, exactly like movements are pushed today.

```
On-prem OASIS (per store)                 Cloud Hub                 Portal
─────────────────────────                 ─────────                 ──────
mande_triage / scorecard /   ── POST ──►  hub_supplier_insight  ──► insight
basket_affinity / halo_pricing  (insight   (typed cards, per      cards +
  → typed insight cards          token)     supplier, per store)   trend feed
```

New hub tables (mirror the movement model's discipline):

- `hub_supplier_insight` — `id, store_id, supplier_id, kind, payload_json,
  period_start, period_end, computed_at, source_ref` (idempotent like movements).
  `kind ∈ {velocity, stockout_risk, halo, sei, ncp, quality, cannibalization,
  reorder, reliability}`.
- `hub_insight_exposure` — `store_id, supplier_id, kind, visible(bool),
  updated_at` — the retailer's per-metric Flex switch (§6). Default-deny.

New endpoints:
- Ingest: `POST /ingest/insights` (store ingest token; same auth wall as movements).
- Portal read: `GET /portal/insights` → only `kind`s whose `hub_insight_exposure`
  is `visible` for that store+supplier, run through the existing
  `visibility.visible_movements` ownership+consent gate first.

**Why push, not compute-in-hub:** the hub has no cost/credit/GRN/basket data and
must not (data-minimization + the salt-caging principle). Reusing the on-prem
engine also means the portal and the store's own consoles show the *same*
numbers — one source of truth.

---

## 5. Portal UX evolution

Keep the current movement feed as the "Activity" tab; add tabs:

1. **Overview** — headline cards: velocity trend, days-of-cover heat, "3 SKUs at
   stockout risk", reliability badge. (Supplier value; drives login habit.)
2. **Performance** — GMROI, halo/attachment lift, broken-halo alerts, archetype
   mix of the supplier's SKUs. (Supplier value.)
3. **Scorecard** — the retailer-gated Flex: SEI, NCP/float, Q_s, cannibalization,
   with the plain-English framing from the doc ("you are trapping N KES"). Only
   the `kind`s the retailer switched visible appear. (Leverage + premium hook.)
4. **Actions** — reorder forecast ("ship X to store Y by Z"), and the two-sided
   CTAs: supplier proposes a slotting/rebate/consignment offer → routed to the
   retailer (this is the monetization loop, §7).

All still same-origin, single-file discipline; identity masking and
ownership/consent gate unchanged.

---

## 6. Privacy & the controlled Flex

The scorecard metrics are the **retailer's private negotiation position** — the
doc treats revealing them as a deliberate tactical act, not a default. So:

- Extend the existing **default-deny** model from movements to insights: a
  supplier sees an insight `kind` only if the store set
  `hub_insight_exposure.visible = true` for that (supplier, kind).
- Retailer admin UI (hub `/admin`): per-supplier toggles — "show this supplier
  their SEI / NCP / quality score?" The retailer stages the Flex, then flips it
  on right before a negotiation.
- Never expose another supplier's numbers, category-absolute figures that leak a
  rival, or the store's raw cost/credit terms — only the supplier's own derived
  scores. Identity masking on stores stays as-is.

This keeps the weapon in the retailer's hand while letting OASIS host the surface.

---

## 7. Monetization — why this pays

Ties directly to Ch. 6 Module 3 ("Non-Margin Revenue: Slotting & Rebates") and
the existing module-SKU licensing (`OfflineLicenseManager`).

- **Supplier free tier**: Activity feed + basic velocity (funnel; costs nothing,
  builds the habit).
- **Supplier premium (subscription, billed by OASIS)**: forecast, halo, days-of-
  cover alerts, category-anonymized benchmarking. Suppliers pay because a
  stockout of *their* SKU is *their* lost revenue — the alert has a hard ROI.
- **The Flex tier (retailer-side, part of the OASIS `revenue`/`network` module)**:
  the retailer pays OASIS for the MANDE scorecard + the ability to expose it to
  suppliers on demand. This is the negotiation weapon as a feature.
- **Slotting/rebate marketplace (take-rate)**: the "Actions" tab lets a supplier
  make a funded offer (slotting fee, volume rebate, consignment) in response to
  their scorecard; OASIS brokers and takes a cut. This is Ch. 6 Module 3 turned
  into a transaction rail.

The connector stays free (LGPL, the funnel); the money is at the hub, as designed.

---

## 8. Phased roadmap

| Phase | Ships | Powered by | New surface |
|---|---|---|---|
| **P0 — Hub-native signal** ✅ SHIPPED (commit 9ed50e2) | velocity (ADS) + 7d trend, days-of-cover, stockout-risk alerts — from the movement stream, zero on-prem coupling. (Reliability badge deferred to P1: needs the on-prem scorecard.) Live-verified against real Odoo data. | hub only | Overview (`oasis_hub/analytics.py`, `/portal/overview`) |
| **P1 — Insight Push channel** ✅ SHIPPED (commit 4089a02) | `hub_supplier_insight` + `hub_insight_exposure`, `POST /ingest/insights`, `POST /admin/insight-exposure` (the Flex switch), gated `GET /portal/insights`; on-prem `oasis/logic/insight_emitter.py` shapes supplier-safe cards (allow-list + forbidden-field guard: GRN/cost/credit can't cross). Double default-deny (consent AND per-kind exposure), live-verified. | on-prem + hub | wire only — UI lands in P2 |
| **P2 — Intelligence panel + push runner** ✅ SHIPPED (commit bc1f8f8) | Portal INTELLIGENCE panel renders exposed cards per-kind (reliability badge, SEI, halo pairs w/ lift, reorder, velocity) + generic fallback + empty state; on-prem `insight_push.py` and `--mode push-insights` build from `supplier_scorecard`/`mande_purge_report` and ship on a schedule (idempotent per period, auto dry-run without a token). | on-prem + hub | Intelligence panel |
| **P2b — remaining Performance metrics** ✅ SHIPPED (commit 803d48b) | `broken_halo`, `archetype`, and `capital_efficiency` card kinds. **GMROI deliberately does NOT ship** — it is margin-over-cost, so only a RELATIVE index + band ("1.3×, top quartile vs category median") crosses, never absolute margin. | `halo_pricing`, DHARAM | extends the panel |
| **P3 — Actions (offers)** ✅ SHIPPED (commit 1968b0b) | `hub_supplier_offer` + `POST/GET /portal/offers` + `GET /admin/offers` & `/respond`; ACTIONS panel with offer form and status table. Slotting / rebate / consignment / price-support as an auditable path. Suppliers address stores only by shown handle, only among consenting stores; masked stores stay masked while transacting; cross-supplier isolation enforced. | hub | Actions panel |
| **P4 — remaining Flex metrics + monetization** ✅ SHIPPED (commit f247340) | `ncp` + `cannibalization` kinds (guard gained a narrow, justified `allow` exemption for the supplier's OWN credit/DIO terms); supplier tiers gating **value** kinds only — Flex kinds are tier-exempt so a negotiation is never paywalled; commission recorded on accepted offers at a configurable `OASIS_HUB_COMMISSION_RATE` (recording only, no money moves, percentage offers defer the amount). | `mande_triage` via P1 | Scorecard/Actions |

P0 is genuinely small and high-signal — it makes the portal feel like a product
using only data we already ingest, with zero on-prem coupling. Recommend starting
there.

---

## 9. Risks & guardrails

- **Don't leak the retailer's book.** Only derived, supplier-own scores cross the
  wire; cost/credit/GRN never do. Enforced structurally by the push channel (the
  store chooses what to emit) + `hub_insight_exposure` (what to reveal).
- **Metric honesty.** Reuse the on-prem functions verbatim so portal numbers ==
  console numbers; never recompute a looser version in the hub. Label
  small-sample/low-confidence metrics (mirror the engine's own guards).
- **Consent still first.** Insights are gated by the *same* ownership+consent
  check as movements before exposure toggles even apply — a non-consenting store
  emits nothing.
- **Scope creep.** The scorecard/Flex is P3 on purpose; P0/P1 deliver value and
  the rail without touching the sensitive negotiation layer.

---

## 9a. Status — roadmap complete (July 2026)

P0 → P4 are all shipped and live-verified. What remains is **commercial policy
and operations, not engineering**:

1. **Set the numbers.** `OASIS_HUB_COMMISSION_RATE` is 0 (disabled) and the
   premium tier has no price attached. Both are deliberate — they are your
   decisions, and the mechanism reads them from configuration.
2. **Terms & agreement.** An accepted offer is currently a recorded handshake.
   Before money changes hands you need supplier T&Cs covering the take-rate, and
   a settlement path for percentage offers (they deliberately defer the amount).
3. **Invoicing.** `hub_supplier_offer.commission_*` is the billing feed; nothing
   generates an invoice yet, by design — no payment credentials live in the hub.
4. **Tier assignment.** `POST /admin/suppliers/tier` is manual; a self-serve
   upgrade flow would need the payment story above first.

## 10. Recommended first slice

Build **P0** (Overview tab: velocity, days-of-cover, stockout-risk, reliability
badge) — it's a few days, needs no on-prem changes, and converts the portal from
"a report" into "a reason to log in every morning." Then P1 lays the Insight Push
rail that everything methodology-grade rides on.
