# O.A.S.I.S. Customer Journey — From Forensic Diagnosis to Self-Driving Procurement

> **Purpose.** The keystone document the UI/UX work must be built around. It maps
> the *client's* journey (the 7-phase Implementation Playbook) to the *actors* who
> live it, the *modules/screens* they touch, and the *trust state* at each step —
> so the interface is organized around how clients actually progress, not around
> how the code happens to be split into apps today.
>
> **Sources (client's own materials):**
> - `OASIS_Client_Implementation_Playbook` (the 7-phase spine + decision gates)
> - `Oasis_pitch_data_ingestion_pipeline` (Phase 0/1 data intake mechanics)
> - `OASIS Visual System Guide — SYS v2.9` (brand: teal-turquoise / slate / white,
>   Montserrat + Space Mono, network-node iconography)
>
> **Status:** reference document — no code changes. Feeds
> `OASIS_UI_UX_Upgrade_Plan.md`. Generated 2026-06-13.

---

## 0. The single most important insight

The Playbook is not a feature list — it is a **trust-escalation ladder** with a
**mode progression** baked in:

```
DIAGNOSE → PROVE → STABILISE → FUND → SHIELD → AUTOMATE → SUSTAIN
 (audit)  (shadow) (AMIT flush) (DHARAM)  (LATA)   (MANDE)   (ongoing)
   │          │         │          │         │        │          │
   └─ the client hands over control one notch at a time, each unlocked by a
      decision gate, each justified by money already recovered.
```

The product's emotional spine is **"you are bleeding capital → here is the proof →
watch us stop it → now we drive."** The current UI represents *none* of this: there
is no visible sense of "what phase am I in," "what mode is the engine in (Shadow /
Active / Autonomous)," or "how much have we recovered so far." **Making phase, mode,
and cumulative value first-class, always-visible state is the highest-leverage UX
decision in the whole system.**

---

## 1. Experience foundation (from the Visual System Guide)

The client-facing UI should adopt the SYS v2.9 brand, replacing the ad-hoc
dark-neon glassmorphism currently hand-coded in `ops_dashboard.py`:

| Token | Value (from guide) | Use |
|---|---|---|
| Primary | **Deep Slate Grey** | App background / chrome |
| Accent | **System Teal-Turquoise (#00E5C…)** | Primary actions, "alive"/healthy state, brand |
| Neutral | **Platform White** | Surfaces, cards, text on dark |
| Display/body type | **Montserrat** (ExtraBold/Bold/Regular) | Headings, UI labels |
| Mono/data type | **Space Mono** | Metrics, codes, data tables, money figures |
| Iconography | Network/node set (server node, live node, security hub, analytics point, encrypted route, deploy-ready) | Status + module identity |

**Brand idea to carry through the UX:** *"a sanctuary of connection — distributed
points united into a single system."* The journey UI should literally show the
client's branches as connected nodes that go from red (bleeding) to teal (healthy)
as they climb the ladder. This is both on-brand and the clearest possible
visualization of value delivered.

This becomes the concrete content of **U1 (theme.py)** in the UI/UX plan — the
design tokens are no longer a guess; they're specified by the guide.

---

## 2. Actors / personas

| Persona | Who | Journey window | What they need from the UI |
|---|---|---|---|
| **iLink Operator** | Internal implementation/admin | All phases | Ingest messy data, run engines, configure thresholds, monitor health |
| **Prospect Owner / Exec** | The decision-maker being sold to | Phase 0–1 | A jaw-dropping, trustworthy diagnosis of their losses — in *their* numbers |
| **Client Executive** | Signs off on each gate | Phase 1–2, every gate | Proof, ROI, and a clear "approve next step" decision |
| **Finance Team** | Tracks recovered capital | Phase 3+ | Capital-recovery tracking, write-off/liquidation reporting |
| **Procurement Team / Buyer** | The daily power user | Phase 2–6 | Shadow comparison → review & approve Daily Master PO → exception handling |
| **Branch Manager** | Per-store operations | Phase 6 | Localized stock health, transfers, stockout response (role-scoped) |

The current UI serves the **Buyer/Branch** personas (the command center) reasonably,
under-serves the **Exec/Finance** personas (no dedicated ROL/recovery view that
follows the journey), and the **Prospect** persona is served by a *separate* pitch
app entirely disconnected from the rest.

---

## 3. The journey map (phase by phase)

Each stage lists: **actor · goal · module(s) in code today · what they see/do ·
decision gate · trust state · UI gap.**

### Stage 0 — First Contact (Day 0)  · DIAGNOSE (setup)
- **Actor:** iLink Operator ↔ Prospect Owner.
- **Goal:** secure the free forensic audit; collect raw data (POS + GRN minimum;
  Returns, Transfers, Stock optional).
- **Module today:** none — a sales conversation + data handoff. Ingestion is the
  `ForensicOperationsIngestor v2` (smart header detection, fuzzy column matching,
  50+ ERP variants, regex reason classification) behind the pitch app.
- **Decision gate:** POS + GRN logs in hand.
- **Trust state:** skeptical. "Prove you won't waste my time."
- **UI gap:** the *ingestion* experience is the first real touch. It must visibly
  forgive messy ERP exports (logos/title rows, odd column names) and show a
  confidence read-out ("matched 6/7 columns; backfilled cost from scorecard").
  Today this is buried in a script, not a guided upload screen.

### Stage 1 — The Forensic Audit (24–48h)  · DIAGNOSE
- **Actor:** iLink Operator presents to Prospect Owner + Client Executive.
- **Goal:** quantify total revenue bleed; close the contract.
- **Modules today:** `pitch_app_v2.py` + `pitch_data_ingestor_v2.py`; detection
  engines **AMIT** (dead stock: ADS<0.2 & SOH>15), **DHARAM** (stockouts:
  ADS>2 & SOH=0), **LATA** (suppliers: fulfilment<85% or lead-var>3σ), **MANDE**
  (returns/transfer entropy). Outputs: `OASIS_Executive_Diagnostic.docx`,
  `OASIS_Forensic_Audit_Data.xlsx`, a live 45-min Streamlit presentation.
- **Decision gate:** contract signed.
- **Trust state:** "these are mathematical facts from my own data" → convinced.
- **UI gap:** the diagnosis is the product's *single best sales asset* and it lives
  in an app disconnected from the platform the client will later use. The "bleed →
  recovery" number introduced here should become the **persistent value thread**
  that follows the client through every later screen.

### Stage 2 — API Hook & Shadow Mode (Weeks 1–2)  · PROVE
- **Actor:** iLink Operator (setup) + Buyer (continues ordering normally) + Exec (review).
- **Goal:** connect live ERP; generate daily Shadow POs (not sent); after 14 days
  present the human-vs-OASIS divergence.
- **Modules today:** `shadow_dashboard.py`, `shadow_monitor.py` daemon,
  `oasis/api/server.py` (the ERP hook), the engine's Shadow Mode.
- **Decision gate:** client approves Shadow → **Active**.
- **Trust state:** "it would have caught what my buyer missed" → trust in the algorithm.
- **UI gap:** Shadow is where trust is *won*. The divergence view (buyer over-ordered =
  future dead stock; buyer missed = future stockout; hostile supplier used unadjusted)
  must be unmissable and exec-readable. This is a dedicated journey screen, not a
  separate dashboard the client has to be told to open.

### Stage 3 — AMIT Flush / Dead-Stock Liquidation (Week 3)  · STABILISE
- **Actor:** iLink Operator + Finance Team + Exec sign-off.
- **Goal:** stop the bleed before buying anything new — liquidate trapped capital.
- **Modules today:** `amit_gatekeeper.py` / `amit_governance.py` → Negative List →
  system purchase blocks; liquidation via promo/write-off/PRTS; weekly Capital
  Recovery reports.
- **Decision gate:** capital frees up → client authorises active purchasing.
- **Trust state:** "real money came back" → belief becomes financial fact.
- **UI gap:** there is no dedicated **Negative List sign-off** screen or **Capital
  Recovery tracker** for Finance. This is the moment the value thread turns from
  *projected* to *realised* — it deserves a first-class screen.

### Stage 4 — DHARAM Hyper-Funding (Weeks 4–6)  · FUND
- **Actor:** Buyer (daily approvals) + Exec (impact review).
- **Goal:** pour freed capital into the top-20% revenue core; drive fast-mover
  stockouts to 0%.
- **Modules today:** `dharam_revenue.py`; Smart Ordering + Approval (the
  `ops_dashboard.py` Smart Ordering tab + `approval_dashboard.py`); order qty =
  `(ADS×LeadTime)+SafetyStock−SOH−InTransit`, grouped by supplier for one-click
  approval.
- **Decision gate:** revenue-core stockout = 0% → authorise full catalog.
- **Trust state:** "the shelves that make my money are never empty now."
- **UI gap:** the Buyer's **daily approval loop** is the workflow that will be used
  every day for years. It must be the most polished surface in the product — and
  the PO-approval dashboard currently has **no login at all**.

### Stage 5 — LATA Supplier Shield (Month 2)  · SHIELD
- **Actor:** Buyer + Exec.
- **Goal:** size safety stock to each supplier's measured reliability
  (Reliable 1.2× / Watch 1.5× / Hostile 2.0×+).
- **Modules today:** `lata_shield.py`; the Supplier Intelligence tab in `ops_dashboard.py`.
- **Decision gate:** safety stock dynamically managed; availability up without capital inflation.
- **Trust state:** "the system protects me from my worst vendors automatically."
- **UI gap:** supplier classification (Green/Yellow/Red) is exactly the kind of
  status that must be **icon + label + colour**, not colour alone (accessibility),
  and should feed a supplier scorecard the Exec can read.

### Stage 6 — MANDE Full Autonomous Ordering (Month 3)  · AUTOMATE
- **Actor:** Buyer → now an **approval manager**; Branch Managers; Exec.
- **Goal:** OASIS runs 100% of daily ordering + branch allocation; team shifts from
  *creating* orders to *approving* exceptions.
- **Modules today:** `mande_triage.py`; the allocation engine + transfer
  intelligence + `scheduler_service.py` daily cycle; Daily Master PO.
- **Decision gate:** self-driving operation.
- **Trust state:** "I just review exceptions" → full reliance.
- **UI gap:** the "Daily Master PO → review → approve → exceptions only" workflow is
  the *destination* of the whole journey. It needs an exception-first inbox, not a
  table the manager scrolls.

### Stage 7 — Post-Implementation (Ongoing)  · SUSTAIN
- **Actor:** Exec + Finance + approval manager.
- **Goal:** hold the gains (Dead stock <5%, fast-mover stockout <2%, capital
  utilisation >95%, fulfilment >85% enforced, transfers ≈0).
- **Modules today:** analytics/KPI views; weekly AMIT, daily LATA, continuous
  demand re-fit.
- **UI gap:** no journey-anchored "are we holding the targets" scorecard that maps
  to the Playbook's Pre→Post value table.

---

## 4. The Mode Progression — make it first-class state

The journey moves the engine through three operating modes. The system *has* these
modes internally; the UI should surface the current one as persistent chrome
(a badge in the shell header) with the gate to advance:

| Mode | Phases | Who acts | UI posture |
|---|---|---|---|
| **SHADOW** | 2 | OASIS observes; humans order | Read-only divergence; "approve to go Active" |
| **ACTIVE** | 3–5 | OASIS proposes; humans approve daily | Daily approval loop is the home screen |
| **AUTONOMOUS** | 6–7 | OASIS orders; humans handle exceptions | Exception inbox is the home screen |

Today a user cannot tell from the screen which mode they're in. A single persistent
**Mode + Phase badge** (e.g. "ACTIVE · Phase 4 DHARAM · KES 4.2M recovered") would
orient every persona instantly and reinforce the trust ladder.

---

## 5. Journey → Information Architecture (what the shell should be)

The journey dictates the navigation far better than the current 10-app split does.
Derived IA for the unified shell (`OASIS_UI_UX_Upgrade_Plan.md` U3):

```
OASIS shell (one app, one login)
├─ HOME / JOURNEY   — Mode+Phase badge, cumulative value recovered, next gate
├─ ONBOARD          — guided data ingestion (Stage 0) + match confidence
├─ DIAGNOSE         — forensic audit results (Stage 1)  [from pitch app]
├─ SHADOW           — human-vs-OASIS divergence (Stage 2)
├─ CAPITAL          — AMIT negative list sign-off + recovery tracker (Stage 3)  [Finance]
├─ ORDERING         — daily PO review & approval (Stages 4–6)  [Buyer — the daily driver]
├─ SUPPLIERS        — LATA scorecards & risk shield (Stage 5)
├─ TRANSFERS        — network allocation & transfers (Stage 6)
├─ ANALYTICS        — Pre→Post target scorecard (Stage 7)  [Exec]
└─ SETTINGS         — engine thresholds, users, mode/phase control  [Operator/Admin]
```

Role gating maps cleanly onto personas: Operator sees all; Exec sees
Home/Diagnose/Shadow/Capital/Analytics; Finance sees Capital/Analytics; Buyer lives
in Ordering/Suppliers/Transfers; Branch Manager sees a store-scoped Ordering/Transfers.

This is the same destination as the UI/UX plan's single shell — but the *journey*
is what justifies each nav item and its audience.

---

## 6. Gaps between the journey and today's UI

| # | Journey need | Today | Severity |
|---|---|---|---|
| J1 | Persistent Mode + Phase + value-recovered state | Absent everywhere | High |
| J2 | The diagnosis is the sales asset *and* the on-ramp | Lives in a disconnected pitch app | High |
| J3 | Guided, forgiving data ingestion with match confidence | Script-driven, no guided upload UX | High |
| J4 | Shadow divergence as an exec-readable trust screen | A separate dashboard you must know to open | High |
| J5 | Finance capital-recovery tracker + Negative-List sign-off | No dedicated screen | High |
| J6 | Buyer daily-approval loop as the polished daily driver | Functional but ungated (approval app has no login) | High (security + UX) |
| J7 | Supplier Green/Yellow/Red as accessible status + scorecard | Colour-only, buried in a tab | Medium |
| J8 | Autonomous "exception inbox" home | A table to scroll | Medium |
| J9 | Pre→Post target scorecard tied to Playbook metrics | Ad-hoc analytics | Medium |
| J10 | One brand (SYS v2.9 teal/slate) across all surfaces | Dark-neon glassmorphism, per-app divergent CSS | Medium |

(J6 and the ungated-app finding overlap with the UI/UX plan's U2 security gap, and
with the logic-review work already done — the *logic* under Ordering is now unified;
the *journey-grade UI* over it is the remaining half.)

## 7. How this drives the UI/UX upgrade plan

The journey doesn't change the *phases* of `OASIS_UI_UX_Upgrade_Plan.md` — it gives
them their *content and priority*:

- **U1 (theme):** adopt SYS v2.9 tokens (teal/slate/white, Montserrat + Space Mono,
  node iconography). No longer a guess.
- **U1/U4 (components):** add journey primitives — `mode_phase_badge()`,
  `value_recovered_meter()`, `decision_gate_card()`, `divergence_row()`,
  `supplier_status_chip()` (icon+label+colour).
- **U2 (auth):** gate every screen — especially Ordering/Approval and the Finance
  Capital views (money).
- **U3 (shell):** build the IA in §5, organised by journey stage, role-gated by persona.
- **U5 (telemetry):** measure progression *along the journey* (which stage each
  client is in, time-in-shadow, approval latency) — the truest "gradeable" metric.

**Recommended addition to the plan:** a **U0 — Journey Spine** slice done with U1:
the Home/Journey screen + persistent Mode/Phase/value badge. It's small, high-impact,
and everything else hangs off it.

---

## 8. Resolved decisions (confirmed 2026-06-13)

These were open questions; now settled and binding on the build.

1. **Pitch/diagnosis = internal iLink-operator tool, one codebase.** The
   prospect-facing pitch is **operated by the internal iLink team only**; the
   prospect is *handed the outputs* (Executive Diagnostic `.docx`, Forensic Audit
   `.xlsx`, the live 45-min presentation) and **never logs into the pitch UI**.
   It is the same codebase as the client platform, exposed as an **operator-only
   `DIAGNOSE` mode** (runs on an iLink machine against prospect data, pre-contract).
   Post-contract, the diagnosis is preserved as a **read-only Stage 1 "here's what
   we found"** view inside the client shell, carrying the bleed→recovery value
   thread forward. → `pitch_app_v2.py` collapses into the shell's operator-gated
   DIAGNOSE page, not a standalone client app.

2. **Operation is a handoff across two operator tiers.** An `ilink_operator`
   superuser role (ingestion, engine thresholds, mode/phase advancement, all
   stores) dominates Phases 0–3 and is **retained permanently** for support /
   monitoring. Client roles grow into ownership across Phases 4–7.
   **ONBOARD / DIAGNOSE / SETTINGS are operator-gated — engine config is not
   client-facing.**

3. **Finance gets its own role.** Scoped to **CAPITAL + ANALYTICS (read-mostly)**:
   capital-recovery tracker, AMIT Negative-List value, write-off/liquidation
   reporting, read-only analytics. No ordering, transfers, or settings. (Phase 3
   names the client finance team explicitly.)

4. **Mode/Phase advancement is hybrid, human-confirmed.** The system computes
   gate-readiness from metrics and surfaces a `decision_gate_card()` prompt
   ("targets met — advance to X?"), but advancement is **always an explicit
   operator/exec approval, logged to audit. Never auto-advance** — this preserves
   the trust-by-consent model the Playbook's decision gates encode.

5. **Branch-manager mobile is deferred (UI plan U6).** No imminent multi-branch
   field-staff client. The mobile API (`oasis/api/server.py`, 8550) stays a
   dormant seam; the web shell is made tablet-responsive as cheap insurance.
   Revisit only when a Phase-6 client with real floor-staff demand appears.

**Resulting role model:** `ilink_operator`, `executive`, `finance`,
`approval_manager` (buyer), `branch_manager`.
