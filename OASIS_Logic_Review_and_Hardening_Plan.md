# O.A.S.I.S. Logic Review & Hardening Plan

> **Status:** Review document — **no code changes yet.** This plan is for sign-off
> before implementation. Each item lists the evidence (file:line), the proposed
> change, blast radius on existing tests, and risk.
> Generated 2026-06-13.

---

## Part A — Allocation / Ordering logic findings

Three behavioural issues surfaced during the workflow audit. All three are
**deliberate-change** items: they alter what quantities/numbers the engine
produces, so each requires regenerating a golden baseline. None are crashes —
the system runs today; these are correctness/честность-of-output fixes.

### A1. Greenfield "skipped" count is structurally under-reported

**Evidence**
- `total_skipped` / `skip_reasons` are written in exactly one place:
  `_gf_pass1_width` → [procurement_mixin.py:559](oasis/logic/procurement_mixin.py:559).
- Items zeroed *after* Pass 1 are never counted as skipped:
  - Pass 1.5 liquidity prune (`_gf_pass1_5_liquidity_prune`, sets qty=0)
  - Premium-cap zeroing (`_gf_premium_trim`)
  - Pass 3 anchor-MOV prune (`_gf_pass3_anchor_mov`)
  - `apply_safety_guards` zeroing items entirely *outside* the engine
    ([order_logic_guards.py:93](oasis/logic/order_logic_guards.py:93))
- `_gf_final_audit` only sums items with `qty > 0`
  ([procurement_mixin.py:1261](oasis/logic/procurement_mixin.py:1261)); it never
  computes "how many ended at zero."

**Impact:** the snapshot at the 150k tier reports `total_skipped: 4` while more
than 4 SKUs ended at quantity 0. A manager reading "4 skipped" trusts a number
that is correct only for Pass 1.

**Proposed change**
1. In `_gf_final_audit`, after the parity loop, compute the *authoritative*
   skip count from the final list:
   `final_skipped = [r for r in recommendations if qty == 0]`.
2. Re-derive `skip_reasons` from the final reasoning tags (the tags already
   exist: `[PRUNED…]`, `[PREMIUM CAP ZEROED]`, `[ANCHOR PRUNE…]`,
   `[PASS 1: SKIPPED]`, plus guard tags). Keep the Pass-1 live counter as an
   intermediate but let the audit be the source of truth.
3. Add a `skipped_by_stage` breakdown (`pass1`, `liquidity_prune`,
   `premium_trim`, `anchor_mov`, `safety_guards`) so the UI can show *where*
   items dropped out — far more useful than one flat number.

**Test impact:** `tests/test_allocation_snapshot.py` baseline regenerates
(summary block changes; per-product quantities do **not**). Regenerate with
`OASIS_UPDATE_SNAPSHOTS=1` and eyeball the new skip counts before committing.

**Risk:** Low. Reporting-only; no quantity math touched.

---

### A2. Pass-4 "Universal Sweep" dumps residual budget into dead stock

**Evidence**
- Phase 4C main loop *does* respect a velocity ceiling
  ([procurement_mixin.py:1200](oasis/logic/procurement_mixin.py:1200)):
  `(current_q + 1) > avg_s * limit_days` skips the item. For a zero-sales item
  `avg_s` is floored to 0.01 → ceiling ≈ 1.2 units, so the main loop barely
  touches it.
- **But the fallback "Universal Sweep"**
  ([procurement_mixin.py:1218-1232](oasis/logic/procurement_mixin.py:1218)) has
  **no velocity ceiling at all** — it adds 1 unit to *any* item with `qty > 0`
  and `cost ≤ remaining`, purely to drive utilization to 100%. The cheapest
  item with a nonzero starting quantity absorbs the entire residual.
- This is how `DEAD STOCK ITEM` (ADS = 0) reached **110 units** at the 1M tier
  in the snapshot: Pass 1 shelf-fill gave it a starting MDQ > 0, then the sweep
  piled the leftover budget onto it because it was cheap.

**Impact:** the engine knowingly buys dead stock to hit a vanity "100% capital
deployed" metric. This is the single most defensible thing to change for real
retail use — over-buying non-selling SKUs is exactly what the platform is
supposed to *prevent*.

**Proposed change (decision required — pick one, A2-a recommended)**
- **A2-a (recommended): Stop forcing 100%.** Introduce a configurable
  `max_utilization_pct` (e.g. 98%) and a `min_velocity_for_mopup` (e.g. ADS ≥
  0.05). The sweep and 4C only deploy into items above that velocity; residual
  cash below the threshold is reported as `unused_budget` with reason
  "no qualifying high-velocity headroom" rather than forced into dead stock.
- **A2-b: Keep 100% target but cap the sweep.** Apply the same
  `avg_s * limit_days` ceiling used by the 4C main loop to the Universal Sweep,
  and exclude items with ADS below a floor. Residual that can't be placed
  within ceilings stays unused.
- **A2-c: Velocity-weighted sweep.** Distribute residual across the *top-N by
  ROI* in proportion to velocity rather than dumping on the cheapest. More
  complex; only if leftover-cash deployment is genuinely valued.

**Recommendation:** A2-a. It matches the stated business goal (don't trap
capital in dead stock) and is the smallest change. `AllocationConfig` already
exists as the home for `max_utilization_pct` / `min_velocity_for_mopup`.

**Test impact:** `tests/test_allocation_snapshot.py` quantities change at both
tiers (the dead-stock pile-on disappears; utilization drops a few %). Regenerate
baseline and verify `DEAD STOCK ITEM` → 0 (or its Pass-1 MDQ only). Add a
dedicated assertion: zero-ADS items never exceed their Pass-1 quantity.

**Risk:** Medium. Changes headline utilization numbers shown in dashboards;
worth a heads-up so the lower (honest) utilization isn't mistaken for a
regression.

---

### A3. Supplier-schedule fallback bunches orders onto the same days

**Evidence**
- When a supplier has no calendar entry, ordering-day falls back to
  `(current_day % gap_days == 0) or (current_day == 1)`
  ([simulation_bridge.py:173](oasis/logic/simulation_bridge.py:173)).
- Every supplier sharing a `gap_days` (e.g. 7) fires on the *same* days of the
  year across every store; `current_day` is day-of-year, so the cycle also
  resets discontinuously at year-end.

**Impact:** synthetic order spikes (all weekly suppliers land on the same day),
uneven logistics load, and a year-boundary discontinuity. Affects replenishment
realism, not greenfield.

**Proposed change**
- Phase-stagger by a deterministic per-supplier offset:
  `((current_day + stable_hash(supplier)) % gap_days) == 0`, where
  `stable_hash` is a fixed hash (e.g. `int(hashlib.md5(supplier).hexdigest,16)`)
  so a given supplier always lands on the same offset but different suppliers
  spread across the cycle.
- Optionally seed the offset from `lead_time_days` so deliveries, not just
  orders, are smoothed.

**Test impact:** `tests/test_replenishment_logic.py` uses `current_day=1`
(always an ordering day) so most cases are unaffected; add explicit tests that
two suppliers with the same gap land on *different* days, and that day-1
behaviour is preserved. No greenfield snapshot impact.

**Risk:** Low–medium. Deterministic and bounded; only changes *which* day an
item orders in multi-day simulation, not the quantity.

---

### A-series sequencing & test strategy

1. Land **A1** first (reporting-only, smallest blast radius, builds confidence
   in the regenerate-baseline workflow).
2. Land **A3** next (isolated to replenishment, own tests, no snapshot churn).
3. Land **A2** last and deliberately — it's the one that visibly moves
   dashboard utilization numbers. Regenerate the greenfield snapshot, diff it
   line-by-line, and capture the before/after utilization in the commit message
   so the drop is understood as intentional.

Each fix is independently committable. The golden snapshot is the safety net:
regenerate only on these intentional commits, never silently.

---

## Part B — Multi-tenancy / SSO / Secrets-vault (onsite, low-cost target)

The stated deployment model is **onsite, cost-sensitive** — typically one
O.A.S.I.S. box per retailer/site. That model collapses most of the complexity
these three items carry in a cloud-SaaS context. Recommendation per item below;
each can be revisited if a hosted multi-retailer offering ever materialises.

### B1. Multi-tenancy

**Current state:** every table already carries a dormant `TENANT_ID` defaulting
to `'default_tenant'` (see `oasis/models.py`). Nothing reads or filters on it.

**Options**
| Option | Fit for onsite | Effort | Notes |
|---|---|---|---|
| **B1-a: One DB per install (silo)** | ✅ Best | ~0 | `TENANT_ID` stays dormant; each site's data is physically isolated by being a separate box/DB. No query changes, no cross-tenant leakage risk. |
| B1-b: Row-level tenancy (shared DB) | ⚠️ Only if hosting many retailers centrally | High | Every query needs a tenant filter; risk of leakage; needs tenant-scoped connection middleware. Justified only for cloud multi-retailer. |
| B1-c: Schema-per-tenant (Postgres) | ⚠️ Middle ground | Medium | Postgres `search_path` per tenant. Useful if one box serves a few branches of one chain but wants data separation. |

**Recommendation: B1-a (silo).** For onsite, multi-tenancy is a non-feature —
one install = one tenant. **Action: none now.** Keep `TENANT_ID` in the schema
(it costs nothing and preserves the option), but do not build row-level
filtering. If a central-hosting product is ever planned, revisit with B1-c.

**If we ever do B1-b/c**, the prerequisite work is: a `TenantContext`
(contextvar) set at request/session start, a connection wrapper in
`oasis/logic/db.py` that injects the tenant filter or `search_path`, and an
audit pass to ensure no query bypasses it. That's a multi-week effort and a
testing burden — explicitly out of scope for onsite.

### B2. SSO / OIDC

**Current state:** username/password auth in `oasis/logic/auth_manager.py`,
bcrypt-hashed, with role-based permissions and session tokens. Solid for a
single-site app.

**Options**
| Option | Fit for onsite | Effort | Notes |
|---|---|---|---|
| **B2-a: Keep local auth** | ✅ Best | 0 | Onsite shops rarely run an IdP. Existing bcrypt + roles is appropriate and already hardened. |
| B2-b: Optional OIDC adapter | ⚠️ Only for chains | Medium | A pluggable `oidc` auth backend (Authlib) behind a feature flag, for a chain that already runs Azure AD / Google Workspace / Keycloak. Local auth stays default. |
| B2-c: Mandatory SSO | ❌ | High | Wrong for onsite — adds an external dependency a single shop can't satisfy. |

**Recommendation: B2-a.** Keep local auth as the default and only path for now.
**Action: none now**, beyond two small hardening touches worth doing whenever we
next touch auth (track as minor items, not this plan): (1) enforce a password
policy on seed/admin-set passwords, (2) add session-expiry sweep for
`OASIS_SESSIONS`. If a chain customer with an existing IdP appears, implement
B2-b as an additive backend — no rip-and-replace.

### B3. Secrets vault

**Current state:** secrets resolved from environment / `.env`
(`OASIS_API_KEY`, `OASIS_SEED_PASSWORD`, `OASIS_LICENSE_SALT`,
`OASIS_DB_URL`); `.env` is gitignored; `.env.example` documents the keys.

**Options**
| Option | Fit for onsite | Effort | Notes |
|---|---|---|---|
| **B3-a: `.env` + file permissions** | ✅ Best | ~0 | For a single onsite box, an OS-permissioned `.env` (chmod 600 / restricted ACL) is the standard, zero-cost answer. |
| B3-b: OS keyring | ⚠️ Optional | Low | `keyring` lib backed by Windows Credential Manager / libsecret. Marginal gain over a locked-down `.env` on a single box. |
| B3-c: Managed vault (Vault/AWS/Azure) | ❌ | High + $ | Only earns its keep in cloud/multi-node hosting. Adds a network dependency and recurring cost — contrary to the brief. |

**Recommendation: B3-a.** Document and (optionally) script the file-permission
hardening: on Windows, an `icacls` step in the installer to restrict `.env` to
the service account; ensure the app never logs secret values (already the case
in `security.py`, which logs only that a key was generated, never the value).
**Action: a short "secrets hardening" note in the deployment guide** — no code
dependency on a vault.

### Part B summary

For an onsite, low-cost target the correct engineering decision for **all three**
is to **not build the cloud-SaaS versions**:
- **Multi-tenancy → silo (one DB per install).** No work; `TENANT_ID` stays as
  a dormant option.
- **SSO → keep local bcrypt auth.** Optional OIDC backend only if a chain with
  an existing IdP signs on.
- **Secrets → permissioned `.env`.** Document the `icacls`/chmod step in
  deployment; no vault.

The only concrete deliverables from Part B are **documentation** (deployment
guide: secrets file hardening; a one-paragraph statement that the product is
single-tenant-per-install by design) plus two minor auth-hardening backlog
items (password policy, session expiry sweep). Net new code: ~none.

---

## Proposed execution order (after this review is approved)

1. **A1** — skip-count audit fix (reporting; regen snapshot summary).
2. **A3** — supplier-schedule phase-stagger (replenishment; own tests).
3. **A2** — mop-up dead-stock fix (decision A2-a; regen snapshot quantities,
   document utilization drop).
4. **B (docs only)** — deployment-guide notes for secrets hardening + the
   single-tenant-by-design statement; file the two auth-hardening backlog items.

Items A1–A3 are the only ones that touch code, are each independently
committable, and are each guarded by a regenerated golden test.
