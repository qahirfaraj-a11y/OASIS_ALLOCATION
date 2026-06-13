# O.A.S.I.S. — Onsite Deployment: Tenancy, Auth & Secrets (Decision Record)

> Companion to `OASIS_Logic_Review_and_Hardening_Plan.md` (Part B).
> **Deployment model:** onsite, cost-sensitive — typically one O.A.S.I.S.
> install per retailer/site. The decisions below follow from that model.
> Generated 2026-06-13.

---

## TL;DR

For an onsite, low-cost target the correct engineering decision for all three
"enterprise" concerns is to **not build the cloud-SaaS versions**:

| Concern | Decision | Net new code |
|---|---|---|
| Multi-tenancy | **Silo — one DB per install.** `TENANT_ID` stays dormant. | None |
| SSO / OIDC | **Keep local bcrypt auth.** OIDC only if a chain with an IdP signs on. | None now |
| Secrets vault | **Permissioned `.env`.** Document file-ACL hardening. | None (docs + installer step) |

---

## 1. Multi-tenancy — decision: SILO

**Rationale.** One install serves one retailer, so data isolation is physical
(separate box / separate DB file or Postgres instance). Row-level tenancy
(filtering every query by `TENANT_ID`) buys nothing here and adds a permanent
leakage-risk surface and testing burden. The `TENANT_ID` column already exists
in every table (`oasis/models.py`) and **stays as a dormant option** — it costs
nothing to keep and preserves a future path.

**What we are NOT building (and why it's safe to defer):** shared-DB row-level
tenancy needs a `TenantContext`, a connection wrapper in `oasis/logic/db.py`
that injects the filter on every query, and an audit that nothing bypasses it.
That is multi-week work justified only by central hosting of many retailers —
explicitly out of scope.

**Trigger to revisit:** a hosted, multi-retailer SaaS offering. At that point
prefer schema-per-tenant on Postgres (`search_path`) over row-level filtering.

## 2. Authentication — decision: KEEP LOCAL

**Current state.** `oasis/logic/auth_manager.py`: bcrypt-hashed passwords,
role-based permissions, session tokens in `OASIS_SESSIONS`. Appropriate and
already hardened for a single site.

**Rationale.** Onsite shops rarely run an identity provider; mandatory SSO would
add an external dependency a single store cannot satisfy. Local auth stays the
default and only path.

**If a chain with an existing IdP signs on:** add an *additive* OIDC backend
(e.g. Authlib) behind a feature flag — local auth remains default, no
rip-and-replace. Azure AD / Google Workspace / Keycloak all speak OIDC.

**Backlog (do whenever auth is next touched — not blocking):**
- **B-AUTH-1: Password policy.** Enforce a minimum strength on seed/admin-set
  passwords in `auth_manager` (length + complexity); reject trivial values.
- **B-AUTH-2: Session expiry sweep.** Periodically purge expired
  `OASIS_SESSIONS` rows (the scheduler service is a natural home) so stale
  tokens don't accumulate.

## 3. Secrets — decision: PERMISSIONED `.env`

**Current state.** Secrets resolved from environment / `.env`
(`OASIS_API_KEY`, `OASIS_SEED_PASSWORD`, `OASIS_LICENSE_SALT`, `OASIS_DB_URL`);
`.env` is gitignored; `.env.example` documents every key; `security.py` logs
only that a key was generated, never its value.

**Rationale.** On a single onsite box an OS-permissioned `.env` is the standard,
zero-cost answer. A managed vault (HashiCorp/AWS/Azure) adds a network
dependency and recurring cost — contrary to the brief — and earns its keep only
in cloud/multi-node hosting.

**Hardening steps for the installer / deployment guide:**
- **Windows:** restrict `.env` to the service account only, e.g.
  `icacls "<install>\.env" /inheritance:r /grant:r "<svc-account>:R"`.
- **Linux:** `chown <svc-user> .env && chmod 600 .env`.
- Confirm logs never echo secret values (already the case in `security.py`).
- Keep `.env` out of backups that leave the box, or encrypt those backups.

**Trigger to revisit:** cloud or multi-node hosting → OS keyring
(`keyring` lib) as a low-effort step up, managed vault only if compliance
demands it.

---

## Net deliverables from Part B

1. This decision record (durable rationale for the three "we chose not to build
   the cloud version" calls).
2. Two auth-hardening backlog items (B-AUTH-1 password policy, B-AUTH-2 session
   expiry sweep) — small, additive, non-blocking.
3. Installer/deploy-guide secrets-hardening steps (above) to fold into
   `DEPLOYMENT_GUIDE.md` when that file is next edited.

No application code changes are required for Part B as scoped.
