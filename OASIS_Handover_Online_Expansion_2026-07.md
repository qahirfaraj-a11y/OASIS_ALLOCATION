# O.A.S.I.S. — Online Expansion Handover · July 2026

**Purpose:** the unvarnished state of the **online expansion chapter** — Cloud
Hub, Odoo connector, supplier portal, and the client-release tightening that
preceded them. Written for anyone — including a future us — who needs to know
exactly what is built, what is proven and at what level, what blocked the final
live demonstration, and precisely how to resume. Companion to
`OASIS_Handover_Real_Position_2026-07.md` (the pre-expansion baseline).

**5 commits** this chapter (`d2fa118..eec70f7`) · **43 files, ~4,000 lines** ·
new-surface tests **58 passed, 0 failed** · full repository suite
**754 passed, 0 failed** (was 681 at the last handover; +73) · ruff clean.

---

## 1. Executive position

Before this chapter OASIS was a sellable **offline** product: on-prem installs,
licenses issued by hand-running a script with the salt. Everything online was ✗.

The chapter built the entire online vertical slice:

```
Odoo (POS + stock) ──addon/XML-RPC──▶ OASIS CLOUD HUB ──▶ Supplier Portal (web)
   :8069                               :8700                :8700/portal-app
                                        │
                              online license issuer
                              (salt lives HERE only)
```

Every piece is **built, tested, and committed**. The Odoo→hub→portal pipeline is
proven end-to-end **in-process** (real mapper → real push client → real hub app
→ real visibility filter → supplier sees only their own products), and the
portal is proven **live in a real browser** against a natively-running hub with
seeded data. The one thing that has **not** happened is the full
containerised bring-up — a real Odoo server container pushing to the hub
container — because the development machine's Docker Desktop is in a crash loop
(§6, environment fault, not OASIS). The harness for that run is finished and
committed; it is a three-command procedure once any healthy Docker (or a hosted
Odoo trial) is available.

We are at: **"the online product exists and demonstrably works; the
containerised live demo is one healthy Docker engine away."**

---

## 2. What was built — commit by commit

| Commit | What it is |
|---|---|
| `d2fa118` | **Strict-whitelist client release.** Packager inverted from blacklist to whitelist (`should_ship_clean`); zip fell 1,095 files / 14.6 MB → **145 files / 0.5 MB**; interactive `install.bat` (3 questions, 4 steps); cold-start proof re-passed 8/8 on the trimmed zip. |
| `0d4a434` | **Cloud Hub foundation** (`oasis_hub/`, `--mode hub`, port 8700). Schema, three auth walls, online license issuer, opt-in ingestion, portal API, and the visibility privacy backbone. |
| `9fc737d` | **Odoo connector** (`connectors/odoo/`). Pure mapping core + stdlib push client, an installable Odoo 16 addon (Apps-store shape), and a standalone XML-RPC backfill needing no addon install. |
| `1449796` | **Supplier portal front-end.** One self-contained HTML file (no build, no deps) served same-origin by the hub at `/portal-app/`; OASIS SYSTEMS v1.0 palette. |
| `eec70f7` | **Integration stack + runbook.** `docker-compose.odoo.yml` (postgres + odoo-init + odoo + hub), slim hub Dockerfile, `bootstrap_hub.py`, `configure_and_sync.py`, `INTEGRATION.md`. Mapper upgraded: customer-bound stock moves = `sale`. |

### Key design decisions (load-bearing)

- **The privacy contract is structural, in one place.** A supplier sees a
  movement iff (a) it matches one of their ownership rules
  (`hub_supplier_brand`: supplier_cd / brand / department / sku) **and** (b) the
  store granted consent (`hub_store_consent.status='granted'`). Default-deny on
  both axes. Store identity masks to a stable `Store #XXXXXXXX` handle unless
  `reveal_identity`. Every portal read goes through
  `oasis_hub/visibility.py::visible_movements` — there is no endpoint that can
  bypass it. This was the user-chosen model ("own products, store opt-in") and
  is hard to loosen later *by design*.
- **The hub is the vendor.** `OASIS_LICENSE_SALT` lives only in the hub
  environment. `OfflineLicenseManager.build_key()` (new) mints keys in memory;
  the hub stores them in a `hub_license` ledger and clients receive only signed
  key bodies — the existing offline verification on client installs is
  unchanged.
- **Connector logic is ERP-agnostic and Odoo-free.** `mapping.py` (Odoo record
  → hub `MovementIn`) and `push_client.py` (batched, idempotent, stdlib-only
  HTTPS) import nothing from `odoo`, are validated directly against the hub's
  pydantic schema, and are reused by both the addon and the XML-RPC backfill.
  Zoho/Sage/Tally are thin adapters over this same core.
- **Idempotency end to end.** Hub dedups on `(store_id, source_ref)`; the addon
  advances its sync watermark only after a successful push. Crash → re-send is
  always safe.
- **Server-side code never ships to clients.** `oasis_hub/` and `connectors/`
  are excluded from the client zip and regression-guarded
  (`test_cloud_hub_never_ships`, `test_erp_connectors_never_ship`) — leaking the
  hub would leak the salt-issuing surface.

---

## 3. Proof matrix — what is proven, at what level

Levels: **L1** unit · **L2** end-to-end in-process (real FastAPI TestClient,
temp DB) · **L3** live process + real browser · **L4** full containerised stack
(real Odoo server) · **✗** not yet.

| Piece | Level reached | Evidence |
|---|---|---|
| Visibility privacy contract | **L1+L2+L3** | 14 spec tests (rival SKU excluded, non-consenting store excluded, revoked/pending/no-rules see nothing, masking stable); held in live browser: seeded rival Pepsi rows appeared nowhere |
| Hub auth (3 walls) | **L2** | admin/ingest/portal all reject without credentials in TestClient e2e |
| Online license issuing | **L2** | issue → ledger → key verifies under the same fingerprint the on-prem client checks; bundles resolve; revoke supersedes |
| Ingestion (batch, idempotent) | **L2+L3** | dedup on re-push proven in tests and against the live server (140 seeded, 112 visible, re-push accepted 0) |
| Odoo mapping (`sale`/`receipt`/`adjustment`/`stock_on_hand`) | **L1 + contract** | per-type tests; every mapper output validates against `oasis_hub.schemas.MovementIn` — connector drift breaks the build |
| Push client (chunking, retry, fail-fast 4xx) | **L1+L2** | batch 2+2+1, transient-retry, permanent-reject tests; used as the real transport in e2e |
| Odoo → hub → supplier (full slice) | **L2** | fabricated Odoo POS lines → real mapper → real push client → hub TestClient → supplier login sees ONLY owned SKU, masked per consent |
| XML-RPC backfill (no-addon path) | **L2** | same slice through `xmlrpc_sync.backfill` against a fake `execute_kw` |
| Supplier portal web app | **L3** | logged in via real browser against live hub: KPIs (2,340 units / 4 SKUs / 2 stores / 112 movements), named + masked stores, filters working |
| Odoo addon (manifest, settings UI, cron, ACL) | **L1 (static) / ✗ (runtime)** | manifest well-formed + data files exist; **never installed into a running Odoo** — models/*.py imports `odoo`, so it is exercised only by the L4 run |
| Containerised bring-up (`docker-compose.odoo.yml`) | **✗ (blocked)** | compose config validates; never executed — Docker Desktop crash loop (§6). This is the only ✗ in the chapter. |
| Client release (whitelist zip) | cold-start proven | 8/8 stages on the 145-file / 0.5 MB zip; copy on the Desktop |

**Test ledger:** new-surface suite (hub api 9 · hub visibility 14 · odoo
connector 14 · release packager 13 · release zip 8) = **58 passed** in ~6s.
Full repository suite re-run for this handover: **754 passed, 0 failed,
0 skipped** in 4m12s.

---

## 4. How to run what exists (no Docker needed)

**Hub, natively:**
```bash
cd .gemini/antigravity/scratch
export OASIS_HUB_DB_URL="sqlite:///<path>/hub.db" \
       OASIS_HUB_ADMIN_KEY=... OASIS_HUB_TOKEN_SECRET=... OASIS_LICENSE_SALT=...
./.oasis_venv/Scripts/python.exe -m uvicorn oasis_hub.app:app --port 8700
# or: python entrypoint.py --mode hub
```
Portal: `http://localhost:8700/portal-app/` · docs: `/docs` · health: `/health`.

**Provision (idempotent):** `python connectors/odoo/bootstrap_hub.py`
— creates tenant `acme`, store `ODOO-01`, supplier `COKE`/`demo123` owning
`supplier_cd=SUP_COKE`, granted consent; prints the ingest token and saves
`.hub_state.json`.

**Demo data without any Odoo:** the seeding block used for the L3 browser proof
lives in this conversation's history; any POST to `/ingest/movements` with the
store token reproduces it.

**Backfill from any reachable Odoo (no addon install):**
```bash
python -m connectors.odoo.xmlrpc_sync --odoo-url https://<odoo> --db <db> \
    --user <u> --password <pw> --hub-url http://localhost:8700 \
    --ingest-token <from bootstrap> --since 2026-06-01
```

---

## 5. The full-integration procedure (when Docker is healthy)

Documented in `connectors/odoo/INTEGRATION.md`. In short, from
`connectors/odoo/`:

```bash
docker compose -f docker-compose.odoo.yml up -d --build   # postgres+odoo+addon+hub
python bootstrap_hub.py                                    # hub side
python configure_and_sync.py                               # odoo side + first sync
# open http://localhost:8700/portal-app/  → COKE / demo123
```

`configure_and_sync.py` writes the addon's `ir.config_parameter` settings
(hub URL is `http://oasis-hub:8700` **in-network**), seeds 4 Coke SKUs × 14 days
of customer-bound stock moves in Odoo, and calls `oasis.sync.run_sync()` inside
Odoo. After that the addon's 30-minute cron keeps the hub current unattended.

---

## 6. The Docker blocker — diagnosis and state

**Symptom:** Docker Desktop engine crash-loops at startup with
`initializing Inference manager: … remove …\Docker\run\dockerInference: The
file cannot be accessed by the system`.

**Diagnosis:** Docker Desktop's Model Runner ("Docker AI") leaves a corrupt
AF_UNIX socket file that Windows cannot unlink from user space (delete/move fail
with the same error even with all Docker processes dead and WSL shut down — a
kernel-held inode). On boot the inference listener tries to remove-and-rebind,
fails, and aborts the whole engine. **Environment fault; nothing in OASIS
touches it.**

**What was tried:** WSL reset (`wsl --shutdown`) — no effect;
`EnableDockerAI=false` written to `%APPDATA%\Docker\settings-store.json` —
**Docker Desktop reset it to `true` on next launch** (flag does not stick when
edited on disk); Windows reboot — cleared the stale file, but the engine
recreated it and crashed again on first start, because Docker AI was re-enabled.

**What will fix it (owner action, in order of likelihood):**
1. Disable Docker AI **through the GUI**: Docker Desktop → Settings → *Beta
   features / AI* → turn off "Docker AI / Model Runner" → Apply & restart. (The
   GUI path persists where the file edit did not.) If the engine won't stay up
   long enough, use *Troubleshoot → Clean / Purge data* first.
2. Update Docker Desktop — the inference-socket crash is a known fixed class of
   bug in newer releases.
3. Reboot once more **after** the setting sticks, so the stale socket is gone
   when a non-AI engine starts.

**No-Docker fallback (works today):** hosted Odoo trial (odoo.com, free)
+ the XML-RPC backfill in §4 + the native hub. Proves "real Odoo → OASIS live"
with zero containers. Note: Odoo Online does not allow custom addons, so this
path exercises the backfill, not the addon; the addon needs any self-hosted
Odoo (the compose stack, or a customer's server).

---

## 7. Remaining work, ranked

1. **Execute the L4 live bring-up** (§5) once Docker is healthy — the only
   unproven piece is the addon running inside a real Odoo (`odoo-init` installs
   it; its runtime code paths mirror the tested XML-RPC collectors).
2. **Odoo Apps store submission** — the addon is in submission shape; swap
   `static/description/banner.png` placeholder art, register the vendor
   account, submit. (OPL-1 license declared in the manifest.)
3. **Zoho connector** (next ERP; cloud REST + OAuth, marketplace listing), then
   Sage, then Tally (on-prem XML bridge, big East-Africa install base) — all
   thin adapters over `mapping.py`/`push_client.py`.
4. **Hub production hardening:** Postgres + Alembic for the hub schema (it
   bootstraps via `create_all` today), HTTPS/reverse-proxy deployment doc,
   rate limiting on `/portal/login`, ingest-token rotation flow, admin audit
   log, and real secrets management. Demo secrets (`demo-admin` etc.) exist
   only in compose defaults and helper scripts — never in library code.
5. **Portal v2 (product decisions pending):** charts/trends, CSV export,
   supplier password self-service/reset, per-supplier department scoping UI.
6. **Client-side upsell hook:** on-prem consoles could surface "your store can
   opt into supplier sharing" — connects the hub to the module-SKU sales motion.

**Debts / cautions**
- The trial-stamp and licensing cautions from the previous handover still stand.
- `configure_and_sync.py` forces `stock.move.state='done'` via `write` — fine
  for demo seeding, not how real Odoo confirms moves (real flows use
  `_action_done`); do not reuse that shortcut outside the demo.
- Supplier sessions are 8h HMAC tokens with no revocation list — acceptable for
  pilot, listed under hardening.
- `.hub_state.json` (created by `bootstrap_hub.py`) holds a live ingest token —
  now git-ignored (added alongside this handover).

---

## 8. Asset inventory (this chapter)

```
oasis_hub/                      the Cloud Hub (server-side, never in client zip)
  app.py                        FastAPI wiring, /portal-app mount, / redirect
  models.py db.py               schema + session (create_all bootstrap)
  visibility.py                 THE privacy filter — all portal reads pass here
  licensing.py                  online issuer (wraps OfflineLicenseManager.build_key)
  security.py tokens.py         3 auth walls; dependency-free signed sessions
  routers/{admin,ingest,portal}.py
  portal_web/index.html         supplier web app (single file, no build)
  Dockerfile                    slim hub image (no torch/ODBC)
connectors/odoo/                (server/ERP-side, never in client zip)
  oasis_connector/              installable Odoo 16 addon
    mapping.py push_client.py   PURE core — reused by every future ERP
    models/ data/ views/ security/ static/description/
  xmlrpc_sync.py                no-addon backfill via Odoo External API
  bootstrap_hub.py configure_and_sync.py
  docker-compose.odoo.yml INTEGRATION.md README.md
tests/test_hub_visibility.py    14 — the privacy spec
tests/test_hub_api.py            9 — e2e vertical slice + auth walls + portal serving
tests/test_odoo_connector.py    14 — mapping/contract/push/manifest + 2 e2e
oasis/logic/license_manager.py  + build_key() (in-memory issue)
entrypoint.py                   + --mode hub
.env.example                    + hub secrets section
dist/OASIS_v2.3.0.zip           145-file / 0.5 MB client release (also on Desktop)
```

Memory: `oasis-cloud-hub.md` in the assistant memory directory tracks this
chapter; `MEMORY.md` indexes it.

---

## 9. Bottom line

The online expansion is **architecturally complete and functionally proven** at
every level our environment allowed: the privacy model is enforced in one
audited chokepoint and demonstrated through a real browser; licensing is now a
server capability with the salt properly caged; the Odoo connector exists in
both marketplace-addon and zero-install forms sharing one tested core; and the
supplier-facing product — Retail Central Intelligence — is a real website a
supplier can log into today. The single outstanding proof, the containerised
real-Odoo run, is blocked by a machine-local Docker fault with a written,
committed, three-command procedure waiting on the other side. Sell it as: *the
online platform works; the last demo is an ops errand, not an engineering
risk.*
