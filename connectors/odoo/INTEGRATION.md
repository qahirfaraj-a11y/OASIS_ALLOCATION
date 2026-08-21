# OASIS ⇄ Odoo — full integration runbook

This is the end-to-end procedure to go from nothing to **seeing OASIS live
against a real Odoo instance** — a supplier logging into the Retail Central
Intelligence portal and watching their own products move, sourced from Odoo.

```
   Odoo (stock; POS optional) ─addon push─▶ OASIS Cloud Hub ─▶ Supplier Portal
   :8069                                     :8700               :8700/portal-app
        ▲  │                                     ▲
        │  └── OASIS app menu ──▶ Transfers → Suggestions (review queue)
        │            ▲                    │
        │            │                    └── approve ──▶ draft internal transfer
        │      Refresh button ──▶ scan_service.py :8710 (on-prem OASIS)
        │ configure_and_sync.py             │ bootstrap_hub.py
        └── ir.config_parameter + seed      └── tenant/store/token/consent
```

Two things are live inside/around Odoo once this is set up: **data flows out**
(Odoo → hub → supplier portal, above) and **OASIS surfaces in** — an "OASIS"
app appears in Odoo's own app switcher with three menu entries (Intelligence,
Operations, Command Center) that open the real OASIS consoles live, embedded in
Odoo's content pane.

## The workflow at a glance

| # | Step | Command | Result |
|---|------|---------|--------|
| 1 | Start the stack | `docker compose -f docker-compose.odoo.yml up -d --build` | Postgres, Odoo (+connector installed), and the hub are running |
| 2 | Provision the hub | `python bootstrap_hub.py` | tenant, store, **ingest token**, supplier, ownership, consent |
| 3 | Wire + seed Odoo | `python configure_and_sync.py` | addon pointed at hub, sample Odoo movement created, first sync pushed |
| 4 | See it live (portal) | open `http://localhost:8700/portal-app/` | log in **COKE / demo123** → live Odoo movement |
| 5 | See it live (in Odoo) | log into Odoo, click the **OASIS** app | Intelligence / Operations / Command Center open embedded, live |

That's the whole loop. Steps 2–3 are one-time; after that the addon's 30-minute
cron keeps the hub current on its own. Step 5 needs the OASIS consoles running
and reachable from your browser — see "OASIS inside Odoo" below.

---

## Prerequisites

- **Docker Desktop running** (the engine, not just the CLI). Verify: `docker info`.
- Python 3 on the host (for the two helper scripts — stdlib only, no venv needed).
- Ports free: **8069** (Odoo), **8700** (hub).

## Step 1 — bring up the stack

From `connectors/odoo/`:

```bash
docker compose -f docker-compose.odoo.yml up -d --build
```

What happens:
- `odoo-db` (Postgres) starts and passes its health check.
- `odoo-init` (one-shot) creates the `oasis` database **with demo data** and
  installs the `oasis_connector` addon, then exits.
- `odoo` serves on http://localhost:8069 with the connector installed.
- `oasis-hub` builds from the slim `oasis_hub/Dockerfile` and serves on :8700.

First run pulls the Odoo image (~1.3 GB) and initialises the DB — allow a few
minutes. Follow progress with `docker compose -f docker-compose.odoo.yml logs -f odoo-init`.

## Step 2 — provision the hub

```bash
python bootstrap_hub.py
```

Registers `Acme Retail Group`, a store `ODOO-01`, a **Coca-Cola** supplier
(`COKE` / `demo123`) that owns `supplier_cd = SUP_COKE`, and a granted consent
(identity revealed). It mints the store's **ingest token**, prints it, and saves
it to `.hub_state.json`. Idempotent — safe to re-run.

## Step 3 — configure the addon + seed + sync

```bash
python configure_and_sync.py
```

Over Odoo's XML-RPC API this:
1. writes the addon settings (hub URL `http://oasis-hub:8700`, the ingest token,
   store code, enabled) — identical to filling in **Settings → OASIS Connector**;
2. seeds a Coca-Cola supplier + 4 products and posts real **done stock moves to a
   customer location** (sell-through) across the last 14 days;
3. calls `oasis.sync.run_sync()` inside Odoo, which maps and pushes to the hub.

> Doing it by hand instead? In Odoo: **Settings → OASIS Connector**, paste the
> Hub URL (`http://oasis-hub:8700` in-network, or `http://localhost:8700` from a
> browser test), the ingest token, and store code; tick **Enable** and
> **Sync now**. Then generate a POS sale or confirm a delivery.

## Step 4 — see OASIS live

Open **http://localhost:8700/portal-app/** and log in **COKE / demo123**.

You'll see KPI tiles (units sold, SKUs, stores, movements), a stores panel, and
a movement table — every row sourced from Odoo. The privacy contract is live: a
supplier sees only their own SKUs, only in consenting stores, identity masked
unless revealed.

## Step 5 — OASIS inside Odoo (Transfers)

The `oasis_connector` addon adds an **OASIS** app to Odoo's own app switcher,
with **Transfers → Suggestions**. That is the whole app, deliberately.

**The consoles are NOT embedded in Odoo, and must not be.** An earlier revision
put Intelligence, Operations and Command Center here as iframes, which shipped
the entire product into a window inside Odoo and handed every module to anyone
who installed the connector. The menus were deleted first; the client action,
its asset bundle and its three URL settings survived that deletion and left
`oasis.sync.open_console('intel')` callable over RPC by any internal user. All
of it is gone as of 16.0.1.3.0 — removing a menu hides an entrance, it does not
close a door.

What belongs in Odoo is the part an Odoo user does in Odoo: review what OASIS
proposes, and approve it into a native document. The intelligence that produced
the proposal stays outside, which is also what is being sold.

1. Post a plan to the queue:
   ```bash
   python connectors/odoo/push_transfer_suggestions.py --limit 200
   ```
2. In Odoo: **OASIS → Transfers → Suggestions**. Each line carries its
   reasoning — cover at both ends, value at risk, and which job it serves.
3. Approve one and it becomes a **draft** internal transfer, grouped one
   picking per route. Nothing is reserved and no stock moves until your team
   confirms it in Inventory.

The **Refresh from OASIS** button calls `scan_service.py` on-prem, so OASIS has
to be reachable from the Odoo container — see `oasis.scan_url`.

## Verify / troubleshoot

```bash
docker compose -f docker-compose.odoo.yml ps                 # all healthy?
curl http://localhost:8700/health                            # hub up
curl http://localhost:8069/web/health                        # odoo up
docker compose -f docker-compose.odoo.yml logs odoo-init     # addon install log
```

- **Portal empty?** Confirm step 3's sync result showed `accepted > 0`, and that
  the ownership key (`SUP_COKE`) matches the seeded products' supplier.
- **`oasis.sync` unknown model?** The addon didn't install — check `odoo-init`
  logs; re-run `... up -d` to retry the one-shot.
- **Reset everything:** `docker compose -f docker-compose.odoo.yml down -v`
  (the `-v` drops the DB + hub volumes) and delete `.hub_state.json`.

## Tear down

```bash
docker compose -f docker-compose.odoo.yml down       # stop, keep data
docker compose -f docker-compose.odoo.yml down -v    # stop, wipe data
```

---

## From demo to production

The demo uses one compose file and demo secrets. For a real deployment:

1. **Real Odoo** — install `oasis_connector/` into the customer's Odoo `addons/`
   (or submit it to the Odoo Apps store and install from there). No code change.
2. **Real hub** — run the hub on its own host with **strong secrets**
   (`OASIS_LICENSE_SALT`, `OASIS_HUB_ADMIN_KEY`, `OASIS_HUB_TOKEN_SECRET`) and a
   Postgres `OASIS_HUB_DB_URL`, behind HTTPS. The salt lives ONLY here.
3. **Per-customer onboarding** — for each store: `POST /admin/tenants`,
   `/admin/stores`, mint an ingest token, create the supplier accounts, declare
   ownership rules, and record each store's consent (`reveal_identity` per their
   contract). `bootstrap_hub.py` is the template for this.
4. **Configure the addon** in the customer's Odoo Settings with the hub's public
   URL + their ingest token + store code. The cron does the rest.
5. Other ERPs (Zoho, Sage, Tally) reuse the same `mapping.py` + `push_client.py`
   core behind a thin platform adapter — the hub and portal are unchanged.
