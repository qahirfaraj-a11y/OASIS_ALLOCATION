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
app appears in Odoo's own app switcher carrying the review queues for whichever
modules are installed. The consoles are deliberately NOT embedded; see step 5.

The addons are separable on purpose: `oasis_transfers`, `oasis_telemetry` and
(next) `oasis_ordering` each install on their own against a shared
`oasis_connector` base, so a client buys only what they use.

## The workflow at a glance

| # | Step | Command | Result |
|---|------|---------|--------|
| 1 | Start the stack | `docker compose -f docker-compose.odoo.yml up -d --build` | Postgres, Odoo (+connector installed), and the hub are running |
| 2 | Provision the hub | `python bootstrap_hub.py` | tenant, store, **ingest token**, supplier, ownership, consent |
| 3 | Wire + seed Odoo | `python configure_and_sync.py` | addon pointed at hub, sample Odoo movement created, first sync pushed |
| 4 | See it live (portal) | open `http://localhost:8700/portal-app/` | log in **COKE / demo123** → live Odoo movement |
| 5 | See it live (in Odoo) | log into Odoo, click the **OASIS** app | Transfers → Suggestions: the plan, with its reasoning, approvable into native documents |

That's the whole loop. Steps 2–3 are one-time; after that the telemetry addon's
30-minute cron keeps the hub current on its own. Step 5 needs a reachable OASIS
for the Refresh button — see "OASIS inside Odoo" below.

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
  installs the OASIS addons, then exits.
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

The `oasis_transfers` addon adds a **Transfers** section to the OASIS app,
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

## Before a customer pilot — the read-only preflight

**Run this first, against the customer's own Odoo, before anything is
installed or agreed.** It writes nothing — `search_count` and `search_read`
only, enforced by a test that fails if any write method is ever reached — so it
is safe against production.

```bash
OASIS_ERP=odoo ODOO_URL=https://their.odoo ODOO_DB=theirdb \
  ODOO_USER=... ODOO_PASSWORD=... \
  python entrypoint.py --mode odoo-preflight
```

It answers the questions that decide whether a pilot is safe, each of which
would otherwise be discovered by the customer:

| check | why it decides the pilot |
|---|---|
| companies | Odoo cannot confirm an internal transfer between two companies. Approval refuses those routes, so stores in different companies never exchange stock |
| warehouse codes | OASIS keys stores on `code`; a warehouse without one falls back to a database id no operator recognises, and duplicates collapse two sites into one org |
| every read vs its cap | under the cap the numbers are right, over it they are **quietly wrong**. The two that matter answer "what is already coming" — truncate those and the scan re-proposes stock in flight, so approving both ships twice |
| projected scan time | the scan reads sites one after another while tills sell, so the plan is a composite of N instants |
| receipt attribution | per-store delivery cadence needs receipts that name a supplier; without it σ stays supplier-level |

Exit code is 0 for PASS/WARN and 2 for FAIL, so it drops into a pipeline.

Two things it will tell you that are easy to get wrong by hand. The
site-scoped reads are judged **per warehouse**, on the busiest site — counting
a whole chain's rows against a per-site cap reads as a breach on a perfectly
healthy instance (28,125 receipts company-wide on the depot against a 20,000
cap, versus 2,971 at the busiest site). And the scan projection is an **upper
bound**: the first site read pays one-off costs a real scan amortises.

## Derive the supplier rhythm from the customer's own receipts

**Run this after the preflight and before the first scan.** Every horizon the
transfer engine uses — the fill target and now σ, the safety floor — comes from
LATA's measured supplier rhythm. `lata_shield` only ENRICHES that rhythm; the
only thing that ever produced it scanned `po_*.xlsx` off disk. A customer whose
history lives in Odoo has nothing to enrich, so the engine falls back to a flat
14 days and says so on every scan.

```bash
OASIS_ERP=odoo python entrypoint.py --mode odoo-rhythm --history-days 730
python -m oasis.logic.lata_shield --data-dir ./oasis/data
```

Read-only against Odoo; it writes only into OASIS's own data directory. Proven
end to end against a real Odoo — seeded cadences of 7/1/14 days came back as
7/1/14, with lead times **measured** from PO date to receipt date rather than
taken from the supplier's stated `delay`, which is the number LATA exists to
distrust.

What it produces, and what each is for:

| file | purpose |
|---|---|
| `supplier_patterns_2025.json` | the rhythm the engine reads |
| `supplier_delivery_gaps.json` | raw gaps, so `lata_shield` measures real variance |
| `supplier_patterns_by_store.json` | cadence per (store, supplier) |

**It refuses to replace richer data with thinner.** Run it against an instance
whose receipts are bare stock moves and it derives nothing — writing that would
erase whatever rhythm was already there, silently, and the engine would just
start answering 14. It refuses, explains why, backs up what it replaces, and
writes atomically. `force=True` is the deliberate override.

**`--history-days` defaults to 730, not the 30 that `--days` uses.** A
fortnightly supplier shows two gaps in a month; that is an anecdote, not a
rhythm.

If it derives nothing, the cause is almost always that receipts were posted as
bare stock moves rather than created from purchase orders — so nothing records
who delivered. The preflight's *receipt attribution* check tells you this
before you get here.

## Addon test suite (runs inside Odoo)

The addons carry their own tests, in Odoo's own framework. Everything else that
exercises them drives from OUTSIDE over XML-RPC against a depot that has to be
seeded first, so none of it notices when an Odoo version bump changes a
signature, renames a view attribute or tightens a constraint — the module just
stops working for the next customer who upgrades.

Run them on throwaway databases, never on `oasis`. **Test each module the way a
customer will actually install it**, because that is the guarantee the split
exists to provide:

```bash
# transfers on its own — no telemetry, no POS
docker exec oasis-odoo-odoo-1 odoo -i oasis_transfers -d tmp_xfer \
  --db_host=odoo-db --db_user=odoo --db_password=odoo \
  --stop-after-init --without-demo=all --no-http --test-enable \
  --test-tags /oasis_connector,/oasis_transfers --log-level=test
```

```bash
# telemetry on its own
docker exec oasis-odoo-odoo-1 odoo -i oasis_telemetry -d tmp_tele \
  --db_host=odoo-db --db_user=odoo --db_password=odoo \
  --stop-after-init --without-demo=all --no-http --test-enable \
  --test-tags /oasis_connector,/oasis_telemetry --log-level=test
```

```bash
# everything together
docker exec oasis-odoo-odoo-1 odoo -i oasis_connector,oasis_telemetry,oasis_transfers -d tmp_all \
  --db_host=odoo-db --db_user=odoo --db_password=odoo \
  --stop-after-init --without-demo=all --no-http --test-enable \
  --test-tags /oasis_connector,/oasis_telemetry,/oasis_transfers --log-level=test
```

Current counts: base alone 7, transfers alone 52, telemetry alone 20, all three
65 — `0 failed, 0 error(s)` in every combination. Some tests skip by design;
they are complementary pairs asserting the with-sibling and without-sibling
behaviour. Drop the databases afterwards.

**Also run it with `point_of_sale` added**, for telemetry and for transfers.
POS is optional and the two configurations take different paths through the
sync: with POS the customer-move exclusion is active, without it those moves
are the only record of a sale.

**Adding a module means adding a mount** in `docker-compose.odoo.yml`. Odoo
does not error on a module it cannot see — it is simply absent from the list,
and `-i` silently installs nothing.

From Git Bash, prefix with `MSYS_NO_PATHCONV=1` or a `--test-tags` value like
`/oasis_transfers` is rewritten into a Windows path and **zero tests run while
still reporting success** — the log says `0 failed ... of 0 tests`, which is
easy to misread.

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

1. **Real Odoo** — install the OASIS addons into the customer's Odoo `addons/`
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
