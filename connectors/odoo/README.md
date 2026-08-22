# OASIS — Odoo modules

**Separate modules, bought and installed independently.** A client who wants
OASIS to move stock between their stores installs Transfers and nothing else. A
client who only wants the supplier-portal feed installs Telemetry and nothing
else. Install two and they cooperate; neither requires the other.

That flexibility is a design constraint, not an accident, and it is pinned by
tests: each module is installed **on its own** in CI-style runs, and
`test_the_modules_are_separable` fails the moment one feature module starts
depending on a sibling.

```
connectors/odoo/
├── oasis_connector/          # ← BASE. The OASIS app + the settings section.
│   │                         #   depends: base ONLY — no stock, no purchase.
│   ├── views/oasis_menus.xml #   the app root every feature hangs off
│   └── static/description/   #   Apps-store listing
├── oasis_transfers/          # ← review queue → draft internal transfer
│   │                         #   depends: oasis_connector, stock
│   ├── models/               #   suggestion model, approval, picking hooks
│   ├── security/             #   access rules + the multi-company record rule
│   └── tests/                #   runs inside Odoo's own framework
├── oasis_telemetry/          # ← opt-in stream to the OASIS Cloud Hub
│   │                         #   depends: oasis_connector, stock (POS optional)
│   ├── mapping.py            #   PURE: Odoo record → hub MovementIn (no odoo import)
│   ├── push_client.py        #   PURE: batched, idempotent HTTPS push (stdlib only)
│   ├── data/ir_cron.xml      #   30-min scheduled push
│   └── models/               #   settings + cron sync engine
└── xmlrpc_sync.py            # ← standalone backfill via Odoo External API (no install)
```

Adding a module means adding a mount in `docker-compose.odoo.yml`. Odoo does
not error on a module it cannot see — it is simply absent from the list.

## Option A — install the addon (production)

1. Copy the modules you have bought into your Odoo `addons/` path.
   `oasis_connector/` is always required; add `oasis_transfers/` and/or
   `oasis_telemetry/`.
2. Update the apps list and install them. Installing a feature module pulls the
   base in automatically.
3. For telemetry — **Settings → OASIS**: paste your **Hub URL**, **Store Ingest Token**
   (issued by OASIS via `POST /admin/stores/{id}/ingest-token`), and **Store
   Code**; pick what to stream (sales / receipts / on-hand); tick **Enable**.
4. Press **Sync now**, or let the 30-minute cron keep OASIS current.

Zero extra Python dependencies — the push uses only the standard library.

## Option B — standalone XML-RPC backfill (evaluations, history seeding)

No module install required; talks to Odoo's public External API.

```bash
python -m connectors.odoo.xmlrpc_sync \
    --odoo-url https://my.odoo.example --db mydb \
    --user admin --password *** \
    --hub-url https://hub.oasis.example --ingest-token oist_xxx \
    --since 2026-06-01
```

## Field mapping

| Odoo source          | Hub movement_type | Ownership keys sent                         |
|----------------------|-------------------|---------------------------------------------|
| `pos.order.line`     | `sale`            | `supplier_cd` (vendor), `brand`, `department` (categ) |
| `stock.move` (vendor→internal) | `receipt` | same |
| `stock.move` (internal)        | `adjustment` | same |
| `stock.quant`        | `stock_on_hand`   | same |

The hub matches those ownership keys against each supplier's rules and only ever
shows a supplier their **own** products, in the stores that **consented** — see
`oasis_hub/visibility.py`. `source_ref` (e.g. `odoo:pos.order.line:42`) makes
every push idempotent.

## Marketplace listing

Each module is a standard Odoo 16 addon and can be listed separately, which is
the point: the Apps store is a shop window, and three focused listings say what
the product does better than one that does everything. Swap
`static/description/banner.png` for final artwork before submission, and settle
the licence first — LGPL-3 is freely redistributable, so a paid listing needs
OPL-1. The same pattern (pure `mapping`/`push_client` + a thin platform
adapter) is what the Zoho, Sage, and Tally connectors will reuse.
```
