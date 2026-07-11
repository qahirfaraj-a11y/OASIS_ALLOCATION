# OASIS — Odoo Connector

Streams opt-in stock-movement telemetry from Odoo to the **OASIS Cloud Hub**
(`oasis_hub`), which powers OASIS ordering/transfer intelligence and the
supplier-facing Retail Central Intelligence portal.

There are two ways to run it. Both use the **same** pure mapping and push code,
so the payloads are identical.

```
connectors/odoo/
├── oasis_connector/          # ← the installable Odoo addon (Apps-store ready)
│   ├── __manifest__.py
│   ├── mapping.py            #   PURE: Odoo record → hub MovementIn (no odoo import)
│   ├── push_client.py        #   PURE: batched, idempotent HTTPS push (stdlib only)
│   ├── models/               #   Odoo-side: settings + cron sync engine
│   ├── data/ir_cron.xml      #   30-min scheduled push
│   ├── views/…               #   Settings → OASIS Connector
│   └── static/description/    #   Apps-store listing
└── xmlrpc_sync.py            # ← standalone backfill via Odoo External API (no install)
```

## Option A — install the addon (production)

1. Copy `oasis_connector/` into your Odoo `addons/` path.
2. Update the apps list and install **OASIS Retail Intelligence Connector**.
3. **Settings → OASIS Connector**: paste your **Hub URL**, **Store Ingest Token**
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

`oasis_connector/` is a standard Odoo 16 addon and can be submitted to the Odoo
Apps store as-is. Swap `static/description/banner.png` for final artwork before
submission. The same pattern (pure `mapping`/`push_client` + a thin platform
adapter) is what the Zoho, Sage, and Tally connectors will reuse.
```
