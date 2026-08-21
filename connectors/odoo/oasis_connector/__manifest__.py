{
    "name": "OASIS Retail Intelligence Connector",
    "version": "16.0.1.4.0",
    "summary": "Stream stock movement to OASIS — algorithmic retail intelligence, "
                "live inside Odoo.",
    "description": """
OASIS Retail Intelligence Connector
===================================
Securely streams opt-in stock-movement telemetry (POS sales, goods receipts,
and stock-on-hand snapshots) from this Odoo instance to the OASIS Cloud Hub,
powering the OASIS ordering, transfer, and Retail Central Intelligence services
— and brings OASIS's transfer and replenishment decisions into Odoo as a
review queue you approve into native documents.

* Zero extra Python dependencies — pushes over HTTPS with the standard library.
* Point of Sale is OPTIONAL. Install it and till sales stream from the POS
  itself; without it, sell-through is read from the stock moves every Odoo
  sale produces anyway, whatever the front end.
* You control what leaves your system: nothing is sent until you enable it and
  provide a store ingest token issued by OASIS.
* Idempotent, batched, and resumable — safe to run on a schedule.
* OASIS → Transfers lists what it would move and WHY — days of cover at both
  ends, the value at risk, and which job it serves. Approve a suggestion and
  it becomes a draft internal transfer; nothing is reserved and no stock moves
  until your team confirms it in Inventory.
* Perishables are surfaced but never queued automatically: transit costs shelf
  life, so a fresh line is only ever moved by a deliberate human decision.

Configure under Settings → OASIS Connector.
""",
    # TODO before submitting to apps.odoo.com: replace with iLink's real,
    # working support/marketing URL. apps.odoo.com requires a live publisher
    # website — a placeholder here will fail listing review.
    "author": "iLink",
    "website": "https://www.oasis-systems.example",
    "category": "Inventory/Inventory",
    "license": "LGPL-3",
    # `stock` ONLY. point_of_sale used to be a hard dependency purely so that
    # pos.order.line could be read by the telemetry sync — which locked every
    # Odoo retailer not running Odoo POS out of installing a module whose
    # headline feature is stock transfers. POS is now detected at runtime; with
    # it, till sales stream from pos.order.line, and without it sell-through
    # comes from customer stock moves, which every Odoo sale produces anyway.
    "depends": ["stock"],
    "data": [
        "security/ir.model.access.csv",
        "security/oasis_security.xml",
        "data/ir_cron.xml",
        "views/res_config_settings_views.xml",
        "views/oasis_transfer_views.xml",
        "views/oasis_menus.xml",
    ],
    # No assets. The bundle used to carry an `oasis_embed` client action that
    # rendered an OASIS console in an iframe; it registered itself on every
    # backend page, so the action stayed reachable long after its menus were
    # deleted. See oasis_sync.py.
    "images": ["static/description/banner.png"],
    "installable": True,
    "application": True,
}
