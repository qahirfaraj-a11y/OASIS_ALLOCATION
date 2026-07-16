{
    "name": "OASIS Retail Intelligence Connector",
    "version": "16.0.1.1.0",
    "summary": "Stream stock movement to OASIS — algorithmic retail intelligence.",
    "description": """
OASIS Retail Intelligence Connector
===================================
Securely streams opt-in stock-movement telemetry (POS sales, goods receipts,
and stock-on-hand snapshots) from this Odoo instance to the OASIS Cloud Hub,
powering the OASIS ordering, transfer, and Retail Central Intelligence services.

* Zero extra Python dependencies — pushes over HTTPS with the standard library.
* You control what leaves your system: nothing is sent until you enable it and
  provide a store ingest token issued by OASIS.
* Idempotent, batched, and resumable — safe to run on a schedule.

Configure under Settings → OASIS Connector.
""",
    "author": "iLink",
    "website": "https://www.oasis-systems.example",
    "category": "Inventory/Inventory",
    "license": "OPL-1",
    "depends": ["stock", "point_of_sale"],
    "data": [
        "security/ir.model.access.csv",
        "data/ir_cron.xml",
        "views/res_config_settings_views.xml",
        "views/oasis_menus.xml",
    ],
    "assets": {
        "web.assets_backend": [
            "oasis_connector/static/src/js/oasis_embed.js",
            "oasis_connector/static/src/xml/oasis_embed.xml",
        ],
    },
    "images": ["static/description/banner.png"],
    "installable": True,
    "application": True,
}
