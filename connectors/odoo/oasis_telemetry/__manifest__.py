{
    "name": "OASIS Telemetry",
    "version": "16.0.2.0.0",
    "summary": "Stream opt-in stock movement to the OASIS Cloud Hub.",
    "description": """
OASIS Telemetry
===============
Streams opt-in stock-movement telemetry — POS sales, goods receipts and
(optionally) stock-on-hand snapshots — from this Odoo instance to the OASIS
Cloud Hub, which powers the OASIS supplier portal and Retail Central
Intelligence.

* **Nothing is sent until you enable it** and provide a store ingest token
  issued by OASIS. You choose which of the three feeds leave your system.
* Zero extra Python dependencies — pushes over HTTPS with the standard library.
* Idempotent, batched and resumable, so it is safe to run on a schedule and a
  first sync on years of history cannot swamp the worker.
* Point of Sale is OPTIONAL. Install it and till sales stream from the POS
  itself; without it, sell-through is read from the stock moves every Odoo
  sale produces anyway, whatever the front end.

This is a SEPARATE purchase from OASIS Transfers and OASIS Replenishment, and
needs neither. A client who only wants OASIS to move stock between their own
stores never has to install it.
""",
    "author": "iLink",
    "website": "https://www.oasis-systems.example",
    "category": "Inventory/Inventory",
    "license": "OPL-1",
    # `stock` because the receipt and on-hand feeds read stock moves and
    # quants. NOT point_of_sale: that was a hard dependency purely to read
    # pos.order.line, which locked out every Odoo retailer not running Odoo
    # POS. It is detected at runtime instead.
    "depends": ["oasis_connector", "stock"],
    "data": [
        "security/ir.model.access.csv",
        "data/ir_cron.xml",
        "views/res_config_settings_views.xml",
    ],
    "installable": True,
    "application": False,
    "auto_install": False,
}
