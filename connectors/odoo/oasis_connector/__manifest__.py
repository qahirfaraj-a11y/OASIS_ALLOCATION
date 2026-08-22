{
    "name": "OASIS Retail Intelligence",
    "version": "16.0.2.0.0",
    "summary": "Base for the OASIS modules — the app, the settings section, "
               "and nothing else.",
    "description": """
OASIS Retail Intelligence — base
================================
This module on its own does nothing visible except create the **OASIS** app and
the OASIS section in Settings. It exists so the OASIS modules can be bought,
installed and removed **independently**:

* **OASIS Transfers** (``oasis_transfers``) — review what OASIS would move
  between your stores and approve it into native internal transfers.
* **OASIS Replenishment** (``oasis_ordering``) — review what OASIS would order
  and approve it into native purchase orders.
* **OASIS Telemetry** (``oasis_telemetry``) — stream opt-in stock movement to
  the OASIS Cloud Hub, which powers the supplier portal.

Install only what you use. Install two and they cooperate; a bridge module
installs itself automatically and does nothing until both sides are present.

Nothing here reads or writes your stock, and nothing leaves your system.
""",
    # TODO before submitting to apps.odoo.com: replace with iLink's real,
    # working support/marketing URL. apps.odoo.com requires a live publisher
    # website — a placeholder here will fail listing review.
    "author": "iLink",
    "website": "https://www.oasis-systems.example",
    "category": "Inventory/Inventory",
    "license": "LGPL-3",
    # `base` ONLY. The base module must never drag in stock or purchase: a
    # client buying ordering alone should not be made to carry Inventory's
    # transfer machinery, and vice versa. Each feature module declares what it
    # actually needs.
    "depends": ["base"],
    "data": [
        "views/oasis_menus.xml",
        "views/res_config_settings_views.xml",
    ],
    "images": ["static/description/banner.png"],
    "installable": True,
    "application": True,
    "auto_install": False,
}
