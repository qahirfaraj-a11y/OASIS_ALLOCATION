{
    "name": "OASIS Transfers",
    "version": "16.0.2.0.0",
    "summary": "Review what OASIS would move between your stores, and approve "
               "it into native internal transfers.",
    "description": """
OASIS Transfers
===============
**OASIS → Transfers → Suggestions** lists what OASIS would move between your
stores and, for every line, **why**: days of cover at both ends, the value at
risk, and which job the movement serves — plugging a gap before a store runs
out, or clearing capital that is sitting still.

* Approving a suggestion creates a **draft** internal transfer, grouped one
  picking per route. Nothing is reserved and no stock moves until your team
  confirms it in Inventory.
* Approving a line the donor can no longer cover is refused **before** anything
  is created, naming the product, the gap and the fix — rather than failing
  later as an Odoo reservation error that never mentions OASIS.
* Perishables are surfaced but never queued automatically: transit costs shelf
  life, so a fresh line is only ever moved by a deliberate human decision.
* Cancel or delete a transfer and its suggestion returns to the queue, so the
  queue never reports a movement as handled when no stock moved.

The analysis runs in OASIS, not inside Odoo. What lands here is the decision
and the reasoning behind it, in a document your warehouse team already knows
how to work. **Requires a reachable OASIS instance** — without one the queue
stays empty and Refresh reports that it could not connect.

Installs and runs on its own. It does not require OASIS Telemetry or OASIS
Replenishment; install Replenishment as well and the two cooperate.
""",
    "author": "iLink",
    "website": "https://www.oasis-systems.example",
    "category": "Inventory/Inventory",
    "license": "LGPL-3",
    "depends": ["oasis_connector", "stock"],
    "data": [
        "security/ir.model.access.csv",
        "security/oasis_security.xml",
        "views/oasis_transfer_views.xml",
        "views/oasis_menus.xml",
        "views/res_config_settings_views.xml",
    ],
    "installable": True,
    "application": False,
    "auto_install": False,
}
