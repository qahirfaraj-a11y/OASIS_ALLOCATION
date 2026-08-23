{
    "name": "OASIS Replenishment",
    "version": "16.0.1.0.0",
    "summary": "Review what OASIS would buy, and approve it into native draft "
               "purchase orders.",
    "description": """
OASIS Replenishment
===================
**OASIS → Replenishment → Suggestions** lists what OASIS would order for each
store and, for every line, **why**: what is on the shelf today, how fast it
sells, when the supplier realistically delivers, and what is already on its way.

* Approving suggestions creates a **draft** purchase order, grouped one order
  per supplier per store. Nothing is sent and no money is committed until your
  buyer confirms it in Purchase.
* Every order is aimed at the ordering store's own receipt operation, so goods
  arrive where the demand was measured rather than at the default warehouse.
* A supplier's minimum order is checked against the lines you actually
  approved, **before** the order is created. Rejecting half a basket can put
  the rest under the supplier's minimum, and OASIS says so by name rather than
  letting you find out from the supplier.
* Cancel or delete a purchase order and its suggestions return to the queue, so
  the queue never reports a line as bought when nothing was ordered.

The analysis runs in OASIS, not inside Odoo. What lands here is the decision
and the reasoning behind it, in a document your buying team already knows how
to work. **Requires a reachable OASIS instance** — without one the queue stays
empty and Refresh reports that it could not connect.

Installs and runs on its own. It does not require OASIS Transfers or OASIS
Telemetry; install Transfers as well and the two cooperate.
""",
    "author": "iLink",
    "website": "https://www.oasis-systems.example",
    "category": "Inventory/Purchase",
    "license": "LGPL-3",
    # `purchase` and the OASIS base — nothing else. Not `stock`: purchase
    # already brings what is needed to receive goods, and a client buying
    # replenishment alone must not be made to carry the transfer machinery.
    # Emphatically not `oasis_transfers`; the two are sold separately.
    "depends": ["oasis_connector", "purchase"],
    "data": [
        "security/ir.model.access.csv",
        "security/oasis_security.xml",
        "views/oasis_order_views.xml",
        "views/oasis_menus.xml",
    ],
    "installable": True,
    "application": False,
    "auto_install": False,
}
