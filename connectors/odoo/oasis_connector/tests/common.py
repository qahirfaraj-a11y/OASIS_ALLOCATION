"""Shared fixtures for the OASIS connector's Odoo-side tests.

These run inside Odoo's own test framework, which is the point: everything
else that tests this addon (the pytest suite, devkit/test_transfer_in_odoo.py)
drives it from OUTSIDE over XML-RPC against a depot that has to be seeded
first. Neither notices when an Odoo version bump changes a signature, renames
a view attribute, or tightens a constraint — the module simply stops working
for the next customer who upgrades.

Fixtures build their own warehouses, products and stock, so nothing here
depends on the 14-store depot existing.
"""

from odoo.tests.common import TransactionCase


class OasisCase(TransactionCase):
    """Two warehouses in one company, a stocked product, and a helper to queue."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        cls.Suggestion = cls.env["oasis.transfer.suggestion"]
        cls.Quant = cls.env["stock.quant"]

        cls.wh_a = cls._make_warehouse("OAS-A", "OASIS Test A")
        cls.wh_b = cls._make_warehouse("OAS-B", "OASIS Test B")

        cls.product = cls.env["product.product"].create({
            "name": "OASIS Test Widget",
            "default_code": "OAS-WIDGET",
            "type": "product",
            "list_price": 100.0,
            "standard_price": 60.0,
        })

    @classmethod
    def _make_warehouse(cls, code, name, company=None):
        return cls.env["stock.warehouse"].create({
            "name": name,
            "code": code,
            "company_id": (company or cls.env.company).id,
        })

    @classmethod
    def _stock(cls, product, warehouse, qty):
        """Put real, unreserved quantity into a warehouse's stock location."""
        cls.Quant._update_available_quantity(
            product, warehouse.lot_stock_id, qty)

    def _suggest(self, qty=10.0, frm=None, to=None, product=None, **kw):
        vals = {
            "product_id": (product or self.product).id,
            "quantity": qty,
            "from_warehouse_id": (frm or self.wh_a).id,
            "to_warehouse_id": (to or self.wh_b).id,
            "kind": "pull",
            "reason": "test",
        }
        vals.update(kw)
        return self.Suggestion.create(vals)

    @staticmethod
    def _row(item_code, from_code, to_code, qty=5.0, **kw):
        """A row in the shape push_transfer_suggestions sends over RPC."""
        row = {
            "item_code": item_code,
            "from_code": from_code,
            "to_code": to_code,
            "quantity": qty,
            "kind": "pull",
            "reason": "because",
            "value": 500.0,
            "donor_ads": 2.0,
            "recipient_ads": 3.0,
            "donor_cover": 40.0,
            "recipient_cover": 2.0,
            "relief_days": 7.0,
            "is_fresh": False,
        }
        row.update(kw)
        return row
