"""Shared fixtures for the OASIS Replenishment tests.

These run inside Odoo's own test framework, which is the point: everything else
that tests this addon drives it from OUTSIDE over XML-RPC against a depot that
has to be seeded first. Neither notices when an Odoo version bump changes a
signature, renames a view attribute, or tightens a constraint — the module
simply stops working for the next customer who upgrades.

Fixtures build their own warehouse, supplier and product, so nothing here
depends on the depot existing.
"""

from odoo.tests.common import TransactionCase


class OasisOrderCase(TransactionCase):
    """One store, one supplier, one product, and a helper to queue a line."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        cls.Suggestion = cls.env["oasis.order.suggestion"]

        # FIVE CHARACTERS. stock.warehouse.code is size=5, and Odoo TRUNCATES
        # rather than refusing — so "OAS-S1" and "OAS-S2" both become "OAS-S"
        # and the second create dies on the (code, company) unique constraint,
        # in setUpClass, taking every test in the class with it.
        cls.wh = cls._make_warehouse("OAS-1", "OASIS Test Store")
        cls.wh_other = cls._make_warehouse("OAS-2", "OASIS Test Store 2")

        cls.supplier = cls.env["res.partner"].create({
            "name": "OASIS Test Supplier",
            "supplier_rank": 1,
        })
        cls.supplier_b = cls.env["res.partner"].create({
            "name": "OASIS Other Supplier",
            "supplier_rank": 1,
        })
        cls.product = cls.env["product.product"].create({
            "name": "OASIS Test Widget",
            "default_code": "OAS-WIDGET",
            "type": "product",
            "list_price": 100.0,
            "standard_price": 60.0,
        })
        cls.product_b = cls.env["product.product"].create({
            "name": "OASIS Test Gadget",
            "default_code": "OAS-GADGET",
            "type": "product",
            "list_price": 50.0,
            "standard_price": 30.0,
        })

    @classmethod
    def _make_warehouse(cls, code, name, company=None):
        return cls.env["stock.warehouse"].create({
            "name": name,
            "code": code,
            "company_id": (company or cls.env.company).id,
        })

    def _suggest(self, qty=10.0, product=None, partner=None, warehouse=None,
                 unit_cost=60.0, **kw):
        vals = {
            "product_id": (product or self.product).id,
            "quantity": qty,
            "partner_id": (partner or self.supplier).id,
            "warehouse_id": (warehouse or self.wh).id,
            "unit_cost": unit_cost,
            "value": qty * unit_cost,
            "reason": "test",
        }
        vals.update(kw)
        return self.Suggestion.create(vals)

    def _approved(self, qty=5):
        """A suggestion that has become a draft purchase order."""
        s = self._suggest(qty=qty)
        s.action_approve()
        return s

    @staticmethod
    def _row(item_code, store_code, qty=5.0, **kw):
        """A row in the shape push_order_suggestions sends over RPC."""
        row = {
            "item_code": item_code,
            "store_code": store_code,
            "supplier_name": "OASIS Test Supplier",
            "quantity": qty,
            "unit_cost": 60.0,
            "value": qty * 60.0,
            "reason": "because",
            "current_stock": 3.0,
            "avg_daily_sales": 2.0,
            "days_cover": 1.5,
            "lead_time_days": 7.0,
            "target_cover_days": 14.0,
            "on_order_qty": 0.0,
            "on_order_eta_days": 999.0,
            "is_fresh": False,
            "pack_size": 1.0,
            "supplier_min_units": 0.0,
            "supplier_min_value": 0.0,
        }
        row.update(kw)
        return row
