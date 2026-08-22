"""Fixtures for the telemetry tests.

Deliberately a copy of the transfers fixtures rather than a shared import:
these modules are sold and installed independently, so a test in one must
never need the other to be present. A shared test helper would quietly
reintroduce the dependency the split exists to remove.
"""

from odoo.tests.common import TransactionCase


class OasisCase(TransactionCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company = cls.env.company
        cls.Quant = cls.env["stock.quant"]

        cls.wh_a = cls.env["stock.warehouse"].create({
            "name": "OASIS Telemetry Test A",
            "code": "OTL-A",
            "company_id": cls.env.company.id,
        })
        cls.product = cls.env["product.product"].create({
            "name": "OASIS Telemetry Widget",
            "default_code": "OTL-WIDGET",
            "type": "product",
            "list_price": 100.0,
            "standard_price": 60.0,
        })

    @classmethod
    def _stock(cls, product, warehouse, qty):
        cls.Quant._update_available_quantity(
            product, warehouse.lot_stock_id, qty)
