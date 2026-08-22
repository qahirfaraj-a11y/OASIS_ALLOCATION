"""Point of Sale is optional, and a till sale must not be streamed twice.

`point_of_sale` was a hard dependency purely so `pos.order.line` could be read
here, which locked every Odoo retailer not running Odoo POS out of installing a
module whose headline feature is stock transfers.

Decoupling exposed a double count: with POS installed, a till sale went to the
hub as `pos.order.line` AND again as the customer-bound stock move its picking
creates. Both feeds default on and carry different source_refs, so the hub
could not dedupe them.
"""

from odoo.tests.common import tagged

from .common import OasisCase


@tagged("post_install", "-at_install")
class TestPosIsOptional(OasisCase):

    def setUp(self):
        super().setUp()
        self.sync = self.env["oasis.sync"]

    def test_pos_presence_is_detected_from_the_registry(self):
        self.assertEqual(self.sync._pos_installed(),
                         "pos.order.line" in self.env)

    def test_collecting_sales_never_raises_without_pos(self):
        """The whole point of the decoupling: this must degrade, not explode."""
        if self.sync._pos_installed():
            self.skipTest("POS is installed in this database")
        movements, watermark, more = self.sync._collect_sales("1970-01-01 00:00:00")
        self.assertEqual(movements, [])
        self.assertFalse(more)
        self.assertEqual(watermark, "1970-01-01 00:00:00",
                         "the watermark moved on a feed that read nothing")

    def test_receipts_still_collect_without_pos(self):
        movements, _, _ = self.sync._collect_receipts("1970-01-01 00:00:00")
        self.assertIsInstance(movements, list)

    def test_sales_are_not_counted_from_pos_when_pos_is_absent(self):
        if self.sync._pos_installed():
            self.skipTest("POS is installed in this database")
        self.assertFalse(self.sync._sales_counted_from_pos())

    def test_the_exclusion_IS_active_when_pos_is_present(self):
        """The other half, and the one that makes the trap test meaningful.

        With POS installed and the sales feed on (its default), customer moves
        must be excluded from the receipts feed — otherwise every till sale is
        streamed to the hub twice. If this is False on a POS database, the
        double count is back and
        TestReceiptDomainKeepsNullPickings is passing vacuously.
        """
        if not self.sync._pos_installed():
            self.skipTest("POS is not installed in this database")
        self.assertTrue(self.sync._sales_counted_from_pos())

    def test_switching_the_sales_feed_off_hands_sell_through_back_to_moves(self):
        """With the till feed off, customer moves are the ONLY record of a
        sale, so they must stop being excluded."""
        self.env["ir.config_parameter"].sudo().set_param(
            "oasis.send_sales", "False")
        self.assertFalse(self.sync._sales_counted_from_pos())

    def test_streaming_is_opt_in(self):
        """Nothing leaves the system until an operator turns it on."""
        self.env["ir.config_parameter"].sudo().set_param("oasis.enabled", "False")
        self.assertEqual(self.sync.run_sync(), {"skipped": True})


@tagged("post_install", "-at_install")
class TestReceiptDomainKeepsNullPickings(OasisCase):
    """The dotted-domain trap, pinned.

    A dotted domain walks the relation, so `picking_id.pos_order_id = False`
    silently drops every move whose picking_id is NULL — the join has nothing
    to walk. Those are ordinary sales. Measured on the depot: 240,962 of
    240,966 customer moves have no picking, and excluding them zeroed demand
    outright. The guarded `OR picking_id = False` form is what makes it safe.
    """

    def test_a_move_with_no_picking_is_still_collected(self):
        sync = self.env["oasis.sync"]
        customers = self.env.ref("stock.stock_location_customers")
        self._stock(self.product, self.wh_a, 10.0)

        move = self.env["stock.move"].create({
            "name": "OASIS test sale",
            "product_id": self.product.id,
            "product_uom": self.product.uom_id.id,
            "product_uom_qty": 3.0,
            "location_id": self.wh_a.lot_stock_id.id,
            "location_dest_id": customers.id,
        })
        move._action_confirm()
        move._action_assign()
        move.move_line_ids.qty_done = 3.0
        move._action_done()
        self.assertFalse(move.picking_id, "fixture built the wrong shape")

        movements, _, _ = sync._collect_receipts("1970-01-01 00:00:00")
        refs = {m["source_ref"] for m in movements}
        self.assertIn("odoo:stock.move:%d" % move.id, refs,
                      "a picking-less customer move was dropped — this is the "
                      "dotted-domain bug that zeroed all demand")

    def test_that_move_is_classified_as_a_sale(self):
        from odoo.addons.oasis_connector import mapping
        movement = mapping.map_stock_move(
            {"id": 1, "product_qty": 3.0, "date": "2026-01-01 00:00:00",
             "location_usage": "internal", "location_dest_usage": "customer"},
            {"default_code": "X", "display_name": "X"})
        self.assertEqual(movement["movement_type"], mapping.SALE)
