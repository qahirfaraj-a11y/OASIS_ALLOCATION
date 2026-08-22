"""What the queue believes after a human touches the document it created.

A suggestion's job does not end when the draft picking exists. Everything here
was found by probing the live lifecycle, and every case below was wrong before
it was written down.
"""

from odoo.exceptions import UserError
from odoo.tests.common import tagged

from .common import OasisCase


@tagged("post_install", "-at_install")
class TestTheQueueLearnsItsDocumentDied(OasisCase):

    def setUp(self):
        super().setUp()
        self._stock(self.product, self.wh_a, 500.0)

    def _approved(self, qty=10):
        s = self._suggest(qty=qty)
        s.action_approve()
        self.assertEqual(s.state, "approved")
        return s

    def test_cancelling_the_transfer_returns_the_suggestion_to_the_queue(self):
        """It used to sit on `approved` forever, reporting a movement that
        never happened."""
        s = self._approved()
        s.picking_id.action_cancel()
        self.assertEqual(s.state, "new",
                         "a cancelled transfer still reads as approved")
        self.assertFalse(s.picking_id,
                         "the suggestion still points at a cancelled document")

    def test_deleting_the_transfer_returns_the_suggestion_to_the_queue(self):
        """The worse case: approved, and pointing at nothing at all."""
        s = self._approved()
        s.picking_id.unlink()
        self.assertEqual(s.state, "new")
        self.assertFalse(s.picking_id)

    def test_a_released_suggestion_can_be_worked_again(self):
        """The point of releasing it. While it was stuck on `approved`,
        action_reset refused to touch it, so the line could not be reopened by
        any route the operator had."""
        s = self._approved()
        s.picking_id.action_cancel()
        s.action_approve()          # must not raise
        self.assertEqual(s.state, "approved")
        self.assertTrue(s.picking_id)
        self.assertEqual(s.picking_id.state, "draft")

    def test_a_released_suggestion_is_cleared_by_the_next_scan(self):
        """Self-healing: a released row is pending again, so the next scan
        replaces it with one computed from current stock."""
        s = self._approved()
        s.picking_id.action_cancel()
        self.Suggestion.oasis_replace_queue(
            [self._row("OAS-WIDGET", "OAS-A", "OAS-B", qty=4)])
        self.assertFalse(s.exists(), "the released row survived a fresh scan")

    def test_a_DONE_transfer_is_left_alone(self):
        """Only a dead document releases. A completed one is the whole point."""
        s = self._approved()
        p = s.picking_id
        p.action_confirm()
        p.action_assign()
        for ml in p.move_ids.move_line_ids:
            ml.qty_done = ml.reserved_uom_qty or 10
        p._action_done()
        self.assertEqual(p.state, "done")
        self.assertEqual(s.state, "approved",
                         "a completed transfer was released back to the queue")
        self.assertTrue(s.picking_id)

    def test_cancelling_an_unrelated_picking_touches_nothing(self):
        s = self._approved()
        other = self.env["stock.picking"].create({
            "picking_type_id": self.wh_a.int_type_id.id,
            "location_id": self.wh_a.lot_stock_id.id,
            "location_dest_id": self.wh_b.lot_stock_id.id,
        })
        other.action_cancel()
        self.assertEqual(s.state, "approved")


@tagged("post_install", "-at_install")
class TestOneProductIsOneMoveLine(OasisCase):
    """A product can appear twice on a route — once pulled, once pushed.

    The picking is a physical document: one product going from A to B is one
    line of N units. Two lines made an operator work out for themselves that
    they were not a duplicate.
    """

    def setUp(self):
        super().setUp()
        self._stock(self.product, self.wh_a, 500.0)

    def test_pull_and_push_of_one_product_merge_into_one_line(self):
        batch = self._suggest(qty=5) | self._suggest(qty=7, kind="push")
        batch.action_approve()
        picking = batch.picking_id
        self.assertEqual(len(picking), 1)
        lines = picking.move_ids.filtered(
            lambda m: m.product_id == self.product)
        self.assertEqual(len(lines), 1, "the same product got two move lines")
        self.assertEqual(lines.product_uom_qty, 12,
                         "merging lost or invented units")

    def test_different_products_keep_their_own_lines(self):
        other = self.env["product.product"].create({
            "name": "OASIS Test Widget 2", "default_code": "OAS-W2",
            "type": "product"})
        self._stock(other, self.wh_a, 50.0)
        batch = self._suggest(qty=5) | self._suggest(qty=7, product=other)
        batch.action_approve()
        self.assertEqual(len(batch.picking_id.move_ids), 2)


@tagged("post_install", "-at_install")
class TestArchivedProducts(OasisCase):

    def test_an_archived_product_is_not_transferred(self):
        """Archiving is a range decision. Stock of a product that has left the
        range needs a write-off or a markdown, not a lorry — and a picking for
        an archived product is a document nobody can tidy up afterwards."""
        self._stock(self.product, self.wh_a, 100.0)
        s = self._suggest(qty=3)
        self.product.active = False
        with self.assertRaises(UserError) as caught:
            s.action_approve()
        self.assertIn("archived", str(caught.exception))
        self.assertEqual(s.state, "new")
        self.assertFalse(s.picking_id)
