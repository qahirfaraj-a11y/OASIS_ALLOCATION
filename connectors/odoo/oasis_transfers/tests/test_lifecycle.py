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

    def test_a_DONE_transfer_is_never_released_back_to_the_queue(self):
        """Only a DEAD document releases. A completed one is the whole point.

        This used to assert the row stayed on `approved`, which was the old
        behaviour rather than the intent: what matters is that a received
        transfer is not put back on the queue to be proposed again. It now
        reaches a real terminal state instead of sitting on `approved` for
        ever, so the assertion follows the intent.
        """
        s = self._approved()
        p = s.picking_id
        p.action_confirm()
        p.action_assign()
        for ml in p.move_ids.move_line_ids:
            ml.qty_done = ml.reserved_uom_qty or 10
        p._action_done()
        self.assertEqual(p.state, "done")
        self.assertEqual(s.state, "done",
                         "a completed transfer did not reach its terminal state")
        self.assertNotIn(s, self.Suggestion.search([("state", "=", "new")]),
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


@tagged("post_install", "-at_install")
class TestTheLifecycleEnds(OasisCase):
    """A completed transfer must FINISH, not merely leave the default view.

    Before this, a received transfer sat on `approved` for ever. It disappeared
    from the queue only because the default filter is state=new, so it had no
    ending — merely a hiding place — and "has this been done?" could only be
    answered from picking_state, a related field that goes blank the moment
    somebody deletes the picking.
    """

    def setUp(self):
        super().setUp()
        self._stock(self.product, self.wh_a, 500.0)

    def _receive(self, s):
        p = s.picking_id
        p.action_confirm()
        p.action_assign()
        for ml in p.move_ids.move_line_ids:
            ml.qty_done = ml.reserved_uom_qty or s.quantity
        p._action_done()
        return p

    def test_receiving_the_goods_completes_the_suggestion(self):
        s = self._suggest(qty=4)
        s.action_approve()
        assert s.state == "approved"
        p = self._receive(s)
        self.assertEqual(p.state, "done")
        self.assertEqual(s.state, "done",
                         "a received transfer is still pending approval")

    def test_a_completed_row_is_off_the_review_queue(self):
        s = self._suggest(qty=4)
        s.action_approve()
        self._receive(s)
        self.assertNotIn(s, self.Suggestion.search([("state", "=", "new")]))

    def test_a_completed_row_survives_the_next_scan(self):
        """It is the audit trail: what was proposed, approved, and landed."""
        s = self._suggest(qty=4)
        s.action_approve()
        self._receive(s)
        self.Suggestion.oasis_replace_queue(
            [self._row("OAS-WIDGET", "OAS-A", "OAS-B", qty=2)])
        self.assertTrue(s.exists(), "the record of a completed transfer was lost")
        self.assertEqual(s.state, "done")

    def test_a_completed_row_cannot_be_reset(self):
        """The stock has moved; re-queueing would propose it a second time."""
        s = self._suggest(qty=4)
        s.action_approve()
        self._receive(s)
        with self.assertRaises(UserError):
            s.action_reset()

    def test_deleting_the_picking_afterwards_does_not_reopen_it(self):
        """_release_from_dead_picking must not undo a movement that happened."""
        s = self._suggest(qty=4)
        s.action_approve()
        p = self._receive(s)
        self.assertEqual(s.state, "done")
        # a done picking cannot normally be deleted, but the guard must hold
        s._release_from_dead_picking("test")
        self.assertEqual(s.state, "done")
