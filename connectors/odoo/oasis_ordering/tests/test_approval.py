"""Approving a suggestion produces a correct draft purchase order.

The two failures worth guarding hardest are both silent: an order that lands at
the wrong warehouse (because picking_type_id is REQUIRED, so omitting it takes
a default rather than failing), and two order lines for the same product on one
document.
"""

from odoo.exceptions import UserError
from odoo.tests.common import tagged

from .common import OasisOrderCase


@tagged("post_install", "-at_install")
class TestApproval(OasisOrderCase):

    def test_approving_creates_a_draft_order_and_nothing_more(self):
        s = self._suggest(qty=5)
        s.action_approve()
        self.assertEqual(s.state, "approved")
        po = s.purchase_order_id
        self.assertTrue(po)
        self.assertEqual(po.state, "draft",
                         "OASIS confirmed an order — money committed without review")
        self.assertEqual(po.partner_id, self.supplier)

    def test_the_order_is_received_at_the_store_that_needed_it(self):
        """picking_type_id is REQUIRED on purchase.order, so it can never be
        empty — which means omitting it does not fail, it silently uses the
        DEFAULT warehouse. A PO computed from one store's demand then delivers
        to another, and nothing on the document says so.
        """
        s = self._suggest(qty=5)
        s.action_approve()
        self.assertEqual(s.purchase_order_id.picking_type_id,
                         self.wh.in_type_id,
                         "the goods are routed to the wrong warehouse")

    def test_one_product_is_one_order_line(self):
        a = self._suggest(qty=4)
        b = self._suggest(qty=6)
        (a | b).action_approve()
        po = a.purchase_order_id
        self.assertEqual(a.purchase_order_id, b.purchase_order_id,
                         "one supplier and one store should be one order")
        self.assertEqual(len(po.order_line), 1,
                         "the same product got two lines on one order")
        self.assertEqual(po.order_line.product_qty, 10)

    def test_two_suppliers_are_two_orders(self):
        a = self._suggest(qty=4)
        b = self._suggest(qty=4, partner=self.supplier_b, product=self.product_b)
        (a | b).action_approve()
        self.assertNotEqual(a.purchase_order_id, b.purchase_order_id)

    def test_two_stores_are_two_orders(self):
        a = self._suggest(qty=4)
        b = self._suggest(qty=4, warehouse=self.wh_other)
        (a | b).action_approve()
        self.assertNotEqual(a.purchase_order_id, b.purchase_order_id,
                            "two stores' goods went onto one order, so half of "
                            "them will be received at the wrong site")

    def test_the_cost_reaches_the_order_line(self):
        s = self._suggest(qty=3, unit_cost=42.5)
        s.action_approve()
        self.assertEqual(s.purchase_order_id.order_line.price_unit, 42.5)

    def test_an_archived_product_is_refused_by_name(self):
        self.product.active = False
        s = self._suggest(qty=5)
        with self.assertRaises(UserError) as e:
            s.action_approve()
        self.assertIn("archived", str(e.exception).lower())
        self.assertEqual(s.state, "new")

    def test_approving_nothing_says_so(self):
        s = self._suggest(qty=5)
        s.action_reject()
        with self.assertRaises(UserError):
            s.action_approve()

    def test_rejecting_then_resetting_returns_it_to_the_queue(self):
        s = self._suggest(qty=5)
        s.action_reject()
        self.assertEqual(s.state, "rejected")
        s.action_reset()
        self.assertEqual(s.state, "new")

    def test_an_ordered_line_cannot_be_reset_by_hand(self):
        """Resetting here would leave the purchase order behind with nothing
        pointing at it. Cancelling the order is the route back, and it sends
        the suggestion here by itself."""
        s = self._suggest(qty=5)
        s.action_approve()
        with self.assertRaises(UserError) as e:
            s.action_reset()
        self.assertIn("Cancel the order", str(e.exception))
