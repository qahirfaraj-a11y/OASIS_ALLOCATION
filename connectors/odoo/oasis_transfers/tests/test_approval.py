"""Approval: the point where a suggestion becomes a document.

The rule the whole app rests on is that approving creates a DRAFT and nothing
moves until a human confirms it in Inventory.
"""

from odoo.exceptions import UserError
from odoo.tests.common import tagged

from .common import OasisCase


@tagged("post_install", "-at_install")
class TestApproval(OasisCase):

    def setUp(self):
        super().setUp()
        self._stock(self.product, self.wh_a, 500.0)

    def test_approving_creates_a_DRAFT_picking_that_reserves_nothing(self):
        s = self._suggest(qty=10)
        s.action_approve()

        self.assertEqual(s.state, "approved")
        self.assertTrue(s.picking_id, "no picking was linked back")
        self.assertEqual(s.picking_id.state, "draft",
                         "the picking is not a draft — stock could move "
                         "without anyone confirming it")
        self.assertEqual(s.picking_id.location_id, self.wh_a.lot_stock_id)
        self.assertEqual(s.picking_id.location_dest_id, self.wh_b.lot_stock_id)
        move = s.picking_id.move_ids
        self.assertEqual(len(move), 1)
        self.assertEqual(move.product_id, self.product)
        self.assertEqual(move.product_uom_qty, 10)

    def test_one_route_is_one_picking_not_one_per_line(self):
        """A van makes one trip. Three lines on one route are one document."""
        other = self.env["product.product"].create({
            "name": "OASIS Test Widget 2", "default_code": "OAS-W2",
            "type": "product", "list_price": 50.0})
        third = self.env["product.product"].create({
            "name": "OASIS Test Widget 3", "default_code": "OAS-W3",
            "type": "product", "list_price": 50.0})
        self._stock(other, self.wh_a, 200.0)
        self._stock(third, self.wh_a, 200.0)

        batch = (self._suggest(qty=1)
                 | self._suggest(qty=2, product=other)
                 | self._suggest(qty=3, product=third))
        batch.action_approve()

        self.assertEqual(len(batch.picking_id), 1,
                         "one route produced more than one picking")
        self.assertEqual(len(batch.picking_id.move_ids), 3)

    def test_separate_routes_get_separate_pickings(self):
        wh_c = self._make_warehouse("OAS-C", "OASIS Test C")
        batch = self._suggest(qty=1) | self._suggest(qty=2, to=wh_c)
        batch.action_approve()
        self.assertEqual(len(batch.picking_id), 2)

    def test_approving_nothing_says_so(self):
        s = self._suggest()
        s.state = "approved"
        with self.assertRaises(UserError):
            s.action_approve()

    def test_a_self_transfer_is_refused(self):
        s = self._suggest(to=self.wh_a)
        with self.assertRaises(UserError):
            s.action_approve()

    def test_reject_and_reset(self):
        s = self._suggest()
        s.action_reject()
        self.assertEqual(s.state, "rejected")
        s.action_reset()
        self.assertEqual(s.state, "new")


@tagged("post_install", "-at_install")
class TestStaleApprovalIsRefusedClearly(OasisCase):
    """M5. A draft picking reserves NOTHING.

    So a suggestion computed against stock that has since sold produced a
    perfectly clean draft and only failed later at CONFIRMATION, as an Odoo
    reservation error naming neither OASIS nor the suggestion behind it. The
    operator saw a stock error on a document they did not create.
    """

    def test_it_refuses_before_creating_anything(self):
        self._stock(self.product, self.wh_a, 5.0)
        s = self._suggest(qty=50)

        with self.assertRaises(UserError) as caught:
            s.action_approve()

        msg = str(caught.exception)
        self.assertIn("no longer holds enough stock", msg)
        self.assertIn("Refresh from OASIS", msg,
                      "the refusal does not tell the operator what to do")
        self.assertIn(self.product.display_name, msg,
                      "the refusal does not name the product")
        self.assertIn("50", msg, "the refusal does not state what was asked")

        self.assertEqual(s.state, "new", "the suggestion was mutated anyway")
        self.assertFalse(s.picking_id, "a picking was created despite refusing")
        self.assertFalse(
            self.env["stock.picking"].search(
                [("location_id", "=", self.wh_a.lot_stock_id.id),
                 ("origin", "like", "OASIS")]),
            "a stray picking was left behind")

    def test_quantities_are_not_rendered_in_scientific_notation(self):
        """`%g` flips past six digits, and '1e+07' in an error about
        quantities is worse than useless to whoever has to act on it."""
        self._stock(self.product, self.wh_a, 5.0)
        s = self._suggest(qty=9999999)
        with self.assertRaises(UserError) as caught:
            s.action_approve()
        self.assertIn("9999999", str(caught.exception))
        self.assertNotIn("e+", str(caught.exception))

    def test_the_same_product_twice_on_a_route_is_summed_before_checking(self):
        """One product can legitimately appear twice — once pulled, once
        pushed — and the picking would carry two move lines against ONE pool
        of stock. Checking them separately would pass both."""
        self._stock(self.product, self.wh_a, 30.0)
        batch = self._suggest(qty=20) | self._suggest(qty=20, kind="push")
        with self.assertRaises(UserError):
            batch.action_approve()   # 40 wanted, 30 held

    def test_enough_stock_still_approves(self):
        self._stock(self.product, self.wh_a, 30.0)
        batch = self._suggest(qty=10) | self._suggest(qty=10, kind="push")
        batch.action_approve()
        self.assertTrue(all(s.state == "approved" for s in batch))

    def test_reserved_stock_does_not_count_as_available(self):
        """Units already promised to another picking cannot be promised again."""
        self._stock(self.product, self.wh_a, 20.0)
        self.Quant._update_reserved_quantity(
            self.product, self.wh_a.lot_stock_id, 18.0)
        s = self._suggest(qty=10)
        with self.assertRaises(UserError) as caught:
            s.action_approve()
        self.assertIn("unreserved", str(caught.exception))
