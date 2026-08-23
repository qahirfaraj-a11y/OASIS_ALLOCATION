"""The basket guard — the one thing ordering has that transfers does not.

A transfer suggestion stands alone. An order line does not: OASIS admits it onto
a purchase order only because the WHOLE basket for that supplier cleared their
minimum units and value. Strike half the basket in review and the remainder can
fall under the minimum that justified it — the supplier then refuses the order
or adds small-order carriage, and the buyer finds out days later from someone
who is not Odoo.

Nothing in Odoo can catch this. It has no idea the lines were justified
together.
"""

from odoo.exceptions import UserError
from odoo.tests.common import tagged

from .common import OasisOrderCase


@tagged("post_install", "-at_install")
class TestSupplierMinimum(OasisOrderCase):

    def test_a_full_basket_clearing_the_minimum_is_approved(self):
        a = self._suggest(qty=6, unit_cost=100.0, supplier_min_units=10,
                          supplier_min_value=1000)
        b = self._suggest(qty=6, unit_cost=100.0, product=self.product_b,
                          supplier_min_units=10, supplier_min_value=1000)
        (a | b).action_approve()
        self.assertEqual(a.state, "approved")
        self.assertEqual(b.state, "approved")

    def test_approving_half_a_basket_is_refused_on_units(self):
        a = self._suggest(qty=6, unit_cost=100.0, supplier_min_units=10,
                          supplier_min_value=0)
        self._suggest(qty=6, unit_cost=100.0, product=self.product_b,
                      supplier_min_units=10, supplier_min_value=0)
        with self.assertRaises(UserError) as e:
            a.action_approve()
        self.assertIn("minimum", str(e.exception).lower())
        self.assertEqual(a.state, "new", "the line was approved anyway")

    def test_approving_half_a_basket_is_refused_on_value(self):
        a = self._suggest(qty=2, unit_cost=100.0, supplier_min_units=0,
                          supplier_min_value=1000)
        with self.assertRaises(UserError) as e:
            a.action_approve()
        self.assertIn("value", str(e.exception).lower())

    def test_the_refusal_names_what_would_close_the_gap(self):
        """A refusal that only says 'too small' leaves the buyer to work out
        the fix by hand. The lines that would close it are right there."""
        a = self._suggest(qty=3, unit_cost=100.0, supplier_min_units=10,
                          supplier_min_value=0)
        self._suggest(qty=8, unit_cost=100.0, product=self.product_b,
                      supplier_min_units=10, supplier_min_value=0)
        with self.assertRaises(UserError) as e:
            a.action_approve()
        msg = str(e.exception)
        self.assertIn("awaiting review", msg)
        self.assertIn("OASIS Test Supplier", msg)

    def test_with_nothing_left_to_add_it_points_at_transfers(self):
        """A basket that is genuinely too small to buy is the exact case the
        transfer engine answers — say so instead of stopping at 'no'."""
        a = self._suggest(qty=1, unit_cost=10.0, supplier_min_units=100,
                          supplier_min_value=0)
        with self.assertRaises(UserError) as e:
            a.action_approve()
        self.assertIn("Transfers", str(e.exception))

    def test_the_override_exists_and_works(self):
        """A buyer who knows this supplier takes short orders must be able to
        say so — deliberately, and on the record."""
        a = self._suggest(qty=1, unit_cost=10.0, supplier_min_units=100)
        a.action_approve_below_minimum()
        self.assertEqual(a.state, "approved")
        self.assertTrue(a.purchase_order_id)

    def test_no_declared_minimum_never_blocks(self):
        """Most clients have no supplier minimums recorded at all. The guard
        must be silent there, not refuse everything."""
        a = self._suggest(qty=1, unit_cost=1.0, supplier_min_units=0,
                          supplier_min_value=0)
        a.action_approve()
        self.assertEqual(a.state, "approved")

    def test_the_minimum_is_judged_per_supplier_not_across_the_selection(self):
        """Two suppliers approved together are two baskets. Summing them would
        let one supplier's large order excuse another's short one."""
        big = self._suggest(qty=50, unit_cost=100.0, supplier_min_units=10)
        small = self._suggest(qty=1, unit_cost=100.0, partner=self.supplier_b,
                              product=self.product_b, supplier_min_units=10)
        with self.assertRaises(UserError) as e:
            (big | small).action_approve()
        self.assertIn("OASIS Other Supplier", str(e.exception))

    def test_the_minimum_is_judged_per_store(self):
        """The same supplier delivering to two stores is two orders, and each
        has to clear the minimum on its own."""
        a = self._suggest(qty=9, unit_cost=100.0, supplier_min_units=10)
        b = self._suggest(qty=1, unit_cost=100.0, warehouse=self.wh_other,
                          supplier_min_units=10)
        with self.assertRaises(UserError):
            (a | b).action_approve()
