"""Multi-company: attribution, then isolation. In that order.

The addon shipped no ir.rule at all, so a stock user in company A could see
AND approve company B's suggestions — and approving creates a picking in a
company the user does not belong to.

Attribution had to be fixed first. company_id defaulted to `self.env.company`,
the RPC pusher's active company, so a scan pushed from company A stamped rows
A even where the stock lives in B. Harmless while nothing filtered on it; once
a rule exists it hides the row from the very people who own the stock.
"""

from odoo.exceptions import UserError
from odoo.tests.common import tagged, new_test_user

from .common import OasisCase


@tagged("post_install", "-at_install")
class TestMultiCompany(OasisCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.company_b = cls.env["res.company"].create({"name": "OASIS Other Co"})
        cls.wh_other = cls._make_warehouse("OAS-Z", "OASIS Other WH",
                                           company=cls.company_b)

    def test_company_comes_from_the_DONOR_warehouse_not_the_pusher(self):
        """The donor is right because the picking created on approval is built
        from the donor's operation type, so this matches the company that
        document will belong to."""
        self.Suggestion.oasis_replace_queue(
            [self._row("OAS-WIDGET", "OAS-Z", "OAS-A")])
        rec = self.Suggestion.sudo().search(
            [("from_warehouse_id", "=", self.wh_other.id)])
        self.assertTrue(rec)
        self.assertEqual(
            rec.company_id, self.company_b,
            "the row was stamped with the pusher's company, not the donor's")

    def test_a_record_rule_actually_ships(self):
        rule = self.env["ir.rule"].search(
            [("model_id.model", "=", "oasis.transfer.suggestion")])
        self.assertTrue(rule, "no record rule on the suggestion model")
        self.assertTrue(rule.filtered("global"),
                        "the rule is not global, so it protects nobody")

    def test_a_user_cannot_see_another_companys_suggestions(self):
        mine = self._suggest()
        theirs = self.Suggestion.create({
            "product_id": self.product.id, "quantity": 5,
            "from_warehouse_id": self.wh_other.id,
            "to_warehouse_id": self.wh_other.id,
            "kind": "pull", "company_id": self.company_b.id,
        })

        user = new_test_user(
            self.env, login="oasis_a_only",
            groups="base.group_user,stock.group_stock_user",
            company_id=self.company.id, company_ids=[(6, 0, [self.company.id])])

        visible = self.Suggestion.with_user(user).search([])
        self.assertIn(mine, visible)
        self.assertNotIn(theirs, visible,
                         "another company's suggestion is visible")

    def test_a_user_in_both_companies_sees_both(self):
        mine = self._suggest()
        theirs = self.Suggestion.create({
            "product_id": self.product.id, "quantity": 5,
            "from_warehouse_id": self.wh_other.id,
            "to_warehouse_id": self.wh_other.id,
            "kind": "pull", "company_id": self.company_b.id,
        })
        both = new_test_user(
            self.env, login="oasis_both",
            groups="base.group_user,stock.group_stock_user",
            company_id=self.company.id,
            company_ids=[(6, 0, [self.company.id, self.company_b.id])])

        visible = self.Suggestion.with_user(both).search([])
        self.assertIn(mine, visible)
        self.assertIn(theirs, visible)

    def test_a_cross_company_route_is_refused_with_an_explanation(self):
        """Odoo cannot confirm an internal picking spanning two companies —
        that movement is a sale and a purchase, not a transfer. The create
        would succeed, leaving a draft that sits forever with no explanation.
        """
        s = self._suggest(to=self.wh_other)
        with self.assertRaises(UserError) as caught:
            s.action_approve()
        self.assertIn("different companies", str(caught.exception))
        self.assertFalse(s.picking_id)
