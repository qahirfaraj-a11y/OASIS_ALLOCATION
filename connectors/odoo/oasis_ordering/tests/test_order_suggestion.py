"""The model itself: what a row says, and when it says it is out of date."""

from odoo import fields
from odoo.tests.common import tagged

from .common import OasisOrderCase


@tagged("post_install", "-at_install")
class TestOrderSuggestion(OasisOrderCase):

    def test_cover_reads_in_words_where_nothing_sells(self):
        """Zero sales means cover cannot be computed, not that it is zero days.
        A buyer reading '0 d' would order urgently for a line nobody buys."""
        s = self._suggest(qty=5, avg_daily_sales=0.0, days_cover=0.0)
        self.assertEqual(s.cover_label, "not selling")

    def test_cover_reads_in_days_where_it_sells(self):
        s = self._suggest(qty=5, avg_daily_sales=2.0, days_cover=3.4)
        self.assertEqual(s.cover_label, "3 d")

    def test_a_fresh_plan_is_not_stale(self):
        s = self._suggest(qty=5, computed_on=fields.Datetime.now())
        self.assertFalse(s.is_stale)

    def test_an_old_plan_is_stale_and_searchable(self):
        from dateutil.relativedelta import relativedelta
        old = fields.Datetime.now() - relativedelta(hours=48)
        s = self._suggest(qty=5, computed_on=old)
        self.assertTrue(s.is_stale)
        self.assertIn(s, self.Suggestion.search([("is_stale", "=", True)]))
        self.assertNotIn(s, self.Suggestion.search([("is_stale", "=", False)]))

    def test_the_staleness_window_is_configurable(self):
        from dateutil.relativedelta import relativedelta
        s = self._suggest(qty=5,
                          computed_on=fields.Datetime.now() - relativedelta(hours=6))
        self.assertFalse(s.is_stale, "6h should be fresh at the 12h default")
        self.env["ir.config_parameter"].sudo().set_param(
            "oasis.order_scan_stale_hours", "1")
        s.invalidate_recordset(["is_stale"])
        self.assertTrue(s.is_stale)

    def test_the_display_name_says_what_and_where(self):
        s = self._suggest(qty=7)
        self.assertIn("OAS-1", s.display_name)

    def test_opening_an_unapproved_line_says_so(self):
        from odoo.exceptions import UserError
        s = self._suggest(qty=5)
        with self.assertRaises(UserError):
            s.action_open_purchase_order()

    def test_refresh_without_an_endpoint_says_what_to_set(self):
        """An operator who presses Refresh and sees nothing change stops
        trusting the whole queue."""
        from odoo.exceptions import UserError
        self.env["ir.config_parameter"].sudo().set_param("oasis.scan_url", "")
        with self.assertRaises(UserError) as e:
            self.Suggestion.action_refresh_suggestions()
        self.assertIn("Connection", str(e.exception))
