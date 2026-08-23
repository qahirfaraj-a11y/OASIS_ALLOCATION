"""Replenishment must stand on its own.

The point of the split is that a client can buy transfers, or replenishment, or
telemetry, in any combination. That guarantee is only real if this module never
quietly starts needing one of its siblings — which is exactly the kind of
dependency that creeps in through a shared helper, a settings field someone else
declares, or a menu that assumes a sibling's parent exists.
"""

from odoo.tests.common import tagged
from odoo.tools.safe_eval import safe_eval

from .common import OasisOrderCase


@tagged("post_install", "-at_install")
class TestOrderingStandsAlone(OasisOrderCase):

    def _deps(self, name="oasis_ordering"):
        module = self.env["ir.module.module"].sudo().search(
            [("name", "=", name)], limit=1)
        self.assertTrue(module, "%s is not present" % name)
        return set(module.dependencies_id.mapped("name"))

    def test_it_needs_only_the_base_and_purchase(self):
        self.assertEqual(
            self._deps(), {"oasis_connector", "purchase"},
            "replenishment has grown a dependency it does not need")

    def test_it_does_not_need_transfers(self):
        """A client who wants OASIS to tell them what to buy should never be
        made to install the transfer machinery to get it."""
        self.assertNotIn("oasis_transfers", self._deps())

    def test_it_does_not_need_telemetry(self):
        self.assertNotIn("oasis_telemetry", self._deps())

    def test_it_does_not_need_point_of_sale(self):
        self.assertNotIn("point_of_sale", self._deps())

    def test_it_works_with_transfers_absent(self):
        """The real test of the split: the whole approve path, with no sibling
        installed."""
        if "oasis.transfer.suggestion" in self.env:
            self.skipTest("oasis_transfers is installed in this database")
        s = self._suggest(qty=5)
        s.action_approve()
        self.assertEqual(s.state, "approved")
        self.assertEqual(s.purchase_order_id.state, "draft")

    def test_the_connection_settings_come_from_the_base(self):
        """scan_url and the token describe the OASIS INSTANCE, not a feature.
        They live in oasis_connector so both queues read the same one — if a
        module ever re-declares them, the settings page shows the field twice
        and nobody can tell which the Refresh button uses."""
        settings = self.env["res.config.settings"]
        for field in ("oasis_scan_url", "oasis_scan_token"):
            self.assertIn(field, settings._fields,
                          "%s is not configurable from the UI" % field)

    def test_its_own_staleness_window_is_separate_from_the_transfer_one(self):
        """An order is a decision about the next lead time; a transfer is a
        decision about stock selling underneath it. Sharing one window would
        force one of them to be wrong."""
        self.env["ir.config_parameter"].sudo().set_param(
            "oasis.order_scan_stale_hours", "36")
        self.assertEqual(self.Suggestion._stale_hours(), 36.0)


@tagged("post_install", "-at_install")
class TestOrderingMenuAndSecurity(OasisOrderCase):

    def test_the_menu_hangs_off_the_shared_root(self):
        root = self.env.ref("oasis_connector.menu_oasis_root")
        section = self.env.ref("oasis_ordering.menu_oasis_ordering")
        self.assertEqual(section.parent_id, root)

    def test_confirmed_work_has_its_own_place(self):
        """Not a filter somebody has to know to apply. Mixing finished work
        into a list whose job is "what needs deciding today" is how a queue
        stops being read."""
        menu = self.env.ref("oasis_ordering.menu_oasis_order_completed")
        self.assertEqual(menu.parent_id,
                         self.env.ref("oasis_ordering.menu_oasis_ordering"))
        action = self.env.ref("oasis_ordering.action_oasis_order_completed")
        self.assertIn("'done'", action.domain,
                      "the Confirmed area does not filter to confirmed rows")

    def test_the_confirmed_area_shows_confirmed_work_and_nothing_else(self):
        pending = self._suggest(qty=2)
        finished = self._suggest(qty=3)
        finished.action_approve()
        finished.purchase_order_id.button_confirm()

        action = self.env.ref("oasis_ordering.action_oasis_order_completed")
        shown = self.Suggestion.search(safe_eval(action.domain))
        self.assertIn(finished, shown)
        self.assertNotIn(pending, shown, "a pending row reached the archive")

    def test_the_model_has_access_rules_and_a_company_rule(self):
        model = self.env["ir.model"].sudo().search(
            [("model", "=", "oasis.order.suggestion")], limit=1)
        self.assertTrue(self.env["ir.model.access"].sudo().search(
            [("model_id", "=", model.id)]), "no access rules")
        rule = self.env["ir.rule"].sudo().search([("model_id", "=", model.id)])
        self.assertTrue(rule, "no record rule")
        self.assertTrue(rule.filtered("global"),
                        "the rule is not global, so it protects nobody")
