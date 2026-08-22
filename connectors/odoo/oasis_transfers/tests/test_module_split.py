"""Transfers must stand on its own.

The point of the split is that a client can buy transfers, or replenishment,
or telemetry, in any combination. That guarantee is only real if the transfers
module never quietly starts needing one of its siblings — which is exactly the
kind of dependency that creeps in through a shared helper, a settings field
someone else declares, or a menu that assumes a sibling's parent exists.
"""

from odoo.tests.common import tagged

from .common import OasisCase


@tagged("post_install", "-at_install")
class TestTransfersStandsAlone(OasisCase):

    def _deps(self, name="oasis_transfers"):
        module = self.env["ir.module.module"].sudo().search(
            [("name", "=", name)], limit=1)
        self.assertTrue(module, "%s is not present" % name)
        return set(module.dependencies_id.mapped("name"))

    def test_it_needs_only_the_base_and_stock(self):
        self.assertEqual(
            self._deps(), {"oasis_connector", "stock"},
            "transfers has grown a dependency it does not need")

    def test_it_does_not_need_telemetry(self):
        """A client who wants OASIS to move stock between their own stores
        should never be made to stream anything to the Cloud Hub."""
        self.assertNotIn("oasis_telemetry", self._deps())

    def test_it_does_not_need_point_of_sale(self):
        self.assertNotIn("point_of_sale", self._deps())

    def test_it_does_not_need_purchase(self):
        """Transfers move stock the business already owns. Buying is a
        different module and a different purchase."""
        self.assertNotIn("purchase", self._deps())

    def test_it_works_with_telemetry_absent(self):
        """The real test of the split: the whole approve path, with no
        sibling installed."""
        if "oasis.sync" in self.env:
            self.skipTest("oasis_telemetry is installed in this database")
        self._stock(self.product, self.wh_a, 100.0)
        s = self._suggest(qty=5)
        s.action_approve()
        self.assertEqual(s.state, "approved")
        self.assertEqual(s.picking_id.state, "draft")

    def test_its_settings_are_reachable_without_a_sibling(self):
        """scan_url and the staleness window were ir.config_parameter keys
        with no interface — survivable inside a module whose settings page
        existed for something else, not in one sold on its own."""
        settings = self.env["res.config.settings"]
        for field in ("oasis_scan_url", "oasis_scan_token",
                      "oasis_scan_stale_hours"):
            self.assertIn(field, settings._fields,
                          "%s is not configurable from the UI" % field)

    def test_the_stale_window_setting_actually_drives_the_model(self):
        self.env["ir.config_parameter"].sudo().set_param(
            "oasis.scan_stale_hours", "9")
        self.assertEqual(self.Suggestion._stale_hours(), 9.0)


@tagged("post_install", "-at_install")
class TestTransfersMenuAndSecurity(OasisCase):

    def test_the_menu_hangs_off_the_shared_root(self):
        root = self.env.ref("oasis_connector.menu_oasis_root")
        section = self.env.ref("oasis_transfers.menu_oasis_transfers")
        self.assertEqual(section.parent_id, root)

    def test_the_menu_points_only_at_the_review_queue(self):
        root = self.env.ref("oasis_connector.menu_oasis_root")
        actions = self.env["ir.ui.menu"].sudo().search(
            [("id", "child_of", root.id), ("action", "!=", False)])
        models = {m.action.res_model for m in actions
                  if m.action and hasattr(m.action, "res_model")}
        self.assertTrue(
            models <= {"oasis.transfer.suggestion", "oasis.order.suggestion"},
            "a menu points somewhere other than the review queues: %s" % models)

    def test_the_model_has_access_rules_and_a_company_rule(self):
        model = self.env["ir.model"].sudo().search(
            [("model", "=", "oasis.transfer.suggestion")], limit=1)
        self.assertTrue(self.env["ir.model.access"].sudo().search(
            [("model_id", "=", model.id)]), "no access rules")
        rule = self.env["ir.rule"].sudo().search([("model_id", "=", model.id)])
        self.assertTrue(rule, "no record rule")
        self.assertTrue(rule.filtered("global"),
                        "the rule is not global, so it protects nobody")
