"""What the module must NOT contain, enforced from inside a running Odoo.

The pytest suite checks the source text. This checks the loaded registry,
which is the only place that can prove an action is genuinely unreachable
rather than merely absent from a menu.
"""

from odoo.tests.common import tagged

from .common import OasisCase


@tagged("post_install", "-at_install")
class TestNoConsoleEmbed(OasisCase):
    """Removing a menu hides an entrance; it does not close a door.

    The three console menus were deleted because embedding Intelligence,
    Operations and Command Center in an iframe shipped the entire product into
    a window inside Odoo and gave away every module to anyone who installed the
    connector. Only the MENUS were deleted: the method survived, the client
    action stayed registered by the asset bundle on every backend page, and
    ir.model.access.csv grants base.group_user WRITE on oasis.sync — so
    oasis.sync.open_console('intel') remained callable by any internal user.
    """

    def test_the_method_is_gone_from_the_registry(self):
        self.assertFalse(
            hasattr(self.env["oasis.sync"], "open_console"),
            "open_console is callable again — the whole suite is reachable "
            "by RPC from any internal user")

    def test_no_console_urls_are_configurable(self):
        settings = self.env["res.config.settings"]
        leaked = [f for f in settings._fields if "console" in f]
        self.assertFalse(leaked, "console URL settings are back: %s" % leaked)

    def test_no_client_action_named_oasis_embed_is_registered(self):
        action = self.env["ir.actions.client"].sudo().search(
            [("tag", "=", "oasis_embed")])
        self.assertFalse(action, "the oasis_embed client action is back")


@tagged("post_install", "-at_install")
class TestModuleShape(OasisCase):

    def test_point_of_sale_is_not_a_declared_dependency(self):
        module = self.env["ir.module.module"].sudo().search(
            [("name", "=", "oasis_connector")], limit=1)
        self.assertTrue(module)
        deps = module.dependencies_id.mapped("name")
        self.assertIn("stock", deps)
        self.assertNotIn(
            "point_of_sale", deps,
            "POS is a hard dependency again — every Odoo retailer not running "
            "Odoo POS is locked out of a stock-transfer module")

    def test_every_oasis_model_has_access_rules(self):
        """A model with no ir.model.access is either unusable or wide open,
        depending on the Odoo version. Neither is acceptable."""
        models = self.env["ir.model"].sudo().search(
            [("model", "=like", "oasis%")])
        self.assertTrue(models)
        for model in models:
            rules = self.env["ir.model.access"].sudo().search(
                [("model_id", "=", model.id)])
            self.assertTrue(rules, "no access rules for %s" % model.model)

    def test_the_menu_is_transfers_only(self):
        """The Odoo app is transfers (and later ordering), not the whole suite."""
        root = self.env.ref("oasis_connector.menu_oasis_root", False)
        self.assertTrue(root, "the OASIS root menu is missing")
        actions = self.env["ir.ui.menu"].sudo().search(
            [("id", "child_of", root.id), ("action", "!=", False)])
        models = {m.action.res_model for m in actions
                  if m.action and hasattr(m.action, "res_model")}
        self.assertTrue(models <= {"oasis.transfer.suggestion",
                                   "oasis.order.suggestion"},
                        "a menu points somewhere other than the review "
                        "queues: %s" % models)
