"""What the BASE module must be, and must not be.

The base exists so the OASIS modules can be bought and installed
independently. Its whole job is the app menu and the settings shell — the
moment it grows a model, a dependency on stock or purchase, or a feature, the
separability it exists to provide is gone.
"""

from odoo.tests.common import TransactionCase, tagged


@tagged("post_install", "-at_install")
class TestBaseStaysABase(TransactionCase):

    def _manifest_deps(self, name):
        module = self.env["ir.module.module"].sudo().search(
            [("name", "=", name)], limit=1)
        self.assertTrue(module, "%s is not present" % name)
        return module.dependencies_id.mapped("name")

    def test_the_base_depends_on_nothing_but_base(self):
        """A client buying replenishment alone must not be made to carry
        Inventory's transfer machinery, and vice versa."""
        deps = self._manifest_deps("oasis_connector")
        self.assertEqual(
            set(deps), {"base"},
            "the base module has grown a dependency: %s" % deps)

    #: The ONE model the base may touch, and only by extending it.
    #:
    #: Odoo records an ir.model.data row for every module in a model's
    #: inheritance chain, so extending res.config.settings registers
    #: `model_res_config_settings` against this module even though it defines
    #: nothing and stores nothing. The connection settings — the OASIS endpoint
    #: and its token — belong here precisely because they describe the OASIS
    #: INSTANCE rather than any one feature: two feature modules each declaring
    #: `oasis_scan_url` would render it twice and leave an operator guessing
    #: which one Refresh reads.
    _MAY_EXTEND = {"model_res_config_settings"}

    def test_the_base_owns_no_models_of_its_own(self):
        owned = self.env["ir.model.data"].sudo().search([
            ("module", "=", "oasis_connector"), ("model", "=", "ir.model")])
        unexpected = set(owned.mapped("name")) - self._MAY_EXTEND
        self.assertFalse(
            unexpected, "the base module now defines models: %s" % unexpected)

    def test_the_base_defines_no_oasis_model(self):
        """The failure the rule above is really guarding against. A business
        model in the base is what would make the modules inseparable — every
        client would carry it whether they bought that feature or not."""
        oasis_models = self.env["ir.model"].sudo().search(
            [("model", "=like", "oasis.%")])
        owned_here = self.env["ir.model.data"].sudo().search([
            ("module", "=", "oasis_connector"), ("model", "=", "ir.model"),
            ("res_id", "in", oasis_models.ids)])
        self.assertFalse(
            owned_here, "the base module defines an OASIS model: %s"
            % owned_here.mapped("name"))

    def test_the_app_root_menu_exists(self):
        self.assertTrue(self.env.ref("oasis_connector.menu_oasis_root", False))

    def test_the_settings_shell_exists_for_features_to_hang_off(self):
        self.assertTrue(self.env.ref(
            "oasis_connector.res_config_settings_view_form_oasis", False))


@tagged("post_install", "-at_install")
class TestNoConsoleEmbed(TransactionCase):
    """Removing a menu hides an entrance; it does not close a door.

    The three console menus were deleted because embedding Intelligence,
    Operations and Command Center in an iframe shipped the entire product into
    a window inside Odoo and gave away every module to anyone who installed the
    connector. Only the MENUS were deleted: the method survived, the client
    action stayed registered by the asset bundle on every backend page, and
    ir.model.access.csv granted base.group_user WRITE on oasis.sync — so
    oasis.sync.open_console('intel') remained callable by any internal user.
    """

    def test_no_client_action_named_oasis_embed_is_registered(self):
        self.assertFalse(
            self.env["ir.actions.client"].sudo().search(
                [("tag", "=", "oasis_embed")]),
            "the oasis_embed client action is back")

    def test_no_console_urls_are_configurable(self):
        leaked = [f for f in self.env["res.config.settings"]._fields
                  if "console" in f]
        self.assertFalse(leaked, "console URL settings are back: %s" % leaked)

    def test_the_method_is_gone_wherever_oasis_sync_lives(self):
        """oasis.sync moved to oasis_telemetry in the split; the method must
        stay gone whether or not that module is installed here."""
        if "oasis.sync" not in self.env:
            self.skipTest("oasis_telemetry is not installed")
        self.assertFalse(hasattr(self.env["oasis.sync"], "open_console"))
