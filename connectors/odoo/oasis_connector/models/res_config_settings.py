from odoo import models, fields


class ResConfigSettings(models.TransientModel):
    _inherit = "res.config.settings"

    oasis_enabled = fields.Boolean(
        string="Enable OASIS streaming",
        config_parameter="oasis.enabled",
        help="Nothing is sent to OASIS until this is on.",
    )
    oasis_hub_url = fields.Char(
        string="OASIS Hub URL",
        config_parameter="oasis.hub_url",
        help="Base URL of the OASIS Cloud Hub, e.g. https://hub.oasis-systems.example",
    )
    oasis_ingest_token = fields.Char(
        string="Store Ingest Token",
        config_parameter="oasis.ingest_token",
        help="Per-store token issued by OASIS. Treat it like a password.",
    )
    oasis_store_code = fields.Char(
        string="Store Code",
        config_parameter="oasis.store_code",
        help="The store code this Odoo instance represents in OASIS.",
    )
    oasis_send_sales = fields.Boolean(
        string="Send POS sales", config_parameter="oasis.send_sales", default=True)
    oasis_send_receipts = fields.Boolean(
        string="Send goods receipts", config_parameter="oasis.send_receipts", default=True)
    oasis_send_on_hand = fields.Boolean(
        string="Send stock-on-hand snapshots",
        config_parameter="oasis.send_on_hand", default=False)
    oasis_console_intel_url = fields.Char(
        string="Intelligence Console URL",
        config_parameter="oasis.console_intel_url",
        help="Where the OASIS Intelligence console is served "
             "(reachable from the USER'S browser).")
    oasis_console_ops_url = fields.Char(
        string="Operations Console URL",
        config_parameter="oasis.console_ops_url")
    oasis_console_command_url = fields.Char(
        string="Command Center URL",
        config_parameter="oasis.console_command_url")

    def action_oasis_sync_now(self):
        """Manual 'Sync now' button — runs one incremental push immediately."""
        self.env["oasis.sync"].run_sync()
        return {
            "type": "ir.actions.client",
            "tag": "display_notification",
            "params": {
                "title": "OASIS",
                "message": "Sync triggered — check the OASIS Connector log for results.",
                "sticky": False,
            },
        }
