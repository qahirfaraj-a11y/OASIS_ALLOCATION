"""Transfers' own settings.

These were `ir.config_parameter` keys with no interface at all: an operator had
to be told to run `set_param` from a shell, or read INTEGRATION.md. That was
survivable while transfers shipped inside a module whose settings page existed
for something else. A module sold on its own has to be configurable on its own.
"""

from odoo import models, fields


class ResConfigSettings(models.TransientModel):
    _inherit = "res.config.settings"

    oasis_scan_url = fields.Char(
        string="OASIS scan endpoint",
        config_parameter="oasis.scan_url",
        help="Where the Refresh button reaches OASIS to recompute the plan. "
             "Resolved from inside the Odoo container, so on Docker this is "
             "usually host.docker.internal or the service name — not "
             "localhost, which would be Odoo itself.",
    )
    oasis_scan_token = fields.Char(
        string="Scan token",
        config_parameter="oasis.scan_token",
        help="Sent to the OASIS scan endpoint. Required whenever that endpoint "
             "listens on anything other than localhost.",
    )
    oasis_scan_stale_hours = fields.Float(
        string="Suggestions go stale after (hours)",
        config_parameter="oasis.scan_stale_hours",
        help="A plan older than this describes a shop floor that has since "
             "moved on. Default is half an hour: demand barely shifts over a "
             "day, but stock does, continuously. Raise it for a chain that "
             "trades slowly or scans nightly.",
    )
