"""How Odoo reaches OASIS — one connection, declared once.

These settings describe the OASIS INSTANCE, not any one feature. Transfers and
Replenishment both ask the same OASIS to recompute, and both judge their queue
against the same staleness window, so they must not each own a copy: two
modules declaring `oasis_scan_url` on res.config.settings would render it twice
on the settings page and leave an operator wondering which one the Refresh
button actually reads.

They lived in oasis_transfers while transfers was the only feature module.
Moving them here is safe on upgrade — these are `config_parameter` fields on a
TransientModel, so nothing is stored against them; the underlying
ir.config_parameter rows are untouched and keep their values.
"""

from odoo import fields, models


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
