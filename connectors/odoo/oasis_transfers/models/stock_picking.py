"""Keep the OASIS queue honest about documents it created.

Approving a suggestion produces a DRAFT picking, which reserves nothing. What
happens to that document afterwards is entirely up to the warehouse team, and
until now the queue never found out. Cancel the transfer and the suggestion
went on saying `approved`; delete it and the suggestion said `approved` while
pointing at nothing, unable to be reopened because action_reset refuses to
touch an approved row.

Neither corrupted the engine — OASIS maps a cancelled picking to CANCELLED and
only credits REQUESTED/IN_TRANSIT as stock in flight, so it never believed the
units were moving. The damage was to the operator: a queue that reports a
movement as handled when nothing moved is a queue people stop trusting.
"""

from odoo import _, models


class StockPicking(models.Model):
    _inherit = "stock.picking"

    def _oasis_suggestions(self):
        return self.env["oasis.transfer.suggestion"].sudo().search(
            [("picking_id", "in", self.ids)])

    def action_cancel(self):
        res = super().action_cancel()
        # After the cancel, not before: if Odoo refuses it the suggestion must
        # stay attached to the document that still exists.
        self._oasis_suggestions()._release_from_dead_picking(
            _("the transfer was cancelled in Inventory"))
        return res

    def unlink(self):
        # Before the delete, because picking_id is `set null` on unlink — once
        # the rows are gone the link is gone with them and there is nothing
        # left to search by.
        suggestions = self._oasis_suggestions()
        res = super().unlink()
        suggestions._release_from_dead_picking(
            _("the transfer was deleted in Inventory"))
        return res
