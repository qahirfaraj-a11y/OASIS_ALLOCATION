"""Keep the OASIS queue honest about orders it created.

Approving a suggestion produces a DRAFT purchase order, which commits nothing.
What happens to that document afterwards is entirely up to the buying team, and
without these hooks the queue never finds out: confirm the order and the
suggestion still reads "draft", cancel it and the suggestion goes on claiming
the line was bought, delete it and the suggestion points at nothing while
action_reset refuses to touch an approved row.

The damage is to the buyer, not the engine. A queue that reports a line as
ordered when no order exists is a queue people stop reading.
"""

from odoo import _, models


class PurchaseOrder(models.Model):
    _inherit = "purchase.order"

    def _oasis_suggestions(self):
        return self.env["oasis.order.suggestion"].sudo().search(
            [("purchase_order_id", "in", self.ids)])

    def button_confirm(self):
        """Confirming the order is what finishes the buying decision."""
        res = super().button_confirm()
        # After super(), because a confirmation Odoo refuses must leave the
        # suggestion exactly as it was.
        self._oasis_suggestions()._mark_confirmed()
        return res

    def button_cancel(self):
        res = super().button_cancel()
        # After the cancel, not before: if Odoo refuses it the suggestion must
        # stay attached to the document that still exists. A cancelled order is
        # released even from `done`, because cancelling a confirmed order means
        # the goods are not coming and the need is real again.
        self._oasis_suggestions()._release_from_dead_order(
            _("the purchase order was cancelled"))
        return res

    def unlink(self):
        # Before the delete, because purchase_order_id is `set null` on unlink —
        # once the rows are gone the link is gone with them and there is nothing
        # left to search by.
        suggestions = self._oasis_suggestions()
        res = super().unlink()
        suggestions._release_from_dead_order(
            _("the purchase order was deleted"))
        return res
