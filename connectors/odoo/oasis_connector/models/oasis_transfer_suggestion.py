# -*- coding: utf-8 -*-
"""OASIS transfer suggestions — a review queue that becomes real Odoo documents.

WHY A SUGGESTION AND NOT JUST A DRAFT PICKING
---------------------------------------------
OASIS can already write a draft internal picking straight into Odoo. But a
draft picking carries no reasoning: the operator sees "move 31 units" with no
account of why this store, why that donor, or what happens if they ignore it.
They then either approve everything blindly or nothing at all, and neither is
worth paying for.

So a suggestion holds the ARGUMENT — days of cover at both ends, the money at
risk, which of the two jobs it serves — and approving it produces the native
document. The reasoning is the product; the picking is just its output.

WHAT APPROVAL DOES
------------------
Creates a DRAFT internal transfer. Nothing is reserved and no stock moves: the
warehouse still confirms and validates in Odoo's own screens. OASIS proposes,
a human presses the button — the same contract the adapter has always kept.

Approving several suggestions on the same route produces ONE picking, not one
each. Odoo's unit of work is the picking, and splitting a single van into
fifteen pickings makes the warehouse pick, pack and validate fifteen times.
"""

from collections import defaultdict

from odoo import _, api, fields, models
from odoo.exceptions import UserError


class OasisTransferSuggestion(models.Model):
    _name = "oasis.transfer.suggestion"
    _description = "OASIS Transfer Suggestion"
    _order = "value_kes desc, id desc"
    _rec_name = "display_name"

    # ── what to move ──────────────────────────────────────────────────────
    product_id = fields.Many2one("product.product", required=True,
                                 ondelete="cascade", index=True)
    quantity = fields.Float("Quantity", required=True, digits="Product Unit of Measure")
    uom_id = fields.Many2one("uom.uom", related="product_id.uom_id", readonly=True)

    from_warehouse_id = fields.Many2one("stock.warehouse", "From", required=True,
                                        ondelete="cascade", index=True)
    to_warehouse_id = fields.Many2one("stock.warehouse", "To", required=True,
                                      ondelete="cascade", index=True)

    # ── why ───────────────────────────────────────────────────────────────
    kind = fields.Selection(
        [("pull", "Plug a gap"), ("push", "Clear idle stock")],
        required=True, index=True,
        help="OASIS does two jobs. PULL moves stock to a store that will run "
             "out before its next delivery. PUSH moves capital that is sitting "
             "still to a store that will sell it.")
    reason = fields.Text("Why", readonly=True,
                         help="The argument for this movement, in plain terms.")
    value_kes = fields.Monetary("Value", currency_field="currency_id")
    currency_id = fields.Many2one("res.currency",
                                  default=lambda s: s.env.company.currency_id)
    donor_days_cover = fields.Float("Donor cover (days)", readonly=True)
    recipient_days_cover = fields.Float("Recipient cover (days)", readonly=True)
    is_fresh = fields.Boolean("Perishable", index=True,
                              help="Fresh lines are never queued automatically — "
                                   "transit shortens shelf life. Approve one only "
                                   "if you can dispatch it today.")

    # ── lifecycle ─────────────────────────────────────────────────────────
    state = fields.Selection(
        [("new", "To review"), ("approved", "Approved"),
         ("rejected", "Rejected")],
        default="new", required=True, index=True)
    picking_id = fields.Many2one("stock.picking", "Transfer", readonly=True,
                                 help="The draft transfer this suggestion became.")
    picking_state = fields.Selection(related="picking_id.state", readonly=True,
                                     string="Transfer status")
    computed_on = fields.Datetime("Computed", default=fields.Datetime.now,
                                  readonly=True)
    company_id = fields.Many2one("res.company", default=lambda s: s.env.company,
                                 required=True, index=True)

    @api.depends("product_id", "from_warehouse_id", "to_warehouse_id", "quantity")
    def _compute_display_name(self):
        for r in self:
            r.display_name = "%s %s: %s → %s" % (
                r.quantity or 0, r.product_id.display_name or "",
                r.from_warehouse_id.code or "", r.to_warehouse_id.code or "")

    display_name = fields.Char(compute="_compute_display_name", store=False)

    # ── actions ───────────────────────────────────────────────────────────
    def action_approve(self):
        """Turn the selected suggestions into DRAFT internal transfers.

        Grouped by route so one van is one picking. Refuses a cross-company
        route outright rather than leaving a draft that can never be confirmed:
        Odoo will not confirm an internal picking spanning two companies, and
        the create succeeds, so an unchecked write produces a document that
        sits at Draft forever with no explanation.
        """
        todo = self.filtered(lambda r: r.state == "new")
        if not todo:
            raise UserError(_("Nothing to approve — these are already handled."))

        routes = defaultdict(lambda: self.browse())
        for r in todo:
            routes[(r.from_warehouse_id, r.to_warehouse_id)] |= r

        made = self.env["stock.picking"].browse()
        for (src, dst), recs in routes.items():
            if src == dst:
                raise UserError(_("%s cannot transfer to itself.") % src.code)
            if src.company_id != dst.company_id:
                raise UserError(_(
                    "%s and %s belong to different companies. Odoo cannot "
                    "confirm an internal transfer between them — that movement "
                    "is a sale and a purchase, not a transfer.")
                    % (src.code, dst.code))
            if not (src.lot_stock_id and dst.lot_stock_id and src.int_type_id):
                raise UserError(_(
                    "%s has no internal operation type or stock location, so "
                    "Odoo has nowhere to book this transfer.") % src.code)

            picking = self.env["stock.picking"].create({
                "picking_type_id": src.int_type_id.id,
                "location_id": src.lot_stock_id.id,
                "location_dest_id": dst.lot_stock_id.id,
                "origin": "OASIS transfer %s→%s" % (src.code, dst.code),
                "move_ids": [(0, 0, {
                    "name": r.product_id.display_name,
                    "product_id": r.product_id.id,
                    "product_uom_qty": r.quantity,
                    "product_uom": r.product_id.uom_id.id,
                    "location_id": src.lot_stock_id.id,
                    "location_dest_id": dst.lot_stock_id.id,
                }) for r in recs],
            })
            recs.write({"state": "approved", "picking_id": picking.id})
            made |= picking

        return {
            "type": "ir.actions.act_window",
            "name": _("Draft transfers"),
            "res_model": "stock.picking",
            "view_mode": "tree,form",
            "domain": [("id", "in", made.ids)],
        }

    def action_reject(self):
        self.filtered(lambda r: r.state == "new").write({"state": "rejected"})

    def action_reset(self):
        """Put a rejected suggestion back in the queue. Approved ones keep their
        document — undoing that means cancelling the picking in Odoo."""
        stuck = self.filtered(lambda r: r.state == "approved")
        if stuck:
            raise UserError(_(
                "%d of these already became transfers. Cancel the transfer in "
                "Inventory first — resetting here would leave the document "
                "behind with nothing pointing at it.") % len(stuck))
        self.write({"state": "new"})

    def action_open_picking(self):
        self.ensure_one()
        if not self.picking_id:
            raise UserError(_("This suggestion has not been approved yet."))
        return {
            "type": "ir.actions.act_window",
            "res_model": "stock.picking",
            "res_id": self.picking_id.id,
            "view_mode": "form",
        }

    # ── ingestion ─────────────────────────────────────────────────────────
    @api.model
    def oasis_replace_queue(self, suggestions, computed_on=None):
        """Replace the pending queue with a fresh scan. Called by OASIS.

        Only rows still awaiting review are cleared: anything approved has
        become a document somebody may already be picking, and anything
        rejected is a decision worth keeping — re-proposing it next scan would
        make the queue an argument the operator has to win repeatedly.

        Products and warehouses are matched on their codes so OASIS never has
        to know Odoo's database ids.
        """
        self.search([("state", "=", "new")]).unlink()

        wh = {w.code: w.id for w in self.env["stock.warehouse"].search([])}
        codes = [s.get("item_code") for s in suggestions if s.get("item_code")]
        prods = self.env["product.product"].search(
            [("default_code", "in", list(set(codes)))])
        pid = {p.default_code: p.id for p in prods}

        rows, skipped = [], 0
        for s in suggestions:
            p = pid.get(s.get("item_code"))
            f, t = wh.get(s.get("from_code")), wh.get(s.get("to_code"))
            if not (p and f and t) or f == t:
                skipped += 1
                continue
            rows.append({
                "product_id": p, "quantity": s.get("quantity") or 0,
                "from_warehouse_id": f, "to_warehouse_id": t,
                "kind": "push" if str(s.get("kind", "")).lower() == "push" else "pull",
                "reason": s.get("reason") or "",
                "value_kes": s.get("value") or 0.0,
                "donor_days_cover": s.get("donor_cover") or 0.0,
                "recipient_days_cover": s.get("recipient_cover") or 0.0,
                "is_fresh": bool(s.get("is_fresh")),
                "computed_on": computed_on or fields.Datetime.now(),
            })
        created = self.create(rows) if rows else self.browse()
        return {"created": len(created), "skipped": skipped}
