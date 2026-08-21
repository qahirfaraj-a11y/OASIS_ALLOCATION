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

from dateutil.relativedelta import relativedelta

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
        string="Decision", required=True, index=True,
        help="OASIS does two jobs. PULL moves stock to a store that will run "
             "out before its next delivery. PUSH moves capital that is sitting "
             "still to a store that will sell it.")
    reason = fields.Text("Why", readonly=True,
                         help="The argument for this movement, in plain terms.")
    categ_id = fields.Many2one(related="product_id.categ_id", store=True,
                               string="Category", readonly=True, index=True,
                               help="Stored so the queue can be pivoted by "
                                    "category without joining product on every "
                                    "read.")
    value_kes = fields.Monetary("Value", currency_field="currency_id")
    currency_id = fields.Many2one("res.currency",
                                  default=lambda s: s.env.company.currency_id)
    # ── the position at both ends ─────────────────────────────────────────
    # Cover is meaningless where nothing sells: the engine carries 999 as its
    # internal "no demand" sentinel, and letting that reach a column poisons
    # every average in the pivot and reads to an operator as a real 999-day
    # figure. It is stripped on ingestion; donor_ads = 0 is what actually says
    # "this store does not sell it", and donor_cover_label says so in words.
    donor_days_cover = fields.Float("Donor cover (days)", readonly=True,
                                    group_operator="avg")
    recipient_days_cover = fields.Float("Recipient cover (days)", readonly=True,
                                        group_operator="avg")
    donor_ads = fields.Float("Donor sales/day", readonly=True, digits=(12, 3),
                             group_operator="avg",
                             help="What the DONOR sells of this line per day. "
                                  "Zero means the stock is idle there.")
    recipient_ads = fields.Float("Recipient sales/day", readonly=True,
                                 digits=(12, 3), group_operator="avg",
                                 help="What the RECIPIENT sells per day — the "
                                      "rate the transferred stock will move at.")
    relief_days = fields.Float("Next delivery (days)", readonly=True,
                               group_operator="avg",
                               help="Days until the recipient's own "
                                    "replenishment is expected to land, derived "
                                    "from this supplier's measured delivery "
                                    "history. The transfer only has to cover "
                                    "until then.")
    donor_cover_label = fields.Char("Donor cover",
                                    compute="_compute_cover_labels")
    recipient_cover_label = fields.Char("Recipient cover",
                                        compute="_compute_cover_labels")

    @api.depends("donor_days_cover", "donor_ads",
                 "recipient_days_cover", "recipient_ads")
    def _compute_cover_labels(self):
        for r in self:
            r.donor_cover_label = (
                "not selling" if r.donor_ads <= 0
                else "%.0f d" % r.donor_days_cover)
            r.recipient_cover_label = (
                "not selling" if r.recipient_ads <= 0
                else "%.0f d" % r.recipient_days_cover)
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
    def _check_donor_stock(self, src, recs):
        """Refuse a route the donor can no longer cover, and say why.

        A draft picking reserves NOTHING. So a suggestion computed against
        stock that has since sold produced a perfectly clean draft, and only
        failed later at confirmation — as an Odoo reservation error naming
        neither OASIS nor the suggestion it came from. The operator saw a
        stock error on a document they did not create and had no route back to
        the cause. The staleness window narrows how often that happens; it does
        nothing for what it looks like when it does.

        Checking here converts it into a refusal that names the product, the
        gap, and the fix.
        """
        wanted = defaultdict(float)
        for r in recs:
            # one product can legitimately appear twice on a route — once
            # pulled, once pushed — and the picking would carry two move lines
            # against ONE pool of stock. Sum before comparing.
            wanted[r.product_id] += r.quantity
        if not wanted:
            return

        quants = self.env["stock.quant"].read_group(
            [("product_id", "in", [p.id for p in wanted]),
             ("location_id", "child_of", src.lot_stock_id.id)],
            ["quantity:sum", "reserved_quantity:sum"], ["product_id"])
        on_hand = {q["product_id"][0]:
                   (q.get("quantity") or 0.0) - (q.get("reserved_quantity") or 0.0)
                   for q in quants if q.get("product_id")}

        short = []
        for product, qty in wanted.items():
            free = on_hand.get(product.id, 0.0)
            if free < qty:
                short.append((product, qty, free))
        if not short:
            return

        def _qty(v):
            # not "%g": it flips to scientific notation past six digits, and a
            # quantity rendered as 1e+07 in an error about quantities is worse
            # than useless to whoever has to act on it.
            v = float(v)
            return "%d" % v if v.is_integer() else "%.2f" % v

        lines = "\n".join(
            _("  • %(name)s — asking %(want)s, %(src)s has %(free)s unreserved")
            % {"name": p.display_name, "want": _qty(w),
               "src": src.code, "free": _qty(f)}
            for p, w, f in short)
        raise UserError(_(
            "%(src)s no longer holds enough stock for this transfer.\n\n"
            "%(lines)s\n\n"
            "These suggestions were computed from a snapshot taken at "
            "%(when)s; the stock has moved since. Press Refresh from OASIS to "
            "recompute against current stock, then approve again.\n\n"
            "(Caught before creating the transfer. Without this check Odoo "
            "would have accepted the draft and failed later at confirmation, "
            "with a reservation error that never mentions OASIS.)")
            % {"src": src.code, "lines": lines,
               "when": recs[:1].computed_on or _("an unknown time")})

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

            self._check_donor_stock(src, recs)

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

    #: A plan older than this describes a shop floor that has since moved on.
    #:
    #: THIRTY MINUTES, not a day. With live POS sales streaming in, on-hand is
    #: the fastest-moving input the plan has — demand barely shifts over a day
    #: (ADS is a 90-day average) but stock does, continuously. A suggestion
    #: computed this morning and approved this afternoon can be for stock that
    #: has since sold; the draft picking then fails at confirmation, which the
    #: operator experiences as the queue being unreliable.
    #:
    #: Raise oasis.scan_stale_hours for a chain that trades slowly or scans
    #: nightly; lower it for one that does not.
    _DEFAULT_STALE_HOURS = 0.5

    is_stale = fields.Boolean("Out of date", compute="_compute_is_stale",
                              search="_search_is_stale",
                              help="Computed before the staleness window. The "
                                   "stores have been trading since; re-run the "
                                   "scan before approving.")

    def _stale_hours(self):
        try:
            return float(self.env["ir.config_parameter"].sudo().get_param(
                "oasis.scan_stale_hours") or self._DEFAULT_STALE_HOURS)
        except (TypeError, ValueError):
            return self._DEFAULT_STALE_HOURS

    @api.depends("computed_on")
    def _compute_is_stale(self):
        cutoff = fields.Datetime.now() - relativedelta(hours=self._stale_hours())
        for r in self:
            r.is_stale = bool(r.computed_on and r.computed_on < cutoff)

    def _search_is_stale(self, operator, value):
        cutoff = fields.Datetime.now() - relativedelta(hours=self._stale_hours())
        stale = [("computed_on", "<", cutoff)]
        fresh = [("computed_on", ">=", cutoff)]
        want = value if operator in ("=", "==") else not value
        return stale if want else fresh

    @api.model
    def action_refresh_suggestions(self):
        """Ask OASIS to re-scan and repost the queue.

        Odoo cannot compute this itself — OASIS is a separate application — so
        the button calls it over HTTP at ``oasis.scan_url``. When that is not
        configured the button says exactly what to set rather than failing
        silently or pretending to work, because an operator who presses Refresh
        and sees nothing change will stop trusting the whole queue.
        """
        import json as _json
        import urllib.error
        import urllib.request

        icp = self.env["ir.config_parameter"].sudo()
        url = (icp.get_param("oasis.scan_url") or "").strip()
        if not url:
            raise UserError(_(
                "No OASIS scan endpoint is configured, so this button has "
                "nothing to call.\n\n"
                "Set the system parameter 'oasis.scan_url' to your OASIS "
                "service (Settings → Technical → System Parameters), or run "
                "the scan from OASIS itself — the queue below refreshes either "
                "way.\n\n"
                "Until then, the 'Computed' column tells you how old this plan "
                "is."))

        req = urllib.request.Request(
            url, method="POST",
            data=_json.dumps({"db": self.env.cr.dbname}).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        token = (icp.get_param("oasis.scan_token") or "").strip()
        if token:
            req.add_header("Authorization", "Bearer %s" % token)
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                body = _json.loads(r.read().decode("utf-8") or "{}")
        except urllib.error.HTTPError as e:
            raise UserError(_("OASIS refused the scan (HTTP %s): %s")
                            % (e.code, (e.read() or b"")[:200].decode("utf-8", "replace")))
        except Exception as e:
            raise UserError(_(
                "Could not reach OASIS at %s.\n\n%s\n\nOdoo must be able to "
                "reach the OASIS service on the network — inside Docker that "
                "is its service name, not localhost.") % (url, str(e)[:200]))

        return {
            "type": "ir.actions.client", "tag": "display_notification",
            "params": {"title": _("Scan complete"),
                       "message": _("%s suggestions posted.")
                                  % body.get("created", "?"),
                       "type": "success", "next": {"type": "ir.actions.act_window_close"}},
        }

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

        # The company comes from the DONOR warehouse, not from whoever happens
        # to be logged in. company_id defaults to `self.env.company` — the RPC
        # user's active company — so a scan pushed by a user sitting in company
        # A stamped every suggestion with A even where the stock lives in B.
        # That is invisible until a record rule exists, and then it is worse
        # than invisible: the rule hides the row from the very people who own
        # the stock. The donor is the right answer because the picking created
        # on approval is built from the donor's operation type, so this matches
        # the company that document will belong to.
        warehouses = self.env["stock.warehouse"].search([])
        wh = {w.code: w.id for w in warehouses}
        wh_company = {w.code: w.company_id.id for w in warehouses}
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
            # Strip the engine's no-demand sentinel HERE, at the boundary, so
            # no column, pivot or average downstream can ever see a 999.
            d_ads = float(s.get("donor_ads") or 0.0)
            r_ads = float(s.get("recipient_ads") or 0.0)
            d_cov = float(s.get("donor_cover") or 0.0)
            r_cov = float(s.get("recipient_cover") or 0.0)
            rows.append({
                "product_id": p, "quantity": s.get("quantity") or 0,
                "from_warehouse_id": f, "to_warehouse_id": t,
                "kind": "push" if str(s.get("kind", "")).lower() == "push" else "pull",
                "reason": s.get("reason") or "",
                "value_kes": s.get("value") or 0.0,
                "donor_days_cover": 0.0 if (d_ads <= 0 or d_cov >= 900) else d_cov,
                "recipient_days_cover": 0.0 if (r_ads <= 0 or r_cov >= 900) else r_cov,
                "donor_ads": d_ads,
                "recipient_ads": r_ads,
                "relief_days": float(s.get("relief_days") or 0.0),
                "is_fresh": bool(s.get("is_fresh")),
                "computed_on": computed_on or fields.Datetime.now(),
                "company_id": wh_company.get(s.get("from_code"))
                or self.env.company.id,
            })
        created = self.create(rows) if rows else self.browse()
        return {"created": len(created), "skipped": skipped}
