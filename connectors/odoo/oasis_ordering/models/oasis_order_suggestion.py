# -*- coding: utf-8 -*-
"""OASIS replenishment suggestions — a review queue that becomes draft POs.

WHY A SUGGESTION AND NOT JUST A DRAFT PO
----------------------------------------
OASIS can already write a draft purchase order straight into Odoo, and has been
able to for months. But a draft PO carries no reasoning: the buyer sees "order
48 units" with no account of what is on the shelf now, how fast it sells, when
the supplier actually delivers, or what is already in transit. They then either
approve everything blindly or nothing at all, and neither is worth paying for.

So a suggestion holds the ARGUMENT and approving it produces the native
document. The reasoning is the product; the purchase order is just its output.

WHAT MAKES ORDERING DIFFERENT FROM TRANSFERS
--------------------------------------------
A transfer suggestion stands alone: donor has it, recipient needs it, move it.
An order line does NOT stand alone. OASIS admits a line onto a purchase order
only after the whole supplier basket clears that supplier's minimum — units and
value. So a buyer who rejects half a basket can leave the rest under the
minimum that justified including it, and the first they would hear of it is the
supplier refusing the order or charging small-order carriage.

That is why approval is basket-aware (see ``_check_supplier_minimum``). The
queue is still line-by-line, because the reasoning is per line and the ability
to strike one item is the point of a review — but the consequence of striking
it is stated before the document is created, not after.

WHAT APPROVAL DOES
------------------
Creates a DRAFT purchase order, one per supplier per store. Nothing is sent and
no money is committed: the buyer still confirms it in Odoo's own screens. OASIS
proposes, a human presses the button — the same contract the adapter has always
kept.
"""

import logging
from collections import defaultdict

from dateutil.relativedelta import relativedelta

from odoo import _, api, fields, models
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class OasisOrderSuggestion(models.Model):
    _name = "oasis.order.suggestion"
    _description = "OASIS Replenishment Suggestion"
    _order = "value desc, id desc"
    _rec_name = "display_name"

    # ── what to buy ───────────────────────────────────────────────────────
    product_id = fields.Many2one("product.product", required=True,
                                 ondelete="cascade", index=True)
    quantity = fields.Float("Order quantity", required=True,
                            digits="Product Unit of Measure")
    uom_id = fields.Many2one("uom.uom", related="product_id.uom_id", readonly=True)
    warehouse_id = fields.Many2one("stock.warehouse", "Store", required=True,
                                   ondelete="cascade", index=True,
                                   help="The store this stock is for. The order "
                                        "is aimed at this store's own receipt "
                                        "operation, so the goods arrive where "
                                        "the demand was measured.")
    partner_id = fields.Many2one("res.partner", "Supplier", required=True,
                                 ondelete="cascade", index=True)

    # ── why ───────────────────────────────────────────────────────────────
    reason = fields.Text("Why", readonly=True,
                         help="The argument for this order, in plain terms.")
    categ_id = fields.Many2one(related="product_id.categ_id", store=True,
                               string="Category", readonly=True, index=True)
    unit_cost = fields.Monetary("Unit cost", currency_field="currency_id")
    value = fields.Monetary("Order value", currency_field="currency_id",
                            help="Quantity x unit cost — what approving this "
                                 "line commits.")
    currency_id = fields.Many2one("res.currency",
                                  default=lambda s: s.env.company.currency_id)

    # ── the position this order answers ───────────────────────────────────
    # Days of cover is meaningless where nothing sells. The engine carries 999
    # as its internal "no demand" sentinel and it is stripped on ingestion, the
    # same way the transfer queue does it: a 999 reaching a column poisons every
    # average in the pivot and reads to a buyer as a real figure.
    current_stock = fields.Float("On hand", readonly=True, group_operator="avg")
    avg_daily_sales = fields.Float("Sales/day", readonly=True, digits=(12, 3),
                                   group_operator="avg")
    days_cover = fields.Float("Cover (days)", readonly=True, group_operator="avg",
                              help="How long the stock on hand lasts at the "
                                   "current rate. Zero sales means no cover can "
                                   "be computed, not zero days.")
    cover_label = fields.Char("Cover", compute="_compute_cover_label")
    lead_time_days = fields.Float("Lead time (days)", readonly=True,
                                  group_operator="avg",
                                  help="How long this supplier actually takes, "
                                       "measured from their own delivery "
                                       "history rather than declared.")
    target_cover_days = fields.Float("Ordering up to (days)", readonly=True,
                                     group_operator="avg",
                                     help="The cover this quantity is intended "
                                          "to restore — lead time plus the "
                                          "supplier's measured rhythm.")
    on_order_qty = fields.Float("Already on order", readonly=True,
                                group_operator="sum",
                                help="Units already on an open purchase order "
                                     "for this store. Subtracted before "
                                     "recommending — without it the same need "
                                     "is bought twice.")
    on_order_eta_days = fields.Float("Due in (days)", readonly=True,
                                     group_operator="avg")
    is_fresh = fields.Boolean("Perishable", index=True)

    # ── the basket this line belongs to ───────────────────────────────────
    # Carried per row so approval can check the supplier's minimum without
    # calling back to OASIS. Without these the module would have to either
    # ignore the minimum — the failure this queue exists to prevent — or make a
    # network call inside a button.
    pack_size = fields.Float("Pack size", readonly=True,
                             help="Orders are placed in whole packs. A "
                                  "quantity below one pack cannot be bought.")
    supplier_min_units = fields.Float("Supplier minimum (units)", readonly=True,
                                      group_operator="avg")
    supplier_min_value = fields.Monetary("Supplier minimum (value)",
                                         currency_field="currency_id",
                                         group_operator="avg")

    @api.depends("days_cover", "avg_daily_sales")
    def _compute_cover_label(self):
        for r in self:
            r.cover_label = ("not selling" if r.avg_daily_sales <= 0
                             else "%.0f d" % r.days_cover)

    # ── lifecycle ─────────────────────────────────────────────────────────
    state = fields.Selection(
        #: DONE means the order was CONFIRMED, not received.
        #:
        #: The decision this queue records is "should we buy this". That
        #: decision is finished the moment the purchase order is confirmed and
        #: the money is committed — what happens on the loading bay afterwards
        #: is the receipt's business, and Odoo already tracks it on the PO.
        #: Waiting for goods to land would leave every suggestion pending for
        #: the length of a lead time, which is precisely when a buyer needs the
        #: queue to be about today.
        [("new", "To review"), ("approved", "Ordered (draft)"),
         ("done", "Confirmed"), ("rejected", "Rejected")],
        default="new", required=True, index=True)
    purchase_order_id = fields.Many2one("purchase.order", "Purchase order",
                                        readonly=True)
    purchase_state = fields.Selection(related="purchase_order_id.state",
                                      readonly=True, string="Order status")
    computed_on = fields.Datetime("Computed", default=fields.Datetime.now,
                                  readonly=True)
    company_id = fields.Many2one("res.company", default=lambda s: s.env.company,
                                 required=True, index=True)

    @api.depends("product_id", "quantity", "warehouse_id", "partner_id")
    def _compute_display_name(self):
        for r in self:
            r.display_name = "%s %s -> %s" % (
                r.quantity or 0, r.product_id.display_name or "",
                r.warehouse_id.code or "")

    display_name = fields.Char(compute="_compute_display_name", store=False)

    # ── the basket guard ──────────────────────────────────────────────────
    def _check_supplier_minimum(self, partner, warehouse, recs):
        """Refuse a basket that no longer clears the supplier's minimum.

        THE FAILURE THIS PREVENTS. OASIS's minimum-order gate runs in two
        stages, and the second is supplier-level: a line is admitted onto a
        purchase order only because the WHOLE basket for that supplier clears
        their minimum units and value. Strike half the basket in review and the
        remainder can fall under it — at which point the supplier refuses the
        order, or accepts it and adds small-order carriage that quietly turns a
        good buying decision into a bad one.

        Odoo cannot catch this: it has no idea the lines were justified
        together. Nothing errors, the PO is perfectly valid, and the buyer
        learns about it from the supplier days later.

        So it is refused here, naming the shortfall AND what would close it —
        the lines from this same supplier still sitting in the queue. Approving
        below the minimum is still allowed, but as a deliberate second action
        rather than by accident.
        """
        min_units = max(recs.mapped("supplier_min_units") or [0.0])
        min_value = max(recs.mapped("supplier_min_value") or [0.0])
        if min_units <= 0 and min_value <= 0:
            return

        units = sum(recs.mapped("quantity"))
        value = sum(recs.mapped("value"))
        short_units = min_units > 0 and units < min_units
        short_value = min_value > 0 and value < min_value
        if not (short_units or short_value):
            return

        # What would close the gap: the same supplier's lines, same store,
        # still awaiting review. A refusal that only says "too small" leaves
        # the buyer to work out the fix by hand.
        rest = self.search([
            ("state", "=", "new"), ("partner_id", "=", partner.id),
            ("warehouse_id", "=", warehouse.id), ("id", "not in", recs.ids)])

        lines = []
        if short_units:
            lines.append(_("  • units: approving %(have)s, %(who)s needs %(want)s")
                         % {"have": "%.0f" % units, "who": partner.display_name,
                            "want": "%.0f" % min_units})
        if short_value:
            lines.append(_("  • value: approving %(have)s, %(who)s needs %(want)s")
                         % {"have": "%.0f" % value, "who": partner.display_name,
                            "want": "%.0f" % min_value})

        if rest:
            fix = _(
                "\n%(n)d more line(s) from %(who)s for %(store)s are still "
                "awaiting review, worth %(units)s units and %(value)s. "
                "Approving enough of them clears the minimum.") % {
                    "n": len(rest), "who": partner.display_name,
                    "store": warehouse.code,
                    "units": "%.0f" % sum(rest.mapped("quantity")),
                    "value": "%.0f" % sum(rest.mapped("value"))}
        else:
            fix = _(
                "\nThere are no other lines for %(who)s at %(store)s to add. "
                "This basket is genuinely too small to buy — which is exactly "
                "the case OASIS Transfers exists to answer: move the stock from "
                "another store instead of ordering it.") % {
                    "who": partner.display_name, "store": warehouse.code}

        raise UserError(_(
            "This order is under %(who)s's minimum.\n\n"
            "%(lines)s\n"
            "%(fix)s\n\n"
            "OASIS included these lines because the FULL basket for this "
            "supplier cleared their minimum. Approving only part of it can put "
            "the rest below — the supplier then refuses the order, or adds "
            "small-order carriage.\n\n"
            "Use 'Approve below minimum' if you know this supplier will take "
            "it anyway.")
            % {"who": partner.display_name, "lines": "\n".join(lines),
               "fix": fix})

    # ── actions ───────────────────────────────────────────────────────────
    def action_approve(self):
        """Turn the selected suggestions into DRAFT purchase orders."""
        return self._approve(check_minimum=True)

    def action_approve_below_minimum(self):
        """Approve anyway, having been told the basket is under the minimum.

        A deliberate second action, not a checkbox: a buyer who knows this
        supplier will take a short order should be able to say so, and the
        record should show that they were told first.
        """
        return self._approve(check_minimum=False)

    def _approve(self, check_minimum=True):
        todo = self.filtered(lambda r: r.state == "new")
        if not todo:
            raise UserError(_("Nothing to approve — these are already handled."))

        # ONE ORDER PER SUPPLIER PER STORE. A purchase order has exactly one
        # partner, and it must be received at the store the demand was measured
        # at — so the basket is (supplier, store), not one or the other.
        baskets = defaultdict(lambda: self.browse())
        for r in todo:
            baskets[(r.partner_id, r.warehouse_id)] |= r

        made = self.env["purchase.order"].browse()
        for (partner, warehouse), recs in baskets.items():
            # THE SITE-SCOPING LESSON, kept. purchase.order.picking_type_id is
            # required, so it can never be empty — which means omitting it does
            # not fail, it silently takes the DEFAULT warehouse's receipt type.
            # A PO computed from one store's stock and demand then delivers to
            # another, and nothing in the document says so.
            picking_type = warehouse.in_type_id
            if not picking_type:
                raise UserError(_(
                    "%s has no receipt operation type, so Odoo has nowhere to "
                    "book the goods from this order.") % warehouse.code)
            if warehouse.company_id != picking_type.company_id:
                raise UserError(_(
                    "%s and its receipt operation belong to different "
                    "companies; Odoo cannot book this order.") % warehouse.code)

            archived = recs.product_id.filtered(lambda p: not p.active)
            if archived:
                raise UserError(_(
                    "These products are archived, so they are no longer part of "
                    "the range:\n\n%s\n\nUn-archive them if the range decision "
                    "has changed; otherwise they should not be re-ordered.")
                    % "\n".join("  • " + p.display_name for p in archived))

            if check_minimum:
                self._check_supplier_minimum(partner, warehouse, recs)

            # ONE PRODUCT IS ONE ORDER LINE. The same product can appear twice
            # in a scan; a purchase order carrying two lines for it makes the
            # buyer work out for themselves that they are not a duplicate.
            per_product = defaultdict(float)
            cost_of = {}
            for r in recs:
                per_product[r.product_id] += r.quantity
                cost_of.setdefault(r.product_id, r.unit_cost)

            order = self.env["purchase.order"].create({
                "partner_id": partner.id,
                "company_id": warehouse.company_id.id,
                "picking_type_id": picking_type.id,
                "origin": "OASIS %s" % warehouse.code,
                "order_line": [(0, 0, {
                    "product_id": product.id,
                    "name": product.display_name,
                    "product_qty": qty,
                    "product_uom": product.uom_po_id.id or product.uom_id.id,
                    "price_unit": cost_of.get(product) or 0.0,
                    "date_planned": fields.Datetime.now(),
                }) for product, qty in per_product.items()],
            })
            recs.write({"state": "approved", "purchase_order_id": order.id})
            made |= order

        return {
            "type": "ir.actions.act_window",
            "name": _("Draft purchase orders"),
            "res_model": "purchase.order",
            "view_mode": "tree,form",
            "domain": [("id", "in", made.ids)],
        }

    def _mark_confirmed(self):
        """The order was placed, so the suggestion is finished rather than pending."""
        live = self.filtered(lambda r: r.state == "approved")
        if not live:
            return
        live.write({"state": "done"})
        _logger.info("OASIS: %d suggestion(s) confirmed — the order was placed",
                     len(live))

    def _release_from_dead_order(self, reason):
        """The document died; the suggestion must stop claiming it is handled.

        Mirrors the transfer queue exactly. A row reading `approved` tells the
        buyer this line is on an order somebody is placing. Cancel or delete
        that order and the row went on saying so — pointing, in the deleted
        case, at nothing. Releasing back to `new` is the truthful state and it
        is self-healing: the next scan clears pending rows and re-proposes from
        current stock, so a need that still exists comes back and one that has
        gone does not.
        """
        live = self.filtered(lambda r: r.state in ("approved", "done"))
        if not live:
            return
        live.write({"state": "new", "purchase_order_id": False})
        _logger.info("OASIS: released %d suggestion(s) back to the queue — %s",
                     len(live), reason)

    def action_reject(self):
        self.filtered(lambda r: r.state == "new").write({"state": "rejected"})

    def action_reset(self):
        """Put a rejected suggestion back in the queue."""
        stuck = self.filtered(lambda r: r.state in ("approved", "done"))
        if stuck:
            raise UserError(_(
                "%d of these are already on a purchase order. Cancel the order "
                "in Purchase first — it sends the suggestion back here by "
                "itself.") % len(stuck))
        self.write({"state": "new"})

    def action_open_purchase_order(self):
        self.ensure_one()
        if not self.purchase_order_id:
            raise UserError(_("This suggestion has not been approved yet."))
        return {
            "type": "ir.actions.act_window",
            "res_model": "purchase.order",
            "res_id": self.purchase_order_id.id,
            "view_mode": "form",
        }

    # ── staleness ─────────────────────────────────────────────────────────
    #: Longer than the transfer queue's half hour, and deliberately so.
    #:
    #: A transfer is approved against stock that is selling underneath it, so a
    #: plan minutes old can already be wrong. An order is a decision about the
    #: next lead time — days — and the inputs behind it (90-day demand, measured
    #: supplier rhythm) barely move within a day. Re-scanning every half hour
    #: would train a buyer to ignore the staleness flag, which is worse than
    #: not having one.
    _DEFAULT_STALE_HOURS = 12.0

    is_stale = fields.Boolean("Out of date", compute="_compute_is_stale",
                              search="_search_is_stale")

    def _stale_hours(self):
        try:
            return float(self.env["ir.config_parameter"].sudo().get_param(
                "oasis.order_scan_stale_hours") or self._DEFAULT_STALE_HOURS)
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
        """Ask OASIS to recompute the ORDER plan and repost the queue.

        One endpoint serves both queues, so the request has to say which plan it
        wants. Without `kind` the service computes transfers — the original
        behaviour, kept so an older OASIS still answers a Refresh from the
        transfer queue — and a button here that omitted it would recompute the
        wrong plan and report success.
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
                "Set it in Settings → OASIS → Connection, or run the scan from "
                "OASIS itself — the queue below refreshes either way.\n\n"
                "Until then, the 'Computed' column tells you how old this plan "
                "is."))

        req = urllib.request.Request(
            url, method="POST",
            data=_json.dumps({"db": self.env.cr.dbname,
                              "kind": "orders"}).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        token = (icp.get_param("oasis.scan_token") or "").strip()
        if token:
            req.add_header("Authorization", "Bearer %s" % token)
        try:
            with urllib.request.urlopen(req, timeout=600) as r:
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
                       "type": "success",
                       "next": {"type": "ir.actions.act_window_close"}},
        }

    # ── ingestion ─────────────────────────────────────────────────────────
    @api.model
    def oasis_replace_queue(self, suggestions, computed_on=None):
        """Replace the pending queue with a fresh scan. Called by OASIS.

        Only rows still awaiting review are cleared: anything approved has
        become a document a buyer may already be placing, and anything rejected
        is a decision worth keeping — re-proposing it next scan would make the
        queue an argument the buyer has to win repeatedly.

        Products and warehouses are matched on their codes so OASIS never has to
        know Odoo's database ids.
        """
        self.search([("state", "=", "new")]).unlink()

        warehouses = self.env["stock.warehouse"].search([])
        wh = {w.code: w for w in warehouses}
        codes = [s.get("item_code") for s in suggestions if s.get("item_code")]
        prods = self.env["product.product"].search(
            [("default_code", "in", list(set(codes)))])
        pid = {p.default_code: p.id for p in prods}

        rows, skipped = [], defaultdict(int)
        for s in suggestions:
            p = pid.get(s.get("item_code"))
            w = wh.get(s.get("store_code"))
            partner = self._resolve_supplier(s)
            if not p:
                skipped["unknown product"] += 1
                continue
            if not w:
                skipped["unknown store"] += 1
                continue
            if not partner:
                # NEVER guess a vendor. The adapter's PO writer falls back to
                # "the first partner with supplier_rank > 0" when it cannot
                # resolve one, which is defensible for a machine-to-machine
                # push and indefensible here: a review queue that attributes a
                # line to an arbitrary supplier invites a buyer to approve an
                # order to the wrong company.
                skipped["unresolved supplier"] += 1
                continue

            ads = float(s.get("avg_daily_sales") or 0.0)
            cover = float(s.get("days_cover") or 0.0)
            eta = float(s.get("on_order_eta_days") or 0.0)
            rows.append({
                "product_id": p,
                "warehouse_id": w.id,
                "partner_id": partner,
                "quantity": s.get("quantity") or 0,
                "reason": s.get("reason") or "",
                "unit_cost": float(s.get("unit_cost") or 0.0),
                "value": float(s.get("value") or 0.0),
                "current_stock": float(s.get("current_stock") or 0.0),
                "avg_daily_sales": ads,
                # Strip the engine's no-demand sentinel HERE, at the boundary,
                # so no column, pivot or average downstream can ever see a 999.
                "days_cover": 0.0 if (ads <= 0 or cover >= 900) else cover,
                "lead_time_days": float(s.get("lead_time_days") or 0.0),
                "target_cover_days": float(s.get("target_cover_days") or 0.0),
                "on_order_qty": float(s.get("on_order_qty") or 0.0),
                "on_order_eta_days": 0.0 if eta >= 900 else eta,
                "is_fresh": bool(s.get("is_fresh")),
                "pack_size": float(s.get("pack_size") or 0.0),
                "supplier_min_units": float(s.get("supplier_min_units") or 0.0),
                "supplier_min_value": float(s.get("supplier_min_value") or 0.0),
                "computed_on": computed_on or fields.Datetime.now(),
                # The company comes from the STORE, not from whoever happens to
                # be logged in: company_id would otherwise default to the RPC
                # user's active company and the record rule would hide the row
                # from the very people who own the stock.
                "company_id": w.company_id.id or self.env.company.id,
            })
        created = self.create(rows) if rows else self.browse()
        return {"created": len(created), "skipped": sum(skipped.values()),
                "skipped_detail": dict(skipped)}

    @api.model
    def _resolve_supplier(self, s):
        """Odoo's partner id for this line's supplier, or None.

        OASIS carries whatever the source ERP calls a supplier. Reading through
        the Odoo adapter that is already the partner id as a string, so the
        common path is a direct lookup; the name match is the fallback for a
        client whose supplier codes come from elsewhere.
        """
        code = str(s.get("supplier_code") or "").strip()
        if code.isdigit():
            found = self.env["res.partner"].browse(int(code)).exists()
            if found:
                return found.id
        name = (s.get("supplier_name") or "").strip()
        if name:
            found = self.env["res.partner"].search(
                [("name", "=", name)], limit=1)
            if found:
                return found.id
        return None
