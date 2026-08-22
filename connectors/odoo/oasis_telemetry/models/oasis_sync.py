"""
OASIS sync engine (Odoo side).

Collects new POS sales, goods receipts, and (optionally) stock-on-hand snapshots
since the last watermark, maps them with the pure ``mapping`` module, and ships
them with the stdlib ``push_client``. Idempotent end-to-end: the hub dedups on
(store, source_ref) and we only advance the watermark after a successful push,
so a crash mid-run simply re-sends safely next time.
"""

import logging

from odoo import models, api, fields

from .. import mapping, push_client

_logger = logging.getLogger(__name__)

_EPOCH = "1970-01-01 00:00:00"
_ODOO_DT = "%Y-%m-%d %H:%M:%S"


class OasisSync(models.AbstractModel):
    _name = "oasis.sync"
    _description = "OASIS Cloud Hub sync engine"

    # ── config accessors ─────────────────────────────────────────────────
    def _param(self, key, default=None):
        return self.env["ir.config_parameter"].sudo().get_param(key, default)

    def _set_param(self, key, value):
        self.env["ir.config_parameter"].sudo().set_param(key, value)

    def _client(self):
        hub = self._param("oasis.hub_url")
        token = self._param("oasis.ingest_token")
        if not hub or not token:
            raise ValueError("OASIS hub URL and ingest token must be configured.")
        return push_client.HubPushClient(hub, token)

    # ── product resolution (batched) ─────────────────────────────────────
    def _product_map(self, product_ids):
        """product_id → enriched record dict for the mapping module."""
        if not product_ids:
            return {}
        products = self.env["product.product"].browse(list(set(product_ids)))
        out = {}
        for p in products:
            seller = p.seller_ids[:1]
            supplier_ref = None
            if seller:
                partner = seller.partner_id
                supplier_ref = partner.ref or partner.name
            brand = getattr(p, "product_brand_id", False)
            out[p.id] = {
                "id": p.id,
                "default_code": p.default_code,
                "name": p.display_name,
                "categ_id": [p.categ_id.id, p.categ_id.display_name] if p.categ_id else False,
                "list_price": p.list_price,
                "supplier_ref": supplier_ref,
                "product_brand_id": [brand.id, brand.display_name] if brand else False,
            }
        return out

    # ── collectors ───────────────────────────────────────────────────────
    def _batch(self, model, domain, since):
        """Records changed since the watermark, capped, oldest first.

        WHY A CAP AT ALL
        ----------------
        These searches were unbounded. On a fresh connection to an instance
        with real history that is one query for everything ever done: measured
        on a 14-store depot, 240,966 done stock moves in a single pass, which
        killed the Odoo worker outright. It could then never record how far it
        had got, so every subsequent run tried the same impossible thing and
        died the same way — while holding a row lock on ir_cron that blocked
        addon upgrades. A first sync is exactly when this bites a customer.

        WHY THE BOUNDARY IS WIDENED
        ---------------------------
        Capping alone would lose data. The watermark is a TIMESTAMP, so if a
        batch ends in the middle of a second, the records sharing that second
        are skipped forever by the next run's ``write_date >``. So when a batch
        comes back full, every record sharing its last timestamp is pulled in
        too. That also guarantees progress: a batch can never be entirely
        discarded for sharing one timestamp, which a naive "drop the tail" fix
        would do to a bulk import written in a single second.
        """
        limit = self._batch_size()
        recs = self.env[model].search(domain + [("write_date", ">", since)],
                                      order="write_date asc, id asc", limit=limit)
        if len(recs) < limit:
            return recs, False
        edge = recs[-1].write_date
        recs |= self.env[model].search(domain + [("write_date", "=", edge)])
        return recs, True

    def _batch_size(self):
        try:
            return max(100, int(self._param("oasis.sync_batch_size", 5000)))
        except (TypeError, ValueError):
            return 5000

    @staticmethod
    def _watermark_of(recs, fallback):
        """How far this run actually got — never how far it hoped to get."""
        return (fields.Datetime.to_string(max(recs.mapped("write_date")))
                if recs else fallback)

    def _pos_installed(self):
        """Whether this Odoo actually has Point of Sale.

        `point_of_sale` is no longer a hard dependency. It was one purely so
        that `pos.order.line` could be read here, which locked every Odoo
        retailer NOT running Odoo POS out of installing a module whose headline
        feature is stock transfers — and transfers need nothing from POS.

        Sell-through is still captured without it: every Odoo sale, whatever
        the front end, depletes inventory through a stock move into a customer
        location, and `map_stock_move` already maps that to a 'sale'.
        """
        return "pos.order.line" in self.env

    def _sales_counted_from_pos(self):
        """Whether the till feed is the one reporting sales this run.

        Decides, for `_collect_receipts`, whether a customer-bound move would
        be a SECOND count of a sale the till already reported.
        """
        return (self._pos_installed()
                and self._param("oasis.send_sales") in
                ("True", "1", "true", True, None))

    def _collect_sales(self, since):
        if not self._pos_installed():
            _logger.info("Point of Sale is not installed — sell-through is "
                         "collected from customer stock moves instead.")
            return [], since, False
        lines, more = self._batch("pos.order.line", [], since)
        pmap = self._product_map(lines.mapped("product_id").ids)
        movements = []
        for ln in lines:
            prod = pmap.get(ln.product_id.id)
            if not prod:
                continue
            movements.append(mapping.map_pos_order_line(
                {"id": ln.id, "qty": ln.qty, "price_unit": ln.price_unit},
                prod, order_date=fields.Datetime.to_string(ln.order_id.date_order),
            ))
        return movements, self._watermark_of(lines, since), more

    def _collect_receipts(self, since):
        dom = [("state", "=", "done")]

        # DO NOT STREAM A POS SALE TWICE.
        #
        # Closing a POS order creates a picking (linked by pos_order_id) whose
        # moves land in a customer location, and map_stock_move maps those to
        # 'sale' — the SAME units pos.order.line already reported. Both feeds
        # default to ON and carry different source_refs, so the hub cannot
        # dedupe them: it simply received every till sale twice.
        #
        # This is the identical defect already fixed on the READ side in
        # odoo_adapter._sales_by_product; it survived here on the WRITE side.
        # The exclusion is only correct while POS is actually reporting: with
        # no POS, or with the sales feed switched off, those customer moves are
        # the only record of the sale and must be kept.
        if self._sales_counted_from_pos():
            try:
                self.env["stock.picking"]._fields["pos_order_id"]
                # KEEP MOVES WITH NO PICKING. A dotted domain walks the
                # relation, so `picking_id.pos_order_id = False` silently drops
                # every move whose picking_id is NULL — the join has nothing to
                # walk. Those are ordinary sales, and excluding them zeroed
                # demand outright when this was first written the other way.
                dom += ["|", ("picking_id", "=", False),
                        ("picking_id.pos_order_id", "=", False)]
            except KeyError:
                _logger.debug("no pos_order_id on stock.picking; streaming "
                              "customer moves unfiltered")

        moves, more = self._batch("stock.move", dom, since)
        pmap = self._product_map(moves.mapped("product_id").ids)
        movements = []
        for mv in moves:
            prod = pmap.get(mv.product_id.id)
            if not prod:
                continue
            movements.append(mapping.map_stock_move({
                "id": mv.id, "product_qty": mv.product_qty,
                "date": fields.Datetime.to_string(mv.date),
                "location_usage": mv.location_id.usage,
                "location_dest_usage": mv.location_dest_id.usage,
                "price_unit": getattr(mv, "price_unit", 0.0),
            }, prod))
        return movements, self._watermark_of(moves, since), more

    def _collect_on_hand(self):
        """A full snapshot, capped. Unlike the movement feeds this is not
        incremental — it is the current position — so a cap TRUNCATES rather
        than defers, and that is worth saying out loud in the log instead of
        quietly shipping a partial picture that looks complete."""
        limit = self._batch_size()
        domain = [("location_id.usage", "=", "internal"), ("quantity", "!=", 0)]
        total = self.env["stock.quant"].search_count(domain)
        if total > limit:
            _logger.warning(
                "OASIS on-hand snapshot TRUNCATED: %s quants on hand, sending "
                "%s. Raise oasis.sync_batch_size to send the whole position.",
                total, limit)
        quants = self.env["stock.quant"].search(
            domain, order="write_date desc, id desc", limit=limit)
        pmap = self._product_map(quants.mapped("product_id").ids)
        movements = []
        for q in quants:
            prod = pmap.get(q.product_id.id)
            if not prod:
                continue
            movements.append(mapping.map_stock_quant({
                "id": q.id, "quantity": q.quantity,
                "in_date": fields.Datetime.to_string(q.in_date or q.write_date),
            }, prod))
        return movements

    # ── entrypoints ──────────────────────────────────────────────────────
    @api.model
    def run_sync(self):
        """One incremental push. Called by the cron and the 'Sync now' button."""
        if self._param("oasis.enabled") not in ("True", "1", "true", True):
            _logger.info("OASIS streaming disabled — skipping sync.")
            return {"skipped": True}

        client = self._client()
        run_started = fields.Datetime.now().strftime(_ODOO_DT)
        totals = {"accepted": 0, "duplicates": 0}

        # The watermark advances to WHAT WAS PROCESSED, never to when the run
        # began. With a batch cap those differ, and writing run_started would
        # silently skip every record the cap left behind — the sync would
        # report success while losing the bulk of the history it exists to
        # stream. `more` says another batch is waiting, so an operator can see
        # a backlog draining instead of guessing.
        totals["pending"] = False

        if self._param("oasis.send_sales") in ("True", "1", "true", True, None):
            wm = self._param("oasis.watermark.sales", _EPOCH)
            movs, reached, more = self._collect_sales(wm)
            if movs:
                res = client.push(movs)
                totals["accepted"] += res["accepted"]
                totals["duplicates"] += res["duplicates"]
            self._set_param("oasis.watermark.sales", reached)
            totals["pending"] = totals["pending"] or more

        if self._param("oasis.send_receipts") in ("True", "1", "true", True, None):
            wm = self._param("oasis.watermark.receipts", _EPOCH)
            movs, reached, more = self._collect_receipts(wm)
            if movs:
                res = client.push(movs)
                totals["accepted"] += res["accepted"]
                totals["duplicates"] += res["duplicates"]
            self._set_param("oasis.watermark.receipts", reached)
            totals["pending"] = totals["pending"] or more

        if self._param("oasis.send_on_hand") in ("True", "1", "true", True):
            movs = self._collect_on_hand()
            if movs:
                res = client.push(movs)
                totals["accepted"] += res["accepted"]
                totals["duplicates"] += res["duplicates"]

        if totals.get("pending"):
            _logger.info("OASIS sync done: %s — MORE PENDING, the next run "
                         "continues from this watermark", totals)
        else:
            _logger.info("OASIS sync done: %s", totals)
        return totals

    @api.model
    def _cron_sync(self):
        try:
            self.run_sync()
        except Exception:                       # never let a push error break the cron
            _logger.exception("OASIS scheduled sync failed")

    # ── THERE IS NO CONSOLE EMBED, AND THERE MUST NOT BE ONE ─────────────
    #
    # An `open_console(kind)` used to live here, returning an `ir.actions.client`
    # that rendered an OASIS Streamlit console in an iframe filling Odoo's
    # content area. The menus pointing at it were removed on the grounds that
    # embedding the consoles "ships the entire product into a window inside
    # Odoo and gives away every module to anyone who installs the connector" —
    # but only the MENUS went. The method stayed, the client action stayed
    # registered by the asset bundle on every backend page, and
    # ir.model.access.csv grants base.group_user WRITE on this model, so the
    # action remained callable by ANY internal user:
    #
    #     oasis.sync.open_console('intel')
    #     -> {'tag': 'oasis_embed', 'params': {'url': '…:8510/?embed=true'}}
    #
    # Removing a menu hides an entrance; it does not close a door. The method,
    # the JS/XML assets, and the three console URL settings are all gone now.
    #
    # What belongs in Odoo is the part an Odoo user does in Odoo: review what
    # OASIS proposes and approve it into native documents. The intelligence
    # that produced those proposals stays outside, which is what is being sold.
