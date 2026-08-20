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

    def _collect_sales(self, since):
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
        moves, more = self._batch("stock.move", [("state", "=", "done")], since)
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

    # ── consoles inside Odoo ─────────────────────────────────────────────
    _CONSOLES = {
        "intel": ("OASIS Intelligence", "oasis.console_intel_url",
                  "http://localhost:8510"),
        "ops": ("OASIS Operations", "oasis.console_ops_url",
                "http://localhost:8500"),
        "command": ("OASIS Command Center", "oasis.console_command_url",
                    "http://localhost:8501"),
    }

    @api.model
    def open_console(self, kind):
        """Client action embedding an OASIS console (Streamlit) in Odoo.

        The iframe URL resolves in the USER'S BROWSER, so the console just has
        to be reachable from the user's machine — it can run beside Odoo on the
        host, in a container, or on another server.

        Note: ir.model.access.csv grants base.group_user WRITE (not just read)
        on this AbstractModel — Odoo's 'code' server actions require model
        write access to execute at all, even though oasis.sync stores no
        records, so there's nothing a write grant actually exposes.
        """
        name, param, default = self._CONSOLES[kind]
        base = (self._param(param) or default).rstrip("/")
        return {
            "type": "ir.actions.client",
            "tag": "oasis_embed",
            "name": name,
            "params": {"url": base + "/?embed=true"},
        }
