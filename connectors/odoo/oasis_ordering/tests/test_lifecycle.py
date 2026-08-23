"""The queue must never say something untrue about a document it created.

A row reading "Ordered (draft)" tells a buyer this line is on an order somebody
is placing. Confirm that order and the row should close; cancel or delete it and
the row must stop claiming the line was bought — otherwise a need that still
exists sits invisible behind a status that is a lie.
"""

from odoo.tests.common import tagged

from .common import OasisOrderCase


@tagged("post_install", "-at_install")
class TestLifecycle(OasisOrderCase):

    def test_confirming_the_order_finishes_the_suggestion(self):
        s = self._approved()
        s.purchase_order_id.button_confirm()
        self.assertEqual(s.state, "done")

    def test_cancelling_the_order_returns_the_line_to_the_queue(self):
        s = self._approved()
        s.purchase_order_id.button_cancel()
        self.assertEqual(s.state, "new")
        self.assertFalse(s.purchase_order_id,
                         "the suggestion still points at a cancelled order")

    def test_cancelling_a_CONFIRMED_order_also_reopens_the_need(self):
        """Cancelling after confirmation means the goods are not coming. The
        need is real again, so the line belongs back on the queue — unlike a
        received transfer, where the stock has physically moved."""
        s = self._approved()
        po = s.purchase_order_id
        po.button_confirm()
        self.assertEqual(s.state, "done")
        po.button_cancel()
        self.assertEqual(s.state, "new")

    def test_a_live_order_cannot_be_deleted_out_from_under_the_queue(self):
        """Odoo refuses to delete a purchase order that has not been cancelled
        — so the queue can never be orphaned by a straight delete. Worth
        asserting rather than assuming: the transfer side has no such rule, a
        picking CAN simply be deleted, and that difference is exactly the kind
        of thing that changes between Odoo versions."""
        from odoo.exceptions import UserError
        s = self._approved()
        with self.assertRaises(UserError):
            s.purchase_order_id.unlink()
        self.assertEqual(s.state, "approved")

    def test_deleting_a_cancelled_order_leaves_the_line_on_the_queue(self):
        """Cancel then delete is the reachable path. The cancel already
        released the line; the delete must not resurrect a link to a document
        that no longer exists."""
        s = self._approved()
        po = s.purchase_order_id
        po.button_cancel()
        po.unlink()
        self.assertEqual(s.state, "new")
        self.assertFalse(s.purchase_order_id,
                         "the suggestion points at a deleted order")

    def test_a_rejected_line_is_untouched_by_any_of_this(self):
        s = self._suggest(qty=5)
        s.action_reject()
        other = self._approved()
        other.purchase_order_id.button_cancel()
        self.assertEqual(s.state, "rejected",
                         "a rejection is a decision and must survive")


@tagged("post_install", "-at_install")
class TestIngestion(OasisOrderCase):

    def test_a_scan_replaces_only_the_pending_queue(self):
        pending = self._suggest(qty=1)
        rejected = self._suggest(qty=2)
        rejected.action_reject()
        ordered = self._approved(qty=3)

        self.Suggestion.oasis_replace_queue(
            [self._row("OAS-WIDGET", "OAS-1")])

        self.assertFalse(pending.exists(), "a pending row survived the rescan")
        self.assertTrue(rejected.exists(), "a rejection was thrown away")
        self.assertTrue(ordered.exists(), "an ordered line was thrown away")

    def test_the_no_demand_sentinel_never_reaches_a_column(self):
        """999 is the engine's internal 'no demand' marker. Letting it through
        poisons every average in the pivot and reads to a buyer as a real
        figure."""
        self.Suggestion.oasis_replace_queue([
            self._row("OAS-WIDGET", "OAS-1", avg_daily_sales=0.0,
                      days_cover=999.0, on_order_eta_days=999.0)])
        row = self.Suggestion.search([("state", "=", "new")], limit=1)
        self.assertEqual(row.days_cover, 0.0)
        self.assertEqual(row.on_order_eta_days, 0.0)
        self.assertEqual(row.cover_label, "not selling")

    def test_an_unknown_product_is_skipped_and_counted(self):
        res = self.Suggestion.oasis_replace_queue([
            self._row("NO-SUCH-SKU", "OAS-1")])
        self.assertEqual(res["created"], 0)
        self.assertEqual(res["skipped"], 1)
        self.assertIn("unknown product", res["skipped_detail"])

    def test_an_unresolved_supplier_is_skipped_rather_than_guessed(self):
        """The adapter's PO writer falls back to 'the first partner with
        supplier_rank > 0' when it cannot resolve a vendor. That is defensible
        machine-to-machine and indefensible in a review queue: it invites a
        buyer to approve an order to a company that was never chosen."""
        res = self.Suggestion.oasis_replace_queue([
            self._row("OAS-WIDGET", "OAS-1", supplier_name="Nobody At All",
                      supplier_code="")])
        self.assertEqual(res["created"], 0)
        self.assertIn("unresolved supplier", res["skipped_detail"])

    def test_the_company_comes_from_the_store_not_the_rpc_user(self):
        """company_id defaults to the logged-in user's active company, so a
        scan pushed by a user sitting in company A would stamp every row with
        A — invisible until a record rule exists, and then worse than
        invisible: the rule hides the row from the people who own the stock."""
        self.Suggestion.oasis_replace_queue([
            self._row("OAS-WIDGET", "OAS-1")])
        row = self.Suggestion.search([("state", "=", "new")], limit=1)
        self.assertEqual(row.company_id, self.wh.company_id)

    def test_the_supplier_minimum_survives_ingestion(self):
        """If it does not arrive on the row, the basket guard silently becomes
        a no-op and the queue is back to letting part-baskets through."""
        self.Suggestion.oasis_replace_queue([
            self._row("OAS-WIDGET", "OAS-1", supplier_min_units=10.0,
                      supplier_min_value=5000.0)])
        row = self.Suggestion.search([("state", "=", "new")], limit=1)
        self.assertEqual(row.supplier_min_units, 10.0)
        self.assertEqual(row.supplier_min_value, 5000.0)
