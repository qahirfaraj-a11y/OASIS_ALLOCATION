"""The queue model: ingestion, staleness, and the sentinel boundary."""

from datetime import timedelta

from odoo import fields
from odoo.tests.common import tagged

from .common import OasisCase


@tagged("post_install", "-at_install")
class TestIngestion(OasisCase):

    def test_a_scan_replaces_only_what_is_still_pending(self):
        """Approved and rejected rows are decisions; a new scan must not undo them.

        Anything approved has become a document somebody may already be
        picking, and re-proposing a rejected line would make the queue an
        argument the operator has to win over and over.
        """
        pending = self._suggest(qty=1)
        approved = self._suggest(qty=2)
        rejected = self._suggest(qty=3)
        approved.state = "approved"
        rejected.state = "rejected"

        self.Suggestion.oasis_replace_queue(
            [self._row("OAS-WIDGET", "OAS-A", "OAS-B", qty=9)])

        self.assertFalse(pending.exists(), "a pending row survived the scan")
        self.assertTrue(approved.exists(), "an APPROVED row was destroyed")
        self.assertTrue(rejected.exists(), "a REJECTED row was destroyed")
        fresh = self.Suggestion.search([("state", "=", "new")])
        self.assertEqual(len(fresh), 1)
        self.assertEqual(fresh.quantity, 9)

    def test_unknown_codes_are_skipped_not_guessed(self):
        res = self.Suggestion.oasis_replace_queue([
            self._row("OAS-WIDGET", "OAS-A", "OAS-B"),
            self._row("NO-SUCH-SKU", "OAS-A", "OAS-B"),
            self._row("OAS-WIDGET", "NO-SUCH-WH", "OAS-B"),
        ])
        self.assertEqual(res["created"], 1)
        self.assertEqual(res["skipped"], 2)

    def test_a_self_transfer_is_never_queued(self):
        res = self.Suggestion.oasis_replace_queue(
            [self._row("OAS-WIDGET", "OAS-A", "OAS-A")])
        self.assertEqual(res["created"], 0)
        self.assertEqual(res["skipped"], 1)

    def test_the_999_sentinel_never_reaches_a_column(self):
        """999 is the engine's 'no demand' marker, not a cover figure.

        It reached operator-facing columns and poisoned pivot averages. It is
        stripped at the ingestion boundary so nothing downstream can see it.
        """
        self.Suggestion.oasis_replace_queue([self._row(
            "OAS-WIDGET", "OAS-A", "OAS-B",
            donor_ads=0.0, donor_cover=999.0,
            recipient_ads=0.0, recipient_cover=999.0)])
        rec = self.Suggestion.search([("state", "=", "new")])
        self.assertEqual(rec.donor_days_cover, 0.0)
        self.assertEqual(rec.recipient_days_cover, 0.0)

    def test_real_cover_is_preserved(self):
        """The sentinel strip must not flatten genuine figures."""
        self.Suggestion.oasis_replace_queue([self._row(
            "OAS-WIDGET", "OAS-A", "OAS-B",
            donor_ads=2.0, donor_cover=40.0,
            recipient_ads=3.0, recipient_cover=1.5)])
        rec = self.Suggestion.search([("state", "=", "new")])
        self.assertEqual(rec.donor_days_cover, 40.0)
        self.assertEqual(rec.recipient_days_cover, 1.5)


@tagged("post_install", "-at_install")
class TestStaleness(OasisCase):

    def test_a_fresh_suggestion_is_not_stale(self):
        s = self._suggest(computed_on=fields.Datetime.now())
        self.assertFalse(s.is_stale)

    def test_an_old_suggestion_is_stale(self):
        old = fields.Datetime.now() - timedelta(hours=4)
        self.assertTrue(self._suggest(computed_on=old).is_stale)

    def test_computed_on_is_compared_in_UTC(self):
        """The staleness window could never fire.

        computed_on was written in LOCAL time while Odoo stores UTC, so a
        timestamp could land in the future and every plan looked permanently
        fresh — a safety feature present, green, and doing nothing. Odoo's own
        Datetime.now() is UTC; this pins that the comparison agrees with it.
        """
        just_inside = fields.Datetime.now() - timedelta(minutes=10)
        just_outside = fields.Datetime.now() - timedelta(minutes=50)
        self.assertFalse(self._suggest(computed_on=just_inside).is_stale)
        self.assertTrue(self._suggest(computed_on=just_outside).is_stale)

    def test_the_window_is_configurable(self):
        old = fields.Datetime.now() - timedelta(hours=4)
        s = self._suggest(computed_on=old)
        self.assertTrue(s.is_stale)
        self.env["ir.config_parameter"].sudo().set_param(
            "oasis.scan_stale_hours", "12")
        s.invalidate_recordset(["is_stale"])
        self.assertFalse(s.is_stale, "a widened window was ignored")

    def test_searching_on_is_stale_matches_the_computed_value(self):
        fresh = self._suggest(computed_on=fields.Datetime.now())
        stale = self._suggest(
            computed_on=fields.Datetime.now() - timedelta(hours=4))
        found_stale = self.Suggestion.search([("is_stale", "=", True)])
        found_fresh = self.Suggestion.search([("is_stale", "=", False)])
        self.assertIn(stale, found_stale)
        self.assertNotIn(fresh, found_stale)
        self.assertIn(fresh, found_fresh)
        self.assertNotIn(stale, found_fresh)
