"""Smart Ordering tab: the properties a code review found missing.

``ops_dashboard.py`` is a 3,400-line Streamlit script that executes on import,
so — following ``test_command_center_parity.py`` — these read its SOURCE and
assert on the text rather than importing it. Source assertions are weak
evidence of behaviour and strong evidence of regression: each one pins a fix
that was made deliberately, so removing the fix fails here rather than in
production six months later.

The review that prompted this also reported three "critical" bugs, one of which
was not a bug. That claim is pinned too, at the bottom, so nobody re-applies
the "fix".
"""

import os
import pathlib
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

CONSOLE = pathlib.Path(__file__).parent.parent / "ops_dashboard.py"


@pytest.fixture(scope="module")
def src() -> str:
    return CONSOLE.read_text(encoding="utf-8", errors="replace")


@pytest.fixture(scope="module")
def ordering_tab(src) -> str:
    """Just the Smart Ordering tab, so a match elsewhere cannot pass a test."""
    start = src.index("# TAB 4: SMART ORDERING")
    end = src.index("# TAB 5:", start)
    body = src[start:end]
    assert len(body) > 10_000, "tab body implausibly small — markers moved?"
    return body


class TestDoubleOrdering:
    """Pushing a PO twice ordered the same items twice.

    After push_purchase_order the script called st.rerun() with the pipeline
    still cached in session_state, so the rerun redrew the same
    recommendations with the same Push button. The on-order quantities that
    would have zeroed them were never re-read.
    """

    def test_the_pipeline_cache_is_dropped_after_a_push(self, ordering_tab):
        i = ordering_tab.index("push_purchase_order")
        after = ordering_tab[i:i + 1200]
        assert "st.session_state.pop(_so_key" in after
        assert after.index("st.session_state.pop(_so_key") < after.index("st.rerun()")

    def test_the_pipeline_cache_is_dropped_after_an_approval(self, ordering_tab):
        """An approved PO is committed stock; ordering must re-price."""
        i = ordering_tab.index('"APPROVED"')
        after = ordering_tab[i:i + 1500]
        assert 'startswith("so_pipeline_")' in after

    def test_the_duplicate_warning_owns_its_adapter(self, ordering_tab):
        """The dedup warning is the last line of defence against a double
        order, and it read an `adapter` that only existed if the Transfer
        Intelligence tab had run. Without that tab it raised NameError into a
        bare `except: pass` and the warning silently never appeared."""
        i = ordering_tab.index("Duplicate PO Warning")
        before = ordering_tab[max(0, i - 1200):i]
        assert "_dedupe_adapter = get_adapter()" in before
        assert "_dedupe_adapter.fetch_pending_pos" in before


class TestTheScenariosAreComparableToTheirBaseline:
    """A scenario is shown as a delta against the live POs, so it has to be
    priced through the same stages. It was not — three differences, all
    pushing the same way, so every scenario appeared to raise the order."""

    def test_every_scenario_goes_through_one_helper(self, ordering_tab):
        assert ordering_tab.count("_scenario_recs(disrupted_products)") == 3
        assert "def _scenario_recs(" in ordering_tab

    def test_no_scenario_prices_itself(self, ordering_tab):
        """Three copies of the pricing call is how they drifted apart."""
        after = ordering_tab[ordering_tab.index("CHAOS & DISRUPTION"):]
        assert "calculate_order_quantity" not in after

    def test_scenarios_price_on_the_gated_risk(self, ordering_tab):
        """OASIS_GNN_ORDERING_WEIGHT gates the unvalidated GNN out of live PO
        quantities. The scenarios bypassed it by passing the blended
        store_risk straight in, so their delta measured the risk model
        changing as well as the disruption."""
        helper = ordering_tab[ordering_tab.index("def _scenario_recs("):]
        helper = helper[:helper.index("CHAOS & DISRUPTION")]
        assert "gnn_risk_score=ordering_risk" in helper
        assert "store_risk" not in helper

    def test_the_gated_risk_survives_the_session_cache(self, ordering_tab):
        """_ordering_risk is computed inside the run-once pipeline block. It
        has to be stored, or it simply does not exist on a cached rerun."""
        assert "'ordering_risk': _ordering_risk," in ordering_tab
        assert "ordering_risk = _so['ordering_risk']" in ordering_tab

    def test_scenarios_use_the_real_weekday(self, ordering_tab):
        """The baseline was priced with use_real_date=True. A scenario priced
        on a simulated weekday differs from it for that reason alone."""
        helper = ordering_tab[ordering_tab.index("def _scenario_recs("):]
        assert "use_real_date=True" in helper[:900]

    def test_scenarios_pass_the_minimum_order_gate(self, ordering_tab):
        """The baseline is post-MOQ-gate. An ungated scenario carries lines
        the baseline had already dropped."""
        helper = ordering_tab[ordering_tab.index("def _scenario_recs("):]
        assert "apply_minimum_order_gate" in helper[:1400]

    def test_scenarios_hold_the_transfer_plan_fixed(self, ordering_tab):
        """The baseline had network transfers deducted. Not deducting them
        from the scenario counts internally-sourced units as new supply."""
        assert "_baseline_transfer_qty" in ordering_tab
        helper = ordering_tab[ordering_tab.index("def _scenario_recs("):]
        assert "_baseline_transfer_qty.get(" in helper[:1400]

    def test_a_what_if_never_writes_the_moq_failure_store(self, ordering_tab):
        """record_moq_failures replaces every entry for the store. Called from
        a scenario it would overwrite the Transfer tab's real deficit list
        with hypothetical numbers."""
        after = ordering_tab[ordering_tab.index("def _scenario_recs("):]
        assert "record_moq_failures" not in after

    def test_a_scenario_says_it_is_only_a_preview(self, ordering_tab):
        """A Streamlit button is True only on the run after the click, so the
        scenario's numbers vanish on the next interaction while the live POs
        return. Claiming the recommendations "now reflect" the scenario
        overstated a preview as a saved plan."""
        after = ordering_tab[ordering_tab.index("CHAOS & DISRUPTION"):]
        assert after.count("preview, not a saved plan") == 3

    def test_the_targeted_supplier_matches_exactly(self, ordering_tab):
        """`sel_supplier.upper() in p_supplier` made "FARM" hit "FARM FRESH",
        pulling unrelated suppliers into the failure."""
        assert "if p_supplier == _target:" in ordering_tab
        assert "if sel_supplier.upper() in p_supplier:" not in ordering_tab


class TestTheApprovalQueue:
    def test_a_quantity_of_zero_or_less_is_not_approved(self, ordering_tab):
        """The QUANTITY column is editable and nothing stopped an edited 0 or
        -5 from being approved and sent to a supplier as a real order line."""
        i = ordering_tab.index('"APPROVED"')
        before = ordering_tab[max(0, i - 1200):i]
        assert "_q <= 0" in before
        assert "continue" in before

    def test_the_operator_is_told_what_was_skipped(self, ordering_tab):
        """Silently approving 4 of 5 selected rows is worse than refusing."""
        i = ordering_tab.index('"APPROVED"')
        assert "skipped" in ordering_tab[max(0, i - 1200):i + 1500]

    def test_a_rejection_records_its_reason(self, ordering_tab):
        """It logged an empty dict — no record of why the buyer refused."""
        i = ordering_tab.index('"REJECTED"')
        window = ordering_tab[max(0, i - 700):i + 700]
        assert "_reject_reason" in window
        assert '{"reason": _reject_reason}' in window


class TestTheExport:
    def test_the_barcode_lookup_is_one_query(self, ordering_tab):
        """It fired one SELECT per line: a 200-line PO cost 200 round trips
        before the download button could be drawn."""
        assert "WHERE I.ITM_CD IN ({_ph})" in ordering_tab
        assert "WHERE I.ITM_CD = ?" not in ordering_tab

    def test_the_connection_closes_on_every_path(self, ordering_tab):
        """_conn.close() sat after the loop inside the try, so an exception
        mid-loop jumped to the except and leaked the connection."""
        i = ordering_tab.index("get_raw_connection()")
        after = ordering_tab[i:i + 1400]
        assert "finally:" in after
        assert after.index("finally:") < after.index("_conn.close()")


class TestNothingComputedForNobody:
    """Each of these was loaded or computed on every render and then read by
    no one. They are pinned as absent so they do not creep back."""

    @pytest.mark.parametrize("dead", [
        "calendar = get_calendar()",      # parsed an xlsx per render, unused
        "disruption_active =",            # assigned, never read
        "supplier_summary = _so[",        # unpacked, never displayed
        "store_risk = _so[",              # unpacked, never displayed
    ])
    def test_it_is_gone(self, ordering_tab, dead):
        assert dead not in ordering_tab

    def test_the_refresh_button_clears_what_ordering_reads(self, ordering_tab):
        """It cleared the three stock caches but not the ADS map (1 h TTL) or
        the risk map (2 min TTL) — both inputs to the ordering maths, so the
        button reloaded the stock and then ordered against stale demand."""
        i = ordering_tab.index("Refresh Stock")
        after = ordering_tab[i:i + 1200]
        assert "_cached_ads_map.clear()" in after
        assert "get_all_store_risks.clear()" in after


class TestAReportedBugThatWasNotOne:
    def test_the_scenario_assignment_still_follows_the_reset(self, src):
        """A review reported `final_recs = po_recs` as unconditionally undoing
        the scenario assignments, and recommended deleting it.

        It could not: that line stood BEFORE the chaos block in the file, and
        Python runs top to bottom, so it executed first. It was a duplicate of
        the identical assignment made when the session cache is unpacked —
        dead, not harmful — and has been removed as tidying, not as a fix.

        This test pins the ordering the claim got backwards: the reset that
        remains must precede every scenario assignment. If that ever inverts,
        the reported bug becomes real.
        """
        reset = src.index("final_recs = po_recs")
        assert src.count("final_recs = po_recs") == 1, \
            "the duplicate is back — one of them is dead code"
        scenario_assignments = [m.start() for m in
                                re.finditer(r"final_recs = disrupted_recs", src)]
        assert len(scenario_assignments) == 3
        assert all(pos > reset for pos in scenario_assignments)
