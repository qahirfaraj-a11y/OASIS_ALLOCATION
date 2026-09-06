"""Measured demand must outrank the static forecast file.

THE DEFECT. enrich_product_data ended its sales-forecasting branch with an
unconditional ``p['avg_daily_sales'] = hist_ads``, so whatever the caller had
measured was thrown away and every order-up-to level was built on a static
JSON instead. It was not a corner case: on the live book it fired for 14,667
of 15,037 SKUs (97.5%), because the only escape — ``live_ads_30d`` — is set by
the CSV parser and never on the database path (measured: 0 of 15,037 rows).

WHAT IT COST. fetch_enriched_products already computes a recency-weighted ADS
from raw POS sales (60% of the last 30 days, 30% of 30-60, 10% of 60-90) and
passes it in this very field. Discarding it meant planning on
sales_forecasting_2025 (1).json, dated 2026-02-21 and 29.1% below the
POS-derived series that reconciles unit-for-unit with the cash extracts.
Total planned daily demand was 5,930 against 8,367 actually being sold, and
Monday's order came out 48.7% smaller than the corrected book.

These drive the real enrichment rather than a local copy of the rule. A
re-implementation would have agreed with itself and missed the bug, which is
what happened the last time this engine was measured by a harness.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.order_engine import OrderEngine


@pytest.fixture
def engine(tmp_path):
    """A real engine with a known forecast file and nothing else."""
    eng = OrderEngine(str(tmp_path))
    eng.databases = {
        'sales_forecasting': {
            'WIDGET': {'avg_daily_sales': 2.0, 'trend': 'stable',
                       'monthly_sales': {'2026-01': 60}},
        },
    }
    # The name index is cached across calls; a stale one would mask the join.
    eng._sales_index_cache = None
    return eng


def _one(eng, **over):
    p = {'product_name': 'WIDGET', 'item_code': 'W1', 'barcode': 'B1',
         'department': 'GENERAL', 'supplier_name': 'ACME',
         'current_stocks': 10}
    p.update(over)
    return eng.enrich_product_data([p])[0]


class TestMeasuredDemandWins:

    def test_a_supplied_ads_survives_enrichment(self, engine):
        """THE REGRESSION, in one assertion. 7.5 went in; 2.0 came out."""
        assert _one(engine, avg_daily_sales=7.5)['avg_daily_sales'] == 7.5

    def test_it_is_not_quietly_averaged_with_the_file(self, engine):
        """(7.5 + 2.0) / 2 = 4.75 would also 'preserve' it, and would be just
        as wrong — the file is a fallback, not a second opinion."""
        assert _one(engine, avg_daily_sales=7.5)['avg_daily_sales'] == 7.5

    def test_the_source_is_recorded(self, engine):
        assert _one(engine, avg_daily_sales=7.5)['ads_source'] == 'supplied'

    def test_a_producer_keeps_its_own_label(self, engine):
        """pos_erp_adapter marks what it measured; enrichment must not
        relabel it, or provenance stops meaning anything."""
        r = _one(engine, avg_daily_sales=7.5, ads_source='pos_weighted')
        assert r['ads_source'] == 'pos_weighted'
        assert r['avg_daily_sales'] == 7.5


class TestTheFallbacksStillWork:

    def test_the_file_is_used_when_nothing_was_measured(self, engine):
        """It is still the right answer when there is no measurement — the
        fix is about precedence, not about distrusting the file."""
        r = _one(engine, avg_daily_sales=0)
        assert r['avg_daily_sales'] == 2.0
        assert r['ads_source'] == 'forecast_file'

    def test_a_missing_ads_key_falls_back_too(self, engine):
        assert _one(engine)['avg_daily_sales'] == 2.0

    def test_a_live_30_day_feed_still_outranks_everything(self, engine):
        """The CSV ingestion path's 70/30 blend predates this and must not
        have been broken by re-ordering the branches:
        0.7*10 + 0.3*2.0 = 7.6."""
        r = _one(engine, avg_daily_sales=5.0, live_ads_30d=10.0)
        assert r['avg_daily_sales'] == pytest.approx(7.6)
        assert r['is_velocity_blended'] is True
        assert r['ads_source'] == 'live_blend_30d'

    def test_an_unknown_sku_keeps_what_it_was_given(self, engine):
        """No forecast record at all — the other branch, which already
        preserved the caller but said nothing about where it came from."""
        r = _one(engine, product_name='NOT IN THE FILE', avg_daily_sales=3.25)
        assert r['avg_daily_sales'] == 3.25
        assert r['ads_source'] == 'supplied'


class TestTheAdapterDeclaresWhatItMeasured:
    """Provenance has to be set where the number is made, or the consumer is
    guessing. These pin the contract without needing a database."""

    def test_the_weighted_branch_is_labelled(self):
        import inspect
        from oasis.logic import pos_erp_adapter as A
        src = inspect.getsource(A.PosErpAdapter.fetch_enriched_products) \
            if hasattr(A, "PosErpAdapter") else inspect.getsource(A)
        assert 'ads_source"] = "pos_weighted"' in src
        assert 'ads_source"] = "pos_flat"' in src

    def test_weighted_ads_is_still_computed_from_raw_pos(self):
        """The recency weighting is the thing worth preserving; if this call
        goes away the fix above has nothing better to prefer."""
        import inspect
        from oasis.logic import pos_erp_adapter as A
        assert "_calc_weighted_ads" in inspect.getsource(A)
