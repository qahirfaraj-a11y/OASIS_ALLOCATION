"""
Multi-Day Simulation Tests
==========================
Tests for: advance_to_day, stock carryover, overnight replenishment,
           day history tracking, multi-day trends, deterministic seeding.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from intraday_sim import IntraDaySimulator, _SkuState


def _build_test_sim(n_stores=2, n_skus=3, seed=42):
    """Build a minimal IntraDaySimulator for testing (no DB required)."""
    stores = {}
    for i in range(1, n_stores + 1):
        org_cd = f"ORG{i:03d}"
        skus = []
        for j in range(1, n_skus + 1):
            skus.append(_SkuState(
                itm_cd=f"ITM{j:04d}",
                name=f"Test Product {j}",
                dept="GROCERY" if j % 2 == 0 else "DAIRY",
                sell_price=100.0 * j,
                ads=5.0 * j,       # 5, 10, 15 units/day
                is_fresh=(j % 2 != 0),
                opening_qty=50.0 * j,   # 50, 100, 150
            ))
        stores[org_cd] = {
            'name': f"Store {i}",
            'dsf': 0.5 + i * 0.25,
            'skus': skus,
        }
    return IntraDaySimulator(stores, seed=seed)


class TestAdvanceToDay(unittest.TestCase):
    """advance_to_day() core behavior."""

    def test_advance_day_1_no_change(self):
        """Calling advance_to_day(1) should remain on day 1."""
        sim = _build_test_sim()
        result = sim.advance_to_day(1)
        self.assertEqual(sim.current_day, 1)
        self.assertEqual(result.get('day'), 1)

    def test_advance_to_day_2_increments(self):
        """advance_to_day(2) should move simulator to day 2."""
        sim = _build_test_sim()
        sim.advance_to_day(2)
        self.assertEqual(sim.current_day, 2)

    def test_advance_to_day_3_transitions(self):
        """advance_to_day(3) should step through days 1→2→3."""
        sim = _build_test_sim()
        sim.advance_to_day(3)
        self.assertEqual(sim.current_day, 3)
        # Days 1 and 2 should be in history as 'closed'
        self.assertEqual(sim.get_day_summary(1).get('status'), 'closed')
        self.assertEqual(sim.get_day_summary(2).get('status'), 'closed')


class TestStockCarryover(unittest.TestCase):
    """Stock should carry from day to day."""

    def test_day_2_opening_stock_equals_day_1_closing(self):
        """After day 1, opening stock on day 2 should equal closing stock of day 1."""
        sim = _build_test_sim()

        # Advance to end of day 1
        sim.advance_to_hour(20)
        day1_closing = {}
        for org_cd, store in sim._stores.items():
            day1_closing[org_cd] = {s.itm_cd: s.current_qty for s in store['skus']}

        # Advance to day 2
        sim.advance_to_day(2)

        # Check opening_qty equals what we recorded
        for org_cd, store in sim._stores.items():
            for sku in store['skus']:
                # opening_qty should match day 1 closing
                # (plus any replenishment if item stocked out)
                closing_val = day1_closing[org_cd][sku.itm_cd]
                if closing_val > 0:
                    self.assertEqual(sku.opening_qty, closing_val,
                        f"{org_cd}/{sku.itm_cd}: opening {sku.opening_qty} != closing {closing_val}")

    def test_depleted_qty_resets_on_new_day(self):
        """depleted_qty should reset to 0 at the start of each new day."""
        sim = _build_test_sim()
        sim.advance_to_hour(14)  # sell some stock
        sim.advance_to_day(2)
        for org_cd, store in sim._stores.items():
            for sku in store['skus']:
                self.assertEqual(sku.depleted_qty, 0.0,
                    f"{org_cd}/{sku.itm_cd}: depleted_qty should be 0 on new day")


class TestOvernightReplenishment(unittest.TestCase):
    """Replenishment orders should arrive on schedule."""

    def test_stockout_generates_pending_order(self):
        """An item that stocks out on day 1 should get a pending replenishment order."""
        sim = _build_test_sim(n_stores=1, n_skus=1)
        # Force the single SKU to stock out by setting very low opening qty
        sku = sim._stores["ORG001"]['skus'][0]
        sku.opening_qty = 1.0
        sku.current_qty = 1.0
        sku.ads = 50.0  # much higher than stock → will stock out

        sim.advance_to_hour(20)
        self.assertLessEqual(sku.current_qty, 0)

        # End-of-day close should create a replenishment order
        sim._end_of_day_close()
        pending = sim._replenishment_pending
        self.assertGreater(len(pending), 0, "Should have pending replenishment orders")

        matching = [o for o in pending if o['itm_cd'] == sku.itm_cd and o['org_cd'] == 'ORG001']
        self.assertGreater(len(matching), 0, "Missing replenishment for stocked-out SKU")

    def test_replenishment_arrives_on_schedule(self):
        """Pending orders with arrival_day <= next_day should be delivered."""
        sim = _build_test_sim()
        # Manually add a pending order arriving on day 2
        sim._replenishment_pending.append({
            'org_cd': 'ORG001',
            'itm_cd': 'ITM0001',
            'qty': 25,
            'arrival_day': 2,
            'order_day': 1,
        })

        sku = next(s for s in sim._stores['ORG001']['skus'] if s.itm_cd == 'ITM0001')
        before_qty = sku.current_qty

        sim._overnight_replenishment(2)

        self.assertEqual(sku.current_qty, before_qty + 25)
        self.assertEqual(len(sim._replenishment_pending), 0,
            "Delivered orders should be removed from pending")


class TestDayHistory(unittest.TestCase):
    """Day history should track summaries."""

    def test_day_history_populated(self):
        """After advance_to_day(3), history should have entries for days 1 and 2."""
        sim = _build_test_sim()
        sim.advance_to_day(3)

        self.assertIn(1, sim._day_history)
        self.assertIn(2, sim._day_history)

        for d in [1, 2]:
            summary = sim._day_history[d]
            self.assertEqual(summary['status'], 'closed')
            self.assertIn('revenue', summary)
            self.assertIn('stockouts', summary)
            self.assertIn('units_sold', summary)

    def test_day_summary_returns_empty_for_future(self):
        """get_day_summary for an unreached day returns empty dict."""
        sim = _build_test_sim()
        result = sim.get_day_summary(99)
        self.assertEqual(result, {})


class TestMultiDayTrends(unittest.TestCase):
    """get_multi_day_trends() should return correct data."""

    def test_trends_count(self):
        """get_multi_day_trends(5) after simulating 5 days returns 5 entries."""
        sim = _build_test_sim()
        sim.advance_to_day(5)
        trends = sim.get_multi_day_trends(5)
        self.assertEqual(len(trends), 5)

    def test_completed_days_have_revenue(self):
        """Closed days should have non-zero revenue."""
        sim = _build_test_sim()
        sim.advance_to_day(3)
        trends = sim.get_multi_day_trends(3)
        for t in trends[:2]:  # days 1, 2 are closed
            self.assertGreater(t.get('revenue', 0), 0,
                f"Day {t['day']} should have positive revenue")


class TestDeterministicSeeding(unittest.TestCase):
    """Same hour on different days should produce different results."""

    def test_different_days_different_sales(self):
        """Day 1 hour 14 and Day 2 hour 14 should produce different sales."""
        sim1 = _build_test_sim(seed=42)
        state1 = sim1.advance_to_hour(14)
        rev_d1 = sum(s.total_revenue for s in state1['hour_stats'].values())

        sim2 = _build_test_sim(seed=42)
        sim2.advance_to_day(2)
        state2 = sim2.advance_to_hour(14)
        rev_d2 = sum(s.total_revenue for s in state2['hour_stats'].values())

        self.assertNotEqual(rev_d1, rev_d2,
            "Same hour on different days should give different results (different seed)")

    def test_same_day_reproducible(self):
        """Two simulators with same seed, same day, same hour → same results."""
        sim1 = _build_test_sim(seed=99)
        s1 = sim1.advance_to_hour(14)
        rev1 = sum(s.total_revenue for s in s1['hour_stats'].values())

        sim2 = _build_test_sim(seed=99)
        s2 = sim2.advance_to_hour(14)
        rev2 = sum(s.total_revenue for s in s2['hour_stats'].values())

        self.assertEqual(rev1, rev2)


class TestRewindPrevention(unittest.TestCase):
    """Simulator should not allow rewinding days."""

    def test_rewind_stays_on_current_day(self):
        """advance_to_day(1) after being on day 3 should not rewind."""
        sim = _build_test_sim()
        sim.advance_to_day(3)
        result = sim.advance_to_day(1)
        self.assertEqual(sim.current_day, 3, "Should not rewind to day 1")


if __name__ == "__main__":
    unittest.main()
