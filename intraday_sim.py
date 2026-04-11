"""
IntraDaySimulator
=================
Simulates intra-day stock depletion and generates live sales events as the
ops_dashboard.py time-of-day slider advances.

Usage (from ops_dashboard.py):
    from intraday_sim import IntraDaySimulator
    sim = IntraDaySimulator.from_db(DB_PATH)
    state = sim.advance_to_hour(sim_hour)
    # state keys: sales_df, stockout_events, transfer_opportunities, hour_stats

Design:
- Opening stock is loaded ONCE at simulator initialisation (hour 0 = store open).
- advance_to_hour(h) burns ADS × (h / TRADING_HOURS) of stock from each item,
  adds ±noise, and returns the accumulated picture for that hour.
- Results are deterministic given the simulator seed, so slider movements are
  repeatable and consistent.
"""

import sqlite3
import random
import math
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import sys
import os

sys.path.append(os.getcwd())
from retail_simulator import STORE_UNIVERSES

from oasis.logic.transfer_state import TransferStateTracker

logger = logging.getLogger("IntraDaySimulator")

TRADING_HOURS      = 14      # store open 06:00 – 20:00
OPENING_HOUR       = 6       # 06:00
VELOCITY_SPIKE_THR = 3.0     # alert if hour-rate > 3× expected

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class StockoutEvent:
    org_cd:       str
    itm_cd:       str
    product_name: str
    department:   str
    hour:         int
    lost_sales_qty: float
    lost_sales_kes: float

@dataclass
class TransferOpportunity:
    from_org:     str
    from_name:    str
    to_org:       str
    to_name:      str
    itm_cd:       str
    product_name: str
    department:   str
    transfer_qty: float
    value_kes:    float
    urgency:      str   # "CRITICAL" | "HIGH" | "MEDIUM"

@dataclass
class HourStats:
    hour:         int
    total_revenue: float
    total_units:  int
    n_bills:      int
    n_stockouts:  int
    n_transfers:  int


# ---------------------------------------------------------------------------
# SKU state helper
# ---------------------------------------------------------------------------

class _SkuState:
    """Per-store per-SKU state tracked through the day."""

    __slots__ = ('itm_cd', 'name', 'dept', 'sell_price', 'ads', 'is_fresh',
                 'opening_qty', 'current_qty', 'depleted_qty', 'is_fresh_flag')

    def __init__(self, itm_cd, name, dept, sell_price, ads, is_fresh, opening_qty):
        self.itm_cd      = itm_cd
        self.name        = name
        self.dept        = dept
        self.sell_price  = sell_price
        self.ads         = ads
        self.is_fresh    = is_fresh
        self.opening_qty = opening_qty
        self.current_qty = opening_qty
        self.depleted_qty = 0.0


# ---------------------------------------------------------------------------
# Main simulator
# ---------------------------------------------------------------------------

class IntraDaySimulator:
    """
    Simulates today's sales for all stores as the time slider moves.

    Attributes
    ----------
    _stores : dict  {org_cd -> {name, dsf, skus: List[_SkuState]}}
    _seed   : int
    _hour   : int   last simulated hour (memoised)
    """

    def __init__(self, stores_state: dict, seed: int = 42, registry_path: Optional[str] = None):
        self._stores  = stores_state   # {org_cd -> {...}}
        self._seed    = seed
        self._hour    = -1
        self._cache: Optional[dict] = None
        self.registry_path = registry_path
        self.tracker = TransferStateTracker()
        if self.registry_path:
            self.tracker.load_from_file(self.registry_path)
        # ── Multi-day state ──
        self._current_day: int = 1
        self._day_history: Dict[int, dict] = {}
        self._replenishment_pending: List[dict] = []  # {org_cd, itm_cd, qty, arrival_day}

    # ------------------------------------------------------------------
    # Factory: load opening stock from SQLite
    # ------------------------------------------------------------------

    @classmethod
    def from_db(cls, db_path: str, seed: int = 42, registry_path: Optional[str] = None) -> "IntraDaySimulator":
        """Build simulator by reading today's opening stock from the mock DB."""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Load all org codes + names
        orgs = {
            row['ORG_CD']: row['ORG_NAME']
            for row in conn.execute("SELECT ORG_CD, ORG_NAME FROM ORGANIZATION_MST")
        }

        # Load demand_scale_factor approximation from org ordering (ORG001 = highest)
        # We infer dsf from the ratio of org's avg stock vs ORG001's
        avg_stock = {}
        for row in conn.execute("""
            SELECT SM_ORG_CD, AVG(SM_QTY) as avg_qty
            FROM STOCK_MASTER WHERE SM_QTY > 0
            GROUP BY SM_ORG_CD
        """):
            avg_stock[row['SM_ORG_CD']] = row['avg_qty']

        ref_avg = max(avg_stock.values()) if avg_stock else 1.0

        # Load SKUs + opening stock per org
        stores_state: dict = {}
        for org_cd, org_name in orgs.items():
            dsf = avg_stock.get(org_cd, ref_avg) / ref_avg
            
            # Map DSF to a reasonable Store Universe
            best_tier = "Mega_100M"
            min_diff = float("inf")
            for t_name, t_config in STORE_UNIVERSES.items():
                if "Online" in t_name: continue
                diff = abs(t_config["demand_scale_factor"] - dsf)
                if diff < min_diff:
                    min_diff = diff
                    best_tier = t_name
                    
            tier_max_skus = STORE_UNIVERSES[best_tier]["max_skus"]

            rows = conn.execute("""
                SELECT
                    i.ITM_CD, i.ITM_LONG_NAME, i.DEPARTMENT,
                    COALESCE(sp.BSP_SP, 100) as SELL_PRICE,
                    s.SM_QTY
                FROM STOCK_MASTER s
                JOIN ITEM_MST i ON i.ITM_CD = s.SM_ITM_CD
                LEFT JOIN BASIC_SP_MST sp
                    ON sp.BSP_ORG_CD = s.SM_ORG_CD AND sp.BSP_ITEM_CD = s.SM_ITM_CD
                WHERE s.SM_ORG_CD = ?
            """, (org_cd,)).fetchall()

            # ADS: derive from BI_SALES_REPORT (last 3 months)
            bi = {
                row['ITM_CD']: row['avg_daily']
                for row in conn.execute("""
                    SELECT ITM_CD,
                           SUM(QUANTITY) / MAX(1, COUNT(DISTINCT REPORT_MONTH) * 30.0) as avg_daily
                    FROM BI_SALES_REPORT
                    WHERE ORG_CD = ?
                    GROUP BY ITM_CD
                """, (org_cd,))
            }

            skus = []
            
            # Compensation factor for missing SKUs
            actual_skus = len(rows)
            sku_compensation = 1.0
            if actual_skus > 0 and actual_skus < tier_max_skus:
                sku_compensation = tier_max_skus / actual_skus
                
            for r in rows:
                base_ads  = max(0.01, bi.get(r['ITM_CD'], 0.5 * dsf))
                # Scale by compensation to hit store targets despite small catalog
                ads = base_ads * sku_compensation
                
                dept = (r['DEPARTMENT'] or 'GROCERY').upper()
                is_f = any(k in dept for k in [
                    'MILK','DAIRY','FRESH','MEAT','CHICKEN','BREAD','BAKERY','FISH','VEGETABLE','FRUIT'
                ])
                skus.append(_SkuState(
                    itm_cd      = r['ITM_CD'],
                    name        = r['ITM_LONG_NAME'],
                    dept        = dept,
                    sell_price  = r['SELL_PRICE'],
                    ads         = ads,
                    is_fresh    = is_f,
                    opening_qty = round(max(0.0, r['SM_QTY'])),
                ))

            stores_state[org_cd] = {
                'name': org_name,
                'dsf':  dsf,
                'skus': skus,
            }
            logger.info(f"  Loaded {org_cd} ({org_name[:30]}): {len(skus)} SKUs")

        conn.close()
        logger.info(f"IntraDaySimulator initialised: {len(stores_state)} stores")
        return cls(stores_state, seed, registry_path=registry_path)

    # ------------------------------------------------------------------
    # Core simulation tick
    # ------------------------------------------------------------------

    def advance_to_hour(self, hour: int) -> dict:
        """
        Advance simulation to `hour` (6–20).

        Returns a dict with:
          - 'sales_rows'  : list[dict] — individual sale line items
          - 'stockouts'   : list[StockoutEvent]
          - 'transfers'   : list[TransferOpportunity]
          - 'hour_stats'  : dict[org_cd -> HourStats]
          - 'hour'        : int
        """
        if hour == self._hour and self._cache is not None:
            return self._cache

        rng = random.Random(self._seed + self._current_day * 100_000 + hour * 1000)

        # Fraction of daily demand that has elapsed by this hour
        elapsed = max(0, hour - OPENING_HOUR)
        frac    = min(1.0, elapsed / TRADING_HOURS)

        sales_rows: List[dict] = []
        stockouts:  List[StockoutEvent] = []
        per_store_skus: Dict[str, List[Tuple[str, float]]] = {}  # org -> [(itm_cd, current_qty)]

        hour_stats: Dict[str, HourStats] = {}

        for org_cd, store in self._stores.items():
            n_bills   = max(5, int(rng.gauss(8 * frac * store['dsf'], 2)))
            revenue   = 0.0
            units     = 0
            n_so      = 0
            store_sku_qtys: List[Tuple[str, float]] = []

            for sku in store['skus']:
                # How much of daily ADS has been sold by this hour?
                expected_sold = sku.ads * frac
                noise         = rng.uniform(0.7, 1.3)
                sold_today    = round(max(0.0, expected_sold * noise))

                # Hourly increment (delta from last simulated step)
                # For simplicity we fully recompute from opening
                remaining = max(0.0, sku.opening_qty - sold_today)
                actual_sold = min(sold_today, sku.opening_qty)
                lost        = max(0.0, sold_today - sku.opening_qty)

                sku.current_qty  = remaining
                sku.depleted_qty = actual_sold

                if actual_sold > 0:
                    revenue += actual_sold * sku.sell_price
                    units   += int(actual_sold)

                    # Emit a representative sale row (1 per SKU per simulated hour)
                    sales_rows.append({
                        'org_cd':    org_cd,
                        'store':     store['name'],
                        'itm_cd':    sku.itm_cd,
                        'name':      sku.name,
                        'dept':      sku.dept,
                        'qty':       int(actual_sold),
                        'price':     sku.sell_price,
                        'revenue':   round(actual_sold * sku.sell_price, 2),
                        'hour':      hour,
                        'is_fresh':  sku.is_fresh,
                    })

                if lost > 0.01:
                    n_so += 1
                    stockouts.append(StockoutEvent(
                        org_cd        = org_cd,
                        itm_cd        = sku.itm_cd,
                        product_name  = sku.name,
                        department    = sku.dept,
                        hour          = hour,
                        lost_sales_qty = round(lost, 2),
                        lost_sales_kes = round(lost * sku.sell_price, 2),
                    ))

                store_sku_qtys.append((sku.itm_cd, remaining))

            per_store_skus[org_cd] = store_sku_qtys
            hour_stats[org_cd] = HourStats(
                hour=hour, total_revenue=round(revenue, 2),
                total_units=units, n_bills=n_bills,
                n_stockouts=n_so, n_transfers=0
            )

        # ------------------------------------------------------------------
        # Transfer opportunity detection
        # Build a SKU -> {org_cd: current_qty} index
        # ------------------------------------------------------------------
        transfers: List[TransferOpportunity] = []
        sku_index: Dict[str, Dict[str, float]] = {}  # itm_cd -> {org: qty}

        for org_cd, store in self._stores.items():
            for sku in store['skus']:
                if sku.itm_cd not in sku_index:
                    sku_index[sku.itm_cd] = {}
                sku_index[sku.itm_cd][org_cd] = sku.current_qty

        # For each stockout, find a donor
        seen_transfers = set()
        
        # Reload latest registry from file if path is set
        if self.registry_path:
            self.tracker.load_from_file(self.registry_path)

        for so in stockouts:
            # OPTIMIZATION: Check if an active transfer is already inbound for this SKU
            active_inbound = self.tracker.get_inbound_qty(so.org_cd, so.itm_cd)
            if active_inbound > 0:
                logger.debug(f"Skipping transfer opportunity for {so.itm_cd} at {so.org_cd} - existing inbound: {active_inbound}")
                continue

            if so.itm_cd not in sku_index:
                continue
            donor_qtys = sku_index[so.itm_cd]

            # Find best donor (most excess over 3× safety stock)
            best_donor = None
            best_excess = 0.0
            for donor_org, donor_qty in donor_qtys.items():
                if donor_org == so.org_cd:
                    continue
                donor_store = self._stores[donor_org]
                donor_sku   = next((s for s in donor_store['skus'] if s.itm_cd == so.itm_cd), None)
                if not donor_sku:
                    continue
                safety_stock = donor_sku.ads * 2   # 2 days cover as minimum
                excess = donor_qty - safety_stock
                if excess > best_excess:
                    best_excess = excess
                    best_donor  = (donor_org, donor_qty, donor_sku)

            key = (so.org_cd, best_donor[0] if best_donor else '', so.itm_cd)
            if best_donor and key not in seen_transfers:
                seen_transfers.add(key)
                donor_org, donor_qty, donor_sku = best_donor
                transfer_qty = min(best_excess * 0.5, so.lost_sales_qty * 2)
                transfer_qty = max(1.0, round(transfer_qty, 1))
                urgency = (
                    'CRITICAL' if so.lost_sales_kes > 5000 else
                    'HIGH'     if so.lost_sales_kes > 1000 else
                    'MEDIUM'
                )
                transfers.append(TransferOpportunity(
                    from_org     = donor_org,
                    from_name    = self._stores[donor_org]['name'],
                    to_org       = so.org_cd,
                    to_name      = self._stores[so.org_cd]['name'],
                    itm_cd       = so.itm_cd,
                    product_name = so.product_name,
                    department   = so.department,
                    transfer_qty = transfer_qty,
                    value_kes    = round(transfer_qty * donor_sku.sell_price, 2),
                    urgency      = urgency,
                ))
                hour_stats[so.org_cd].n_transfers += 1

        self._hour  = hour
        self._cache = {
            'sales_rows' : sales_rows,
            'stockouts'  : stockouts,
            'transfers'  : transfers,
            'hour_stats' : hour_stats,
            'hour'       : hour,
            'frac'       : frac,
        }
        return self._cache

    # ------------------------------------------------------------------
    # Multi-day simulation
    # ------------------------------------------------------------------

    @property
    def current_day(self) -> int:
        """Current simulation day (1-based)."""
        return self._current_day

    def advance_to_day(self, target_day: int) -> dict:
        """
        Advance the simulation to `target_day` (1-based).

        If the simulator is on day 1 and you call advance_to_day(3), it will:
          1. Auto-complete day 1 (advance_to_hour(20))
          2. Run end-of-day close for day 1
          3. Run overnight replenishment for day 2
          4. Transition to day 2, repeat until reaching target_day

        Returns the day summary for the current (target) day.
        """
        if target_day < 1:
            target_day = 1

        # If going backwards, we'd need to reinitialise — for now, no-op
        if target_day < self._current_day:
            logger.warning(
                f"Cannot rewind from day {self._current_day} to {target_day}. "
                f"Returning cached summary."
            )
            return self._day_history.get(target_day, {})

        # Already on the target day
        if target_day == self._current_day:
            return self._day_history.get(self._current_day, {
                'day': self._current_day,
                'status': 'in_progress',
            })

        # Step through each intermediate day
        while self._current_day < target_day:
            # 1. Complete the current day
            self.advance_to_hour(20)

            # 2. End-of-day close — snapshot metrics
            self._end_of_day_close()

            # 3. Transition to next day
            next_day = self._current_day + 1

            # 4. Overnight replenishment for the new day
            self._overnight_replenishment(next_day)

            # 5. Reset hour state for the new day
            self._current_day = next_day
            self._hour = -1
            self._cache = None

            # 6. Carry forward: current_qty becomes new opening_qty
            for org_cd, store in self._stores.items():
                for sku in store['skus']:
                    sku.opening_qty = sku.current_qty
                    sku.depleted_qty = 0.0

            logger.info(f"Day {next_day} started — stock carried forward")

        return self._day_history.get(self._current_day, {
            'day': self._current_day,
            'status': 'in_progress',
        })

    def _end_of_day_close(self):
        """Snapshot end-of-day metrics and build replenishment orders for stocked-out items."""
        state = self._cache or self.advance_to_hour(20)

        # Aggregate metrics
        total_revenue = sum(s.total_revenue for s in state['hour_stats'].values())
        total_units   = sum(s.total_units   for s in state['hour_stats'].values())
        total_so      = sum(s.n_stockouts   for s in state['hour_stats'].values())
        total_xfer    = sum(s.n_transfers   for s in state['hour_stats'].values())

        summary = {
            'day':          self._current_day,
            'status':       'closed',
            'revenue':      round(total_revenue, 2),
            'units_sold':   total_units,
            'stockouts':    total_so,
            'transfers':    total_xfer,
            'lost_sales':   round(
                sum(e.lost_sales_kes for e in state['stockouts']), 2
            ),
        }
        self._day_history[self._current_day] = summary

        # Auto-generate replenishment orders for stocked-out items
        for org_cd, store in self._stores.items():
            for sku in store['skus']:
                if sku.current_qty <= 0 and sku.ads > 0:
                    # Determine lead time by department
                    dept_upper = (sku.dept or '').upper()
                    is_fresh = any(k in dept_upper for k in [
                        'MILK', 'DAIRY', 'FRESH', 'MEAT', 'CHICKEN',
                        'BREAD', 'BAKERY', 'FISH', 'VEGETABLE', 'FRUIT'
                    ])
                    lead_time = 1 if is_fresh else 2

                    order_qty = round(sku.ads * (lead_time + 1) * 1.2)
                    arrival_day = self._current_day + lead_time

                    self._replenishment_pending.append({
                        'org_cd':      org_cd,
                        'itm_cd':      sku.itm_cd,
                        'qty':         max(1, order_qty),
                        'arrival_day': arrival_day,
                        'order_day':   self._current_day,
                    })

        logger.info(
            f"Day {self._current_day} closed — "
            f"Rev: {total_revenue:,.0f}, SO: {total_so}, "
            f"Pending orders: {len(self._replenishment_pending)}"
        )

    def _overnight_replenishment(self, next_day: int):
        """Deliver pending orders scheduled for `next_day`."""
        delivered = 0
        remaining = []

        for order in self._replenishment_pending:
            if order['arrival_day'] <= next_day:
                # Find the SKU and add stock
                store = self._stores.get(order['org_cd'])
                if store:
                    sku = next(
                        (s for s in store['skus'] if s.itm_cd == order['itm_cd']),
                        None
                    )
                    if sku:
                        sku.current_qty += order['qty']
                        delivered += 1
                        logger.debug(
                            f"  Delivered {order['qty']} of {order['itm_cd']} "
                            f"to {order['org_cd']}"
                        )
            else:
                remaining.append(order)

        self._replenishment_pending = remaining
        if delivered:
            logger.info(
                f"Overnight replenishment: {delivered} orders delivered, "
                f"{len(remaining)} still pending"
            )

    def get_day_summary(self, day: int) -> dict:
        """Return the stored summary for a given day."""
        return self._day_history.get(day, {})

    def get_multi_day_trends(self, up_to_day: int) -> List[dict]:
        """
        Return a list of day summaries from day 1 to up_to_day.
        Days not yet completed will have partial/empty summaries.
        """
        trends = []
        for d in range(1, up_to_day + 1):
            entry = self._day_history.get(d, {
                'day': d, 'status': 'not_reached',
                'revenue': 0, 'units_sold': 0,
                'stockouts': 0, 'transfers': 0, 'lost_sales': 0,
            })
            trends.append(entry)
        return trends

    # ------------------------------------------------------------------
    # Helpers for ops_dashboard
    # ------------------------------------------------------------------

    def get_store_names(self) -> Dict[str, str]:
        return {org_cd: s['name'] for org_cd, s in self._stores.items()}

    def get_stockout_df(self, hour: int):
        """Return stockout events as a list-of-dicts for the given hour."""
        state = self.advance_to_hour(hour)
        return [
            {
                'Store'      : self._stores[e.org_cd]['name'],
                'Product'    : e.product_name,
                'Department' : e.department,
                'Hour'       : f"{e.hour:02d}:00",
                'Lost Qty'   : e.lost_sales_qty,
                'Lost KES'   : e.lost_sales_kes,
            }
            for e in state['stockouts']
        ]

    def get_transfer_df(self, hour: int):
        """Return transfer opportunities as a list-of-dicts for the given hour."""
        state = self.advance_to_hour(hour)
        return [
            {
                'Urgency'    : t.urgency,
                'From'       : t.from_name,
                'To'         : t.to_name,
                'Product'    : t.product_name,
                'Department' : t.department,
                'Qty'        : t.transfer_qty,
                'Value KES'  : t.value_kes,
            }
            for t in state['transfers']
        ]

    def get_revenue_by_hour(self, up_to_hour: int) -> List[dict]:
        """Cumulative revenue and units by hour across all stores."""
        out = []
        for h in range(OPENING_HOUR, up_to_hour + 1):
            state = self.advance_to_hour(h)
            total_rev   = sum(s.total_revenue for s in state['hour_stats'].values())
            total_units = sum(s.total_units   for s in state['hour_stats'].values())
            out.append({'hour': h, 'revenue': total_rev, 'units': total_units})
        return out
