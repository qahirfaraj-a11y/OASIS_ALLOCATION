"""
Retail Replenishment & Sales Simulator
=======================================
Simulates daily sales, inventory depletion, and replenishment cycles
for different store tiers using actual product and supplier data.

Usage:
    python retail_simulator.py --days 30 --tier Small_200k
    python retail_simulator.py --days 60 --all-tiers
    python retail_simulator.py --test
"""

import sys
import os
import json
import random
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

# --- Bridge Import ---
from oasis.logic.simulation_bridge import SimulationOrderUtil
from oasis.logic.order_engine import OrderEngine

# --- Configuration ---
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"

# Store Universe Configurations (Keep existing config dicts)
STORE_UNIVERSES = {
    "Micro_100k": {
        "budget": 100_000,
        "reorder_budget_pct": 0.40, 
        "safety_days": 14,
        "max_skus": 200,
        "reorder_frequency_days": 3,
        "demand_scale_factor": 0.0015,
        "description": "Small Kiosk / Duka",
        "min_order_value": 1_000 # Corrected Micro Low
    },
    "Small_200k": {
        "budget": 200_000,
        "reorder_budget_pct": 0.35,
        "safety_days": 12,
        "max_skus": 400,
        "reorder_frequency_days": 3,
        "demand_scale_factor": 0.003,
        "description": "Mini-Mart / Corner Store",
        "min_order_value": 2_500
    },
    "Medium_1M": {
        "budget": 1_000_000,
        "reorder_budget_pct": 0.30,
        "safety_days": 10,
        "max_skus": 1500,
        "reorder_frequency_days": 1, 
        "demand_scale_factor": 0.015,
        "description": "Medium Supermarket",
        "min_order_value": 8_000
    },
    "Large_10M": {
        "budget": 10_000_000,
        "reorder_budget_pct": 0.25,
        "safety_days": 10,
        "max_skus": 5000,
        "reorder_frequency_days": 1,
        "demand_scale_factor": 0.12,
        "description": "Large Supermarket",
        "min_order_value": 15_000
    },
    "Mega_100M": {
        "budget": 100_000_000,
        "reorder_budget_pct": 0.20,
        "safety_days": 10,
        "max_skus": 15000,
        "reorder_frequency_days": 1,
        "demand_scale_factor": 1.0, 
        "description": "Hypermarket / Mega Store",
        "min_order_value": 25_000
    },
    
    # === ONLINE STORE ARCHETYPE (Kenyan E-Commerce) ===
    "Online_5M": {
        "budget": 5_000_000,
        "reorder_budget_pct": 0.35,       # Higher turnover, more frequent orders
        "safety_days": 5,                 # Tighter stock (predictable demand)
        "max_skus": 2000,                 # Focused on daily essentials
        "reorder_frequency_days": 1,      # Daily replenishment (fresh focus)
        "demand_scale_factor": 0.06,      # Mid-size online operation
        "description": "Online Grocery Store",
        "min_order_value": 8_000,         # Lower MOV for agile ordering
        
        # KENYAN ONLINE GROCERY BEHAVIORS
        "is_online": True,
        "fresh_demand_boost": 1.4,        # 40% HIGHER fresh demand
        "artisanal_demand_boost": 1.3,    # 30% higher artisanal/specialty
        "supplier_concentration_risk": 1.5, # 50% more vulnerable to supplier failure
        "daily_essentials_focus": True,   # Prioritize daily consumption items
        "order_frequency_multiplier": 1.2, # More frequent smaller orders
        "basket_size_avg": 8,             # Smaller, more frequent baskets
    }
}

# --- Data Classes (Keep existing dataclasses) ---
@dataclass
class SKUState:
    """Tracks the state of a single SKU across simulation days."""
    product_name: str
    supplier: str
    department: str
    unit_price: float
    cost_price: float
    avg_daily_sales: float
    demand_cv: float
    lead_time_days: int
    current_stock: float = 0.0
    on_order: float = 0.0
    days_until_arrival: int = 0
    is_fresh: bool = False
    reorder_point_override: Optional[float] = None
    
    # Tracking metrics
    total_sales: float = 0.0
    total_demand: float = 0.0
    lost_sales: float = 0.0
    substituted_sales: float = 0.0
    stockout_days: int = 0
    orders_placed: int = 0
    total_ordered: float = 0.0
    first_stockout_day: Optional[int] = None

@dataclass 
class DailyLog:
    """Log of a single day's activity."""
    day: int
    date: str
    total_sales: float = 0.0
    total_demand: float = 0.0
    lost_sales: float = 0.0
    substituted_sales: float = 0.0
    stockout_count: int = 0
    orders_placed: int = 0
    order_value: float = 0.0
    inventory_value: float = 0.0
    fill_rate: float = 100.0

@dataclass
class SimulationResult:
    """Final results of a simulation run."""
    tier_name: str
    days_simulated: int
    store_config: dict
    daily_logs: List[DailyLog] = field(default_factory=list)
    final_sku_states: Dict[str, SKUState] = field(default_factory=dict)
    
    # Summary KPIs
    avg_fill_rate: float = 0.0
    total_revenue: float = 0.0
    total_cost: float = 0.0
    total_lost_sales: float = 0.0
    stockout_rate: float = 0.0
    inventory_turnover: float = 0.0
    capital_efficiency: float = 0.0
    roi: float = 0.0

# --- Data Loading ---
def load_supplier_patterns() -> Dict:
    """Load supplier lead time and reliability data."""
    patterns_file = os.path.join(DATA_DIR, "supplier_patterns_2025 (3).json")
    try:
        with open(patterns_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: Supplier patterns file not found at {patterns_file}")
        return {}

def load_sales_forecasting() -> Dict:
    """Load sales forecasting data for demand variability."""
    forecast_file = os.path.join(DATA_DIR, "sales_forecasting_2025 (1).json")
    try:
        with open(forecast_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: Sales forecasting file not found at {forecast_file}")
        return {}
    
def calculate_demand_cv(monthly_sales: Dict[str, int]) -> float:
    # Keep existing implementation
    if not monthly_sales:
        return 0.5 
    values = list(monthly_sales.values())
    if len(values) < 2:
        return 0.3
    mean = np.mean(values)
    if mean == 0:
        return 0.5
    std = np.std(values)
    cv = std / mean
    return min(cv, 2.0)

def load_scorecard_data(engine: OrderEngine, budget: float, tier_name: str, demand_scale_factor: float = 1.0) -> List[SKUState]:
    """
    Load product data using the REAL OrderEngine Day 1 Logic.
    """
    print(f"Running Day 1 Allocation for {tier_name} (${budget:,.0f})...")
    
    # 1. Load Raw Scorecard Data as input
    try:
        raw_df = pd.read_csv(SCORECARD_FILE)
    except FileNotFoundError:
        print(f"Error: Scorecard file not found at {SCORECARD_FILE}")
        sys.exit(1)
        
    # Convert DF to list of dicts for OrderEngine
    # Map columns to expected keys
    products = []
    for _, row in raw_df.iterrows():
        products.append({
            'product_name': str(row.get('Product', 'Unknown')),
            'supplier_name': str(row.get('Supplier', 'Unknown')),
            'product_category': str(row.get('Department', 'GENERAL')),
            'selling_price': float(row.get('Unit_Price', 0) or 0),
            'margin_pct': float(row.get('Margin_Pct', 25) or 25),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) or 0) * demand_scale_factor, # Pre-scale ADS?
            # Wait, OrderEngine does its own scaling sometimes? No, supply raw ADS. 
            # Actually, OrderEngine is designed for a single store. If we simulate "Micro", we must feed it "Micro ADS"?
            # Or does OrderEngine scale internally?
            # OrderEngine logic uses `avg_daily_sales` from input.
            # So pass the SCALED ADS here.
            'current_stocks': 0.0, # Greenfield
            'pack_size': 1, # Default
            'ABC_Class': str(row.get('ABC_Class', 'C')),
            'reliability_score': 90, # Default, will be enriched
            'is_consignment': False # Enriched later
        })
        
    # 2. Enrich (This loads DBs if not loaded)
    # Note: engine.enrich_product_data works in-place
    # But wait, `enrich_product_data` relies on `self.databases`.
    # `engine` instance passed in should have DBs loaded.
    
    # We need to ensure `avg_daily_sales` is respected as the scaled version.
    # `enrich_product_data` might overwrite ADS from intelligence DBs!
    # Correct. `enrich_product_data` line 488: `p['avg_daily_sales'] = sales_data...`
    # We must OVERRIDE ADS *after* enrichment if we want to simulate a smaller store than the DB reflects.
    
    enriched = engine.enrich_product_data(products)
    
    # FORCE PROXY DEMAND SCALING
    # The Intelligence DB has "Mega Store" sales. We are simulating a Micro store.
    for p in enriched:
        db_ads = p.get('avg_daily_sales', 0)
        p['avg_daily_sales'] = db_ads * demand_scale_factor
        p['units_sold_last_month'] = (db_ads * 30) * demand_scale_factor
        
        # Also clean up fields for Logic
        if 'recommended_quantity' not in p: p['recommended_quantity'] = 0

    # 3. Apply Greenfield Allocation (The Core Logic)
    alloc_result = engine.apply_greenfield_allocation(enriched, budget)
    recommendations = alloc_result['recommendations']
    
    # 4. Convert to SKUState
    sku_states = []
    
    allocated_count = 0
    for rec in recommendations:
        qty = rec.get('recommended_quantity', 0)
        if qty > 0:
            allocated_count += 1
            
            p_name = rec['product_name']
            supp = rec.get('supplier_name', 'Unknown')
            dept = rec.get('product_category', 'GENERAL')
            
            unit_price = float(rec.get('selling_price', 0))
            # Recalculate cost using Simulator's simple logic or Engine's?
            # Let's use Engine's enrichment to be consistent
            # But we need cost_price in SKUState
            margin = rec.get('margin_pct', 25)
            cost_price = unit_price * (1 - margin/100)
            
            ads = rec.get('avg_daily_sales', 0)
            cv = rec.get('demand_cv', 0.5)
            lead = int(rec.get('estimated_delivery_days', 7))
            is_fresh = rec.get('is_fresh', False)
            
            sku = SKUState(
                product_name=p_name,
                supplier=supp,
                department=dept,
                unit_price=unit_price,
                cost_price=cost_price,
                avg_daily_sales=ads,
                demand_cv=cv,
                lead_time_days=lead,
                current_stock=qty, # The allocated amount!
                is_fresh=is_fresh
            )
            sku_states.append(sku)
            
    print(f"Allocation Complete. Stocked {allocated_count} SKUs.")
    return sku_states

# --- Simulation Engine ---
class RetailSimulator:
    """Core simulation engine for retail inventory dynamics."""
    
    def __init__(self, tier_name: str, store_config: dict, seed: int = None, bridge: SimulationOrderUtil = None, initial_skus: List[SKUState] = None):
        self.tier_name = tier_name
        self.config = store_config
        self.seed = seed or random.randint(1, 10000)
        random.seed(self.seed)
        np.random.seed(self.seed)
        
        # Initialize Logic Bridge & Engine
        # Share bridge instance if provided to save loading time
        if bridge:
             self.bridge = bridge
        else:
             self.bridge = SimulationOrderUtil(DATA_DIR)
             self.bridge.engine.load_local_databases() # Ensure loaded
        
        # Load SKUs with Order Engine Logic
        self.skus: Dict[str, SKUState] = {}
        self.skus_by_dept: Dict[str, List[SKUState]] = {} 
        
        demand_scale = store_config.get("demand_scale_factor", 1.0)
        
        if initial_skus:
             # Use provided list (Integration Mode)
             sku_list = initial_skus
        else:
             # Default Loader (Standalone Mode)
             budget = store_config["budget"]
             sku_list = load_scorecard_data(self.bridge.engine, budget, tier_name, demand_scale)
        
        for sku in sku_list:
            self.skus[sku.product_name] = sku
            if sku.department not in self.skus_by_dept:
                self.skus_by_dept[sku.department] = []
            self.skus_by_dept[sku.department].append(sku)
        
        # Sort dept lists by price for better proximity matching
        for dept in self.skus_by_dept:
            self.skus_by_dept[dept].sort(key=lambda s: s.unit_price)
        
        print(f"Demand Scale Factor: {demand_scale:.0%} of Mega baseline")
        
        # Supplier patterns for lead time variability
        self.supplier_patterns = load_supplier_patterns()
        
        # Daily logs
        self.daily_logs: List[DailyLog] = []
        
        # Reorder tracking
        self.pending_orders: List[Tuple[int, str, float]] = []  # (arrival_day, product, qty)
        
        # Initialize Logic Bridge with CORRECT path (root scratch dir where JSONs are)
        self.bridge = SimulationOrderUtil(os.getcwd())
        
    def simulate_daily_demand(self, sku: SKUState) -> float:
        """
        Generate stochastic daily demand based on ADS and CV.
        Uses Poisson for low-volume items, Normal for high-volume.
        """
        if sku.avg_daily_sales <= 0:
            return 0.0
        
        # Fresh items have higher volatility
        cv = sku.demand_cv * (1.3 if sku.is_fresh else 1.0)
        
        if sku.avg_daily_sales < 1.0:
            # Low volume: use Poisson (discrete)
            demand = np.random.poisson(sku.avg_daily_sales)
        else:
            # Higher volume: use Normal (continuous, rounded)
            std_dev = sku.avg_daily_sales * cv
            demand = np.random.normal(sku.avg_daily_sales, std_dev)
            demand = max(0, round(demand))
        
        return float(demand)
    
    def calculate_reorder_point(self, sku: SKUState) -> float:
        """Calculate ROP = (lead_time + safety) * ADS, or use override."""
        if sku.reorder_point_override is not None and sku.reorder_point_override > 0:
             return sku.reorder_point_override

        # Fallback (Should rarely be used if Bridge is working)
        safety_days = self.config["safety_days"]
        if sku.is_fresh:
            safety_days = min(safety_days, 2)  # Fresh needs minimal safety
        
        # Default ROP
        rop = (sku.lead_time_days + safety_days) * sku.avg_daily_sales
        return max(1, rop)
    
    def calculate_order_quantity(self, sku: SKUState) -> float:
        """Calculate EOQ-style order quantity for target coverage."""
        # Target coverage based on tier
        if sku.is_fresh:
            target_days = min(7, self.config["safety_days"]) # Increased cap to 7 days for robust cycle
        else:
            target_days = self.config["safety_days"] + sku.lead_time_days
        
        target_stock = sku.avg_daily_sales * target_days
        current_position = sku.current_stock + sku.on_order
        order_qty = max(0, target_stock - current_position)
        
        # Minimum order quantity (MDQ)
        mdq = max(1, sku.avg_daily_sales * 3)
        if order_qty > 0:
            order_qty = max(order_qty, mdq)
        
        return round(order_qty)
    
    def process_arrivals(self, current_day: int) -> float:
        """Process pending orders that arrive today."""
        arrived_value = 0.0
        arrived = []
        remaining = []
        
        for arrival_day, product_name, qty in self.pending_orders:
            if arrival_day <= current_day:
                if product_name in self.skus:
                    sku = self.skus[product_name]
                    sku.current_stock += qty
                    sku.on_order -= qty
                    sku.on_order = max(0, sku.on_order)
                    arrived_value += qty * sku.cost_price
                arrived.append((arrival_day, product_name, qty))
            else:
                remaining.append((arrival_day, product_name, qty))
        
        self.pending_orders = remaining
        return arrived_value
    
    def place_reorders(self, current_day: int) -> Tuple[int, float]:
        """
        Check ROP and place reorders using Oasis Logic Bridge.
        Implements Supplier Batching & Minimum Order Value (MOV) Enforcement.
        """
        orders_placed_count = 0
        total_orders_value = 0.0
        
        # 1. Prepare Data for Bridge
        candidates = []
        for sku in self.skus.values():
            candidates.append({
                'product_name': sku.product_name,
                'supplier_name': sku.supplier,
                'current_stock': sku.current_stock,
                'on_order_qty': sku.on_order,
                'avg_daily_sales': sku.avg_daily_sales,
                'product_category': sku.department,
                'pack_size': 1,
                'barcode': '',
                'unit_price': sku.unit_price,
                'is_fresh': sku.is_fresh,
                'demand_cv': sku.demand_cv  # Pass Volatility
            })
            
        # 2. Enrich (Oasis Logic)
        enriched = self.bridge.prepare_sku_data(candidates)
        
        # 3. Override ADS & State (Fix Oasis DB mismatches)
        for rec in enriched:
            sku = self.skus.get(rec['product_name'])
            if sku:
                rec['avg_daily_sales'] = sku.avg_daily_sales
                rec['on_order_qty'] = sku.on_order 
                rec['current_stock'] = sku.current_stock
                rec['is_fresh'] = sku.is_fresh
                rec['demand_cv'] = sku.demand_cv # Ensure Bridge gets it
                # Mock metadata to bypass Stale Guards (Simulation is self-contained)
                rec['days_since_delivery'] = 1 
                rec['total_units_sold_last_90d'] = sku.avg_daily_sales * 90 # Dynamic
                
                # CRITICAL FIX: Re-calculate Reorder Point using SCALED ADS
                # Otherwise, Bridge uses unscaled historical velocity (e.g. 33 vs 372)
                tgt_days = rec.get('target_coverage_days', 7)
                rec['reorder_point'] = sku.avg_daily_sales * tgt_days
        
        # 4. Calculate Quantities
        recs = self.bridge.calculate_order_quantity(enriched, store_config={}, current_day=current_day)
        
        # 5. Finalize (Guards)
        finalized = self.bridge.finalize_orders(recs)
        
        # 6. BATCHING & MOV LOGIC
        # Group valid orders by Supplier
        supplier_batches = defaultdict(list) # {supplier: [(rec, sku, qty, cost), ...]}
        
        for r in finalized:
             qty = r.get('recommended_quantity', 0)
             if qty > 0:
                 p_name = r['product_name']
                 sku = self.skus.get(p_name)
                 if sku:
                     cost = qty * sku.cost_price
                     supplier = sku.supplier.upper().strip()
                     supplier_batches[supplier].append((r, sku, qty, cost))
        
        # Process each Supplier Batch
        min_order_val = self.config.get('min_order_value', 0)
        
        for supplier, items in supplier_batches.items():
            batch_value = sum(item[3] for item in items)
            has_critical = False
            
            # Check for critical items in this batch
            for _, sku, _, _ in items:
                # Critical Definition: Stockout OR < 2 days coverage (3 for Fresh)
                # Dynamic Criticality: We MUST order if Stock < Lead Time + 2 days safety
                # Hardcoding 2.0 days was causing stockouts for suppliers with longer lead times.
                
                # FIX: Calculate days_cover
                if sku.avg_daily_sales > 0:
                    days_cover = sku.current_stock / sku.avg_daily_sales
                else:
                    days_cover = 999.0 # Safe/Infinite coverage if no sales

                # Dynamic Threshold: Lead Time + Safety Buffer
                # If we are within the "Lead Time Danger Zone", we MUST order now.
                safety_buffer = 3.0 if sku.is_fresh else 2.0
                critical_thresh = sku.lead_time_days + safety_buffer
                
                if sku.current_stock <= 0 or days_cover < critical_thresh:
                    has_critical = True
                    # print(f"  [CRITICAL] {sku.product_name}: Cover {days_cover:.1f}d < Thresh {critical_thresh:.1f}d (Lead {sku.lead_time_days}d)")
                    break
            
            # Decision: Approve or Defer
            approved = False
            if has_critical:
                approved = True # Critical items override MOV
            elif batch_value >= min_order_val:
                approved = True # Met financial threshold
            else:
                approved = False # Efficiently deferred
                
            if approved:
                for (r, sku, qty, cost) in items:
                    # Place the Order
                    lead_variability = random.uniform(0.8, 1.2)
                    lead_days = r.get('estimated_delivery_days', sku.lead_time_days)
                    actual_lead = max(1, int(round(lead_days * lead_variability)))
                    
                    arrival_day = current_day + actual_lead
                    
                    self.pending_orders.append((arrival_day, sku.product_name, qty))
                    sku.on_order += qty
                    sku.orders_placed += 1
                    sku.total_ordered += qty
                    
                    orders_placed_count += 1
                    total_orders_value += cost
            else:
                # Optional: tracking deferred value statistics could go here
                pass
                    
        return orders_placed_count, total_orders_value
    
    def simulate_day(self, day_num: int, date: datetime) -> DailyLog:
        """Simulate a single day of operations."""
        log = DailyLog(day=day_num, date=date.strftime("%Y-%m-%d"))
        
        # 1. Process arrivals
        self.process_arrivals(day_num)
        
        # 2. Simulate sales for each SKU
        for sku in self.skus.values():
            demand = self.simulate_daily_demand(sku)
            sku.total_demand += demand
            
            # Fulfill what we can
            fulfilled = min(demand, sku.current_stock)
            lost = demand - fulfilled
            
            sku.current_stock -= fulfilled
            sku.total_sales += fulfilled
            sku.lost_sales += lost
            
            # --- SUBSTITUTION LOGIC (The "Sachet Effect") ---
            if lost > 0:
                sub = self.find_substitute(sku)
                if sub and sub.current_stock > 0:
                    # Can we fulfill the lost demand with the substitute?
                    # Note: We assume 1:1 substitution rate for simplicity 
                    # (Customer needs 1 unit of soap, buys 1 unit of other soap)
                    sub_fulfilled = min(lost, sub.current_stock)
                    
                    sub.current_stock -= sub_fulfilled
                    sub.total_sales += sub_fulfilled
                    sub.substituted_sales += sub_fulfilled
                    
                    log.substituted_sales += sub_fulfilled
                    
                    # Technically, we "saved" the sale for the store, but the original SKU still lost it.
                    # We track it in the log's total_sales.
            
            log.total_demand += demand
            log.total_sales += fulfilled
            log.lost_sales += lost
            
            if sku.current_stock == 0 and demand > 0:
                sku.stockout_days += 1
                if sku.first_stockout_day is None:
                    sku.first_stockout_day = day_num
                log.stockout_count += 1
        
        # 3. Calculate fill rate
        if log.total_demand > 0:
            log.fill_rate = (log.total_sales / log.total_demand) * 100
        
        # 4. Place reorders (check every N days based on tier)
        reorder_freq = self.config["reorder_frequency_days"]
        if day_num % reorder_freq == 0 or day_num == 1:
            orders, value = self.place_reorders(day_num)
            log.orders_placed = orders
            log.order_value = value
        
        # 5. Calculate inventory value
        log.inventory_value = sum(
            sku.current_stock * sku.cost_price 
            for sku in self.skus.values()
        )
        
        return log

    def find_substitute(self, original_sku: SKUState) -> Optional[SKUState]:
        """Finds a valid substitute in the same department with similar price."""
        candidates = self.skus_by_dept.get(original_sku.department, [])
        if not candidates:
            return None
            
        # Candidates are already sorted by price.
        # Simple scan for now (optimized for speed over binary search as lists are small-ish)
        # Criteria: +/- 20% price, Has Stock, Not the same item
        
        target_price = original_sku.unit_price
        min_p = target_price * 0.8
        max_p = target_price * 1.2
        
        # Heuristic: Pick the first valid candidate (simulates shelf adjacency or brand dominance)
        for cand in candidates:
            if cand.product_name == original_sku.product_name:
                continue
            if cand.current_stock > 0:
                if min_p <= cand.unit_price <= max_p:
                    return cand
        return None
    
    def run(self, days: int, start_date: datetime = None) -> SimulationResult:
        """Run the full simulation for specified days."""
        print(f"\n{'='*60}")
        print(f"SIMULATION: {self.tier_name}")
        print(f"Config: {self.config['description']}")
        print(f"SKUs: {len(self.skus)}, Days: {days}, Seed: {self.seed}")
        print(f"{'='*60}")
        
        start_date = start_date or datetime.now()
        
        for day in range(1, days + 1):
            current_date = start_date + timedelta(days=day-1)
            log = self.simulate_day(day, current_date)
            self.daily_logs.append(log)
            
            # Detailed Daily Reporting (Requested by User)
            print(f"  Day {day:02d} [{current_date.strftime('%b %d')}]: "
                  f"Fill Rate {log.fill_rate:5.1f}%, "
                  f"Stockouts: {log.stockout_count:3d}, "
                  f"Orders: {log.orders_placed:2d}")
        
        # Build result
        result = SimulationResult(
            tier_name=self.tier_name,
            days_simulated=days,
            store_config=self.config,
            daily_logs=self.daily_logs,
            final_sku_states=self.skus
        )
        
        # Calculate summary KPIs
        self._calculate_summary_kpis(result)
        
        return result
    
    def _calculate_summary_kpis(self, result: SimulationResult):
        """Calculate final summary KPIs."""
        logs = result.daily_logs
        skus = result.final_sku_states
        
        # Average fill rate
        result.avg_fill_rate = np.mean([log.fill_rate for log in logs])
        
        # Total revenue and cost
        result.total_revenue = sum(sku.total_sales * sku.unit_price for sku in skus.values())
        result.total_cost = sum(sku.total_ordered * sku.cost_price for sku in skus.values())
        
        # Lost sales value
        result.total_lost_sales = sum(sku.lost_sales * sku.unit_price for sku in skus.values())
        
        # Stockout rate (% of SKU-days with stockout)
        total_sku_days = len(skus) * result.days_simulated
        total_stockout_days = sum(sku.stockout_days for sku in skus.values())
        result.stockout_rate = (total_stockout_days / total_sku_days * 100) if total_sku_days > 0 else 0
        
        # Inventory turnover (annualized)
        avg_inventory = np.mean([log.inventory_value for log in logs])
        cogs = sum(sku.total_sales * sku.cost_price for sku in skus.values())
        if avg_inventory > 0:
            daily_turns = cogs / avg_inventory
            result.inventory_turnover = daily_turns * (365 / result.days_simulated)
        
        # Capital efficiency (Revenue / Capital Deployed)
        initial_investment = self.config["budget"]
        result.capital_efficiency = (result.total_revenue / initial_investment * 100) if initial_investment > 0 else 0
        
        # ROI ((Revenue - Cost) / Cost)
        cogs_sales = sum(sku.total_sales * sku.cost_price for sku in skus.values())
        result.roi = ((result.total_revenue - cogs_sales) / cogs_sales * 100) if cogs_sales > 0 else 0

# --- Reporting ---
def print_simulation_summary(result: SimulationResult):
    """Print summary of simulation results."""
    print(f"\n{'='*60}")
    print(f"SIMULATION RESULTS: {result.tier_name}")
    print(f"{'='*60}")
    print(f"Days Simulated: {result.days_simulated}")
    print(f"SKUs Tracked: {len(result.final_sku_states)}")
    print()
    print("KEY PERFORMANCE INDICATORS:")
    print(f"  [FILL]  Fill Rate:           {result.avg_fill_rate:.1f}%")
    print(f"  [OUT]   Stockout Rate:       {result.stockout_rate:.2f}%")
    print(f"  [REV]   Total Revenue:       KES {result.total_revenue:,.0f}")
    print(f"  [LOST]  Lost Sales:          KES {result.total_lost_sales:,.0f}")
    print(f"  [TURN]  Inventory Turns:     {result.inventory_turnover:.1f}x (annualized)")
    print(f"  [EFF]   Capital Efficiency:  {result.capital_efficiency:.1f}% of budget")
    print(f"  [ROI]   Return on Inv:       {result.roi:.1f}%")
    print()
    
    # Calculate Avg Days to First Stockout (for those that stocked out)
    stockout_skus = [s for s in result.final_sku_states.values() if s.first_stockout_day is not None]
    avg_days_to_stockout = np.mean([s.first_stockout_day for s in stockout_skus]) if stockout_skus else 0
    
    print(f"  [TIME]  Avg Days to Stockout:{avg_days_to_stockout:>5.1f} days (for {len(stockout_skus)} affected SKUs)")
    
    # Top stockout SKUs
    skus_sorted = sorted(result.final_sku_states.values(), 
                         key=lambda x: x.lost_sales, reverse=True)
    print("TOP 5 STOCKOUT CULPRITS:")
    for sku in skus_sorted[:5]:
        if sku.lost_sales > 0:
            day_msg = f"(Day {sku.first_stockout_day})" if sku.first_stockout_day else ""
            print(f"  - {sku.product_name[:40]}: {sku.lost_sales:.0f} units lost {day_msg}")

def export_to_excel(results: List[SimulationResult], output_file: str):
    """Export simulation results to Excel."""
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # Sheet 1: KPI Summary
        summary_data = []
        for r in results:
            summary_data.append({
                "Tier": r.tier_name,
                "Days": r.days_simulated,
                "SKUs": len(r.final_sku_states),
                "Fill Rate %": round(r.avg_fill_rate, 1),
                "Stockout Rate %": round(r.stockout_rate, 2),
                "Revenue KES": round(r.total_revenue, 0),
                "Lost Sales KES": round(r.total_lost_sales, 0),
                "Substituted Sales KES": round(sum(sku.substituted_sales * sku.unit_price for sku in r.final_sku_states.values()), 0), # New Metric
                "Inventory Turns": round(r.inventory_turnover, 1),
                "Inventory Turns": round(r.inventory_turnover, 1),
                "Capital Efficiency %": round(r.capital_efficiency, 1),
                "ROI %": round(r.roi, 1)
            })
        pd.DataFrame(summary_data).to_excel(writer, sheet_name="KPI Summary", index=False)
        
        # Sheet 2: Daily Logs (first result only for space)
        if results:
            daily_data = [
                {
                    "Day": log.day,
                    "Date": log.date,
                    "Demand": log.total_demand,
                    "Sales": log.total_sales,
                    "Substituted": log.substituted_sales, # New Metric
                    "Lost Sales": log.lost_sales,
                    "Fill Rate %": round(log.fill_rate, 1),
                    "Stockouts": log.stockout_count,
                    "Orders": log.orders_placed,
                    "Order Value": log.order_value,
                    "Inventory Value": log.inventory_value
                }
                for log in results[0].daily_logs
            ]
            pd.DataFrame(daily_data).to_excel(writer, sheet_name="Daily Log", index=False)
        
        # Sheet 3: SKU Performance (first result)
        if results:
            sku_data = [
                {
                    "Product": sku.product_name[:50],
                    "Supplier": sku.supplier[:30],
                    "Department": sku.department,
                    "Unit Price": sku.unit_price,
                    "ADS": sku.avg_daily_sales,
                    "Demand CV": round(sku.demand_cv, 2),
                    "Lead Time": sku.lead_time_days,
                    "Total Demand": sku.total_demand,
                    "Total Sales": sku.total_sales,
                    "Substituted Sales": sku.substituted_sales, # New Metric
                    "Lost Sales": sku.lost_sales,
                    "Stockout Days": sku.stockout_days,
                    "First Stockout Day": sku.first_stockout_day,
                    "Orders Placed": sku.orders_placed,
                    "Fill Rate %": round((sku.total_sales / sku.total_demand * 100) 
                                         if sku.total_demand > 0 else 100, 1)
                }
                for sku in results[0].final_sku_states.values()
            ]
            pd.DataFrame(sku_data).to_excel(writer, sheet_name="SKU Performance", index=False)
    
    print(f"\n[OK] Results exported to: {output_file}")

# --- Main Entry Point ---
def main():
    parser = argparse.ArgumentParser(description="Retail Replenishment & Sales Simulator")
    parser.add_argument("--days", type=int, default=30, help="Number of days to simulate")
    parser.add_argument("--tier", type=str, default="Small_200k", 
                        choices=list(STORE_UNIVERSES.keys()),
                        help="Store tier to simulate")
    parser.add_argument("--all-tiers", action="store_true", help="Simulate all tiers")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--test", action="store_true", help="Run test simulation")
    parser.add_argument("--output", type=str, default=None, help="Output Excel file path")
    
    args = parser.parse_args()
    
    if args.test:
        # Quick test run
        print("Running test simulation (10 days, Small_200k)...")
        args.days = 10
        args.tier = "Small_200k"
        args.seed = 42
    
    # Initialize Shared Engine (Optimization)
    print("Initializing Simulation Engine (Loading Databases)...")
    shared_bridge = SimulationOrderUtil(DATA_DIR)
    
    results = []
    
    if args.all_tiers:
        # Run all tiers
        for tier_name, config in STORE_UNIVERSES.items():
            print(f"\n--- Starting Simulation: {tier_name} ---")
            sim = RetailSimulator(tier_name, config, seed=args.seed, bridge=shared_bridge)
            result = sim.run(args.days)
            results.append(result)
            print_simulation_summary(result)
    else:
        # Run single tier
        config = STORE_UNIVERSES[args.tier]
        sim = RetailSimulator(args.tier, config, seed=args.seed, bridge=shared_bridge)
        result = sim.run(args.days)
        results.append(result)
        print_simulation_summary(result)
    
    # Export results
    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = args.output or f"simulation_results_{timestamp}.xlsx"
        export_to_excel(results, output_file)
    
    # Print tier comparison if all-tiers
    if args.all_tiers and len(results) > 1:
        print(f"\n{'='*60}")
        print("TIER COMPARISON")
        print(f"{'='*60}")
        print(f"{'Tier':<15} {'Fill%':<8} {'Stockout%':<10} {'Turns':<8} {'Efficiency%':<12} {'Sub%':<6}")
        print("-" * 70)
        for r in results:
            sub_sales = sum(sku.substituted_sales * sku.unit_price for sku in r.final_sku_states.values())
            sub_pct = (sub_sales / r.total_revenue * 100) if r.total_revenue > 0 else 0
            print(f"{r.tier_name:<15} {r.avg_fill_rate:<8.1f} {r.stockout_rate:<10.2f} "
                  f"{r.inventory_turnover:<8.1f} {r.capital_efficiency:<12.1f} {sub_pct:<6.1f}")

if __name__ == "__main__":
    main()
