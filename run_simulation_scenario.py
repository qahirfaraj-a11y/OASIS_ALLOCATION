"""
OASIS Simulation Runner with Black Swan Events
===============================================
Runs retail simulation with support for:
1. Supplier Failure Scenarios (critical node disruption)
2. Competitive Environment Changes (market pressure)

Usage Examples:
    # Baseline simulation
    python run_simulation_scenario.py --scenario Baseline --days 30 --budget 300000
    
    # Simulate Carrefour opening causing 6% decline
    python run_simulation_scenario.py --scenario CompetitorEntry --competitor Carrefour --competitor-impact -6.0 --days 60
    
    # Simulate critical supplier failure on Day 10
    python run_simulation_scenario.py --scenario SupplierCrisis --fail-supplier "BROOKSIDE" --fail-day 10 --fail-duration 14
    
    # Simulate top FRESH MILK supplier failure
    python run_simulation_scenario.py --scenario FreshCrisis --fail-dept "FRESH MILK" --fail-day 5
    
    # Combined stress test
    python run_simulation_scenario.py --scenario StressTest --fail-supplier BROOKSIDE --fail-day 10 --competitor Carrefour --days 60
"""

import logging
import os
import sys
import copy
import argparse
import json
import pandas as pd
from typing import List, Dict, Optional

# Setup Path to import oasis
sys.path.append(os.path.abspath("C:/Users/iLink/.gemini/antigravity/scratch"))

from oasis.logic.order_engine import OrderEngine
from oasis.simulation.simulation_engine import SalesSimulator, InventoryTracker, RiskModel, ReplenishmentLogic
from oasis.simulation.data_loader import HistoricalDataLoader
from oasis.simulation.black_swan_events import (
    SupplierFailureEvent, CompetitiveEvent, EventType, 
    SupplierRiskAnalyzer, SCENARIO_TEMPLATES, DEPARTMENT_SENSITIVITY
)

# Configure Logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("SimulationRunner")


def run_simulation(scenario_name: str = "Baseline", 
                   duration_days: int = 30, 
                   target_month: str = "JAN", 
                   budget_override: float = 300000.0,
                   # Black Swan: Supplier Failure
                   fail_supplier: str = None,
                   fail_department: str = None,
                   fail_day: int = 10,
                   fail_duration: int = 14,
                   fail_mode: str = "COMPLETE",
                   # Black Swan: Competitive Environment
                   competitor: str = None,
                   competitor_impact: float = -6.0,
                   competitor_ramp_days: int = 30,
                   competitor_template: str = None):
    """
    Run a retail simulation with optional Black Swan events.
    
    Args:
        scenario_name: Name for output files
        duration_days: Simulation length
        target_month: Month for seasonality
        budget_override: Initial capital
        fail_supplier: Supplier name to fail (e.g., "BROOKSIDE")
        fail_department: Department to fail top supplier of (alternative to fail_supplier)
        fail_day: Day to trigger supplier failure
        fail_duration: How long failure lasts
        fail_mode: "COMPLETE", "PARTIAL", or "DELAYED"
        competitor: Competitor name (e.g., "Carrefour")
        competitor_impact: YoY impact percentage (negative = loss)
        competitor_ramp_days: Days to reach full impact
        competitor_template: Use predefined template (e.g., "carrefour_100m")
    """
    
    logger.info(f"{'='*60}")
    logger.info(f"OASIS SIMULATION: {scenario_name}")
    logger.info(f"{'='*60}")
    logger.info(f"Parameters: Month={target_month}, Budget=${budget_override:,.0f}, Duration={duration_days}d")
    
    # Log Black Swan events
    if fail_supplier or fail_department:
        target = fail_supplier or f"Top of {fail_department}"
        logger.info(f"⚠️ BLACK SWAN: Supplier Failure scheduled - {target} on Day {fail_day} for {fail_duration} days")
    if competitor or competitor_template:
        comp_name = competitor or competitor_template.split('_')[0].title()
        logger.info(f"🏪 BLACK SWAN: Competitive Entry - {comp_name} with {competitor_impact:+.1f}% impact")
    
    data_dir = "C:/Users/iLink/.gemini/antigravity/scratch/oasis/data"
    scratch_dir = "C:/Users/iLink/.gemini/antigravity/scratch"
    
    # 0. Load Historical Intelligence
    loader = HistoricalDataLoader(data_dir)
    seasonality_map = loader.load_seasonality_indices()
    trend_map = loader.load_item_trends()
    
    month_factor = seasonality_map.get(target_month.upper(), 1.0)
    logger.info(f"Seasonality Factor for {target_month}: {month_factor:.2f}")
    
    # 1. Initialize Allocation
    logger.info("Step 1: Generating Day 1 Allocation...")
    engine = OrderEngine(data_dir) # Fix: Point to oasis/data for intelligence
    engine.load_local_databases()
    
    # Load Scorecard Data (use v7 if available, fallback to v3)
    scorecard_path = os.path.join(scratch_dir, "Full_Product_Allocation_Scorecard_v7.csv")
    if not os.path.exists(scorecard_path):
        scorecard_path = os.path.join(scratch_dir, "Full_Product_Allocation_Scorecard_v3.csv")
    
    try:
        df = pd.read_csv(scorecard_path, encoding='utf-8-sig')
    except Exception:
        df = pd.read_csv(scorecard_path, encoding='latin1')
        
    df['Supplier'] = df['Supplier'].fillna('UNKNOWN')
    recommendations_in = []
    
    benchmark_value = 115_000_000.0
    traffic_scale = budget_override / benchmark_value
    logger.info(f"Demand Calibration: Traffic Scale = {traffic_scale:.5f}")
    scale_to_use_in_sim = 1.0

    for _, row in df.iterrows():
        p_name = str(row.get('Product')).strip().upper()
        trend = trend_map.get(p_name, 1.0)
        
        raw_sales = float(row.get('Avg_Daily_Sales', 0))
        scaled_sales = raw_sales * traffic_scale
        
        rec = {
            'product_name': row.get('Product'),
            'item_code': str(row.get('Product')),
            'product_category': row.get('Department'),
            'supplier_name': row.get('Supplier'),
            'avg_daily_sales': scaled_sales,
            'selling_price': float(row.get('Unit_Price', 0)),
            'current_stock': float(row.get('Current_Stock', 0)),
            'pack_size': 1,
            'is_consignment': False,
            'ABC_Class': row.get('ABC_Class', 'C'),
            'margin_pct': float(row.get('Margin_Pct', 0)),
            'is_staple': str(row.get('Is_Staple')).upper() == 'TRUE',
            'supplier_reliability': float(row.get('Supplier_Reliability', 0.5)),
            'estimated_delivery_days': float(row.get('Lead_Time_Days', 7)),
            'trend_multiplier': trend 
        }
        recommendations_in.append(rec)
    
    # Run Allocation
    # v7.7: Load Specific Monthly Demand for Hybrid Allocation
    seasonal_map = loader.load_monthly_demand(target_month)
    
    recs_for_engine = copy.deepcopy(recommendations_in)
    result = engine.apply_greenfield_allocation(recs_for_engine, total_budget=budget_override, seasonal_demand_map=seasonal_map)
    allocated_items = [r for r in result['recommendations'] if r.get('recommended_quantity', 0) > 0]
    
    logger.info(f"Day 1 Allocation Complete. Stocked {len(allocated_items)} SKUs")
    
    # 2. Initialize Simulation Components
    tracker = InventoryTracker()
    tracker.initialize_stock(allocated_items)
    
    sales_sim = SalesSimulator(seed=42)
    risk_model = RiskModel()
    replenisher = ReplenishmentLogic(check_frequency_days=1)
    
    # 3. Initial Risk Assessment
    hhi = risk_model.calculate_hhi_concentration(tracker.inventory)
    logger.info(f"Risk Assessment: Overall HHI = {hhi:.2f}")
    
    # Analyze critical suppliers
    critical_suppliers = risk_model.identify_critical_suppliers(tracker.inventory)
    if critical_suppliers:
        logger.info(f"Critical Suppliers Identified: {len(critical_suppliers)}")
        for cs in critical_suppliers[:5]:
            logger.info(f"  - {cs['supplier']}: {cs['share_pct']:.1f}% of {cs['department']} (${cs['revenue_at_risk']:,.0f}/mo)")
    
    # 4. Setup Black Swan Events
    supplier_failure_active = False
    supplier_failure_data = None
    competitive_event_active = False
    
    # Setup Competitive Event
    if competitor_template and competitor_template in SCENARIO_TEMPLATES:
        comp_event = SCENARIO_TEMPLATES[competitor_template]
        risk_model.set_competitive_event(comp_event)
        competitive_event_active = True
    elif competitor:
        comp_event = CompetitiveEvent(
            event_type=EventType.NEW_COMPETITOR,
            competitor_name=competitor,
            distance_meters=100,
            impact_pct=competitor_impact,
            start_day=1,
            ramp_up_days=competitor_ramp_days
        )
        risk_model.set_competitive_event(comp_event)
        competitive_event_active = True
    
    # 5. Run Simulation Loop
    logger.info(f"\nStep 2: Running {duration_days}-Day Simulation Loop...")
    logger.info("-" * 60)
    
    all_draft_orders = []
    daily_metrics = []
    
    for day in range(1, duration_days + 1):
        
        # A. Check for Scheduled Supplier Failure
        if (fail_supplier or fail_department) and day == fail_day and not supplier_failure_active:
            supplier_failure_data = risk_model.trigger_supplier_failure(
                tracker.inventory,
                supplier_name=fail_supplier,
                department=fail_department,
                duration_days=fail_duration,
                failure_mode=fail_mode
            )
            supplier_failure_active = True
            logger.info(f"Day {day}: ⚠️ SUPPLIER FAILURE TRIGGERED - {len(supplier_failure_data.get('blocked_skus', []))} SKUs blocked")
        
        # B. Check for Failure Recovery
        if supplier_failure_active and day >= (fail_day + fail_duration):
            supplier_name = fail_supplier or risk_model.get_top_supplier_for_department(tracker.inventory, fail_department)
            if supplier_name:
                restored = risk_model.restore_supplier(tracker.inventory, supplier_name)
                supplier_failure_active = False
                logger.info(f"Day {day}: ✓ SUPPLIER RESTORED - {restored} SKUs back online")
        
        # C. Receive Stock (Morning)
        received = tracker.receive_stock(day)
        
        # D. Calculate Demand Multiplier (Competitive Erosion)
        if competitive_event_active:
            # Get base competitive multiplier
            comp_multiplier = risk_model.get_demand_multiplier(day)
        else:
            comp_multiplier = 1.0
        
        # E. Simulate Sales (Day) - Apply competitive erosion to month_factor
        effective_factor = month_factor * comp_multiplier
        daily_stats = tracker.process_daily_sales(
            sales_sim, 
            day_index=day, 
            month_factor=effective_factor, 
            store_scale_factor=scale_to_use_in_sim
        )
        
        # F. Replenishment Check (Evening)
        draft_orders = replenisher.check_for_reorder(
            tracker.inventory, 
            day_index=day, 
            month_factor=month_factor,
            sales_simulator=sales_sim  # v7.2 Fix: Pass simulator for Lookahead Logic
        )
        
        # Process orders (skip blocked suppliers)
        valid_orders = []
        for do in draft_orders:
            # Check if supplier is blocked
            supplier = do.get('supplier', '').upper().strip()
            if supplier in risk_model.active_supplier_failures:
                continue  # Skip blocked supplier orders
            
            do['day_generated'] = day
            lt = do.get('lead_time_days', 2)
            arrival = day + int(lt) + 1
            
            tracker.pending_orders.append({
                'sku': do['sku'],
                'qty': do['qty'],
                'arrival_day': arrival
            })
            
            do['arrival_day'] = arrival
            valid_orders.append(do)
            all_draft_orders.append(do)
        
        # G. Track Daily Metrics
        fill_rate = 0.0
        if daily_stats['revenue'] + daily_stats['lost_revenue'] > 0:
            fill_rate = daily_stats['revenue'] / (daily_stats['revenue'] + daily_stats['lost_revenue']) * 100
        
        daily_metrics.append({
            'day': day,
            'revenue': daily_stats['revenue'],
            'lost_revenue': daily_stats['lost_revenue'],
            'fill_rate': fill_rate,
            'stockouts': daily_stats['stockouts'],
            'orders_placed': len(valid_orders),
            'competitive_multiplier': comp_multiplier,
            'supplier_failure_active': supplier_failure_active
        })
        
        # H. Weekly Logging
        if day % 7 == 0:
            status = ""
            if supplier_failure_active:
                status += " [SUPPLIER CRISIS]"
            if competitive_event_active and comp_multiplier < 0.99:
                status += f" [COMPETITOR: {comp_multiplier:.1%}]"
            
            logger.info(f"Week {day//7}: Fill={fill_rate:.1f}%, Rev=${daily_stats['revenue']:,.0f}, "
                       f"Lost=${daily_stats['lost_revenue']:,.0f}{status}")
    
    # 6. Generate Reports
    logger.info("-" * 60)
    logger.info("SIMULATION COMPLETE")
    logger.info("=" * 60)
    
    # Calculate Summary KPIs
    total_revenue = tracker.total_revenue
    total_lost = tracker.total_lost_revenue
    avg_fill_rate = sum(m['fill_rate'] for m in daily_metrics) / len(daily_metrics) if daily_metrics else 0
    
    logger.info(f"Total Revenue:     ${total_revenue:,.2f}")
    logger.info(f"Lost Revenue:      ${total_lost:,.2f}")
    logger.info(f"Average Fill Rate: {avg_fill_rate:.1f}%")
    
    # Black Swan Impact Summary
    if supplier_failure_data and supplier_failure_data.get('blocked_skus'):
        # Calculate period impact
        failure_days = [m for m in daily_metrics if m['supplier_failure_active']]
        if failure_days:
            failure_lost = sum(m['lost_revenue'] for m in failure_days)
            logger.info(f"\n⚠️ SUPPLIER FAILURE IMPACT:")
            logger.info(f"   SKUs Blocked: {len(supplier_failure_data['blocked_skus'])}")
            logger.info(f"   Duration: {fail_duration} days")
            logger.info(f"   Lost Revenue During Crisis: ${failure_lost:,.2f}")
    
    if competitive_event_active:
        # Calculate competitive erosion
        total_erosion = sum(1 - m['competitive_multiplier'] for m in daily_metrics)
        avg_erosion = total_erosion / len(daily_metrics) if daily_metrics else 0
        logger.info(f"\n🏪 COMPETITIVE EVENT IMPACT:")
        logger.info(f"   Competitor: {competitor or competitor_template}")
        logger.info(f"   Average Demand Erosion: {avg_erosion:.1%}")
        logger.info(f"   Estimated Lost Sales: ${total_revenue * avg_erosion:,.2f}")
    
    # 7. Export Results
    if all_draft_orders:
        out_csv = f"orders_{scenario_name}_{target_month}.csv"
        pd.DataFrame(all_draft_orders).to_csv(out_csv, index=False)
        logger.info(f"\nOrders saved to: {out_csv}")
    
    # Export daily metrics
    metrics_csv = f"simulation_metrics_{scenario_name}_{target_month}.csv"
    pd.DataFrame(daily_metrics).to_csv(metrics_csv, index=False)
    logger.info(f"Metrics saved to: {metrics_csv}")
    
    # Export Simulation Feedback for Gap Analysis
    feedback_file = os.path.join(scratch_dir, "oasis/data/simulation_feedback.json")
    sku_feedback = {}
    
    for sku, data in tracker.inventory.items():
        # Only log items that had issues or significant activity
        if data.get('stockout_days', 0) > 0 or data.get('lost_sales_units', 0) > 0:
            sku_feedback[sku] = {
                'stockout_frequency': data['stockout_days'] / duration_days,
                'avg_first_stockout_day': data['first_stockout_day'],
                'lost_sales': data['lost_sales_units'],
                'stockout_days': data['stockout_days']
            }
            
    feedback_data = {
        'simulation_count': 1, # Reset for this run
        'sku_feedback': sku_feedback
    }
    
    with open(feedback_file, 'w') as f:
        json.dump(feedback_data, f, indent=4)
        
    logger.info(f"Feedback Analysis saved to: {feedback_file}")
    
    return {
        'total_revenue': total_revenue,
        'total_lost': total_lost,
        'avg_fill_rate': avg_fill_rate,
        'daily_metrics': daily_metrics,
        'supplier_failure_impact': supplier_failure_data,
        'competitive_active': competitive_event_active
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="OASIS Simulation Runner with Black Swan Events",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic simulation
  python run_simulation_scenario.py --scenario Baseline --days 30 --budget 300000

  # Competitor entry (Carrefour causing 6% decline)
  python run_simulation_scenario.py --scenario CarrefourEntry --competitor Carrefour --competitor-impact -6.0 --days 60

  # Critical supplier failure
  python run_simulation_scenario.py --scenario DairyCrisis --fail-dept "FRESH MILK" --fail-day 10 --days 30

  # Combined stress test
  python run_simulation_scenario.py --scenario StressTest --fail-supplier BROOKSIDE --competitor Carrefour --days 60
        """
    )
    
    # Basic Parameters
    parser.add_argument("--scenario", type=str, default="Baseline", help="Scenario name for output files")
    parser.add_argument("--month", type=str, default="JAN", help="Target Month (JAN, FEB... DEC)")
    parser.add_argument("--budget", type=float, default=300000.0, help="Opening Budget in KES")
    parser.add_argument("--days", type=int, default=30, help="Simulation Duration")
    
    # Supplier Failure Parameters
    parser.add_argument("--fail-supplier", type=str, help="Supplier to fail (e.g., 'BROOKSIDE')")
    parser.add_argument("--fail-dept", type=str, help="Fail top supplier of this department")
    parser.add_argument("--fail-day", type=int, default=10, help="Day to trigger failure")
    parser.add_argument("--fail-duration", type=int, default=14, help="Duration of failure in days")
    parser.add_argument("--fail-mode", type=str, default="COMPLETE", 
                       choices=["COMPLETE", "PARTIAL", "DELAYED"],
                       help="Failure mode: COMPLETE (no supply), PARTIAL (50%%), DELAYED (2x lead time)")
    
    # Competitive Event Parameters
    parser.add_argument("--competitor", type=str, help="Competitor name (e.g., 'Carrefour')")
    parser.add_argument("--competitor-impact", type=float, default=-6.0, help="YoY impact %% (negative = loss)")
    parser.add_argument("--competitor-ramp", type=int, default=30, help="Days to reach full impact")
    parser.add_argument("--competitor-template", type=str, 
                       choices=list(SCENARIO_TEMPLATES.keys()),
                       help="Use predefined competitive scenario")
    
    args = parser.parse_args()
    
    run_simulation(
        scenario_name=args.scenario,
        duration_days=args.days,
        target_month=args.month,
        budget_override=args.budget,
        fail_supplier=args.fail_supplier,
        fail_department=args.fail_dept,
        fail_day=args.fail_day,
        fail_duration=args.fail_duration,
        fail_mode=args.fail_mode,
        competitor=args.competitor,
        competitor_impact=args.competitor_impact,
        competitor_ramp_days=args.competitor_ramp,
        competitor_template=args.competitor_template
    )
