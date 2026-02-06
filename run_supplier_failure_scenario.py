"""
Interactive Supplier Failure Scenario Selection
================================================
CLI tool for selecting and simulating black swan supplier failure events.

Usage:
    python run_supplier_failure_scenario.py
    python run_supplier_failure_scenario.py --list-suppliers "FRESH MILK"
    python run_supplier_failure_scenario.py --store-type Online_5M --days 30
"""

import sys
import argparse
import pandas as pd
from pathlib import Path

# Add parent path for imports
sys.path.insert(0, str(Path(__file__).parent))

from oasis.analytics.supplier_analytics import (
    get_top_suppliers_by_department,
    get_major_categories,
    analyze_supplier_failure_impact,
    print_supplier_dropdown,
    load_scorecard_data
)
from retail_simulator import STORE_UNIVERSES, RetailSimulator, print_simulation_summary, export_to_excel

# Data paths
SCORECARD_FILE = Path(r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv")
OUTPUT_DIR = Path(r"c:\Users\iLink\.gemini\antigravity\scratch")


def list_suppliers_command(category: str):
    """List top 10 suppliers for a given category."""
    print_supplier_dropdown(category, 10)


def interactive_failure_selection():
    """
    Interactive CLI for selecting a supplier failure scenario.
    
    Steps:
    1. Select major category
    2. View top 10 suppliers in that category
    3. Select supplier to fail
    4. Preview impact
    5. Confirm and run simulation
    """
    print("\n" + "=" * 70)
    print("🦢 BLACK SWAN SIMULATION: Supplier Failure Scenario")
    print("=" * 70)
    
    # Load scorecard for analysis
    try:
        df = load_scorecard_data()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return None
    
    # 1. Select Category
    categories = get_major_categories()
    print("\n📦 Select a major product category:\n")
    for i, cat in enumerate(categories, 1):
        print(f"  {i:2}. {cat}")
    
    try:
        cat_input = input("\nEnter category number (1-15): ").strip()
        cat_idx = int(cat_input) - 1
        if not (0 <= cat_idx < len(categories)):
            print("Invalid selection. Exiting.")
            return None
        selected_category = categories[cat_idx]
    except (ValueError, KeyboardInterrupt):
        print("\nCancelled.")
        return None
    
    # 2. Get Top 10 Suppliers
    top_suppliers = get_top_suppliers_by_department(df, selected_category, 10)
    
    if not top_suppliers:
        print(f"\nNo suppliers found for category: {selected_category}")
        return None
    
    print(f"\n📊 Top Suppliers in {selected_category}:")
    print("-" * 70)
    print(f"{'#':>3} | {'Supplier':<40} | {'Share %':>8} | {'SKUs':>5}")
    print("-" * 70)
    
    for i, sup in enumerate(top_suppliers, 1):
        print(f"{i:>3} | {sup.supplier_name[:40]:<40} | {sup.share_pct:>7.1f}% | {sup.sku_count:>5}")
    
    print("-" * 70)
    
    # 3. Select Supplier to Fail
    try:
        sup_input = input(f"\nSelect supplier to fail (1-{len(top_suppliers)}): ").strip()
        sup_idx = int(sup_input) - 1
        if not (0 <= sup_idx < len(top_suppliers)):
            print("Invalid selection. Exiting.")
            return None
        selected_supplier = top_suppliers[sup_idx].supplier_name
    except (ValueError, KeyboardInterrupt):
        print("\nCancelled.")
        return None
    
    # 4. Show Impact Preview
    impact = analyze_supplier_failure_impact(df, selected_supplier, selected_category)
    
    print(f"\n⚠️  IMPACT PREVIEW: {selected_supplier}")
    print("=" * 50)
    print(f"  Revenue at Risk:     KES {impact['revenue_at_risk']:>12,.0f}")
    print(f"  Coverage Loss:       {impact['coverage_loss_pct']:>11.1f}%")
    print(f"  Affected SKUs:       {impact['affected_sku_count']:>11}")
    print(f"  Substitute Avail:    {impact['substitute_availability']*100:>10.0f}%")
    print(f"  Severity:            {impact['estimated_stockout_severity']:>11}")
    print("=" * 50)
    
    # 5. Confirm and Configure
    confirm = input("\nProceed with simulation? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Simulation cancelled.")
        return None
    
    # Get failure parameters
    try:
        start_day = input("Start day of failure (default 10): ").strip()
        start_day = int(start_day) if start_day else 10
        
        duration = input("Duration in days (default 14): ").strip()
        duration = int(duration) if duration else 14
        
        failure_mode = input("Failure mode [COMPLETE/PARTIAL/DELAYED] (default COMPLETE): ").strip().upper()
        if failure_mode not in ['COMPLETE', 'PARTIAL', 'DELAYED']:
            failure_mode = 'COMPLETE'
    except (ValueError, KeyboardInterrupt):
        print("\nUsing defaults.")
        start_day = 10
        duration = 14
        failure_mode = 'COMPLETE'
    
    return {
        'supplier': selected_supplier,
        'department': selected_category,
        'start_day': start_day,
        'duration': duration,
        'failure_mode': failure_mode,
        'impact_preview': impact
    }


def run_failure_simulation(scenario: dict, store_type: str = "Medium_1M", days: int = 30, export_excel: bool = True):
    """
    Run the simulation with the selected supplier failure scenario.
    """
    if store_type not in STORE_UNIVERSES:
        print(f"Error: Unknown store type '{store_type}'")
        print(f"Available: {list(STORE_UNIVERSES.keys())}")
        return None
    
    config = STORE_UNIVERSES[store_type]
    
    print(f"\n🏪 Running Simulation: {store_type}")
    print(f"   Budget: KES {config['budget']:,}")
    print(f"   Days: {days}")
    print(f"   Failure: {scenario['supplier']} ({scenario['failure_mode']})")
    print(f"   Start Day: {scenario['start_day']}, Duration: {scenario['duration']} days")
    print()
    
    # Initialize simulator
    simulator = RetailSimulator(store_type, config)
    
    # Set up the failure event
    from oasis.simulation.simulation_engine import RiskModel
    risk_model = RiskModel()
    
    # Convert SKU states to inventory dict format for RiskModel
    inventory = {}
    for sku_name, sku_state in simulator.skus.items():
        inventory[sku_name] = {
            'current_stock': sku_state.current_stock,
            'avg_daily_sales': sku_state.avg_daily_sales,
            'price': sku_state.unit_price,
            'supplier': sku_state.supplier,
            'department': sku_state.department,
            'lead_time_days': sku_state.lead_time_days
        }
    
    # We'll trigger the failure at the specified start day
    # For now, run the simulation (the failure injection would be done mid-simulation)
    result = simulator.run(days=days)
    
    # Print summary
    print_simulation_summary(result)
    
    # Export to Excel if requested
    if export_excel:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = OUTPUT_DIR / f"simulation_results_{store_type}_{timestamp}.xlsx"
        export_to_excel([result], str(excel_file))
        print(f"\n📊 Results exported to: {excel_file}")
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Black Swan Supplier Failure Simulation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_supplier_failure_scenario.py
      Interactive mode - select category, supplier, and run simulation
      
  python run_supplier_failure_scenario.py --list-suppliers "FRESH MILK"
      List top 10 suppliers in FRESH MILK category
      
  python run_supplier_failure_scenario.py --store-type Online_5M --days 30
      Run online store simulation for 30 days
      
  python run_supplier_failure_scenario.py --store-type Online_5M --days 30 --export
      Run simulation and export results to Excel
        """
    )
    
    parser.add_argument("--list-suppliers", type=str, metavar="CATEGORY",
                        help="List top 10 suppliers for a category")
    
    parser.add_argument("--store-type", type=str, default="Medium_1M",
                        choices=list(STORE_UNIVERSES.keys()),
                        help="Store archetype to simulate")
    
    parser.add_argument("--days", type=int, default=30,
                        help="Number of days to simulate")
    
    parser.add_argument("--interactive", action="store_true",
                        help="Interactive supplier failure selection")
    
    parser.add_argument("--export", action="store_true",
                        help="Export results to Excel file with daily logs and stockouts")
    
    args = parser.parse_args()
    
    # List suppliers mode
    if args.list_suppliers:
        list_suppliers_command(args.list_suppliers)
        return
    
    # Interactive mode
    if args.interactive or len(sys.argv) == 1:
        scenario = interactive_failure_selection()
        if scenario:
            run_failure_simulation(scenario, args.store_type, args.days, export_excel=args.export)
    else:
        # Just run a simulation without failure
        print(f"Running {args.store_type} simulation for {args.days} days...")
        config = STORE_UNIVERSES[args.store_type]
        simulator = RetailSimulator(args.store_type, config)
        result = simulator.run(days=args.days)
        print_simulation_summary(result)
        
        # Export if requested
        if args.export:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_file = OUTPUT_DIR / f"simulation_results_{args.store_type}_{timestamp}.xlsx"
            export_to_excel([result], str(excel_file))
            print(f"\n[OK] Results exported to: {excel_file}")


if __name__ == "__main__":
    main()

