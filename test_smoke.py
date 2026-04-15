import sys
import os
import traceback

# Add project root to sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.append(project_root)

def smoke_test_component(name, import_stmt):
    print(f"--- Testing {name} ---")
    try:
        exec(import_stmt)
        print(f"[SUCCESS] {name} imported successfully.")
        return True
    except Exception as e:
        print(f"[FAILURE] {name} failed to import/initialize: {e}")
        # traceback.print_exc()
        return False

def test_order_engine():
    print("--- Testing OrderEngine ---")
    try:
        from oasis.logic.order_engine import OrderEngine
        data_dir = "oasis/data"
        if not os.path.exists(data_dir): os.makedirs(data_dir)
        engine = OrderEngine(data_dir=data_dir)
        print("[SUCCESS] OrderEngine initialized.")
        return True
    except Exception as e:
        print(f"[FAILURE] OrderEngine error: {e}")
        return False

def main():
    results = {}
    
    # Core Logic
    results['OrderEngine'] = test_order_engine()
    
    # Major Apps (Imports)
    results['IntegratedApp_Import'] = smoke_test_component("IntegratedApp", "import integrated_app")
    results['AllocationApp_Import'] = smoke_test_component("AllocationApp", "import allocation_app")
    results['OpsDashboard_Import'] = smoke_test_component("OpsDashboard", "import ops_dashboard")
    results['RetailSimulator_Import'] = smoke_test_component("RetailSimulator", "import retail_simulator")
    
    # Services
    results['SchedulerService'] = smoke_test_component("OasisScheduler", "from oasis.logic.scheduler_service import OasisScheduler")
    results['NotificationService'] = smoke_test_component("NotificationService", "from oasis.logic.notification_service import NotificationService")
    results['FulfillmentDecider'] = smoke_test_component("FulfillmentDecider", "from oasis.logic.fulfillment_decider import FulfillmentDecider")

    # Additional Entry Points from Batch Scripts
    results['SimulationScenarioRunner'] = smoke_test_component("SimulationScenario", "import run_simulation_scenario")
    results['ST_GAT_Dashboard'] = smoke_test_component("ST-GAT Dashboard", "import st_gat_dashboard")
    results['FletMain_Offline'] = smoke_test_component("Flet Main Offline", "import oasis.main")
    results['FletMain_Online'] = smoke_test_component("Flet Main Online", "import oasis.main_online")

    print("\n" + "="*30)
    print("SMOKE TEST SUMMARY")
    print("="*30)
    all_passed = True
    for component, passed in results.items():
        status = "PASSED" if passed else "FAILED"
        print(f"{component:30} : {status}")
        if not passed: all_passed = False
    
    if all_passed:
        print("\nAll major components are able to initialize perfectly!")
    else:
        print("\nSome components failed. Check the logs above.")

if __name__ == "__main__":
    main()
