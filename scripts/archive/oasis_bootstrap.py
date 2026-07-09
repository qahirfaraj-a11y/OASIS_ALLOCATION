"""
O.A.S.I.S. Day-0 Bootstrap CLI
=================================
Initializes a new retail universe from scratch.
Run this once after installation to ingest historical data
and warm up the intelligence engines.

Usage:
    python oasis_bootstrap.py
    python oasis_bootstrap.py --config path/to/config.json
    python oasis_bootstrap.py --skip-engines  (data only, no AMIT/LATA/DHARAM)
"""

import os
import sys
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Ensure project root is in path
PROJECT_ROOT = str(Path(__file__).parent.resolve())
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

os.environ["PYTHONUTF8"] = "1"
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BOOTSTRAP] %(levelname)s: %(message)s",
)
logger = logging.getLogger("OASIS.Bootstrap")


def print_banner():
    print()
    print("  ╔══════════════════════════════════════════════════╗")
    print("  ║     O.A.S.I.S. Day-0 Bootstrap                  ║")
    print("  ║     Retail Universe Initialization               ║")
    print("  ╚══════════════════════════════════════════════════╝")
    print()


def run_bootstrap(config_path: str = None, skip_engines: bool = False):
    """Execute the full Day-0 bootstrap sequence."""
    start = time.time()

    from oasis.logic.data_gateway import DataGateway

    print_banner()

    # Step 1: Load config and create gateway
    print("[1/6] Loading client configuration...")
    gw = DataGateway(config_path=config_path)
    config = gw.config
    client_name = config.get("client", {}).get("client_name", "Unknown")
    print(f"       Client: {client_name}")
    print(f"       Pathway: {gw.pathway}")
    print(f"       Stores: {len(config.get('stores', []))}")
    print()

    # Step 2: Initialize database
    print("[2/6] Initializing database...")
    try:
        from oasis.logic.db_connector import ensure_oasis_tables
        ensure_oasis_tables(gw.db_path)
        print(f"       [OK] Database ready: {gw.db_path}")
    except Exception as e:
        print(f"       [FAIL] {e}")
        return False
    print()

    # Step 3: Ingest stock data
    print("[3/6] Ingesting product catalog...")
    total_products = 0
    stores = config.get("stores", [])

    if not stores:
        # No stores configured — try default ingestion
        print("       No stores in config. Attempting global ingestion...")
        products = gw.get_stock_snapshot("GLOBAL")
        total_products = len(products)
        print(f"       [OK] {total_products} products loaded")
    else:
        for store in stores:
            org_cd = store["org_cd"]
            name = store.get("name", org_cd)
            try:
                products = gw.get_stock_snapshot(org_cd)
                total_products += len(products)
                print(f"       [OK] {name}: {len(products)} products")
            except Exception as e:
                print(f"       [FAIL] {name}: {e}")
    print(f"       Total: {total_products} products across {max(len(stores), 1)} store(s)")
    print()

    # Step 4: Ingest sales history
    print("[4/6] Loading sales history...")
    try:
        first_org = stores[0]["org_cd"] if stores else "GLOBAL"
        sales = gw.get_sales_history(first_org, days=90)
        print(f"       [OK] {len(sales)} sales records (90-day window)")
    except Exception as e:
        print(f"       [WARN] {e}")
    print()

    # Step 5: Ingest GRN history
    print("[5/6] Loading supplier delivery history...")
    try:
        grn = gw.get_grn_history(days=365)
        if not grn.empty:
            print(f"       [OK] {len(grn)} GRN records (12-month window)")
        else:
            print("       [INFO] No GRN data available. LATA will use defaults.")
    except Exception as e:
        print(f"       [WARN] {e}")
    print()

    # Step 6: Engine warm-up
    if skip_engines:
        print("[6/6] Engine warm-up SKIPPED (--skip-engines flag)")
    else:
        print("[6/6] Warming up intelligence engines...")
        engine_cfg = config.get("engines", {})
        scorecard = gw._resolve_scorecard_path()
        data_dir = gw.data_dir
        nn_path = engine_cfg.get("gnn_model_path") or os.path.join(
            PROJECT_ROOT, "neutral_network_export"
        )

        # AMIT
        if engine_cfg.get("amit", {}).get("enabled", True) and scorecard:
            try:
                from oasis.logic.amit_governance import AMITGovernance
                amit = AMITGovernance(data_dir)
                neg = amit.generate_negative_list(scorecard)
                blocked = len(neg) if not neg.empty else 0
                print(f"       [OK] AMIT: {blocked} items on negative list")
            except Exception as e:
                print(f"       [WARN] AMIT: {e}")

        # LATA
        if engine_cfg.get("lata", {}).get("enabled", True):
            try:
                from oasis.logic.lata_shield import run_lata
                run_lata(data_dir, nn_path)
                print("       [OK] LATA: Supplier risk baselines generated")
            except Exception as e:
                print(f"       [WARN] LATA: {e}")

        # DHARAM
        if engine_cfg.get("dharam", {}).get("enabled", True):
            try:
                from oasis.logic.dharam_revenue import run_dharam
                result = run_dharam(nn_path, data_dir)
                patches = result.get("stats", {}).get("total_demand_patches", 0)
                print(f"       [OK] DHARAM: {patches} ghost demand patches")
            except Exception as e:
                print(f"       [WARN] DHARAM: {e}")

    elapsed = time.time() - start
    print()
    print("  ══════════════════════════════════════════════════")
    print(f"  Bootstrap completed in {elapsed:.1f} seconds")
    print(f"  Products: {total_products}")
    print(f"  Database: {gw.db_path}")
    print()
    print("  NEXT: Start the O.A.S.I.S. service:")
    print(f"    python oasis_service.py --run-direct")
    print()
    print("  Or launch a dashboard:")
    print(f"    python -m streamlit run ops_dashboard.py")
    print("  ══════════════════════════════════════════════════")
    print()

    return True


def main():
    parser = argparse.ArgumentParser(description="O.A.S.I.S. Day-0 Bootstrap")
    parser.add_argument("--config", type=str, default=None,
                        help="Path to oasis_client_config.json")
    parser.add_argument("--skip-engines", action="store_true",
                        help="Skip AMIT/LATA/DHARAM warm-up")
    args = parser.parse_args()

    success = run_bootstrap(
        config_path=args.config,
        skip_engines=args.skip_engines,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
