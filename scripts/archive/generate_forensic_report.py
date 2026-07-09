import pandas as pd
import os
from pitch_data_ingestor_v2 import ForensicOperationsIngestor
from datetime import datetime

# Configuration
ORG_ID = "ORG001"
SALES_FILE = r"C:\Oasis\inbound_drops\bootstrap\ORG001_sales.csv"
GRN_FILE = r"C:\Oasis\inbound_drops\bootstrap\ORG001_grn.csv"
STOCK_FILE = r"C:\Oasis\inbound_drops\bootstrap\ORG001_stock.csv"
REPORT_OUTPUT = r"C:\Oasis\inbound_drops\bootstrap\ORG001_Forensic_Audit_Report.xlsx"

def run_audit():
    print(f"Starting Forensic Audit for {ORG_ID}...")
    
    # Initialize Ingestor
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ingestor = ForensicOperationsIngestor(base_dir)
    
    # Load Logs
    print("Loading enriched data logs...")
    ingestor.load_logs(
        pos_file=SALES_FILE,
        grn_file=GRN_FILE,
        inventory_file=STOCK_FILE
    )
    
    # Run Forensic Engines
    print("Running AMIT (Capital Trap)...")
    ingestor.run_pos_analysis()
    
    print("Running LATA (Supplier Toxicity)...")
    ingestor.run_supplier_analysis()
    
    print("Running MANDE (Entropy & Shrink)...")
    ingestor.run_network_analysis()
    
    # Get Audit Data
    audit = ingestor.get_full_audit()
    
    # Prepare DataFrames for Export
    print("Preparing export sheets...")
    
    # 1. Dead Stock (Capital Trap)
    dead_stock_df = pd.DataFrame(audit['catalog']['dead_stock_list'])
    
    # 2. Ghost Demand (Revenue Bleed)
    ghost_demand_df = pd.DataFrame(audit['catalog']['ghost_demand_list'])
    
    # 3. Supplier Toxicity (LATA)
    supplier_toxic_df = pd.DataFrame(audit['suppliers']['supplier_list'])
    
    # 4. Executive Summary
    summary_data = [
        {"Metric": "Total SKUs Scanned", "Value": audit['catalog']['total_skus_scanned']},
        {"Metric": "Total Capital Tied (KES)", "Value": f"{audit['catalog']['total_capital_tied']:,.2f}"},
        {"Metric": "Dead Stock Value (KES)", "Value": f"{audit['catalog']['dead_stock_value']:,.2f}"},
        {"Metric": "Ghost Demand / Revenue Bleed (KES)", "Value": f"{audit['catalog']['ghost_demand_value']:,.2f}"},
        {"Metric": "At-Risk (Criminal/Hostile) Suppliers", "Value": audit['suppliers']['criminal_count']},
    ]
    summary_df = pd.DataFrame(summary_data)
    
    # Export to Excel
    print(f"Exporting results to {REPORT_OUTPUT}...")
    with pd.ExcelWriter(REPORT_OUTPUT, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='Executive Summary', index=False)
        dead_stock_df.to_excel(writer, sheet_name='AMIT - Dead Stock', index=False)
        ghost_demand_df.to_excel(writer, sheet_name='DHARAM - Ghost Demand', index=False)
        supplier_toxic_df.to_excel(writer, sheet_name='LATA - Supplier Toxicity', index=False)
        
    print("\n[SUCCESS] Forensic Audit Report generated successfully.")
    print(f"Location: {REPORT_OUTPUT}")

if __name__ == "__main__":
    run_audit()
