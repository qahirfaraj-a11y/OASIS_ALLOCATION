
import xlwings as xw
import pandas as pd
import os
import sys

# Ensure we can find the oasis module
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from oasis.logic.order_engine import OrderEngine

@xw.sub
def generate_optimization():
    """
    Reads the active sheet, runs OASIS Order Engine, and creates a recommendation sheet.
    """
    wb = xw.Book.caller()
    sheet = wb.sheets.active
    
    # 1. Read Data
    # Assume data starts at A1 with headers
    # We look for key columns: Product, Avg_Daily_Sales, Unit_Price, etc.
    try:
        df = sheet.range('A1').options(pd.DataFrame, expand='table', index=False).value
    except Exception as e:
        xw.msgbox(f"Error reading data: {e}\nMake sure your data has headers starting at A1.")
        return

    if df is None or df.empty:
        xw.msgbox("No data found in active sheet.")
        return

    # 2. Initialize Engine
    # Point to the 'oasis/data' directory relative to this script
    data_path = os.path.join(current_dir, 'oasis', 'data')
    try:
        engine = OrderEngine(data_path)
        engine.load_local_databases()
    except Exception as e:
        xw.msgbox(f"Failed to initialize OASIS Engine: {e}")
        return

    # 3. Prepare Data for Engine
    # Map Excel columns to Engine expected keys (flexible mapping)
    # Expected: product_name, avg_daily_sales, selling_price, etc.
    
    input_records = []
    
    # Simple column mapper (Key = Engine, Value = Likely Excel Header)
    col_map = {
        'product_name': ['Product', 'Item', 'Description', 'Name'],
        'avg_daily_sales': ['Avg_Daily_Sales', 'ADS', 'Daily Sales', 'Velocity'],
        'selling_price': ['Unit_Price', 'Price', 'Selling Price', 'RRP'],
        'current_stock': ['Current_Stock', 'Stock', 'QOH', 'Inventory'],
        'product_category': ['Department', 'Category', 'Dept'],
        'supplier_name': ['Supplier', 'Vendor'],
        'pack_size': ['Pack_Size', 'Pack'],
        'lead_time_days': ['Lead_Time', 'Lead_Time_Days']
    }
    
    # Normalize DF columns to lowercase for matching
    df_cols = {c.lower(): c for c in df.columns}
    
    mapping = {}
    for engine_key, candidates in col_map.items():
        for cand in candidates:
            if cand.lower() in df_cols:
                mapping[engine_key] = df_cols[cand.lower()]
                break
    
    if 'product_name' not in mapping:
        xw.msgbox("Critical Error: Could not find a 'Product' column.\nPlease ensure your data has a header named 'Product' or 'Item'.")
        return

    for _, row in df.iterrows():
        rec = {
            'product_name': row.get(mapping.get('product_name')),
            'avg_daily_sales': float(row.get(mapping.get('avg_daily_sales'), 0) or 0),
            'selling_price': float(row.get(mapping.get('selling_price'), 0) or 0),
            'current_stock': float(row.get(mapping.get('current_stock'), 0) or 0),
            'product_category': row.get(mapping.get('product_category'), 'GENERAL'),
            'supplier_name': row.get(mapping.get('supplier_name'), 'Unknown'),
            'estimated_delivery_days': float(row.get(mapping.get('lead_time_days'), 2) or 2), # Default 2 from fix
            'is_consignment': False, # Can add column logic later
            'pack_size': 1
        }
        input_records.append(rec)

    # 4. Run Logic
    # Ask user for budget? For now, let's assume Replenishment (Unlimited) or ask via InputBox
    # OR: Just run Apply Greenfield with a high budget to see "Pure Demand"
    
    # Let's prompt for Budget
    budget_input = wb.app.inputbox("Enter Capital Budget for Allocation ($):", Default="1000000", Type=1)
    if budget_input is False: # Cancelled
        return
        
    budget = float(budget_input)
    
    # Enrich first
    enriched = engine.enrich_product_data(input_records)
    
    # Run Allocation
    result = engine.apply_greenfield_allocation(enriched, total_budget=budget)
    recs = result['recommendations']
    summary = result['summary']
    
    # 5. Write Output
    output_rows = []
    for r in recs:
        qty = r.get('recommended_quantity', 0)
        if qty > 0:
            output_rows.append({
                'Product': r['product_name'],
                'Department': r['product_category'],
                'Supplier': r.get('supplier_name', 'Unknown'),
                'Recommended_Qty': qty,
                'Allocated_Cost': r.get('allocated_cost', 0) if 'allocated_cost' in r else (qty * r.get('selling_price',0)*0.75), # Approx
                'Reasoning': r.get('reasoning', '')
            })
            
    if not output_rows:
        xw.msgbox("Optimization Complete.\n\nNo items were recommended (check Budget or Sales Data).")
        return

    out_df = pd.DataFrame(output_rows)
    
    # Create new sheet
    sheet_name = "OASIS_Output"
    if sheet_name in [s.name for s in wb.sheets]:
        wb.sheets[sheet_name].delete()
        
    out_sheet = wb.sheets.add(sheet_name)
    out_sheet.range('A1').value = out_df
    out_sheet.autofit()
    
    # Summary Box
    metrics = [
        ["Metric", "Value"],
        ["Total Items", len(output_rows)],
        ["Budget Used", f"${summary['total_cost']:.2f}"],
        ["Utilization", f"{summary['utilization_pct']:.1f}%"]
    ]
    out_sheet.range('G1').value = metrics
    
    xw.msgbox(f"Optimization Complete!\nRecommended {len(output_rows)} items.")

@xw.sub
def create_template():
    """Generates a blank template for the user."""
    wb = xw.Book.caller()
    sheet = wb.sheets.add("OASIS_Template")
    headers = ["Product", "Department", "Supplier", "Avg_Daily_Sales", "Unit_Price", "Current_Stock"]
    sheet.range('A1').value = headers
    sheet.range('A2').value = ["Example Product", "Groceries", "Supplier A", 10, 100, 0]
    sheet.autofit()

if __name__ == "__main__":
    xw.Book("oasis_excel.xlsm").set_mock_caller()
    generate_optimization()
