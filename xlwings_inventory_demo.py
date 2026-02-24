import xlwings as xw
import pandas as pd
import random
from datetime import datetime

def run_inventory_demo():
    # 1. Connect to or create the Excel Workbook
    # This checks if there's an active book, otherwise creates one.
    # We specifically name it to avoid the "BookN" error you saw!
    wb_name = "Inventory_Manager.xlsx"
    try:
        wb = xw.Book(wb_name)
    except FileNotFoundError:
        wb = xw.Book()
        wb.save(wb_name)
    
    # 2. Setup Sheets
    # We want a 'Config' sheet for user inputs and 'Results' for outputs
    required_sheets = ['Config', 'Results']
    for sheet_name in required_sheets:
        if sheet_name not in [s.name for s in wb.sheets]:
            wb.sheets.add(sheet_name)
            
    sht_config = wb.sheets['Config']
    sht_results = wb.sheets['Results']

    # 3. Setup Default Config if empty (First run experience)
    if sht_config.range('A1').value is None:
        sht_config.range('A1').value = [['Parameter', 'Value'],
                                        ['Total Budget', 50000],
                                        ['Min Order Value', 1000],
                                        ['Safety Stock (Days)', 14],
                                        ['Restock Threshold', 0.2]]
        # Make it look nice
        sht_config.range('A1:B1').font.bold = True
        sht_config.range('A1:B5').color = (220, 230, 241) # Light blue
        sht_config.autofit()

    # 4. Read Configuration from Excel
    # This is the "Frontend" part - reading what the user typed in Excel
    print("Reading configuration from Excel...")
    config_df = sht_config.range('A1').options(pd.DataFrame, expand='table', index=False).value
    # Convert list of parameters to a dictionary for easy access
    config = dict(zip(config_df['Parameter'], config_df['Value']))
    
    budget = config.get('Total Budget', 50000)
    mov = config.get('Min Order Value', 1000)
    
    print(f"Running allocation with Budget: ${budget:,.2f} and MOV: ${mov:,.2f}")

    # 5. Run Mock Allocation Logic (The "Backend")
    # Generating some dummy inventory data to simulate your real logic
    items = ['Olive Oil 5L', 'Rice 25kg', 'Pasta 500g', 'Canned Tomatoes', 'Tuna Cans', 'Flour 1kg', 'Sugar 1kg']
    data = []
    
    current_spend = 0
    
    for item in items:
        # Mock logic: Random stock levels
        current_stock = random.randint(0, 50)
        demand = random.randint(5, 20)
        
        # Simple Reorder Logic
        reorder_qty = 0
        cost = random.uniform(5, 50)
        
        if current_stock < demand * 2: # Low stock logic
            reorder_qty = demand * 4
            
        total_cost = reorder_qty * cost
        
        # Check Budget
        if current_spend + total_cost <= budget:
            status = "Approved"
            current_spend += total_cost
        else:
            status = "Budget Exceeded"
            reorder_qty = 0
            total_cost = 0

        data.append([item, current_stock, demand, cost, reorder_qty, total_cost, status])

    # Create DataFrame
    columns = ['Item', 'Current Stock', 'Daily Demand', 'Unit Cost', 'Reorder Qty', 'Total Cost', 'Status']
    df_results = pd.DataFrame(data, columns=columns)

    # 6. Write Results back to Excel
    # This is the "Reporting" part
    print("Writing results to Excel...")
    
    # Clear previous results
    sht_results.clear()
    
    # Write Header and Data
    sht_results.range('A1').value = df_results
    
    # 7. Apply Formatting with xlwings
    # Header formatting
    sht_results.range('A1:G1').font.bold = True
    sht_results.range('A1:G1').color = (0, 0, 0) # Black background
    sht_results.range('A1:G1').font.color = (255, 255, 255) # White text
    
    # Highlight "Budget Exceeded" rows
    end_row = len(df_results) + 1
    # Check status column (G)
    status_col = sht_results.range(f'G2:G{end_row}').value
    for i, status in enumerate(status_col):
        if status == "Budget Exceeded":
            # Highlight the whole row red
            row_num = i + 2
            sht_results.range(f'A{row_num}:G{row_num}').color = (255, 199, 206) # Light Red
    
    # Auto-fit columns
    sht_results.autofit()
    
    print("Done! Check 'Inventory_Manager.xlsx'")

if __name__ == "__main__":
    run_inventory_demo()
