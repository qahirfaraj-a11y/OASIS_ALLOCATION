import pandas as pd
import os

item_csv = 'powerbi_item_analysis.csv'
supplier_csv = 'powerbi_supplier_scorecard.csv'
output_xlsx = 'powerbi_data_inspection.xlsx'

try:
    with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
        if os.path.exists(item_csv):
            print(f"Converting {item_csv}...")
            pd.read_csv(item_csv).to_excel(writer, sheet_name='Item Analysis', index=False)
        else:
            print(f"Warning: {item_csv} not found.")

        if os.path.exists(supplier_csv):
            print(f"Converting {supplier_csv}...")
            pd.read_csv(supplier_csv).to_excel(writer, sheet_name='Supplier Scorecard', index=False)
        else:
            print(f"Warning: {supplier_csv} not found.")
            
    print(f"Successfully created {output_xlsx}")
except Exception as e:
    print(f"Error converting to Excel: {e}")
