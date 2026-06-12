import pandas as pd

file_path = r"C:\Users\iLink\.gemini\antigravity\scratch\market_competitiveness_master.xlsx"

try:
    # Load Departmental Share
    df_dept = pd.read_excel(file_path, sheet_name='Departmental Share')
    
    # Filter for relevant departments (assuming Diapers, Sanitary Towels, Wipes, Fabric Conditioner)
    target_depts = ['Diapers', 'Sanitary Towels', 'Wipes', 'Fabric Conditioner', 'Hygiene'] # Adjust based on actual names if needed
    
    # Check what departments actually exist
    print("Available Departments:", df_dept['Department'].unique())
    
    # Filter rows where Department is in our target list (or normalize if needed)
    # The previous output showed "Diapers" as a department.
    # Note: The dataframe might have NaN for Department if it's merged cells, heavily depends on structure.
    # From previous `head` output: index 0 has Dept='Diapers', index 1 has NaN.
    # We need to forward fill the Department column if it's sparse.
    df_dept['Department'] = df_dept['Department'].ffill()
    
    relevant_df = df_dept[df_dept['Department'].isin(['Diapers', 'Sanitary Towels', 'Wipes', 'Fabric Conditioner'])]
    
    print("\n--- Competitor Share Breakdown by Department ---")
    # Group by Dept and Name, verify shares
    for dept in relevant_df['Department'].unique():
        print(f"\n[Department: {dept}]")
        dept_data = relevant_df[relevant_df['Department'] == dept].sort_values(by='Dept_Volume_Share_%', ascending=False)
        print(dept_data[['VENDOR_NAME', 'Dept_Volume_Share_%', 'EST_REVENUE']].head(10).to_string())

    # Load SKU Deep Dive to find specific winning items for "Baby Brands"
    print("\n--- SKU Winners for Baby Brands vs Hayat ---")
    df_sku = pd.read_excel(file_path, sheet_name='SKU Deep Dive')
    # Look for Baby Brands items
    baby_brands = df_sku[df_sku['VENDOR_NAME'].str.contains("BABY BRANDS", case=False, na=False)]
    print(baby_brands[['Department', 'ITM_NAME', 'TOTAL_10MO_SALES', 'TREND']].head(10).to_string())


except Exception as e:
    print(f"Error: {e}")
