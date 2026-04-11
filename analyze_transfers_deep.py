import pandas as pd
import os

file_in = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trn_1_12.xlsx"
file_out = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trout_1_12.xlsx"

def analyze_transfers(path, name):
    print(f"\n=== Detailed Analysis: {name} ===")
    if not os.path.exists(path):
        print("File not found.")
        return
    
    df = pd.read_excel(path)
    
    # Precise Mapping for these specific files
    if "TRN" in name:
        from_col = "From Org Code/ Name"
        to_col = "To Org Code/ Name"
        qty_col = "STO Qty"
        item_col = "Item Name"
        date_col = "STI Date"
        reason_col = None
    else:
        from_col = "From Org Code/ Name"
        to_col = "To Org Code/ Name"
        qty_col = "STO Qty"
        item_col = "Item Name"
        date_col = "STO Date"
        reason_col = "Remarks"

    # Data Cleaning
    df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
    df = df[df[qty_col] > 0] # Only look at actual movements
    
    # Filter out 'Total' rows
    df = df[~df[from_col].astype(str).str.contains('total', case=False, na=False)]
    
    print(f"\nTotal Records: {len(df)}")
    print(f"Total Units: {df[qty_col].sum():.2f}")
    
    print("\nTop 5 Donor Stores (by Volume):")
    print(df.groupby(from_col)[qty_col].sum().sort_values(ascending=False).head(5))
    
    print("\nTop 5 Recipient Stores (by Volume):")
    print(df.groupby(to_col)[qty_col].sum().sort_values(ascending=False).head(5))
    
    print("\nTop 10 Transferred Items:")
    print(df.groupby(item_col)[qty_col].sum().sort_values(ascending=False).head(10))

    if reason_col:
        print("\nPrimary Transfer Reasons:")
        print(df[reason_col].value_counts().head(10))

    if date_col:
        try:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            print("\nMonthly Trend (Units):")
            print(df.groupby(df[date_col].dt.to_period('M'))[qty_col].sum())
        except:
            pass

analyze_transfers(file_in, "TRN")
analyze_transfers(file_out, "TROUT")
