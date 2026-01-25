import pandas as pd
import json
import os
import numpy as np

# Configuration
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data"
SCORECARD_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v2.csv"
STOCK_PROFILE_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\capital_allocation_report.xlsx"

def main():
    print("Reverse-Engineering Stock Allocation Algorithm...")
    
    if not os.path.exists(SCORECARD_PATH):
        print("Scorecard not found. Please run the allocation script first.")
        return

    # Load Scorecard (contains Sales, Lead Time, Dept, etc.)
    df = pd.read_csv(SCORECARD_PATH)
    
    # Load Physical Stock Data (we need actual CURRENT STOCK to see the algorithm's output)
    # I'll re-aggregate stock from the dept files to be sure
    dept_files = glob_files = [
        "dept_1_50.xlsx", "dept_51_100.xlsx", "dept_101_150.xlsx",
        "dept_151_200.xlsx", "dept_201_250.xlsx", "dept_301_350.xlsx"
    ]
    
    stock_list = []
    for f in dept_files:
        p = os.path.join(DATA_DIR, f)
        if os.path.exists(p):
            tmp = pd.read_excel(p)
            tmp.columns = [c.strip().upper() for c in tmp.columns]
            if 'ITM_NAME' in tmp.columns and 'STOCK' in tmp.columns:
                stock_list.append(tmp[['ITM_NAME', 'STOCK']])
    
    stock_df = pd.concat(stock_list).groupby('ITM_NAME')['STOCK'].sum().reset_index()
    stock_df['Product'] = stock_df['ITM_NAME'].astype(str).str.strip().str.upper()
    
    # Merge Scorecard with Actual Stock
    analysis_df = df.merge(stock_df[['Product', 'STOCK']], on='Product', how='inner')
    
    # Calculate Observed Coverage (Days of Stock)
    # Coverage = Stock / Avg_Daily_Sales
    # Handle zero sales to avoid infinity
    analysis_df['Observed_Coverage_Days'] = analysis_df.apply(
        lambda x: x['STOCK'] / x['Avg_Daily_Sales'] if x['Avg_Daily_Sales'] > 0 else 0, axis=1
    )
    
    # Analyze Correlations
    print("\n--- Correlation Analysis (Allocation Drivers) ---")
    
    # 1. Coverage vs Lead Time
    # If Algorithm is "Lead Time + Buffer", we should see a high correlation
    cor_lead = analysis_df[['Observed_Coverage_Days', 'Lead_Time_Days']].corr().iloc[0,1]
    print(f"Correlation (Coverage vs Lead Time): {cor_lead:.2f}")
    
    # 2. stock vs sales
    cor_sales = analysis_df[['STOCK', 'Avg_Daily_Sales']].corr().iloc[0,1]
    print(f"Correlation (Stock Level vs Sales Velocity): {cor_sales:.2f}")

    # 3. Coverage by ABC Class
    abc_coverage = analysis_df.groupby('ABC_Class')['Observed_Coverage_Days'].median()
    print("\nMedian Coverage Days by ABC Class:")
    print(abc_coverage)
    
    # 4. Coverage by Department (Top 10)
    dept_coverage = analysis_df.groupby('Department')['Observed_Coverage_Days'].median().sort_values(ascending=False).head(10)
    print("\nTop 10 Departments by Median Coverage (Stock Buffering):")
    print(dept_coverage)

    # 5. Reverse Engineering the Factor
    # If Stock = Sales * (LeadTime * X), find X
    analysis_df['Implicit_Multiplier'] = analysis_df.apply(
        lambda x: x['Observed_Coverage_Days'] / x['Lead_Time_Days'] if x['Lead_Time_Days'] > 0 else 0, axis=1
    )
    avg_multiplier = analysis_df[analysis_df['Avg_Daily_Sales'] > 0.1]['Implicit_Multiplier'].median()
    print(f"\nDetected Implicit Ordering Factor: {avg_multiplier:.2f}x Lead Time")

if __name__ == "__main__":
    main()
