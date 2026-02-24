"""
Recalculate Department Scaling Ratios for Allocation Engine

This script fixes GAP 4 & 5 from the allocation gap analysis by:
1. Loading the current department scaling ratios
2. Recalculating Capital_Weight based on Total_Value
3. Providing minimum weights for departments with no sales history
"""

import pandas as pd
import os

# Configuration
INPUT_FILE = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\department_scaling_ratios.csv"
OUTPUT_FILE = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\department_scaling_ratios.csv"
BACKUP_FILE = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\department_scaling_ratios_backup.csv"

# Minimum weight for departments with no sales history
MIN_WEIGHT_BASE = 0.0001  # 0.01% minimum

def recalculate_weights():
    print("=" * 60)
    print("Department Scaling Ratios Recalculation")
    print("=" * 60)
    
    # Load current data
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df)} departments")
    
    # Backup original
    df.to_csv(BACKUP_FILE, index=False)
    print(f"Backup saved to: {BACKUP_FILE}")
    
    # Calculate total value across all departments
    total_value = df['Total_Value'].sum()
    print(f"\nTotal Sales Value: ${total_value:,.2f}")
    
    # Count departments with zero value
    zero_value_depts = df[df['Total_Value'] == 0.0]['Department'].tolist()
    print(f"Departments with $0 sales: {len(zero_value_depts)}")
    
    # Departments with sales
    has_value = df[df['Total_Value'] > 0]
    print(f"Departments with sales: {len(has_value)}")
    
    # Recalculate weights
    # Strategy: 
    # 1. Departments with sales get proportional weight based on value
    # 2. Departments with no sales get minimum weight based on SKU count
    
    # Calculate total SKUs for zero-value departments (for proportional minimum)
    total_skus_zero = df[df['Total_Value'] == 0.0]['SKU_Count'].sum()
    
    # Reserve 5% of total budget for zero-value departments
    ZERO_VALUE_RESERVE = 0.05
    SALES_BASED_SHARE = 1.0 - ZERO_VALUE_RESERVE
    
    new_weights = []
    for _, row in df.iterrows():
        dept = row['Department']
        value = row['Total_Value']
        sku_count = row['SKU_Count']
        
        if value > 0:
            # Weight based on sales value (using 95% of total budget)
            weight = (value / total_value) * SALES_BASED_SHARE
        else:
            # Weight based on SKU count proportion (using 5% reserve)
            if total_skus_zero > 0:
                sku_share = sku_count / total_skus_zero
                weight = sku_share * ZERO_VALUE_RESERVE
            else:
                weight = MIN_WEIGHT_BASE
        
        new_weights.append(weight)
    
    df['Capital_Weight'] = new_weights
    
    # Verify weights sum to ~1.0
    total_weight = df['Capital_Weight'].sum()
    print(f"\nTotal Weight Sum: {total_weight:.6f} (should be ~1.0)")
    
    # Show top departments by new weight
    print("\n--- Top 10 Departments by New Weight ---")
    top_depts = df.nlargest(10, 'Capital_Weight')[['Department', 'Total_Value', 'Capital_Weight']]
    for _, row in top_depts.iterrows():
        print(f"  {row['Department']}: {row['Capital_Weight']:.4%} (${row['Total_Value']:,.0f})")
    
    # Show sample of previously zero-weight departments
    print("\n--- Sample Zero-Value Departments (now with weight) ---")
    zero_sample = df[df['Total_Value'] == 0.0].head(5)[['Department', 'SKU_Count', 'Capital_Weight']]
    for _, row in zero_sample.iterrows():
        print(f"  {row['Department']}: {row['Capital_Weight']:.4%} ({row['SKU_Count']} SKUs)")
    
    # Save updated file
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✓ Updated weights saved to: {OUTPUT_FILE}")
    
    # Statistics
    print("\n--- Summary Statistics ---")
    print(f"Departments with >1% weight: {len(df[df['Capital_Weight'] > 0.01])}")
    print(f"Departments with 0.1-1% weight: {len(df[(df['Capital_Weight'] >= 0.001) & (df['Capital_Weight'] <= 0.01)])}")
    print(f"Departments with <0.1% weight: {len(df[df['Capital_Weight'] < 0.001])}")

if __name__ == "__main__":
    recalculate_weights()
