"""
Supplier Analytics for Black Swan Simulation
=============================================
Provides category-level supplier concentration analysis:
- Top 10 suppliers per major department
- Revenue potential calculation  
- SKU concentration metrics
- Black swan failure impact preview

Author: OASIS Team
Created: 2026-02-06
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from pathlib import Path
import json
import os

# Determine paths relative to this script
# Layout: scratch/oasis/analytics/supplier_analytics.py
# Scorecard is in: scratch/
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent.parent  # scratch/
DATA_DIR = PROJECT_ROOT / "oasis" / "data"

def find_latest_scorecard():
    # Search for latest scorecard in project root
    candidates = list(PROJECT_ROOT.glob("Full_Product_Allocation_Scorecard_v*.csv"))
    if not candidates:
        # Fallback
        return PROJECT_ROOT / "Full_Product_Allocation_Scorecard_v7.csv"
    
    # Sort by version number
    def get_version(p):
        try:
            return int(p.stem.split('_v')[-1])
        except:
            return 0
            
    latest = max(candidates, key=get_version)
    return latest

SCORECARD_FILE = find_latest_scorecard()
SUPPLIER_MAP_FILE = DATA_DIR / "product_supplier_map.json"


# === Data Classes ===

@dataclass
class SupplierStats:
    """Statistics for a single supplier within a category."""
    supplier_name: str
    sku_count: int
    revenue_potential: float
    share_pct: float
    risk_score: float  # Composite: 0.7 * revenue_rank + 0.3 * sku_rank
    avg_margin_pct: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            'supplier': self.supplier_name,
            'sku_count': self.sku_count,
            'revenue_potential': self.revenue_potential,
            'share_pct': round(self.share_pct, 1),
            'risk_score': round(self.risk_score, 2),
            'avg_margin': round(self.avg_margin_pct, 1)
        }


@dataclass
class SupplierConcentrationReport:
    """Holds supplier concentration data for a department."""
    department: str
    suppliers: List[SupplierStats] = field(default_factory=list)
    hhi_score: float = 0.0  # Herfindahl-Hirschman Index
    top_supplier_share: float = 0.0  # % of department revenue
    total_revenue: float = 0.0
    total_skus: int = 0


# === Major Categories for Dropdown ===

MAJOR_CATEGORIES = [
    "FRESH MILK",
    "BREAD", 
    "COOKING OIL",
    "BEER",
    "SOFT DRINKS",
    "YOGHURT",
    "CHEESE",
    "HOUSEHOLD",
    "COSMETICS",
    "BABY ITEMS",
    "RICE & PASTA",
    "BISCUITS",
    "CHOCOLATES",
    "TEA",
    "COFFEE"
]


def get_major_categories() -> List[str]:
    """Returns list of major retail categories for dropdown."""
    return MAJOR_CATEGORIES.copy()


def load_scorecard_data() -> pd.DataFrame:
    """Load the product allocation scorecard with supplier info."""
    if not SCORECARD_FILE.exists():
        raise FileNotFoundError(f"Scorecard not found: {SCORECARD_FILE}")
    
    df = pd.read_csv(SCORECARD_FILE)
    
    # Ensure required columns exist
    required = ['Product', 'Department', 'Supplier', 'Unit_Price', 'Score_Weighted']
    # Fall back to Avg_Daily_Sales if Score_Weighted not present
    if 'Score_Weighted' not in df.columns and 'Avg_Daily_Sales' in df.columns:
        df['Score_Weighted'] = df['Avg_Daily_Sales']  # Use ADS as proxy
        required = ['Product', 'Department', 'Supplier', 'Unit_Price', 'Avg_Daily_Sales']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in scorecard: {missing}")
    
    return df


def load_supplier_map() -> Dict[str, str]:
    """Load the product-to-supplier mapping."""
    if SUPPLIER_MAP_FILE.exists():
        with open(SUPPLIER_MAP_FILE, 'r') as f:
            return json.load(f)
    return {}


def calculate_hhi(share_percentages: List[float]) -> float:
    """
    Calculate Herfindahl-Hirschman Index (HHI) for concentration.
    
    HHI = Sum of (Market Share %)^2
    Range: 0 (Perfect Diversity) to 10,000 (Monopoly)
    
    Interpretation:
    - < 1500: Unconcentrated
    - 1500-2500: Moderately concentrated  
    - > 2500: Highly concentrated
    """
    return sum(s ** 2 for s in share_percentages)


def get_top_suppliers_by_department(
    df: pd.DataFrame = None,
    department: str = None,
    top_n: int = 10
) -> List[SupplierStats]:
    """
    Returns top N critical suppliers for a department ranked by risk score.
    
    Risk Score = 0.7 * (revenue_rank / total) + 0.3 * (sku_rank / total)
    Higher score = more critical supplier
    
    Args:
        df: DataFrame with scorecard data (loads if None)
        department: Department to analyze
        top_n: Number of top suppliers to return
        
    Returns:
        List of SupplierStats sorted by risk score descending
    """
    if df is None:
        df = load_scorecard_data()
    
    # Filter to department
    dept_df = df[df['Department'].str.upper() == department.upper()].copy()
    
    if dept_df.empty:
        return []
    
    # Ensure Supplier column is clean
    dept_df['Supplier'] = dept_df['Supplier'].fillna('Unknown').astype(str)
    
    # Calculate revenue potential per item (price * weighted score as proxy for velocity)
    if 'Score_Weighted' in dept_df.columns:
        dept_df['Revenue_Potential'] = dept_df['Unit_Price'] * dept_df['Score_Weighted']
    else:
        dept_df['Revenue_Potential'] = dept_df['Unit_Price'] * dept_df['Avg_Daily_Sales']
    
    # Get margin if available
    if 'Margin_Pct' in dept_df.columns:
        margin_col = 'Margin_Pct'
    else:
        margin_col = None
    
    # Group by supplier
    supplier_stats = dept_df.groupby('Supplier').agg({
        'Product': 'count',  # SKU count
        'Revenue_Potential': 'sum',
        'Unit_Price': 'mean'
    }).reset_index()
    
    supplier_stats.columns = ['supplier', 'sku_count', 'revenue_potential', 'avg_price']
    
    # Calculate margin average if available
    if margin_col:
        margin_avg = dept_df.groupby('Supplier')[margin_col].mean().reset_index()
        margin_avg.columns = ['supplier', 'avg_margin']
        supplier_stats = supplier_stats.merge(margin_avg, on='supplier', how='left')
    else:
        supplier_stats['avg_margin'] = 0.0
    
    # Calculate share percentages
    total_revenue = supplier_stats['revenue_potential'].sum()
    total_skus = supplier_stats['sku_count'].sum()
    
    supplier_stats['share_pct'] = (supplier_stats['revenue_potential'] / total_revenue * 100)
    
    # Calculate risk scores (higher = more critical)
    # Rank by revenue (descending) and sku count (descending)
    supplier_stats['revenue_rank'] = supplier_stats['revenue_potential'].rank(ascending=False)
    supplier_stats['sku_rank'] = supplier_stats['sku_count'].rank(ascending=False)
    
    n_suppliers = len(supplier_stats)
    
    # Risk score: invert ranks so highest revenue/SKU = highest score
    supplier_stats['risk_score'] = (
        0.7 * (1 - (supplier_stats['revenue_rank'] - 1) / max(n_suppliers, 1)) +
        0.3 * (1 - (supplier_stats['sku_rank'] - 1) / max(n_suppliers, 1))
    )
    
    # Sort by risk score
    supplier_stats = supplier_stats.sort_values('risk_score', ascending=False).head(top_n)
    
    # Convert to SupplierStats objects
    result = []
    for _, row in supplier_stats.iterrows():
        result.append(SupplierStats(
            supplier_name=row['supplier'],
            sku_count=int(row['sku_count']),
            revenue_potential=row['revenue_potential'],
            share_pct=row['share_pct'],
            risk_score=row['risk_score'],
            avg_margin_pct=row['avg_margin']
        ))
    
    return result


def generate_supplier_risk_report(
    df: pd.DataFrame = None,
    departments: List[str] = None
) -> Dict[str, SupplierConcentrationReport]:
    """
    Generates full risk report across specified categories.
    
    Args:
        df: DataFrame with scorecard data
        departments: List of departments to analyze (defaults to MAJOR_CATEGORIES)
        
    Returns:
        Dict mapping department name to SupplierConcentrationReport
    """
    if df is None:
        df = load_scorecard_data()
    
    if departments is None:
        departments = MAJOR_CATEGORIES
    
    reports = {}
    
    for dept in departments:
        top_suppliers = get_top_suppliers_by_department(df, dept, top_n=15)
        
        if not top_suppliers:
            continue
        
        # Calculate HHI
        share_pcts = [s.share_pct for s in top_suppliers]
        hhi = calculate_hhi(share_pcts)
        
        # Top supplier share
        top_share = top_suppliers[0].share_pct if top_suppliers else 0.0
        
        # Total metrics
        total_rev = sum(s.revenue_potential for s in top_suppliers)
        total_skus = sum(s.sku_count for s in top_suppliers)
        
        reports[dept] = SupplierConcentrationReport(
            department=dept,
            suppliers=top_suppliers[:10],  # Keep top 10 for report
            hhi_score=hhi,
            top_supplier_share=top_share,
            total_revenue=total_rev,
            total_skus=total_skus
        )
    
    return reports


def analyze_supplier_failure_impact(
    df: pd.DataFrame,
    supplier_name: str,
    department: str = None
) -> Dict[str, Any]:
    """
    Pre-simulation analysis: What happens if this supplier fails?
    
    Args:
        df: Scorecard DataFrame
        supplier_name: Supplier to simulate failure for
        department: Optional - restrict to specific department
        
    Returns:
        {
            'affected_skus': List of product names,
            'affected_sku_count': int,
            'revenue_at_risk': float,
            'coverage_loss_pct': float,
            'departments_affected': List of department names,
            'substitute_availability': float (0-1),
            'estimated_stockout_severity': str ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')
        }
    """
    # Filter by supplier
    supplier_filter = df['Supplier'].str.upper().str.strip() == supplier_name.upper().strip()
    
    if department:
        dept_filter = df['Department'].str.upper() == department.upper()
        affected = df[supplier_filter & dept_filter]
        baseline = df[dept_filter]
    else:
        affected = df[supplier_filter]
        baseline = df
    
    if affected.empty:
        return {
            'affected_skus': [],
            'affected_sku_count': 0,
            'revenue_at_risk': 0,
            'coverage_loss_pct': 0,
            'departments_affected': [],
            'substitute_availability': 1.0,
            'estimated_stockout_severity': 'NONE'
        }
    
    # Calculate impact
    affected_skus = affected['Product'].tolist()
    affected_count = len(affected_skus)
    
    # Revenue at risk (using price * ADS as proxy)
    affected = affected.copy()
    price_col = 'Unit_Price'
    velocity_col = 'Avg_Daily_Sales' if 'Avg_Daily_Sales' in affected.columns else 'Score_Weighted'
    affected['rev_proxy'] = affected[price_col] * affected[velocity_col]
    
    baseline_copy = baseline.copy()
    velocity_col_base = 'Avg_Daily_Sales' if 'Avg_Daily_Sales' in baseline_copy.columns else 'Score_Weighted'
    baseline_copy['rev_proxy'] = baseline_copy[price_col] * baseline_copy[velocity_col_base]
    
    revenue_at_risk = affected['rev_proxy'].sum()
    total_revenue = baseline_copy['rev_proxy'].sum()
    
    coverage_loss = (revenue_at_risk / total_revenue * 100) if total_revenue > 0 else 0
    
    # Departments affected
    depts_affected = affected['Department'].unique().tolist()
    
    # Estimate substitute availability
    # Check how many other suppliers exist for the same departments
    dept_suppliers = {}
    for dept in depts_affected:
        dept_data = df[df['Department'] == dept]
        other_suppliers = dept_data[dept_data['Supplier'].str.upper().str.strip() != supplier_name.upper().strip()]
        dept_suppliers[dept] = len(other_suppliers['Supplier'].unique())
    
    avg_alternatives = np.mean(list(dept_suppliers.values())) if dept_suppliers else 0
    substitute_availability = min(1.0, avg_alternatives / 5)  # Normalize: 5+ suppliers = full substitution
    
    # Severity rating
    if coverage_loss >= 30:
        severity = 'CRITICAL'
    elif coverage_loss >= 15:
        severity = 'HIGH'
    elif coverage_loss >= 5:
        severity = 'MEDIUM'
    else:
        severity = 'LOW'
    
    return {
        'affected_skus': affected_skus[:20],  # Limit list size
        'affected_sku_count': affected_count,
        'revenue_at_risk': revenue_at_risk,
        'coverage_loss_pct': coverage_loss,
        'departments_affected': depts_affected,
        'substitute_availability': substitute_availability,
        'estimated_stockout_severity': severity
    }


def print_supplier_dropdown(department: str, top_n: int = 10):
    """
    Pretty-print top suppliers for a department.
    Used in CLI interactive mode.
    """
    suppliers = get_top_suppliers_by_department(department=department, top_n=top_n)
    
    print(f"\n{'='*70}")
    print(f"Top {len(suppliers)} Suppliers in {department}")
    print(f"{'='*70}")
    print(f"{'#':>3} | {'Supplier':<40} | {'Share %':>8} | {'SKUs':>5}")
    print(f"{'-'*70}")
    
    for i, sup in enumerate(suppliers, 1):
        print(f"{i:>3} | {sup.supplier_name[:40]:<40} | {sup.share_pct:>7.1f}% | {sup.sku_count:>5}")
    
    print(f"{'='*70}\n")
    return suppliers


# === Main for testing ===
if __name__ == "__main__":
    print("Loading scorecard data...")
    df = load_scorecard_data()
    print(f"Loaded {len(df)} products\n")
    
    # Test: Get top 10 for FRESH MILK
    print_supplier_dropdown("FRESH MILK", 10)
    
    # Test: Analyze BROOKSIDE failure
    print("\n--- Impact Analysis: BROOKSIDE Failure ---")
    impact = analyze_supplier_failure_impact(df, "BROOKSIDE DAIRY LIMITED", "FRESH MILK")
    print(f"Revenue at Risk: KES {impact['revenue_at_risk']:,.0f}")
    print(f"Coverage Loss: {impact['coverage_loss_pct']:.1f}%")
    print(f"Severity: {impact['estimated_stockout_severity']}")
    print(f"Affected SKUs: {impact['affected_sku_count']}")
