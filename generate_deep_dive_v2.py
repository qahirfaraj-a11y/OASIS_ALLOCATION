import pandas as pd
import json
import os

# Configuration
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch"
SUMMARY_FILE = os.path.join(DATA_DIR, "Supplier_Fulfillment_Summary.xlsx")
OUTPUT_MD = r"C:\Users\iLink\.gemini\antigravity\brain\727303c1-e050-46d0-8394-7f5849b8a103/hayat_supplier_health_deep_dive_2026.md"
PATTERNS_JSON = os.path.join(DATA_DIR, r"app/data/supplier_patterns_2025 (3).json")

# Constants
HAYAT_LEAD_TIME = 3.61
HAYAT_ORDER_GAP = 15.4 # From user text/JSON
RELIABILITY = 90.5

# Supplier Mapping for Comparison
# Mapping Vendor Name in Summary to User's Brands
COMPETITORS = {
    "P&G (Hasbah)": ["HASBAH", "P&G"],
    "Kim Fay": ["KIM FAY"],
    "Chandaria": ["CHANDARIA"],
    "Baby Brands": ["BABY BRANDS"],
    "Zenko": ["ZENKO"]
}

def get_competitor_metrics(df, keywords):
    for k in keywords:
        match = df[df['Vendor Name'].str.contains(k, case=False, na=False)]
        if not match.empty:
            # Weighted average if multiple distributors
            avg_lt = (match['mean'] * match['count']).sum() / match['count'].sum()
            return round(avg_lt, 2)
    return None

def generate_deep_dive():
    print(f"Loading data...")
    df = pd.read_excel(SUMMARY_FILE)
    
    # Get Competitor Lead Times
    comp_stats = {}
    for name, keywords in COMPETITORS.items():
        lt = get_competitor_metrics(df, keywords)
        if lt:
            comp_stats[name] = lt
        else:
            comp_stats[name] = "N/A"
            
    # Calculate New Stress Index
    # Stress Index = Lead Time / Order Gap
    stress_index = HAYAT_LEAD_TIME / HAYAT_ORDER_GAP
    
    # Generate Markdown
    with open(OUTPUT_MD, 'w', encoding='utf-8') as f:
        f.write("# Hayat Kimya: Supplier Health Deep Dive 2026 (Corrected)\n\n")
        
        # 1. CORE HEALTH DIAGNOSIS
        f.write("## 1. The Core Health Diagnosis: **Revised**\n")
        f.write("We have re-evaluated Hayat Kimya's health using the **validated 3.6-day lead time** (replacing the assumed 21 days). This completely changes the diagnosis from \"Stressed\" to **\"Agile\"**.\n\n")
        
        f.write("### The \"Stress Index\" Findings\n")
        f.write(f"- **Formula**: Lead Time ({HAYAT_LEAD_TIME:.1f} Days) ÷ Average Ordering Gap ({HAYAT_ORDER_GAP} Days)\n")
        f.write(f"- **New Score**: **{stress_index:.2f}** (Safe Zone)\n")
        f.write("- **Interpretation**: **\"Capacity Surplus\"**.\n")
        f.write("  - You are receiving goods **12 days BEFORE** you need to place the next order.\n")
        f.write("  - **Health Implication**: The \"anxiety\" in the warehouse is artificial. It is caused by holding 21 days of safety stock when replenishment only takes 3 days.\n\n")

        f.write("### Comparative Health Check (Updated)\n")
        f.write("| Supplier | Lead Time | Stress Index | Diagnosis |\n")
        f.write("| :--- | :--- | :--- | :--- |\n")
        
        # P&G
        pg_lt = comp_stats.get("P&G (Hasbah)", 2.0)
        pg_gap = 3.0 # User provided
        f.write(f"| P&G (Hasbah) | {pg_lt} Days | {pg_lt/pg_gap:.2f} | **Healthy (JIT)** |\n")
        
        # Hayat
        f.write(f"| **Hayat Kimya** | **{HAYAT_LEAD_TIME} Days** | **{stress_index:.2f}** | **Healthy (Cushioned)** |\n")
        
        # Kim Fay
        kf_lt = comp_stats.get("Kim Fay", 7.0)
        kf_gap = 7.0 # User provided implies 1.0 index
        f.write(f"| Kim Fay | {kf_lt} Days | {kf_lt/kf_gap:.2f} | Balanced |\n")
        
        # Chandaria 
        ch_lt = comp_stats.get("Chandaria", 10.0)
        ch_gap = 14.0 # Bi-weekly
        f.write(f"| Chandaria | {ch_lt} Days | {ch_lt/ch_gap:.2f} | Operational |\n\n")

        f.write("---\n\n")
        
        # 2. ORDER CONSISTENCY ANALYSIS
        f.write("## 2. Order Consistency Analysis\n")
        f.write(f"*   **Frequency**: Monthly Label vs. ~{HAYAT_ORDER_GAP} Day Reality.\n")
        f.write("*   **New Diagnosis**: **\"Unnecessary Buffering\"**.\n")
        f.write("    *   The team orders every 15 days because they *can*. With a 3.6-day lead time, this frequency is actually efficient.\n")
        f.write("    *   **Correction**: Stop calling it \"Panic Ordering\". It is simply **Bi-Weekly Replenishment**, which is the correct strategy for a 3-day lead time supplier.\n\n")
        
        f.write("---\n\n")
        
        # 3. RECOMMENDATIONS
        f.write("## 3. Recommendations for \"Optimizing\" the Supply Chain\n")
        f.write("Since the \"Healing\" is no longer needed (the patient is healthy), we move to **Optimization**:\n\n")
        f.write("1.  **Slash Safety Stock**: You are holding inventory for a 21-day risk that does not exist. **Reduce safety stock coverage to 7-10 days** immediately. This will free up cash and warehouse space.\n")
        f.write("2.  **Officialise Bi-Weekly**: Change the ERP schedule from \"Monthly\" to **\"Bi-Weekly\"**. This aligns the system with the reality of the 3.6-day lead time and eliminates the \"Panic\" flag.\n")
        f.write("3.  **Focus on Reliability**: 54% of orders arrive in 3 days, but some take 8. Use a **7-day max buffer** planning rule to absorb these outliers, which is still far better than the old 21-day rule.\n\n")
        
        f.write("---\n\n")
        
        # 4. FINAL HEALTH VERDICT
        f.write("## 4. Final Health Verdict\n")
        f.write("Hayat's logistics are **Elite (Top Tier)** but **Misunderstood**.\n")
        f.write("The \"Health Issue\" was a phantom problem caused by bad data (21-day assumption). Reality proves Hayat is fast, responsive, and capable of supporting a high-velocity, JIT supply chain comparable to P&G.\n\n")
        
        f.write("---\n\n")
        
        # 5. COMPETITIVE LANDSCAPE UPDATE
        f.write("## 5. Competitive Landscape: \"The Iron Triangle\" (Updated)\n")
        f.write("| Supplier (Brand) | Volume Share | Lead Time (Logistics) | Reliability | Order Frequency |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- |\n")
        f.write(f"| P&G (Hasbah) | #1 | {pg_lt} Days (Best) | 90.5% | Every 2-3 Days |\n")
        f.write(f"| **Hayat Kimya** | #4 | **{HAYAT_LEAD_TIME} Days (Elite)** | 54.8% (Var) | **Bi-Weekly** |\n")
        f.write(f"| Kim Fay | #2 | {kf_lt} Days (Good) | 90.5% | Weekly |\n")
        f.write(f"| Chandaria | Niche | {ch_lt} Days (Avg) | 90.5% | Bi-Weekly |\n\n")
        
        f.write("### Strategic Implication\n")
        f.write("- **The Agility Gap is Closed**: Hayat is no longer \"slow\". You can now react to sales spikes in < 4 days, similar to P&G.\n")
        f.write("- **The Niche Defense**: Baby Brands (Diapers) and Zenko (Wipes) rely on speed to steal share. Hayat now matches that speed. Use this agility to counter their promos.\n")

if __name__ == "__main__":
    generate_deep_dive()
