import json
import os

# --- CONFIGURATION ---
DATA_DIR = r"C:\Users\iLink\.gemini\antigravity\scratch\app\data"
SALES_FORECAST_JSON = os.path.join(DATA_DIR, "sales_forecasting_2025 (1).json")
GRN_GLOB = os.path.join(DATA_DIR, "grnds_*.xlsx") # Not used for speed, relying on JSON

CATEGORIES = {
    "SANITARY TOWELS": ["SANITARY", "TOWEL", "PAD", "ALWAYS", "KOTEX", "MOLPED", "SOFY", "LIBRESSE"],
    "FABRIC CONDITIONER": ["FABRIC", "CONDITIONER", "SOFTENER", "DOWNY", "STA-SOFT", "COMFORT", "BINGO", "KLEANS"],
    "DIAPERS": ["DIAPER", "PAMPERS", "HUGGIES", "MOLFIX", "SOFTCARE", "BEBEM", "SNOOGGMS", "NAPPY"],
    "WIPES": ["WIPES", "WET", "BABY WIPES"]
}

HAYAT_KEYWORDS = ["HAYAT", "MOLFIX", "MOLPED", "BINGO", "PAPIA", "FAMILIA", "BEBEM"]

def normalize(text):
    if not text: return ""
    return str(text).upper().strip()

def classify_category(product_name):
    norm = normalize(product_name)
    # Exclusions
    if "SNAPPY" in norm: return None
    
    for cat, keywords in CATEGORIES.items():
        for k in keywords:
            if k in norm:
                return cat
    return None

def is_hayat(product_name):
    norm_prod = normalize(product_name)
    if any(k in norm_prod for k in HAYAT_KEYWORDS): return True
    return False

def main():
    if not os.path.exists(SALES_FORECAST_JSON):
        print(f"Error: Data file not found at {SALES_FORECAST_JSON}")
        return

    with open(SALES_FORECAST_JSON, 'r') as f:
        sales_forecast = json.load(f)

    # Bucket Data
    data_buckets = {cat: {"hayat": [], "competitors": []} for cat in CATEGORIES}

    for product_name, data in sales_forecast.items():
        cat = classify_category(product_name)
        if not cat: continue
        
        qty = data.get('total_10mo_sales', 0)
        # We assume price is roughly correlated or just compare volume "Beat"
        # Ideally we'd map supplier names again but for pure product comparison, name is enough.
        
        item = {"name": product_name, "qty": qty}
        
        if is_hayat(product_name):
            data_buckets[cat]["hayat"].append(item)
        else:
            data_buckets[cat]["competitors"].append(item)

    # Generate Markdown Table
    print("# Competitor Gap Analysis: Side-by-Side Breakdown\n")
    print("This analysis identifies specific competitor products that are outperforming Hayat's portfolio in each category.\n")

    for cat in CATEGORIES:
        print(f"## {cat}\n")
        
        h_items = sorted(data_buckets[cat]["hayat"], key=lambda x: x['qty'], reverse=True)
        c_items = sorted(data_buckets[cat]["competitors"], key=lambda x: x['qty'], reverse=True)
        
        # We want to show Top Competitors and identifying if Hayat has a direct rival
        # Let's show a Top 10 Comparison Table
        
        print("| Rank | Top Competitor Item | Units (10mo) | Top Hayat Item (Comparable) | Units (10mo) | Volume Gap |")
        print("|:--- |:--- |:--- |:--- |:--- |:--- |")
        
        limit = max(len(h_items), 10) # Show at least 10, or all Hayat if more
        limit = min(limit, 20) # Cap at 20 rows
        
        for i in range(limit):
            rank = i + 1
            
            # Competitor Data
            c_name = c_items[i]['name'] if i < len(c_items) else "-"
            c_qty = f"{c_items[i]['qty']:,}" if i < len(c_items) else "-"
            c_raw = c_items[i]['qty'] if i < len(c_items) else 0

            # Hayat Data
            h_name = h_items[i]['name'] if i < len(h_items) else "-"
            h_qty = f"{h_items[i]['qty']:,}" if i < len(h_items) else "-"
            h_raw = h_items[i]['qty'] if i < len(h_items) else 0
            
            gap = c_raw - h_raw
            gap_str = f"**-{gap:,}**" if gap > 0 else f"+{abs(gap):,}"
            if c_name == "-" and h_name == "-": continue

            print(f"| {rank} | {c_name} | {c_qty} | {h_name} | {h_qty} | {gap_str} |")
        
        print("\n")
        # Insight
        if c_items and h_items:
            ratio = c_items[0]['qty'] / max(1, h_items[0]['qty'])
            print(f"> **Insight:** The top competitor item sells **{ratio:.1f}x** more volume than the top Hayat item.\n")
        elif c_items and not h_items:
            print(f"> **Insight:** Hayat has **NO presence** in the top tier of this category.\n")

if __name__ == "__main__":
    main()
