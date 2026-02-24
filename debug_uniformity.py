
import logging
from oasis.logic.order_engine import OrderEngine

# Setup basic logging
logging.basicConfig(level=logging.INFO)

# Config
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
BUDGET = 200000 # Micro

# Mock Items based on User Report (Approximated from memory or typical values)
# "Brookside dairy best pouch" -> UHT/ESL, usually 500ml, High Sales
# "Ilara fresh pouch 500" -> Fresh, 500ml, Lower Sales (based on user saying Brookside has HIGHER sales)

# Note: Scaled Down ADS (assuming 0.003 factor for Micro)
# Mega ADS: Brookside ~1000 -> Micro ~3.0
# Mega ADS: Ilara ~800 -> Micro ~2.4

items = [
    {
        'product_name': "BROOKSIDE DAIRY BEST POUCH",
        'product_category': "FRESH MILK", # Often categorized here even if UHT/ESL
        'selling_price': 60.0,
        'avg_daily_sales': 3.0, # Scaled
        'is_fresh': False, # User says Long Life
        'pack_size': 12, # Standard crate
        'min_display_qty': 6, # Half crate?
        'estimated_delivery_days': 7, # Weekly
        'supplier_frequency': 'weekly'
    },
    {
        'product_name': "ILARA FRESH POUCH 500ML",
        'product_category': "FRESH MILK",
        'selling_price': 55.0,
        'avg_daily_sales': 1.5, # Lower
        'is_fresh': True, # Fresh
        'pack_size': 12,
        'min_display_qty': 6,
        'estimated_delivery_days': 1, # Daily
        'supplier_frequency': 'daily'
    }
]

engine = OrderEngine(DATA_DIR)

# MOCK SUPPLIER SPEND TO AVOID PRUNING
# We need to inject into engine.supplier_sales or similar?
# Or clearer: Just interpret the result before "Pass 3" if possible?
# engine.apply_greenfield_allocation runs all passes.
# Easiest way: Set Store Profile "min_order_value" to 0 via specific monkey patch or just make items expensive enough?
# Let's just make the items expensive enough to pass the $3000 threshold.
# 12 units * $60 = $720. Need 5 simulated items.
for i in range(5):
    items.append(items[0].copy())
    items[-1]['product_name'] = f"FILLER {i}"

print(f"Running Allocation for Budget ${BUDGET}...")
result = engine.apply_greenfield_allocation(items, BUDGET)

print("\nRESULTS:")
for r in result['recommendations']:
    name = r['product_name']
    if "FILLER" in name: continue
    
    qty = r['recommended_quantity']
    reason = r['reasoning']
    ads = r['avg_daily_sales']
    
    # Calculate implicit days
    days = qty / ads if ads > 0 else 0
    
    print(f"Item: {name:<30} | Qty: {qty} | ADS: {ads:.2f} | Days: {days:.1f} | Reason: {reason}")
    print(f"    Raw fields: PackSize={r.get('pack_size')}, Target={r.get('target_qty', 'N/A')}")
