import json
import pandas as pd
import numpy as np
import os
import difflib

# --- Configuration ---
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
OUTPUT_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"

FILES = {
    "forecast": "sales_forecasting_2025 (1).json",
    "profitability": "sales_profitability_intelligence_2025_updated.json",
    "suppliers": "supplier_patterns_2025 (3).json",
    "supplier_map": "product_supplier_map.json",
    "grn_map": "product_supplier_map_grn.json",
    "department_map": "product_department_map.json",
    "staples": "staple_products.json",
    "ratios": "department_scaling_ratios.csv",
    "grn_freq": "sku_grn_frequency.json",
    "grn_freq": "sku_grn_frequency.json",
    "sup_share": "supplier_dept_ratios.json",
    "synthetic_proxy": "synthetic_core_profile.json"
}

EXCLUDED_SUPPLIERS = [
    "BIGCOLD KENYA SIMPLIFINE BAKERY NO GRN",
    "CARD GROUP EAST AFRICA LTD NO GRN",
    "CORNER SHOP PREPACK NO GRN",
    "HIDDEN TREASURES BOOK W S LIMITED NO GRN",
    "KENCHIC BUTCHERY NO GRN",
    "ROYAL BLOOMS S ENTERPRISES NO GRN",
    "NATION MEDIA GROUP LTD ACC 2 NO GRN",
    "SELECTIONS WORLD LIMITED NO GRN",
    "STENTOR ENTERPRISES LIMITED NO GRN",
    "SUNPOWER PRODUCTS LTD DELI NO GRN",
    "THE CORNER SHOP LIMITED NO GRN",
    "THE STANDARD GROUP PLC NO GRN",
    "THE STAR PUBLICATIONS LTD NO GRN"
]

# Scenarios & Strategy Framework
SCENARIO_CONFIGS = {
    "Small_200k": {
        "target_budget": 200000,
        "multiplier": 0.4,
        "coverage": 14,
        "grn_threshold": 0.50,      # WAS 0.85: Lowered to 50% to include Core items (Variety)
        "staple_dept_share": 0.60,  # WAS 0.80: Lowered to 60% to force Room for Discretionary
        "depth_cap": 5,             # WAS 7: Reduced depth to conserve budget for width
        "max_packs": 3,             # NEW: Hard Cap (3 packs max) to prevent hoarding
        "price_ceiling_pct": 0.02   # 2% of Dept Wallet
    },
    "Med_2.5M": {
        "target_budget": 2500000,
        "multiplier": 0.8,
        "coverage": 21,
        "grn_threshold": 0.65,
        "staple_dept_share": 0.60,
        "depth_cap": 14,
        "price_ceiling_pct": 0.05
    },
    "Large_10M": {
        "target_budget": 10000000,
        "multiplier": 1.2,
        "coverage": 30,
        "grn_threshold": 0.30,
        "staple_dept_share": 0.50,
        "depth_cap": 30,
        "price_ceiling_pct": 1.0     # Effectively no ceiling
    },
    "Mega_115M": {
        "target_budget": 115000000,
        "multiplier": 1.5,
        "coverage": 45,
        "grn_threshold": 0.0,
        "staple_dept_share": 0.40,
        "depth_cap": 45,
        "price_ceiling_pct": 1.0
    }
}

STAPLE_DEPARTMENTS = [
    "FRESH MILK", "BREAD", "FLOUR", "COOKING OIL", "SUGAR", "EGGS", 
    "MINERAL WATER", "SODA", "TOILET ROLL", "RICE", "TISSUE PAPER", 
    "SALT", "BREAKFAST CEREALS", "YOGHURT", "BUTTER"
]

FRESH_DEPARTMENTS = [
    "FRESH MILK", "BREAD", "POULTRY", "MEAT", "VEGETABLES", "FRUITS", 
    "BAKERY FOODPLUS", "DELICATESSEN", "PASTRY", "EGGS",
    "YOGHURT", "CHEESE", "BUTTER"
]

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"Warning: File not found: {path}")
        return {}
    with open(path, 'r') as f:
        return json.load(f)

def determine_department(product_name, supplier_name, lead_time_days, department_map):
    """
    Determines department based on:
    1. Direct Map (Comprehensive Department Files)
    2. Supplier-based mapping
    3. Keyword heuristics
    4. Lead-time heuristics (Daily = Fresh)
    """
    name_upper = str(product_name).upper().strip()
    sup_upper = str(supplier_name).upper().strip()
    
    # 1. Direct Map
    if name_upper in department_map:
        return department_map[name_upper]
    
    # Attempt to find item name without leading codes
    parts = name_upper.split(None, 1)
    if len(parts) > 1:
        reduced_name = parts[1].strip()
        if reduced_name in department_map:
            return department_map[reduced_name]

    # 2. Supplier-based Heuristics
    supplier_dept_map = {
        "COCA COLA": "SODA",
        "BROOKSIDE": "FRESH MILK",
        "UNILEVER": "HOUSEHOLD ITEMS",
        "PROCTER & GAMBLE": "HOUSEHOLD ITEMS",
        "KCC": "FRESH MILK",
        "EABL": "BEER",
        "NESTLE": "BABY FOODS",
        "BIDCO": "COOKING OIL",
        "PZ CUSSONS": "BATH SOAP",
        "KAPA OIL": "COOKING OIL",
        "PEPSI": "SODA",
        "KWAL": "WINES",
        "LOREAL": "COSMETICS",
        "GLAXOSMITHKLINE": "MEDICARE",
        "MARS": "CHOCOLATES",
        "WRIGLEY": "CHEWING GUM",
        "CADBURY": "CHOCOLATES",
        "FERRERO": "CHOCOLATES",
        "PAMPERS": "DIAPERS",
        "HUGGIES": "DIAPERS",
        "KOTEX": "SANITARY TOWELS",
        "ALWAYS": "SANITARY TOWELS",
        "KIM-FAY": "TISSUE PAPER",
        "CHANDARIA": "TISSUE PAPER",
        "DORMANS": "COFFEE",
        "KETEPA": "TEA"
    }
    
    for sup_keyword, dept in supplier_dept_map.items():
        if sup_keyword in sup_upper:
            return dept

    # 3. Comprehensive Keyword Heuristics
    keywords = {
        "WINES": ["WINE", "MERLOT", "CABERNET", "SAUVIGNON", "SHIRAZ", "ROSE", "SWEET RED", "CHARDONNAY"],
        "SPIRITS": ["WHISKY", "WHISKEY", "VODKA", "GIN", "RUM", "BRANDY", "COGNAC", "LIQUEUR", "TEQUILA"],
        "BEER": ["BEER", "LAGER", "CIDER", "PILSENER", "TUSKER", "GUINNESS", "HEINEKEN"],
        "SODA": ["SODA", "COKE", "PEPSI", "FANTA", "SPRITE", "STONEY", "KREST", "SCHWEPPES"],
        "TETRA PACK JUICE": ["JUICE", "CERES", "PICK N PAY", "DEL MONTE", "FRUTTA", "ORCHARD"],
        "MINERAL WATER": ["WATER", "AQUAMIST", "DASANI", "KERINGET", "QUENCHER"],
        "BISCUITS": ["BISCUIT", "COOKIE", "WAFER", "BRITANNIA", "NUCHOI", "MANJI", "BAKERS"],
        "CHOCOLATES": ["CHOCOLATE", "DAIRY MILK", "KIT KAT", "SNICKERS", "MARS", "BOUNTY", "TWIX"],
        "SWEETS": ["SWEET", "CANDY", "GUMMY", "LOLLIPOP", "MENTOS", "SKITTLES"],
        "COOKING OIL": ["OIL", "SUNFLOWER OIL", "VEGETABLE OIL", "ELIANTRO", "GOLDEN FRY", "FRESH FRI"],
        "COOKING FAT": ["FAT", "KASUKU", "KIMPBO"],
        "FLOUR": ["FLOUR", "ATTA", "UNGA", "MAIZE", "WHEAT"],
        "RICE": ["RICE", "BASMATI", "BIRYANI", "PISHORI"],
        "SUGAR": ["SUGAR", "MUMIAS", "KABRAS"],
        "SALT": ["SALT", "KENSALT"],
        "SPICES": ["SPICE", "PILAU MASALA", "CURRY POWDER", "BLACK PEPPER", "CINNAMON", "GINGER", "GARLIC", "SULTANA"],
        "PASTA": ["PASTA", "SPAGHETTI", "MACARONI", "FUSILLI", "PENNE", "SANTA MARIA"],
        "BREAKFAST CEREALS": ["CEREAL", "OATS", "WEETABIX", "CORNFLAKES", "MUESLI", "GRANOLA"],
        "SNACKS": ["CHIA SNACK", "SNACK BAR", "CHEVDA", "CRISPS"],
        "TEA": ["TEA", "KETEPA", "FAHARI"],
        "COFFEE": ["COFFEE", "NESCAFE", "DORMANS"],
        "DETERGENTS": ["DETERGENT", "ARIEL", "OMO", "PERSIL", "SUNLIGHT POWDER"],
        "BAR SOAP": ["BAR SOAP", "WHITE STAR", "JAMA"],
        "BATH SOAP": ["SOAP", "DETTOL", "LIFEBUOY", "LUX", "GEISHA"],
        "SHAMPOOS/CONDITIONER": ["SHAMPOO", "CONDITIONER", "PANTENE", "HEAD & SHOULDERS"],
        "TOOTHPASTES": ["TOOTHPASTE", "COLGATE", "AQUAFRESH", "CLOSE UP"],
        "DIAPERS": ["DIAPER", "PANTS", "HUGGIES", "PAMPERS", "MOLFIX"],
        "SANITARY TOWELS": ["SANITARY", "PADS", "ALWAYS", "KOTEX", "TENA LADY"],
        "WIPES": ["WIPES"],
        "TISSUE PAPER": ["TISSUE", "TOILET PAPER", "SERVIETTE", "VIVA", "HANABIE"],
        "DISH WASHING PASTE": ["DISH WASH", "AXION", "SUNLIGHT LIQUID"],
        "ALL CLEANERS": ["CLEANER", "HARPIC", "JIK", "DETTOL LIQUID", "BUCKET", "WRINGER"],
        "PET DOG FOOD": ["DOG FOOD", "PEDIGREE", "REFLEX"],
        "PET CAT FOOD": ["CAT FOOD", "WHISKAS"],
        "PET ASSESORIES": ["BRUSH", "LEASH", "PET", "CRUFTS"],
        "STATIONARIES": ["PEN", "PENCIL", "MARKER", "STENCIL", "ZEBRA", "YOSOGO", "NOTEBOOK"],
        "YOGHURT": ["YOGHURT", "BIO", "YURT", "YOGURT"],
        "FRESH MILK": ["MILK", "LALA", "BROOKSIDE", "KCC"],
        "BUTTER": ["BUTTER", "MARGARINE", "BLUE BAND"],
        "HOUSEHOLD ITEMS": ["BATTERY", "CANDLE", "MATCH BOX", "FOIL", "CLING FILM", "CUTLERY", "SPOON", "FORK", "KNIFE", "POT", "PAN", "COOKING", "KITCHEN", "STRAW", "SHAKER", "BOWL", "CLEAR LID"],
        "WOMEN/UNISEX LOTION": ["LOTION", "REPAIR LOTION", "DRY SKIN", "YVES ROCHER"]
    }

    for dept, keys in keywords.items():
        if any(k in name_upper for k in keys):
            return dept

    # 4. Lead-time heuristics (Daily = Fresh)
    if lead_time_days <= 1.5:
        if any(x in name_upper for x in ["MILK", "YOGHURT", "GHEE", "BUTTER", "DAIRY"]):
             return "FRESH MILK"
        return "FRESH GOURMET"

    return "General Merchandise" # Fallback

def get_supplier_reliability(product_name, supplier_data, product_supplier_map, grn_map):
    # 1. Direct Map Lookup (Definitive)
    supplier_name = product_supplier_map.get(product_name)
    
    # 2. GRN Map Lookup (Fallback)
    if not supplier_name:
        supplier_name = grn_map.get(product_name)
    
    if not supplier_name:
        supplier_name = "Unknown"

    s_stats = supplier_data.get(supplier_name, {})
    return supplier_name, s_stats.get("reliability_score", 0.5), s_stats.get("median_gap_days", 7)

def main():
    print("Loading data...")
    forecast_data = load_json(FILES["forecast"])
    profit_data_raw = load_json(FILES["profitability"])
    supplier_data_raw = load_json(FILES["suppliers"])
    product_supplier_map_raw = load_json(FILES["supplier_map"])
    department_map_raw = load_json(FILES["department_map"])
    grn_map_raw = load_json(FILES["grn_map"])
    grn_freq_map_raw = load_json(FILES["grn_freq"])
    grn_freq_map_raw = load_json(FILES["grn_freq"])
    sup_share_map_raw = load_json(FILES["sup_share"])
    synthetic_proxy_raw = load_json(FILES["synthetic_proxy"])
    
    # Normalize keys to upper case for robust lookups
    profit_data = {k.strip().upper(): v for k, v in profit_data_raw.items()}
    supplier_data = {k.strip().upper(): v for k, v in supplier_data_raw.items()}
    product_supplier_map = {k.strip().upper(): v for k, v in product_supplier_map_raw.items()}
    department_map = {k.strip().upper(): v for k, v in department_map_raw.items()}
    grn_map = {k.strip().upper(): v for k, v in grn_map_raw.items()}
    
    grn_keys = list(grn_map.keys())
    print(f"Loaded {len(forecast_data)} Forecast SKUs")
    print(f"Loaded {len(department_map)} Department Mapped SKUs")
    print(f"Loaded {len(grn_map)} GRN Mapped SKUs")

    # --- OPTIMIZED BATCH FUZZY MATCHING (Blocked by First Letter) ---
    print("Performing Optimized Supplier Discovery (Blocked Fuzzy Matching)...")
    # 1. Identify items needing supplier
    unknown_items = []
    for item in forecast_data.keys():
        cleaned = item.strip().upper()
        if cleaned not in product_supplier_map and cleaned not in grn_map:
            unknown_items.append(cleaned)
    
    print(f"Found {len(unknown_items)} items with unknown suppliers. Starting matching...")

    # 2. Index GRN keys by first letter
    grn_blocks = {}
    for k in grn_keys:
        first_char = k[0] if k else '#'
        if first_char not in grn_blocks:
            grn_blocks[first_char] = []
        grn_blocks[first_char].append(k)
        
    # 3. Match
    matched_count = 0
    for item in unknown_items:
        first_char = item[0] if item else '#'
        # Only search in relevant block
        candidates = grn_blocks.get(first_char, [])
        if candidates:
            # Quick check: 0.7 cutoff
            matches = difflib.get_close_matches(item, candidates, n=1, cutoff=0.7)
            if matches:
                best_match = matches[0]
                found_supplier = grn_map[best_match]
                # Update grn_map directly so main loop finds it
                grn_map[item] = found_supplier
                matched_count += 1
                
    print(f"Batch Optimization: Resolved {matched_count} unknown suppliers via fuzzy blocking.")

    print("Calculating Departmental Price Averages for fallbacks...")
    # Temporarily calculate prices for everyone we can
    temp_prices = []
    for product_name, p_data in profit_data.items():
        price = p_data.get("unit_selling_price", 0)
        if price == 0 and p_data.get("total_qty_sold", 0) > 0:
            price = p_data.get("revenue", 0) / p_data.get("total_qty_sold")
        
        if price > 0:
            # We need dept for this item, but we don't have the final dept yet. 
            # We'll do a quick look-up
            dept = department_map.get(product_name.strip().upper(), "Unknown")
            if dept != "Unknown":
                temp_prices.append({"Dept": dept, "Price": price})
    
    price_df = pd.DataFrame(temp_prices)
    dept_avg_prices = price_df.groupby("Dept")["Price"].median().to_dict()
    global_median_price = price_df["Price"].median()
    print(f"Departmental averages calculated for {len(dept_avg_prices)} departments.")

    print("Fusing data (Matrix Generation)...")
    combined_data = []

    for product_name, f_data in forecast_data.items():
        item_name_clean = product_name.strip().upper()
        
        # Get Supplier
        supplier_name, reliability, lead_time = get_supplier_reliability(
            item_name_clean, supplier_data, product_supplier_map, grn_map
        )
        
        # Get Dept
        department = department_map.get(item_name_clean)
        if not department:
            department = determine_department(item_name_clean, supplier_name, lead_time, department_map)
            
        # Get Profitability
        p_data = profit_data.get(item_name_clean, {})
        avg_daily_sales = f_data.get("avg_daily_sales", 0)
        
        # Robust Price Calculation with Fallbacks
        unit_price_approx = p_data.get("unit_selling_price", 0)
        if unit_price_approx == 0 and p_data.get("total_qty_sold", 0) > 0:
            unit_price_approx = p_data.get("revenue", 0) / p_data.get("total_qty_sold")
            
        if unit_price_approx <= 0:
            # Try Department Fallback
            unit_price_approx = dept_avg_prices.get(department, global_median_price)
            if pd.isna(unit_price_approx): unit_price_approx = global_median_price
            
        margin_pct = p_data.get("margin_pct", 5)

        # --- PHASE 2: VELOCITY TIERS (Perfect Allocation blueprint) ---
        grn_freq = grn_freq_map_raw.get(item_name_clean, 0.0)
        if grn_freq > 0.8:
            velocity_tier = "A (Staple)"
            target_coverage = 21 # Aggressive for fast movers
        elif grn_freq > 0.5:
            velocity_tier = "B (Core)"
            target_coverage = 30
        elif grn_freq > 0.2:
            velocity_tier = "C (Filler)"
            target_coverage = 45 # Protect shelf presentation
        else:
            velocity_tier = "D (Risk)"
            target_coverage = 0 # MDQ Only

        # --- PHASE 3: FILL FORMULA (MDQ Enforcement) ---
        # MDQ = 1 Pack. We infer pack size or default to 1.
        pack_size = 12 if any(x in item_name_clean for x in ["6PK", "12PK", "SODA", "WATER"]) else 1
        mdq = pack_size * 1
        
        # InitialAllocation = MAX(MDQ, ADS * SizeFactor * Coverage)
        # For the base scorecard column, we reflect the 'Ideal/Mega' scenario (1.5x Multiplier)
        # but apply the FRESH CAP (2 days) for perishables.
        
        effective_coverage = target_coverage
        if department in FRESH_DEPARTMENTS:
            effective_coverage = min(effective_coverage, 2)
            
        required_qty = max(mdq, (avg_daily_sales * 1.5) * effective_coverage)
        capital_required = required_qty * unit_price_approx
        estimated_revenue = (avg_daily_sales * 30) * unit_price_approx

        # Eligibility Check
        is_eligible = True
        if supplier_name in EXCLUDED_SUPPLIERS: is_eligible = False
        if avg_daily_sales <= 0 and velocity_tier == "D (Risk)": is_eligible = False

        # --- DAY 1 GREENFIELD OVERRIDE ---
        current_stock = 0.0 # Standard for Greenfield Allocation
        logic_trace = f"Day 1 | Tier:{velocity_tier} | Vol:{avg_daily_sales:.2f} | LT:{lead_time}"
        
        # Flow Risk
        flow_risk = "High" if lead_time > 14 and reliability < 0.3 else "Low"

        combined_data.append({
            "Product": item_name_clean,
            "Department": department,
            "Supplier": supplier_name,
            "Supplier_Reliability": reliability,
            "Lead_Time_Days": lead_time,
            "Avg_Daily_Sales": avg_daily_sales,
            "Current_Stock": current_stock,
            "Unit_Price": unit_price_approx,
            "Margin_Pct": margin_pct,
            "Total_Revenue": estimated_revenue,
            "Recommended_Qty": required_qty,
            "Capital_Required": capital_required,
            "Velocity_Tier": velocity_tier,
            "GRN_Frequency": grn_freq,
            "Flow_Risk": flow_risk,
            "Trend_Pct": f_data.get("trend_pct", 0),
            "Is_Eligible_Basic": is_eligible,
            "Logic_Trace": logic_trace
        })

    df = pd.DataFrame(combined_data)
    
    # --- Logic: Strict Exclusion of Consignment Suppliers ---
    print(f"Initial Row Count: {len(df)}")
    df = df[~df["Supplier"].isin(EXCLUDED_SUPPLIERS)].copy()
    print(f"Row Count after Consignment Removal: {len(df)}")

    # --- SUPPLIER DOMINANT DEPARTMENT LOGIC ---
    print("Applying Supplier Dominant Department Logic...")
    # Calculate dominant dept per supplier
    # We filter out General Merchandise to get the "True" Departments
    valid_dept_df = df[df["Department"] != "General Merchandise"]
    supplier_dept_counts = valid_dept_df.groupby(["Supplier", "Department"]).size().reset_index(name="count")
    # For each supplier, get the department with max count
    supplier_dominant_Dept = {}
    
    # We sort by count desc, then drop duplicates (keeping first=max)
    sorted_counts = supplier_dept_counts.sort_values(by=["Supplier", "count"], ascending=[True, False])
    unique_suppliers_depts = sorted_counts.drop_duplicates(subset=["Supplier"], keep="first")
    
    for _, row in unique_suppliers_depts.iterrows():
        supplier_dominant_Dept[row["Supplier"]] = row["Department"]
        
    # Apply to General Merchandise items
    dom_count = 0
    gen_merch_indices = df[df["Department"] == "General Merchandise"].index
    for idx in gen_merch_indices:
        supplier = df.at[idx, "Supplier"]
        if supplier != "Unknown" and supplier in supplier_dominant_Dept:
            dom_dept = supplier_dominant_Dept[supplier]
            df.at[idx, "Department"] = dom_dept
            dom_count += 1
            
    print(f"Supplier Dominance: Re-classified {dom_count} items checking dominants.")

    # --- FUZZY DEPARTMENT MATCHING ---
    print("Applying Supplier-based Fuzzy Matching for General Merchandise...")
    # 1. Create a reference map: Supplier -> List of (Product, Department)
    # Only for items that are NOT General Merchandise
    reference_data = df[df["Department"] != "General Merchandise"][["Supplier", "Product", "Department"]]
    supplier_dept_lib = {}
    for _, row in reference_data.iterrows():
        s = row["Supplier"]
        if s not in supplier_dept_lib:
            supplier_dept_lib[s] = []
        supplier_dept_lib[s].append((row["Product"], row["Department"]))

    # 2. Iterate General Merchandise items
    gen_merch_indices = df[df["Department"] == "General Merchandise"].index
    corrected_count = 0
    
    for idx in gen_merch_indices:
        item_name = df.at[idx, "Product"]
        supplier = df.at[idx, "Supplier"]
        
        if supplier in supplier_dept_lib and supplier != "Unknown":
            # Get matches
            library = supplier_dept_lib[supplier]
            choices = [p for p, d in library]
            matches = difflib.get_close_matches(item_name, choices, n=1, cutoff=0.7)
            
            if matches:
                best_match = matches[0]
                for p, d in library:
                    if p == best_match:
                         df.at[idx, "Department"] = d
                         corrected_count += 1
                         break
    
    print(f"Fuzzy Match: Re-classified {corrected_count} General Merchandise items based on supplier patterns.")

    # --- KEYWORD MAPPING (O(N) Deterministic Classification) ---
    print("Applying Keyword Mapping for Unknown Suppliers...")
    gen_merch_indices = df[df["Department"] == "General Merchandise"].index
    # EXPANDED KEYWORD MAP - Comprehensive Classification to Reduce MISC Allocation
    keyword_map = {
        # Stationery & Office
        "STATIONARIES": [
            "TAPE", "GLUE", "FILE", "PEN", "MARKER", "NOTEBOOK", "STAPLER", "PAPER",
            "PENCIL", "ERASER", "RULER", "SCISSORS", "ENVELOPE", "FOLDER", "BINDER",
            "HIGHLIGHTER", "STAMP", "INK", "REFILL", "SHARPENER", "CALCULATOR", "DIARY",
            "CALENDAR", "STICKER", "CLIP", "PIN", "RUBBER BAND", "CORRECTION", "TIPP-EX"
        ],
        # Household & Kitchen
        "HOUSEHOLD ITEMS": [
            "LADLE", "SPOON", "STRAINER", "KNIFE", "GRATER", "KETTLE", "JUG", "MOP",
            "BROOM", "BRUSH", "SCRUB", "FOIL", "CLING", "BUCKET", "BASIN", "DUSTPAN",
            "HANGER", "PEG", "CLOTHESPIN", "RACK", "HOOK", "CONTAINER", "STORAGE",
            "TRAY", "THERMOS", "FLASK", "LUNCH BOX", "COOLER", "TRASH BAG", "GARBAGE",
            "BIN", "DUSTBIN", "SPONGE", "WIPER", "SQUEEGEE", "DUSTER", "CLOTH", "RAG",
            "APRON", "GLOVE", "OVEN MITT", "POT HOLDER", "PEELER", "OPENER", "CORKSCREW",
            "CUTTING BOARD", "CHOPPING", "SIEVE", "COLANDER", "FUNNEL", "MEASURING",
            "SCALE", "TIMER", "THERMOMETER", "BLENDER", "MIXER", "JUICER", "IRON",
            "IRONING BOARD", "CLOTHES LINE", "ROPE"
        ],
        # Candles & Lighting
        "CANDLES": ["MATCHES", "LIGHTER", "CANDLE", "WAX", "TEALIGHT", "WICK", "TORCH", "FLASHLIGHT"],
        # Incense & Air Fresheners
        "INCENSE STICKS": ["INCENSE", "COPAL", "AIR FRESHENER", "ROOM SPRAY", "DIFFUSER", "FRAGRANCE"],
        # Health & Supplements
        "VITAMIN SUPPLEMENTS": [
            "VITAMIN", "SUPPLEMENT", "TABLET", "CAPSULE", "MULTIVITAMIN", "OMEGA",
            "CALCIUM", "IRON", "ZINC", "PROBIOTIC", "FIBER", "PROTEIN POWDER"
        ],
        # Cosmetics & Beauty
        "COSMETICS": [
            "FACE WASH", "LOTION", "CREAM", "GEL", "LIPGLOSS", "PERFUME", "COLOGNE",
            "MASCARA", "EYELINER", "EYESHADOW", "FOUNDATION", "CONCEALER", "BLUSH",
            "BRONZER", "POWDER", "LIPSTICK", "LIP BALM", "NAIL POLISH", "NAIL",
            "MAKEUP", "REMOVER", "TONER", "SERUM", "MOISTURIZER", "SUNSCREEN", "SPF",
            "ANTI-AGING", "WRINKLE", "ACNE", "CLEANSER", "EXFOLIANT", "MASK", "SCRUB",
            "BEAUTY", "COSMETIC", "KAJAL", "BROW", "LASH"
        ],
        # Spices & Seasonings
        "SPICES": [
            "SEEDS", "SPICE", "PEPPER", "MASALA", "GINGER", "GARLIC", "TURMERIC",
            "CUMIN", "CORIANDER", "CARDAMOM", "CLOVE", "CINNAMON", "NUTMEG", "BAY LEAF",
            "OREGANO", "BASIL", "THYME", "ROSEMARY", "PARSLEY", "DILL", "MINT",
            "CHILI", "PAPRIKA", "CURRY", "PILAU", "BIRYANI", "TANDOORI", "BBQ",
            "SEASONING", "HERB", "SAFFRON", "VANILLA", "EXTRACT", "ESSENCE"
        ],
        # Glassware & Tableware
        "GLASSWARE ITEMS": [
            "GLASS", "CUP", "PLATE", "BOWL", "MUG", "TUMBLER", "GOBLET", "WINE GLASS",
            "BEER MUG", "SHOT GLASS", "PITCHER", "CARAFE", "DECANTER", "VASE",
            "SAUCER", "DISH", "PLATTER", "SERVING", "DINNER SET", "TEA SET", "CUTLERY SET"
        ],
        # Toys & Games
        "TOYS": [
            "TOY", "BALL", "DOLL", "PUZZLE", "GAME", "LEGO", "BUILDING BLOCK",
            "ACTION FIGURE", "TEDDY", "STUFFED", "PLUSH", "REMOTE CONTROL", "RC CAR",
            "BOARD GAME", "CARD GAME", "PLAYING CARDS", "DICE", "CHESS", "CHECKERS"
        ],
        # Pet Products
        "PET ASSESORIES": [
            "DOG", "CAT", "PET", "LEASH", "COLLAR", "HARNESS", "BOWL", "FEEDER",
            "LITTER", "CAGE", "AQUARIUM", "FISH FOOD", "BIRD SEED", "TREAT", "CHEW"
        ],
        # Beverages (catch-all for drinks not in SODA/JUICE)
        "BEVERAGES": [
            "DRINKING CHOCOLATE", "HOT CHOCOLATE", "COCOA", "MILO", "OVALTINE",
            "ENERGY DRINK", "RED BULL", "MONSTER", "POWER HORSE", "SPORTS DRINK",
            "GATORADE", "LUCOZADE", "ELECTROLYTE", "ISOTONIC"
        ],
        # Snacks & Confectionery
        "SNACKS": [
            "CHIPS", "CRISPS", "POPCORN", "NUTS", "PEANUT", "CASHEW", "ALMOND",
            "TRAIL MIX", "DRIED FRUIT", "RAISIN", "PRUNE", "DATE", "FIG",
            "CRACKER", "PRETZEL", "RICE CAKE", "GRANOLA BAR", "ENERGY BAR", "PROTEIN BAR"
        ],
        # Sweets & Candy
        "SWEETS": [
            "CANDY", "SWEET", "LOLLIPOP", "GUMMY", "JELLY BEAN", "HARD CANDY",
            "TOFFEE", "CARAMEL", "FUDGE", "MARSHMALLOW", "LIQUORICE", "MINT",
            "CHEWING GUM", "BUBBLE GUM", "MENTOS", "HALLS", "STREPSILS"
        ],
        # Chocolates
        "CHOCOLATES": [
            "CHOCOLATE", "CHOCO", "COCOA", "TRUFFLE", "PRALINE", "FERRERO",
            "LINDT", "GODIVA", "TOBLERONE", "SNICKERS", "MARS", "TWIX", "BOUNTY",
            "MILKY WAY", "KIT KAT", "HERSHEY", "CADBURY", "DAIRY MILK", "OREO"
        ],
        # Bakery & Pastry
        "CAKES": [
            "CAKE", "CUPCAKE", "MUFFIN", "BROWNIE", "COOKIE", "PASTRY", "PIE",
            "TART", "CROISSANT", "DONUT", "DOUGHNUT", "DANISH", "ECLAIR",
            "SWISS ROLL", "SPONGE", "GATEAU"
        ],
        # Biscuits & Cookies
        "BISCUITS": [
            "BISCUIT", "WAFER", "DIGESTIVE", "MARIE", "SHORTBREAD", "CREAM BISCUIT",
            "SANDWICH BISCUIT", "OATMEAL COOKIE", "GINGER SNAP", "CRUNCHIE"
        ],
        # Canned & Preserved Foods
        "CANNED MEAT": [
            "CANNED", "TINNED", "CORNED BEEF", "SPAM", "TUNA", "SARDINE", "MACKEREL",
            "SALMON", "ANCHOVY", "PILCHARD"
        ],
        "CANNED VEGETABLES": [
            "CANNED BEANS", "CANNED PEAS", "CANNED CORN", "CANNED TOMATO",
            "CANNED MUSHROOM", "CANNED OLIVE", "PICKLED", "PRESERVED"
        ],
        # Sauces & Condiments
        "SAUCES": [
            "SAUCE", "KETCHUP", "MAYONNAISE", "MAYO", "MUSTARD", "CHUTNEY", "RELISH",
            "SOY SAUCE", "TERIYAKI", "WORCESTERSHIRE", "HOT SAUCE", "TABASCO",
            "BBQ SAUCE", "SALSA", "GUACAMOLE", "HUMMUS", "PESTO", "DRESSING",
            "VINEGAR", "VINAIGRETTE", "MARINADE"
        ],
        # Jams & Spreads
        "JAMS": [
            "JAM", "JELLY", "MARMALADE", "PRESERVE", "SPREAD", "PEANUT BUTTER",
            "NUTELLA", "HAZELNUT", "CHOCOLATE SPREAD", "HONEY", "MAPLE SYRUP",
            "GOLDEN SYRUP", "MOLASSES", "TREACLE"
        ],
        # Cleaning Products
        "ALL CLEANERS": [
            "CLEANER", "DETERGENT", "DISINFECTANT", "SANITIZER", "BLEACH", "JIK",
            "HARPIC", "TOILET CLEANER", "DRAIN CLEANER", "GLASS CLEANER", "WINDEX",
            "FLOOR CLEANER", "TILE CLEANER", "MULTI-PURPOSE", "ALL PURPOSE",
            "DEGREASER", "STAIN REMOVER", "FABRIC SOFTENER", "CONDITIONER"
        ],
        # Laundry
        "DETERGENTS": [
            "WASHING POWDER", "LAUNDRY", "ARIEL", "OMO", "PERSIL", "TIDE",
            "SURF", "DOWNY", "COMFORT", "SOFTENER", "STARCH"
        ],
        # Personal Care
        "BATH SOAP": [
            "SOAP", "BODY WASH", "SHOWER GEL", "BATH", "LUX", "DOVE", "DETTOL",
            "LIFEBUOY", "PALMOLIVE", "GEISHA", "PROTEX", "SAFEGUARD"
        ],
        "SHAMPOOS/CONDITIONER": [
            "SHAMPOO", "CONDITIONER", "HAIR", "HEAD", "SHOULDERS", "PANTENE",
            "SUNSILK", "CLEAR", "TRESEMME", "HERBAL ESSENCE", "GARNIER",
            "ANTI-DANDRUFF", "2-IN-1", "KERATIN"
        ],
        "TOOTHPASTES": [
            "TOOTHPASTE", "TOOTHBRUSH", "MOUTHWASH", "DENTAL", "ORAL",
            "COLGATE", "SENSODYNE", "AQUAFRESH", "CLOSE UP", "CREST",
            "LISTERINE", "FLOSS", "WHITENING"
        ],
        "DEODORANTS": [
            "DEODORANT", "ANTIPERSPIRANT", "ROLL ON", "SPRAY", "BODY SPRAY",
            "AXE", "NIVEA", "REXONA", "SURE", "OLD SPICE", "DEGREE"
        ],
        # Baby Products
        "DIAPERS": [
            "DIAPER", "NAPPY", "PAMPERS", "HUGGIES", "MOLFIX", "SOFTCARE",
            "PULL UP", "TRAINING PANTS", "BABY DRY"
        ],
        "BABY FOODS": [
            "BABY FOOD", "INFANT", "FORMULA", "CERELAC", "NESTUM", "NAN",
            "SIMILAC", "ENFAMIL", "BABY CEREAL", "PABLUM", "BABY BISCUIT"
        ],
        "BABY COSMETICS": [
            "BABY LOTION", "BABY OIL", "BABY POWDER", "BABY CREAM", "BABY WASH",
            "BABY SHAMPOO", "JOHNSON", "BABY WIPES"
        ],
        # Sanitary Products
        "SANITARY TOWELS": [
            "SANITARY", "PAD", "PANTY LINER", "ALWAYS", "KOTEX", "STAYFREE",
            "WHISPER", "TAMPAX", "TAMPON", "MENSTRUAL", "PERIOD"
        ],
        # Tissue & Paper Products
        "TISSUE PAPER": [
            "TISSUE", "TOILET PAPER", "KITCHEN ROLL", "PAPER TOWEL", "SERVIETTE",
            "NAPKIN", "FACIAL TISSUE", "KLEENEX", "FINE", "VIVA"
        ],
        # Electronics & Batteries
        "BATTERIES": [
            "BATTERY", "DURACELL", "ENERGIZER", "EVEREADY", "PANASONIC",
            "ALKALINE", "RECHARGEABLE", "AAA", "AA", "9V", "D CELL", "C CELL"
        ],
        # Party & Events
        "PARTY ITEMS": [
            "PARTY", "BALLOON", "BANNER", "STREAMER", "CONFETTI", "DECORATION",
            "DISPOSABLE", "PAPER PLATE", "PAPER CUP", "PLASTIC FORK", "PLASTIC SPOON",
            "NAPKIN", "TABLECLOTH", "GIFT WRAP", "RIBBON", "BOW", "GIFT BAG",
            "PARTY HAT", "BLOWER", "WHISTLE", "PIÑATA", "CANDLE HOLDER"
        ],
        # Gift & Wrap
        "GIFT WRAP&RIBBON": [
            "GIFT", "WRAP", "WRAPPING", "RIBBON", "BOW", "GIFT BOX", "GIFT BAG",
            "TISSUE PAPER", "GREETING CARD", "CARD", "ENVELOPE"
        ],
        # Hardware & Tools (basic)
        "HARDWARE": [
            "NAIL", "SCREW", "BOLT", "NUT", "WASHER", "HINGE", "LOCK", "KEY",
            "HAMMER", "SCREWDRIVER", "PLIER", "WRENCH", "TAPE MEASURE", "LEVEL",
            "DRILL", "SAW", "SANDPAPER", "PAINT", "BRUSH", "ROLLER", "PUTTY"
        ],
        # Gardening
        "GARDENING": [
            "SEED", "PLANT", "FERTILIZER", "PESTICIDE", "INSECTICIDE", "HERBICIDE",
            "GARDEN", "POT", "PLANTER", "WATERING CAN", "HOSE", "SPRINKLER",
            "RAKE", "SHOVEL", "SPADE", "HOE", "WHEELBARROW", "GLOVE"
        ],
        # Automotive
        "AUTOMOTIVE": [
            "CAR", "MOTOR", "ENGINE", "OIL", "LUBRICANT", "BRAKE", "COOLANT",
            "WIPER", "BULB", "HEADLIGHT", "AIR FRESHENER", "CAR WASH", "POLISH",
            "WAX", "TIRE", "TYRE", "PUMP", "JACK", "JUMPER CABLE"
        ],
        # Books & Media
        "BOOKS": [
            "BOOK", "NOVEL", "MAGAZINE", "NEWSPAPER", "COMIC", "JOURNAL",
            "TEXTBOOK", "DICTIONARY", "ENCYCLOPEDIA", "ATLAS", "MAP"
        ],
        # Sports & Fitness
        "SPORTS": [
            "FOOTBALL", "SOCCER", "BASKETBALL", "TENNIS", "BADMINTON", "VOLLEYBALL",
            "CRICKET", "RACKET", "BAT", "BALL", "NET", "GOAL", "JERSEY",
            "SHORTS", "TRAINERS", "SNEAKERS", "GYM", "YOGA", "MAT", "DUMBBELL",
            "WEIGHT", "SKIPPING ROPE", "JUMP ROPE"
        ],
        # Clothing Accessories
        "CLOTHING ACCESSORIES": [
            "SOCK", "UNDERWEAR", "VEST", "BRIEF", "BOXER", "BRA", "PANTY",
            "BELT", "TIE", "SCARF", "HAT", "CAP", "GLOVE", "UMBRELLA",
            "WALLET", "PURSE", "BAG", "BACKPACK", "LUGGAGE", "SUITCASE"
        ],
        # Frozen Foods
        "FROZEN GOURMET": [
            "FROZEN", "ICE", "FRIES", "NUGGET", "FISH FINGER", "PIZZA",
            "SAMOSA", "SPRING ROLL", "MEAT PIE", "SAUSAGE ROLL"
        ],
        "ICE-CREAM": [
            "ICE CREAM", "ICECREAM", "GELATO", "SORBET", "FROZEN YOGURT",
            "POPSICLE", "ICE LOLLY", "CONE", "SUNDAE", "RIPPLE", "MELBA", "DL 1L"
        ],
        # Dairy Products
        "CHEESE": [
            "CHEESE", "CHEDDAR", "MOZZARELLA", "PARMESAN", "GOUDA", "BRIE",
            "FETA", "CREAM CHEESE", "COTTAGE CHEESE", "RICOTTA"
        ],
        # Alcoholic Beverages
        "WINES": [
            "WINE", "CHAMPAGNE", "PROSECCO", "SPARKLING", "RED WINE", "WHITE WINE",
            "ROSE", "MERLOT", "CABERNET", "CHARDONNAY", "SAUVIGNON", "PINOT",
            "SHIRAZ", "RIESLING", "MOSCATO"
        ],
        "SPIRITS": [
            "WHISKY", "WHISKEY", "VODKA", "GIN", "RUM", "BRANDY", "COGNAC",
            "TEQUILA", "LIQUEUR", "SCOTCH", "BOURBON", "HENNESSY", "JOHNNIE WALKER",
            "SMIRNOFF", "ABSOLUT", "BACARDI", "CAPTAIN MORGAN", "BAILEYS"
        ],
        "BEER": [
            "BEER", "LAGER", "ALE", "STOUT", "PILSNER", "IPA", "CRAFT BEER",
            "TUSKER", "GUINNESS", "HEINEKEN", "CORONA", "BUDWEISER", "STELLA",
            "CARLSBERG", "BAVARIA"
        ],
        # Medicinal (OTC)
        "MEDICARE": [
            "PANADOL", "PARACETAMOL", "ASPIRIN", "IBUPROFEN", "ANTACID",
            "COUGH SYRUP", "COLD", "FLU", "ALLERGY", "BANDAGE", "PLASTER",
            "FIRST AID", "ANTISEPTIC", "COTTON WOOL", "GAUZE", "THERMOMETER"
        ],
        # Insect Control
        "INSECTICIDES": [
            "INSECTICIDE", "PESTICIDE", "DOOM", "RAID", "MORTEIN", "BAYGON",
            "MOSQUITO", "FLY", "COCKROACH", "ANT", "BUG SPRAY", "REPELLENT",
            "COIL", "FUMIGATOR"
        ],
        # Kitchen Utensils (Brand-Specific: ROSS, VINOD, KMW)
        "KITCHEN UTENSILS": [
            "TURNER", "SKIMMER", "MASHER", "WHISK", "WOK", "CASSEROLE", "ROASTER",
            "BASTING", "LADLE", "SPATULA", "TONGS", "PASTA SERVER", "SLOTTED",
            "EGG BEATER", "CHOPPER", "CUTTER", "PEELER", "GRATER", "SLICER",
            "ROSS ", "VINOD ", "KMW ", "KITCH ", "LIONSTAR", "HOMKART", "DKW ",
            "TABLEWARE", "CUTLERY SET", "AIR FRYER", "LINER", "LUNCHBOX",
            "FOOD COVER", "EGG HOLDER", "PILL BOX", "MEDICINE BOX"
        ],
        # Exercise Books & Stationery (Brand: POLAR)
        "EXERCISE BOOKS": [
            "POLAR ", "EX BK", "EXERCISE BOOK", "NOTEBOOK", "RULED", "SQUARED",
            "A4", "A5", "64PG", "80PG", "96PG", "120PG", "LAMINATING", "POUCHES"
        ],
        # Ayurvedic & Herbal Products (Brands: ZANDU, HIMALAYA, KAYAM, HAWABAN)
        "AYURVEDIC PRODUCTS": [
            "AYURVEDIC", "AYURVEDA", "ZANDU", "HIMALAYA", "KAYAM", "HAWABAN",
            "CHURNA", "GRANUELS", "DIGESTION", "HERBAL", "TRUPTI", "GASEX"
        ],
        # Hair Care (African specific brands)
        "HAIR CARE": [
            "AFRICAN PRIDE", "DREAM KIDS", "OLIVE MIRACLE", "DETANGLER",
            "RELAXER", "EDGE CONTROL", "BRAID", "HAIR OIL", "HAIR FOOD",
            "SCALP", "MOISTURE", "SILKY SMOOTH"
        ],
        # Infant Formula & Baby (Brand: S26, SIMILAC, etc.)
        "BABY FORMULA": [
            "S26 ", "PROMIL", "STAGE 1", "STAGE 2", "STAGE 3", "INFANT FORMULA",
            "FOLLOW-ON", "GROWING UP", "NAN ", "LACTOGEN", "ENFAMIL", "SIMILAC"
        ],
        # Conserves & Preserves
        "CONSERVES": [
            "CONSERVE", "PRESERVE", "STUTE ", "FORNO BONOMI", "SAVOIARDI",
            "LADY FINGER", "BISCOTTI"
        ],
        # Novelty & Party (PMS Brand specifics)
        "NOVELTY ITEMS": [
            "PMS ", "YOYO", "SOFTLINGS", "FOODIES", "GOGGLES", "EAR PLUG",
            "WRIST BAND", "CITRONELLA", "DEHUMIDIFIER", "WARDROBE", "MAZE",
            "CROSS WORD", "CRAFT MODEL", "SEWING KIT", "LAMPSHADE"
        ],
        # Bathroom & Shower
        "BATHROOM ACCESSORIES": [
            "SHOWER", "DUCHA", "BATH MAT", "LOOFAH", "SCRUBBER", "BATH SPONGE",
            "FAUCET", "TAP", "PIPE", "SHOWERHEAD", "FAME SUPA"
        ],
        # Art & Drawing Supplies
        "ART SUPPLIES": [
            "ARTLINE", "DRAWING SET", "SKETCH", "CANVAS", "EASEL", "PAINT BRUSH",
            "WATERCOLOR", "ACRYLIC", "OIL PAINT", "CHARCOAL", "PASTEL"
        ],
        # Dish Washing (catch more brands)
        "WASHING UP LIQUID": [
            "WASH UP", "DISH WASH", "ECOVER", "CAMOMILE", "CLEMENTINE",
            "ECO FRIENDLY", "PLANT BASED"
        ],
        # Energy Drinks (additional keywords)
        "ENERGY DRINKS": [
            "RUBICON", "RAW ENERGY", "BOOST", "REDBULL", "RED BULL", "MONSTER",
            "ROCKSTAR", "POWER HORSE", "V ENERGY", "EMERGE"
        ],
        # Spices (additional Indian spices)
        "SPICES": [
            "FENUCREEK", "FENUGREEK", "CRUSHED", "WHOLE SPICE", "METHI",
            "JEERA", "AJWAIN", "ASAFOETIDA", "HING", "MUSTARD SEED"
        ],
        # Combs & Hair Tools
        "HAIR ACCESSORIES": [
            "COMB", "HAIR BRUSH", "HAIR CLIP", "BOBBY PIN", "HAIR BAND",
            "SCRUNCHIE", "HAIR TIE", "HEADBAND", "BARRETTE"
        ],
        # Pressure Cookers & Heavy Kitchen
        "COOKWARE": [
            "PRESSURE COOKER", "P/COOKER", "ALLUMINIUM", "ALUMINUM", "STAINLESS",
            "NON STICK", "NON-STICK", "N/S ", "TEFLON", "CERAMIC"
        ],
        # Air Fresheners (catch remaining brands)
        "AIR FRESHENERS": [
            "GLADE", "AIR FRESHENER", "A/FRESHENER", "TRIGGER", "ODOUR ELIMINATOR",
            "ODUOR ELIMINATOR", "SENSO", "ROOM SPRAY", "AEROSOL", "AUTOMATIC SPRAY"
        ],
        # Dried Vegetables (African greens)
        "DRIED VEGETABLES": [
            "DRIED SAGA", "DRIED DODO", "DRIED SUKUMA", "DRIED KUNDE", "DRIED MANAGU",
            "DRIED TERERE", "DRIED MRENDA", "DRIED SPINACH", "DRIED KALE", "SUN DRIED"
        ],
        # Body Wash & Men's Grooming
        "MENS GROOMING": [
            "MEN SPORT", "MEN BODY", "MENS BODY", "AFTERSHAVE", "SHAVING",
            "RAZOR", "BLADE", "GILLETTE", "NIVEA MEN", "NIVEA FOR MEN"
        ],
        # Office Supplies (badges, filing, envelopes)
        "OFFICE SUPPLIES": [
            "BADGE", "NAME TAG", "CONFERENCE", "FILING", "POCKET", "ENVELOPE",
            "EMVELOPE", "FOLDER", "BINDER", "CLIP", "STAPLE"
        ],
        # Olives & Mediterranean
        "OLIVES": [
            "OLIVE", "PITTED", "KVUZAT", "YAVNE", "KALAMATA", "STUFFED OLIVE"
        ],
        # Pest Control
        "PEST CONTROL": [
            "COCKRAKILL", "ROACH KILLER", "RAT POISON", "MOUSE TRAP", "RODENT",
            "PEST CONTROL", "TERMITE", "ANT KILLER"
        ],
        # Health Supplements & Tonics
        "HEALTH TONICS": [
            "SCOTT EMULSION", "EMULSION", "HAJMOLA", "DABUR", "TONIC", "SYRUP",
            "LIVER OIL", "COD LIVER", "OMEGA 3"
        ],
        # Soft Drinks & Beverages (catch more brands)
        "SOFT DRINKS": [
            "PEP ", "OSTERBERG", "OSTERBER", "APPLE GRAPE", "ALOE VERA DRINK",
            "SPARKLING", "FIZZY", "CARBONATED"
        ],
        # Gluten-Free & Specialty Bread
        "SPECIALTY BREAD": [
            "KIRSTEN", "GLUTEN FREE", "GF BREAD", "SOURDOUGH", "ARTISAN BREAD",
            "WHOLE GRAIN BREAD", "MULTIGRAIN BREAD"
        ],
        # Service Items (To mark as non-product - they'll still go to MISC but that's OK)
        "SERVICE FEES": [
            "DELIVERY", "ADVERTISING FEE", "PILFERAGE", "BONUS CARD", "EMPTY BOX",
            "SERVICE CHARGE", "HANDLING FEE"
        ],
        # Plastics & Storage
        "PLASTIC CONTAINERS": [
            "SAVENA", "PLASTIC", "CONTAINER", "TUPPERWARE", "STORAGE BOX"
        ]
    }
    
    keyword_corrected = 0
    for idx in gen_merch_indices:
        p_name = str(df.at[idx, "Product"]).upper()
        
        # Check keywords
        found = False
        for target_dept, keywords in keyword_map.items():
            if any(k in p_name for k in keywords):
                df.at[idx, "Department"] = target_dept
                keyword_corrected += 1
                found = True
                break
    
    print(f"Keyword Map: Re-classified {keyword_corrected} items based on text tokens.")

    # --- GLOBAL FUZZY MATCHING (Optimized) ---
    print("Applying Global Fuzzy Matching (Optimized Library)...")
    remaining_gen_merch = df[df["Department"] == "General Merchandise"].index
    
    if len(remaining_gen_merch) > 0:
        # Optimization: Build library from Top 5000 items only to speed up matching 5x
        valid_items = df[df["Department"] != "General Merchandise"]
        # Sort by Revenue to ensure we match against prominent/correct items
        library_source = valid_items.sort_values(by="Total_Revenue", ascending=False).head(5000)
        
        global_library = library_source[["Product", "Department"]].values.tolist()
        global_choices = [p for p, d in global_library]
        
        print(f"  Fuzzy Library Size: {len(global_choices)} items (Top Performers)")
        
        global_corrected = 0
        for idx in remaining_gen_merch:
            item_name = df.at[idx, "Product"]
            # Cutoff 0.6 is looser to catch more, but matched against High Quality Library
            matches = difflib.get_close_matches(item_name, global_choices, n=1, cutoff=0.6)
            if matches:
                 best_global_match = matches[0]
                 for p, d in global_library:
                     if p == best_global_match:
                         df.at[idx, "Department"] = d
                         global_corrected += 1
                         break
    print(f"Global Fuzzy Match: Re-classified {global_corrected} leftover items.")

    # --- FINAL SAFETY NET: MISC. DEPARTMENT ---
    # Any item still in 'General Merchandise' is moved to 'MISC. DEPARTMENT'.
    remaining = df[df["Department"] == "General Merchandise"].index
    if len(remaining) > 0:
        print(f"Final Cleanup: Moving {len(remaining)} stubborn items to 'MISC. DEPARTMENT'.")
        df.loc[remaining, "Department"] = "MISC. DEPARTMENT"

    # --- LOAD STAPLES AND RATIOS ---
    staples_path = os.path.join(DATA_DIR, FILES["staples"])
    with open(staples_path, "r") as f:
        staples_raw = json.load(f)
    staples_list = [s.strip().upper() for s in staples_raw]
        
    ratios_path = os.path.join(DATA_DIR, FILES["ratios"])
    ratios_df = pd.read_csv(ratios_path)
    # Normalize department names to upper for index and aggregate duplicates
    ratios_df['Department'] = ratios_df['Department'].str.strip().str.upper()
    ratios_df = ratios_df.groupby('Department').agg({
        'Capital_Weight': 'sum',
        'SKU_per_Million': 'mean' # Or however we want to blend SKU depth
    }).reset_index()
    
    # Convert to dict for faster lookups in allocation function
    dept_ratios = ratios_df.set_index('Department').to_dict('index')
    
    # STRICT RE-DEFINITION: "Staple" = Survival/Strategic Depts Only.
    # We ignore the product list because it contains luxury items (Ice Cream etc).
    
    # 1. Update Staple Depts (Ensure Sugar is captured)
    # (Checking the global list STAPLE_DEPARTMENTS is correct)
    
    df['Is_Staple'] = df['Department'].isin(STAPLE_DEPARTMENTS)
    
    # Debug Sugar
    sugar_staples = df[(df['Department'] == 'SUGAR') & (df['Is_Staple'] == True)]
    print(f"Staple Identification (Strict Dept): Tagged {df['Is_Staple'].sum()} staple items.")
    print(f"DEBUG: Classified {len(sugar_staples)} Sugar items as Staples.")

    # --- Logic: Detailed Step-by-Step Stocking Breakdown ---
    def get_stocking_breakdown(row):
        steps = []
        is_eligible = True
        
        # Step 1: Revenue Check
        if row["Total_Revenue"] > 0:
            steps.append("Rev:OK")
        else:
            steps.append("Rev:NONE")
            is_eligible = False
            
        # Step 2: Price Check
        if row["Unit_Price"] > 0:
            steps.append("Price:OK")
        else:
            steps.append("Price:ZERO")
            is_eligible = False

        # Note: Strategy check happens later after columns are added, 
        # but we can pre-emptively check loose criteria if needed. 
        # For now, we finalize the breakdown after ABC/Strat calculation.
        
        return " | ".join(steps), True # Default to eligible for initial data fusion

    # Apply initial checks
    df[["Logic_Trace", "Is_Eligible_Basic"]] = df.apply(
        lambda r: pd.Series(get_stocking_breakdown(r)), axis=1
    )

    # --- ABC Analysis ---
    print("Calculating Strategy Metrics...")
    df = df.sort_values(by="Total_Revenue", ascending=False)
    total_revenue_sum = df["Total_Revenue"].sum()
    df["Cumulative_Revenue"] = df["Total_Revenue"].cumsum()
    df["Revenue_Share"] = df["Cumulative_Revenue"] / total_revenue_sum
    
    def assign_abc(x):
        if x <= 0.80: return "A"
        elif x <= 0.95: return "B"
        else: return "C"
    df["ABC_Class"] = df["Revenue_Share"].apply(assign_abc)
    df.loc[df["Total_Revenue"] == 0, "ABC_Class"] = "D" 

    # --- GMROI ---
    df["Gross_Profit_Amt"] = df["Total_Revenue"] * (df["Margin_Pct"] / 100)
    df["GMROI"] = np.where(df["Capital_Required"] > 0, df["Gross_Profit_Amt"] / df["Capital_Required"], 0)
    
    # --- Strategy Role ---
    conditions = [
        (df["ABC_Class"] == "A") & (df["Margin_Pct"] > 10), # Cash Cow
        (df["Trend_Pct"] > 20), # Star
        (df["ABC_Class"].isin(["C", "D"])) & (df["Margin_Pct"] < 5), # Dog
        (df["ABC_Class"].isin(["B", "C"])) & (df["Margin_Pct"] > 15) # Profit Driver
    ]
    choices = ["Cash Cow", "Star", "Dog", "Profit Driver"]
    df["Strategy_Role"] = np.select(conditions, choices, default="Standard")
    
    action_conditions = [
        (df["Strategy_Role"] == "Cash Cow"),
        (df["Strategy_Role"] == "Star"),
        (df["Strategy_Role"] == "Dog"),
        (df["Strategy_Role"] == "Profit Driver")        
    ]
    action_choices = ["Stock Heavy / Never Out", "Stock / Invest Marketing", "Delist / Liquidate", "Stock / Maintain Price"]
    df["Recommended_Action"] = np.select(action_conditions, action_choices, default="Review")

    # --- Finalize Stocking Logic Trace ---
    def finalize_trace(row):
        trace = row["Logic_Trace"]
        if not row["Is_Eligible_Basic"]:
            return trace + " -> Ineligible"
            
        # Strategy Check
        if row["Recommended_Action"] == "Delist / Liquidate":
            return trace + " | Strat:DELIST -> Ineligible"
        
        return trace + " | Strat:OK -> Eligible"

    df["Stocking_Notes"] = df.apply(finalize_trace, axis=1)
    
    # Overwrite eligibility for strategic items - If it's a Staple, it's ALWAYS eligible
    df.loc[df['Is_Staple'] == True, 'Is_Eligible_Basic'] = True

    # --- Scoring ---


    # --- Scoring ---
    # Balanced Weighting: Volume (40%), ABC (30%), Trend (20%), GMROI (10%)
    abc_map = {"A": 100, "B": 70, "C": 40, "D": 10}
    df["Score_Weighted"] = (
        (df["Avg_Daily_Sales"] * 10) * 0.4 +
        df["ABC_Class"].map(abc_map) * 0.3 +
        df["Trend_Pct"] * 0.2 +
        df["GMROI"] * 0.1
    )

    # --- HYBRID INTEGRATION: Synthetic Proxy Boost ---
    print("Applying Synthetic Proxy Boost (Hybrid Logic)...")
    proxy_products = set(synthetic_proxy_raw.keys())
    
    # Flag Core Items
    df["Is_Proxy_Core"] = df["Product"].isin(proxy_products)
    
    # Calculate Hybrid Score:
    # 1. Base Boost: Proxy Core items get +5000.
    df["Hybrid_Score"] = df["Score_Weighted"] + np.where(df["Is_Proxy_Core"], 5000, 0)
    
    # 2. Cost Efficiency Drag (Smart Penalty)
    # Goal: Squeeze more breadth into Small Tier by demoting expensive non-essential packs.
    # Logic: If Cost > 1000 AND Not Essential -> Apply -2000 Penalty.
    
    # We need to infer Pack Size roughly to get "Unit Price" vs "Pack Cost"
    # Note: Unit_Price in profit data is usually "Per Unit" (Bottle), but we buy in "Packs".
    # Logic assumption: allocation logic multiplies by pack size later. 
    # But for sorting, we care about the "Entry Ticket Price".
    # Let's approximate Pack Price = Unit_Price * 6 (avg pack size) if not clear.
    # Actually, we can use a simpler heuristic: Unit Price > 500 is likely a high ticket item.
    
    
    # Define Essential Departments (Exempt from penalty)
    ESSENTIAL_DEPTS = STAPLE_DEPARTMENTS 
    
    def calculate_cost_drag(row):
        # Infer Pack Size mainly for Cost Calculation
        p_name = str(row['Product'])
        pack_size = 1
        if any(x in p_name for x in ["6PK", "6 PK"]): pack_size = 6
        elif any(x in p_name for x in ["12PK", "12 PK", "SODA", "WATER"]): pack_size = 12
        elif "24PK" in p_name or "24 PK" in p_name: pack_size = 24
        
        pack_cost = row['Unit_Price'] * pack_size
        
        # If it's a Staple/Essential, NO PENALTY.
        if row['Department'] in ESSENTIAL_DEPTS: return 0
        
        # Check PACK COST Threshold (Entry Ticket)
        # > 1500 is a heavy item for a micro-store (e.g. Case of Juice)
        if pack_cost > 1500:
            return -2000 
        return 0
        
    df["Cost_Drag"] = df.apply(calculate_cost_drag, axis=1)
    df["Hybrid_Score"] = df["Hybrid_Score"] + df["Cost_Drag"]

    print(f"Hybrid Score Calculated. Identified {df['Is_Proxy_Core'].sum()} Core Proxy Items.")
    print(f"Cost Drag applied to {(df['Cost_Drag'] < 0).sum()} expensive non-essential items.")

    # --- DYNAMIC SCALING ALLOCATION ---
    def allocate_cap(tier_name, config):
        target_budget = config["target_budget"]
        size_modifier = config["multiplier"]
        tier_coverage = config["coverage"]
        grn_threshold = config["grn_threshold"]
        staple_dept_share = config["staple_dept_share"]
        pass2_depth_cap = config.get("depth_cap", tier_coverage)
        pass2_depth_cap = config.get("depth_cap", tier_coverage)
        price_ceiling_pct = config.get("price_ceiling_pct", 1.0)
        max_packs = config.get("max_packs", 999) # Default to effectively unlimited

        print(f"\nRefined Tiered Allocation for {tier_name} (${target_budget:,.1f})...")
        df[tier_name] = False
        
        current_spent = 0
        bucket_spend = {}
        dept_sku_counts = {} # NEW: Track count per dept to enforce caps

        # Pre-calculate Dept Wallets for Price Ceiling check
        dept_wallets = {}
        for d, stats in dept_ratios.items():
            dept_wallets[d] = target_budget * stats.get('Capital_Weight', 0.01)

        # 1. Implementation: Selection Filter
        eligible_mask = (df['Is_Eligible_Basic'] == True)
        # CRITICAL FIX: Allow Proxy Core items to bypass the GRN Threshold.
        # Strategy (Proxy) > Frequency Constraint.
        selection_mask = (df['GRN_Frequency'] >= grn_threshold) | (df['Is_Staple'] == True) | (df['Is_Proxy_Core'] == True)
        
        # 2. Elastic Budget Pass (Staple vs Discretionary Pools)
        staple_budget_cap = target_budget * staple_dept_share
        disc_budget_cap = target_budget - staple_budget_cap
        
        staple_pool_spent = 0
        disc_pool_spent = 0

        # Optimization Helper: Calculate cost per item based on Tier-Specific Formula
        def calc_item_cost(idx, override_coverage=None):
            avg_daily_sales = df.at[idx, 'Avg_Daily_Sales']
            p_name = str(df.at[idx, 'Product'])
            dept = df.at[idx, 'Department']
            
            # Improved pack_size inference
            pack_size = 1
            if any(x in p_name for x in ["6PK", "6 PK"]): pack_size = 6
            elif any(x in p_name for x in ["12PK", "12 PK", "SODA", "WATER"]): pack_size = 12
            elif "24PK" in p_name or "24 PK" in p_name: pack_size = 24
            
            mdq = pack_size * 1
            
            # FRESH ITEM CAP: 2 Days max for Fresh Depts regardless of tier_coverage
            effective_coverage = override_coverage if override_coverage is not None else tier_coverage
            if dept in FRESH_DEPARTMENTS:
                effective_coverage = min(effective_coverage, 2)
            
            # Formula: Order_qty = MAX(MDQ, GlobalADS * Multiplier * Coverage)
            calc_qty = (avg_daily_sales * size_modifier) * effective_coverage
            
            # Apply Max Packs Cap (New Logic)
            # We allow at least MDQ, but cap the upside of the calculated quantity
            max_qty_allowed = pack_size * max_packs
            capped_calc_qty = min(calc_qty, max_qty_allowed)
            
            final_qty = max(mdq, capped_calc_qty)
            return final_qty * df.at[idx, 'Unit_Price']

        # BREADTH-FIRST LOGIC: 
        # Pass 1: Give every eligible item 1 pack (MDQ) to maximize variety
        # HYBRID UPDATE: Sort by Hybrid_Score to prioritize Proxy Core items
        candidates = df[eligible_mask & selection_mask].sort_values(by="Hybrid_Score", ascending=False)
        
        print(f"  Starting Pass 1 (Variety/MDQ) for {len(candidates)} candidates...")
        for idx in candidates.index:
            dept = df.at[idx, 'Department']
            is_staple_dept = dept in STAPLE_DEPARTMENTS
            
            cost = calc_item_cost(idx, override_coverage=0) # Only MDQ
            if cost <= 0: continue
            
            # STRATEGIC GUARDRAIL: Price Ceiling (Small Tier)
            # We add a floor of $500 to the ceiling to prevent excluding multi-packs 
            # in niche departments with very small wallets.
            wallet_limit = dept_wallets.get(dept, 0) * price_ceiling_pct
            relaxed_ceiling = max(500, wallet_limit) 
            
            # STAPLE BYPASS: Staple departments ALWAYS pass price ceiling
            if cost > relaxed_ceiling and "Small" in tier_name and not is_staple_dept:
                continue
            
            # Pool Check
            if is_staple_dept:
                if staple_pool_spent + cost > staple_budget_cap: continue
            else:
                if disc_pool_spent + cost > disc_budget_cap: continue
            
            if current_spent + cost <= target_budget:
                # NEW: Department SKU Cap for Small Stores
                # Prevent Yoghurt/Bread from taking 254 slots.
                current_dept_count = dept_sku_counts.get(dept, 0)
                sku_cap = 999 
                if "Small" in tier_name and dept in FRESH_DEPARTMENTS:
                     sku_cap = 20 # Hard cap for Fresh Depts in Small Stores
                
                # DEBUG: Print if we are hitting high numbers in Fresh
                if dept == "BREAD" and current_dept_count == 21:
                    print(f"DEBUG: BREAD hit 21. Cap is {sku_cap}. In Fresh List? {'BREAD' in FRESH_DEPARTMENTS}")
                
                if current_dept_count >= sku_cap:
                    continue

                df.at[idx, tier_name] = True
                current_spent += cost
                if is_staple_dept: staple_pool_spent += cost
                else: disc_pool_spent += cost
                bucket_spend[(dept, df.at[idx, 'Supplier'])] = bucket_spend.get((dept, df.at[idx, 'Supplier']), 0) + cost
                dept_sku_counts[dept] = current_dept_count + 1

        print(f"  Pass 1 Result: {df[df[tier_name]].shape[0]} SKUs, Spent: ${current_spent:,.2f}")

        # Pass 2: Fill to Target Coverage (Depth)
        print(f"  Starting Pass 2 (Coverage Depth - Cap: {pass2_depth_cap} days)...")
        for idx in candidates.index:
            if not df.at[idx, tier_name]: continue
            
            dept = df.at[idx, 'Department']
            is_staple_dept = dept in STAPLE_DEPARTMENTS
            
            # Apply Depth Cap for Pass 2 (e.g. 7 days for small)
            full_cost = calc_item_cost(idx, override_coverage=pass2_depth_cap)
            added_cost = full_cost - calc_item_cost(idx, override_coverage=0)
            
            if added_cost <= 0: continue
            
            # Pool Check
            if is_staple_dept:
                if staple_pool_spent + added_cost > staple_budget_cap: continue
            else:
                if disc_pool_spent + added_cost > disc_budget_cap: continue
                
            if current_spent + added_cost <= target_budget:
                # Supplier/Dept Weight Protection (Bucket Ceiling) - Strict Wallet enforcement
                dept_weight = dept_ratios.get(dept, {'Capital_Weight': 0.05})['Capital_Weight']
                dept_sup_shares = sup_share_map_raw.get(dept, {"Unknown": 1.0})
                sup = df.at[idx, 'Supplier']
                
                # Multiplier buffer: 1.0x (Strict) for Small, up to unlimited for Mega
                wallet_buffer = 1.0 if "Small" in tier_name else 1.5 if "Med" in tier_name else 100.0
                bucket_limit = (target_budget * dept_weight * dept_sup_shares.get(sup, 0.2)) * wallet_buffer
                
                if (bucket_spend.get((dept, sup), 0) + added_cost) > bucket_limit:
                    continue
                
                current_spent += added_cost
                if is_staple_dept: staple_pool_spent += added_cost
                else: disc_pool_spent += added_cost
                bucket_spend[(dept, df.at[idx, 'Supplier'])] = bucket_spend.get((dept, df.at[idx, 'Supplier']), 0) + added_cost

        print(f"  Final Assortment: {df[df[tier_name]].shape[0]} SKUs, Total Spent: ${current_spent:,.2f}")
        print(f"  Pool Balance: Staple ${staple_pool_spent:,.0f} ({staple_pool_spent/max(1,target_budget):.1%}) | Disc ${disc_pool_spent:,.0f} ({disc_pool_spent/max(1,target_budget):.1%})")
        return current_spent
    
    # Run Scenarios and capture stats
    SUMMARY_STATS = {}
    for tier, config in SCENARIO_CONFIGS.items():
        spent = allocate_cap(tier, config)
        SUMMARY_STATS[tier] = spent

    # Final cleanup and summary
    print("\n--- Allocation Stats ---")
    for scenario in SCENARIO_CONFIGS.keys():
        total_skus = df[df[scenario] == True].shape[0]
        actual_val = SUMMARY_STATS[scenario]
        print(f"{scenario}: {total_skus} SKUs, Total Capital: ${actual_val:,.2f}")

    print("\nScorecard Sample (Top 15 Items):")
    print(df.sort_values(by="Score_Weighted", ascending=False)[["Product", "Department", "Score_Weighted", "Is_Staple", "Small_200k", "Mega_115M"]].head(15).to_string(index=False))

    print(f"\nSaving {len(df)} rows to {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)

if __name__ == "__main__":
    main()
