import json
import difflib
import re

def load_json(filepath):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {filepath}")
        return None

def save_json(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)
    print(f"Saved data to {filepath}")

def normalize_text(text):
    """Lowercases and removes special characters for better matching."""
    if not text:
        return ""
    return re.sub(r'[^a-z0-9\s]', '', text.lower()).strip()

def reclassify_items():
    map_file = 'app/data/product_department_map.json'
    target_file = 'app/data/sales_profitability_intelligence_2025_updated.json'
    output_file = 'app/data/sales_profitability_intelligence_2025_reclassified.json'

    print("Loading data...")
    dept_map = load_json(map_file)
    sales_data = load_json(target_file)

    if not dept_map or not sales_data:
        return

    # Create a normalized version of the map for easier lookups
    normalized_dept_map = {normalize_text(k): v for k, v in dept_map.items()}
    
    # Also create a reverse lookup or just a list of normalized product names for fuzzy matching
    # Using the map keys (product names) as the corpus for fuzzy matching
    map_product_names = list(dept_map.keys())
    normalized_map_product_names = list(normalized_dept_map.keys())

    reclassified_count = 0
    total_unknowns = 0
    
    print("Starting reclassification...")
    
    for product_name, details in sales_data.items():
        if details.get('category') == 'unknown' or details.get('category') == 'Misc.':
            total_unknowns += 1
            
            # Strategy 1: Exact Match in Map
            if product_name in dept_map:
                details['category'] = dept_map[product_name]
                reclassified_count += 1
                continue

            # Strategy 2: Normalized Match
            norm_name = normalize_text(product_name)
            if norm_name in normalized_dept_map:
                details['category'] = normalized_dept_map[norm_name]
                reclassified_count += 1
                continue
                
            # Strategy 3: Keyword Search (Simple Heuristics)
            # Sometimes the map keys might contain the target product name as a substring or vice versa
            found_substring = False
            for map_key, map_dept in dept_map.items():
                if product_name in map_key or map_key in product_name:
                     # Check if it's a "good" match (e.g., significant length overlap)
                     # For now, simplistic: if one is substring of other and length diff isn't massive logic could be added
                     # But let's trust the map_key's department if it's a substring match for now, carefully.
                     # Actually, explicit simplistic keyword matching might be safer than fuzzy for "Milk", "Bread" etc.
                     pass 

            # Strategy 4: Fuzzy Match
            # We match against the keys in the department map
            # This can be slow if the map is huge (40k items).
            # Optimization: maybe only fuzzy match if other methods fail.
            
            # Using difflib.get_close_matches
            # It expects a list of possibilities.
            matches = difflib.get_close_matches(product_name, map_product_names, n=1, cutoff=0.7)
            if matches:
                 best_match = matches[0]
                 details['category'] = dept_map[best_match]
                 # Mark as inferred so we know
                 details['reclassification_method'] = f"fuzzy_match: {best_match}"
                 reclassified_count += 1
                 continue
            
            # Try fuzzy on normalized strings if raw failed
            matches_norm = difflib.get_close_matches(norm_name, normalized_map_product_names, n=1, cutoff=0.7)
            if matches_norm:
                 best_match_norm = matches_norm[0]
                 details['category'] = normalized_dept_map[best_match_norm]
                 details['reclassification_method'] = f"fuzzy_match_normalized: {best_match_norm}"
                 reclassified_count += 1
                 continue

    print(f"Reclassification complete.")
    print(f"Total 'unknown' items processed: {total_unknowns}")
    print(f"Successfully reclassified: {reclassified_count}")
    print(f"Remaining unknowns: {total_unknowns - reclassified_count}")

    save_json(output_file, sales_data)

if __name__ == "__main__":
    reclassify_items()
