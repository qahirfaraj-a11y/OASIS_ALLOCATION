import json
import os

def extract_store_coords(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    stores_coords = {}
    for store in data.get('stores', []):
        org_cd = store.get('store_id')
        name = store.get('name')
        lat = store.get('latitude')
        lon = store.get('longitude')
        if org_cd:
            stores_coords[org_cd] = {
                "name": name,
                "lat": lat,
                "lon": lon
            }
    
    with open('store_coords.json', 'w') as f:
        json.dump(stores_coords, f, indent=2)
    print(f"Extracted {len(stores_coords)} store coordinates.")

if __name__ == "__main__":
    extract_store_coords('stores_network.json')
