import requests
import pandas as pd
import json
import os
import time

def fetch_supermarket_network(brands=["Naivas", "Quickmart", "Carrefour"]):
    """
    Modular pipeline to extract GPS coordinates for retail chains in Kenya via Overpass API.
    """
    overpass_url = "http://overpass-api.de/api/interpreter"
    all_stores = []
    
    # Bounding box for Kenya (approx: S 4.7, N 5.1, W 33.9, E 41.9)
    # We use area code for Kenya admin_level=2
    
    for brand in brands:
        print(f"Querying Overpass API for {brand} locations in Kenya...")
        
        # Query OSM for nodes/ways matching the brand name
        overpass_query = f"""
        [out:json][timeout:60];
        area["ISO3166-1"="KE"][admin_level=2]->.searchArea;
        (
          node["name"~"{brand}", i](area.searchArea);
          way["name"~"{brand}", i](area.searchArea);
        );
        out center;
        """
        
        try:
            response = requests.get(overpass_url, params={'data': overpass_query}, timeout=65)
            response.raise_for_status()
            data = response.json()
            
            count = 0
            for element in data.get('elements', []):
                lat = element.get('lat') or element.get('center', {}).get('lat')
                lon = element.get('lon') or element.get('center', {}).get('lon')
                name = element.get('tags', {}).get('name', f'Unknown {brand}')
                
                if lat and lon:
                    all_stores.append({
                        "Store_Name": name,
                        "Latitude": lat,
                        "Longitude": lon,
                        "Chain": brand,
                        "Source": "OSM_Overpass"
                    })
                    count += 1
            print(f"Successfully extracted {count} nodes for {brand}.")
            
            # Avoid hitting API too fast
            time.sleep(1)
            
        except Exception as e:
            print(f"Error fetching {brand}: {e}")
            # Fallback for Naivas if API fails (based on curated list)
            if brand == "Naivas" and not any(s['Chain'] == 'Naivas' for s in all_stores):
                print("Using curated fallback for Naivas...")
                fallback_naivas = [
                    {"Store_Name": "Naivas Development House", "Latitude": -1.2847, "Longitude": 36.8244, "Chain": "Naivas", "Source": "Fallback"},
                    {"Store_Name": "Naivas Westlands", "Latitude": -1.2646, "Longitude": 36.8045, "Chain": "Naivas", "Source": "Fallback"},
                    {"Store_Name": "Naivas Prestige", "Latitude": -1.3005, "Longitude": 36.7972, "Chain": "Naivas", "Source": "Fallback"},
                    {"Store_Name": "Naivas Gateway Mall", "Latitude": -1.3485, "Longitude": 36.9275, "Chain": "Naivas", "Source": "Fallback"},
                    {"Store_Name": "Naivas Ciata City", "Latitude": -1.2135, "Longitude": 36.8480, "Chain": "Naivas", "Source": "Fallback"},
                    {"Store_Name": "Naivas TRM", "Latitude": -1.2215, "Longitude": 36.8830, "Chain": "Naivas", "Source": "Fallback"},
                    {"Store_Name": "Naivas Nyali", "Latitude": -4.0270, "Longitude": 39.7121, "Chain": "Naivas", "Source": "Fallback"},
                    {"Store_Name": "Naivas Kisumu City", "Latitude": -0.1022, "Longitude": 34.7533, "Chain": "Naivas", "Source": "Fallback"},
                    {"Store_Name": "Naivas Westside Mall", "Latitude": -0.2830, "Longitude": 36.0664, "Chain": "Naivas", "Source": "Fallback"},
                    {"Store_Name": "Naivas Zion Mall", "Latitude": 0.5143, "Longitude": 35.2698, "Chain": "Naivas", "Source": "Fallback"}
                ]
                all_stores.extend(fallback_naivas)

    df = pd.DataFrame(all_stores)
    return df

def run_ingestion(output_file="competitor_network.csv"):
    df = fetch_supermarket_network()
    if not df.empty:
        df.to_csv(output_file, index=False)
        print(f"Dataset saved to {output_file} ({len(df)} total stores)")
    else:
        print("Final dataset is empty. Check internet/API status.")

if __name__ == "__main__":
    run_ingestion()
