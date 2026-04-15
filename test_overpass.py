import requests
import pandas as pd
import json

def fetch_supermarket_network(brand_name="Naivas"):
    print(f"Querying Overpass API for {brand_name} locations in Kenya...")
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # Query OSM for any node or way matching the brand name in Kenya
    overpass_query = f"""
    [out:json];
    area["ISO3166-1"="KE"][admin_level=2]->.searchArea;
    (
      node["name"~"{brand_name}", i](area.searchArea);
      way["name"~"{brand_name}", i](area.searchArea);
    );
    out center;
    """
    
    try:
        response = requests.get(overpass_url, params={'data': overpass_query}, timeout=30)
        data = response.json()
        
        store_list = []
        
        for element in data['elements']:
            lat = element.get('lat') or element.get('center', {}).get('lat')
            lon = element.get('lon') or element.get('center', {}).get('lon')
            name = element.get('tags', {}).get('name', f'Unknown {brand_name}')
            
            if lat and lon:
                store_list.append({
                    "Store_Name": name,
                    "Latitude": lat,
                    "Longitude": lon,
                    "Chain": brand_name
                })
                
        df = pd.DataFrame(store_list)
        print(f"Successfully extracted {len(df)} nodes.")
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    df = fetch_supermarket_network("Naivas")
    if not df.empty:
        df.to_csv("naivas_complete_network.csv", index=False)
        print("Saved to naivas_complete_network.csv")
    else:
        print("No data fetched.")
