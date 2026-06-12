
import json
import pandas as pd
import os

def analyze_patterns():
    file_path = os.path.join('oasis', 'data', 'sku_grn_frequency.json')
    if not os.path.exists(file_path):
        print("File not found!")
        return

    with open(file_path, 'r') as f:
        data = json.load(f)
        
    df = pd.DataFrame(list(data.items()), columns=['Product', 'Frequency'])
    
    print(f"Total Items: {len(df)}")
    print("\n--- FREQUENCY DISTRIBUTION ---")
    print(df['Frequency'].value_counts().sort_index(ascending=False).head(10))
    
    print("\n--- KEY CATEGORY ANALYSIS ---")
    categories = {
        'MILK': ['MILK', 'MAZIWA'],
        'BREAD': ['BREAD'],
        'YOGHURT': ['YOGHURT', 'YOG'],
        'RICE': ['RICE'],
        'SUGAR': ['SUGAR']
    }
    
    for cat, keywords in categories.items():
        print(f"\n{cat} ITEMS:")
        mask = df['Product'].str.contains('|'.join(keywords), case=False)
        subset = df[mask]
        print(subset['Frequency'].describe())
        print("Sample High Freq:")
        print(subset[subset['Frequency'] >= 0.9].head(5)['Product'].tolist())
        print("Sample Low Freq:")
        print(subset[subset['Frequency'] < 0.5].head(5)['Product'].tolist())

if __name__ == "__main__":
    analyze_patterns()
