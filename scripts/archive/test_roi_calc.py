import pandas as pd
import numpy as np

def test_roi_calc():
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    
    # 1. Load detail for CP calculation
    detail_path = r"C:\Users\iLink\.gemini\antigravity\scratch\All_Suppliers_Fulfillment_Detail.xlsx"
    df_detail = pd.read_excel(detail_path)
    kapa_detail = df_detail[df_detail['Vendor Name'].astype(str).str.contains('KAPA', case=False, na=False)].copy()
    kapa_detail['Item_Name_upper'] = kapa_detail['Item Name'].astype(str).str.strip().str.upper()
    kapa_detail['calculated_cp'] = kapa_detail['Net Amt'] / kapa_detail['GRN Qty']
    cp_map = kapa_detail.groupby('Item_Name_upper')['calculated_cp'].mean().to_dict()
    
    # Also get item category mapping from detail or node data
    nodes_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\nodes.csv"
    df_nodes = pd.read_csv(nodes_csv)
    df_nodes['id_upper'] = df_nodes['id'].astype(str).str.strip().str.upper()
    
    # Create category map and velocity map from nodes
    node_dept_map = df_nodes.set_index('id_upper')['department'].to_dict()
    node_vel_map = df_nodes.set_index('id_upper')['velocity_ads'].to_dict()
    node_price_map = df_nodes.set_index('id_upper')['price'].to_dict()
    
    # Let's clean the department strings
    clean_dept_map = {}
    for k, v in node_dept_map.items():
        if pd.notna(v):
            clean_dept_map[k] = str(v).replace('[[','').replace(']]','').strip()
            
    # Calculate category margins from GRN-matched items
    dept_margins = {}
    matched_margins = []
    
    for idx, row in df_kapa.iterrows():
        desc = row['DESCRIPTION']
        if pd.isna(desc):
            continue
        desc_upper = str(desc).strip().upper()
        sp = row['SP']
        # Remove commas if SP is string
        if isinstance(sp, str):
            sp = float(sp.replace(',', ''))
        else:
            sp = float(sp)
            
        cp = cp_map.get(desc_upper, None)
        if cp is not None and cp > 0 and sp > 0:
            margin = (sp - cp) / sp
            dept = clean_dept_map.get(desc_upper, 'GENERAL')
            matched_margins.append({'dept': dept, 'margin': margin})
            
    df_mm = pd.DataFrame(matched_margins)
    if not df_mm.empty:
        dept_avg_margins = df_mm.groupby('dept')['margin'].mean().to_dict()
        overall_avg_margin = df_mm['margin'].mean()
    else:
        dept_avg_margins = {}
        overall_avg_margin = 0.15 # default 15%
        
    print(f"Overall average margin from GRN matched: {overall_avg_margin * 100:.2f}%")
    print("Department margins:")
    for k, v in dept_avg_margins.items():
        print(f"  {k}: {v*100:.2f}%")
        
    # Calculate final ROI for each item
    results = []
    for idx, row in df_kapa.iterrows():
        desc = row['DESCRIPTION']
        if pd.isna(desc):
            continue
        desc_upper = str(desc).strip().upper()
        sp = row['SP']
        if isinstance(sp, str):
            sp = float(sp.replace(',', ''))
        else:
            sp = float(sp)
            
        # Get CP
        cp = cp_map.get(desc_upper, None)
        is_imputed_cp = False
        if cp is None or cp <= 0:
            dept = clean_dept_map.get(desc_upper, 'GENERAL')
            margin = dept_avg_margins.get(dept, overall_avg_margin)
            cp = sp * (1 - margin)
            is_imputed_cp = True
            
        # Get daily velocity
        ads = node_vel_map.get(desc_upper, 0.0)
        
        # Calculate ROI
        # T = 13.0 days, L = 5.0 days
        # Average inventory = (T/2 + L) * ADS = 11.5 * ADS
        # ROI_30D = 30 * ADS * (SP - CP) / (11.5 * ADS * CP) * 100
        # If ADS is 0, ROI is 0
        if ads > 0:
            roi_30d = (30 * (sp - cp)) / (11.5 * cp) * 100
        else:
            roi_30d = 0.0
            
        results.append({
            'DESCRIPTION': desc,
            'SP': sp,
            'CP': cp,
            'ADS': ads,
            'is_imputed_cp': is_imputed_cp,
            'ROI_30D': roi_30d
        })
        
    df_res = pd.DataFrame(results)
    print("\n=== Samples of calculated ROIs ===")
    print(df_res.head(20).to_string())
    print("\nTotal non-zero ROIs:", len(df_res[df_res['ROI_30D'] > 0]))
    print("Max ROI:", df_res['ROI_30D'].max())
    print("Min ROI:", df_res['ROI_30D'].min())
    print("Mean ROI:", df_res['ROI_30D'].mean())

test_roi_calc()
