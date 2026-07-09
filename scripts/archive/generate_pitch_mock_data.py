import pandas as pd
import numpy as np
import datetime
import os
import random

# Load the scorecard
scorecard_path = "Full_Product_Allocation_Scorecard_v7.csv"
if not os.path.exists(scorecard_path):
    scorecard_path = "Full_Product_Allocation_Scorecard.csv"

df = pd.read_csv(scorecard_path)

# Normalize columns
if "Item_Name" not in df.columns and "Product" in df.columns:
    df = df.rename(columns={"Product": "Item_Name"})
if "ADS" not in df.columns and "Avg_Daily_Sales" in df.columns:
    df = df.rename(columns={"Avg_Daily_Sales": "ADS"})
if "Unit_Price" not in df.columns and "Selling_Price" in df.columns:
    df = df.rename(columns={"Selling_Price": "Unit_Price"})

# Sample 40 products
df_sample = df.sample(n=min(40, len(df)), random_state=7).copy()
if "Barcode" not in df_sample.columns:
    df_sample["Barcode"] = [f"800{i:04d}" for i in range(len(df_sample))]
if "Supplier" not in df_sample.columns and "Supplier_Name" in df_sample.columns:
    df_sample = df_sample.rename(columns={"Supplier_Name": "Supplier"})
if "Supplier" not in df_sample.columns:
    df_sample["Supplier"] = ["Supplier_A", "Supplier_B", "Supplier_C"] * (len(df_sample)//3) + ["Supplier_A"] * (len(df_sample)%3)

# 90 Day Window
end_date = datetime.date.today()
start_date = end_date - datetime.timedelta(days=90)
date_range = [start_date + datetime.timedelta(days=i) for i in range(90)]

# --- 1. POS DATA (.csv) ---
pos_records = []
txn_id = 10000
for _, row in df_sample.iterrows():
    ads = float(row.get("ADS", 2.0))
    price = float(row.get("Unit_Price", 150.0))
    cost = price * 0.8
    for d in date_range:
        qty = np.random.poisson(ads)
        if qty > 0:
            pos_records.append({
                "Date": d.strftime("%Y-%m-%d"),
                "Transaction_ID": f"TXN-{txn_id}",
                "Barcode": row["Barcode"],
                "Item_Name": row["Item_Name"],
                "Qty_Sold": qty,
                "Unit_Price_KES": price,
                "Unit_Cost_KES": cost
            })
            txn_id += 1
pos_df = pd.DataFrame(pos_records)

# --- 2. GRN DATA (.xlsx) ---
grn_records = []
po_id = 5000
for supplier, group in df_sample.groupby("Supplier"):
    for day_offset in range(0, 90, 14):
        order_date = start_date + datetime.timedelta(days=day_offset)
        receive_date = order_date + datetime.timedelta(days=random.randint(2, 7))
        po_str = f"PO-{po_id}"
        for _, row in group.iterrows():
            ordered = int(row.get("ADS", 2) * 14)
            received = ordered if random.random() > 0.1 else int(ordered * 0.8)
            grn_records.append({
                "PO_Number": po_str,
                "Supplier_Name": supplier,
                "Item_Name": row["Item_Name"],
                "Order_Date": order_date.strftime("%Y-%m-%d"),
                "Received_Date": receive_date.strftime("%Y-%m-%d"),
                "Ordered_Qty": ordered,
                "Received_Qty": received
            })
        po_id += 1
grn_df = pd.DataFrame(grn_records)

# --- 3. SHRINK DATA (.csv - fallback for .xls) ---
shrink_records = []
reasons = ["Expired", "Damaged", "Short Supply", "Missing"]
for _, row in df_sample.iterrows():
    if random.random() < 0.2:
        qty = random.randint(1, 3)
        shrink_records.append({
            "Date": (start_date + datetime.timedelta(days=random.randint(0, 89))).strftime("%Y-%m-%d"),
            "Item_Name": row["Item_Name"],
            "Qty_Adjusted": qty,
            "Reason": random.choice(reasons),
            "Cost_Value": float(row.get("Unit_Price", 150)) * 0.8 * qty
        })
shrink_df = pd.DataFrame(shrink_records)

# --- 4. TRANSFERS DATA (.json) ---
transfer_records = []
branches = ["Westlands", "Karen", "Kilimani", "CBD"]
for _, row in df_sample.iterrows():
    if random.random() < 0.15:
        qty = random.randint(2, 10)
        transfer_records.append({
            "Date": (start_date + datetime.timedelta(days=random.randint(0, 89))).strftime("%Y-%m-%d"),
            "Item_Name": row["Item_Name"],
            "From_Branch": random.choice(branches),
            "To_Branch": random.choice(branches),
            "Qty_Transferred": qty,
            "Cost_Value": float(row.get("Unit_Price", 150)) * 0.8 * qty
        })
transfer_df = pd.DataFrame(transfer_records)

# OUTPUT
out_dir = "Pitch_Mock_Data_v2"
os.makedirs(out_dir, exist_ok=True)
pos_df.to_csv(os.path.join(out_dir, "POS_Log_90D.csv"), index=False)
grn_df.to_excel(os.path.join(out_dir, "GRN_Log_90D.xlsx"), index=False)
shrink_df.to_csv(os.path.join(out_dir, "Shrink_Log_90D.csv"), index=False) # Labeled as CSV but represents the data
transfer_df.to_json(os.path.join(out_dir, "Transfers_Log_90D.json"), orient="records", indent=4)

print(f"Mock data generated in {out_dir}")
