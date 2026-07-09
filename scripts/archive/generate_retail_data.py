import csv
import random
from datetime import datetime, timedelta

# Configuration
ORG_ID = "ORG001"
OUTPUT_DIR = r"C:\Oasis\inbound_drops\bootstrap"
TARGET_CAPITAL = 1000000
NUM_DAYS = 30
END_DATE = datetime(2026, 5, 10)
START_DATE = END_DATE - timedelta(days=NUM_DAYS-1)

# SKU Master Data (Base Templates)
SKU_TEMPLATES = [
    {"Item_Name": "TUSKER LAGER 500ML", "Department": "BEER", "Supplier": "EABL", "Unit_Cost": 165.0, "Selling_Price": 210.0, "Pack_Size": 25, "Lead_Time": 3, "ADS": 15.0},
    {"Item_Name": "GUINNESS STOUT 500ML", "Department": "BEER", "Supplier": "EABL", "Unit_Cost": 185.0, "Selling_Price": 240.0, "Pack_Size": 25, "Lead_Time": 3, "ADS": 8.0},
    {"Item_Name": "WHITE CAP LAGER 500ML", "Department": "BEER", "Supplier": "EABL", "Unit_Cost": 170.0, "Selling_Price": 220.0, "Pack_Size": 25, "Lead_Time": 3, "ADS": 10.0},
    {"Item_Name": "BROADWAYS BREAD 400G", "Department": "BREAD", "Supplier": "BROADWAYS", "Unit_Cost": 55.0, "Selling_Price": 65.0, "Pack_Size": 1, "Lead_Time": 0, "ADS": 45.0},
    {"Item_Name": "SUPA LOAF 400G", "Department": "BREAD", "Supplier": "SUPA LOAF", "Unit_Cost": 55.0, "Selling_Price": 65.0, "Pack_Size": 1, "Lead_Time": 0, "ADS": 40.0},
    {"Item_Name": "JOGOO MAIZE FLOUR 2KG", "Department": "MAIZE FLOUR", "Supplier": "UNGA LTD", "Unit_Cost": 175.0, "Selling_Price": 205.0, "Pack_Size": 12, "Lead_Time": 2, "ADS": 20.0},
    {"Item_Name": "UNGA HOSTESS 2KG", "Department": "MAIZE FLOUR", "Supplier": "UNGA LTD", "Unit_Cost": 195.0, "Selling_Price": 230.0, "Pack_Size": 12, "Lead_Time": 2, "ADS": 8.0},
    {"Item_Name": "PEMBE MAIZE FLOUR 2KG", "Department": "MAIZE FLOUR", "Supplier": "PEMBE", "Unit_Cost": 170.0, "Selling_Price": 195.0, "Pack_Size": 12, "Lead_Time": 2, "ADS": 18.0},
    {"Item_Name": "EXE WHEAT FLOUR 2KG", "Department": "WHEAT FLOUR", "Supplier": "UNGA LTD", "Unit_Cost": 185.0, "Selling_Price": 215.0, "Pack_Size": 12, "Lead_Time": 2, "ADS": 12.0},
    {"Item_Name": "KAPA KASUKU 2KG", "Department": "COOKING FAT", "Supplier": "KAPA", "Unit_Cost": 385.0, "Selling_Price": 450.0, "Pack_Size": 6, "Lead_Time": 5, "ADS": 5.0},
    {"Item_Name": "COWBOY FAT 1KG", "Department": "COOKING FAT", "Supplier": "BIDCO", "Unit_Cost": 210.0, "Selling_Price": 250.0, "Pack_Size": 12, "Lead_Time": 4, "ADS": 4.0},
    {"Item_Name": "GOLDEN FRY 1L", "Department": "COOKING OIL", "Supplier": "BIDCO", "Unit_Cost": 245.0, "Selling_Price": 290.0, "Pack_Size": 12, "Lead_Time": 4, "ADS": 10.0},
    {"Item_Name": "FRESH FRI 1L", "Department": "COOKING OIL", "Supplier": "Pwani Oil", "Unit_Cost": 250.0, "Selling_Price": 300.0, "Pack_Size": 12, "Lead_Time": 4, "ADS": 9.0},
    {"Item_Name": "WHITE STAR BAR SOAP 800G", "Department": "SOAPS", "Supplier": "PZ CUSSONS", "Unit_Cost": 145.0, "Selling_Price": 180.0, "Pack_Size": 25, "Lead_Time": 7, "ADS": 6.0},
    {"Item_Name": "MENENGAI BAR SOAP 800G", "Department": "SOAPS", "Supplier": "MENENGAI", "Unit_Cost": 130.0, "Selling_Price": 160.0, "Pack_Size": 25, "Lead_Time": 5, "ADS": 12.0},
    {"Item_Name": "ARIELL POWDER 500G", "Department": "DETERGENTS", "Supplier": "P&G", "Unit_Cost": 185.0, "Selling_Price": 220.0, "Pack_Size": 20, "Lead_Time": 10, "ADS": 3.0},
    {"Item_Name": "OMO POWDER 500G", "Department": "DETERGENTS", "Supplier": "UNILEVER", "Unit_Cost": 190.0, "Selling_Price": 230.0, "Pack_Size": 20, "Lead_Time": 5, "ADS": 5.0},
    {"Item_Name": "KABRAS SUGAR 1KG", "Department": "SUGAR", "Supplier": "WEST KENYA", "Unit_Cost": 140.0, "Selling_Price": 170.0, "Pack_Size": 20, "Lead_Time": 3, "ADS": 25.0},
    {"Item_Name": "MUMIAS SUGAR 1KG", "Department": "SUGAR", "Supplier": "MUMIAS", "Unit_Cost": 145.0, "Selling_Price": 175.0, "Pack_Size": 20, "Lead_Time": 5, "ADS": 15.0},
    {"Item_Name": "COCA COLA 500ML", "Department": "SODA", "Supplier": "COCA COLA", "Unit_Cost": 55.0, "Selling_Price": 70.0, "Pack_Size": 24, "Lead_Time": 2, "ADS": 50.0},
    {"Item_Name": "FANTA ORANGE 500ML", "Department": "SODA", "Supplier": "COCA COLA", "Unit_Cost": 55.0, "Selling_Price": 70.0, "Pack_Size": 24, "Lead_Time": 2, "ADS": 30.0},
    {"Item_Name": "SPRITE 500ML", "Department": "SODA", "Supplier": "COCA COLA", "Unit_Cost": 55.0, "Selling_Price": 70.0, "Pack_Size": 24, "Lead_Time": 2, "ADS": 20.0},
    {"Item_Name": "BROOKSIDE MILK 500ML", "Department": "MILK", "Supplier": "BROOKSIDE", "Unit_Cost": 52.0, "Selling_Price": 65.0, "Pack_Size": 12, "Lead_Time": 1, "ADS": 60.0},
    {"Item_Name": "KCC MILK 500ML", "Department": "MILK", "Supplier": "KCC", "Unit_Cost": 50.0, "Selling_Price": 60.0, "Pack_Size": 12, "Lead_Time": 1, "ADS": 55.0},
    {"Item_Name": "KETEPA TEA 250G", "Department": "TEA", "Supplier": "KETEPA", "Unit_Cost": 180.0, "Selling_Price": 220.0, "Pack_Size": 20, "Lead_Time": 4, "ADS": 4.0},
    {"Item_Name": "NESCAFE 50G", "Department": "COFFEE", "Supplier": "NESTLE", "Unit_Cost": 280.0, "Selling_Price": 350.0, "Pack_Size": 10, "Lead_Time": 7, "ADS": 2.0},
    {"Item_Name": "BLUEBAND 500G", "Department": "SPREADS", "Supplier": "UPFIELD", "Unit_Cost": 240.0, "Selling_Price": 290.0, "Pack_Size": 12, "Lead_Time": 3, "ADS": 7.0},
    {"Item_Name": "KIMBO FAT 1KG", "Department": "COOKING FAT", "Supplier": "BIDCO", "Unit_Cost": 205.0, "Selling_Price": 245.0, "Pack_Size": 12, "Lead_Time": 4, "ADS": 3.0},
    {"Item_Name": "TROPICALS SWEETS PKT", "Department": "SNACKS", "Supplier": "KENAFRIC", "Unit_Cost": 120.0, "Selling_Price": 150.0, "Pack_Size": 1, "Lead_Time": 5, "ADS": 5.0},
    {"Item_Name": "KRACKLES POTATO CHIPS", "Department": "SNACKS", "Supplier": "NORDA", "Unit_Cost": 40.0, "Selling_Price": 50.0, "Pack_Size": 20, "Lead_Time": 3, "ADS": 15.0},
]

# Expand to ~50 SKUs by adding variants or more brands if needed, 
# but 30 high-volume ones is a good start. I'll double some with "SMALL" / "LARGE" variants.
EXTENDED_SKUS = []
for template in SKU_TEMPLATES:
    EXTENDED_SKUS.append(template)
    # Add a variation for some
    if template["Department"] in ["DETERGENTS", "SOAPS", "COOKING OIL"]:
        variant = template.copy()
        variant["Item_Name"] = variant["Item_Name"].replace("500G", "1KG").replace("1L", "3L").replace("800G", "1.5KG")
        variant["Unit_Cost"] *= 1.8
        variant["Selling_Price"] *= 1.8
        variant["ADS"] *= 0.4
        EXTENDED_SKUS.append(variant)

# Assign random barcodes
for i, sku in enumerate(EXTENDED_SKUS):
    sku["Barcode"] = f"616110{100000 + i}"

# Scale SOH to reach ~1,000,000 capital
total_base_cost = sum(s["Unit_Cost"] * (s["ADS"] * 10) for s in EXTENDED_SKUS)
scaling_factor = TARGET_CAPITAL / total_base_cost

for sku in EXTENDED_SKUS:
    # Default: ~10 days of cover
    base_soh = int(sku["ADS"] * 10 * scaling_factor)
    
    # INJECT SINS:
    # 1. Dead Stock (Capital Trap): Detergents and Soaps with massive overstock
    if sku["Department"] in ["DETERGENTS", "SOAPS"] and "1KG" in sku["Item_Name"]:
        sku["SOH"] = int(sku["ADS"] * 150) # 150 days of cover
    # 2. Ghost Demand (Stockouts): High-velocity items set to 0
    elif sku["Item_Name"] in ["TUSKER LAGER 500ML", "BROOKSIDE MILK 500ML", "COCA COLA 500ML"]:
        sku["SOH"] = 0
    else:
        sku["SOH"] = base_soh

# Generate Sales Data
sales_rows = []
txn_id_counter = 10001
for day_offset in range(NUM_DAYS):
    current_date = (START_DATE + timedelta(days=day_offset)).strftime("%Y-%m-%d")
    for sku in EXTENDED_SKUS:
        # Randomize daily sales around ADS
        qty = max(0, int(random.normalvariate(sku["ADS"], sku["ADS"] * 0.2)))
        if qty > 0:
            # Group into transactions
            num_txns = max(1, qty // 2)
            for _ in range(num_txns):
                txn_qty = 1 if num_txns > qty else (qty // num_txns)
                if txn_qty == 0: txn_qty = 1
                sales_rows.append({
                    "Date": current_date,
                    "Item_Name": sku["Item_Name"],
                    "Qty_Sold": txn_qty,
                    "Unit_Price_KES": sku["Selling_Price"],
                    "Unit_Cost_KES": sku["Unit_Cost"],
                    "Transaction_ID": f"TXN-{txn_id_counter}",
                    "Barcode": sku["Barcode"]
                })
                txn_id_counter += 1

# Generate GRN Data
grn_rows = []
po_counter = 9001
# Suppliers replenish when stock would drop below a threshold (e.g. 3 days of ADS)
# For this simulation, we'll just do weekly drops for simplicity
for day_offset in range(0, NUM_DAYS, 7):
    order_date = (START_DATE + timedelta(days=day_offset - 3)).strftime("%Y-%m-%d")
    received_date = (START_DATE + timedelta(days=day_offset)).strftime("%Y-%m-%d")
    for sku in EXTENDED_SKUS:
        # Order enough to last 10 days
        ordered_qty = int(sku["ADS"] * 10)
        
        # 3. Supplier Toxicity (LATA): Make EABL and BIDCO "Hostile/Criminal"
        received_qty = ordered_qty
        if sku["Supplier"] == "EABL":
            received_qty = int(ordered_qty * random.uniform(0.3, 0.6)) # Heavy shortages
        elif sku["Supplier"] == "BIDCO":
            received_qty = int(ordered_qty * random.uniform(0.7, 0.9)) # Moderate shortages
            
        grn_rows.append({
            "Order_Date": order_date,
            "Received_Date": received_date,
            "Supplier_Name": sku["Supplier"],
            "Item_Name": sku["Item_Name"],
            "Ordered_Qty": ordered_qty,
            "Received_Qty": received_qty,
            "PO_Number": f"PO-{po_counter}",
            "Barcode": sku["Barcode"]
        })
        po_counter += 1

# Write Files
def write_csv(filename, fieldnames, rows):
    path = f"{OUTPUT_DIR}\\{filename}"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

# Stock/ORG files
stock_fieldnames = ["Item_Name", "SOH", "ADS", "Unit_Cost", "Barcode", "Department", "Supplier", "Lead_Time", "Pack_Size", "Selling_Price"]
write_csv(f"{ORG_ID}_stock.csv", stock_fieldnames, EXTENDED_SKUS)
write_csv(f"{ORG_ID}.csv", stock_fieldnames, EXTENDED_SKUS)

# Sales
sales_fieldnames = ["Date", "Item_Name", "Qty_Sold", "Unit_Price_KES", "Unit_Cost_KES", "Transaction_ID", "Barcode"]
write_csv(f"{ORG_ID}_sales.csv", sales_fieldnames, sales_rows)

# GRN
grn_fieldnames = ["Order_Date", "Received_Date", "Supplier_Name", "Item_Name", "Ordered_Qty", "Received_Qty", "PO_Number", "Barcode"]
write_csv(f"{ORG_ID}_grn.csv", grn_fieldnames, grn_rows)

print(f"Generated {len(EXTENDED_SKUS)} SKUs.")
print(f"Total Capital: {sum(s['SOH'] * s['Unit_Cost'] for s in EXTENDED_SKUS):,.2f} KES")
print(f"Total Sales Records: {len(sales_rows)}")
print(f"Total GRN Records: {len(grn_rows)}")
