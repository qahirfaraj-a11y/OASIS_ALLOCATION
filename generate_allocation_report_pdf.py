import sys
import os
import pandas as pd
import logging
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet

# Ensure app is in path
sys.path.append(os.getcwd())
from app.logic.order_engine import OrderEngine

SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v2.csv"

def generate_allocation_pdf(budget, output_path):
    # 1. Run Data Logic (Same as simulation)
    df = pd.read_csv(SCORECARD_FILE)
    recommendations = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': 1,
            'moq_floor': 0,
            'recommended_quantity': 0,
            'reasoning': ''
        }
        recommendations.append(rec)
    
    engine = OrderEngine(r"c:\Users\iLink\.gemini\antigravity\scratch")
    final_recs = engine.apply_greenfield_allocation(recommendations, budget)
    
    # Filter for stocked items
    stocked_items = [r for r in final_recs if r['recommended_quantity'] > 0]
    
    # 2. PDF Generation
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4
    y = height - 20*mm
    
    # Header
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(width/2, y, "FIRST OPENING STOCK ALLOCATION")
    y -= 10*mm
    c.setFont("Helvetica", 10)
    c.drawCentredString(width/2, y, "Micro-Duka Optimization Model (Robust Variant)")
    y -= 15*mm
    
    # Summary Table
    total_val = sum([r['recommended_quantity'] * r['selling_price'] * 0.75 for r in stocked_items])
    sku_count = len(stocked_items)
    liquidity = budget - total_val
    
    data = [
        ["Metric", "Value"],
        ["Target Budget", f"KES {budget:,.2f}"],
        ["Allocated Stock (Cost)", f"KES {total_val:,.2f}"],
        ["Retained Liquidity", f"KES {liquidity:,.2f}"],
        ["Unique SKU Width", str(sku_count)],
        ["Strategy", "Robust Variety (3 Brands / Sachet Bias)"]
    ]
    
    table = Table(data, colWidths=[40*mm, 80*mm])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    
    tw, th = table.wrap(width, height)
    table.drawOn(c, (width-tw)/2, y - th)
    y -= (th + 20*mm)
    
    # Top Items Table Header
    c.setFont("Helvetica-Bold", 12)
    c.drawString(20*mm, y, "TOP STOCKING ITEMS (BY VALUE)")
    y -= 8*mm
    
    # Top 15 Items
    top_items = sorted(stocked_items, key=lambda x: x['recommended_quantity'] * x['selling_price'], reverse=True)[:15]
    item_data = [["Department", "Product Title", "Qty", "Cost Est"]]
    for r in top_items:
        cost = r['recommended_quantity'] * r['selling_price'] * 0.75
        item_data.append([
            r['product_category'][:15],
            r['product_name'][:40],
            str(r['recommended_quantity']),
            f"{cost:,.0f}"
        ])
        
    item_table = Table(item_data, colWidths=[35*mm, 100*mm, 15*mm, 25*mm])
    item_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
    ]))
    
    tw, th = item_table.wrap(width, height)
    item_table.drawOn(c, 20*mm, y - th)
    y -= (th + 20*mm)
    
    # Signature Footer
    c.setFont("Helvetica-Bold", 10)
    c.drawString(20*mm, 30*mm, "Branch Manager Signature:")
    c.line(20*mm, 28*mm, 80*mm, 28*mm)
    
    c.drawRightString(width - 20*mm, 30*mm, "Date:")
    c.line(width - 80*mm, 28*mm, width - 20*mm, 28*mm)
    
    # Page Number
    c.setFont("Helvetica", 8)
    c.drawCentredString(width/2, 10*mm, "Confidential - Prepared for CHANDARANA Opening Operations")
    
    c.save()
    print(f"Report Generated: {output_path}")

if __name__ == "__main__":
    generate_allocation_pdf(100000, r"c:\Users\iLink\.gemini\antigravity\scratch\Opening_Stock_Plan_100k.pdf")
