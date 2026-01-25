
import os
import calendar
from datetime import datetime, date, timedelta
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Frame, PageTemplate
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from app.logic.calendar_analyzer import CalendarAnalyzer

def generate_calendar_pdf(output_path: str, year: int = 2026):
    """Generate the Supplier Order Calendar PDF in Master Schedule Format."""
    
    # 1. Get Data
    analyzer = CalendarAnalyzer()
    analyzer.load_data()
    supplier_data = analyzer.analyze()
    
    # Filter active suppliers
    active_suppliers = {k: v for k, v in supplier_data.items() if v['order_count'] >= 3}
    
    # Categorize
    daily_suppliers = []
    weekly_suppliers = {i: [] for i in range(7)} # 0=Mon, 6=Sun
    monthly_suppliers = {i: [] for i in range(1, 32)} # 1-31
    irregular_suppliers = []
    
    for name, data in active_suppliers.items():
        cat = data['category']
        if cat == 'Daily':
            daily_suppliers.append((name, data['lead_time_days']))
        elif cat == 'Weekly':
            day = data['preferred_day']
            weekly_suppliers[day].append(name)
        elif cat == 'Monthly':
            day = data['preferred_day']
            monthly_suppliers[day].append(name)
        else:
            irregular_suppliers.append((name, data['category']))
            
    # Sort lists
    daily_suppliers.sort(key=lambda x: x[0])
    for d in weekly_suppliers: weekly_suppliers[d].sort()
    for d in monthly_suppliers: monthly_suppliers[d].sort()
    
    # 2. Setup PDF
    doc = SimpleDocTemplate(
        output_path, 
        pagesize=landscape(A4),
        rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30
    )
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = styles['Title']
    title_style.fontSize = 24
    title_style.textColor = colors.HexColor("#2C3E50")
    
    h1_style = ParagraphStyle('H1', parent=styles['Heading1'], fontSize=16, spaceAfter=12, textColor=colors.HexColor("#34495E"))
    h2_style = ParagraphStyle('H2', parent=styles['Heading2'], fontSize=12, spaceAfter=6, textColor=colors.HexColor("#7F8C8D"))
    
    normal_style = styles['Normal']
    
    # --- PAGE 1: WEEKLY ROUTINE ---
    elements.append(Paragraph(f"Supplier Master Schedule {year}", title_style))
    elements.append(Paragraph("WEEKLY ROUTINE (Recurring Orders)", h1_style))
    elements.append(Spacer(1, 10))
    
    # Weekly Data - Use Flowables (Paragraphs) instead of Table to allow page splitting
    week_headers = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']
    
    for day_idx, day_name in enumerate(week_headers):
        sups = weekly_suppliers[day_idx]
        if not sups: continue
        
        # Day Header
        elements.append(Paragraph(day_name, h2_style))
        
        # Suppliers List (Comma separated for flow)
        # Clean names slightly to save space
        clean_sups = [s.replace('LIMITED', '').replace('LTD', '').strip() for s in sups]
        content = ", ".join(clean_sups)
        elements.append(Paragraph(content, normal_style))
        elements.append(Spacer(1, 8))
    
    elements.append(PageBreak())

    
    # --- PAGE 2: MONTHLY ROUTINE ---
    elements.append(Paragraph("MONTHLY ROUTINE (Date Specific)", h1_style))
    elements.append(Spacer(1, 10))
    
    # Monthly Data - Linear List for safety
    # Check if we have data
    total_monthly = sum(len(ml) for ml in monthly_suppliers.values())
    if total_monthly == 0:
        elements.append(Paragraph("No monthly patterns detected.", normal_style))
    else:
        for day in range(1, 32):
            sups = monthly_suppliers[day]
            if not sups: continue
            
            day_str = f"{day}{get_ordinal(day)}"
            
            # Header: Day
            elements.append(Paragraph(f"Date: {day_str}", h2_style))
            
            # Content
            # Clean names
            clean_sups = [s.replace('LIMITED', '').replace('LTD', '').strip() for s in sups]
            content = ", ".join(clean_sups)
            elements.append(Paragraph(content, normal_style))
            elements.append(Spacer(1, 6))
            
    elements.append(PageBreak())
    
    # --- PAGE 3: DAILY & IRREGULAR ---
    elements.append(Paragraph("DAILY & IRREGULAR ORDERS", h1_style))
    elements.append(Spacer(1, 10))
    
    # Daily Table
    elements.append(Paragraph("Daily Suppliers (High Frequency)", h2_style))
    
    daily_data_rows = [['Supplier Name', 'Avg Lead Time']]
    for name, lt in daily_suppliers:
        daily_data_rows.append([name, f"{lt} days"])
        
    # Split daily into 3 columns if long
    # Actually just 3 cols: Supplier, Lead Time | Supplier, Lead Time | Supplier, Lead Time
    
    # Let's simple format: Grid of boxes
    
    # ... Or simply a nice table
    t_daily = Table(daily_data_rows, colWidths=[300, 100])
    t_daily.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#E67E22")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('GRID', (0,0), (-1,-1), 1, colors.HexColor("#BDC3C7")),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.whitesmoke, colors.white]),
    ]))
    elements.append(t_daily)
    elements.append(Spacer(1, 20))
    
    # Irregular Table
    if irregular_suppliers:
        elements.append(Paragraph("Bi-Weekly / Irregular Pattern", h2_style))
        irr_data = [['Supplier Name', 'Detected Pattern']]
        for name, cat in irregular_suppliers:
            irr_data.append([name, cat])
            
        t_irr = Table(irr_data, colWidths=[300, 150])
        t_irr.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#7F8C8D")),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('GRID', (0,0), (-1,-1), 1, colors.HexColor("#BDC3C7")),
        ]))
        elements.append(t_irr)

    # Build PDF
    doc.build(elements)
    print(f"Master Schedule generated at: {output_path}")

def get_ordinal(n):
    if 11 <= (n % 100) <= 13: suffix = 'th'
    else: suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')
    return suffix

if __name__ == "__main__":
    out_file = os.path.join(os.getcwd(), f"Supplier_Master_Schedule_V2_2026.pdf")
    generate_calendar_pdf(out_file)
