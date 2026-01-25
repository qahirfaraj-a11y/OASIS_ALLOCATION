from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm

def create_payslip(output_path, data):
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4
    
    # Font setup
    # Standard fonts: Courier, Courier-Bold, Times-Roman, Times-Bold, Helvetica, Helvetica-Bold
    
    y = height - 20*mm
    left_margin = 15*mm
    right_margin = width - 15*mm
    
    # --- Header ---
    c.setFont("Times-Bold", 14)
    c.drawCentredString(width/2, y, "CHANDARANA SUPERMARKET LTD.")
    y -= 20*mm
    
    c.setFont("Times-Roman", 10)
    c.drawCentredString(width/2, y, "CORNERSTONE PLACE - WESTLANDS, NAIROBI,")
    y -= 4*mm
    c.drawCentredString(width/2, y, "KENYA.")
    y -= 8*mm # Increased from 10mm to 15mm for more gap
    
    # Line
    c.line(left_margin, y, right_margin, y)
    y -= 8*mm # Increased from 5mm
    
    # Subheader
    c.setFont("Courier-Bold", 11)
    month_year = data.get("month_year", "October 2025")
    c.drawCentredString(width/2, y, f"Pay Slip - {month_year}")
    y -= 3*mm
    
    # Line
    c.line(left_margin, y, right_margin, y)
    y -= 6*mm
    
    # --- Employee Info ---
    c.setFont("Courier-Bold", 10) # Labels are bold in reference
    emp_info = data.get("employee_info", [])
    
    label_x = left_margin + 2*mm
    colon_x = left_margin + 35*mm
    value_x = left_margin + 38*mm
    
    for label, value in emp_info:
        c.setFont("Courier-Bold", 10)
        c.drawString(label_x, y, label)
        c.drawString(colon_x, y, ":")
        c.setFont("Courier", 10)
        c.drawString(value_x, y, value)
        y -= 4.5*mm
        
    y -= 1.5*mm
    # Line
    c.line(left_margin, y, right_margin, y)
    y -= 5*mm
    
    # --- Financials Headers ---
    c.setFont("Courier-Bold", 10)
    c.drawString(left_margin + 2*mm, y, "Description")
    c.drawRightString(right_margin - 2*mm, y, "Amount")
    y -= 2*mm
    
    c.line(left_margin, y, right_margin, y)
    y -= 5*mm
    
    # --- Financials Body ---
    financials = data.get("financials", [])
    
    def draw_row(description, amount, is_bold=False, is_total=False):
        nonlocal y
        if is_total:
             c.line(left_margin, y+4*mm, right_margin, y+4*mm)
        
        font = "Courier-Bold" if is_bold else "Courier"
        c.setFont(font, 10)
        c.drawString(left_margin + 2*mm, y, description)
        
        c.drawRightString(right_margin - 2*mm, y, f"{amount} KSHS")
        
        y -= 4.5*mm
        
        if is_total: # Add verify double line or single line below? 
             # Reference has line above and below for GROSS and TOTAL DEDUCTIONS
             # And double line for NET PAY
             pass

    # Process items
    # We need a structured list from the input data
    # Basic, Housing -> Gross
    # Deductions -> Total Deductions
    # Tax info
    # Net Pay
    
    # Earnings
    c.setFont("Courier", 10)
    for item in financials.get("earnings", []):
        c.drawString(left_margin + 2*mm, y, item[0])
        c.drawRightString(right_margin - 2*mm, y, f"{item[1]} KSHS")
        y -= 4.5*mm
        
    # Gross Pay
    y -= 1*mm
    c.line(left_margin, y+4.5*mm, right_margin, y+4.5*mm) # Top line
    c.setFont("Courier-Bold", 10)
    c.drawString(left_margin + 2*mm, y, "GROSS PAY")
    c.drawRightString(right_margin - 2*mm, y, f"{financials.get('gross_pay', '0.00')} KSHS")
    c.line(left_margin, y-1.5*mm, right_margin, y-1.5*mm) # Bottom line
    y -= 6*mm
    
    # Deductions
    c.setFont("Courier", 10)
    for item in financials.get("deductions", []):
        c.drawString(left_margin + 2*mm, y, item[0])
        c.drawRightString(right_margin - 2*mm, y, f"{item[1]} KSHS")
        y -= 4.5*mm
        
    # Total Deductions
    y -= 1*mm
    c.line(left_margin, y+4.5*mm, right_margin, y+4.5*mm)
    c.setFont("Courier-Bold", 10)
    c.drawString(left_margin + 2*mm, y, "TOTAL DEDUCTIONS")
    c.drawRightString(right_margin - 2*mm, y, f"{financials.get('total_deductions', '0.00')} KSHS")
    c.line(left_margin, y-1.5*mm, right_margin, y-1.5*mm)
    y -= 6*mm
    
    # Tax Details Block (Reference Match)
    # The reference has a block of details between Total Deductions and Net Pay
    # Logic: It looks like a list similar to deductions but without the header/line separators
    
    tax_details = financials.get("tax_details", [])
    if tax_details:
        y -= 2*mm # Small gap
        c.setFont("Courier", 10)
        for item in tax_details:
            c.drawString(left_margin + 2*mm, y, item[0])
            c.drawRightString(right_margin - 2*mm, y, f"{item[1]} KSHS")
            y -= 4.5*mm
    
    y -= 1*mm
    
    # Net Pay
    y -= 1*mm
    c.line(left_margin, y+4.5*mm, right_margin, y+4.5*mm)
    c.setFont("Courier-Bold", 10)
    c.drawString(left_margin + 2*mm, y, "NET PAY")
    c.drawRightString(right_margin - 2*mm, y, f"{financials.get('net_pay', '0.00')} KSHS")
    # Double line
    c.line(left_margin, y-1.5*mm, right_margin, y-1.5*mm)
    c.line(left_margin, y-2.3*mm, right_margin, y-2.3*mm)
    y -= 15*mm
    
    # Footer
    c.setFont("Courier-Bold", 10)
    c.drawString(left_margin + 2*mm, y, "Signature")
    c.drawRightString(right_margin - 2*mm, y, "Cheque")
    y -= 2*mm
    c.line(left_margin, y, right_margin, y)
    y -= 5*mm
    
    c.setFont("Courier-Bold", 10)
    c.drawCentredString(width/2, y, "This is a computer generated document.")
    
    # Outer Border
    # Start border higher up to enclose the header
    border_top_y = height - 10*mm
    c.rect(left_margin, y - 5*mm, width - 2*left_margin, border_top_y - (y - 5*mm))

    c.save()

import random
import os
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm

def calculate_payroll(gross_pay, staff_contribution=0.0):
    # Fixed Rates & Caps 2025
    housing_levy_rate = 0.015
    shif_rate = 0.0275
    nssf_lower_limit = 8000
    nssf_upper_limit = 72000
    nssf_rate = 0.06
    personal_relief = 2400.0

    # 1. Statutory Deductions
    # Housing Levy
    housing_levy = gross_pay * housing_levy_rate
    
    # SHIF
    shif = gross_pay * shif_rate
    
    # NSSF
    # Tier 1
    tier_1_pensionable = min(gross_pay, nssf_lower_limit)
    nssf_tier_1 = tier_1_pensionable * nssf_rate
    
    # Tier 2
    tier_2_pensionable = 0
    if gross_pay > nssf_lower_limit:
        pensionable_balance = min(gross_pay, nssf_upper_limit) - nssf_lower_limit
        tier_2_pensionable = max(0, pensionable_balance)
    
    nssf_tier_2 = tier_2_pensionable * nssf_rate
    
    nssf_total = nssf_tier_1 + nssf_tier_2
    
    # 2. Tax Calculation
    # Taxable Income = Gross - (NSSF + SHIF + Housing Levy)
    taxable_income = gross_pay - (nssf_total + shif + housing_levy)
    
    # PAYE Bands (Annual / Monthly) - Using Monthly
    # 0 - 24,000: 10%
    # Next 8,333 (24,001 - 32,333): 25%
    # Next 467,667 (32,334 - 500,000): 30%
    # Next 300,000 (500,001 - 800,000): 32.5%
    # Above 800,000: 35%
    
    tax_chargeable = 0.0
    remaining_taxable = taxable_income
    
    # Band 1: First 24,000
    b1 = 24000
    if remaining_taxable > 0:
        taxable_amount = min(remaining_taxable, b1)
        tax_chargeable += taxable_amount * 0.10
        remaining_taxable -= taxable_amount
        
    # Band 2: Next 8,333
    b2 = 8333
    if remaining_taxable > 0:
        taxable_amount = min(remaining_taxable, b2)
        tax_chargeable += taxable_amount * 0.25
        remaining_taxable -= taxable_amount

    # Band 3: Next 467,667
    b3 = 467667
    if remaining_taxable > 0:
        taxable_amount = min(remaining_taxable, b3)
        tax_chargeable += taxable_amount * 0.30
        remaining_taxable -= taxable_amount
        
    # Band 4: Next 300,000
    b4 = 300000
    if remaining_taxable > 0:
        taxable_amount = min(remaining_taxable, b4)
        tax_chargeable += taxable_amount * 0.325
        remaining_taxable -= taxable_amount

    # Band 5: Above
    if remaining_taxable > 0:
        tax_chargeable += remaining_taxable * 0.35
        
    # PAYE
    paye = max(0, tax_chargeable - personal_relief)
    
    # 3. Totals
    # Total Deductions
    total_deductions = nssf_total + shif + housing_levy + paye + staff_contribution
    
    # Net Pay
    net_pay = gross_pay - total_deductions
    
    return {
        "gross_pay": gross_pay,
        "nssf_total": nssf_total,
        "nssf_tier_1": nssf_tier_1,
        "nssf_tier_2": nssf_tier_2,
        "shif": shif,
        "housing_levy": housing_levy,
        "taxable_income": taxable_income,
        "tax_chargeable": tax_chargeable,
        "personal_relief": personal_relief,
        "paye": paye,
        "staff_contribution": staff_contribution,
        "total_deductions": total_deductions,
        "net_pay": net_pay
    }

def format_currency(value):
    return "{:,.2f}".format(value)

def solve_gross_for_net(target_net):
    # Binary search to find Gross Pay that results in Target Net
    low = target_net
    high = target_net * 2.0 # Reasonable upper bound estimate
    tolerance = 0.01
    max_iterations = 100
    
    for _ in range(max_iterations):
        mid = (low + high) / 2
        result = calculate_payroll(mid)
        current_net = result["net_pay"]
        
        if abs(current_net - target_net) < tolerance:
            return mid
        elif current_net < target_net:
            low = mid
        else:
            high = mid
            
    return low # Best effort

if __name__ == "__main__":
    # Constants
    TARGET_NET_PAY = 120000.00
    
    print(f"Calculating required Gross Pay for Net Target: {format_currency(TARGET_NET_PAY)}...")
    GROSS_SALARY_INPUT = solve_gross_for_net(TARGET_NET_PAY)
    print(f"Solved Gross Pay: {format_currency(GROSS_SALARY_INPUT)}")

    # Split Gross into Basic and Housing (approx 85/15 split for realism)
    basic_pay = GROSS_SALARY_INPUT / 1.15
    housing_pay = GROSS_SALARY_INPUT - basic_pay
    
    # Verify exactness
    assert abs((basic_pay + housing_pay) - GROSS_SALARY_INPUT) < 0.01

    base_employee_info = [
        ("Name", "QAHIR MEHOBOOB FARAJ"), 
        ("Payroll No.", "CSL3973"),
        ("Location", "RHAPTA"),
        ("Department", "MANAGERS"),
        ("Designation", "BRANCH MANAGER"),
        ("NSSF No.", "203699700X"),
        ("NHIF No.", "R7281415"),
        ("PIN No.", "A011197027K"),
        ("National Id No", "35801507"),
        
    ]

    months = [
        "January 2025", "February 2025", "March 2025", "April 2025",
        "May 2025", "June 2025", "July 2025", "August 2025",
        "September 2025", "October 2025", "November 2025", "December 2025"
    ]
    random_month_for_contribution = random.choice(months)
    print(f"Generating payslips for {len(months)} months...")

    output_dir = "C:\\Users\\iLink\\.gemini\\antigravity\\scratch\\payslip_generator"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for month in months:
        # Determine Staff Contribution
        staff_contrib = 1000.00 if month == random_month_for_contribution else 0.00
        
        # Calculate Logic
        # Note: We use the FIXED Gross Salary derived from the base Net Target.
        # If there is a staff contribution, the Net Pay for THAT month will naturally drop below the target.
        # This is standard payroll behavior (deductions reduce net).
        res = calculate_payroll(GROSS_SALARY_INPUT, staff_contrib)
        
        # Map to Display Data
        earnings = [
            ("Basic Salary", format_currency(basic_pay)),
            ("Housing Allowance", format_currency(housing_pay)),
        ]
        
        deductions = [
            ("PAYE", format_currency(res["paye"])),
            ("NSSF", format_currency(res["nssf_total"])),
            ("SHIF", format_currency(res["shif"])),
            ("Housing Levy", format_currency(res["housing_levy"])),
        ]
        
        if staff_contrib > 0:
            deductions.append(("Staff Contribution", format_currency(staff_contrib)))
            
        tax_details = [
            ("Contributions(-)", format_currency(res["nssf_total"])), 
            ("Taxable Income", format_currency(res["taxable_income"])),
            ("Tax Chargeable", format_currency(res["tax_chargeable"])),
            ("Relief of Month", format_currency(res["personal_relief"])),
            ("NSSF Tier 1", format_currency(res["nssf_tier_1"])),
            ("NSSF Tier 2", format_currency(res["nssf_tier_2"])),
            ("Housing Levy", format_currency(res["housing_levy"]))
        ]

        payslip_data = {
            "month_year": month,
            "employee_info": base_employee_info,
            "financials": {
                "earnings": earnings,
                "gross_pay": format_currency(res["gross_pay"]),
                "deductions": deductions,
                "total_deductions": format_currency(res["total_deductions"]),
                "tax_details": tax_details,
                "net_pay": format_currency(res["net_pay"])
            }
        }
        
        file_name = f"Payslip_{month.replace(' ', '_')}.pdf"
        output_path = os.path.join(output_dir, file_name)
        create_payslip(output_path, payslip_data)
        print(f"Generated: {output_path}")
