import streamlit as st
import pandas as pd
import io
import plotly.express as px
import plotly.graph_objects as go
from pitch_data_ingestor import ProspectDataIngestor
import os

st.set_page_config(page_title="O.A.S.I.S. Pitch Engine", layout="wide", initial_sidebar_state="expanded")

def generate_excel_report(diagnostic_data):
    """Generates an Excel byte stream for download."""
    output = io.BytesIO()
    
    # Extract lists
    dead_stock_df = pd.DataFrame(diagnostic_data.get('dead_stock_list', []))
    stockout_df = pd.DataFrame(diagnostic_data.get('stockout_list', []))
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Formats
        currency_format = workbook.add_format({'num_format': '$#,##0.00'})
        bold_format = workbook.add_format({'bold': True})
        
        # Sheet 1: Executive Summary
        summary_df = pd.DataFrame({
            'Metric': [
                'Total Capital Tied Up (KES)', 
                'Dead Stock Value (Trapped Capital)', 
                'Estimated Lost Revenue (30D)'
            ],
            'Value': [
                diagnostic_data.get('total_capital_tied', 0),
                diagnostic_data.get('dead_stock_value', 0),
                diagnostic_data.get('lost_revenue_opportunity', 0)
            ]
        })
        summary_df.to_excel(writer, sheet_name='Executive Summary', index=False)
        worksheet = writer.sheets['Executive Summary']
        worksheet.set_column('A:A', 35)
        worksheet.set_column('B:B', 20, currency_format)
        
        # Sheet 2: The Kill List (AMIT)
        if not dead_stock_df.empty:
            dead_stock_df = dead_stock_df[['product_name', 'department', 'current_stock', 'unit_cost']]
            dead_stock_df['capital_trapped'] = dead_stock_df['current_stock'] * dead_stock_df['unit_cost']
            dead_stock_df = dead_stock_df.sort_values(by='capital_trapped', ascending=False)
            dead_stock_df.to_excel(writer, sheet_name='Kill List (Dead Stock)', index=False)
            worksheet2 = writer.sheets['Kill List (Dead Stock)']
            worksheet2.set_column('A:B', 30)
            worksheet2.set_column('D:E', 15, currency_format)

        # Sheet 3: The Recovery Plan (DHARAM)
        if not stockout_df.empty:
            stockout_df = stockout_df[['product_name', 'department', 'avg_daily_sales', 'unit_price']]
            stockout_df['est_14d_lost_revenue'] = (stockout_df['avg_daily_sales'] * 14 * stockout_df['unit_price']).round(2)
            stockout_df = stockout_df.sort_values(by='est_14d_lost_revenue', ascending=False)
            stockout_df.to_excel(writer, sheet_name='Recovery Plan (Stockouts)', index=False)
            worksheet3 = writer.sheets['Recovery Plan (Stockouts)']
            worksheet3.set_column('A:B', 30)
            worksheet3.set_column('D:E', 20, currency_format)

    output.seek(0)
    return output

# --- UI Layout ---
st.title("🌐 O.A.S.I.S. Prospect Pitch Engine")
st.markdown("Upload raw prospect data (Sales + Inventory) to instantly calculate trapped capital and lost revenue.")

# Sidebar for Upload
st.sidebar.header("📁 Step 1: Upload Prospect Data")
uploaded_file = st.sidebar.file_uploader("Upload CSV Dump", type=["csv"])

# Use mock data by default for demonstration
use_mock = st.sidebar.button("Use Mock Prospect Data")

file_to_process = None
if uploaded_file is not None:
    file_to_process = uploaded_file
elif use_mock:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_to_process = os.path.join(base_dir, "raw_prospect_data.csv")

if file_to_process is not None:
    # 1. Ingest
    ingestor = ProspectDataIngestor(file_to_process)
    ingestor.sanitize()
    res = ingestor.run_diagnostic_audit()
    
    st.sidebar.success(f"Successfully processed {res.get('total_items', 0)} SKU records.")
    
    # 2. Main Dashboard Display
    st.header("📊 Current State Diagnostic")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Capital on Shelf", f"KES {res['total_capital_tied']:,.0f}")
    col2.metric("Dead Stock (Trapped Capital)", f"KES {res['dead_stock_value']:,.0f}", f"- {res['dead_stock_count']} Fluff Items", delta_color="inverse")
    col3.metric("Lost Revenue (Stockouts)", f"KES {res['lost_revenue_opportunity']:,.0f}", f"{res['stockout_count']} Staples Empty", delta_color="inverse")

    st.divider()

    col_charts1, col_charts2 = st.columns(2)
    
    with col_charts1:
        st.subheader("Capital Utilization Breakdown")
        healthy_capital = res['total_capital_tied'] - res['dead_stock_value']
        fig = px.pie(
            names=['Working (Healthy) Capital', 'Trapped (Dead) Capital'],
            values=[healthy_capital, res['dead_stock_value']],
            hole=0.4,
            color_discrete_sequence=['#2ecc71', '#e74c3c']
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_charts2:
        st.subheader("Top 10 Ghost Demand Stockouts")
        if res['stockout_list']:
            stockout_df = pd.DataFrame(res['stockout_list'])
            stockout_df['Est. Lost Revenue (14D)'] = stockout_df['avg_daily_sales'] * 14 * stockout_df['unit_price']
            top_10 = stockout_df.sort_values('Est. Lost Revenue (14D)', ascending=False).head(10)
            
            fig2 = px.bar(
                top_10, 
                y='product_name', 
                x='Est. Lost Revenue (14D)', 
                orientation='h',
                color_discrete_sequence=['#f39c12']
            )
            fig2.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig2, use_container_width=True)
            
    st.divider()
    
    st.header("🎯 The O.A.S.I.S. Solution")
    st.info("If this prospect switched to O.A.S.I.S., the AMIT Engine would immediately liquidate the trapped capital, and DHARAM would instantly patch the broken safety stocks causing the lost revenue.")
    
    # 3. Excel Export Request
    excel_data = generate_excel_report(res)
    st.download_button(
        label="📥 Download Prospect Excel Audit Report",
        data=excel_data,
        file_name="OASIS_Audit_Report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    
else:
    st.warning("Please upload a CSV file or use the mock data to begin the pitch projection.")
