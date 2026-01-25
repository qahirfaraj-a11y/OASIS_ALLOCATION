@echo off
echo ===================================================
echo Starting Dynamic Inventory Allocation App...
echo ===================================================

:: Navigate to the script directory
cd /d "c:\Users\iLink\.gemini\antigravity\scratch"

:: Check if Streamlit is installed
python -c "import streamlit" 2>NUL
if %errorlevel% neq 0 (
    echo Streamlit is not installed. Installing now...
    pip install streamlit pandas plotly
)

:: Run the App
streamlit run allocation_app.py

pause
