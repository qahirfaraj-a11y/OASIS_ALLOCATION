@echo off
echo ===================================================
echo Starting ST-GAT Market Pulse Dashboard...
echo ===================================================

:: Navigate to the script directory
cd /d "%~dp0"

echo.
echo Checking for Streamlit...
python -c "import streamlit" 2>NUL
if %errorlevel% neq 0 (
    echo Streamlit not found. Installing...
    pip install streamlit plotly pydeck pandas torch
)

echo.
echo Launching Dashboard...
streamlit run st_gat_dashboard.py

pause
