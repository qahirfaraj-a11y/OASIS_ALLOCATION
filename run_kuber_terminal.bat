@echo off
echo ===================================================
echo   KUBER RETAIL EXCHANGE: FINANCIAL TERMINAL
echo ===================================================

:: Navigate to the script directory
cd /d "%~dp0"

echo.
echo Checking for Streamlit...
python -c "import streamlit" 2>NUL
if %errorlevel% neq 0 (
    echo Streamlit not found. Installing dependencies...
    pip install streamlit plotly pandas
)

echo.
echo Launching Terminal Dashboard...
streamlit run kuber_terminal.py

if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Terminal failed to start.
    echo Trying fallback: python -m streamlit run kuber_terminal.py
    python -m streamlit run kuber_terminal.py
)

pause
