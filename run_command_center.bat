@echo off
title OASIS Command Center
echo.
echo ===========================================
echo   OASIS Operations Command Center
echo   Operations . Allocation . Sales . Simulation
echo ===========================================
echo.

:: Set working directory to script location
cd /d "%~dp0"

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Please install Python 3.10+.
    pause
    exit /b 1
)

:: Check if mock DB exists, build if not
if not exist "oasis\data\mock_pos_erp.db" (
    echo [SETUP] Building mock POS/ERP database...
    python -m oasis.logic.mock_pos_erp
    echo.
)

:: Install deps quietly if needed
echo [CHECK] Verifying dependencies...
pip install streamlit plotly pandas numpy sqlalchemy --quiet >nul 2>&1

:: Launch dashboard
echo.
echo [LAUNCH] Starting OASIS Command Center...
echo          Open your browser to: http://localhost:8501
echo          Press Ctrl+C to stop.
echo.
streamlit run ops_dashboard.py --server.headless true --browser.gatherUsageStats false

pause
