@echo off
setlocal enabledelayedexpansion
title O.A.S.I.S. ST-GAT Market Pulse
cd /d "%~dp0"
echo ===================================================
echo Starting ST-GAT Market Pulse Dashboard...
echo ===================================================
echo.

:: Fix Windows charmap encoding errors for Python print statements
set PYTHONUTF8=1

:: VENV Detection
if exist ".oasis_venv\Scripts\activate.bat" (
    set PYTHON_EXEC=.oasis_venv\Scripts\python.exe
    set ST_EXEC=.oasis_venv\Scripts\streamlit.exe
    call .\.oasis_venv\Scripts\activate.bat
    echo [INFO] Environment: .oasis_venv detected.
) else (
    set PYTHON_EXEC=python
    set ST_EXEC=streamlit
    echo [WARNING] No virtual environment found. Running with global python.
    echo Checking for Streamlit...
    python -c "import streamlit" 2>NUL
    if !errorlevel! neq 0 (
        echo Streamlit not found. Installing...
        pip install streamlit plotly pydeck pandas torch
    )
)

:: Port Hunting (default 8502, but checks upward if busy)
set PORT=8502
:findport
netstat -ano | findstr :!PORT! >nul
if !errorlevel! equ 0 (
    echo [PORT] !PORT! is busy. Hunting for next open port...
    set /a PORT+=1
    goto findport
)

:: Supervisor Loop
:RESTART
echo [%DATE% %TIME%] [START] Launching ST-GAT Dashboard at http://localhost:!PORT!
"%ST_EXEC%" run st_gat_dashboard.py --server.port !PORT! --server.headless true --browser.gatherUsageStats false

if %errorlevel% neq 0 (
    echo.
    echo [ALERT] ST-GAT Dashboard crashed with exit code %errorlevel%
    echo [%DATE% %TIME%] Auto-recovering in 3 seconds...
    timeout /t 3 >nul
    goto RESTART
)

echo [%DATE% %TIME%] [STOP] Dashboard closed gracefully.
pause
