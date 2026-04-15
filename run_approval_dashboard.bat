@echo off
setlocal enabledelayedexpansion
title O.A.S.I.S. PO Approval Center
cd /d "%~dp0"
echo ========================================================
echo Starting O.A.S.I.S. Purchase Order Approval Center
echo ========================================================
echo Running Phases 4-6: Daily Procurement Workflow
echo Close this window to stop the server.
echo.

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
)

:: Port Hunting (default 8504)
set PORT=8504
:findport
netstat -ano | findstr :!PORT! >nul
if !errorlevel! equ 0 (
    echo [PORT] !PORT! is busy. Hunting for next open port...
    set /a PORT+=1
    goto findport
)

:: Supervisor Loop
:RESTART
echo [%DATE% %TIME%] [START] Launching Approval Center at http://localhost:!PORT!
"%ST_EXEC%" run approval_dashboard.py --server.port !PORT! --server.headless true --browser.gatherUsageStats false

if %errorlevel% neq 0 (
    echo.
    echo [ALERT] Approval Dashboard crashed with exit code %errorlevel%
    echo [%DATE% %TIME%] Auto-recovering in 3 seconds...
    timeout /t 3 >nul
    goto RESTART
)

echo [%DATE% %TIME%] [STOP] Approval Center closed gracefully.
pause
