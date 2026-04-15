@echo off
setlocal enabledelayedexpansion
title O.A.S.I.S. Retail Lifecycle [Integration Prototype]
cd /d "%~dp0"
echo ===================================================
echo   Oasis Retail Lifecycle (Integration Prototype)
echo ===================================================
echo Starting Application...
echo.

:: VENV Detection
if exist ".oasis_venv\Scripts\activate.bat" (
    set ST_EXEC=.oasis_venv\Scripts\streamlit.exe
    call .\.oasis_venv\Scripts\activate.bat
    echo [INFO] Environment: .oasis_venv detected.
) else (
    set ST_EXEC=streamlit
    echo [WARNING] No virtual environment found. Running with global python.
)

:: Port Hunting (default 8503)
set PORT=8503
:findport
netstat -ano | findstr :!PORT! >nul
if !errorlevel! equ 0 (
    echo [PORT] !PORT! is busy. Hunting for next open port...
    set /a PORT+=1
    goto findport
)

:: Supervisor Loop
:RESTART
echo [%DATE% %TIME%] [START] Launching at http://localhost:!PORT!
"%ST_EXEC%" run integrated_app.py --server.port !PORT! --server.headless true --browser.gatherUsageStats false

if %errorlevel% neq 0 (
    echo.
    echo [ALERT] Integrated App crashed with exit code %errorlevel%
    echo [%DATE% %TIME%] Auto-recovering in 3 seconds...
    timeout /t 3 >nul
    goto RESTART
)

echo [%DATE% %TIME%] [STOP] Integrated App closed gracefully.
pause
