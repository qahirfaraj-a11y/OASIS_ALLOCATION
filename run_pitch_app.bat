@echo off
setlocal enabledelayedexpansion
title O.A.S.I.S. Pitch Engine [Production Hardened]
echo.
echo ============================================================
echo   O.A.S.I.S. Visual Forecasting ^& Forensic Audit
echo   Premium Pitch Launcher v2.0
echo ============================================================
echo.

:: 1. ENV CONFIG
set LOG_FILE=shadow_logs\pitch_app.log
if not exist shadow_logs mkdir shadow_logs

:: 2. VENV DETECTION
if exist ".oasis_venv\Scripts\activate.bat" (
    set PYTHON_EXEC=.oasis_venv\Scripts\python.exe
    set ST_EXEC=.oasis_venv\Scripts\streamlit.exe
) else (
    set PYTHON_EXEC=python
    set ST_EXEC=streamlit
    echo [WARNING] No virtual environment found. Running with global python.
)

:: 3. CLI ARGS (Diagnostics)
if "%1"=="--diag" (
    echo [SYSTEM] Running Comprehensive Diagnostics...
    "%PYTHON_EXEC%" production_diagnostic.py
    pause
    exit /b 0
)

:: 4. PRE-FLIGHT
echo [CHECK] Verifying Forensic Diagnostic Health...
"%PYTHON_EXEC%" production_diagnostic.py
if %errorlevel% neq 0 (
    echo [CRITICAL] Diagnostic check failed. Startup aborted.
    pause
    exit /b 1
)

:: 5. PORT HUNTING
set PORT=8501
:findport
netstat -ano | findstr :!PORT! >nul
if !errorlevel! equ 0 (
    echo [PORT] !PORT! is busy. Hunting for next open node...
    set /a PORT+=1
    goto findport
)

:: 6. SUPERVISOR LOOP
:RESTART
echo [%DATE% %TIME%] [START] Launching O.A.S.I.S. Pitch Engine...
echo [%DATE% %TIME%] [LIVE] Pitch Mirror at http://localhost:!PORT!

"%ST_EXEC%" run pitch_app_v2.py --server.port !PORT! --server.headless true --browser.gatherUsageStats false >> "%LOG_FILE%" 2>&1

if %errorlevel% neq 0 (
    echo.
    echo [ALERT] Pitch App crashed with exit code %errorlevel%
    echo [%DATE% %TIME%] Auto-recovering in 3 seconds...
    timeout /t 3 >nul
    goto RESTART
)

echo [%DATE% %TIME%] [STOP] Pitch Engine closed gracefully.
pause
