@echo off
setlocal enabledelayedexpansion
title O.A.S.I.S. Command Center [Production]
echo.
echo ===========================================
echo   O.A.S.I.S. Operations Command Center
echo   Hardened Production Launcher v1.1
echo ===========================================
echo.

:: Set working directory
cd /d "%~dp0"

:: 1. ENV CONFIG
set PORT=8501
set LOG_FILE=shadow_logs\command_center.log
if not exist shadow_logs mkdir shadow_logs

:: 2. VENV DETECTION (CC1 fix: moved BEFORE diagnostic calls)
if exist .oasis_venv\Scripts\activate.bat (
    set PYTHON_EXEC=.oasis_venv\Scripts\python.exe
    set ST_EXEC=.oasis_venv\Scripts\streamlit.exe
    echo [INFO] Environment: .oasis_venv detected.
) else (
    set PYTHON_EXEC=python
    set ST_EXEC=streamlit
    echo [WARNING] No virtual environment found. Running with global python.
)

:: 3. CLI ARGS
if "%1"=="--diag" (
    echo [SYSTEM] Running Comprehensive Diagnostics...
    "%PYTHON_EXEC%" production_diagnostic.py
    pause
    exit /b 0
)

:: Run quick health check on startup
"%PYTHON_EXEC%" production_diagnostic.py
if %errorlevel% neq 0 (
    echo [CRITICAL] Diagnostic check failed. Startup aborted.
    pause
    exit /b 1
)

:: 4. PORT HUNTING (CC2 fix: hunt instead of abort)
:findport
netstat -ano | findstr :!PORT! >nul 2>&1
if !errorlevel! equ 0 (
    echo [PORT] !PORT! is busy. Hunting for next open port...
    set /a PORT+=1
    goto findport
)

:: 5. DB INTEGRITY
if not exist "oasis\data\mock_pos_erp.db" (
    echo [SYSTEM] Building initial database state...
    "%PYTHON_EXEC%" -m oasis.logic.mock_pos_erp >> "%LOG_FILE%" 2>&1
)

:: 6. SUPERVISOR LOOP
:RESTART
echo [%DATE% %TIME%] [START] Launching O.A.S.I.S. Dashboard...
echo [%DATE% %TIME%] [LIVE] Monitoring at http://localhost:!PORT!

"%ST_EXEC%" run ops_dashboard.py --server.port !PORT! --server.headless true --browser.gatherUsageStats false >> "%LOG_FILE%" 2>&1

if %errorlevel% neq 0 (
    echo.
    echo [ALERT] O.A.S.I.S Dashboard crashed with exit code %errorlevel%
    echo [%DATE% %TIME%] Restarting in 5 seconds... (Ctrl+C to abort)
    timeout /t 5 >nul
    goto RESTART
)

echo [%DATE% %TIME%] [STOP] Dashboard closed gracefully.
pause

