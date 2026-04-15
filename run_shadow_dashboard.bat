@echo off
setlocal enabledelayedexpansion
echo ============================================================
echo O.A.S.I.S. Shadow Mode BACKGROUND DAEMON - HARDENED
echo ============================================================

:: 1. Check Virtual Environment
if not exist ".oasis_venv\Scripts\activate.bat" (
    echo [ERROR] Virtual Environment .oasis_venv not found.
    pause
    exit /b
)
call .oasis_venv\Scripts\activate.bat

:: 2. Pre-flight Setup
if not exist "monitoring\inbound" mkdir monitoring\inbound
if not exist "monitoring\reports" mkdir monitoring\reports
if not exist "monitoring\archive" mkdir monitoring\archive
if not exist "shadow_logs" mkdir shadow_logs

:: 3. Production Diagnostics
echo [DIAGNOSTIC] Running pre-flight system check...
python production_diagnostic.py
if errorlevel 1 (
    echo [ERROR] Shadow Environment health check failed.
    pause
    exit /b
)

:: 4. Mode Selection
set MODE=file
if "%1"=="sql" set MODE=sql

:: 5. Port Hunting for Dashboard
set PORT=8506
:port_hunt
netstat -ano | findstr :%PORT% > nul
if %errorlevel% == 0 (
    set /a PORT+=1
    goto port_hunt
)

:launch
echo [DAEMON] Initializing O.A.S.I.S. Background Auditor...
echo [DASHBOARD] Starting Shadow Visualizer on port %PORT%...
echo [ROUTING] Dynamic (Pattern: StoreID_Scorecard_Date.xlsx)
echo [LOGGING] Redirecting to shadow_logs/shadow_launcher.log
echo ============================================================

:: 6. Launch Supervisor
:: Starts both the background daemon and the foreground dashboard
start "OASIS_SHADOW_DAEMON" /min cmd /c "python shadow_monitor.py --mode %MODE% --root . > shadow_logs\shadow_daemon_console.log 2>&1"

echo [SUCCESS] Shadow Auditor is running in the background.
echo [LAUNCH] Opening Audit Dashboard...

:: Run Dashboard in the foreground to keep the window active
python -m streamlit run shadow_dashboard.py --server.port %PORT% --server.headless true

if errorlevel 1 (
    echo [CRASH] Shadow Dashboard exited unexpectedly. Restarting supervisor...
    timeout /t 5
    goto launch
)

pause
