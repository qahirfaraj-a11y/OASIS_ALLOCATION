@echo off
setlocal enabledelayedexpansion
title O.A.S.I.S. Command Center [SHOWCASE MODE]
echo.
echo ===========================================
echo   O.A.S.I.S. Operations Command Center
echo   Premium Showcase Launcher v2.0
echo ===========================================
echo.

:: 1. ENV CONFIG
set OASIS_SHOWCASE_MODE=true
set OASIS_DB_PATH=oasis\data\mock_pos_erp_showcase.db
set LOG_FILE=shadow_logs\showcase_ops.log
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
    echo [SYSTEM] Running Showcase Health Check...
    "%PYTHON_EXEC%" production_diagnostic.py
    pause
    exit /b 0
)

:: 4. PORT HUNTING
set PORT=8505
:findport
netstat -ano | findstr :!PORT! >nul
if !errorlevel! equ 0 (
    echo [PORT] !PORT! is busy. Hunting...
    set /a PORT+=1
    goto findport
)

:: 5. SUPERVISOR LOOP (Narrative Reset Focus)
:RESTART
echo [%DATE% %TIME%] [PREP] Resetting Showcase Narrative...
"%PYTHON_EXEC%" generate_showcase_scenario.py >> "%LOG_FILE%" 2>&1

echo [%DATE% %TIME%] [START] Launching O.A.S.I.S. Showcase...
echo [%DATE% %TIME%] [LIVE] High-Impact Demo at http://localhost:!PORT!

"%ST_EXEC%" run ops_dashboard.py --server.port !PORT! --server.headless true --browser.gatherUsageStats false >> "%LOG_FILE%" 2>&1

if %errorlevel% neq 0 (
    echo.
    echo [ALERT] Showcase crashed with exit code %errorlevel%
    echo [%DATE% %TIME%] Auto-Recovering Narrative (1s)...
    timeout /t 1 >nul
    goto RESTART
)

echo [%DATE% %TIME%] [STOP] Showcase closed gracefully.
pause
