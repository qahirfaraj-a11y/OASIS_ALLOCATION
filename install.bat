@echo off
setlocal EnableDelayedExpansion
title O.A.S.I.S. Installer
cd /d "%~dp0"

REM Get VERSION for the welcome banner
set "OASIS_VER=?"
if exist VERSION (for /f "usebackq delims=" %%v in ("VERSION") do set "OASIS_VER=%%v")

cls
echo.
echo  ============================================================
echo    [ AUDITED LOGIC_ENGINE_V!OASIS_VER! ]
echo.
echo         O . A . S . I . S .   Installation
echo         Algorithmic Retail Systems ^& Logic
echo  ============================================================
echo.

REM 1. Tenant name -> branding.json seed
if not exist branding.json (
    set /p "CLIENT_NAME=  Company / store name (blank = OASIS default): "
    if not "!CLIENT_NAME!"=="" (
        > branding.json (
            echo {
            echo   "tenant_name": "!CLIENT_NAME!",
            echo   "product_name": "OASIS",
            echo   "primary_color": "#00F5D4",
            echo   "accent_color":  "#00C4AB"
            echo }
        )
        echo   [OK] Branding saved to branding.json
    )
) else (
    echo   [OK] Existing branding.json preserved
)
echo.

REM 2. Install password
if "%OASIS_SEED_PASSWORD%"=="" (
    set /p "OASIS_SEED_PASSWORD=  Admin password (blank = oasis2026): "
    if "!OASIS_SEED_PASSWORD!"=="" set "OASIS_SEED_PASSWORD=oasis2026"
)
echo.

REM 3. Store topology
set "OASIS_PROFILE="
set /p "OASIS_PROFILE=  Single store or multi-store network? [single/multi] (default single): "
if "!OASIS_PROFILE!"=="" set "OASIS_PROFILE=single"
if /I not "!OASIS_PROFILE!"=="multi" set "OASIS_PROFILE=single"
echo.

REM 4. Python venv
echo  [1/4] Creating Python environment...
python -m venv .oasis_venv >nul 2>&1
if errorlevel 1 (
    echo.
    echo   ERROR: Python 3.10+ was not found on PATH.
    echo   Install it from https://python.org and re-run install.bat
    pause
    exit /b 1
)

echo  [2/4] Installing dependencies...
.oasis_venv\Scripts\python.exe -m pip install --upgrade pip -q
.oasis_venv\Scripts\python.exe -m pip install -r requirements.txt -q
if errorlevel 1 (
    echo   ERROR: dependency install failed. Check network / proxy and re-run.
    pause & exit /b 1
)

REM 3. Data setup is chosen on first launch (sample / empty / connect a POS /
REM    multi-store demo) — a fresh install NEVER silently builds mock data.
echo  [3/4] Data setup will run on first launch.
echo        ^(You'll choose: sample store, empty store, connect your POS,
echo         build from catalogue, or multi-store demo network.^)

echo  [4/4] License status:
.oasis_venv\Scripts\python.exe entrypoint.py --mode license-status

echo.
echo  ============================================================
echo    Installation complete.
echo.
echo    Launch:      double-click  OASIS.bat  (or run_oasis_home.bat)
echo    Service:     double-click  serve.bat  (or register_service.bat)
echo    First run:   OASIS asks where your data comes from --
echo                 try sample store, empty store, connect POS, or build.
echo    Log in:      admin password shown above
echo                 (change: .oasis_venv\Scripts\python.exe entrypoint.py --mode set-password)
echo    License key: drop it beside install.bat as  oasis_license.key
echo                 (without one, the 14-day evaluation trial is active)
echo.
echo    Support: contact iLink
echo  ============================================================
pause
