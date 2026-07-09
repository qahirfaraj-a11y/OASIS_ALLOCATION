@echo off
setlocal
title O.A.S.I.S. Installer
cd /d "%~dp0"
echo ==========================================================
echo   O.A.S.I.S. - Installation
echo ==========================================================

echo [1/5] Creating Python environment...
python -m venv .oasis_venv
if errorlevel 1 (
    echo ERROR: Python 3.10+ is required on PATH. Install it and re-run.
    pause & exit /b 1
)

echo [2/5] Installing dependencies...
.oasis_venv\Scripts\python.exe -m pip install --upgrade pip -q
.oasis_venv\Scripts\python.exe -m pip install -r requirements.txt -q
if errorlevel 1 (
    echo ERROR: dependency installation failed. Check network / proxy.
    pause & exit /b 1
)

REM Seed a stable install password BEFORE preflight seeds the user table.
REM The installer prompts the operator interactively; blank keeps the demo
REM default (oasis2026), safe for evaluation.
if "%OASIS_SEED_PASSWORD%"=="" (
    echo.
    echo [3/5] Set the install password for OASIS admin users
    set /p OASIS_SEED_PASSWORD="  Password (blank = oasis2026 for evaluation): "
    if "%OASIS_SEED_PASSWORD%"=="" set OASIS_SEED_PASSWORD=oasis2026
) else (
    echo [3/5] OASIS_SEED_PASSWORD already set from environment
)

echo [4/5] Version + license status
.oasis_venv\Scripts\python.exe entrypoint.py --mode version
.oasis_venv\Scripts\python.exe entrypoint.py --mode license-status

echo [5/5] Preflight check (missing tables are expected on a fresh install)
.oasis_venv\Scripts\python.exe entrypoint.py --mode preflight

echo.
echo ==========================================================
echo   Installed. Next steps:
echo   1. Place your license key as oasis_license.key here
echo      (without it, a 14-day evaluation starts on first run).
echo   2. Onboard your data:
echo        --mode build-pos-db      (single store from Excel exports)
echo        --mode build-multi-store-db   (multi-store network)
echo        --mode build-views       (live ERP views)
echo   3. Launch: run_oasis_home.bat
echo ==========================================================
pause
