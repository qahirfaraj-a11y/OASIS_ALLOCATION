@echo off
setlocal
title O.A.S.I.S. Installer
cd /d "%~dp0"
echo ==========================================================
echo   O.A.S.I.S. - Installation
echo ==========================================================

echo [1/4] Creating Python environment...
python -m venv .oasis_venv
if errorlevel 1 (
    echo ERROR: Python 3.10+ is required on PATH. Install it and re-run.
    pause & exit /b 1
)

echo [2/4] Installing dependencies...
.oasis_venv\Scripts\python.exe -m pip install --upgrade pip -q
.oasis_venv\Scripts\python.exe -m pip install -r requirements.txt -q
if errorlevel 1 (
    echo ERROR: dependency installation failed. Check network / proxy.
    pause & exit /b 1
)

echo [3/4] Preflight check...
.oasis_venv\Scripts\python.exe entrypoint.py --mode preflight

echo [4/4] Version + license status...
.oasis_venv\Scripts\python.exe entrypoint.py --mode version
.oasis_venv\Scripts\python.exe entrypoint.py --mode license-status

echo.
echo ==========================================================
echo   Installed. Next steps:
echo   1. Place your license key as oasis_license.key here
echo      (without it, a 14-day evaluation starts on first run).
echo   2. Onboard your data:
echo        entrypoint.py --mode build-views   (live ERP views)  or
echo        entrypoint.py --mode build-pos-db  (from Excel exports)
echo   3. Set the admin password:
echo        entrypoint.py --mode set-password --username ops_admin
echo   4. Launch: run_oasis_home.bat
echo ==========================================================
pause
