@echo off
setlocal EnableDelayedExpansion
title O.A.S.I.S.
cd /d "%~dp0"

set "PY=.oasis_venv\Scripts\python.exe"
REM NOTE: OASIS_SEED_PASSWORD is deliberately NOT set here. auth_manager falls
REM back to it when seeding a store's accounts, so exporting a known value into
REM every console this menu launches would hand every real store the same admin
REM password — the defect a91ae9d7 fixed by making seeded passwords random and
REM one-time. install.bat asks the operator for it, once, on purpose.
if not exist "%PY%" (
    echo  OASIS is not installed yet — running the installer...
    call install.bat
    if not exist "%PY%" exit /b 1
)

:menu
cls
echo.
echo  ============================================================
echo     O . A . S . I . S .    Algorithmic Retail Systems ^& Logic
echo  ============================================================
echo.
echo    0  OASIS Desktop   (RECOMMENDED — single native window)
echo    1  OASIS Home      (first-run setup)
echo    ............................................................
echo    Advanced — browser consoles
echo    2  Operations      console  :8500
echo    3  Intelligence    console  :8510
echo    4  Command Center  console  :8501
echo    5  Market Intel    console  :8505
echo    ............................................................
echo    System
echo    6  Cloud Hub       server   :8700
echo    7  OASIS Service   supervisor
echo    8  License status
echo    9  Demo / sample data options...
echo    Q  Quit
echo.
set /p "CH=  Choose: "
if /I "!CH!"=="0" start "OASIS Desktop"      cmd /c "%PY% entrypoint.py --mode desktop"
if /I "!CH!"=="1" start "OASIS Home"         cmd /c "%PY% entrypoint.py --mode home"
if /I "!CH!"=="2" start "OASIS Operations"   cmd /c "%PY% entrypoint.py --mode shell"
if /I "!CH!"=="3" start "OASIS Intel"        cmd /c "%PY% entrypoint.py --mode intel"
if /I "!CH!"=="4" start "OASIS Command"      cmd /c "%PY% entrypoint.py --mode dashboard --dashboard command"
if /I "!CH!"=="5" start "OASIS Market"       cmd /c "%PY% entrypoint.py --mode dashboard --dashboard stgat"
if /I "!CH!"=="6" start "OASIS Hub"          cmd /c "%PY% entrypoint.py --mode hub"
if /I "!CH!"=="7" start "OASIS Service"      cmd /c "%PY% entrypoint.py --mode serve"
if /I "!CH!"=="8" "%PY%" entrypoint.py --mode license-status & pause
if /I "!CH!"=="9" goto demomenu
if /I "!CH!"=="Q" exit /b 0
goto menu

:demomenu
cls
echo.
echo  ============================================================
echo     Demo / Sample Data
echo  ============================================================
echo.
echo    These REPLACE the active store with sample data. A store you have
echo    already onboarded will be overwritten.
echo.
echo    1  Build multi-store sample network   (5 outlets + history)  [default]
echo    2  Build single-store sample data     (catalogue + 14 days of sales)
echo    3  Add more sales history to the active store
echo    4  Stream live mock POS sales         (Ctrl-C to stop)
echo    B  Back to Main Menu
echo.
set /p "DCH=  Choose: "
if /I "!DCH!"=="1" "%PY%" entrypoint.py --mode demo-multi & pause
if /I "!DCH!"=="2" "%PY%" entrypoint.py --mode demo-single & pause
if /I "!DCH!"=="3" "%PY%" entrypoint.py --mode demo-bills & pause
if /I "!DCH!"=="4" "%PY%" entrypoint.py --mode pos-stream & pause
if /I "!DCH!"=="B" goto menu
goto demomenu
