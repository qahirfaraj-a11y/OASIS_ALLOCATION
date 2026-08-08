@echo off
setlocal EnableDelayedExpansion
title O.A.S.I.S. Advanced Modes
cd /d "%~dp0"

set "PY=.oasis_venv\Scripts\python.exe"
if not exist "%PY%" (set "PY=python")

:menu
cls
echo.
echo  ============================================================
echo      O . A . S . I . S .   Advanced CLI Modes
echo  ============================================================
echo.
echo    1  Showcase             (feature demo)
echo    2  Simulation           (allocation scenario)
echo    3  Preflight            (environment check)
echo    4  Build Views          (database views)
echo    5  Bootstrap Intel      (intelligence seed)
echo    6  Bootstrap Governance (governance seed)
echo    7  Build Graph          (graph store)
echo    8  Build Store Graph    (multi-store graph)
echo    9  Build Baskets        (basket builder)
echo    A  POS Sim             (POS simulator)
echo    B  POS Inject          (POS injector)
echo    C  Seed History        (history seed)
echo    D  Inject GRN Costs    (GRN cost injector)
echo    E  Push Insights       (insight push)
echo    F  Package Release     (build release zip)
echo    G  Set Branding        (branding config)
echo    H  Show Branding       (branding status)
echo    I  Metering Report     (metering usage report)
echo    J  Restore             (database restore)
echo    K  Init                (first-run init)
echo    L  Serve (Supervisor)  (engine supervisor)
echo    M  API Server          (mobile API, port 8550)
echo    N  API Bridge          (ERP bridge, port 8600)
echo    O  Hub                 (oasis_hub, port 8700)
echo    Q  Quit
echo.
set /p "CH=  Choose: "
if /I "!CH!"=="1"  "%PY%" entrypoint.py --mode showcase & pause
if /I "!CH!"=="2"  "%PY%" entrypoint.py --mode simulation & pause
if /I "!CH!"=="3"  "%PY%" entrypoint.py --mode preflight & pause
if /I "!CH!"=="4"  "%PY%" entrypoint.py --mode build-views & pause
if /I "!CH!"=="5"  "%PY%" entrypoint.py --mode bootstrap-intel & pause
if /I "!CH!"=="6"  "%PY%" entrypoint.py --mode bootstrap-governance & pause
if /I "!CH!"=="7"  "%PY%" entrypoint.py --mode build-graph & pause
if /I "!CH!"=="8"  "%PY%" entrypoint.py --mode build-store-graph & pause
if /I "!CH!"=="9"  "%PY%" entrypoint.py --mode build-baskets & pause
if /I "!CH!"=="A"  "%PY%" entrypoint.py --mode pos-sim & pause
if /I "!CH!"=="B"  "%PY%" entrypoint.py --mode pos-inject & pause
if /I "!CH!"=="C"  "%PY%" entrypoint.py --mode seed-history & pause
if /I "!CH!"=="D"  "%PY%" entrypoint.py --mode inject-grn-costs & pause
if /I "!CH!"=="E"  "%PY%" entrypoint.py --mode push-insights & pause
if /I "!CH!"=="F"  "%PY%" entrypoint.py --mode package-release & pause
if /I "!CH!"=="G"  "%PY%" entrypoint.py --mode set-branding & pause
if /I "!CH!"=="H"  "%PY%" entrypoint.py --mode show-branding & pause
if /I "!CH!"=="I"  "%PY%" entrypoint.py --mode metering-report & pause
if /I "!CH!"=="J"  "%PY%" entrypoint.py --mode restore & pause
if /I "!CH!"=="K"  "%PY%" entrypoint.py --mode init & pause
if /I "!CH!"=="L"  "%PY%" entrypoint.py --mode serve & pause
if /I "!CH!"=="M"  "%PY%" entrypoint.py --mode api & pause
if /I "!CH!"=="N"  "%PY%" entrypoint.py --mode bridge & pause
if /I "!CH!"=="O"  "%PY%" entrypoint.py --mode hub & pause
if /I "!CH!"=="Q" exit /b 0
goto menu
