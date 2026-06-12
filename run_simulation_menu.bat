@echo off
setlocal
title O.A.S.I.S. Simulation Runner
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)

echo ===================================================
echo       OASIS RETAIL SIMULATION ENGINE
echo ===================================================
echo.
echo Select a Simulation Scenario:
echo.
echo 1. Baseline Model (30 Days, 300k, Avg Demand)
echo 2. High Capital Run (30 Days, 500k, Avg Demand)
echo 3. High Season [JAN] (30 Days, 500k, 2.9x Demand)
echo 4. Resilience Test [OCT] (30 Days, 500k, Low/Bad Data)
echo 5. Long Run Stability (365 Days, 500k)
echo 6. Custom Configuration
echo.
set /p choice="Enter Selection (1-6): "

if "%choice%"=="1" "%PYTHON_EXEC%" entrypoint.py --mode simulation --scenario Baseline --days 30 --budget 300000 --month NOV
if "%choice%"=="2" "%PYTHON_EXEC%" entrypoint.py --mode simulation --scenario HighCap --days 30 --budget 500000 --month NOV
if "%choice%"=="3" "%PYTHON_EXEC%" entrypoint.py --mode simulation --scenario HighSeason --days 30 --budget 500000 --month JAN
if "%choice%"=="4" "%PYTHON_EXEC%" entrypoint.py --mode simulation --scenario LowSeason --days 30 --budget 500000 --month OCT
if "%choice%"=="5" "%PYTHON_EXEC%" entrypoint.py --mode simulation --scenario YearStability --days 365 --budget 500000 --month JAN
if "%choice%"=="6" goto custom
goto end

:custom
set /p budget="Enter Budget (Default 300000): "
if "%budget%"=="" set budget=300000
set /p days="Enter Duration Days (Default 30): "
if "%days%"=="" set days=30
set /p month="Enter Month (JAN-DEC or NOV for Avg): "
if "%month%"=="" set month=NOV
"%PYTHON_EXEC%" entrypoint.py --mode simulation --scenario CustomRun --days %days% --budget %budget% --month %month%

:end
pause
