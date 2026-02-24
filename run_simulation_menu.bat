@echo off
title OASIS Simulation Runner
color 0A
cls
echo ===================================================
echo       OASIS RETAIL SIMULATION ENGINE
echo ===================================================
echo.
echo Select a Simulation Scenario:
echo.
echo 1. Baseline Model (30 Days, $300k, Avg Demand)
echo 2. High Capital Run (30 Days, $500k, Avg Demand)
echo 3. High Season [JAN] (30 Days, $500k, 2.9x Demand)
echo 4. Resilience Test [OCT] (30 Days, $500k, Low/Bad Data)
echo 5. Long Run Stability (365 Days, $500k, Ad Infinitum)
echo 6. Custom Configuration
echo.
set /p choice="Enter Selection (1-6): "

if "%choice%"=="1" goto baseline
if "%choice%"=="2" goto highcap
if "%choice%"=="3" goto highseason
if "%choice%"=="4" goto lowseason
if "%choice%"=="5" goto longrun
if "%choice%"=="6" goto custom

goto end

:baseline
cls
echo Starting Baseline Simulation...
python run_simulation_scenario.py --scenario Baseline --days 30 --budget 300000 --month NOV
pause
goto end

:highcap
cls
echo Starting High Capital Simulation...
python run_simulation_scenario.py --scenario HighCap --days 30 --budget 500000 --month NOV
pause
goto end

:highseason
cls
echo Starting High Season (JAN) Simulation...
python run_simulation_scenario.py --scenario HighSeason --days 30 --budget 500000 --month JAN
pause
goto end

:lowseason
cls
echo Starting Resilience Test (OCT)...
python run_simulation_scenario.py --scenario LowSeason --days 30 --budget 500000 --month OCT
pause
goto end

:longrun
cls
echo Starting 1-Year Stability Test...
echo This may take a moment to compute...
python run_simulation_scenario.py --scenario YearStability --days 365 --budget 500000 --month JAN
pause
goto end

:custom
cls
echo --- Custom Simulation ---
set /p budget="Enter Opening Budget (Default 300000): "
if "%budget%"=="" set budget=300000
set /p days="Enter Duration Days (Default 30): "
if "%days%"=="" set days=30
set /p month="Enter Seasonality Month (JAN, FEB... or NOV for Avg): "
if "%month%"=="" set month=NOV

echo.
echo Running Custom Scenario: Budget=$%budget%, Days=%days%, Month=%month%
python run_simulation_scenario.py --scenario CustomRun --days %days% --budget %budget% --month %month%
pause
goto end

:end
echo.
echo Simulation Complete.
pause
