@echo off
setlocal EnableDelayedExpansion
title O.A.S.I.S.
cd /d "%~dp0"

set "PY=.oasis_venv\Scripts\python.exe"
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
echo    1  Home            (start here — launcher + first-run setup)
echo    2  Operations      console  :8500
echo    3  Intelligence    console  :8510
echo    4  Command Center  console  :8501
echo    5  Market Intel    console  :8505
echo    6  Cloud Hub       server   :8700
echo    7  OASIS Service   supervisor
echo    8  License status
echo    Q  Quit
echo.
set /p "CH=  Choose: "
if /I "!CH!"=="1" start "OASIS Home"        cmd /c "%PY% entrypoint.py --mode home"
if /I "!CH!"=="2" start "OASIS Operations"  cmd /c "%PY% entrypoint.py --mode shell"
if /I "!CH!"=="3" start "OASIS Intel"       cmd /c "%PY% entrypoint.py --mode intel"
if /I "!CH!"=="4" start "OASIS Command"     cmd /c "%PY% entrypoint.py --mode dashboard --dashboard command"
if /I "!CH!"=="5" start "OASIS Market"      cmd /c "%PY% entrypoint.py --mode dashboard --dashboard stgat"
if /I "!CH!"=="6" start "OASIS Hub"         cmd /c "%PY% entrypoint.py --mode hub"
if /I "!CH!"=="7" start "OASIS Service"     cmd /c "%PY% entrypoint.py --mode serve"
if /I "!CH!"=="8" "%PY%" entrypoint.py --mode license-status & pause
if /I "!CH!"=="Q" exit /b 0
goto menu
