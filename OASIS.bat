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
echo    M  Maintenance ^& recovery options...
echo    I  Integrations (mobile API / manager bridge)...
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
if /I "!CH!"=="M" goto maintmenu
if /I "!CH!"=="I" goto intmenu
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

:maintmenu
cls
echo.
echo  ============================================================
echo     Maintenance ^& Recovery
echo  ============================================================
echo.
echo    Your data lives in oasis\data. Back it up before any upgrade.
echo.
echo    1  Back up the active store now
echo    2  List available backups
echo    3  Restore from a backup        (asks which one first)
echo    4  Upgrade this installation
echo    5  Health check                 (day-0 assessment)
echo    6  Value report
echo    7  Change a user's password
echo    B  Back to Main Menu
echo.
set /p "MCH=  Choose: "
if /I "!MCH!"=="1" "%PY%" entrypoint.py --mode backup & pause
if /I "!MCH!"=="2" "%PY%" entrypoint.py --mode list-backups & pause
if /I "!MCH!"=="3" goto restoreprompt
if /I "!MCH!"=="4" "%PY%" entrypoint.py --mode upgrade & pause
if /I "!MCH!"=="5" "%PY%" entrypoint.py --mode assess & pause
if /I "!MCH!"=="6" "%PY%" entrypoint.py --mode value-report & pause
if /I "!MCH!"=="7" goto pwprompt
if /I "!MCH!"=="B" goto menu
goto maintmenu

:restoreprompt
cls
echo.
echo  RESTORE — this REPLACES the active store with a backup.
echo  The current database is kept alongside it as .pre_restore.
echo  Close every OASIS console first: restore refuses to run while
echo  a console holds the database open.
echo.
"%PY%" entrypoint.py --mode restore
echo.
set /p "BK=  Backup number to restore (blank = cancel): "
if "!BK!"=="" goto maintmenu
"%PY%" entrypoint.py --mode restore --file "!BK!"
pause
goto maintmenu

:pwprompt
cls
echo.
set /p "UN=  Username to change the password for (blank = cancel): "
if "!UN!"=="" goto maintmenu
"%PY%" entrypoint.py --mode set-password --username "!UN!"
pause
goto maintmenu

:intmenu
cls
echo.
echo  ============================================================
echo     Integrations
echo  ============================================================
echo.
echo    These publish an authenticated HTTP API on ALL network
echo    interfaces. Every endpoint requires the X-API-Key header.
echo    The key is OASIS_API_KEY, or a generated one stored in
echo    oasis\data\.oasis_api_key if you have not set that.
echo    Only start these if you intend other machines to connect.
echo.
echo    1  Mobile API       :8550
echo    2  Manager Bridge   :8600
echo    B  Back to Main Menu
echo.
set /p "ICH=  Choose: "
if /I "!ICH!"=="1" start "OASIS Mobile API"     cmd /c "%PY% entrypoint.py --mode api"
if /I "!ICH!"=="2" start "OASIS Manager Bridge" cmd /c "%PY% entrypoint.py --mode bridge"
if /I "!ICH!"=="B" goto menu
goto intmenu
