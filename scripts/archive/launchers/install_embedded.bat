@echo off
setlocal EnableDelayedExpansion
chcp 65001 >nul 2>&1
title O.A.S.I.S. Installer v2.3.0

:: ============================================================
:: O.A.S.I.S. — Embedded-Python Windows Installer
:: ============================================================
:: This installer is fully self-contained. It does NOT require
:: Python to be installed on the client machine.
::
:: It uses a bundled Python 3.10 embeddable runtime (in the
:: runtime/ folder) to create a local virtual environment with
:: all dependencies pre-packed.
::
:: For online installs, set OASIS_ONLINE_INSTALL=1 to download
:: Python and dependencies from the internet instead.
:: ============================================================

set "INSTALL_DIR=%~dp0"
set "RUNTIME_DIR=%INSTALL_DIR%runtime"
set "PYTHON_EMBED_DIR=%INSTALL_DIR%python_runtime"
set "VENV_DIR=%INSTALL_DIR%.oasis_venv"
set "CONFIG_FILE=%INSTALL_DIR%oasis_client_config.json"
set "CONFIG_TEMPLATE=%INSTALL_DIR%oasis_client_config.template.json"
set "LOG_FILE=%INSTALL_DIR%install_log.txt"

:: Clear log
echo O.A.S.I.S. Installation Log - %date% %time% > "%LOG_FILE%"

echo.
echo  ╔══════════════════════════════════════════════════════════╗
echo  ║                                                          ║
echo  ║     ██████╗  █████╗ ███████╗██╗███████╗                 ║
echo  ║    ██╔═══██╗██╔══██╗██╔════╝██║██╔════╝                 ║
echo  ║    ██║   ██║███████║███████╗██║███████╗                  ║
echo  ║    ██║   ██║██╔══██║╚════██║██║╚════██║                  ║
echo  ║    ╚██████╔╝██║  ██║███████║██║███████║                  ║
echo  ║     ╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝╚══════╝                ║
echo  ║                                                          ║
echo  ║    Autonomous Supply Intelligence System  v2.3.0         ║
echo  ║    Self-Contained Installer                              ║
echo  ║                                                          ║
echo  ╚══════════════════════════════════════════════════════════╝
echo.

:: ── Step 1: Locate or Unpack Python Runtime ─────────────────
echo [1/7] Setting up Python runtime...
echo [1/7] Python runtime setup... >> "%LOG_FILE%"

:: Check if we already have an unpacked runtime
if exist "%PYTHON_EMBED_DIR%\python.exe" (
    echo     [OK] Embedded Python already unpacked.
    echo     Already unpacked >> "%LOG_FILE%"
    goto :python_ready
)

:: Check for bundled runtime zip
set "EMBED_ZIP="
for %%F in ("%RUNTIME_DIR%\python-3.10*-embed-amd64.zip") do (
    set "EMBED_ZIP=%%F"
)

if defined EMBED_ZIP (
    echo     [INFO] Found bundled Python: !EMBED_ZIP!
    echo     Unpacking bundled Python... >> "%LOG_FILE%"
    mkdir "%PYTHON_EMBED_DIR%" 2>nul

    :: Use PowerShell to extract (available on all Win10/11)
    powershell -NoProfile -Command "Expand-Archive -Path '!EMBED_ZIP!' -DestinationPath '%PYTHON_EMBED_DIR%' -Force" 2>>"%LOG_FILE%"
    if !errorlevel! neq 0 (
        echo     [ERROR] Failed to unpack Python runtime.
        echo     Unpack failed >> "%LOG_FILE%"
        pause
        exit /b 1
    )
    echo     [OK] Python runtime unpacked.
    goto :python_ready
)

:: Check if system Python is available as fallback
set "SYS_PYTHON="
for %%P in (python python3 py) do (
    where %%P >nul 2>&1
    if !errorlevel! equ 0 (
        for /f "tokens=2 delims= " %%V in ('%%P --version 2^>^&1') do (
            for /f "tokens=1,2 delims=." %%A in ("%%V") do (
                if %%A geq 3 if %%B geq 10 (
                    set "SYS_PYTHON=%%P"
                    echo     [INFO] Using system Python: %%P %%V
                )
            )
        )
    )
)

if defined SYS_PYTHON (
    echo     [INFO] No bundled runtime found; using system Python.
    echo     Using system Python >> "%LOG_FILE%"
    goto :use_system_python
)

echo.
echo     [ERROR] No Python runtime found.
echo.
echo     This installer expects a bundled runtime in:
echo       %RUNTIME_DIR%\python-3.10.x-embed-amd64.zip
echo.
echo     Or install Python 3.10+ from https://www.python.org/downloads/
echo     and ensure "Add Python to PATH" is checked.
echo.
echo     Python not found >> "%LOG_FILE%"
pause
exit /b 1

:python_ready
set "PYTHON_EXE=%PYTHON_EMBED_DIR%\python.exe"

:: ── Enable pip in the embeddable distribution ───────────────
:: The embeddable Python ships with import restrictions. We need
:: to uncomment the "import site" line in pythonXY._pth to enable
:: pip and package installation.
echo     [INFO] Enabling pip support in embedded Python...

:: Find and patch the ._pth file
for %%F in ("%PYTHON_EMBED_DIR%\python3*._pth") do (
    set "PTH_FILE=%%F"
)

if defined PTH_FILE (
    :: Check if already patched (import site uncommented)
    findstr /R /C:"^import site" "!PTH_FILE!" >nul 2>&1
    if !errorlevel! neq 0 (
        :: Replace "#import site" with "import site"
        powershell -NoProfile -Command "(Get-Content '!PTH_FILE!') -replace '^#\s*import site', 'import site' | Set-Content '!PTH_FILE!'" 2>>"%LOG_FILE%"
        echo     [OK] Patched !PTH_FILE!
    ) else (
        echo     [OK] Already patched.
    )
)

:: Bootstrap pip if not present
if not exist "%PYTHON_EMBED_DIR%\Scripts\pip.exe" (
    echo     [INFO] Bootstrapping pip...
    if exist "%RUNTIME_DIR%\get-pip.py" (
        "%PYTHON_EXE%" "%RUNTIME_DIR%\get-pip.py" --no-warn-script-location 2>>"%LOG_FILE%"
    ) else (
        echo     [ERROR] get-pip.py not found in %RUNTIME_DIR%
        echo     Please ensure the runtime bundle is complete.
        pause
        exit /b 1
    )
    echo     [OK] pip bootstrapped.
) else (
    echo     [OK] pip already available.
)

set "PIP_EXE=%PYTHON_EMBED_DIR%\Scripts\pip.exe"
goto :install_deps

:use_system_python
:: Create a standard venv from system Python
if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo     Creating virtual environment...
    %SYS_PYTHON% -m venv "%VENV_DIR%"
    if !errorlevel! neq 0 (
        echo     [ERROR] Failed to create virtual environment.
        pause
        exit /b 1
    )
)
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
set "PIP_EXE=%VENV_DIR%\Scripts\pip.exe"
set "PYTHON_EMBED_DIR=%VENV_DIR%"

:: ── Step 2: Install Dependencies ────────────────────────────
:install_deps
echo.
echo [2/7] Installing dependencies...
echo [2/7] Installing deps... >> "%LOG_FILE%"

:: Upgrade pip first
"%PIP_EXE%" install --upgrade pip --quiet 2>>"%LOG_FILE%"

:: Check for offline wheelhouse
if exist "%RUNTIME_DIR%\wheelhouse" (
    echo     [INFO] Installing from offline wheelhouse...
    "%PIP_EXE%" install --no-index --find-links="%RUNTIME_DIR%\wheelhouse" -r "%INSTALL_DIR%requirements.txt" --quiet 2>>"%LOG_FILE%"
    if !errorlevel! neq 0 (
        echo     [WARNING] Some offline packages failed. Trying with fallback...
        "%PIP_EXE%" install --no-index --find-links="%RUNTIME_DIR%\wheelhouse" -r "%INSTALL_DIR%requirements.txt" 2>>"%LOG_FILE%"
    )
) else (
    echo     [INFO] No wheelhouse found. Installing from internet...
    "%PIP_EXE%" install -r "%INSTALL_DIR%requirements.txt" --quiet 2>>"%LOG_FILE%"
)

if !errorlevel! neq 0 (
    echo     [WARNING] Dependency installation had issues. Check install_log.txt
    echo     Dep install issues >> "%LOG_FILE%"
) else (
    echo     [OK] All dependencies installed.
)

:: ── Step 3: Client Configuration ────────────────────────────
echo.
echo [3/7] Checking client configuration...
echo [3/7] Config check... >> "%LOG_FILE%"

if exist "%CONFIG_FILE%" (
    echo     [OK] oasis_client_config.json already exists.
) else (
    if exist "%CONFIG_TEMPLATE%" (
        copy "%CONFIG_TEMPLATE%" "%CONFIG_FILE%" >nul
        echo     [CREATED] oasis_client_config.json from template.
        echo     NOTE: Edit this file with your client-specific settings.
    ) else (
        echo     [CREATED] Minimal oasis_client_config.json
        (
            echo {
            echo     "client": {"client_id": "new_client", "client_name": "New Client"},
            echo     "data_pathway": "file",
            echo     "paths": {"data_dir": "oasis\\data", "db_path": "oasis.db"}
            echo }
        ) > "%CONFIG_FILE%"
    )
)

:: ── Step 4: Create Directory Structure ──────────────────────
echo.
echo [4/7] Creating directory structure...
echo [4/7] Creating dirs... >> "%LOG_FILE%"

for %%D in (
    "%INSTALL_DIR%inbound_drops"
    "%INSTALL_DIR%inbound_drops\bootstrap"
    "%INSTALL_DIR%inbound_drops\archive"
    "%INSTALL_DIR%logs"
    "%INSTALL_DIR%oasis\data"
    "%INSTALL_DIR%reports"
    "%INSTALL_DIR%backups"
) do (
    if not exist %%D mkdir %%D 2>nul
)
echo     [OK] Directory structure created.

:: ── Step 5: Generate Launcher Scripts ───────────────────────
echo.
echo [5/7] Generating launcher scripts...
echo [5/7] Generating launchers... >> "%LOG_FILE%"

:: Determine which Python to reference in launchers
set "LAUNCHER_PYTHON=%PYTHON_EXE%"

:: Main launcher: O.A.S.I.S. Home
(
    echo @echo off
    echo setlocal
    echo title O.A.S.I.S. Home - Suite Launcher
    echo cd /d "%%~dp0"
    echo set "OASIS_DB_PATH=%%~dp0oasis\data\rhapta_pos.db"
    echo "%LAUNCHER_PYTHON%" entrypoint.py --mode home %%*
    echo pause
) > "%INSTALL_DIR%run_oasis_home.bat"

:: Operations Console
(
    echo @echo off
    echo setlocal
    echo title O.A.S.I.S. Operations Console
    echo cd /d "%%~dp0"
    echo set "OASIS_DB_PATH=%%~dp0oasis\data\rhapta_pos.db"
    echo "%LAUNCHER_PYTHON%" entrypoint.py --mode shell %%*
    echo pause
) > "%INSTALL_DIR%run_oasis.bat"

:: Intelligence Console
(
    echo @echo off
    echo setlocal
    echo title O.A.S.I.S. Intelligence Console
    echo cd /d "%%~dp0"
    echo set "OASIS_DB_PATH=%%~dp0oasis\data\rhapta_pos.db"
    echo "%LAUNCHER_PYTHON%" entrypoint.py --mode intel %%*
    echo pause
) > "%INSTALL_DIR%run_oasis_intel.bat"

:: Command Center
(
    echo @echo off
    echo setlocal
    echo title O.A.S.I.S. Command Center
    echo cd /d "%%~dp0"
    echo set "OASIS_DB_PATH=%%~dp0oasis\data\rhapta_pos.db"
    echo "%LAUNCHER_PYTHON%" -m streamlit run ops_dashboard.py --server.port 8501 %%*
    echo pause
) > "%INSTALL_DIR%run_command_center.bat"

echo     [OK] Launcher scripts generated.

:: ── Step 6: Preflight Check ─────────────────────────────────
echo.
echo [6/7] Running preflight check...
echo [6/7] Preflight... >> "%LOG_FILE%"

"%PYTHON_EXE%" entrypoint.py --mode preflight 2>>"%LOG_FILE%"

:: ── Step 7: Version + License Status ────────────────────────
echo.
echo [7/7] Verifying installation...
echo [7/7] Verify... >> "%LOG_FILE%"

"%PYTHON_EXE%" entrypoint.py --mode version 2>>"%LOG_FILE%"
"%PYTHON_EXE%" entrypoint.py --mode license-status 2>>"%LOG_FILE%"

:: ── Summary ─────────────────────────────────────────────────
echo.
echo  ╔══════════════════════════════════════════════════════════╗
echo  ║              INSTALLATION COMPLETE                       ║
echo  ╚══════════════════════════════════════════════════════════╝
echo.
echo  Installation directory:  %INSTALL_DIR%
echo  Python runtime:          %PYTHON_EXE%
echo  Configuration:           %CONFIG_FILE%
echo  Log file:                %LOG_FILE%
echo.
echo  ─────────────────────────────────────────────────────────
echo  NEXT STEPS:
echo  ─────────────────────────────────────────────────────────
echo.
echo  1. Place your license key as oasis_license.key here
echo     (without it, a 14-day evaluation starts on first run).
echo.
echo  2. Onboard your data:
echo       entrypoint.py --mode build-views   (live ERP views)  or
echo       entrypoint.py --mode build-pos-db  (from Excel exports)
echo.
echo  3. Set the admin password:
echo       entrypoint.py --mode set-password --username ops_admin
echo.
echo  4. Launch: run_oasis_home.bat
echo.
echo  ─────────────────────────────────────────────────────────
echo.

echo Installation completed at %date% %time% >> "%LOG_FILE%"
pause
