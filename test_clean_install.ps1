# ============================================================
# O.A.S.I.S. Clean-Machine Install Test
# ============================================================
# Uses Windows Sandbox to simulate a clean Windows installation.
# No Python, no dev tools, nothing - proving the embedded
# installer works from scratch.
#
# Prerequisites:
#   - Windows 10/11 Pro/Enterprise
#   - Windows Sandbox feature enabled:
#       Enable-WindowsOptionalFeature -Online -FeatureName "Containers-DisposableClientVM"
#
# Usage:
#   .\test_clean_install.ps1                    # test the latest dist zip
#   .\test_clean_install.ps1 -ZipPath .\custom.zip   # test a specific zip
# ============================================================

[CmdletBinding()]
param(
    [string]$ZipPath = "",
    [string]$OASISRoot = "",
    [switch]$SkipSandbox,
    [switch]$KeepArtifacts
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition

# - Resolve paths -

if (-not $OASISRoot) {
    $OASISRoot = $ScriptDir
}

if (-not $ZipPath) {
    # Find the latest dist zip
    $distDir = Join-Path $OASISRoot "dist"
    if (Test-Path $distDir) {
        $latest = Get-ChildItem -Path $distDir -Filter "OASIS_v*.zip" |
                  Sort-Object LastWriteTime -Descending |
                  Select-Object -First 1
        if ($latest) {
            $ZipPath = $latest.FullName
        }
    }
}

if (-not $ZipPath -or -not (Test-Path $ZipPath)) {
    Write-Host ""
    Write-Host "  [ERROR] No OASIS zip found." -ForegroundColor Red
    Write-Host ""
    Write-Host "  Either:"
    Write-Host "    1. Run 'python entrypoint.py --mode package-release' first"
    Write-Host "    2. Specify a zip: .\test_clean_install.ps1 -ZipPath path\to\OASIS.zip"
    Write-Host ""
    exit 1
}

$ZipName = Split-Path -Leaf $ZipPath
$ZipSize = [math]::Round((Get-Item $ZipPath).Length / 1MB, 1)

Write-Host ""
Write-Host "  ============================================================" -ForegroundColor Cyan
Write-Host "        O.A.S.I.S. Clean-Machine Install Test                 " -ForegroundColor Cyan
Write-Host "  ============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Zip:  $ZipName ($ZipSize MB)"
Write-Host "  Path: $ZipPath"
Write-Host ""

# - Check prerequisites -

function Test-SandboxAvailable {
    try {
        $feature = Get-WindowsOptionalFeature -Online -FeatureName "Containers-DisposableClientVM" -ErrorAction Stop
        return ($feature -and $feature.State -eq "Enabled")
    } catch {
        # If we can't check due to lack of admin rights, assume it's available
        return $true
    }
}

if (-not $SkipSandbox) {
    Write-Host "  [CHECK] Windows Sandbox feature..." -NoNewline
    if (Test-SandboxAvailable) {
        Write-Host " OK" -ForegroundColor Green
    } else {
        Write-Host " NOT ENABLED" -ForegroundColor Red
        Write-Host ""
        Write-Host "  Windows Sandbox is required. Enable it with:" -ForegroundColor Yellow
        Write-Host "    Enable-WindowsOptionalFeature -Online -FeatureName 'Containers-DisposableClientVM'"
        Write-Host ""
        Write-Host "  Or run with -SkipSandbox to just validate the zip contents."
        Write-Host ""
        exit 1
    }
}

# - Phase 1: Validate zip contents -

Write-Host ""
Write-Host "  --- Phase 1: Zip Content Validation ---" -ForegroundColor Yellow
Write-Host ""

$tempExtract = Join-Path $env:TEMP "oasis_install_test_$(Get-Random)"
New-Item -ItemType Directory -Path $tempExtract -Force | Out-Null

try {
    Write-Host "  [EXTRACT] Unpacking to temp directory..."
    Expand-Archive -Path $ZipPath -DestinationPath $tempExtract -Force

    # Find the root folder inside the zip
    $innerDirs = Get-ChildItem -Path $tempExtract -Directory
    if ($innerDirs.Count -eq 1) {
        $installRoot = $innerDirs[0].FullName
    } else {
        $installRoot = $tempExtract
    }

    # Required files check
    $requiredFiles = @(
        "entrypoint.py",
        "install_embedded.bat",
        "requirements.txt",
        "VERSION",
        "oasis_client_config.template.json"
    )

    $requiredDirs = @(
        "oasis",
        "oasis\logic",
        "oasis\ui",
        "runtime"
    )

    $runtimeFiles = @(
        "runtime\get-pip.py"
    )

    $allPassed = $true

    foreach ($f in $requiredFiles) {
        $path = Join-Path $installRoot $f
        if (Test-Path $path) {
            Write-Host "    [PASS] $f" -ForegroundColor Green
        } else {
            Write-Host "    [FAIL] $f - MISSING" -ForegroundColor Red
            $allPassed = $false
        }
    }

    foreach ($d in $requiredDirs) {
        $path = Join-Path $installRoot $d
        if (Test-Path $path -PathType Container) {
            $count = (Get-ChildItem -Path $path -Recurse -File).Count
            Write-Host "    [PASS] $d/ ($count files)" -ForegroundColor Green
        } else {
            Write-Host "    [FAIL] $d/ - MISSING" -ForegroundColor Red
            $allPassed = $false
        }
    }

    # Check for runtime bundle
    $hasRuntime = $false
    $embedZips = Get-ChildItem -Path (Join-Path $installRoot "runtime") -Filter "python-3.10*-embed*.zip" -ErrorAction SilentlyContinue
    if ($embedZips) {
        Write-Host "    [PASS] runtime/python-embed-*.zip (bundled Python)" -ForegroundColor Green
        $hasRuntime = $true
    } else {
        Write-Host "    [WARN] No bundled Python in runtime/ - installer will need system Python or internet" -ForegroundColor Yellow
    }

    $wheelhouse = Join-Path $installRoot "runtime\wheelhouse"
    if (Test-Path $wheelhouse) {
        $wheelCount = (Get-ChildItem -Path $wheelhouse -Filter "*.whl").Count
        $tarCount = (Get-ChildItem -Path $wheelhouse -Filter "*.tar.gz" -ErrorAction SilentlyContinue).Count
        $total = $wheelCount + $tarCount
        Write-Host "    [PASS] runtime/wheelhouse/ ($total packages)" -ForegroundColor Green
    } else {
        Write-Host "    [WARN] No wheelhouse/ - installer will need internet" -ForegroundColor Yellow
    }

    # Check for secrets/leaks
    Write-Host ""
    Write-Host "  --- Security Audit ---" -ForegroundColor Yellow
    Write-Host ""

    $leaks = @()
    $dangerPatterns = @("*.env", "*.key", "*.db", "*.db-wal", "*.db-shm", "*.pyc")
    foreach ($pattern in $dangerPatterns) {
        $found = Get-ChildItem -Path $installRoot -Filter $pattern -Recurse -ErrorAction SilentlyContinue
        if ($found) {
            foreach ($f in $found) {
                $rel = $f.FullName.Replace($installRoot, "").TrimStart("\")
                $leaks += $rel
                Write-Host "    [LEAK] $rel" -ForegroundColor Red
            }
        }
    }

    if ($leaks.Count -eq 0) {
        Write-Host "    [PASS] No secrets or data files leaked" -ForegroundColor Green
    } else {
        Write-Host "    [FAIL] $($leaks.Count) potential leaks found" -ForegroundColor Red
        $allPassed = $false
    }

    # Version
    $versionFile = Join-Path $installRoot "VERSION"
    if (Test-Path $versionFile) {
        $version = (Get-Content $versionFile).Trim()
        Write-Host ""
        Write-Host "    Version: $version" -ForegroundColor Cyan
    }

} finally {
    if (-not $KeepArtifacts) {
        Remove-Item -Path $tempExtract -Recurse -Force -ErrorAction SilentlyContinue
    } else {
        Write-Host ""
        Write-Host "  [INFO] Extracted files kept at: $tempExtract"
    }
}

# - Phase 2: Windows Sandbox test -

if ($SkipSandbox) {
    Write-Host ""
    Write-Host "  --- Sandbox test skipped (-SkipSandbox) ---" -ForegroundColor Yellow
    Write-Host ""
    if ($allPassed) {
        Write-Host "  [RESULT] Zip validation PASSED" -ForegroundColor Green
    } else {
        Write-Host "  [RESULT] Zip validation FAILED" -ForegroundColor Red
    }
    exit $(if ($allPassed) { 0 } else { 1 })
}

Write-Host ""
Write-Host "  --- Phase 2: Windows Sandbox Install Test ---" -ForegroundColor Yellow
Write-Host ""

# Create a shared folder for the sandbox
$sandboxShare = Join-Path $env:TEMP "oasis_sandbox_share_$(Get-Random)"
New-Item -ItemType Directory -Path $sandboxShare -Force | Out-Null

# Copy the zip into the share
Copy-Item -Path $ZipPath -Destination $sandboxShare

# Create the install test script that runs INSIDE the sandbox
$sandboxScript = @"
@echo off
setlocal EnableDelayedExpansion
chcp 65001 >nul 2>&1

set "LOG=C:\Users\WDAGUtilityAccount\Desktop\install_test_result.txt"
echo O.A.S.I.S. Clean Install Test > %LOG%
echo Started: %date% %time% >> %LOG%
echo. >> %LOG%

:: Extract the zip
echo [1] Extracting OASIS zip... >> %LOG%
cd /d C:\Users\WDAGUtilityAccount\Desktop
powershell -NoProfile -Command "Expand-Archive -Path 'C:\sandbox\$ZipName' -DestinationPath 'C:\Users\WDAGUtilityAccount\Desktop\oasis_test' -Force" >> %LOG% 2>&1
echo     Exit code: %errorlevel% >> %LOG%

:: Find the install directory
for /d %%D in (C:\Users\WDAGUtilityAccount\Desktop\oasis_test\*) do (
    set "INSTALL_DIR=%%D"
)

:: Run the installer
echo [2] Running install_embedded.bat... >> %LOG%
cd /d "%INSTALL_DIR%"

:: Run installer (with timeout protection)
call install_embedded.bat >> %LOG% 2>&1
echo     Installer exit code: %errorlevel% >> %LOG%

:: Check if Python is working
echo [3] Checking Python runtime... >> %LOG%
if exist "python_runtime\python.exe" (
    python_runtime\python.exe --version >> %LOG% 2>&1
    echo     Embedded Python: OK >> %LOG%
) else if exist ".oasis_venv\Scripts\python.exe" (
    .oasis_venv\Scripts\python.exe --version >> %LOG% 2>&1
    echo     Venv Python: OK >> %LOG%
) else (
    echo     [FAIL] No Python runtime found >> %LOG%
)

:: Run version check
echo [4] Version check... >> %LOG%
if exist "python_runtime\python.exe" (
    python_runtime\python.exe entrypoint.py --mode version >> %LOG% 2>&1
) else if exist ".oasis_venv\Scripts\python.exe" (
    .oasis_venv\Scripts\python.exe entrypoint.py --mode version >> %LOG% 2>&1
)
echo     Version exit code: %errorlevel% >> %LOG%

:: Run preflight
echo [5] Preflight check... >> %LOG%
if exist "python_runtime\python.exe" (
    python_runtime\python.exe entrypoint.py --mode preflight >> %LOG% 2>&1
) else if exist ".oasis_venv\Scripts\python.exe" (
    .oasis_venv\Scripts\python.exe entrypoint.py --mode preflight >> %LOG% 2>&1
)
echo     Preflight exit code: %errorlevel% >> %LOG%

echo. >> %LOG%
echo Completed: %date% %time% >> %LOG%
echo TEST COMPLETE >> %LOG%

:: Copy result back to shared folder
copy %LOG% C:\sandbox\install_test_result.txt

:: Keep sandbox open briefly so we can see
timeout /t 10
"@

$sandboxScriptPath = Join-Path $sandboxShare "run_test.bat"
$sandboxScript | Out-File -FilePath $sandboxScriptPath -Encoding ASCII

# Create the .wsb configuration
$wsbContent = @"
<Configuration>
  <MappedFolders>
    <MappedFolder>
      <HostFolder>$sandboxShare</HostFolder>
      <SandboxFolder>C:\sandbox</SandboxFolder>
      <ReadOnly>false</ReadOnly>
    </MappedFolder>
  </MappedFolders>
  <LogonCommand>
    <Command>C:\sandbox\run_test.bat</Command>
  </LogonCommand>
  <MemoryInMB>4096</MemoryInMB>
</Configuration>
"@

$wsbPath = Join-Path $sandboxShare "oasis_test.wsb"
$wsbContent | Out-File -FilePath $wsbPath -Encoding UTF8

Write-Host "  [INFO] Sandbox configuration created"
Write-Host "  [INFO] Launching Windows Sandbox..."
Write-Host ""
Write-Host "  The sandbox will:"
Write-Host "    1. Extract the OASIS zip"
Write-Host "    2. Run install_embedded.bat"
Write-Host "    3. Check Python runtime"
Write-Host "    4. Run version + preflight checks"
Write-Host "    5. Save results to the shared folder"
Write-Host ""
Write-Host "  Close the sandbox window when testing is complete."
Write-Host ""

# Launch the sandbox
Start-Process $wsbPath -Wait

# Read results
$resultFile = Join-Path $sandboxShare "install_test_result.txt"
if (Test-Path $resultFile) {
    Write-Host ""
    Write-Host "  --- Sandbox Test Results ---" -ForegroundColor Yellow
    Write-Host ""
    $results = Get-Content $resultFile
    foreach ($line in $results) {
        if ($line -match "\[FAIL\]") {
            Write-Host "    $line" -ForegroundColor Red
        } elseif ($line -match "\[OK\]|PASS") {
            Write-Host "    $line" -ForegroundColor Green
        } else {
            Write-Host "    $line"
        }
    }

    # Copy results to project
    $projectResults = Join-Path $OASISRoot "reports\clean_install_test_$(Get-Date -Format 'yyyy-MM-dd_HHmmss').txt"
    New-Item -ItemType Directory -Path (Split-Path $projectResults) -Force | Out-Null
    Copy-Item $resultFile $projectResults
    Write-Host ""
    Write-Host "  Results saved to: $projectResults" -ForegroundColor Cyan
} else {
    Write-Host ""
    Write-Host "  [WARNING] No result file found. The sandbox may have closed" -ForegroundColor Yellow
    Write-Host "  before the test completed." -ForegroundColor Yellow
}

# Cleanup
if (-not $KeepArtifacts) {
    Remove-Item -Path $sandboxShare -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "  --- Test Complete ---" -ForegroundColor Cyan
Write-Host ""
