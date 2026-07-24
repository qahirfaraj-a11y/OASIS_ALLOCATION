@echo off
REM Register OASIS with Task Scheduler: starts at logon, restarts what dies.
cd /d "%~dp0"
schtasks /create /tn "OASIS Service" /tr "\"%~dp0serve.bat\"" /sc onlogon /f
if errorlevel 1 (echo Failed - run as Administrator & pause & exit /b 1)
echo OASIS Service registered. It starts automatically at logon.
echo Start it now without logging off:  schtasks /run /tn "OASIS Service"
pause
