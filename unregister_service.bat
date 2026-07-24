@echo off
schtasks /end /tn "OASIS Service" >nul 2>&1
schtasks /delete /tn "OASIS Service" /f
pause
