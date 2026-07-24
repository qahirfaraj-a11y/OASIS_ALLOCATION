@echo off
echo Starting Pipeline Daemon...
python -m oasis.logic.scheduler_service
pause
