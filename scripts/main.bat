@echo on
echo started %date% %time% >> %~dp0\backup.log
call activate WaimakTide
call python %~dp0\main.py >> %~dp0\backup.log
echo ended %date% %time% >> %~dp0\backup.log
echo ============= >> %~dp0\backup.log