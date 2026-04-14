@echo off

:: Map network path to a drive letter
net use Z: "\\EMDSWN45P\data\jlu2\haver_data" /persistent:no 2>nul

:: Re-launch from mapped drive if not already there
if "%~d0"=="Z:" goto :main
Z:
cd "Z:\haver-data"
cmd /c "Z:\haver-data\run_pull.bat"
net use Z: /delete /yes
exit /b

:main
cd /d "Z:\haver-data"

echo [%date% %time%] Starting pull >> logs\scheduler.log

:: Add git to PATH
set PATH=%PATH%;D:\Apps\Git\bin

:: Pull latest config from GitHub
git pull >> logs\scheduler.log 2>&1

:: Run the pull script
D:\APPS\python\Python311\python.exe src/pull.py >> logs\scheduler.log 2>&1

:: Commit and push updated data
git add data\data.parquet data\metadata.parquet logs\pull.log
git commit -m "Auto pull %date% %time%" >> logs\scheduler.log 2>&1
git push >> logs\scheduler.log 2>&1

echo [%date% %time%] Done >> logs\scheduler.log