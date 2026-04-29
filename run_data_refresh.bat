@echo off

:: Map network path to a drive letter
net use Z: "\\EMDSWN45P\data\jlu2\haver_data" /persistent:no 2>nul

:: Re-launch from mapped drive if not already there
if "%~d0"=="Z:" goto :main
Z:
cd "Z:\haver-data"
cmd /c "Z:\haver-data\run_data_refresh.bat"
net use Z: /delete /yes
exit /b

:main
cd /d "Z:\haver-data"

echo [%date% %time%] Starting pull >> logs\scheduler.log

:: Add git to PATH
set PATH=%PATH%;D:\Apps\Git\bin

:: Capture HEAD before pull to detect config changes
for /f %%i in ('git rev-parse HEAD') do set OLD_HEAD=%%i

:: Pull latest config from GitHub
git pull >> logs\scheduler.log 2>&1

:: Report any new series added to config
for /f %%i in ('git rev-parse HEAD') do set NEW_HEAD=%%i
if not "%OLD_HEAD%"=="%NEW_HEAD%" (
    git diff %OLD_HEAD% %NEW_HEAD% -- config/series.yaml > "%TEMP%\series_diff.txt" 2>&1
    findstr /C:"+- code:" "%TEMP%\series_diff.txt" > nul 2>&1
    if not errorlevel 1 (
        echo [%date% %time%] New series detected in config: >> logs\scheduler.log
        findstr /C:"+- code:" "%TEMP%\series_diff.txt" >> logs\scheduler.log
    )
    del "%TEMP%\series_diff.txt" 2>nul
)

:: Run the pull script
D:\APPS\python\Python311\python.exe src/pull.py >> logs\scheduler.log 2>&1

:: Commit and push updated data
git add data\data.parquet data\metadata.parquet logs\pull.log
git commit -m "Auto pull %date% %time%" >> logs\scheduler.log 2>&1
git push >> logs\scheduler.log 2>&1

echo [%date% %time%] Done >> logs\scheduler.log