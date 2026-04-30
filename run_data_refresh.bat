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

call :log "Starting pull"

:: Add git to PATH and disable interactive credential prompts
:: (otherwise an expired PAT causes git push to hang waiting for input)
set PATH=%PATH%;D:\Apps\Git\bin
set GIT_TERMINAL_PROMPT=0

:: Capture HEAD before pull to detect config changes
for /f %%i in ('git rev-parse HEAD') do set OLD_HEAD=%%i

:: Pull latest config from GitHub
git pull >> logs\scheduler.log 2>&1

:: Report whether config changed
for /f %%i in ('git rev-parse HEAD') do set NEW_HEAD=%%i
if not "%OLD_HEAD%"=="%NEW_HEAD%" (
    call :log "Config changed - see git diff for details"
    git diff %OLD_HEAD% %NEW_HEAD% -- config/series.yaml >> logs\scheduler.log 2>&1
) else (
    call :log "No config changes"
)

:: Run the pull script
call :log "Running pull.py..."
D:\APPS\python\Python311\python.exe src/pull.py >> logs\scheduler.log 2>&1
call :log "pull.py finished"

:: Stage data files
git add data\data.parquet data\metadata.parquet logs\pull.log

:: Try to commit
git commit -m "Auto pull %date% %time%" >> logs\scheduler.log 2>&1
if errorlevel 1 (
    call :log "Nothing to commit"
    goto :done
)

:: Push - redirect stdin from nul so git can never prompt
call :log "Pushing to GitHub..."
git push < nul >> logs\scheduler.log 2>&1
if errorlevel 1 (
    call :log "ERROR: git push failed - check scheduler.log for details"
) else (
    call :log "Pushed commit to GitHub"
)

:done
call :log "Done"
goto :eof

:log
echo [%date% %time%] %~1
echo [%date% %time%] %~1 >> logs\scheduler.log
exit /b
