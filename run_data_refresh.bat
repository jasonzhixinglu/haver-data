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
setlocal enabledelayedexpansion

call :log "Starting pull"

:: Add git to PATH
set PATH=%PATH%;D:\Apps\Git\bin

:: Capture HEAD before pull to detect config changes
for /f %%i in ('git rev-parse HEAD') do set OLD_HEAD=%%i

:: Pull latest config from GitHub (verbose output to log only)
git pull >> logs\scheduler.log 2>&1

:: Report config status: series count, new codes, or no change
for /f %%i in ('git rev-parse HEAD') do set NEW_HEAD=%%i
D:\APPS\python\Python311\python.exe -c "print(open('config/series.yaml').read().count('- code:'))" > "%TEMP%\hd_count.txt" 2>nul
set /p SERIES_COUNT=<"%TEMP%\hd_count.txt"
del "%TEMP%\hd_count.txt" 2>nul

if not "!OLD_HEAD!"=="!NEW_HEAD!" (
    git diff !OLD_HEAD! !NEW_HEAD! -- config/series.yaml > "%TEMP%\hd_diff.txt" 2>&1
    findstr /C:"+- code:" "%TEMP%\hd_diff.txt" > "%TEMP%\hd_new.txt" 2>nul
    for %%A in ("%TEMP%\hd_new.txt") do set NEW_SIZE=%%~zA
    if !NEW_SIZE! gtr 0 (
        call :log "New series added to config (total: !SERIES_COUNT!):"
        type "%TEMP%\hd_new.txt"
        type "%TEMP%\hd_new.txt" >> logs\scheduler.log
    ) else (
        call :log "Config updated, no new series (total: !SERIES_COUNT!)"
    )
    del "%TEMP%\hd_diff.txt" "%TEMP%\hd_new.txt" 2>nul
) else (
    call :log "No config changes (!SERIES_COUNT! series tracked)"
)

:: Run the pull script (output to log only)
D:\APPS\python\Python311\python.exe src/pull.py >> logs\scheduler.log 2>&1

:: Show pull outcome: last line of pull.log + error count if any
D:\APPS\python\Python311\python.exe -c "lines=open('logs/pull.log').readlines(); errs=sum(1 for l in lines if 'ERROR:' in l); last=next((l.strip() for l in reversed(lines) if l.strip()),''); print(last + (' | ' + str(errs) + ' error(s)' if errs else ''))" > "%TEMP%\hd_out.txt" 2>nul
set /p PULL_OUTCOME=<"%TEMP%\hd_out.txt"
call :log "!PULL_OUTCOME!"
del "%TEMP%\hd_out.txt" 2>nul

:: Commit and push updated data
git add data\data.parquet data\metadata.parquet logs\pull.log
git commit -m "Auto pull %date% %time%" >> logs\scheduler.log 2>&1
if errorlevel 1 (
    call :log "Nothing to commit (no data changes)"
) else (
    git push >> logs\scheduler.log 2>&1
    if errorlevel 1 (
        call :log "ERROR: git push failed - check scheduler.log"
    ) else (
        call :log "Pushed commit to GitHub"
    )
)

call :log "Done"
goto :eof

:log
set _MSG=[%date% %time%] %~1
echo !_MSG!
echo !_MSG! >> logs\scheduler.log
exit /b
