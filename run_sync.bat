@echo off

cd /d D:\

net use Z: "\\EMDSWN45P\data\jlu2\haver_data" /persistent:no 2>nul

cd /d Z:\haver-data

set PATH=%PATH%;D:\Apps\Git\bin

echo [%date% %time%] Syncing latest from GitHub...
git pull

echo [%date% %time%] Done.

net use Z: /delete /yes
