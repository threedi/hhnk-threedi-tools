@echo off
setlocal

cd /d "D:\vergelijkingstool"
if errorlevel 1 goto error

pixi install
if errorlevel 1 goto error

git config --global --add safe.directory "D:/vergelijkingstool/packages/hhnk-threedi-tools"
if errorlevel 1 goto error

pixi run update-tools
if errorlevel 1 goto error

pixi run start
if errorlevel 1 goto error

exit /b 0

:error
echo.
echo Vergelijkingstool could not be installed or started.
pause
exit /b 1