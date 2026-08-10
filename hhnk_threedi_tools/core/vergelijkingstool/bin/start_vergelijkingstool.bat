@echo off
setlocal

cd /d "%~dp0"
set "PIXI_CACHE_DIR=%~dp0.pixi-cache"

pixi install
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