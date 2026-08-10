@echo off
setlocal

set "APP_DIR=D:\vergelijkingstool"

cd /d "%APP_DIR%"
if errorlevel 1 goto error

set "PIXI_CACHE_DIR=%APP_DIR%\.pixi\cache"

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