@echo off
setlocal

set "APP_DIR=D:\vergelijkingstool"
set "USER_APP_DIR=%USERPROFILE%\vergelijkingstool"
set "PIXI_CONFIG_FILE=%USER_APP_DIR%\pixi-config.toml"

if not exist "%USER_APP_DIR%" mkdir "%USER_APP_DIR%"

(
echo detached-environments = "%USERPROFILE:\=/%/vergelijkingstool/.pixi"
) > "%PIXI_CONFIG_FILE%"

cd /d "%APP_DIR%"
if errorlevel 1 goto error

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