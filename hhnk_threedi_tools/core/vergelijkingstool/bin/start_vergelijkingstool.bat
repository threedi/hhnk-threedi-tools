@echo off
setlocal

REM ============================================================
REM Vergelijkingstool launcher
REM ============================================================

set "SHARED_DIR=D:\vergelijkingstool"
set "USER_DIR=%USERPROFILE%\vergelijkingstool"
set "TOOLS_DIR=%USER_DIR%\packages\hhnk-threedi-tools"
set "BRANCH=26057-Vergelijkingstool"
set "REPO_URL=https://github.com/threedi/hhnk-threedi-tools.git"

echo.
echo ============================================================
echo Starting Vergelijkingstool
echo ============================================================
echo User workspace: %USER_DIR%
echo.

REM ------------------------------------------------------------
REM 1. Create local user workspace
REM ------------------------------------------------------------

if not exist "%USER_DIR%" mkdir "%USER_DIR%"
if not exist "%USER_DIR%\packages" mkdir "%USER_DIR%\packages"

REM ------------------------------------------------------------
REM 2. Copy latest Pixi configuration from shared folder
REM ------------------------------------------------------------

copy /Y "%SHARED_DIR%\pixi.toml" "%USER_DIR%\pixi.toml" >nul
if errorlevel 1 goto error

copy /Y "%SHARED_DIR%\pixi.lock" "%USER_DIR%\pixi.lock" >nul
if errorlevel 1 goto error

REM ------------------------------------------------------------
REM 3. Clone hhnk-threedi-tools if not present
REM ------------------------------------------------------------

if not exist "%TOOLS_DIR%\.git" (
    echo.
    echo Installing hhnk-threedi-tools for this user...
    git clone --branch "%BRANCH%" "%REPO_URL%" "%TOOLS_DIR%"
    if errorlevel 1 goto error
)

REM ------------------------------------------------------------
REM 4. Update local hhnk-threedi-tools repository
REM ------------------------------------------------------------

echo.
echo Updating hhnk-threedi-tools...

git -C "%TOOLS_DIR%" switch "%BRANCH%"
if errorlevel 1 goto error

git -C "%TOOLS_DIR%" pull --ff-only origin "%BRANCH%"
if errorlevel 1 goto error

REM ------------------------------------------------------------
REM 5. Make hhnk_threedi_tools available to Python
REM ------------------------------------------------------------

set "PYTHONPATH=%TOOLS_DIR%;%PYTHONPATH%"

REM ------------------------------------------------------------
REM 6. Install/update Pixi environment
REM ------------------------------------------------------------

cd /d "%USER_DIR%"
if errorlevel 1 goto error

echo.
echo Checking Pixi environment...

pixi install
if errorlevel 1 goto error

REM ------------------------------------------------------------
REM 7. Start Vergelijkingstool
REM ------------------------------------------------------------

echo.
echo Starting Vergelijkingstool...

pixi run jupyter lab --FileCheckpoints.checkpoint_dir=.pixi/checkpoints "%TOOLS_DIR%\hhnk_threedi_tools\core\vergelijkingstool\app\vergelijkingstool.ipynb"
if errorlevel 1 goto error

exit /b 0


:error
echo.
echo ============================================================
echo Vergelijkingstool could not be installed or started.
echo ============================================================
echo.
pause
exit /b 1