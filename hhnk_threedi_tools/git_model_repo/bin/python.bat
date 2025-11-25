@echo off

REM Roep Python aan binnen de pixi-omgeving
rem Prefer an explicit project root so pixi always uses the intended pixi.toml.
rem Fallback to three-levels-up from the script location if explicit root is not available.
set "EXPLICIT_ROOT=E:\\02.modelrepos\\hhnk-threedi-tools"

if exist "%EXPLICIT_ROOT%\pixi.toml" (
    pushd "%EXPLICIT_ROOT%" >NUL 2>&1 || (
        echo Failed to change directory to explicit project root "%EXPLICIT_ROOT%"
        exit /b 1
    )
) else (
    rem Fallback: change to the repository root (three levels up from this script)
    pushd "%~dp0..\..\.." >NUL 2>&1 || (
        echo Failed to change directory to project root "%~dp0..\..\.."
        exit /b 1
    )
)

pixi run python %*
set "rc=%ERRORLEVEL%"

if %rc% neq 0 (
    echo Error: Python script failed (exit %rc%)
)

popd
exit /b %rc%
