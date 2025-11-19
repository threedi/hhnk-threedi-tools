@echo off

REM Call Python inside the pixi environment, forwarding all arguments.
REM Use %* to forward all command-line arguments in cmd.exe (Windows).

REM When hooks or callers start cmd.exe with a UNC working directory, some
REM commands fail. Temporarily switch to the script's parent directory which
REM resides on a local drive using pushd. After running, restore the
REM original directory with popd. This prevents the "UNC paths are not
REM supported" behaviour.
pushd "%~dp0.." >nul 2>&1

pixi run python %*
set "_RC=%ERRORLEVEL%"

popd >nul 2>&1

if not "%_RC%"=="0" (
    echo Error: Python script failed (exit code %_RC%)
    exit /b %_RC%
)
