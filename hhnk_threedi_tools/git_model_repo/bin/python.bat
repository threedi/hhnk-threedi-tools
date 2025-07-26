@echo off

REM Make sure we don't accidentally use Python libraries outside the virtualenv
set PYTHONPATH=
set PYTHONHOME=

call %~dp0activate_conda.bat

REM Call Python in the virtualenv
call python.exe %*
if errorlevel 1 (
    echo Error: Python script failed
    exit /b 1
)
