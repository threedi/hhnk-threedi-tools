@echo off
cls

setlocal

set PYTHON_CMD="%~dp0python.bat"
set SCRIPT="%~dp0..\run_hook.py"

echo Using Python: %PYTHON_CMD%
echo Running script: %SCRIPT%

call %PYTHON_CMD% %SCRIPT% %*

endlocal