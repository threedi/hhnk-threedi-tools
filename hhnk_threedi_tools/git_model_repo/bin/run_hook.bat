@echo off

cls

set PYTHON_CMD="%~dp0python.bat"
set SCRIPT="D:\github\00_modellen_db\hhnk-threedi-tools\hhnk_threedi_tools\git_model_repo\run_hook.py"

call %PYTHON_CMD% %SCRIPT% %*

