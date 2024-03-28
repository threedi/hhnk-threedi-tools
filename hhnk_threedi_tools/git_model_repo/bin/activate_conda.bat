@echo off

REM load settings from .env file and set them as variables
REM for /f "eol=- delims=" %%a in (%~dp0..\.env) do set "%%a"
for /f "eol=# delims=" %%a in (%~dp0..\.env) do if not "%%a"=="" set "%%a"

if NOT DEFINED ACTIVE_ENV echo ".env file is missing or setting 'ACTIVE_ENV' is missing"
if NOT DEFINED CONDA_CMD echo ".env file is missing or setting 'CONDA_DIR' is missing"

call "%CONDA_CMD" activate %ACTIVE_ENV% %*
