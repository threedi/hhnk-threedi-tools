@echo off
setlocal enabledelayedexpansion

REM Als argument leeg is, neem dan de werkdirectory van de gebruiker
if "%~1"=="" (
    set user_dir=%cd%
) else (
    set user_dir=%~1
)

REM Controleer of de directory een git repository is
call %~dp0\check_dir_is_git_repo.bat "%user_dir%"
if %errorlevel% neq 0 exit /b %errorlevel%

REM Voer het commando uit
REM voeg directory toe vanuit waar de script wordt aangeroepen
call "D:\github\00_modellen_db\hhnk-threedi-tools\hhnk_threedi_tools\git_model_repo\bin\python.bat" "D:\github\00_modellen_db\hhnk-threedi-tools\hhnk_threedi_tools\git_model_repo\install_hooks.py" "%user_dir%"


