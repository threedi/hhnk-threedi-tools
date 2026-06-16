@echo off
setlocal enabledelayedexpansion

set user_dir=%~1

REM Controleer of de directory bestaat, anders vraag of het moet worden aangemaakt
if not exist "%user_dir%" (
    echo Directory %user_dir% does not exist, create it first and initialize the directory as git repository
    exit /b 1
)

REM Controleer of git is geïnitialiseerd, anders vraag of het moet worden aangemaakt
if not exist "%user_dir%\.git" (
    echo Directory %user_dir% is not a git repository, initialize it first
    exit /b 1
)
