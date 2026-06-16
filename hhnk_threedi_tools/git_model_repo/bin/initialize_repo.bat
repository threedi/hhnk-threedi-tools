@echo off
setlocal enabledelayedexpansion

REM Als argument leeg is, neem dan de werkdirectory van de gebruiker
if "%~1"=="" (
    set user_dir=%cd%
) else (
    set user_dir=%~1
)

call %~dp0\sub\install_git_lfs.bat "%user_dir%"
if %errorlevel% neq 0 exit /b %errorlevel%

call %~dp0\sub\install_git_hooks.bat "%user_dir%"
if %errorlevel% neq 0 exit /b %errorlevel%

echo Git hooks and LFS installed
