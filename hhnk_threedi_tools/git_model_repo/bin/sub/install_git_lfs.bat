@echo off
setlocal enabledelayedexpansion

REM Als argument leeg is, neem dan de werkdirectory van de gebruiker
if "%~1"=="" (
    set user_dir=%cd%
) else (
    set user_dir=%~1
)

REM exit if script fails
if not exist "%~dp0\check_dir_is_git_repo.bat" (
    echo Het bestand check_dir_is_git_repo.bat bestaat niet.
    exit /b 1
)

call %~dp0\check_dir_is_git_repo.bat "%user_dir%"
if %errorlevel% neq 0 exit /b %errorlevel%

REM make sure LFS is installed
call git lfs install --force

set path_file=%user_dir%\.gitattributes

REM create an .gitattributes if not exists yet
if not exist "%path_file%" (
    echo Creating .gitattributes file
    (
        echo *.nc filter=lfs diff=lfs merge=lfs -text
        echo *.tiff filter=lfs diff=lfs merge=lfs -text
        echo *.tif filter=lfs diff=lfs merge=lfs -text
        echo *.gpkg filter=lfs diff=lfs merge=lfs -text
        echo *.sqlite filter=lfs diff=lfs merge=lfs -text
        echo *.xlsx filter=lfs diff=lfs merge=lfs -text
    ) > "%path_file%"

    call git add "%path_file%"
    call git commit -m "Add .gitattributes file for LFS"
) else (
    echo .gitattributes file already exists, make sure it contains the correct LFS filters
)
