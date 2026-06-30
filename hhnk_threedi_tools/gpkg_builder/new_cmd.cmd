@echo off
setlocal

set "QGIS_BAT=C:\Program Files\3DiModellerInterface 3.34\bin\python-qgis-ltr.bat"
set "SCRIPT=%~dp0run_impervious_surface.py"

set "QGIS_PLUGIN_DIR=%APPDATA%\3Di\QGIS3\profiles\default\python\plugins"
set "THREEDI_DEPS=%QGIS_PLUGIN_DIR%\threedi_results_analysis\deps"

set "PYTHONPATH=%THREEDI_DEPS%;%QGIS_PLUGIN_DIR%;%PYTHONPATH%"

echo QGIS_BAT=%QGIS_BAT%
echo SCRIPT=%SCRIPT%
echo QGIS_PLUGIN_DIR=%QGIS_PLUGIN_DIR%
echo THREEDI_DEPS=%THREEDI_DEPS%
echo.

call "%QGIS_BAT%" "%SCRIPT%" %*

exit /b %ERRORLEVEL%