@echo off

SET commando=%~dp0python.bat

"%commando%" %~dp0../run_hook.py %*
