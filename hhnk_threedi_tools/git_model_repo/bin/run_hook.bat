@echo off

cls
SET commando=%~dp0python.bat

call "%commando%" %~dp0../run_hook.py %*
