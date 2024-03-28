@echo off

SET commando=%~dp0python.cmd

"%commando%" %~dp0../run_hook.py %*
