@echo off

SET commando=%~dp0python.cmd

"%commando%" %~dp0../install_hooks.py %*
