@echo off

# Roep Python aan binnen de pixi-omgeving
pixi run python "$@"
if errorlevel 1 (
    echo Error: Python script failed
    exit /b 1
)
