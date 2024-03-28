#!/bin/zsh

# Stop het script als er fouten optreden
set -e

# Laad instellingen vanuit .env bestand en stel ze in als variabelen
if [ -f "$(dirname "$0")/../../.env" ]; then
    export $(grep -v '^#' "$(dirname "$0")/../../.env" | xargs)
else
    echo ".env bestand ontbreekt"
    exit 1
fi

# Controleer of de vereiste variabelen zijn ingesteld
if [ -z "$ACTIVE_ENV" ]; then
    echo "Instelling 'ACTIVE_ENV' ontbreekt in het .env bestand"
    exit 1
fi

if [ -z "$CONDA_CMD" ]; then
    echo "Instelling 'CONDA_CMD' ontbreekt in het .env bestand"
    exit 1
fi

eval "$("$CONDA_CMD" shell hook --shell zsh)"

# echo "$CONDA_CMD" activate "$ACTIVE_ENV"
# Activeer de Conda-omgeving
"$CONDA_CMD" activate "$ACTIVE_ENV"
