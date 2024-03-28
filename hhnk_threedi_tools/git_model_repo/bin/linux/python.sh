#!/bin/zsh

# Maak zeker dat we geen Python libraries gebruiken buiten de virtualenv
unset PYTHONPATH
unset PYTHONHOME

# Activeer Conda
source "$(dirname "$0")/activate_conda.sh"

# Roep Python aan in de virtualenv
python3 "$@"
